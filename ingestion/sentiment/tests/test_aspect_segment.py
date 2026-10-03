"""Tests cho aspect-aware segmentation (plan.md §4.1, §7.1)."""

from __future__ import annotations

import pytest

from ingestion.sentiment.aspects import (
    build_aspect_segments,
    build_aspect_segments_batch,
    segment_hash,
)
from ingestion.sentiment.aspects.dictionary import (
    build_aspect_keyword_index,
    load_aspect_keywords,
)
from ingestion.sentiment.text import split_clauses, window_segment


@pytest.fixture
def keyword_index():
    return build_aspect_keyword_index(load_aspect_keywords())


class TestSegmentHash:
    def test_deterministic(self):
        assert segment_hash("món ăn ngon") == segment_hash("món ăn ngon")

    def test_length_16(self):
        assert len(segment_hash("abc")) == 16


class TestBuildAspectSegments:
    def test_empty_text(self):
        assert build_aspect_segments("") == []
        assert build_aspect_segments(None) == []

    def test_clause_split_two_aspects(self, keyword_index):
        """Plan 7.1: 1 câu 2 clause -> 2 segment, mỗi segment đúng 1 aspect."""
        segs = build_aspect_segments(
            "món ăn ngon nhưng phục vụ kém",
            cfg={"segment": {"strategy": "clause"}},
            keyword_index=keyword_index,
        )
        aspects = {s.aspect for s in segs}
        assert "food" in aspects
        assert "service" in aspects

        by_aspect = {s.aspect: s for s in segs}
        assert "ngon" in by_aspect["food"].segment_text or "món ăn" in by_aspect["food"].segment_text
        assert "phục vụ" in by_aspect["service"].segment_text
        # Clause riêng -> không gộp 2 khía cạnh chung 1 đoạn
        assert by_aspect["food"].segment_text != by_aspect["service"].segment_text
        assert by_aspect["food"].clause_idx != by_aspect["service"].clause_idx

    def test_offsets_in_sentence(self, keyword_index):
        """Plan 7.1: char_start/char_end trỏ đúng keyword trong câu gốc."""
        sentence = "quán này giá chát quá"
        segs = build_aspect_segments(
            sentence,
            cfg={"segment": {"strategy": "clause"}},
            keyword_index=keyword_index,
        )
        price = [s for s in segs if s.aspect == "price"]
        assert price, "phải detect được aspect price"
        seg = price[0]
        assert sentence[seg.char_start:seg.char_end] == seg.keyword

    def test_negation_flag(self, keyword_index):
        # "phòng không sạch" -> keyword "sạch" (cleanliness) bị phủ định.
        segs = build_aspect_segments(
            "phòng không sạch",
            cfg={"segment": {"strategy": "clause"}},
            keyword_index=keyword_index,
        )
        clean_negated = [s for s in segs if s.aspect == "cleanliness" and s.negated]
        assert clean_negated, "match 'sạch' phải bị đánh dấu phủ định"

    def test_unmatched_aspects_other(self, keyword_index):
        segs = build_aspect_segments(
            "xyz abc def ghi jkl",
            cfg={"segment": {"strategy": "clause"}, "keep_unmatched": True},
            keyword_index=keyword_index,
        )
        assert len(segs) == 1
        assert segs[0].aspect == "other"
        assert segs[0].segment_type == "fallback"
        assert segs[0].segment_text

    def test_keep_unmatched_false(self, keyword_index):
        segs = build_aspect_segments(
            "xyz abc def ghi jkl",
            cfg={"segment": {"strategy": "clause"}, "keep_unmatched": False},
            keyword_index=keyword_index,
        )
        assert segs == []

    def test_dedupe_keeps_distinct_aspects(self, keyword_index):
        """Dedupe theo (aspect, text): vẫn giữ 2 aspect khác nhau chung text."""
        segs = build_aspect_segments(
            "món ăn ngon phục vụ tốt",
            cfg={"segment": {"strategy": "clause"}, "dedupe_by_hash": True},
            keyword_index=keyword_index,
        )
        aspect_set = {s.aspect for s in segs}
        assert len(aspect_set) >= 2
        keys = [(s.aspect, segment_hash(s.segment_text)) for s in segs]
        assert len(keys) == len(set(keys))

    def test_max_aspects_cap(self, keyword_index):
        segs = build_aspect_segments(
            "món ăn ngon phục vụ tốt không gian đẹp giá rẻ",
            cfg={"segment": {"strategy": "clause"}, "max_aspects_per_sentence": 2},
            keyword_index=keyword_index,
        )
        assert len(segs) <= 2

    def test_segment_batch(self, keyword_index):
        rows = [
            {"sentence_id": "c1_s0", "sentence_text": "món ăn ngon nhưng phục vụ kém"},
            {"sentence_id": "c2_s0", "sentence_text": "không gian đẹp"},
        ]
        out = build_aspect_segments_batch(rows, cfg={"segment": {"strategy": "clause"}},
                                          keyword_index=keyword_index)
        assert set(out.keys()) == {"c1_s0", "c2_s0"}
        assert len(out["c1_s0"]) >= 2

    def test_prefill_sentence_fallback(self, keyword_index):
        """use_clause_split=False -> segment = cả câu."""
        segs = build_aspect_segments(
            "món ăn ngon phục vụ tốt",
            cfg={"segment": {"strategy": "clause"}, "use_clause_split": False},
            keyword_index=keyword_index,
        )
        assert all(s.segment_text == "món ăn ngon phục vụ tốt" for s in segs)

    def test_window_strategy_narrower(self, keyword_index):
        """Plan 7.1: window phải ngắn hơn (hoặc bằng) clause."""
        sentence = "đồ ăn ở đây khá ổn về chất lượng và hương vị"
        clause = split_clauses(sentence, {"min_clause_len": 3})
        assert len(clause) == 1  # không có từ nối -> 1 clause

        seg_clause = build_aspect_segments(
            sentence,
            cfg={"segment": {"strategy": "clause"}},
            keyword_index=keyword_index,
        )
        seg_window = build_aspect_segments(
            sentence,
            cfg={"segment": {"strategy": "window", "window_before": 2, "window_after": 2}},
            keyword_index=keyword_index,
        )
        assert seg_clause and seg_window
        min_clause = min(len(s.segment_text) for s in seg_clause)
        min_window = min(len(s.segment_text) for s in seg_window)
        assert min_window <= min_clause
        assert any(s.segment_type == "window" for s in seg_window)

    def test_hybrid_multi_aspect(self, keyword_index):
        """Hybrid: clause chứa nhiều aspect -> window cho từng keyword."""
        segs = build_aspect_segments(
            "món ăn ngon phục vụ tốt",
            cfg={"segment": {"strategy": "hybrid", "window_before": 2, "window_after": 2}},
            keyword_index=keyword_index,
        )
        aspect_set = {s.aspect for s in segs}
        assert "food" in aspect_set
        assert "service" in aspect_set
        # window riêng cho từng keyword -> 2 segment không cùng text
        by_aspect = {s.aspect: s for s in segs}
        assert by_aspect["food"].segment_text != by_aspect["service"].segment_text


class TestWindowSegment:
    def test_basic(self):
        text = "món ăn ở đây khá ổn về chất lượng"
        start = text.index("chất lượng")
        seg, s0, e0 = window_segment(text, start, start + len("chất lượng"),
                                     before=2, after=2)
        assert "chất lượng" in seg
        assert text[s0:e0].strip() == seg
        tokens = seg.split()
        assert len(tokens) <= 2 + 2 + 1

    def test_edges(self):
        seg, s0, e0 = window_segment("ngon", 0, 4, before=5, after=5)
        assert seg == "ngon"
        assert (s0, e0) == (0, 4)

    def test_empty(self):
        assert window_segment("", 0, 0) == ("", 0, 0)

    def test_exclude_keyword(self):
        text = "giá cả rất chát luôn"
        start = text.index("chát")
        seg, _, _ = window_segment(text, start, start + 4,
                                   before=5, after=5, include_keyword=False)
        assert "chát" not in seg


if __name__ == "__main__":
    pytest.main([__file__, "-v"])