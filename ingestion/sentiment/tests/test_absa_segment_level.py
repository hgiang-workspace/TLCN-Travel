"""Tests cho ABSA segment-level assembly (plan.md §4.3, §7.1)."""

from __future__ import annotations

import pytest

from ingestion.sentiment.aspects import (
    assemble_absa,
    assemble_absa_segment_level,
    build_aspect_segments,
)
from ingestion.sentiment.builders import build_s6_rows, segment_hash
from ingestion.sentiment.aspects.dictionary import (
    build_aspect_keyword_index,
    load_aspect_keywords,
)


def _mock_s2_row(**overrides):
    row = {
        "source": "tripadvisor",
        "comment_id": "c1",
        "place_id": "p1",
        "place_name": "Test Place",
        "sentence_id": "c1_s0",
        "sentence_hash": "abc123",
        "sentence_text": "món ăn ngon nhưng phục vụ kém",
        "sentence_idx": 0,
        "rating": 4.0,
        "run_id": "test_run",
        "processed_at": "2024-01-01T00:00:00",
    }
    row.update(overrides)
    return row


def _build_s6_rows():
    """Chạy segmentation thật trên câu 2 clause -> rows S6 đầy đủ segment."""
    s2 = _mock_s2_row()
    keyword_index = build_aspect_keyword_index(load_aspect_keywords())
    segs = build_aspect_segments(
        s2["sentence_text"],
        cfg={"segment": {"strategy": "clause"}},
        keyword_index=keyword_index,
    )
    return build_s6_rows(s2, aspect_segments=segs, run_id="test_run")


class TestSegmentLevelAssembly:
    def test_builds_segment_rows(self):
        s6_rows = _build_s6_rows()
        aspects = {r["aspect"] for r in s6_rows}
        assert "food" in aspects
        assert "service" in aspects
        for r in s6_rows:
            assert r["segment_text"]
            assert r["segment_hash"] == segment_hash(r["segment_text"])
            assert r["segment_type"] == "clause"

    def test_segment_level_output_fields(self):
        s6_rows = _build_s6_rows()
        s7_rows = assemble_absa_segment_level(
            s6_rows, model_dir="dummy", provider="stub",
        )
        assert len(s7_rows) == len(s6_rows)
        for row in s7_rows:
            assert row["record_id"]
            assert row["aspect"] in ("food", "service")
            assert row["sentiment"] in ("negative", "neutral", "positive")
            assert 0.0 <= row["confidence_score"] <= 1.0
            assert row["segment_text"]
            assert row["segment_hash"] == segment_hash(row["segment_text"])
            assert row["segment_type"] == "clause"
            assert row["negation_applied"] is False
            # "sentence" giữ câu gốc (ML compat), model input nằm ở segment_text
            assert row["sentence"] == _mock_s2_row()["sentence_text"]

    def test_same_segment_same_sentiment(self):
        """Segment trùng hash -> cùng nhãn (cache dedupe hoạt động)."""
        s2 = _mock_s2_row()
        keyword_index = build_aspect_keyword_index(load_aspect_keywords())
        segs = build_aspect_segments(
            "món ăn ngon", cfg={"segment": {"strategy": "clause"}},
            keyword_index=keyword_index,
        )
        # 2 row cùng segment_text -> cùng label
        rows = build_s6_rows(s2, aspect_segments=segs, run_id="test_run")
        rows = rows + [dict(rows[0])]
        s7 = assemble_absa_segment_level(rows, model_dir="dummy", provider="stub")
        assert s7[0]["segment_hash"] == s7[1]["segment_hash"]
        assert s7[0]["sentiment"] == s7[1]["sentiment"]
        assert s7[0]["confidence_score"] == s7[1]["confidence_score"]

    def test_negation_flips_label(self):
        s6_rows = _build_s6_rows()
        for r in s6_rows:
            r["aspect_negated"] = True
            r["segment_text"] = "món không ngon"
            r["segment_hash"] = segment_hash(r["segment_text"])
        s7 = assemble_absa_segment_level(s6_rows, model_dir="dummy", provider="stub")
        for row in s7:
            # label sau khi đảo phải hợp lệ; neutral -> neutral
            assert row["sentiment"] in ("negative", "neutral", "positive")
            if row["sentiment"] != "neutral":
                assert row["negation_applied"] is True

    def test_empty_input(self):
        assert assemble_absa_segment_level([], model_dir="dummy", provider="stub") == []

    def test_entry_point_dispatch(self):
        s6_rows = _build_s6_rows()
        s7 = assemble_absa(
            s6_rows, model_dir="dummy", granularity="aspect_span",
            provider="stub", text_cfg={"use_clause_split": True, "min_clause_len": 3},
        )
        assert len(s7) == len(s6_rows)
        assert all(r["segment_text"] for r in s7)

        # alias "segment" cùng kết quả
        s7_alias = assemble_absa(
            s6_rows, model_dir="dummy", granularity="segment", provider="stub",
        )
        assert len(s7_alias) == len(s7)

    def test_backward_compat_sentence_level(self):
        """Legacy S6 rows (không segment) vẫn chạy được sentence-level."""
        legacy_rows = [
            {
                "source": "tripadvisor", "comment_id": "c1", "place_id": "p1",
                "place_name": "Test Place", "sentence_id": "c1_s0",
                "sentence_hash": "abc123", "sentence_text": "món ăn ngon",
                "sentence_idx": 0, "aspect": "food", "aspect_keyword": "ngon",
                "aspect_negated": False, "rating": 4.0, "run_id": "test_run",
                "processed_at": "2024-01-01T00:00:00",
            },
        ]
        s7 = assemble_absa(legacy_rows, model_dir="dummy", granularity="sentence",
                           provider="stub")
        assert len(s7) == 1
        assert s7[0]["segment_type"] == "sentence"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])