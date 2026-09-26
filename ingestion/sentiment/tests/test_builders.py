"""Tests cho builders."""

from __future__ import annotations

import pytest

from ingestion.sentiment.adapters import Comment
from ingestion.sentiment.builders import build_s1_row, build_s2_rows, process_comments_to_sentences, sentence_hash


class TestBuilders:
    def test_sentence_hash_deterministic(self):
        h1 = sentence_hash("câu test")
        h2 = sentence_hash("câu test")
        assert h1 == h2
        assert len(h1) == 16

    def test_build_s1_row(self):
        c = Comment(
            source="tripadvisor",
            comment_id="r1",
            place_id="p1",
            place_name="Hotel",
            raw_text="Phòng sạch",
            rating=5.0,
            collected_at="2024-01-01T00:00:00",
            meta={"author": "John"},
        )
        row = build_s1_row(c, normalized_text="phòng sạch", run_id="run1")
        assert row["source"] == "tripadvisor"
        assert row["comment_id"] == "r1"
        assert row["normalized_text"] == "phòng sạch"
        assert row["run_id"] == "run1"

    def test_build_s2_rows_dedupe(self):
        s1 = {
            "source": "tripadvisor",
            "comment_id": "r1",
            "place_id": "p1",
            "place_name": "Hotel",
            "normalized_text": "phòng sạch. phòng sạch.",
            "rating": 5.0,
        }
        rows = build_s2_rows(s1, sentences=["phòng sạch", "phòng sạch"], text_cfg={}, run_id="run1")
        # Deduped: chỉ 1 câu
        assert len(rows) == 1
        assert rows[0]["sentence_text"] == "phòng sạch"

    def test_process_comments_to_sentences(self):
        comments = [
            Comment(
                source="tripadvisor",
                comment_id="r1",
                place_id="p1",
                place_name="Hotel",
                raw_text="Phòng sạch. View đẹp!",
                rating=5.0,
                collected_at="2024-01-01",
                meta={},
            )
        ]
        s1_rows, s2_rows = process_comments_to_sentences(comments, run_id="run1", text_cfg={})
        assert len(s1_rows) == 1
        assert len(s2_rows) == 2  # 2 câu
        assert s2_rows[0]["sentence_text"] == "phòng sạch"
        assert s2_rows[1]["sentence_text"] == "view đẹp"