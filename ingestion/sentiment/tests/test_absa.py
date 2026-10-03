"""Tests cho ABSA assembly."""

from __future__ import annotations

import pytest

from ingestion.sentiment.aspects import assemble_absa_sentence_level, assemble_absa_clause_level, assemble_absa
from ingestion.sentiment.builders import build_s6_rows


def build_mock_s6_rows():
    """Tạo mock S6 rows để test."""
    return [
        {
            "source": "tripadvisor",
            "comment_id": "c1",
            "place_id": "p1",
            "place_name": "Test Place",
            "sentence_id": "c1_s0",
            "sentence_hash": "abc123",
            "sentence_text": "món ăn ngon, phục vụ tốt",
            "sentence_idx": 0,
            "aspect": "food",
            "aspect_keyword": "ngon",
            "aspect_negated": False,
            "rating": 4.5,
            "run_id": "test_run",
            "processed_at": "2024-01-01T00:00:00",
        },
        {
            "source": "tripadvisor",
            "comment_id": "c1",
            "place_id": "p1",
            "place_name": "Test Place",
            "sentence_id": "c1_s0",
            "sentence_hash": "abc123",
            "sentence_text": "món ăn ngon, phục vụ tốt",
            "sentence_idx": 0,
            "aspect": "service",
            "aspect_keyword": "phục vụ",
            "aspect_negated": False,
            "rating": 4.5,
            "run_id": "test_run",
            "processed_at": "2024-01-01T00:00:00",
        },
        {
            "source": "tripadvisor",
            "comment_id": "c2",
            "place_id": "p1",
            "place_name": "Test Place",
            "sentence_id": "c2_s0",
            "sentence_hash": "def456",
            "sentence_text": "không gian đẹp",
            "sentence_idx": 0,
            "aspect": "ambiance",
            "aspect_keyword": "đẹp",
            "aspect_negated": False,
            "rating": 4.0,
            "run_id": "test_run",
            "processed_at": "2024-01-01T00:00:00",
        },
    ]


def test_assemble_absa_sentence_level_stub():
    """Test ABSA sentence-level với stub provider."""
    s6_rows = build_mock_s6_rows()

    s7_rows = assemble_absa_sentence_level(
        s6_rows,
        model_dir="dummy",
        provider="stub",
    )

    assert len(s7_rows) == 3
    for row in s7_rows:
        assert "record_id" in row
        assert "entity_id" in row
        assert "source" in row
        assert "sentence" in row
        assert "aspect" in row
        assert "sentiment" in row
        assert "confidence_score" in row
        assert "platform" in row
        assert "created_at" in row
        assert row["sentiment"] in ("negative", "neutral", "positive")
        assert 0.0 <= row["confidence_score"] <= 1.0
        assert row["clause_idx"] == 0


def test_assemble_absa_sentence_level_negation():
    """Test ABSA sentence-level với negation."""
    s6_rows = [
        {
            "source": "tripadvisor",
            "comment_id": "c1",
            "place_id": "p1",
            "place_name": "Test Place",
            "sentence_id": "c1_s0",
            "sentence_hash": "abc123",
            "sentence_text": "món ăn không ngon",
            "sentence_idx": 0,
            "aspect": "food",
            "aspect_keyword": "ngon",
            "aspect_negated": True,
            "rating": 2.0,
            "run_id": "test_run",
            "processed_at": "2024-01-01T00:00:00",
        },
    ]

    s7_rows = assemble_absa_sentence_level(
        s6_rows,
        model_dir="dummy",
        provider="stub",
    )

    assert len(s7_rows) == 1
    row = s7_rows[0]
    assert row["aspect"] == "food"
    assert row["aspect_negated"] is True


def test_assemble_absa_clause_level_stub():
    """Test ABSA clause-level với stub provider."""
    s6_rows = [
        {
            "source": "tripadvisor",
            "comment_id": "c1",
            "place_id": "p1",
            "place_name": "Test Place",
            "sentence_id": "c1_s0",
            "sentence_hash": "abc123",
            "sentence_text": "món ăn ngon nhưng phục vụ kém",
            "sentence_idx": 0,
            "aspect": "food",
            "aspect_keyword": "ngon",
            "aspect_negated": False,
            "rating": 4.0,
            "run_id": "test_run",
            "processed_at": "2024-01-01T00:00:00",
        },
        {
            "source": "tripadvisor",
            "comment_id": "c1",
            "place_id": "p1",
            "place_name": "Test Place",
            "sentence_id": "c1_s0",
            "sentence_hash": "abc123",
            "sentence_text": "món ăn ngon nhưng phục vụ kém",
            "sentence_idx": 0,
            "aspect": "service",
            "aspect_keyword": "phục vụ",
            "aspect_negated": False,
            "rating": 4.0,
            "run_id": "test_run",
            "processed_at": "2024-01-01T00:00:00",
        },
    ]

    s7_rows = assemble_absa_clause_level(
        s6_rows,
        model_dir="dummy",
        provider="stub",
        text_cfg={"use_clause_split": True, "min_clause_len": 5},
    )

    assert len(s7_rows) >= 2
    for row in s7_rows:
        assert row["clause_idx"] in (0, 1)


def test_assemble_absa_entry_point():
    """Test entry point assemble_absa."""
    s6_rows = build_mock_s6_rows()

    s7_rows = assemble_absa(
        s6_rows,
        model_dir="dummy",
        granularity="sentence",
        provider="stub",
    )
    assert len(s7_rows) == 3
    for row in s7_rows:
        assert row["clause_idx"] == 0

    s7_rows_clause = assemble_absa(
        s6_rows,
        model_dir="dummy",
        granularity="clause",
        provider="stub",
        text_cfg={"use_clause_split": True, "min_clause_len": 5},
    )
    assert len(s7_rows_clause) >= 3


if __name__ == "__main__":
    pytest.main([__file__, "-v"])