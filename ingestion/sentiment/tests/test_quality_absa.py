"""Tests cho ABSA quality validation và metrics."""

from __future__ import annotations

import pytest

from ingestion.sentiment.quality import (
    ABSAQualityConfig,
    validate_absa_row,
    validate_absa_batch,
    build_absa_quality_config,
)
from ingestion.sentiment.report import compute_absa_metrics


def test_validate_absa_row_valid():
    """Test validate ABSA row hợp lệ."""
    row = {
        "record_id": "tripadvisor_c1_c1_s0_food_c0",
        "entity_id": "p1",
        "source": "tripadvisor",
        "sentence": "món ăn ngon",
        "aspect": "food",
        "sentiment": "positive",
        "confidence_score": 0.9,
        "platform": "tripadvisor",
        "created_at": "2024-01-01T00:00:00",
    }
    cfg = ABSAQualityConfig()
    is_valid, reasons = validate_absa_row(row, cfg)
    assert is_valid is True
    assert reasons == []


def test_validate_absa_row_missing_fields():
    """Test validate ABSA row thiếu fields."""
    row = {
        "record_id": "tripadvisor_c1_c1_s0_food_c0",
    }
    cfg = ABSAQualityConfig()
    is_valid, reasons = validate_absa_row(row, cfg)
    assert is_valid is False
    assert "MISSING_ABSA_FIELDS" in reasons


def test_validate_absa_row_invalid_sentiment():
    """Test validate ABSA row với sentiment không hợp lệ."""
    row = {
        "record_id": "tripadvisor_c1_c1_s0_food_c0",
        "entity_id": "p1",
        "source": "tripadvisor",
        "sentence": "món ăn ngon",
        "aspect": "food",
        "sentiment": "invalid",
        "confidence_score": 0.9,
        "platform": "tripadvisor",
        "created_at": "2024-01-01T00:00:00",
    }
    cfg = ABSAQualityConfig()
    is_valid, reasons = validate_absa_row(row, cfg)
    assert is_valid is False
    assert "INVALID_LABEL" in reasons


def test_validate_absa_row_invalid_score():
    """Test validate ABSA row với score ngoài phạm vi."""
    row = {
        "record_id": "tripadvisor_c1_c1_s0_food_c0",
        "entity_id": "p1",
        "source": "tripadvisor",
        "sentence": "món ăn ngon",
        "aspect": "food",
        "sentiment": "positive",
        "confidence_score": 1.5,
        "platform": "tripadvisor",
        "created_at": "2024-01-01T00:00:00",
    }
    cfg = ABSAQualityConfig()
    is_valid, reasons = validate_absa_row(row, cfg)
    assert is_valid is False
    assert "LOW_CONFIDENCE" in reasons


def test_validate_absa_batch():
    """Test validate batch ABSA rows."""
    valid_row = {
        "record_id": "tripadvisor_c1_c1_s0_food_c0",
        "entity_id": "p1",
        "source": "tripadvisor",
        "sentence": "món ăn ngon",
        "aspect": "food",
        "sentiment": "positive",
        "confidence_score": 0.9,
        "platform": "tripadvisor",
        "created_at": "2024-01-01T00:00:00",
    }
    invalid_row = {
        "record_id": "tripadvisor_c2_c2_s0_service_c0",
        "entity_id": "p1",
        "source": "tripadvisor",
        "sentence": "phục vụ kém",
        "aspect": "service",
        "sentiment": "invalid",
        "confidence_score": 0.8,
        "platform": "tripadvisor",
        "created_at": "2024-01-01T00:00:00",
    }

    valid, quarantined = validate_absa_batch([valid_row, invalid_row])

    assert len(valid) == 1
    assert len(quarantined) == 1
    assert quarantined[0]["quarantine_reasons"] == ["INVALID_LABEL"]


def test_validate_absa_row_empty_segment():
    """Test validate ABSA row với segment_text rỗng (aspect segmentation)."""
    row = {
        "record_id": "tripadvisor_c1_c1_s0_food_c0",
        "entity_id": "p1",
        "source": "tripadvisor",
        "sentence": "món ăn ngon",
        "aspect": "food",
        "sentiment": "positive",
        "confidence_score": 0.9,
        "platform": "tripadvisor",
        "created_at": "2024-01-01T00:00:00",
        "segment_text": "   ",
        "segment_type": "clause",
    }
    cfg = ABSAQualityConfig()
    is_valid, reasons = validate_absa_row(row, cfg)
    assert is_valid is False
    assert "EMPTY_SEGMENT" in reasons


def test_validate_absa_row_valid_segment():
    """Row có segment_text hợp lệ -> vẫn valid."""
    row = {
        "record_id": "tripadvisor_c1_c1_s0_food_c0",
        "entity_id": "p1",
        "source": "tripadvisor",
        "sentence": "món ăn ngon",
        "aspect": "food",
        "sentiment": "positive",
        "confidence_score": 0.9,
        "platform": "tripadvisor",
        "created_at": "2024-01-01T00:00:00",
        "segment_text": "món ăn ngon",
        "segment_type": "clause",
    }
    cfg = ABSAQualityConfig()
    is_valid, reasons = validate_absa_row(row, cfg)
    assert is_valid is True
    assert reasons == []


def test_compute_absa_metrics_segment_keys():
    """Metric ABSA có các key segment (plan 4.11)."""
    rows = [
        {
            "record_id": "r1", "entity_id": "p1", "source": "tripadvisor",
            "sentence": "món ăn ngon", "aspect": "food", "sentiment": "positive",
            "confidence_score": 0.9, "platform": "tripadvisor",
            "created_at": "2024-01-01T00:00:00", "sentence_id": "c1_s0",
            "segment_text": "món ăn ngon", "segment_hash": "h1",
            "segment_type": "clause",
        },
        {
            "record_id": "r2", "entity_id": "p1", "source": "tripadvisor",
            "sentence": "phục vụ tốt", "aspect": "service", "sentiment": "positive",
            "confidence_score": 0.8, "platform": "tripadvisor",
            "created_at": "2024-01-01T00:00:00", "sentence_id": "c1_s0",
            "segment_text": "phục vụ tốt", "segment_hash": "h2",
            "segment_type": "window",
        },
    ]
    metrics = compute_absa_metrics(rows, [], 1)
    assert metrics["segment_type_distribution"]["clause"] == 1
    assert metrics["segment_type_distribution"]["window"] == 1
    assert metrics["unique_segment_ratio"] == 1.0
    assert metrics["avg_segments_per_sentence"] == 2.0


def test_build_absa_quality_config():
    """Test build ABSAQualityConfig từ pipeline config."""
    cfg = {
        "aspects": {
            "quality": {
                "min_aspect_coverage": 0.5,
                "required_aspects": ["food", "service"],
                "max_aspects_per_sentence": 5,
            }
        }
    }
    qcfg = build_absa_quality_config(cfg)
    assert qcfg.min_aspect_coverage == 0.5
    assert qcfg.required_aspects == ("food", "service")
    assert qcfg.max_aspects_per_sentence == 5


def test_compute_absa_metrics():
    """Test compute ABSA metrics."""
    valid_rows = [
        {
            "record_id": "tripadvisor_c1_c1_s0_food_c0",
            "entity_id": "p1",
            "source": "tripadvisor",
            "sentence": "món ăn ngon",
            "aspect": "food",
            "sentiment": "positive",
            "confidence_score": 0.9,
            "platform": "tripadvisor",
            "created_at": "2024-01-01T00:00:00",
            "sentence_id": "c1_s0",
        },
        {
            "record_id": "tripadvisor_c1_c1_s0_service_c0",
            "entity_id": "p1",
            "source": "tripadvisor",
            "sentence": "phục vụ tốt",
            "aspect": "service",
            "sentiment": "positive",
            "confidence_score": 0.8,
            "platform": "tripadvisor",
            "created_at": "2024-01-01T00:00:00",
            "sentence_id": "c1_s0",
        },
        {
            "record_id": "tripadvisor_c2_c2_s0_food_c0",
            "entity_id": "p2",
            "source": "tripadvisor",
            "sentence": "món ăn dở",
            "aspect": "food",
            "sentiment": "negative",
            "confidence_score": 0.7,
            "platform": "tripadvisor",
            "created_at": "2024-01-01T00:00:00",
            "sentence_id": "c2_s0",
        },
    ]
    quarantined_rows = []
    s2_total = 2

    metrics = compute_absa_metrics(valid_rows, quarantined_rows, s2_total)

    assert metrics["total_input"] == 3
    assert metrics["valid"] == 3
    assert metrics["quarantined"] == 0
    assert metrics["aspect_distribution"]["food"] == 2
    assert metrics["aspect_distribution"]["service"] == 1
    assert metrics["sentiment_distribution"]["positive"] == 2
    assert metrics["sentiment_distribution"]["negative"] == 1
    assert metrics["aspect_coverage"] == 1.0
    assert metrics["avg_aspects_per_sentence"] == 1.5


def test_compute_absa_metrics_empty():
    """Test compute ABSA metrics với input rỗng."""
    metrics = compute_absa_metrics([], [], 0)
    assert metrics["total_input"] == 0
    assert metrics["valid"] == 0
    assert metrics["aspect_coverage"] == 0.0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])