"""Tests cho quality validation."""

from __future__ import annotations

import pytest

from ingestion.sentiment.quality import validate_row, validate_batch, build_quality_config, QualityConfig


class TestQualityValidation:
    def test_valid_row(self):
        row = {
            "sentence_id": "s1",
            "sentence_text": "phòng sạch",
            "sentence_hash": "abc123",
            "label": "positive",
            "score": 0.95,
            "label_id": 2,
        }
        cfg = QualityConfig()
        valid, reasons = validate_row(row, cfg)
        assert valid is True
        assert reasons == []

    def test_invalid_label(self):
        row = {
            "sentence_id": "s1",
            "sentence_text": "phòng sạch",
            "sentence_hash": "abc123",
            "label": "invalid_label",
            "score": 0.95,
            "label_id": 99,
        }
        cfg = QualityConfig()
        valid, reasons = validate_row(row, cfg)
        assert valid is False
        assert "INVALID_LABEL" in reasons[0]

    def test_low_confidence(self):
        row = {
            "sentence_id": "s1",
            "sentence_text": "phòng sạch",
            "sentence_hash": "abc123",
            "label": "positive",
            "score": 0.1,  # < min_score 0.3
            "label_id": 2,
        }
        cfg = QualityConfig()
        valid, reasons = validate_row(row, cfg)
        assert valid is False
        assert "LOW_CONFIDENCE" in reasons[0]

    def test_empty_text(self):
        row = {
            "sentence_id": "s1",
            "sentence_text": "",
            "sentence_hash": "abc123",
            "label": "positive",
            "score": 0.95,
            "label_id": 2,
        }
        cfg = QualityConfig()
        valid, reasons = validate_row(row, cfg)
        assert valid is False
        assert "EMPTY_TEXT" in reasons[0]

    def test_too_short(self):
        row = {
            "sentence_id": "s1",
            "sentence_text": "ok",
            "sentence_hash": "abc123",
            "label": "positive",
            "score": 0.95,
            "label_id": 2,
        }
        cfg = QualityConfig(min_sentence_len=3)
        valid, reasons = validate_row(row, cfg)
        assert valid is False
        assert "TOO_SHORT" in reasons[0]

    def test_missing_fields(self):
        row = {"sentence_id": "s1"}  # thiếu các field khác
        cfg = QualityConfig()
        valid, reasons = validate_row(row, cfg)
        assert valid is False
        assert len(reasons) > 0

    def test_validate_batch(self):
        rows = [
            {"sentence_id": "s1", "sentence_text": "phòng sạch", "sentence_hash": "h1", "label": "positive", "score": 0.9, "label_id": 2},
            {"sentence_id": "s2", "sentence_text": "nhân viên chậm", "sentence_hash": "h2", "label": "negative", "score": 0.8, "label_id": 0},
            {"sentence_id": "s3", "sentence_text": "ok", "sentence_hash": "h3", "label": "neutral", "score": 0.5, "label_id": 1},  # too short
        ]
        valid, quarantined = validate_batch(rows)
        assert len(valid) == 2
        assert len(quarantined) == 1
        assert quarantined[0]["quarantine_reasons"][0] == "TOO_SHORT"

    def test_build_quality_config(self):
        cfg = {"quality": {"min_score": 0.5, "min_sentence_len": 5}}
        qc = build_quality_config(cfg)
        assert qc.min_score == 0.5
        assert qc.min_sentence_len == 5