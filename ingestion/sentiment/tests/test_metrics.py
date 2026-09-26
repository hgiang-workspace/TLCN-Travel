"""Tests cho metrics report."""

from __future__ import annotations

import pytest

from ingestion.sentiment.report import compute_metrics, compute_label_consistency


class TestMetrics:
    def test_compute_metrics_basic(self):
        valid = [
            {"label": "positive", "score": 0.9},
            {"label": "positive", "score": 0.8},
            {"label": "negative", "score": 0.7},
            {"label": "neutral", "score": 0.6},
        ]
        quarantined = [
            {"quarantine_reasons": ["LOW_CONFIDENCE"]},
            {"quarantine_reasons": ["TOO_SHORT"]},
        ]
        metrics = compute_metrics(valid, quarantined)
        assert metrics["total_input"] == 6
        assert metrics["valid"] == 4
        assert metrics["quarantined"] == 2
        assert metrics["quarantine_rate"] == pytest.approx(2/6, rel=1e-3)
        assert metrics["label_distribution"]["positive"] == 2
        assert metrics["label_distribution"]["negative"] == 1
        assert metrics["label_distribution"]["neutral"] == 1

    def test_empty_input(self):
        metrics = compute_metrics([], [])
        assert metrics["total_input"] == 0
        assert metrics["valid"] == 0

    def test_label_consistency_tripadvisor(self):
        valid = [
            {"label": "positive", "rating": 5.0},
            {"label": "positive", "rating": 4.0},
            {"label": "neutral", "rating": 3.0},
            {"label": "negative", "rating": 2.0},
            {"label": "negative", "rating": 1.0},
            {"label": "positive", "rating": 3.0},  # mismatch: rating 3 -> neutral, label positive
        ]
        consistency = compute_label_consistency(valid)
        assert consistency["total_with_rating"] == 6
        assert consistency["matched"] == 5
        assert consistency["consistency_rate"] == pytest.approx(5/6, rel=1e-3)

    def test_consistency_no_rating(self):
        valid = [{"label": "positive"}, {"label": "negative"}]
        consistency = compute_label_consistency(valid)
        assert consistency["total_with_rating"] == 0
        assert consistency["consistency_rate"] == 0.0