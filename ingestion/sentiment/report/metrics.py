"""Quality metrics và reporting."""

from __future__ import annotations

from collections import Counter
from typing import Any


def compute_metrics(valid_rows: list[dict[str, Any]], quarantined_rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Tính metric chất lượng từ kết quả S4."""
    total = len(valid_rows) + len(quarantined_rows)
    if total == 0:
        return {
            "total_input": 0, "valid": 0, "quarantined": 0, "quarantine_rate": 0.0,
            "label_distribution": {}, "label_percentage": {},
            "avg_confidence": 0.0, "min_confidence": 0.0, "max_confidence": 0.0,
            "quarantine_reasons": {},
        }

    # Label distribution
    label_counts = Counter(r["label"] for r in valid_rows)
    label_pct = {k: v / len(valid_rows) for k, v in label_counts.items()}

    # Score stats
    scores = [r["score"] for r in valid_rows]
    avg_score = sum(scores) / len(scores) if scores else 0.0

    # Quarantine reasons
    quarantine_reasons = Counter()
    for r in quarantined_rows:
        for reason in r.get("quarantine_reasons", []):
            quarantine_reasons[reason] += 1

    return {
        "total_input": total,
        "valid": len(valid_rows),
        "quarantined": len(quarantined_rows),
        "quarantine_rate": round(len(quarantined_rows) / total, 4),
        "label_distribution": label_counts,
        "label_percentage": {k: round(v, 4) for k, v in label_pct.items()},
        "avg_confidence": round(avg_score, 4),
        "min_confidence": round(min(scores), 4) if scores else 0.0,
        "max_confidence": round(max(scores), 4) if scores else 0.0,
        "quarantine_reasons": dict(quarantine_reasons),
    }


def compute_label_consistency(
    valid_rows: list[dict[str, Any]],
    rating_field: str = "rating",
) -> dict[str, Any]:
    """Tính consistency giữa sentiment label và rating (1-5).

    Chỉ áp dụng cho source có rating (TripAdvisor).
    Mapping: 1-2 -> negative, 3 -> neutral, 4-5 -> positive
    """
    if not valid_rows:
        return {"consistency_rate": 0.0, "matched": 0, "total_with_rating": 0}

    def rating_to_sentiment(rating: float | None) -> str | None:
        if rating is None:
            return None
        if rating <= 2:
            return "negative"
        elif rating == 3:
            return "neutral"
        else:
            return "positive"

    matched = 0
    total_with_rating = 0

    for row in valid_rows:
        rating = row.get(rating_field)
        if rating is not None:
            total_with_rating += 1
            expected = rating_to_sentiment(float(rating))
            if expected and row["label"] == expected:
                matched += 1

    return {
        "consistency_rate": round(matched / total_with_rating, 4) if total_with_rating > 0 else 0.0,
        "matched": matched,
        "total_with_rating": total_with_rating,
    }


__all__ = ["compute_metrics", "compute_label_consistency"]