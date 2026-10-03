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


# ===== ABSA Metrics =====

def _sentence_key(row: dict[str, Any]) -> str:
    """Khóa câu duy nhất: (comment_id, sentence_hash) nếu có, fallback sentence_id.

    `sentence_id` = "<comment_id>_s<idx>" có thể trùng giữa các place khác nhau
    (comment_id = review.index) -> dùng composite để đo coverage/avg đúng.
    """
    cid = row.get("comment_id")
    shash = row.get("sentence_hash")
    if cid is not None and shash:
        return f"{cid}#{shash}"
    return str(row.get("sentence_id", ""))


def compute_absa_metrics(
    valid_rows: list[dict[str, Any]],
    quarantined_rows: list[dict[str, Any]],
    s2_total_sentences: int = 0,
) -> dict[str, Any]:
    """Tính metric chất lượng cho ABSA (S7 output).

    Args:
        valid_rows: Valid ABSA rows (S7).
        quarantined_rows: Quarantined ABSA rows.
        s2_total_sentences: Tổng số câu từ S2 (để tính aspect_coverage).

    Returns:
        Dict chứa các metric ABSA.
    """
    total = len(valid_rows) + len(quarantined_rows)
    if total == 0:
        return {
            "total_input": 0,
            "valid": 0,
            "quarantined": 0,
            "quarantine_rate": 0.0,
            "aspect_distribution": {},
            "aspect_percentage": {},
            "sentiment_distribution": {},
            "sentiment_percentage": {},
            "aspect_sentiment_crosstab": {},
            "avg_confidence": 0.0,
            "min_confidence": 0.0,
            "max_confidence": 0.0,
            "aspect_coverage": 0.0,
            "sentences_with_aspect": 0,
            "total_sentences": s2_total_sentences,
            "avg_aspects_per_sentence": 0.0,
            "segment_type_distribution": {},
            "avg_segments_per_sentence": 0.0,
            "unique_segment_ratio": 0.0,
            "quarantine_reasons": {},
        }

    # Aspect distribution
    aspect_counts = Counter(r["aspect"] for r in valid_rows)
    aspect_pct = {k: v / len(valid_rows) for k, v in aspect_counts.items()}

    # Sentiment distribution
    sentiment_counts = Counter(r["sentiment"] for r in valid_rows)
    sentiment_pct = {k: v / len(valid_rows) for k, v in sentiment_counts.items()}

    # Aspect x Sentiment crosstab
    crosstab = Counter()
    for r in valid_rows:
        crosstab[(r["aspect"], r["sentiment"])] += 1

    # Confidence stats
    scores = [r["confidence_score"] for r in valid_rows]
    avg_score = sum(scores) / len(scores) if scores else 0.0

    # Aspect coverage: số câu unique có ít nhất 1 aspect != "other" / total
    rows_with_aspect = [r for r in valid_rows if r.get("aspect") != "other"]
    sentences_with_aspect = len({_sentence_key(r) for r in rows_with_aspect})
    aspect_coverage = sentences_with_aspect / s2_total_sentences if s2_total_sentences > 0 else 0.0

    # Avg aspect thực (loại other) per sentence
    aspects_per_sentence = Counter(_sentence_key(r) for r in rows_with_aspect)
    avg_aspects = sum(aspects_per_sentence.values()) / len(aspects_per_sentence) if aspects_per_sentence else 0.0

    # Segment metrics (aspect-aware segmentation): đếm MỌI segment (gồm other)
    segment_type_counts = Counter(r.get("segment_type", "unknown") for r in valid_rows)
    seg_hashes = {r.get("segment_hash") for r in valid_rows if r.get("segment_hash")}
    unique_segment_ratio = (len(seg_hashes) / len(valid_rows)) if valid_rows else 0.0
    all_segments_per_sentence = Counter(_sentence_key(r) for r in valid_rows)
    avg_segments = (
        sum(all_segments_per_sentence.values()) / len(all_segments_per_sentence)
        if all_segments_per_sentence else 0.0
    )

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
        "aspect_distribution": dict(aspect_counts),
        "aspect_percentage": {k: round(v, 4) for k, v in aspect_pct.items()},
        "sentiment_distribution": dict(sentiment_counts),
        "sentiment_percentage": {k: round(v, 4) for k, v in sentiment_pct.items()},
        "aspect_sentiment_crosstab": {f"{a}|{s}": c for (a, s), c in crosstab.items()},
        "avg_confidence": round(avg_score, 4),
        "min_confidence": round(min(scores), 4) if scores else 0.0,
        "max_confidence": round(max(scores), 4) if scores else 0.0,
        "aspect_coverage": round(aspect_coverage, 4),
        "sentences_with_aspect": sentences_with_aspect,
        "total_sentences": s2_total_sentences,
        "avg_aspects_per_sentence": round(avg_aspects, 4),
        "segment_type_distribution": dict(segment_type_counts),
        "avg_segments_per_sentence": round(avg_segments, 4),
        "unique_segment_ratio": round(unique_segment_ratio, 4),
        "quarantine_reasons": dict(quarantine_reasons),
    }


__all__ = ["compute_metrics", "compute_label_consistency", "compute_absa_metrics"]