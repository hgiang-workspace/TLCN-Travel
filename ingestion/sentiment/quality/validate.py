"""Quality validation rules cho sentiment predictions + ABSA."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class QualityConfig:
    """Config kiểm định chất lượng."""
    min_score: float = 0.3           # Ngưỡng confidence tối thiểu
    max_score: float = 1.0
    min_sentence_len: int = 3        # Độ dài câu tối thiểu (ký tự)
    max_sentence_len: int = 512      # Độ dài câu tối đa
    required_labels: tuple[str, ...] = ("negative", "neutral", "positive")
    label_distribution_min_pct: float = 0.01  # Mỗi label ≥ 1%


@dataclass(frozen=True)
class ABSAQualityConfig:
    """Config kiểm định chất lượng cho ABSA."""
    min_aspect_coverage: float = 0.0  # Tỷ lệ câu có ít nhất 1 aspect (0-1)
    required_aspects: tuple[str, ...] = ()  # Aspects bắt buộc phải có (nếu rỗng thì không check)
    max_aspects_per_sentence: int = 10  # Giới hạn aspect/câu để tránh noise


QUARANTINE_REASONS = {
    "LOW_CONFIDENCE": "score < min_score",
    "HIGH_CONFIDENCE": "score > max_score (impossible)",
    "EMPTY_TEXT": "sentence_text rỗng",
    "TOO_SHORT": "sentence_text quá ngắn",
    "TOO_LONG": "sentence_text quá dài",
    "INVALID_LABEL": "label không hợp lệ",
    "MISSING_FIELDS": "thiếu trường bắt buộc",
}


ABSA_QUARANTINE_REASONS = {
    **QUARANTINE_REASONS,
    "NO_ASPECT": "không phát hiện aspect nào (gán other)",
    "TOO_MANY_ASPECTS": "quá nhiều aspect trong 1 câu",
    "INVALID_ASPECT": "aspect không hợp lệ",
    "MISSING_ABSA_FIELDS": "thiếu trường ABSA bắt buộc",
    "EMPTY_SEGMENT": "segment_text rỗng (đoạn đưa vào model)",
}


def validate_row(row: dict[str, Any], cfg: QualityConfig) -> tuple[bool, list[str]]:
    """Kiểm tra 1 row prediction.

    Returns:
        (is_valid, list_reasons)
    """
    reasons: list[str] = []

    # Required fields
    for field in ("sentence_id", "sentence_text", "sentence_hash", "label", "score", "label_id"):
        if field not in row or row[field] is None:
            reasons.append("MISSING_FIELDS")

    # Text checks
    text = row.get("sentence_text", "")
    if not text or not text.strip():
        reasons.append("EMPTY_TEXT")
    elif len(text) < cfg.min_sentence_len:
        reasons.append("TOO_SHORT")
    elif len(text) > cfg.max_sentence_len:
        reasons.append("TOO_LONG")

    # Label check
    label = row.get("label")
    if label not in cfg.required_labels:
        reasons.append("INVALID_LABEL")

    # Score check
    score = row.get("score")
    if score is not None:
        if score < cfg.min_score:
            reasons.append("LOW_CONFIDENCE")
        elif score > cfg.max_score:
            reasons.append("HIGH_CONFIDENCE")

    return len(reasons) == 0, reasons


def validate_absa_row(row: dict[str, Any], cfg: ABSAQualityConfig) -> tuple[bool, list[str]]:
    """Kiểm tra 1 row ABSA (S7 output)."""
    reasons: list[str] = []

    # Required ABSA fields
    required = ("record_id", "entity_id", "source", "sentence", "aspect", "sentiment", "confidence_score", "platform", "created_at")
    for field in required:
        if field not in row or row[field] is None:
            reasons.append("MISSING_ABSA_FIELDS")

    # Aspect check
    aspect = row.get("aspect")
    if not aspect:
        reasons.append("INVALID_ASPECT")

    # Sentiment check
    sentiment = row.get("sentiment")
    if sentiment not in ("negative", "neutral", "positive"):
        reasons.append("INVALID_LABEL")

    # Segment check (aspect-aware segmentation): chỉ kiểm khi field có mặt,
    # để tương thích ngược với row legacy không có segment_text.
    if "segment_text" in row:
        seg = row.get("segment_text")
        if seg is None or not str(seg).strip():
            reasons.append("EMPTY_SEGMENT")

    # Score check
    score = row.get("confidence_score")
    if score is not None:
        if score < 0.0 or score > 1.0:
            reasons.append("LOW_CONFIDENCE")

    return len(reasons) == 0, reasons


def validate_batch(rows: list[dict[str, Any]], cfg: QualityConfig | None = None) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Validate batch rows.

    Returns:
        (valid_rows, quarantined_rows)
    """
    cfg = cfg or QualityConfig()
    valid: list[dict[str, Any]] = []
    quarantined: list[dict[str, Any]] = []

    for row in rows:
        is_valid, reasons = validate_row(row, cfg)
        if is_valid:
            valid.append(row)
        else:
            q_row = row.copy()
            q_row["quarantine_reasons"] = reasons
            quarantined.append(q_row)

    return valid, quarantined


def validate_absa_batch(rows: list[dict[str, Any]], cfg: ABSAQualityConfig | None = None) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Validate batch ABSA rows (S7)."""
    cfg = cfg or ABSAQualityConfig()
    valid: list[dict[str, Any]] = []
    quarantined: list[dict[str, Any]] = []

    for row in rows:
        is_valid, reasons = validate_absa_row(row, cfg)
        if is_valid:
            valid.append(row)
        else:
            q_row = row.copy()
            q_row["quarantine_reasons"] = reasons
            quarantined.append(q_row)

    return valid, quarantined


def build_quality_config(cfg: dict[str, Any] | None = None) -> QualityConfig:
    """Tạo QualityConfig từ pipeline config."""
    if not cfg:
        return QualityConfig()
    q = cfg.get("quality", {})
    return QualityConfig(
        min_score=q.get("min_score", 0.3),
        max_score=q.get("max_score", 1.0),
        min_sentence_len=q.get("min_sentence_len", 3),
        max_sentence_len=q.get("max_sentence_len", 512),
        required_labels=tuple(q.get("required_labels", ["negative", "neutral", "positive"])),
        label_distribution_min_pct=q.get("label_distribution_min_pct", 0.01),
    )


def build_absa_quality_config(cfg: dict[str, Any] | None = None) -> ABSAQualityConfig:
    """Tạo ABSAQualityConfig từ pipeline config."""
    if not cfg:
        return ABSAQualityConfig()
    q = cfg.get("aspects", {}).get("quality", {})
    return ABSAQualityConfig(
        min_aspect_coverage=q.get("min_aspect_coverage", 0.0),
        required_aspects=tuple(q.get("required_aspects", [])),
        max_aspects_per_sentence=q.get("max_aspects_per_sentence", 10),
    )


__all__ = [
    "QualityConfig",
    "ABSAQualityConfig",
    "validate_row",
    "validate_absa_row",
    "validate_batch",
    "validate_absa_batch",
    "build_quality_config",
    "build_absa_quality_config",
    "QUARANTINE_REASONS",
    "ABSA_QUARANTINE_REASONS",
]