"""Quality validation rules cho sentiment predictions."""

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


QUARANTINE_REASONS = {
    "LOW_CONFIDENCE": "score < min_score",
    "HIGH_CONFIDENCE": "score > max_score (impossible)",
    "EMPTY_TEXT": "sentence_text rỗng",
    "TOO_SHORT": "sentence_text quá ngắn",
    "TOO_LONG": "sentence_text quá dài",
    "INVALID_LABEL": "label không hợp lệ",
    "MISSING_FIELDS": "thiếu trường bắt buộc",
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


__all__ = ["QualityConfig", "validate_row", "validate_batch", "build_quality_config", "QUARANTINE_REASONS"]