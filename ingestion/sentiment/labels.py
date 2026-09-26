"""Nhãn cảm xúc chuẩn cho pipeline.

Quy chuẩn:
- 3 nhãn gốc từ ViSoBERT checkpoint: NEG, NEU, POS
- Map sang dạng chữ thường: negative, neutral, positive (dùng cho downstream)
- Provider stub cho test offline không cần tải model.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any


class SentimentLabel(str, Enum):
    """Nhãn cảm xúc chuẩn (lowercase)."""

    NEGATIVE = "negative"
    NEUTRAL = "neutral"
    POSITIVE = "positive"


# Map từ nhãn model (thường là NEG/NEU/POS) sang nhãn chuẩn
MODEL_LABEL_TO_STANDARD: dict[str, SentimentLabel] = {
    "NEG": SentimentLabel.NEGATIVE,
    "NEGATIVE": SentimentLabel.NEGATIVE,
    "NEU": SentimentLabel.NEUTRAL,
    "NEUTRAL": SentimentLabel.NEUTRAL,
    "POS": SentimentLabel.POSITIVE,
    "POSITIVE": SentimentLabel.POSITIVE,
}

STANDARD_TO_INT = {
    SentimentLabel.NEGATIVE: 0,
    SentimentLabel.NEUTRAL: 1,
    SentimentLabel.POSITIVE: 2,
}

INT_TO_STANDARD = {v: k for k, v in STANDARD_TO_INT.items()}


@dataclass(frozen=True)
class LabelProvider:
    """Interface provider nhãn - cho phép stub khi test."""

    id2label: dict[int, str]

    def to_standard(self, model_label: str) -> SentimentLabel:
        """Chuyển nhãn model (có thể là NEG/NEU/POS hoặc negative/neutral/positive) sang chuẩn."""
        upper = model_label.upper()
        return MODEL_LABEL_TO_STANDARD.get(upper, MODEL_LABEL_TO_STANDARD.get(model_label, SentimentLabel.NEUTRAL))

    def to_int(self, label: SentimentLabel) -> int:
        return STANDARD_TO_INT[label]

    def from_int(self, idx: int) -> SentimentLabel:
        return INT_TO_STANDARD.get(idx, SentimentLabel.NEUTRAL)


def build_label_provider(model_config: dict[str, Any] | None = None) -> LabelProvider:
    """Tạo provider từ config model (id2label từ checkpoint)."""
    if model_config and "id2label" in model_config:
        id2label = {int(k): v for k, v in model_config["id2label"].items()}
    else:
        # Default cho 5CD-AI/Vietnamese-Sentiment-visobert (3 class NEG/NEU/POS)
        id2label = {0: "NEG", 1: "NEU", 2: "POS"}
    return LabelProvider(id2label=id2label)


def build_stub_provider() -> LabelProvider:
    """Provider giả cho test/CI offline."""
    return LabelProvider(id2label={0: "NEG", 1: "NEU", 2: "POS"})


__all__ = [
    "SentimentLabel",
    "MODEL_LABEL_TO_STANDARD",
    "STANDARD_TO_INT",
    "INT_TO_STANDARD",
    "LabelProvider",
    "build_label_provider",
    "build_stub_provider",
]