"""Tách câu tiếng Việt.

Sử dụng underthesea nếu có, fallback regex đơn giản.
Không import heavy ở module level để chạy được offline/test.
"""

from __future__ import annotations

import re
from typing import Any

# Regex fallback: tách theo . ! ? ... + space
# Vietnamese: không phải lúc nào cũng viết hoa đầu câu
_FALLBACK_SPLIT = re.compile(r"(?<=[.!?…])\s+")


def _split_fallback(text: str) -> list[str]:
    """Fallback đơn giản: tách theo dấu câu + space."""
    if not text:
        return []
    parts = _FALLBACK_SPLIT.split(text.strip())
    # Lọc câu quá ngắn (< 3 ký tự) và câu rỗng, strip trailing punctuation
    sentences = []
    for p in parts:
        stripped = p.strip()
        if not stripped:
            continue
        # Strip trailing punctuation first, then check length
        cleaned = stripped.rstrip(".!?…")
        if len(cleaned) >= 3:
            sentences.append(cleaned)
    return sentences


def split_sentences(text: str, cfg: dict[str, Any] | None = None) -> list[str]:
    """Tách văn bản thành danh sách câu.

    Args:
        text: văn bản đã normalize
        cfg: config có key 'use_underthesea' (bool, default True nếu cài được)

    Returns:
        Danh sách câu, mỗi câu đã strip.
    """
    if not text or not isinstance(text, str):
        return []

    # Thử underthesea
    use_ut = cfg.get("use_underthesea", True) if cfg else True
    if use_ut:
        try:
            from underthesea import sent_tokenize  # type: ignore
            sentences = sent_tokenize(text)
            if sentences:
                return [s.strip() for s in sentences if len(s.strip()) >= 3]
        except Exception:
            pass  # fallback

    return _split_fallback(text)


def split_sentences_batch(texts: list[str], cfg: dict[str, Any] | None = None) -> list[list[str]]:
    return [split_sentences(t, cfg) for t in texts]


__all__ = ["split_sentences", "split_sentences_batch"]