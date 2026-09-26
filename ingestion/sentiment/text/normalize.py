"""Chuẩn hoá văn bản tiếng Việt cho sentiment."""

from __future__ import annotations

import re
import unicodedata
from typing import Any

# Regex compile once
_RE_MULTI_SPACE = re.compile(r"\s+")
_RE_URL = re.compile(r"https?://\S+|www\.\S+")
_RE_EMAIL = re.compile(r"\S+@\S+\.\S+")
_RE_EMOJI = re.compile(
    "["
    "\U0001F600-\U0001F64F"  # emoticons
    "\U0001F300-\U0001F5FF"  # symbols & pictographs
    "\U0001F680-\U0001F6FF"  # transport & map
    "\U0001F1E0-\U0001F1FF"  # flags
    "\U00002700-\U000027BF"  # dingbats
    "\U00002600-\U000026FF"  # misc symbols
    "]+",
    flags=re.UNICODE,
)


def normalize_text(text: str, cfg: dict[str, Any] | None = None) -> str:
    """Chuẩn hoá text: NFC, bỏ URL/email, gộp whitespace, giữ dấu câu."""
    if not text or not isinstance(text, str):
        return ""

    # 1. Unicode normalize
    text = unicodedata.normalize("NFC", text)

    # 2. Lowercase (tùy config, mặc định True cho ViSoBERT)
    if cfg is None or cfg.get("lowercase", True):
        text = text.lower()

    # 3. Bỏ URL, email
    text = _RE_URL.sub(" ", text)
    text = _RE_EMAIL.sub(" ", text)

    # 4. Xử lý emoji: thay bằng space (giữ vị trí từ)
    text = _RE_EMOJI.sub(" ", text)

    # 5. Gộp whitespace
    text = _RE_MULTI_SPACE.sub(" ", text).strip()

    return text


def normalize_batch(texts: list[str], cfg: dict[str, Any] | None = None) -> list[str]:
    return [normalize_text(t, cfg) for t in texts]


__all__ = ["normalize_text", "normalize_batch"]