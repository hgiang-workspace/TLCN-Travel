"""Aspect Detection + Negation Handling.

- Phát hiện aspect trong câu/clause bằng keyword matching (từ điển).
- Xử lý phủ định: phát hiện từ phủ định gần aspect, đảo ngược sentiment.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from ingestion.sentiment.aspects.dictionary import (
    build_aspect_keyword_index,
    load_aspect_keywords,
)
from ingestion.sentiment.labels import AspectLabel


# ===== Negation patterns =====
# Các từ phủ định tiếng Việt (đã lowercase)
NEGATION_WORDS = {
    "không",
    "chẳng",
    "chả",
    "đâu",
    "đâu có",
    "hông",
    "hok",
    "ko",
    "k",
    "kh",
    "chưa",
    "chẳng bao giờ",
    "không bao giờ",
    "chẳng có",
    "không có",
    "không phải",
    "chẳng phải",
    "đối lập",
    "ngược lại",
    "trái ngược",
}

# Pattern để tìm từ phủ định (word boundary)
_NEGATION_PATTERN = re.compile(
    r"\b(?:" + "|".join(re.escape(w) for w in sorted(NEGATION_WORDS, key=len, reverse=True)) + r")\b",
    re.IGNORECASE | re.UNICODE
)

# Khoảng cách tối đa từ từ phủ định đến aspect/keyword (số từ)
NEGATION_WINDOW = 4


@dataclass(frozen=True)
class AspectMatch:
    """Kết quả khớp 1 aspect trong text."""
    aspect: str          # aspect name (snake_case, vd: "food")
    keyword: str         # keyword đã khớp
    start: int           # vị trí bắt đầu keyword trong text
    end: int             # vị trí kết thúc keyword trong text
    negated: bool        # có bị phủ định không


def detect_negation(text: str, keyword_start: int, keyword_end: int) -> bool:
    """Kiểm tra xem keyword có bị phủ định không.

    Tìm từ phủ định trong cửa sổ NEGATION_WINDOW từ trước keyword.

    Args:
        text: Văn bản đầy đủ (đã lowercase).
        keyword_start: Vị trí bắt đầu keyword.
        keyword_end: Vị trí kết thúc keyword.

    Returns:
        True nếu có từ phủ định trong cửa sổ.
    """
    # Lấy đoạn text trước keyword (tính theo word)
    before_text = text[:keyword_start]
    words = before_text.split()
    # Chỉ lấy NEGATION_WINDOW từ cuối cùng
    window_words = words[-NEGATION_WINDOW:] if len(words) >= NEGATION_WINDOW else words
    window_text = " ".join(window_words)

    # Tìm từ phủ định trong window
    return bool(_NEGATION_PATTERN.search(window_text))


def detect_aspects(
    text: str,
    *,
    keyword_index: dict[str, str] | None = None,
    keywords: dict[str, list[str]] | None = None,
    prefer_longest_match: bool = False,
) -> list[AspectMatch]:
    """Phát hiện các aspect trong text bằng keyword matching.

    Args:
        text: Văn bản đã normalize (lowercase).
        keyword_index: Dict keyword -> aspect (pre-built). Nếu None, tự build.
        keywords: Dict aspect -> list[keywords] (fallback nếu keyword_index None).
        prefer_longest_match: Nếu True, ưu tiên cụm keyword dài và loại các
            match ngắn nằm chồng lấn (vd "máy" bên trong "thang máy").

    Returns:
        Danh sách AspectMatch (có thể trùng aspect nếu khớp nhiều keyword).
    """
    if not text or not isinstance(text, str):
        return []

    if keyword_index is None:
        if keywords is None:
            keywords = load_aspect_keywords()
        keyword_index = build_aspect_keyword_index(keywords)

    matches: list[AspectMatch] = []
    text_lower = text.lower()

    items = list(keyword_index.items())
    if prefer_longest_match:
        items.sort(key=lambda kv: len(kv[0]), reverse=True)
    accepted_spans: list[tuple[int, int]] = []

    # Tìm tất cả keyword trong text
    for keyword, aspect in items:
        # Tìm tất cả vị trí xuất hiện của keyword
        start = 0
        kw_len = len(keyword)
        while True:
            idx = text_lower.find(keyword, start)
            if idx == -1:
                break
            # Kiểm tra word boundary (tránh match một phần từ)
            # Trước: space, đầu chuỗi, hoặc punctuation
            # Sau: space, cuối chuỗi, hoặc punctuation
            before_ok = idx == 0 or not text_lower[idx - 1].isalnum()
            after_idx = idx + kw_len
            after_ok = after_idx >= len(text_lower) or not text_lower[after_idx].isalnum()

            if before_ok and after_idx:
                overlap = prefer_longest_match and any(
                    not (after_idx <= s or idx >= e) for s, e in accepted_spans
                )
                if not overlap:
                    negated = detect_negation(text_lower, idx, after_idx)
                    matches.append(AspectMatch(
                        aspect=aspect,
                        keyword=keyword,
                        start=idx,
                        end=after_idx,
                        negated=negated,
                    ))
                    accepted_spans.append((idx, after_idx))
            start = idx + 1

    # Sắp xếp theo vị trí xuất hiện
    matches.sort(key=lambda m: m.start)
    return matches


def deduplicate_aspects(matches: list[AspectMatch]) -> list[AspectMatch]:
    """Loại bỏ aspect trùng lặp (giữ match đầu tiên của mỗi aspect).

    Ưu tiên match không bị phủ định.
    """
    seen: dict[str, AspectMatch] = {}
    for m in matches:
        if m.aspect not in seen:
            seen[m.aspect] = m
        elif m.negated != seen[m.aspect].negated:
            # Ưu tiên match không bị phủ định
            if not m.negated:
                seen[m.aspect] = m
    # Trả về theo thứ tự xuất hiện
    return sorted(seen.values(), key=lambda m: m.start)


def get_aspect_sentiment_modifier(negated: bool) -> int:
    """Trả về modifier cho sentiment dựa trên phủ định.

    Returns:
        -1 nếu bị phủ định (đảo ngược sentiment), 1 nếu bình thường.
        Dùng: final_score = base_score * modifier
    """
    return -1 if negated else 1


__all__ = [
    "AspectMatch",
    "detect_negation",
    "detect_aspects",
    "deduplicate_aspects",
    "get_aspect_sentiment_modifier",
    "NEGATION_WORDS",
    "NEGATION_WINDOW",
]