"""Clause Splitter for ABSA.

Tách câu thành các mệnh đề (clauses) dựa trên các từ nối/liên từ tiếng Việt.
Dùng để gán sentiment ở cấp độ clause (fine-grained) thay vì cả câu.
"""

from __future__ import annotations

import re
from typing import Any

# Các từ nối/liên từ phân chia mệnh đề trong tiếng Việt
# Được sắp xếp theo độ ưu tiên (từ dài trước, từ ngắn sau)
CLAUSE_CONNECTORS = [
    # Liên từ đối lập / chuyển hướng
    " nhưng ",
    " tuy nhiên ",
    " mà ",
    " lại ",
    " còn ",
    " song ",
    " nhiên ",
    # Liên từ nguyên nhân - kết quả
    " vì ",
    " do ",
    " nên ",
    " do đó ",
    " nên ",
    " dẫn đến ",
    " khiến ",
    " làm ",
    # Liên từ mục đích
    " để ",
    " cho ",
    " nhằm ",
    # Liên từ điều kiện
    " nếu ",
    " khi ",
    " khi nào ",
    " trường hợp ",
    # Liên từ nhượng bộ
    " dù ",
    " mặc dù ",
    " dù là ",
    " cho dù ",
    # Dấu câu phân chia mạnh
    " ; ",
    " : ",
    " — ",
    " – ",
]

# Compile regex pattern cho performance
# Escape các ký tự đặc biệt trong regex
_ESCAPED_CONNECTORS = [re.escape(c.strip()) for c in CLAUSE_CONNECTORS]
# Pattern: tìm các connector được bao quanh bởi space hoặc đầu/cuối chuỗi
_CLAUSE_SPLIT_PATTERN = re.compile(
    r"(?:\s+|^)(?:" + "|".join(_ESCAPED_CONNECTORS) + r")(?:\s+|$)",
    re.IGNORECASE | re.UNICODE
)


def split_clauses(text: str, cfg: dict[str, Any] | None = None) -> list[str]:
    """Tách văn bản thành các mệnh đề (clauses).

    Args:
        text: Văn bản đã normalize (lowercase, no URL/email).
        cfg: Config có thể chứa 'use_clause_split' (bool, default True),
             'min_clause_len' (int, default 5).

    Returns:
        Danh sách các clause, mỗi clause đã strip.
    """
    if not text or not isinstance(text, str):
        return []

    use_clause = cfg.get("use_clause_split", True) if cfg else True
    min_len = cfg.get("min_clause_len", 5) if cfg else 5

    if not use_clause:
        # Không tách clause, trả về cả câu
        return [text.strip()] if len(text.strip()) >= min_len else []

    # Tách theo pattern
    parts = _CLAUSE_SPLIT_PATTERN.split(text)

    # Lọc và clean
    clauses = []
    for part in parts:
        stripped = part.strip()
        if not stripped:
            continue
        # Strip trailing punctuation
        cleaned = stripped.rstrip(".!?…;:,")
        if len(cleaned) >= min_len:
            clauses.append(cleaned)

    # Nếu không tách được clause nào (không có connector), trả về cả câu
    # Chỉ fallback nếu có connector thực sự (parts > 1)
    if not clauses:
        if len(parts) > 1 and len(text.strip()) >= min_len:
            # Có connector nhưng các clause đều quá ngắn - trả về rỗng
            pass
        elif len(text.strip()) >= min_len:
            clauses = [text.strip().rstrip(".!?…;:,")]

    return clauses


def split_clauses_batch(texts: list[str], cfg: dict[str, Any] | None = None) -> list[list[str]]:
    """Batch version của split_clauses."""
    return [split_clauses(t, cfg) for t in texts]


def window_segment(
    text: str,
    char_start: int,
    char_end: int,
    before: int = 6,
    after: int = 6,
    include_keyword: bool = True,
) -> tuple[str, int, int]:
    """Trích cửa sổ token quanh keyword [char_start, char_end].

    Dùng cho aspect-aware segmentation (strategy="window"/"hybrid") khi clause
    quá dài hoặc chứa nhiều aspect cùng lúc.

    Args:
        text: đoạn text chứa keyword (thường là 1 clause đã normalize).
        char_start: vị trí bắt đầu keyword trong `text`.
        char_end: vị trí kết thúc keyword trong `text`.
        before: số token lấy trước keyword.
        after: số token lấy sau keyword.
        include_keyword: nếu False, cắt bỏ chính keyword khỏi segment.

    Returns:
        (segment_text, seg_start, seg_end) - segment đã strip và offset của nó
        trong `text` (dùng để map ngược lên câu gốc).
    """
    if not text:
        return "", 0, 0

    tokens = list(re.finditer(r"\S+", text))
    if not tokens:
        return "", 0, 0

    # Tìm token chứa char_start (fallback token đầu/cuối)
    kw_token_idx = 0
    for i, tok in enumerate(tokens):
        if tok.start() <= char_start < tok.end():
            kw_token_idx = i
            break
    else:
        # char_start nằm ngoài (vd =0 hoặc cuối) -> chọn token gần nhất
        if char_start <= 0:
            kw_token_idx = 0
        else:
            kw_token_idx = len(tokens) - 1

    lo = max(0, kw_token_idx - max(0, before))
    hi = min(len(tokens), kw_token_idx + max(0, after) + 1)

    seg_start = tokens[lo].start()
    seg_end = tokens[hi - 1].end()

    # Nếu không muốn giữ keyword: cắt bỏ token trùng keyword (chỉ khi keyword
    # nằm gọn trong 1 token) để segment chỉ còn ngữ cảnh.
    if not include_keyword and tokens[kw_token_idx].start() >= char_start and tokens[kw_token_idx].end() <= char_end:
        left = text[seg_start:tokens[kw_token_idx].start()].rstrip()
        right = text[tokens[kw_token_idx].end():seg_end].lstrip()
        seg = (left + " " + right).strip() if right else left
    else:
        seg = text[seg_start:seg_end].strip()

    return seg, seg_start, seg_end


def window_segment_batch(
    texts: list[str],
    spans: list[tuple[int, int]],
    before: int = 6,
    after: int = 6,
    include_keyword: bool = True,
) -> list[tuple[str, int, int]]:
    """Batch version của window_segment."""
    return [
        window_segment(t, s, e, before=before, after=after, include_keyword=include_keyword)
        for t, (s, e) in zip(texts, spans)
    ]


__all__ = ["split_clauses", "split_clauses_batch", "window_segment", "window_segment_batch"]