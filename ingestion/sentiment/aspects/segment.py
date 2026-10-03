"""Aspect-Aware Segmentation.

Bến 1 câu thành các
"aspect segment" — đoạn văn bản liên tục nhỏ nhất nhưng chứa trọn ý kiến về
đúng MỘT khía cạnh. Mỗi segment sau đó được đưa RIÊNG vào ViSoBERT để dự đoán
sentiment thực tế cho khía cạnh đó.

"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any

from ingestion.sentiment.aspects.detect import deduplicate_aspects, detect_aspects
from ingestion.sentiment.aspects.dictionary import (
    build_aspect_keyword_index,
    load_aspect_keywords,
)
from ingestion.sentiment.text import split_clauses, window_segment

DEFAULT_SEGMENT_CFG: dict[str, Any] = {
    "strategy": "clause",       # "clause" | "window" | "hybrid"
    "window_before": 6,
    "window_after": 6,
    "include_keyword": True,
    "dedupe_by_hash": True,
    "prefer_longest_match": False,
    "max_aspects_per_sentence": 0,   # 0 = không giới hạn
}


@dataclass(frozen=True)
class AspectSegment:
    """Một đoạn văn bản gắn với đúng 1 khía cạnh (đơn vị input cho model)."""

    aspect: str
    keyword: str
    clause_idx: int
    clause_text: str
    segment_text: str
    segment_type: str          # "clause" | "window" | "fallback"
    char_start: int            # vị trí keyword (bắt đầu) trong câu gốc
    char_end: int              # vị trí keyword (kết thúc) trong câu gốc
    negated: bool


def segment_hash(text: str) -> str:
    """Hash chuẩn của segment (để dedupe/cache inference)."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def _seg_opt(cfg: dict[str, Any] | None, key: str, default: Any) -> Any:
    """Đọc option segmentation: ưu tiên cfg['segment'][key] > cfg[key] > default."""
    cfg = cfg or {}
    seg = cfg.get("segment") or {}
    if key in seg:
        return seg[key]
    if key in cfg:
        return cfg[key]
    return default


def build_aspect_segments(
    sentence_text: str,
    *,
    clauses: list[str] | None = None,
    cfg: dict[str, Any] | None = None,
    keyword_index: dict[str, str] | None = None,
) -> list[AspectSegment]:
    """Dựng các aspect segment cho 1 câu.

    Quy trình:
      B1. Tách clause (nếu chưa truyền sẵn `clauses`).
      B2. Detect aspect trong từng clause (keyword matching + phủ định).
      B3. Dedupe aspect trong từng clause (ưu tiên match không bị phủ định).
      B4. Dựng `segment_text` theo `strategy` (clause | window | hybrid).
      B5. Clause không khớp aspect nào -> aspect="other" (nếu keep_unmatched).

    Args:
        sentence_text: câu đã normalize (lowercase, bỏ URL/email/emoji).
        clauses: danh sách clause có sẵn (bỏ qua bước tách nếu truyền).
        cfg: config `aspects` (có thể chứa key `segment` lồng nhau).
        keyword_index: index keyword->aspect (tự build nếu None).

    Returns:
        List[AspectSegment] (rỗng nếu câu rỗng).
    """
    if not sentence_text or not isinstance(sentence_text, str):
        return []

    cfg = cfg or {}
    if keyword_index is None:
        keyword_index = build_aspect_keyword_index(load_aspect_keywords())

    keep_unmatched = bool(cfg.get("keep_unmatched", True))
    min_clause_len = int(cfg.get("min_clause_len", 3))
    min_segment_len = int(cfg.get("min_segment_len", 3))
    strategy = str(_seg_opt(cfg, "strategy", DEFAULT_SEGMENT_CFG["strategy"]))
    window_before = int(_seg_opt(cfg, "window_before", DEFAULT_SEGMENT_CFG["window_before"]))
    window_after = int(_seg_opt(cfg, "window_after", DEFAULT_SEGMENT_CFG["window_after"]))
    include_keyword = bool(_seg_opt(cfg, "include_keyword", DEFAULT_SEGMENT_CFG["include_keyword"]))
    dedupe_by_hash = bool(_seg_opt(cfg, "dedupe_by_hash", DEFAULT_SEGMENT_CFG["dedupe_by_hash"]))
    prefer_longest = bool(_seg_opt(cfg, "prefer_longest_match", DEFAULT_SEGMENT_CFG["prefer_longest_match"]))
    max_aspects = int(_seg_opt(cfg, "max_aspects_per_sentence", 0) or 0)

    if clauses is None:
        clause_cfg = {
            "use_clause_split": cfg.get("use_clause_split", True),
            "min_clause_len": min_clause_len,
        }
        clauses = split_clauses(sentence_text, clause_cfg)

    segments: list[AspectSegment] = []
    seen_keys: set[tuple[str, str]] = set()
    search_pos = 0

    for clause_idx, raw_clause in enumerate(clauses):
        clause_text = raw_clause.strip()
        if len(clause_text) < min_clause_len:
            continue

        # Định vị clause trong câu gốc để map offset.
        found = sentence_text.find(clause_text, search_pos)
        if found == -1:
            found = sentence_text.find(clause_text)
        clause_offset = found if found != -1 else 0
        if found != -1:
            search_pos = found + len(clause_text)

        matches = deduplicate_aspects(detect_aspects(
            clause_text,
            keyword_index=keyword_index,
            prefer_longest_match=prefer_longest,
        ))

        if not matches:
            if keep_unmatched:
                segments.append(AspectSegment(
                    aspect="other",
                    keyword="",
                    clause_idx=clause_idx,
                    clause_text=clause_text,
                    segment_text=clause_text,
                    segment_type="fallback",
                    char_start=clause_offset,
                    char_end=clause_offset + len(clause_text),
                    negated=False,
                ))
            continue

        multi = len({m.aspect for m in matches}) > 1
        for m in matches:
            use_window = strategy == "window" or (strategy == "hybrid" and multi)
            if use_window:
                seg_text, s0, s1 = window_segment(
                    clause_text, m.start, m.end,
                    before=window_before, after=window_after,
                    include_keyword=include_keyword,
                )
                seg_type = "window"
                char_start = clause_offset + s0
                char_end = clause_offset + s1
            else:
                seg_text = clause_text
                seg_type = "clause"
                char_start = clause_offset + m.start
                char_end = clause_offset + m.end

            if len(seg_text.strip()) < min_segment_len:
                seg_text = clause_text
                seg_type = "fallback"

            segments.append(AspectSegment(
                aspect=m.aspect,
                keyword=m.keyword,
                clause_idx=clause_idx,
                clause_text=clause_text,
                segment_text=seg_text,
                segment_type=seg_type,
                char_start=char_start,
                char_end=char_end,
                negated=m.negated,
            ))

    if max_aspects > 0 and len(segments) > max_aspects:
        segments = segments[:max_aspects]

    if dedupe_by_hash:
        unique: list[AspectSegment] = []
        for seg in segments:
            # Dedupe theo (aspect, segment_text): chỉ bỏ row trùng hệt,
            # KHÔNG bỏ 2 aspect khác nhau dù chung segment_text.
            key = (seg.aspect, segment_hash(seg.segment_text))
            if key in seen_keys:
                continue
            seen_keys.add(key)
            unique.append(seg)
        segments = unique

    return segments


def build_aspect_segments_batch(
    rows: list[dict[str, Any]],
    *,
    cfg: dict[str, Any] | None = None,
    keyword_index: dict[str, str] | None = None,
) -> dict[str, list[AspectSegment]]:
    """Batch version: nhận list row (có `sentence_text`) -> dict map id->segments."""
    if keyword_index is None:
        keyword_index = build_aspect_keyword_index(load_aspect_keywords())

    out: dict[str, list[AspectSegment]] = {}
    for row in rows:
        sid = row.get("sentence_id") or row.get("sentence_hash") or ""
        out[sid] = build_aspect_segments(
            row.get("sentence_text", ""),
            cfg=cfg,
            keyword_index=keyword_index,
        )
    return out


__all__ = [
    "AspectSegment",
    "segment_hash",
    "build_aspect_segments",
    "build_aspect_segments_batch",
    "DEFAULT_SEGMENT_CFG",
]