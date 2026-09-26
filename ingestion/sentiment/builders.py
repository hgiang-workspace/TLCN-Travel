"""Builder functions: dựng row cho từng tầng staging.

Pure Python, không phụ thuộc pyspark. Dùng cho cả local (jsonl) và Spark (mapInPandas).
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any

from ingestion.sentiment.adapters import Comment
from ingestion.sentiment.labels import SentimentLabel
from ingestion.sentiment.text import normalize_text, split_sentences


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def sentence_hash(text: str) -> str:
    """Hash chuẩn của câu (để dedupe + resume)."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


# ===== S1: raw comment -> normalized comment (staging) =====

def build_s1_row(
    comment: Comment,
    *,
    normalized_text: str,
    run_id: str,
) -> dict[str, Any]:
    """Row cho bảng staging comments (S1 output)."""
    return {
        "source": comment.source,
        "comment_id": comment.comment_id,
        "place_id": comment.place_id,
        "place_name": comment.place_name,
        "raw_text": comment.raw_text,
        "normalized_text": normalized_text,
        "rating": comment.rating,
        "collected_at": comment.collected_at,
        "meta": json.dumps(comment.meta, ensure_ascii=False),
        "run_id": run_id,
        "processed_at": utc_now_iso(),
    }


# ===== S2: normalized comment -> sentences (deduped) =====

def build_s2_rows(
    s1_row: dict[str, Any],
    *,
    sentences: list[str],
    text_cfg: dict[str, Any],
    run_id: str,
) -> list[dict[str, Any]]:
    """Tách normalized_text thành các câu, dedupe theo sentence_hash.

    Returns:
        List rows cho staging sentences (S2 output).
    """
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()

    for idx, sent in enumerate(sentences):
        h = sentence_hash(sent)
        if h in seen:
            continue
        seen.add(h)

        rows.append({
            "source": s1_row["source"],
            "comment_id": s1_row["comment_id"],
            "place_id": s1_row["place_id"],
            "place_name": s1_row["place_name"],
            "sentence_id": f"{s1_row['comment_id']}_s{idx}",
            "sentence_hash": h,
            "sentence_text": sent,
            "sentence_idx": idx,
            "rating": s1_row["rating"],
            "run_id": run_id,
            "processed_at": utc_now_iso(),
        })
    return rows


# ===== Helper: run S1 + S2 trên một batch comment (dùng cho local test) =====

def process_comments_to_sentences(
    comments: list[Comment],
    *,
    run_id: str,
    text_cfg: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Chạy S1 + S2 trên list Comment, trả về (s1_rows, s2_rows)."""
    s1_rows: list[dict[str, Any]] = []
    s2_rows: list[dict[str, Any]] = []

    for c in comments:
        norm = normalize_text(c.raw_text, text_cfg)
        s1 = build_s1_row(c, normalized_text=norm, run_id=run_id)
        s1_rows.append(s1)

        sentences = split_sentences(norm, text_cfg)
        s2_rows.extend(build_s2_rows(s1, sentences=sentences, text_cfg=text_cfg, run_id=run_id))

    return s1_rows, s2_rows


__all__ = [
    "build_s1_row",
    "build_s2_rows",
    "process_comments_to_sentences",
    "sentence_hash",
]