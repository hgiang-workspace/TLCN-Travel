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
from ingestion.sentiment.labels import AspectLabel, SentimentLabel
from ingestion.sentiment.text import normalize_text, split_sentences, split_clauses
from ingestion.sentiment.aspects import detect_aspects, deduplicate_aspects, build_aspect_keyword_index


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def sentence_hash(text: str) -> str:
    """Hash chuẩn của câu (để dedupe + resume)."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def segment_hash(text: str) -> str:
    """Hash chuẩn của aspect segment (để dedupe/cache inference)."""
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


# ===== S6: sentence -> aspects (staging) =====

def build_s6_rows(
    s2_row: dict[str, Any],
    *,
    aspect_segments: list[Any],  # list[AspectSegment]
    run_id: str,
) -> list[dict[str, Any]]:
    """Tạo rows S6 từ sentence + aspect segments.

    Mỗi aspect segment (đoạn văn bản gắn 1 khía cạnh) tạo 1 row S6.
    Row mang sẵn `segment_text`/`segment_hash` để S7 chỉ việc predict.
    """
    rows: list[dict[str, Any]] = []
    for seg in aspect_segments:
        seg_text = getattr(seg, "segment_text", "") or s2_row["sentence_text"]
        rows.append({
            "source": s2_row["source"],
            "comment_id": s2_row["comment_id"],
            "place_id": s2_row["place_id"],
            "place_name": s2_row["place_name"],
            "sentence_id": s2_row["sentence_id"],
            "sentence_hash": s2_row["sentence_hash"],
            "sentence_text": s2_row["sentence_text"],
            "sentence_idx": s2_row["sentence_idx"],
            "aspect": seg.aspect,
            "aspect_keyword": seg.keyword,
            "aspect_negated": seg.negated,
            "clause_idx": getattr(seg, "clause_idx", 0),
            "clause_text": getattr(seg, "clause_text", ""),
            "segment_text": seg_text,
            "segment_hash": segment_hash(seg_text),
            "segment_type": getattr(seg, "segment_type", "clause"),
            "aspect_char_start": getattr(seg, "char_start", 0),
            "aspect_char_end": getattr(seg, "char_end", 0),
            "negation_applied": False,
            "rating": s2_row.get("rating"),
            "run_id": run_id,
            "processed_at": utc_now_iso(),
        })
    return rows


# ===== S7: ABSA final output =====

def build_absa_record_id(
    source: str,
    comment_id: str,
    sentence_id: str,
    aspect: str,
    clause_idx: int,
) -> str:
    """Tạo record_id unique cho ABSA row."""
    return f"{source}_{comment_id}_{sentence_id}_{aspect}_c{clause_idx}"


def build_s7_rows(
    s6_row: dict[str, Any],
    *,
    sentiment_label: str,
    sentiment_score: float,
    model_version: str,
    clause_text: str | None = None,
    clause_idx: int = 0,
    segment_text: str | None = None,
    segment_type: str | None = None,
    negation_applied: bool = False,
    run_id: str,
) -> list[dict[str, Any]]:
    """Tạo rows S7 (ABSA final) từ S6 row + sentiment prediction.

    Args:
        s6_row: Row từ S6 output.
        sentiment_label: negative/neutral/positive.
        sentiment_score: Confidence score (0-1).
        model_version: Version của model sentiment.
        clause_text: Text của clause (nếu clause-level). Nếu None, dùng sentence_text.
        clause_idx: 0 = sentence-level, >0 = clause index.
        segment_text: Đoạn text thực sự đưa vào model (input dự đoán).
        segment_type: "clause" | "window" | "sentence" | "fallback".
        negation_applied: Đã đảo nhãn do phủ định chưa.
        run_id: Pipeline run_id.

    Returns:
        List chứa 1 row ABSA (có thể mở rộng để multi-clause sau).
    """
    text = clause_text if clause_text is not None else s6_row["sentence_text"]
    seg_text = segment_text if segment_text is not None else text
    seg_type = segment_type if segment_type is not None else s6_row.get("segment_type", "sentence")
    record_id = build_absa_record_id(
        s6_row["source"],
        s6_row["comment_id"],
        s6_row["sentence_id"],
        s6_row["aspect"],
        clause_idx,
    )

    row = {
        "record_id": record_id,
        "entity_id": s6_row["place_id"],
        "source": s6_row["source"],
        "sentence": text,
        "aspect": s6_row["aspect"],
        "sentiment": sentiment_label,
        "confidence_score": sentiment_score,
        "platform": s6_row["source"],  # alias for ML compat
        "created_at": utc_now_iso(),
        # Extended fields
        "comment_id": s6_row["comment_id"],
        "sentence_id": s6_row["sentence_id"],
        "sentence_hash": s6_row["sentence_hash"],
        "sentence_idx": s6_row["sentence_idx"],
        "clause_idx": clause_idx,
        "segment_text": seg_text,
        "segment_hash": segment_hash(seg_text),
        "segment_type": seg_type,
        "aspect_keyword": s6_row["aspect_keyword"],
        "aspect_negated": s6_row["aspect_negated"],
        "negation_applied": negation_applied,
        "rating": s6_row.get("rating"),
        "model_version": model_version,
        "run_id": run_id,
        "processed_at": utc_now_iso(),
    }
    return [row]


def build_s7_rows_from_clauses(
    s6_row: dict[str, Any],
    *,
    clauses: list[str],
    sentiments: list[tuple[str, float]],  # list of (label, score)
    model_version: str,
    run_id: str,
) -> list[dict[str, Any]]:
    """Tạo rows S7 từ S6 row + nhiều clause sentiments.

    Args:
        s6_row: Row từ S6 output.
        clauses: Danh sách clause texts.
        sentiments: Danh sách (label, score) cho từng clause.
        model_version: Version của model sentiment.
        run_id: Pipeline run_id.

    Returns:
        List rows ABSA (mỗi clause 1 row).
    """
    rows: list[dict[str, Any]] = []
    for clause_idx, (clause_text, (label, score)) in enumerate(zip(clauses, sentiments)):
        rows.extend(build_s7_rows(
            s6_row,
            sentiment_label=label,
            sentiment_score=score,
            model_version=model_version,
            clause_text=clause_text,
            clause_idx=clause_idx,
            run_id=run_id,
        ))
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
    "build_s6_rows",
    "build_s7_rows",
    "build_s7_rows_from_clauses",
    "build_absa_record_id",
    "process_comments_to_sentences",
    "sentence_hash",
    "segment_hash",
]