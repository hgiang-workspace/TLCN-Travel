"""ABSA Assembly: Gắn sentiment vào aspect.

Kết hợp aspect detection (S6) + sentiment inference (S3) để tạo ABSA output (S7).
Hỗ trợ 2 granularity:
- sentence-level: 1 sentiment cho cả câu, áp dụng cho tất cả aspect trong câu.
- clause-level: tách câu thành clauses, chạy sentiment cho từng clause, gán cho aspect trong clause.
"""

from __future__ import annotations

from typing import Any

from ingestion.sentiment.aspects.detect import deduplicate_aspects, detect_aspects
from ingestion.sentiment.aspects.dictionary import (
    build_aspect_keyword_index,
    load_aspect_keywords,
)
from ingestion.sentiment.aspects.segment import build_aspect_segments, segment_hash
from ingestion.sentiment.inference import predict_batch
from ingestion.sentiment.text import split_clauses


def _apply_negation(label: str, negated: bool) -> tuple[str, bool]:
    """Đảo nhãn nếu aspect bị phủ định.

    Returns:
        (label_mới, negation_applied)
    """
    if not negated:
        return label, False
    if label == "negative":
        return "positive", True
    if label == "positive":
        return "negative", True
    return label, False


def assemble_absa_sentence_level(
    s6_rows: list[dict[str, Any]],
    *,
    model_dir: str,
    batch_size: int = 32,
    max_length: int = 256,
    provider: str = "transformers",
) -> list[dict[str, Any]]:
    """Assemble ABSA ở granularity sentence-level.

    Mỗi S6 row đã có aspect + sentence_text. Ta gom unique sentences, chạy inference
    1 lần, sau đó gán sentiment cho từng aspect trong câu đó.

    Args:
        s6_rows: List rows từ S6 output (mỗi row = 1 aspect trong 1 sentence).
        model_dir: Thư mục model ViSoBERT.
        batch_size: Batch size cho inference.
        max_length: Max sequence length.
        provider: "transformers" | "stub".

    Returns:
        List rows S7 (ABSA_FIELDS).
    """
    if not s6_rows:
        return []

    # Gom unique sentences theo sentence_id
    sentence_map: dict[str, dict[str, Any]] = {}
    for row in s6_rows:
        sid = row["sentence_id"]
        if sid not in sentence_map:
            sentence_map[sid] = row

    # Chạy inference cho các unique sentences
    unique_sentences = list(sentence_map.values())
    texts = [r["sentence_text"] for r in unique_sentences]

    preds = predict_batch(
        texts,
        model_dir,
        batch_size=batch_size,
        max_length=max_length,
        provider=provider,
    )

    # Map sentence_id -> (label, score)
    sent_sentiment: dict[str, tuple[str, float]] = {}
    for sent_row, pred in zip(unique_sentences, preds):
        sent_sentiment[sent_row["sentence_id"]] = (pred["label"], pred["score"])

    # Build S7 rows
    from ingestion.sentiment.builders import build_s7_rows

    s7_rows: list[dict[str, Any]] = []
    for s6_row in s6_rows:
        sid = s6_row["sentence_id"]
        label, score = sent_sentiment.get(sid, ("neutral", 0.5))
        # Áp dụng negation modifier nếu có (đảo negative <-> positive)
        label, negation_applied = _apply_negation(label, bool(s6_row.get("aspect_negated")))
        s7_rows.extend(build_s7_rows(
            s6_row,
            sentiment_label=label,
            sentiment_score=score,
            model_version="visobert",
            clause_idx=0,
            segment_text=s6_row.get("segment_text") or s6_row.get("sentence_text"),
            segment_type="sentence",
            negation_applied=negation_applied,
            run_id=s6_row["run_id"],
        ))

    return s7_rows


def assemble_absa_clause_level(
    s6_rows: list[dict[str, Any]],
    *,
    model_dir: str,
    batch_size: int = 32,
    max_length: int = 256,
    provider: str = "transformers",
    text_cfg: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Assemble ABSA ở granularity clause-level.

    Với mỗi sentence:
    1. Tách thành clauses
    2. Chạy inference cho từng clause
    3. Gán aspect vào clause phù hợp (dựa trên vị trí keyword)
    4. Tạo S7 row cho mỗi (clause, aspect) pair.

    Args:
        s6_rows: List rows từ S6 output.
        model_dir: Thư mục model ViSoBERT.
        batch_size: Batch size cho inference.
        max_length: Max sequence length.
        provider: "transformers" | "stub".
        text_cfg: Config cho clause split (min_clause_len, use_clause_split).

    Returns:
        List rows S7 (ABSA_FIELDS).
    """
    if not s6_rows:
        return []

    from collections import defaultdict
    s6_by_sentence: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in s6_rows:
        s6_by_sentence[row["sentence_id"]].append(row)

    s7_rows: list[dict[str, Any]] = []

    for sentence_id, aspects in s6_by_sentence.items():
        sentence_text = aspects[0]["sentence_text"]
        clauses = split_clauses(sentence_text, text_cfg)

        if not clauses:
            s7_rows.extend(assemble_absa_sentence_level(
                aspects,
                model_dir=model_dir,
                batch_size=batch_size,
                max_length=max_length,
                provider=provider,
            ))
            continue

        clause_preds = predict_batch(
            clauses,
            model_dir,
            batch_size=batch_size,
            max_length=max_length,
            provider=provider,
        )

        aspect_to_clause: dict[str, int] = {}
        for aspect_row in aspects:
            keyword = aspect_row.get("aspect_keyword", "").lower()
            assigned = False
            for ci, clause in enumerate(clauses):
                if keyword and keyword in clause.lower():
                    aspect_to_clause[aspect_row["aspect"]] = ci
                    assigned = True
                    break
            if not assigned:
                aspect_to_clause[aspect_row["aspect"]] = 0

        from ingestion.sentiment.builders import build_s7_rows_from_clauses, build_s7_rows

        sentiments = [(p["label"], p["score"]) for p in clause_preds]

        for aspect_row in aspects:
            clause_idx = aspect_to_clause.get(aspect_row["aspect"], 0)
            label, score = sentiments[clause_idx] if clause_idx < len(sentiments) else ("neutral", 0.5)

            label, negation_applied = _apply_negation(label, bool(aspect_row.get("aspect_negated")))

            s7_rows.extend(build_s7_rows(
                aspect_row,
                sentiment_label=label,
                sentiment_score=score,
                model_version="visobert",
                clause_text=clauses[clause_idx],
                clause_idx=clause_idx,
                segment_text=clauses[clause_idx],
                segment_type="clause",
                negation_applied=negation_applied,
                run_id=aspect_row["run_id"],
            ))

    return s7_rows


def assemble_absa_segment_level(
    s6_rows: list[dict[str, Any]],
    *,
    model_dir: str,
    batch_size: int = 32,
    max_length: int = 256,
    provider: str = "transformers",
    text_cfg: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Assemble ABSA ở granularity aspect-segment-level (mặc định mới).

    Mỗi S6 row đã mang sẵn `segment_text`/`segment_hash`. Ta:
      1. Chuẩn hoá segment_text (fallback clause/sentence nếu rỗng).
      2. Dedupe theo `segment_hash` -> mỗi đoạn text duy nhất chỉ predict 1 lần.
      3. Gọi model trên từng đoạn -> cache nhãn theo hash.
      4. Gán nhãn (đã áp phủ định) cho từng (sentence, aspect) row.

    Args:
        s6_rows: List rows từ S6 output (mỗi row = 1 aspect segment).
        model_dir: Thư mục model ViSoBERT.
        batch_size: Batch size cho inference.
        max_length: Max sequence length.
        provider: "transformers" | "stub".
        text_cfg: (giữ để tương thích chữ ký, không dùng ở đây).

    Returns:
        List rows S7 (ABSA_FIELDS).
    """
    if not s6_rows:
        return []

    from ingestion.sentiment.builders import build_s7_rows

    # B1 + B2: chuẩn hoá segment_text và dedupe theo hash
    unique_segments: dict[str, str] = {}
    for row in s6_rows:
        seg_text = (
            row.get("segment_text")
            or row.get("clause_text")
            or row.get("sentence_text", "")
        )
        h = row.get("segment_hash") or segment_hash(seg_text)
        row["_segment_text"] = seg_text
        row["_segment_hash"] = h
        if h not in unique_segments:
            unique_segments[h] = seg_text

    # B3: predict trên các đoạn unique
    hashes = list(unique_segments.keys())
    texts = [unique_segments[h] for h in hashes]
    preds = predict_batch(
        texts,
        model_dir,
        batch_size=batch_size,
        max_length=max_length,
        provider=provider,
    )
    cache: dict[str, tuple[str, float]] = {
        h: (p["label"], p["score"]) for h, p in zip(hashes, preds)
    }

    # B4: gán nhãn cho từng row
    s7_rows: list[dict[str, Any]] = []
    for row in s6_rows:
        h = row["_segment_hash"]
        label, score = cache.get(h, ("neutral", 0.5))
        label, negation_applied = _apply_negation(label, bool(row.get("aspect_negated")))
        s7_rows.extend(build_s7_rows(
            row,
            sentiment_label=label,
            sentiment_score=score,
            model_version="visobert",
            # sentence giữ câu gốc (ML compat) — model input nằm ở segment_text
            clause_text=None,
            clause_idx=int(row.get("clause_idx", 0) or 0),
            segment_text=row["_segment_text"],
            segment_type=row.get("segment_type", "clause"),
            negation_applied=negation_applied,
            run_id=row["run_id"],
        ))

    return s7_rows


def assemble_absa(
    s6_rows: list[dict[str, Any]],
    *,
    model_dir: str,
    granularity: str = "sentence",
    batch_size: int = 32,
    max_length: int = 256,
    provider: str = "transformers",
    text_cfg: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Entry point cho ABSA assembly.

    Args:
        s6_rows: List rows từ S6 output.
        model_dir: Thư mục model ViSoBERT.
        granularity: "aspect_span" (mặc định mới) | "clause" | "sentence".
        batch_size: Batch size cho inference.
        max_length: Max sequence length.
        provider: "transformers" | "stub".
        text_cfg: Config cho text processing (dùng cho clause split).

    Returns:
        List rows S7 (ABSA_FIELDS).
    """
    if granularity in ("aspect_span", "segment", "aspect"):
        return assemble_absa_segment_level(
            s6_rows,
            model_dir=model_dir,
            batch_size=batch_size,
            max_length=max_length,
            provider=provider,
            text_cfg=text_cfg,
        )
    if granularity == "clause":
        return assemble_absa_clause_level(
            s6_rows,
            model_dir=model_dir,
            batch_size=batch_size,
            max_length=max_length,
            provider=provider,
            text_cfg=text_cfg,
        )
    return assemble_absa_sentence_level(
        s6_rows,
        model_dir=model_dir,
        batch_size=batch_size,
        max_length=max_length,
        provider=provider,
    )


def run_s6_aspect_detection(
    s2_rows: list[dict[str, Any]],
    *,
    run_id: str,
    keywords: dict[str, list[str]] | None = None,
    cfg: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Chạy S6: Aspect detection + aspect-aware segmentation trên S2 sentences.

    Mỗi câu được: tách clause -> detect aspect trong từng clause -> dựng
    aspect segment (đoạn chứa đúng 1 khía cạnh). Mỗi segment = 1 row S6.

    Args:
        s2_rows: List rows từ S2 output (staging sentences).
        run_id: Pipeline run_id.
        keywords: Aspect keywords dict (optional, sẽ load từ config nếu None).
        cfg: Config `aspects` (chứa granularity/min_clause_len/segment...).
             Có thể truyền cả full pipeline cfg (có key "aspects").

    Returns:
        List rows S6 (S6_FIELDS).
    """
    if keywords is None:
        keywords = load_aspect_keywords()
    keyword_index = build_aspect_keyword_index(keywords)

    aspect_cfg: dict[str, Any] = cfg or {}
    if "aspects" in aspect_cfg:
        aspect_cfg = aspect_cfg["aspects"] or {}

    from ingestion.sentiment.builders import build_s6_rows

    s6_rows: list[dict[str, Any]] = []
    for s2_row in s2_rows:
        segments = build_aspect_segments(
            s2_row["sentence_text"],
            cfg=aspect_cfg,
            keyword_index=keyword_index,
        )
        s6_rows.extend(build_s6_rows(s2_row, aspect_segments=segments, run_id=run_id))

    return s6_rows


__all__ = [
    "assemble_absa",
    "assemble_absa_sentence_level",
    "assemble_absa_clause_level",
    "assemble_absa_segment_level",
    "run_s6_aspect_detection",
]