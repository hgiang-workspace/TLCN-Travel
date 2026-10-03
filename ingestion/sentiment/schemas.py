"""Schemas cho các tầng dữ liệu."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


# S1: Normalized comments
S1_FIELDS = [
    "source", "comment_id", "place_id", "place_name",
    "raw_text", "normalized_text", "rating", "collected_at",
    "meta", "run_id", "processed_at",
]

# S2: Sentences (deduped)
S2_FIELDS = [
    "source", "comment_id", "place_id", "place_name",
    "sentence_id", "sentence_hash", "sentence_text", "sentence_idx",
    "rating", "run_id", "processed_at",
]

# S3: Predictions raw
S3_FIELDS = S2_FIELDS + ["label", "score", "label_id", "model_version"]

# S4: Final output (valid)
S4_FIELDS = S3_FIELDS

# Quarantine
QUARANTINE_FIELDS = S3_FIELDS + ["quarantine_reasons"]


# ===== ABSA Schemas (S6, S7) =====

# S6: Aspect detection output (staging)
# Mỗi row = 1 (sentence, aspect) pair + aspect segment (đoạn đưa vào model)
S6_FIELDS = [
    "source", "comment_id", "place_id", "place_name",
    "sentence_id", "sentence_hash", "sentence_text", "sentence_idx",
    "aspect", "aspect_keyword", "aspect_negated",
    "clause_idx", "clause_text",
    "segment_text", "segment_hash", "segment_type",
    "aspect_char_start", "aspect_char_end", "negation_applied",
    "rating", "run_id", "processed_at",
]

# S7: ABSA final output (aspect + sentiment)
# Mỗi row = 1 (sentence, aspect, sentiment) triplet
# Ghi chú: clause_id = sentence_id nếu granularity=sentence, else {sentence_id}_c{clause_idx}
ABSA_FIELDS = [
    "record_id",          # unique: {source}_{comment_id}_{sentence_id}_{aspect}_{clause_idx}
    "entity_id",          # place_id
    "source",             # tripadvisor, tiktok, foody
    "sentence",           # sentence_text (hoặc clause_text)
    "aspect",             # food, service, ambiance, etc.
    "sentiment",          # negative, neutral, positive
    "confidence_score",   # score từ model (0-1)
    "platform",           # alias cho source (giữ compat ML)
    "created_at",         # processed_at (ISO)
    # Extended fields (optional, cho debugging/traceability)
    "comment_id",         # original comment_id
    "sentence_id",        # original sentence_id
    "sentence_hash",      # sentence_hash
    "sentence_idx",       # sentence_idx
    "clause_idx",         # 0 = sentence-level, >0 = clause index
    "segment_text",       # đoạn text thực sự đưa vào model (input dự đoán)
    "segment_hash",       # hash segment (dedupe/cache)
    "segment_type",       # "clause" | "window" | "sentence" | "fallback"
    "negation_applied",   # bool - đã đảo nhãn do phủ định
    "aspect_keyword",     # keyword matched
    "aspect_negated",     # bool
    "rating",             # original rating (nếu có)
    "model_version",      # visobert version
    "run_id",             # pipeline run_id
    "processed_at",       # timestamp xử lý
]

# Schema cho ML dataset (compat ml.absa_dataset)
ML_ABSA_FIELDS = [
    "record_id", "entity_id", "source", "sentence", "aspect",
    "sentiment", "confidence_score", "platform", "created_at",
]


@dataclass(frozen=True)
class SchemaInfo:
    name: str
    fields: list[str]


SCHEMAS = {
    "s1_comments": SchemaInfo("s1_comments", S1_FIELDS),
    "s2_sentences": SchemaInfo("s2_sentences", S2_FIELDS),
    "s3_predictions": SchemaInfo("s3_predictions", S3_FIELDS),
    "s4_final": SchemaInfo("s4_final", S4_FIELDS),
    "quarantine": SchemaInfo("quarantine", QUARANTINE_FIELDS),
    "s6_aspects": SchemaInfo("s6_aspects", S6_FIELDS),
    "s7_absa": SchemaInfo("s7_absa", ABSA_FIELDS),
    "ml_absa": SchemaInfo("ml_absa", ML_ABSA_FIELDS),
}


def get_schema(name: str) -> SchemaInfo:
    if name not in SCHEMAS:
        raise ValueError(f"Unknown schema: {name}. Available: {list(SCHEMAS.keys())}")
    return SCHEMAS[name]


def to_spark_struct(schema_name: str):
    """Convert to pyspark StructType (lazy import)."""
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType, IntegerType, BooleanType, TimestampType,
    )
    schema = get_schema(schema_name)

    type_map = {
        "source": StringType(),
        "comment_id": StringType(),
        "place_id": StringType(),
        "place_name": StringType(),
        "raw_text": StringType(),
        "normalized_text": StringType(),
        "rating": DoubleType(),
        "collected_at": StringType(),
        "meta": StringType(),
        "run_id": StringType(),
        "processed_at": StringType(),
        "sentence_id": StringType(),
        "sentence_hash": StringType(),
        "sentence_text": StringType(),
        "sentence_idx": IntegerType(),
        "label": StringType(),
        "score": DoubleType(),
        "label_id": IntegerType(),
        "model_version": StringType(),
        "quarantine_reasons": StringType(),
        # S6
        "aspect": StringType(),
        "aspect_keyword": StringType(),
        "aspect_negated": BooleanType(),
        "clause_text": StringType(),
        "segment_text": StringType(),
        "segment_hash": StringType(),
        "segment_type": StringType(),
        "aspect_char_start": IntegerType(),
        "aspect_char_end": IntegerType(),
        "negation_applied": BooleanType(),
        # S7 / ABSA
        "record_id": StringType(),
        "entity_id": StringType(),
        "sentiment": StringType(),
        "confidence_score": DoubleType(),
        "platform": StringType(),
        "created_at": StringType(),
        "clause_idx": IntegerType(),
    }

    fields = [StructField(f, type_map.get(f, StringType()), True) for f in schema.fields]
    return StructType(fields)


__all__ = [
    "S1_FIELDS", "S2_FIELDS", "S3_FIELDS", "S4_FIELDS", "QUARANTINE_FIELDS",
    "S6_FIELDS", "ABSA_FIELDS", "ML_ABSA_FIELDS",
    "SchemaInfo", "SCHEMAS", "get_schema", "to_spark_struct",
]