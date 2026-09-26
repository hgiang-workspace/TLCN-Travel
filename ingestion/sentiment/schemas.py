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
}


def get_schema(name: str) -> SchemaInfo:
    if name not in SCHEMAS:
        raise ValueError(f"Unknown schema: {name}. Available: {list(SCHEMAS.keys())}")
    return SCHEMAS[name]


def to_spark_struct(schema_name: str):
    """Convert to pyspark StructType (lazy import)."""
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType, IntegerType, TimestampType,
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
    }

    fields = [StructField(f, type_map.get(f, StringType()), True) for f in schema.fields]
    return StructType(fields)


__all__ = [
    "S1_FIELDS", "S2_FIELDS", "S3_FIELDS", "S4_FIELDS", "QUARANTINE_FIELDS",
    "SchemaInfo", "SCHEMAS", "get_schema", "to_spark_struct",
]