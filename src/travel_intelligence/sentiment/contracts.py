"""Proposed durable Silver sentiment and aspect-sentiment contracts.

The DDL in this module is documentation/contract data only.  TASK-004A does
not create or mutate Iceberg tables and does not execute ViSoBERT.
"""

from __future__ import annotations

import hashlib
from typing import Any

from travel_intelligence.silver.normalization import _id

REVIEW_SENTIMENT_TABLE = """
CREATE TABLE IF NOT EXISTS travel.silver.review_sentiment (
  review_sentiment_id STRING NOT NULL,
  review_id STRING NOT NULL,
  source_record_sk STRING NOT NULL,
  source_id STRING NOT NULL,
  batch_id STRING NOT NULL,
  source_row_index BIGINT NOT NULL,
  source_record_id STRING,
  place_id STRING,
  review_text_hash STRING NOT NULL,
  sentiment_label STRING NOT NULL,
  confidence DOUBLE NOT NULL,
  model_id STRING NOT NULL,
  model_fingerprint STRING NOT NULL,
  config_fingerprint STRING NOT NULL,
  processing_run_id STRING NOT NULL,
  source_release_id STRING NOT NULL,
  processed_at TIMESTAMP NOT NULL,
  quality_codes ARRAY<STRING> NOT NULL
) USING iceberg
TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='zstd')
"""

REVIEW_ASPECT_SENTIMENT_TABLE = """
CREATE TABLE IF NOT EXISTS travel.silver.review_aspect_sentiment (
  aspect_sentiment_id STRING NOT NULL,
  review_sentiment_id STRING NOT NULL,
  review_id STRING NOT NULL,
  source_record_sk STRING NOT NULL,
  source_id STRING NOT NULL,
  batch_id STRING NOT NULL,
  source_row_index BIGINT NOT NULL,
  segment_ordinal BIGINT NOT NULL,
  sentence_ordinal BIGINT,
  clause_ordinal BIGINT,
  segment_start_char BIGINT,
  segment_end_char BIGINT,
  segment_text STRING NOT NULL,
  segment_text_hash STRING NOT NULL,
  aspect STRING NOT NULL,
  matched_keyword STRING,
  segmentation_strategy STRING NOT NULL,
  negation_detected BOOLEAN NOT NULL,
  negation_metadata STRING,
  sentiment_label STRING NOT NULL,
  confidence DOUBLE NOT NULL,
  model_id STRING NOT NULL,
  model_fingerprint STRING NOT NULL,
  config_fingerprint STRING NOT NULL,
  processing_run_id STRING NOT NULL,
  source_release_id STRING NOT NULL,
  processed_at TIMESTAMP NOT NULL,
  quality_codes ARRAY<STRING> NOT NULL
) USING iceberg
TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='zstd')
"""

# This is a separate additive run ledger.  It does not alter the source
# normalization ledger and is the publication gate for the two enrichment
# tables, which cannot be committed atomically with one another.
REVIEW_ENRICHMENT_RUN_TABLE = """
CREATE TABLE IF NOT EXISTS travel.silver.review_enrichment_run (
  processing_run_id STRING NOT NULL,
  source_id STRING NOT NULL,
  source_release_id STRING NOT NULL,
  model_id STRING NOT NULL,
  model_fingerprint STRING NOT NULL,
  config_fingerprint STRING NOT NULL,
  input_fingerprint STRING NOT NULL,
  status STRING NOT NULL,
  expected_review_count BIGINT NOT NULL,
  expected_aspect_count BIGINT NOT NULL,
  actual_review_count BIGINT NOT NULL,
  actual_aspect_count BIGINT NOT NULL,
  review_snapshot_id STRING,
  aspect_snapshot_id STRING,
  started_at TIMESTAMP NOT NULL,
  finished_at TIMESTAMP,
  error_summary STRING
) USING iceberg
TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='zstd')
"""


def text_hash(text: str) -> str:
    return "sha256:" + hashlib.sha256(text.encode("utf-8")).hexdigest()


def review_sentiment_id(
    review_id: str,
    model_fingerprint: str,
    config_fingerprint: str,
    review_text_hash: str | None = None,
    source_record_sk: str | None = None,
    review_ordinal: int | None = None,
) -> str:
    return _id(
        "silver-review-sentiment-v1",
        review_id,
        review_text_hash or "",
        source_record_sk or "",
        review_ordinal if review_ordinal is not None else "",
        model_fingerprint,
        config_fingerprint,
    )


def aspect_sentiment_id(
    review_sentiment_id_value: str,
    segment_ordinal: int,
    aspect: str,
    matched_keyword: str | None,
    segment_hash: str,
    model_fingerprint: str,
    config_fingerprint: str,
) -> str:
    return _id(
        "silver-review-aspect-sentiment-v1",
        review_sentiment_id_value,
        segment_ordinal,
        aspect,
        matched_keyword or "",
        segment_hash,
        model_fingerprint,
        config_fingerprint,
    )


def build_review_sentiment_key(
    occurrence: dict[str, Any],
    *,
    model_fingerprint: str,
    config_fingerprint: str,
) -> str:
    "Build the replay-safe logical key without running classification."
    required = ("review_id", "source_record_sk", "batch_id", "source_row_index")
    missing = [key for key in required if key not in occurrence]
    if missing:
        raise ValueError("review occurrence missing: " + ", ".join(missing))
    return review_sentiment_id(
        str(occurrence["review_id"]),
        model_fingerprint,
        config_fingerprint,
        text_hash(str(occurrence["review_text"]))
        if occurrence.get("review_text") is not None
        else None,
        str(occurrence.get("source_record_sk"))
        if occurrence.get("source_record_sk") is not None
        else None,
        int(occurrence["review_ordinal"])
        if occurrence.get("review_ordinal") is not None
        else None,
    )


def build_aspect_sentiment_key(
    *,
    parent_review_sentiment_id: str,
    segment_ordinal: int,
    aspect: str,
    matched_keyword: str | None,
    segment_text: str,
    model_fingerprint: str,
    config_fingerprint: str,
) -> str:
    if not segment_text.strip():
        raise ValueError("aspect segment text must be non-empty")
    return aspect_sentiment_id(
        parent_review_sentiment_id,
        segment_ordinal,
        aspect,
        matched_keyword,
        text_hash(segment_text),
        model_fingerprint,
        config_fingerprint,
    )


__all__ = [
    "REVIEW_ASPECT_SENTIMENT_TABLE",
    "REVIEW_SENTIMENT_TABLE",
    "REVIEW_ENRICHMENT_RUN_TABLE",
    "aspect_sentiment_id",
    "build_aspect_sentiment_key",
    "build_review_sentiment_key",
    "review_sentiment_id",
    "text_hash",
]
