"""Replay-safe MiniLM embedding and Qdrant publication primitives for T04-D."""

from __future__ import annotations

import hashlib
import json
import math
import time
import uuid
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol

from travel_intelligence.common.configuration import configuration_fingerprint
from travel_intelligence.qdrant.provisioning import QdrantProvisioningError


class IndexingError(RuntimeError):
    """Raised when an embedding or publication invariant is violated."""


def sha256_text(value: str) -> str:
    return "sha256:" + hashlib.sha256(value.encode("utf-8")).hexdigest()


def generation_id(
    source_release_id: str,
    enrichment_run_id: str,
    model_fingerprint: str,
    config_fingerprint: str,
) -> str:
    raw = "|".join(
        (source_release_id, enrichment_run_id, model_fingerprint, config_fingerprint)
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def point_id(generation: str, kind: str, identity: str, chunk_index: int) -> str:
    value = hashlib.sha256(
        f"t04d|{generation}|{kind}|{identity}|{chunk_index}".encode("utf-8")
    ).hexdigest()[:32]
    return str(uuid.UUID(value))


def validate_vector(vector: Sequence[float], dimension: int) -> list[float]:
    if len(vector) != dimension:
        raise IndexingError(
            f"embedding dimension {len(vector)} != configured {dimension}"
        )
    result = [float(value) for value in vector]
    if any(not math.isfinite(value) for value in result):
        raise IndexingError("embedding contains a non-finite value")
    return result


@dataclass(frozen=True)
class TextChunk:
    index: int
    count: int
    text: str
    text_hash: str


class MiniLMEmbedder:
    """Load the configured SentenceTransformer and perform token-aware encoding."""

    def __init__(self, config: Mapping[str, Any], config_fingerprint: str, root: Any):
        model_config = config["model"]
        vector_config = config["vector"]
        inference = config["inference"]
        self.model_id = str(model_config["id"])
        self.revision = str(model_config["revision"])
        self.dimension = int(vector_config["dimension"])
        self.normalize = bool(vector_config.get("normalize", False))
        self.batch_size = int(inference["batch_size"])
        self.max_seq_length = int(inference["max_seq_length"])
        self.config_fingerprint = config_fingerprint
        self.model_fingerprint = sha256_text(
            json.dumps(
                {"id": self.model_id, "revision": self.revision},
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        local_path = root / str(model_config["local_artifact_path"])
        from sentence_transformers import SentenceTransformer

        source = str(local_path) if local_path.exists() else self.model_id
        kwargs: dict[str, Any] = {"device": self._device(inference.get("device"))}
        if source == self.model_id:
            kwargs["revision"] = self.revision
        self.model = SentenceTransformer(source, **kwargs)
        self.model.max_seq_length = self.max_seq_length
        tokenizer = self.model.tokenizer
        if tokenizer is None:
            raise IndexingError("MiniLM tokenizer is unavailable")
        self.tokenizer = tokenizer
        actual_dimension = self.model.get_sentence_embedding_dimension()
        if actual_dimension != self.dimension:
            raise IndexingError(
                f"MiniLM dimension {actual_dimension} != configured {self.dimension}"
            )

    @staticmethod
    def _device(value: Any) -> str:
        if value in (None, "auto"):
            try:
                import torch

                return "cuda" if torch.cuda.is_available() else "cpu"
            except ImportError:
                return "cpu"
        return str(value)

    def chunks(self, text: str) -> tuple[TextChunk, ...]:
        if not isinstance(text, str) or not text.strip():
            raise IndexingError("cannot embed empty or invalid text")
        token_ids = self.tokenizer.encode(text, add_special_tokens=False)
        special = self.tokenizer.num_special_tokens_to_add(pair=False)
        budget = self.max_seq_length - special
        if budget <= 0:
            raise IndexingError("MiniLM token budget is invalid")
        chunks: list[str] = []
        for start in range(0, len(token_ids), budget):
            decoded = self.tokenizer.decode(
                token_ids[start : start + budget],
                skip_special_tokens=True,
                clean_up_tokenization_spaces=False,
            ).strip()
            if not decoded:
                raise IndexingError("token-aware chunk decoded to empty text")
            chunks.append(decoded)
        count = len(chunks)
        return tuple(
            TextChunk(i, count, value, sha256_text(value))
            for i, value in enumerate(chunks)
        )

    def encode(self, chunks: Sequence[TextChunk]) -> tuple[list[float], ...]:
        texts = [chunk.text for chunk in chunks]
        vectors = self.model.encode(
            texts,
            batch_size=self.batch_size,
            normalize_embeddings=self.normalize,
            convert_to_numpy=True,
            show_progress_bar=False,
        )
        return tuple(
            validate_vector(vector.tolist(), self.dimension) for vector in vectors
        )


def _finite_confidence(value: Any) -> float:
    result = float(value)
    if not math.isfinite(result):
        raise IndexingError("sentiment confidence is not finite")
    return result


def build_review_points(
    rows: Iterable[Mapping[str, Any]],
    embedder: MiniLMEmbedder,
    *,
    generation: str,
) -> tuple[tuple[str, list[float], dict[str, Any]], ...]:
    points = []
    for row in rows:
        text = row.get("review_text")
        chunks = embedder.chunks(text)
        vectors = embedder.encode(chunks)
        identity = str(row["review_sentiment_id"])
        for chunk, vector in zip(chunks, vectors, strict=True):
            payload = {
                "review_sentiment_id": identity,
                "review_id": str(row["review_id"]),
                "chunk_index": chunk.index,
                "chunk_count": chunk.count,
                "chunk_text": chunk.text,
                "chunk_text_hash": chunk.text_hash,
                "text_hash": str(row["review_text_hash"]),
                "place_id": row.get("place_id"),
                "source_id": str(row["source_id"]),
                "source_record_sk": str(row["source_record_sk"]),
                "source_release_id": str(row["source_release_id"]),
                "enrichment_run_id": str(row["processing_run_id"]),
                "sentiment": str(row["sentiment_label"]),
                "confidence": _finite_confidence(row["confidence"]),
                "sentiment_model_id": str(row["model_id"]),
                "sentiment_model_fingerprint": str(row["model_fingerprint"]),
                "sentiment_config_fingerprint": str(row["config_fingerprint"]),
                "embedding_model_id": embedder.model_id,
                "embedding_model_revision": embedder.revision,
                "embedding_model_fingerprint": embedder.model_fingerprint,
                "embedding_config_fingerprint": embedder.config_fingerprint,
                "index_generation": generation,
            }
            points.append(
                (point_id(generation, "review", identity, chunk.index), vector, payload)
            )
    return tuple(points)


def build_aspect_points(
    rows: Iterable[Mapping[str, Any]],
    embedder: MiniLMEmbedder,
    *,
    generation: str,
    reviews_by_id: Mapping[str, Mapping[str, Any]],
) -> tuple[tuple[str, list[float], dict[str, Any]], ...]:
    points = []
    for row in rows:
        parent = reviews_by_id[str(row["review_sentiment_id"])]
        chunks = embedder.chunks(str(row["segment_text"]))
        vectors = embedder.encode(chunks)
        identity = str(row["aspect_sentiment_id"])
        for chunk, vector in zip(chunks, vectors, strict=True):
            payload = {
                "aspect_sentiment_id": identity,
                "review_sentiment_id": str(row["review_sentiment_id"]),
                "review_id": str(row["review_id"]),
                "segment_index": int(row["segment_ordinal"]),
                "segment_chunk_index": chunk.index,
                "segment_chunk_count": chunk.count,
                "segment_text_hash": str(row["segment_text_hash"]),
                "segment_chunk_text_hash": chunk.text_hash,
                "segment_text": chunk.text,
                "segment_start_char": row.get("segment_start_char"),
                "segment_end_char": row.get("segment_end_char"),
                "sentence_index": row.get("sentence_ordinal"),
                "clause_index": row.get("clause_ordinal"),
                "aspect": str(row["aspect"]),
                "aspect_keyword": row.get("matched_keyword"),
                "aspect_negated": bool(row["negation_detected"]),
                "negation_metadata": row.get("negation_metadata"),
                "place_id": parent.get("place_id"),
                "source_id": str(row["source_id"]),
                "source_record_sk": str(row["source_record_sk"]),
                "source_release_id": str(row["source_release_id"]),
                "enrichment_run_id": str(row["processing_run_id"]),
                "sentiment": str(row["sentiment_label"]),
                "confidence": _finite_confidence(row["confidence"]),
                "sentiment_model_id": str(row["model_id"]),
                "sentiment_model_fingerprint": str(row["model_fingerprint"]),
                "sentiment_config_fingerprint": str(row["config_fingerprint"]),
                "embedding_model_id": embedder.model_id,
                "embedding_model_revision": embedder.revision,
                "embedding_model_fingerprint": embedder.model_fingerprint,
                "embedding_config_fingerprint": embedder.config_fingerprint,
                "index_generation": generation,
            }
            points.append(
                (point_id(generation, "aspect", identity, chunk.index), vector, payload)
            )
    return tuple(points)


class QdrantBatchApi(Protocol):
    def upsert_points(self, name: str, points: Sequence[Mapping[str, Any]]) -> None: ...

    def point(self, name: str, point_id: str) -> dict[str, Any] | None: ...


def upsert_with_retry(
    api: QdrantBatchApi,
    collection: str,
    points: Sequence[tuple[str, list[float], dict[str, Any]]],
    *,
    batch_size: int,
    retries: int = 3,
    retry_delay: float = 0.2,
) -> None:
    for start in range(0, len(points), batch_size):
        batch = points[start : start + batch_size]
        body = [
            {"id": item[0], "vector": item[1], "payload": item[2]}
            for item in batch
        ]
        for attempt in range(retries + 1):
            try:
                api.upsert_points(collection, body)
                break
            except Exception as error:
                if attempt >= retries:
                    raise QdrantProvisioningError(
                        f"Qdrant upsert failed after {retries + 1} attempts"
                    ) from error
                time.sleep(retry_delay * (2**attempt))


def reconcile_points(
    api: QdrantBatchApi,
    collection: str,
    points: Sequence[tuple[str, list[float], dict[str, Any]]],
) -> int:
    missing = [point[0] for point in points if api.point(collection, point[0]) is None]
    if missing:
        raise IndexingError(f"Qdrant reconciliation missing {len(missing)} points")
    return len(points)


def now_utc() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


__all__ = [
    "IndexingError",
    "MiniLMEmbedder",
    "TextChunk",
    "build_aspect_points",
    "build_review_points",
    "configuration_fingerprint",
    "generation_id",
    "now_utc",
    "point_id",
    "reconcile_points",
    "sha256_text",
    "upsert_with_retry",
    "validate_vector",
]


INDEX_RUN_TABLE = """
CREATE TABLE IF NOT EXISTS travel.silver.review_index_run (
  index_generation STRING NOT NULL,
  source_id STRING NOT NULL,
  source_release_id STRING NOT NULL,
  enrichment_run_id STRING NOT NULL,
  embedding_model_id STRING NOT NULL,
  embedding_model_revision STRING NOT NULL,
  embedding_model_fingerprint STRING NOT NULL,
  embedding_config_fingerprint STRING NOT NULL,
  status STRING NOT NULL,
  expected_review_point_count BIGINT NOT NULL,
  expected_aspect_point_count BIGINT NOT NULL,
  actual_review_point_count BIGINT NOT NULL,
  actual_aspect_point_count BIGINT NOT NULL,
  review_collection STRING NOT NULL,
  aspect_collection STRING NOT NULL,
  started_at TIMESTAMP NOT NULL,
  finished_at TIMESTAMP,
  error_summary STRING
) USING iceberg
TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='zstd')
"""


class SparkIndexRunLedger:
    """Durable Iceberg publication gate for the two non-atomic Qdrant writes."""

    def __init__(self, spark: Any):
        self.spark = spark
        self.table = "travel.silver.review_index_run"

    def create_table(self) -> None:
        self.spark.sql(INDEX_RUN_TABLE)

    def get(self, generation: str) -> dict[str, Any] | None:
        rows = self.spark.sql(
            f"SELECT * FROM {self.table} WHERE index_generation = "
            f"'{generation.replace(chr(39), chr(39) * 2)}'"
        ).collect()
        return rows[0].asDict(recursive=True) if rows else None

    def write(self, row: Mapping[str, Any]) -> None:
        from pyspark.sql.types import (
            LongType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        schema = StructType(
            [
                StructField("index_generation", StringType(), False),
                StructField("source_id", StringType(), False),
                StructField("source_release_id", StringType(), False),
                StructField("enrichment_run_id", StringType(), False),
                StructField("embedding_model_id", StringType(), False),
                StructField("embedding_model_revision", StringType(), False),
                StructField("embedding_model_fingerprint", StringType(), False),
                StructField("embedding_config_fingerprint", StringType(), False),
                StructField("status", StringType(), False),
                StructField("expected_review_point_count", LongType(), False),
                StructField("expected_aspect_point_count", LongType(), False),
                StructField("actual_review_point_count", LongType(), False),
                StructField("actual_aspect_point_count", LongType(), False),
                StructField("review_collection", StringType(), False),
                StructField("aspect_collection", StringType(), False),
                StructField("started_at", TimestampType(), False),
                StructField("finished_at", TimestampType(), True),
                StructField("error_summary", StringType(), True),
            ]
        )
        frame = self.spark.createDataFrame([dict(row)], schema=schema)
        frame.createOrReplaceTempView("t04d_index_run")
        columns = [field.name for field in schema.fields]
        assignments = ", ".join(f"t.{column}=s.{column}" for column in columns)
        self.spark.sql(
            f"MERGE INTO {self.table} t USING t04d_index_run s "
            f"ON t.index_generation=s.index_generation "
            f"WHEN MATCHED THEN UPDATE SET {assignments} "
            f"WHEN NOT MATCHED THEN INSERT ({', '.join(columns)}) "
            f"VALUES ({', '.join('s.' + column for column in columns)})"
        )

    def begin(self, row: Mapping[str, Any]) -> None:
        existing = self.get(str(row["index_generation"]))
        if existing and existing.get("status") == "committed":
            return
        self.write(
            {
                **row,
                "status": "partial",
                "actual_review_point_count": 0,
                "actual_aspect_point_count": 0,
                "started_at": now_utc(),
                "finished_at": None,
                "error_summary": None,
            }
        )

    def finish(
        self, row: Mapping[str, Any], review_count: int, aspect_count: int
    ) -> None:
        self.write(
            {
                **row,
                "status": "committed",
                "actual_review_point_count": review_count,
                "actual_aspect_point_count": aspect_count,
                "started_at": self.get(str(row["index_generation"]))["started_at"],
                "finished_at": now_utc(),
                "error_summary": None,
            }
        )

    def fail(
        self, row: Mapping[str, Any], error: str, review_count: int, aspect_count: int
    ) -> None:
        existing = self.get(str(row["index_generation"]))
        self.write(
            {
                **row,
                "status": "failed",
                "actual_review_point_count": review_count,
                "actual_aspect_point_count": aspect_count,
                "started_at": existing.get("started_at") if existing else now_utc(),
                "finished_at": now_utc(),
                "error_summary": error[:2000],
            }
        )


__all__.append("INDEX_RUN_TABLE")
__all__.append("SparkIndexRunLedger")
