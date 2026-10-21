"""Source-agnostic, staged Bronze Parquet publication."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Protocol

import pyarrow as pa
import pyarrow.parquet as pq

from .tripadvisor import BronzeRow

BRONZE_SCHEMA = pa.schema(
    [
        ("source_id", pa.string()),
        ("record_type", pa.string()),
        ("source_record_id", pa.string()),
        ("source_uri", pa.string()),
        ("batch_id", pa.string()),
        ("ingested_at", pa.timestamp("us", tz="UTC")),
        ("source_collected_at_raw", pa.string()),
        ("source_file", pa.string()),
        ("payload_hash", pa.string()),
        ("payload_json", pa.string()),
        ("source_row_index", pa.int64()),
    ]
)


class ObjectStore(Protocol):
    def get(self, key: str) -> bytes | None: ...

    def put(self, key: str, data: bytes, content_type: str) -> None: ...


@dataclass(frozen=True)
class PublicationResult:
    batch_id: str
    status: str
    parquet_key: str
    manifest_key: str
    row_count: int
    nested_review_count: int
    payload_hashes: tuple[str, ...]


def _rows_table(rows: tuple[BronzeRow, ...]) -> pa.Table:
    values = {field.name: [] for field in BRONZE_SCHEMA}
    for row in rows:
        values["source_id"].append(row.source_id)
        values["record_type"].append(row.record_type)
        values["source_record_id"].append(row.source_record_id)
        values["source_uri"].append(row.source_uri)
        values["batch_id"].append(row.batch_id)
        values["ingested_at"].append(
            datetime.fromisoformat(row.ingested_at.replace("Z", "+00:00"))
        )
        values["source_collected_at_raw"].append(row.source_collected_at_raw)
        values["source_file"].append(row.source_file)
        values["payload_hash"].append(row.payload_hash)
        values["payload_json"].append(row.payload_json)
        values["source_row_index"].append(row.source_row_index)
    return pa.Table.from_pydict(values, schema=BRONZE_SCHEMA)


def _parquet_bytes(rows: tuple[BronzeRow, ...]) -> bytes:
    output = BytesIO()
    pq.write_table(_rows_table(rows), output, compression="zstd")
    return output.getvalue()


def _verify_parquet(
    data: bytes, rows: tuple[BronzeRow, ...], nested_field: str | None
) -> tuple[int, int]:
    table = pq.read_table(BytesIO(data))
    if table.schema != BRONZE_SCHEMA:
        raise ValueError("Bronze Parquet schema mismatch")
    actual = table.to_pylist()
    expected_hashes = [row.payload_hash for row in rows]
    actual_hashes = [row["payload_hash"] for row in actual]
    if actual_hashes != expected_hashes:
        raise ValueError("Parquet payload hash reconciliation failed")
    actual_indices = [row["source_row_index"] for row in actual]
    expected_indices = [row.source_row_index for row in rows]
    if actual_indices != expected_indices:
        raise ValueError("Parquet source row index reconciliation failed")
    if any(index < 0 for index in actual_indices):
        raise ValueError("source_row_index must be non-negative")
    nested_count = (
        sum(len(json.loads(row["payload_json"])[nested_field]) for row in actual)
        if nested_field
        else 0
    )
    return len(actual), nested_count


def publish_bronze(
    store: ObjectStore,
    result,
    *,
    bucket_prefix: str = "bronze",
    ingest_date: date | None = None,
    fail_after_staging: bool = False,
) -> PublicationResult:
    """Publish an already validated batch, idempotently and without deletes."""
    if result.rejected_records:
        raise ValueError("cannot publish a batch containing rejected records")
    source_id = getattr(result, "source_id", "tripadvisor")
    record_type = getattr(result, "record_type", "place")
    nested_field = getattr(result, "nested_field", "reviews")
    day = (ingest_date or datetime.now(timezone.utc).date()).isoformat()
    base = (
        f"{bucket_prefix}/{source_id}/{record_type}/ingest_date={day}/"
        f"batch_id={result.batch_id}"
    )
    parquet_key = f"{base}/part-00000.parquet"
    manifest_key = (
        f"{bucket_prefix}/_manifests/{source_id}/{result.batch_id}.json"
    )
    existing = store.get(manifest_key)
    if existing is not None:
        manifest = json.loads(existing)
        if manifest.get("status") == "success":
            return PublicationResult(
                result.batch_id,
                "already_published",
                manifest["parquet_key"],
                manifest_key,
                manifest["row_count"],
                manifest["nested_review_count"],
                tuple(manifest["payload_hashes"]),
            )
        raise ValueError("batch has an existing non-success manifest")

    data = _parquet_bytes(result.rows)
    staging_key = f"{base}/_staging/part-00000.parquet"
    store.put(staging_key, data, "application/vnd.apache.parquet")
    if fail_after_staging:
        raise RuntimeError("simulated failure after staging")
    row_count, nested_count = _verify_parquet(data, result.rows, nested_field)
    if (
        row_count != result.accepted_records
        or nested_count != result.nested_review_count
    ):
        raise ValueError("Bronze count reconciliation failed")
    store.put(parquet_key, data, "application/vnd.apache.parquet")
    manifest = {
        "source_id": source_id,
        "record_type": record_type,
        "batch_id": result.batch_id,
        "contract_version": "bronze-contract-v2",
        "schema_version": "bronze-v2",
        "status": "success",
        "input_sha256": result.input_sha256,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "row_count": row_count,
        "nested_review_count": nested_count,
        "quarantined_count": result.rejected_records,
        "parquet_key": parquet_key,
        "payload_hashes": list(result.rows[i].payload_hash for i in range(row_count)),
        "schema": str(BRONZE_SCHEMA),
        "staging_key": staging_key,
    }
    manifest_bytes = json.dumps(manifest, ensure_ascii=False, sort_keys=True).encode()
    store.put(manifest_key, manifest_bytes, "application/json")
    return PublicationResult(
        result.batch_id,
        "published",
        parquet_key,
        manifest_key,
        row_count,
        nested_count,
        tuple(result.rows[i].payload_hash for i in range(row_count)),
    )


class LocalObjectStore:
    """Filesystem object store used by tests and local dry runs."""

    def __init__(self, root: str | Path):
        self.root = Path(root)

    def get(self, key: str) -> bytes | None:
        path = self.root / key
        return path.read_bytes() if path.exists() else None

    def put(self, key: str, data: bytes, content_type: str) -> None:
        path = self.root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists() and path.read_bytes() != data:
            raise FileExistsError(f"immutable object differs: {key}")
        path.write_bytes(data)


class MinioObjectStore:
    def __init__(self, client, bucket: str):
        self.client = client
        self.bucket = bucket

    def get(self, key: str) -> bytes | None:
        try:
            response = self.client.get_object(self.bucket, key)
            try:
                return response.read()
            finally:
                response.close()
                response.release_conn()
        except Exception as exc:
            if exc.__class__.__name__ == "S3Error" and getattr(exc, "code", "") in {
                "NoSuchKey",
                "NoSuchObject",
            }:
                return None
            raise

    def put(self, key: str, data: bytes, content_type: str) -> None:
        self.client.put_object(
            self.bucket,
            key,
            BytesIO(data),
            len(data),
            content_type=content_type,
        )


__all__ = ["BRONZE_SCHEMA", "LocalObjectStore", "MinioObjectStore", "publish_bronze"]
