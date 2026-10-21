"""Synthetic Bronze v2 verification against an existing MinIO instance."""

from __future__ import annotations

import json
import os
from datetime import date
from io import BytesIO
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from minio import Minio

from travel_intelligence.ingestion.bronze_writer import (
    BRONZE_SCHEMA,
    MinioObjectStore,
    publish_bronze,
)
from travel_intelligence.ingestion.multisource import (
    adapt_foody,
    adapt_tiktok,
    adapt_vexere,
)
from travel_intelligence.ingestion.tripadvisor import (
    ADAPTER_VERSION,
    adapt_tripadvisor,
    batch_id_for,
)

FIXTURE_ROOT = Path(__file__).parents[1] / "fixtures"
CASES = (
    ("tripadvisor", "tripadvisor_minimal.json", adapt_tripadvisor, 1, 2, "reviews"),
    ("foody", "foody_minimal.json", adapt_foody, 1, 0, None),
    ("tiktok", "tiktok_minimal.json", adapt_tiktok, 1, 2, "comments"),
    ("vexere", "vexere_minimal.json", adapt_vexere, 1, 0, None),
)


@pytest.fixture(scope="module")
def minio_context():
    names = ("MINIO_ENDPOINT", "MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD", "S3_BUCKET")
    values = {name: os.environ.get(name, "") for name in names}
    missing = [name for name, value in values.items() if not value]
    if missing:
        pytest.skip("MinIO unavailable; missing " + ", ".join(missing))
    endpoint = values["MINIO_ENDPOINT"]
    secure = endpoint.startswith("https://")
    endpoint = endpoint.removeprefix("http://").removeprefix("https://")
    client = Minio(
        endpoint,
        access_key=values["MINIO_ROOT_USER"],
        secret_key=values["MINIO_ROOT_PASSWORD"],
        secure=secure,
    )
    try:
        if not client.bucket_exists(values["S3_BUCKET"]):
            pytest.skip("MinIO unavailable; configured bucket does not exist")
    except Exception as exc:
        pytest.skip(f"MinIO unavailable: {type(exc).__name__}")
    return client, values["S3_BUCKET"]


def _get(client, bucket, key):
    response = client.get_object(bucket, key)
    try:
        return response.read()
    finally:
        response.close()
        response.release_conn()


def _final_objects(client, bucket, source):
    return {
        item.object_name
        for item in client.list_objects(
            bucket, prefix=f"bronze/{source}/", recursive=True
        )
        if "/_staging/" not in item.object_name
        and not item.object_name.endswith(".json")
    }


def _verify_final(client, bucket, publication, adapter_result, expected_nested, field):
    manifest = json.loads(_get(client, bucket, publication.manifest_key))
    table = pq.read_table(BytesIO(_get(client, bucket, publication.parquet_key)))
    rows = table.to_pylist()
    assert table.schema == BRONZE_SCHEMA
    assert len(table.schema) == 11
    assert table.schema.field("source_row_index").type == pa.int64()
    assert all(row["source_row_index"] is not None for row in rows)
    assert manifest["status"] == "success"
    assert manifest["contract_version"] == "bronze-contract-v2"
    assert manifest["schema_version"] == "bronze-v2"
    assert "source_row_index" in manifest["schema"]
    assert manifest["parquet_key"] == publication.parquet_key
    assert manifest["row_count"] == len(rows) == adapter_result.accepted_records
    assert manifest["nested_review_count"] == expected_nested
    actual_nested = (
        sum(len(json.loads(row["payload_json"])[field]) for row in rows)
        if field
        else 0
    )
    assert actual_nested == manifest["nested_review_count"]
    assert manifest["payload_hashes"] == [row["payload_hash"] for row in rows]
    assert [row["source_row_index"] for row in rows] == [
        row.source_row_index for row in adapter_result.rows
    ]
    assert [row["payload_json"] for row in rows] == [
        row.payload_json for row in adapter_result.rows
    ]


@pytest.mark.parametrize(
    ("source", "filename", "adapter", "expected_rows", "expected_nested", "field"),
    CASES,
)
def test_bronze_v2_minio_publish_and_idempotency(
    minio_context, source, filename, adapter, expected_rows, expected_nested, field
):
    client, bucket = minio_context
    adapter_result = adapter(FIXTURE_ROOT / filename)
    before = _final_objects(client, bucket, source)
    first = publish_bronze(MinioObjectStore(client, bucket), adapter_result)
    assert first.status in {"published", "already_published"}
    assert first.row_count == expected_rows
    _verify_final(client, bucket, first, adapter_result, expected_nested, field)
    second = publish_bronze(MinioObjectStore(client, bucket), adapter_result)
    assert second.status == "already_published"
    assert second.batch_id == first.batch_id
    assert second.parquet_key == first.parquet_key
    assert _final_objects(client, bucket, source) == before | {first.parquet_key}


def test_bronze_v2_identity_and_recovery(minio_context, tmp_path):
    client, bucket = minio_context
    record = json.loads((FIXTURE_ROOT / "tripadvisor_minimal.json").read_text())[0]
    duplicate_path = tmp_path / "duplicate.json"
    duplicate_path.write_text(json.dumps([record, record]), encoding="utf-8")
    duplicate = adapt_tripadvisor(duplicate_path)
    assert [row.source_row_index for row in duplicate.rows] == [0, 1]
    assert duplicate.rows[0].payload_hash == duplicate.rows[1].payload_hash
    assert duplicate.rows[0].source_row_index != duplicate.rows[1].source_row_index

    gap_path = tmp_path / "gap.json"
    gap_path.write_text(
        json.dumps([{"record_id": "invalid", "source_url": "x"}, record]),
        encoding="utf-8",
    )
    gap = adapt_tripadvisor(gap_path)
    assert gap.rejected_records == 1
    assert [row.source_row_index for row in gap.rows] == [1]

    v1_batch = batch_id_for(
        duplicate_path.read_bytes(),
        "tripadvisor",
        ADAPTER_VERSION,
        contract_version="bronze-contract-v1",
    )
    assert v1_batch != duplicate.batch_id

    recovery_record = dict(record)
    recovery_record["record_id"] = "synthetic-recovery-v2"
    recovery_path = tmp_path / "recovery.json"
    recovery_path.write_text(json.dumps([recovery_record]), encoding="utf-8")
    recovery = adapt_tripadvisor(recovery_path)
    store = MinioObjectStore(client, bucket)
    ingest_date = date.today()
    with pytest.raises(RuntimeError, match="simulated failure"):
        publish_bronze(
            store, recovery, ingest_date=ingest_date, fail_after_staging=True
        )
    staging = (
        f"bronze/tripadvisor/place/ingest_date={ingest_date.isoformat()}"
        f"/batch_id={recovery.batch_id}/_staging/part-00000.parquet"
    )
    assert store.get(staging) is not None
    retry = publish_bronze(store, recovery, ingest_date=ingest_date)
    assert retry.status == "published"
    assert publish_bronze(store, recovery, ingest_date=ingest_date).status == (
        "already_published"
    )
