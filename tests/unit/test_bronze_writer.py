import json
from datetime import date
from pathlib import Path

import pyarrow as pa

from travel_intelligence.ingestion.bronze_writer import BRONZE_SCHEMA, publish_bronze
from travel_intelligence.ingestion.tripadvisor import (
    ADAPTER_VERSION,
    CONTRACT_VERSION,
    adapt_tripadvisor,
    batch_id_for,
)


class MemoryStore:
    def __init__(self):
        self.objects = {}

    def get(self, key):
        return self.objects.get(key)

    def put(self, key, data, content_type):
        if key in self.objects and self.objects[key] != data:
            raise AssertionError("immutable object changed")
        self.objects[key] = data


FIXTURE = Path("tests/fixtures/tripadvisor_minimal.json")


def test_publish_readback_and_idempotent_rerun():
    result = adapt_tripadvisor(FIXTURE)
    store = MemoryStore()
    first = publish_bronze(store, result, ingest_date=date(2026, 10, 17))
    count = len(store.objects)
    second = publish_bronze(store, result, ingest_date=date(2026, 10, 17))
    assert first.status == "published"
    assert second.status == "already_published"
    assert len(store.objects) == count
    assert first.row_count == 1
    assert first.nested_review_count == 2
    assert json.loads(store.objects[first.manifest_key])["status"] == "success"


def test_failure_leaves_staging_and_retry_publishes():
    result = adapt_tripadvisor(FIXTURE)
    store = MemoryStore()
    try:
        publish_bronze(
            store,
            result,
            ingest_date=date(2026, 10, 17),
            fail_after_staging=True,
        )
    except RuntimeError:
        pass
    assert any("_staging" in key for key in store.objects)
    assert not any("_manifests" in key for key in store.objects)
    retry = publish_bronze(store, result, ingest_date=date(2026, 10, 17))
    assert retry.status == "published"


def test_changed_input_has_different_batch(tmp_path):
    changed = tmp_path / "changed.json"
    changed.write_text(FIXTURE.read_text().replace("ta-1", "ta-2"), encoding="utf-8")
    assert adapt_tripadvisor(changed).batch_id != adapt_tripadvisor(FIXTURE).batch_id


def test_v2_schema_manifest_and_v1_batch_identity_separation():
    result = adapt_tripadvisor(FIXTURE)
    store = MemoryStore()
    publication = publish_bronze(store, result, ingest_date=date(2026, 10, 17))
    manifest = json.loads(store.objects[publication.manifest_key])
    v1_batch = batch_id_for(
        FIXTURE.read_bytes(),
        "tripadvisor",
        ADAPTER_VERSION,
        contract_version="bronze-contract-v1",
    )
    assert CONTRACT_VERSION == "bronze-contract-v2"
    assert result.batch_id != v1_batch
    assert len(BRONZE_SCHEMA) == 11
    assert BRONZE_SCHEMA.field("source_row_index").type == pa.int64()
    assert manifest["contract_version"] == "bronze-contract-v2"
    assert manifest["schema_version"] == "bronze-v2"

