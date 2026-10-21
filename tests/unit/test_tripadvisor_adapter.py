import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from travel_intelligence.ingestion.json_snapshot import (
    DuplicateJSONKeyError,
    SnapshotReadError,
    read_json_snapshot,
)
from travel_intelligence.ingestion.tripadvisor import (
    adapt_tripadvisor,
    batch_id,
    canonical_json,
    payload_hash,
)

FIXTURE = Path("tests/fixtures/tripadvisor_minimal.json")


@pytest.mark.parametrize(
    ("value", "expected", "digest"),
    [
        (
            {"b": 2, "a": 1},
            '{"a":1,"b":2}',
            "43258cff783fe7036d8a43033f830adfc60ec037382473548ac742b888292777",
        ),
        (
            {"x": [2, 1], "n": None},
            '{"n":null,"x":[2,1]}',
            "b26a24068d4ba6e6a6097f2ebb42b45f9f935b4685d4a88ebf3ae4bfbf6a8f60",
        ),
        (
            {"text": "Café / 景色很好"},
            '{"text":"Café / 景色很好"}',
            "ff233b681611a590994234eea842e78b950909c1b26841d696f4b39f29842bd5",
        ),
        (
            1.0,
            "1",
            "6b86b273ff34fce19d6b804eff5a3f5747ada4eaa22f1d49c01e52ddb7875b4b",
        ),
        (
            -0.0,
            "0",
            "5feceb66ffc86f38d952786c6d696c79c2dbc239dd4e91b46729d73a27fb57e9",
        ),
        (
            1e-7,
            "1e-7",
            "5b33e02f2c5103a05d32f6ba9cb058294452bfbf393967f68bb30c1bdcbbab22",
        ),
        (
            1e-6,
            "0.000001",
            "159fb29a827ad04b260aa6c8ab6d8637f8f2b38af5c4f3cb49d6a21205e040f8",
        ),
        (
            1e20,
            "100000000000000000000",
            "c344e9487bfbd5c4e03c9fb90d62a5dde5e00b54d55c46e9f4a803aea162b80c",
        ),
        (
            1e21,
            "1e+21",
            "241c4643fa70b1dcde1205b71be4e3bebb17e9f880c8e1a33d0ead6c27271d3c",
        ),
    ],
)
def test_canonical_json_golden_vectors(value, expected, digest):
    assert canonical_json(value) == expected
    assert payload_hash(value) == digest


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -float("inf")])
def test_canonical_json_rejects_non_finite_numbers(value):
    with pytest.raises(ValueError):
        canonical_json(value)


def test_snapshot_reader_rejects_duplicate_object_keys(tmp_path):
    path = tmp_path / "duplicate-key.json"
    path.write_text('[{"record_id":"x","record_id":"y"}]', encoding="utf-8")
    with pytest.raises(SnapshotReadError) as exc_info:
        read_json_snapshot(path)
    assert isinstance(exc_info.value.__cause__, DuplicateJSONKeyError)


def test_adapter_preserves_nested_order_and_source_payload():
    result = adapt_tripadvisor(
        FIXTURE,
        ingested_at=datetime(2026, 10, 17, tzinfo=timezone.utc),
    )
    assert result.input_records == 1
    assert result.accepted_records == 1
    assert result.rejected_records == 0
    assert result.nested_review_count == 2
    row = result.rows[0]
    assert row.source_row_index == 0
    payload = json.loads(row.payload_json)
    assert [review["index"] for review in payload["reviews"]] == [1, 2]
    assert payload["reviews"][1]["detail_review"] == "景色很好"
    assert payload["detail_review"] not in [
        review["detail_review"] for review in payload["reviews"]
    ]
    assert row.source_record_id == "ta-1"
    assert row.source_collected_at_raw.endswith("+00:00")
    assert row.ingested_at == "2026-10-17T00:00:00.000000Z"
    assert any(issue.code == "crawler_status" for issue in result.warnings)


def test_repeatability_and_duplicate_warning(tmp_path):
    content = json.loads(FIXTURE.read_text())
    content.append(content[0])
    path = tmp_path / "duplicate.json"
    path.write_text(json.dumps(content, ensure_ascii=False), encoding="utf-8")
    first = adapt_tripadvisor(path)
    second = adapt_tripadvisor(path)
    assert first.batch_id == second.batch_id == batch_id(path.read_bytes())
    assert [row.payload_hash for row in first.rows] == [
        row.payload_hash for row in second.rows
    ]
    assert any(issue.code == "duplicate_record_id" for issue in first.warnings)


def test_structural_invalid_record_is_rejected(tmp_path):
    path = tmp_path / "invalid.json"
    path.write_text(
        json.dumps([{"record_id": "missing-reviews", "source_url": "x"}]),
        encoding="utf-8",
    )
    result = adapt_tripadvisor(path)
    assert result.accepted_records == 0
    assert result.rejected_records == 1
    assert result.errors[0].code == "missing_required_field"


def test_source_row_index_preserves_gaps_and_duplicate_occurrences(tmp_path):
    record = json.loads(FIXTURE.read_text())[0]
    path = tmp_path / "indexed.json"
    path.write_text(
        json.dumps([{"record_id": "invalid", "source_url": "x"}, record, record]),
        encoding="utf-8",
    )
    result = adapt_tripadvisor(path)
    assert result.rejected_records == 1
    assert [row.source_row_index for row in result.rows] == [1, 2]
    assert result.rows[0].payload_hash == result.rows[1].payload_hash


def test_snapshot_reader_rejects_non_array(tmp_path):
    path = tmp_path / "not-array.json"
    path.write_text(json.dumps({"record_id": "x"}), encoding="utf-8")
    with pytest.raises(SnapshotReadError):
        adapt_tripadvisor(path)

