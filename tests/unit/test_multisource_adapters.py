import json
from pathlib import Path

from travel_intelligence.ingestion.bronze_writer import LocalObjectStore, publish_bronze
from travel_intelligence.ingestion.multisource import (
    adapt_foody,
    adapt_tiktok,
    adapt_vexere,
)
from travel_intelligence.silver.normalization import normalize_tiktok_row


def test_adapters_preserve_payload_and_source_contracts():
    foody = adapt_foody(Path("tests/fixtures/foody_minimal.json"))
    tiktok = adapt_tiktok(Path("tests/fixtures/tiktok_minimal.json"))
    vexere = adapt_vexere(Path("tests/fixtures/vexere_minimal.json"))
    assert (foody.record_type, foody.accepted_records) == ("restaurant", 1)
    assert (tiktok.record_type, tiktok.nested_record_count) == ("video", 2)
    assert (vexere.record_type, vexere.accepted_records) == ("transport_offer", 1)
    assert json.loads(tiktok.rows[0].payload_json)["comments"][1]["text"] == "很好"
    assert vexere.rows[0].source_record_id != vexere.rows[0].source_uri
    assert tiktok.rows[0].source_collected_at_raw == "2026-09-24T22:44:49.286774"


def test_tiktok_duplicate_and_mismatch_warnings(tmp_path):
    path = tmp_path / "tiktok.json"
    path.write_text(
        json.dumps(
            [
                {
                    "video_id": "v-1",
                    "comment_count": 5,
                    "comments": [
                        {"comment_id": "c-1"},
                        {"comment_id": "c-1"},
                        {"text": "x"},
                    ],
                }
            ]
        ),
        encoding="utf-8",
    )
    result = adapt_tiktok(path)
    assert result.accepted_records == 1
    assert result.nested_record_count == 3
    assert sum(issue.code == "comment_count_mismatch" for issue in result.warnings) == 1
    assert sum(issue.code == "missing_comment_id" for issue in result.warnings) == 1


def test_all_sources_publish_with_source_prefixes(tmp_path):
    store = LocalObjectStore(tmp_path)
    for adapter, filename in (
        (adapt_foody, "foody_minimal.json"),
        (adapt_tiktok, "tiktok_minimal.json"),
        (adapt_vexere, "vexere_minimal.json"),
    ):
        result = adapter(Path("tests/fixtures") / filename)
        publication = publish_bronze(store, result)
        rerun = publish_bronze(store, result)
        assert publication.status == "published"
        assert rerun.status == "already_published"
        assert publication.parquet_key.startswith(
            f"bronze/{result.source_id}/{result.record_type}/"
        )


def test_task003f_tiktok_batches_are_deterministic_and_isolated():
    recovery_path = Path("tests/fixtures/tiktok_task003f_recovery.json")
    changed_path = Path("tests/fixtures/tiktok_task003f_changed.json")
    recovery_first = adapt_tiktok(recovery_path)
    recovery_second = adapt_tiktok(recovery_path)
    changed = adapt_tiktok(changed_path)

    assert recovery_first.batch_id == recovery_second.batch_id
    assert recovery_first.batch_id != changed.batch_id
    assert recovery_first.input_sha256 != changed.input_sha256


def test_task003f_nested_count_fixture_is_isolated_with_three_comments():
    nested = adapt_tiktok(
        Path("tests/fixtures/tiktok_task003f_nested_count.json")
    )
    recovery = adapt_tiktok(Path("tests/fixtures/tiktok_task003f_recovery.json"))
    changed = adapt_tiktok(Path("tests/fixtures/tiktok_task003f_changed.json"))

    assert nested.accepted_records == 1
    assert nested.nested_record_count == 3
    assert nested.batch_id not in {recovery.batch_id, changed.batch_id}


def test_changed_tiktok_input_preserves_distinct_observation_history():
    recovery = adapt_tiktok(Path("tests/fixtures/tiktok_task003f_recovery.json"))
    changed = adapt_tiktok(Path("tests/fixtures/tiktok_task003f_changed.json"))
    recovery_record = normalize_tiktok_row(
        recovery.rows[0], "bronze/task003f-recovery.parquet"
    )
    changed_record = normalize_tiktok_row(
        changed.rows[0], "bronze/task003f-changed.parquet"
    )

    recovery_source = recovery_record["source_record"][0]
    changed_source = changed_record["source_record"][0]
    assert recovery_source["batch_id"] != changed_source["batch_id"]
    assert recovery_source["source_record_sk"] != changed_source["source_record_sk"]
    assert recovery_record["social_content"][0]["content_id"] != (
        changed_record["social_content"][0]["content_id"]
    )
