import pytest

from travel_intelligence.silver.foundation import (
    SILVER_TABLES,
    ReleaseLedger,
    committed_replay_is_noop,
    execute_table_writes,
    release_is_visible,
    validate_bronze_manifest,
    validate_release_tables,
)


def test_silver_contract_defines_all_tables_and_transport_name():
    names = {table.name for table in SILVER_TABLES}
    assert names == {
        "place",
        "place_rating",
        "review",
        "social_content",
        "social_engagement",
        "social_comment",
        "transport_route",
        "source_record",
        "source_entity_mapping",
        "normalization_run",
        "tourism_supply",
    }
    assert any(
        "source_row_index BIGINT NOT NULL" in table.columns_sql
        for table in SILVER_TABLES
    )


def test_bronze_v1_manifest_is_rejected():
    manifest = {
        "status": "success",
        "contract_version": "bronze-contract-v1",
        "schema_version": "bronze-v1",
        "source_id": "tripadvisor",
        "batch_id": "b1",
        "parquet_key": "bronze/x.parquet",
        "row_count": 0,
        "payload_hashes": [],
    }
    with pytest.raises(ValueError, match="not eligible"):
        validate_bronze_manifest(manifest)


def test_release_requires_every_table_snapshot():
    with pytest.raises(ValueError, match="missing tables"):
        validate_release_tables(("source_record", "normalization_run"), {})
    validate_release_tables(
        ("source_record", "normalization_run"),
        {"source_record": "snapshot-1", "normalization_run": "snapshot-2"},
    )


def test_release_ledger_does_not_commit_partial_tables():
    ledger = ReleaseLedger(("source_record", "place"))
    ledger.record_table_commit("source_record", "snapshot-1")
    with pytest.raises(ValueError, match="missing tables"):
        ledger.commit()
    assert ledger.status == "partial"
    ledger.record_table_commit("place", "snapshot-2")
    ledger.commit()
    assert ledger.status == "committed"


def test_committed_only_visibility_rejects_partial_and_incomplete_runs():
    required = ("source_record", "social_content", "social_engagement")
    partial_refs = {
        "source_record": "snapshot-1",
        "social_content": "snapshot-2",
    }
    assert not release_is_visible("partial", required, partial_refs)
    assert not release_is_visible("committed", required, partial_refs)
    assert not release_is_visible("failed", required, partial_refs)


def test_retry_makes_one_complete_release_visible_without_duplicates():
    required = ("source_record", "social_content", "social_engagement")
    rows_by_table = {table: {} for table in required}
    deterministic_ids = {
        "source_record": "source-record-1",
        "social_content": "content-1",
        "social_engagement": "engagement-1",
    }

    # Failure after an intermediate write leaves physical rows but no release.
    for table in required[:2]:
        rows_by_table[table][deterministic_ids[table]] = {"batch_id": "batch-1"}
    assert not release_is_visible(
        "partial",
        required,
        {"source_record": "snapshot-1", "social_content": "snapshot-2"},
    )

    # Recovery uses the same deterministic MERGE keys.
    refs = {}
    for position, table in enumerate(required, start=1):
        rows_by_table[table][deterministic_ids[table]] = {"batch_id": "batch-1"}
        refs[table] = f"snapshot-{position}"
    assert release_is_visible("committed", required, refs)
    assert {table: len(rows) for table, rows in rows_by_table.items()} == {
        table: 1 for table in required
    }


def test_failure_injection_occurs_after_intermediate_table_write():
    written = []

    def write_table(table):
        written.append(table)
        return f"{table}-snapshot"

    with pytest.raises(RuntimeError, match="after social_content"):
        execute_table_writes(
            ("source_record", "social_content", "social_engagement"),
            write_table,
            fail_after_table="social_content",
        )

    assert written == ["source_record", "social_content"]


def _committed_run(**overrides):
    run = {
        "source_id": "tiktok",
        "bronze_batch_id": "batch-1",
        "normalization_version": "normalization-v1",
        "input_schema_fingerprint": "input-fingerprint",
        "target_schema_fingerprint": "target-fingerprint",
        "status": "committed",
        "table_snapshot_refs": {
            "source_record": "snapshot-source",
            "social_content": "snapshot-content",
            "normalization_run": "snapshot-ledger",
        },
    }
    run.update(overrides)
    return run


def _replay_is_noop(run, snapshot_exists):
    return committed_replay_is_noop(
        run,
        source_id="tiktok",
        batch_id="batch-1",
        normalization_version="normalization-v1",
        input_schema_fingerprint="input-fingerprint",
        target_schema_fingerprint="target-fingerprint",
        required_tables=("source_record", "social_content", "normalization_run"),
        snapshot_exists=snapshot_exists,
    )


def test_valid_committed_replay_is_true_noop_without_writes_or_ref_changes():
    run = _committed_run()
    original_refs = dict(run["table_snapshot_refs"])
    checked = []
    writes = []

    is_noop = _replay_is_noop(
        run,
        lambda table, snapshot: checked.append((table, snapshot)) or True,
    )
    if not is_noop:
        writes.append("unexpected-write")

    assert is_noop
    assert writes == []
    assert run["table_snapshot_refs"] == original_refs
    assert len(checked) == 3


def test_partial_replay_is_never_skipped_and_does_not_check_snapshots():
    checks = []
    assert not _replay_is_noop(
        _committed_run(status="partial", table_snapshot_refs=None),
        lambda table, snapshot: checks.append((table, snapshot)) or True,
    )
    assert checks == []


@pytest.mark.parametrize(
    "field,value",
    [
        ("source_id", "foody"),
        ("bronze_batch_id", "other-batch"),
        ("normalization_version", "other-version"),
        ("input_schema_fingerprint", "other-input"),
        ("target_schema_fingerprint", "other-schema"),
    ],
)
def test_committed_replay_conflicts_fail_safely(field, value):
    with pytest.raises(ValueError, match=field):
        _replay_is_noop(_committed_run(**{field: value}), lambda table, snapshot: True)


def test_committed_replay_requires_complete_existing_snapshot_evidence():
    with pytest.raises(ValueError, match="incomplete snapshot evidence"):
        _replay_is_noop(
            _committed_run(
                table_snapshot_refs={"source_record": "snapshot-source"}
            ),
            lambda table, snapshot: True,
        )


def test_committed_replay_requires_referenced_snapshots_to_exist():
    with pytest.raises(ValueError, match="social_content"):
        _replay_is_noop(
            _committed_run(),
            lambda table, snapshot: table != "social_content",
        )

