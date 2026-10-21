import pytest

from travel_intelligence.backfill.normalization import (
    execute_plan,
    existing_normalizer_command,
    plan_backfill,
)


def _manifest(
    batch_id,
    *,
    source_id="tripadvisor",
    contract="bronze-contract-v2",
    schema="bronze-v2",
    provenance=None,
):
    row = {
        "manifest_key": f"bronze/_manifests/{source_id}/{batch_id}.json",
        "batch_id": batch_id,
        "source_id": source_id,
        "status": "success",
        "contract_version": contract,
        "schema_version": schema,
        "row_count": 1,
        "payload_hashes": ["hash"],
        "ingested_at": "2026-10-20T01:00:00Z",
        "nested_review_count": 2,
    }
    if provenance is not None:
        row["provenance_class"] = provenance
    return row


def _release(batch_id, status="committed", refs=True):
    return {
        "run_id": f"run-{batch_id}",
        "source_id": "tripadvisor",
        "bronze_batch_id": batch_id,
        "normalization_version": "silver-foundation-v1",
        "status": status,
        "table_snapshot_refs": (
            {
                table: f"snapshot-{table}"
                for table in (
                    "source_record",
                    "place",
                    "place_rating",
                    "review",
                    "source_entity_mapping",
                    "normalization_run",
                )
            }
            if refs
            else None
        ),
    }


def test_tripadvisor_only_and_provenance_and_legacy_exclusion():
    plan = plan_backfill(
        [
            _manifest("ta-synthetic", provenance="synthetic"),
            _manifest("foody", source_id="foody", provenance="synthetic"),
            _manifest("ta-unknown"),
            _manifest(
                "ta-legacy",
                contract="bronze-contract-v1",
                schema="bronze-v1",
                provenance="synthetic",
            ),
        ],
        [],
    )
    assert [item.batch_id for item in plan.decisions] == [
        "foody",
        "ta-legacy",
        "ta-synthetic",
        "ta-unknown",
    ]
    by_batch = {item.batch_id: item for item in plan.decisions}
    assert by_batch["ta-synthetic"].status == "pending"
    assert "source_not_tripadvisor" in by_batch["foody"].reasons
    assert "bronze_contract_ineligible" in by_batch["ta-legacy"].reasons
    assert "provenance_not_verified_synthetic" in by_batch["ta-unknown"].reasons


def test_committed_incomplete_and_failed_releases_are_distinguished():
    manifests = [
        _manifest("committed", provenance="synthetic"),
        _manifest("incomplete", provenance="synthetic"),
        _manifest("failed", provenance="synthetic"),
    ]
    plan = plan_backfill(
        manifests,
        [
            _release("committed"),
            _release("incomplete", refs=False),
            _release("failed", status="failed"),
        ],
    )
    statuses = {item.batch_id: item.status for item in plan.decisions}
    assert statuses == {
        "committed": "already_committed",
        "failed": "failed_or_incomplete",
        "incomplete": "failed_or_incomplete",
    }


def test_deterministic_order_and_bound():
    manifests = [
        _manifest("b", provenance="synthetic"),
        {
            **_manifest("a", provenance="synthetic"),
            "ingested_at": "2026-01-01T00:00:00Z",
        },
        {
            **_manifest("c", provenance="synthetic"),
            "ingested_at": "2026-01-01T00:00:00Z",
        },
    ]
    plan = plan_backfill(manifests, [], max_batches=2)
    assert plan.selected_batch_ids == ("a", "c")
    assert plan.to_dict()["pending_batch_ids"] == ["a", "c", "b"]


def test_execute_requires_authorization_and_isolates_failures():
    plan = plan_backfill(
        [
            _manifest("a", provenance="synthetic"),
            _manifest("b", provenance="synthetic"),
        ],
        [],
    )
    with pytest.raises(PermissionError):
        execute_plan(plan, confirm_writes=False, runner=lambda item: None)
    calls = []

    def runner(item):
        calls.append(item.batch_id)
        if item.batch_id == "a":
            raise RuntimeError("isolated failure")

    result = execute_plan(plan, confirm_writes=True, runner=runner)
    assert calls == ["a", "b"]
    assert result.failed_batch_ids == ("a",)
    assert result.completed_batch_ids == ("b",)


def test_empty_workload_and_existing_driver_contract():
    plan = plan_backfill(
        [_manifest("done", provenance="synthetic")], [_release("done")]
    )
    result = execute_plan(
        plan,
        confirm_writes=True,
        runner=lambda item: pytest.fail("unexpected write"),
    )
    assert result.planned_batch_ids == ()
    assert existing_normalizer_command(
        plan.decisions[0], script="normalize.py", bucket="bucket"
    ) == [
        "python",
        "normalize.py",
        "--manifest-key",
        plan.decisions[0].manifest_key,
        "--bucket",
        "bucket",
    ]
