from datetime import datetime, timezone

from travel_intelligence.backfill.inventory import (
    CATALOG_FILES,
    TRIPADVISOR_REQUIRED_RELEASE_TABLES,
    build_inventory_report,
    classify_provenance,
    evaluate_occurrences,
    snapshot_completeness,
)


def manifest(batch="b1", source="tripadvisor", *, status="success"):
    return {
        "batch_id": batch,
        "source_id": source,
        "status": status,
        "contract_version": "bronze-contract-v2",
        "schema_version": "bronze-v2",
        "row_count": 1,
        "payload_hashes": ["sha256:p"],
        "source_file": "tests/fixtures/tripadvisor_minimal.json",
    }


def release(batch="b1", *, status="committed", refs=None):
    return {
        "run_id": f"run-{batch}",
        "source_id": "tripadvisor",
        "bronze_batch_id": batch,
        "status": status,
        "table_snapshot_refs": refs
        if refs is not None
        else {table: f"snap-{table}" for table in TRIPADVISOR_REQUIRED_RELEASE_TABLES},
    }


def source_record(batch="b1", row_index=0, key="sr-1"):
    return {
        "source_record_sk": key,
        "source_id": "tripadvisor",
        "batch_id": batch,
        "source_row_index": row_index,
    }


def review(batch="b1", row_index=0, source_key="sr-1", review_id="r-1", ordinal=0):
    return {
        "source_id": "tripadvisor",
        "batch_id": batch,
        "source_row_index": row_index,
        "source_record_sk": source_key,
        "review_id": review_id,
        "review_ordinal": ordinal,
        "review_text": "Rất tốt",
        "detail_review": None,
        "place_id": "p-1",
    }


def test_provenance_requires_stored_evidence():
    assert classify_provenance(manifest()) == "synthetic"
    assert classify_provenance({"source_file": "crawler/run.json"}) == "real_crawler"
    assert classify_provenance({"source_file": "opaque/object.json"}) == "unknown"
    assert (
        classify_provenance(
            {"input_sha256": "abc"}, synthetic_input_hashes=("sha256:abc",)
        )
        == "synthetic"
    )


def test_snapshot_completeness_and_committed_gate():
    complete, missing = snapshot_completeness(release())
    assert complete is True
    assert missing == ()
    incomplete = release(refs={"source_record": "snap"})
    complete, missing = snapshot_completeness(incomplete)
    assert complete is False
    assert "review" in missing

    decisions, exclusions = evaluate_occurrences(
        [review()],
        [source_record()],
        [release(refs={"source_record": "snap"})],
        [manifest()],
    )
    assert decisions[0].eligible is False
    assert "normalization_release_not_committed_or_incomplete" in decisions[0].reasons
    assert exclusions[0]["logical_review_key"] == "r-1"


def test_occurrences_are_not_collapsed_by_logical_review_id():
    rows = [
        review(source_key="sr-1", row_index=0, review_id="same-review"),
        review(source_key="sr-2", row_index=1, review_id="same-review"),
    ]
    decisions, exclusions = evaluate_occurrences(
        rows,
        [source_record(key="sr-1"), source_record(row_index=1, key="sr-2")],
        [release()],
        [manifest()],
    )
    assert [item.eligible for item in decisions] == [True, True]
    assert exclusions == ()


def test_duplicate_invalid_and_non_tripadvisor_rows_are_excluded():
    duplicate = review()
    rows = [duplicate, duplicate, {**review(batch="b2"), "source_id": "foody"}]
    decisions, exclusions = evaluate_occurrences(
        rows,
        [source_record()],
        [release()],
        [manifest(), manifest(batch="b2", source="foody")],
    )
    assert sum(item.eligible for item in decisions) == 1
    assert any("duplicate_source_occurrence" in item.reasons for item in decisions)
    assert any("source_not_tripadvisor" in item.reasons for item in decisions)
    assert len(exclusions) == 2


def test_missing_lineage_and_unknown_provenance_are_fail_closed():
    row = {**review(), "source_record_sk": "missing", "review_text": ""}
    decisions, _ = evaluate_occurrences(
        [row],
        [],
        [release()],
        [{**manifest(), "source_file": "opaque/object.json"}],
    )
    assert decisions[0].eligible is False
    assert "missing_source_record" in decisions[0].reasons
    assert "provenance_not_permitted" in decisions[0].reasons
    assert "empty_review_text" in decisions[0].reasons


def test_report_is_deterministic_and_reconciles_enrichment(tmp_path):
    for name in CATALOG_FILES:
        (tmp_path / name).write_text(name, encoding="utf-8")
    args = dict(
        catalog_root=tmp_path,
        manifests=[
            {
                "ingested_at": datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc),
                **manifest(),
            }
        ],
        normalization_releases=[
            release()
        ],
        source_records=[source_record()],
        reviews=[review()],
        sentiment_rows=[
            {
                "source_record_sk": "sr-1",
                "batch_id": "b1",
                "review_id": "r-1",
                "source_row_index": 0,
                "processing_run_id": "enrich-1",
            }
        ],
        aspect_rows=[{"processing_run_id": "enrich-1"}],
        enrichment_runs=[
            {
                "processing_run_id": "enrich-1",
                "status": "committed",
                "expected_review_count": 1,
                "actual_review_count": 1,
                "expected_aspect_count": 1,
                "actual_aspect_count": 1,
            }
        ],
        index_runs=[
            {
                "index_generation": "index-1",
                "status": "committed",
                "expected_review_point_count": 1,
                "actual_review_point_count": 1,
                "expected_aspect_point_count": 1,
                "actual_aspect_point_count": 1,
            }
        ],
        qdrant_collections=[{"name": "travel_reviews_v1"}],
        runtime_status="verified",
    )
    first = build_inventory_report(**args)
    second = build_inventory_report(**args)
    assert first.to_json() == second.to_json()
    assert "2026-01-02T03:04:05+00:00" in first.to_json()
    assert first.status == "IMPLEMENTED"
    assert first.counts["eligible_occurrence_count"] == 1
    assert (
        first.reconciliation["coverage"]["enrichment_reconciliation"][0][
            "counts_reconcile"
        ]
        is True
    )


def test_empty_inventory_is_explicit(tmp_path):
    report = build_inventory_report(
        catalog_root=tmp_path, runtime_status="not_connected"
    )
    assert report.counts["bronze_manifest_count"] == 0
    assert report.counts["eligible_occurrence_count"] == 0
    assert report.status == "IMPLEMENTED / NOT VERIFIED"
    assert "runtime_metadata_not_verified" in report.blockers
