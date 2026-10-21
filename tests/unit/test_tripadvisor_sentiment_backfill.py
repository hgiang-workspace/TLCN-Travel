from datetime import datetime

import pytest

from travel_intelligence.backfill.sentiment import (
    VISOBERT_MODEL_ID,
    execute_sentiment_plan,
    existing_enrichment_command,
    plan_sentiment_backfill,
)
from travel_intelligence.sentiment.contracts import text_hash
from travel_intelligence.silver.tripadvisor_source import (
    TRIPADVISOR_REQUIRED_RELEASE_TABLES,
    TripAdvisorReviewOccurrence,
)

CONFIG = "sha256:config"


def occurrence(
    review_id="review-1",
    source_record_sk="source-1",
    source_row_index=1,
    review_ordinal=0,
    batch_id="batch-1",
    release_id="release-1",
    source_id="tripadvisor",
):
    return TripAdvisorReviewOccurrence(
        review_id=review_id,
        review_ordinal=review_ordinal,
        review_text="Món ăn ngon và không gian đẹp.",
        source_record_sk=source_record_sk,
        source_id=source_id,
        batch_id=batch_id,
        source_row_index=source_row_index,
        source_record_id="record-1",
        place_id="place-1",
        source_uri="https://example.test/1",
        source_release_id=release_id,
    )


def manifest(batch_id="batch-1", **overrides):
    value = {
        "batch_id": batch_id,
        "source_id": "tripadvisor",
        "status": "success",
        "contract_version": "bronze-contract-v2",
        "schema_version": "bronze-v2",
        "row_count": 1,
        "payload_hashes": ["hash"],
        "input_sha256": "sha256:fixture",
    }
    value.update(overrides)
    return value


def release(batch_id="batch-1", release_id="release-1", **overrides):
    value = {
        "run_id": release_id,
        "source_id": "tripadvisor",
        "bronze_batch_id": batch_id,
        "status": "committed",
        "table_snapshot_refs": {
            table: f"snapshot-{table}" for table in TRIPADVISOR_REQUIRED_RELEASE_TABLES
        },
    }
    value.update(overrides)
    return value


def enrichment_run(
    processing_run_id="enrich-1",
    release_id="release-1",
    *,
    status="committed",
    model_id=VISOBERT_MODEL_ID,
    config_fingerprint=CONFIG,
    model_fingerprint="sha256:model",
    expected_review_count=1,
    expected_aspect_count=0,
):
    return {
        "processing_run_id": processing_run_id,
        "source_id": "tripadvisor",
        "source_release_id": release_id,
        "status": status,
        "model_id": model_id,
        "model_fingerprint": model_fingerprint,
        "config_fingerprint": config_fingerprint,
        "expected_review_count": expected_review_count,
        "actual_review_count": expected_review_count,
        "expected_aspect_count": expected_aspect_count,
        "actual_aspect_count": expected_aspect_count,
        "review_snapshot_id": "review-snapshot",
        "aspect_snapshot_id": "aspect-snapshot",
        "started_at": datetime(2026, 1, 1),
    }


def sentiment_row(item=occurrence(), processing_run_id="enrich-1", **overrides):
    value = {
        "review_sentiment_id": "sentiment-1",
        "review_id": item.review_id,
        "source_record_sk": item.source_record_sk,
        "source_id": item.source_id,
        "batch_id": item.batch_id,
        "source_row_index": item.source_row_index,
        "review_text_hash": text_hash(item.review_text),
        "model_id": VISOBERT_MODEL_ID,
        "config_fingerprint": CONFIG,
        "processing_run_id": processing_run_id,
        "source_release_id": item.source_release_id,
    }
    value.update(overrides)
    return value


def plan(**overrides):
    values = {
        "occurrences": [occurrence()],
        "manifests": [manifest()],
        "normalization_runs": [release()],
        "enrichment_runs": [],
        "sentiment_rows": [],
        "aspect_rows": [],
        "config_fingerprint": CONFIG,
        "synthetic_input_hashes": ["sha256:fixture"],
    }
    values.update(overrides)
    return plan_sentiment_backfill(**values)


def test_tripadvisor_only_and_missing_selection_preserve_occurrence_identity():
    result = plan(
        occurrences=[occurrence(), occurrence(source_id="foody", review_id="f-1")]
    )
    by_id = {item.review_id: item for item in result.decisions}
    assert by_id["review-1"].status == "missing"
    assert by_id["review-1"].occurrence_key == ("source-1", "batch-1", "review-1", 1, 0)
    assert by_id["f-1"].status == "excluded"
    assert "source_not_tripadvisor" in by_id["f-1"].reasons


def test_snapshot_incomplete_and_unknown_provenance_are_excluded():
    incomplete = release(table_snapshot_refs={"source_record": "snapshot"})
    result = plan(normalization_runs=[incomplete])
    assert result.decisions[0].status == "excluded"
    assert (
        "normalization_release_not_committed_or_incomplete"
        in result.decisions[0].reasons
    )
    result = plan(manifests=[manifest(input_sha256="sha256:unknown")])
    assert result.decisions[0].status == "excluded"
    assert "provenance_not_permitted" in result.decisions[0].reasons


def test_compatible_committed_enrichment_is_skipped():
    item = occurrence()
    result = plan(
        enrichment_runs=[enrichment_run()],
        sentiment_rows=[sentiment_row(item)],
    )
    assert result.decisions[0].status == "already_enriched"
    assert result.decisions[0].compatible_processing_run_id == "enrich-1"
    assert result.to_dict()["counts"]["missing_enrichment_count"] == 0


@pytest.mark.parametrize(
    "run_overrides, row_overrides, expected",
    [
        ({"status": "partial"}, {}, "enrichment_run_not_committed"),
        ({"config_fingerprint": "sha256:other"}, {}, "config_fingerprint_mismatch"),
        (
            {},
            {"review_text_hash": "sha256:other"},
            "enrichment_result_lineage_or_text_mismatch",
        ),
    ],
)
def test_invalid_historical_coverage_is_not_reused(
    run_overrides, row_overrides, expected
):
    result = plan(
        enrichment_runs=[enrichment_run(**run_overrides)],
        sentiment_rows=[sentiment_row(**row_overrides)],
    )
    assert result.decisions[0].status == "excluded"
    assert expected in result.decisions[0].reasons


def test_historical_ledger_result_count_mismatch_is_reported():
    result = plan(
        enrichment_runs=[enrichment_run(processing_run_id="old-run")],
        sentiment_rows=[],
    )
    assert result.to_dict()["invalid_historical_generation_count"] == 1
    assert (
        result.to_dict()["invalid_historical_runs"][0]["processing_run_id"] == "old-run"
    )


def test_deterministic_order_bounded_execution_and_authorization():
    first = occurrence(review_id="a", source_record_sk="a")
    second = occurrence(review_id="b", source_record_sk="b", source_row_index=2)
    result = plan(occurrences=[second, first], max_occurrences=1)
    assert result.selected_occurrence_keys == (result.decisions[0].occurrence_key,)
    with pytest.raises(PermissionError):
        execute_sentiment_plan(result, confirm_writes=False, runner=lambda item: None)
    calls = []
    executed = execute_sentiment_plan(
        result,
        confirm_writes=True,
        runner=lambda item: calls.append(item.occurrence_key),
    )
    assert len(calls) == 1
    assert executed.status == "completed"


def test_model_revision_is_pinned_and_existing_driver_is_invoked():
    with pytest.raises(ValueError, match="pinned ViSoBERT"):
        plan(model_revision="other")
    decision = plan().decisions[0]
    command = existing_enrichment_command(decision, script="enrich.py")
    assert command[:4] == ["python", "enrich.py", "--source-release-id", "release-1"]
    assert "--occurrence-key" in command
