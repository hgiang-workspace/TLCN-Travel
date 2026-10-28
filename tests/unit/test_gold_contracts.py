from __future__ import annotations

from copy import deepcopy

import pytest

from travel_intelligence.gold.configuration import (
    GOLD_CONFIG_PATHS,
    load_gold_configuration,
)
from travel_intelligence.gold.contracts import (
    GOLD_TABLES,
    VALIDATION_NAMESPACE,
    contract_fingerprint,
    validation_ddl,
)
from travel_intelligence.gold.deduplication import (
    GoldDeduplicationError,
    reconcile_generations,
)
from travel_intelligence.gold.manifest import (
    CandidateRun,
    GoldManifestError,
    GoldRunStateError,
    build_gold_manifest,
    candidate_ids,
    committed_replay_is_noop,
    transition_status,
)

SOURCE_RELEASE = "release-1"
SILVER_TABLES = (
    "source_record",
    "place",
    "place_rating",
    "review",
    "source_entity_mapping",
    "normalization_run",
)


def normalization(*, status: str = "committed", complete: bool = True):
    refs = {name: f"snapshot-{name}" for name in SILVER_TABLES}
    if not complete:
        refs.pop("review")
    return {
        "run_id": SOURCE_RELEASE,
        "source_id": "tripadvisor",
        "bronze_batch_id": "batch-1",
        "normalization_version": "silver-foundation-v1",
        "status": status,
        "accepted_row_count": 2,
        "nested_input_count": 3,
        "table_snapshot_refs": refs,
    }


def enrichment(run_id: str = "run-1", *, status: str = "committed"):
    return {
        "processing_run_id": run_id,
        "source_id": "tripadvisor",
        "source_release_id": SOURCE_RELEASE,
        "model_id": "model-1",
        "model_fingerprint": "sha256:model",
        "config_fingerprint": "sha256:enrichment",
        "input_fingerprint": f"sha256:input-{run_id}",
        "status": status,
        "expected_review_count": 2,
        "actual_review_count": 2,
        "expected_aspect_count": 4,
        "actual_aspect_count": 4,
        "review_snapshot_id": f"review-snapshot-{run_id}",
        "aspect_snapshot_id": f"aspect-snapshot-{run_id}",
        "started_at": "2026-01-01T00:00:00Z",
    }


def qdrant():
    return {
        "index_generation": "generation-1",
        "source_release_id": SOURCE_RELEASE,
        "status": "committed",
        "input_manifest_id": "qdrant-input-1",
        "input_manifest_sha256": "sha256:qdrant",
        "expected_input_review_count": 2,
        "expected_input_aspect_count": 4,
        "actual_review_point_count": 3,
        "actual_aspect_point_count": 4,
    }


def manifest(runs=None, generation=None, *, include_qdrant=True):
    return build_gold_manifest(
        normalization(),
        runs or [enrichment()],
        source_id="tripadvisor",
        source_release_id=SOURCE_RELEASE,
        contract_fingerprint=contract_fingerprint(),
        config_fingerprint="sha256:gold-config",
        qdrant_generation=(qdrant() if generation is None else generation)
        if include_qdrant
        else None,
    )


def test_contract_has_expected_objects_and_no_entity_profile_table():
    names = [table.name for table in GOLD_TABLES]
    assert len(names) == 15
    assert len(names) == len(set(names))
    assert "dim_entity" in names
    assert "gold_build_run" in names
    assert "entity_profile" not in names
    assert contract_fingerprint().startswith("sha256:")


def test_validation_ddl_is_isolated_and_uses_iceberg_v2():
    statements = validation_ddl()
    assert statements[0] == (
        f"CREATE NAMESPACE IF NOT EXISTS travel.{VALIDATION_NAMESPACE}"
    )
    assert len(statements) == len(GOLD_TABLES) + 1
    assert all(
        f"travel.{VALIDATION_NAMESPACE}." in sql
        and "USING iceberg" in sql
        and "'format-version'='2'" in sql
        for sql in statements[1:]
    )
    assert all("travel.gold." not in sql for sql in statements)


def test_gold_configuration_loads_and_fingerprint_is_stable():
    first, first_fp = load_gold_configuration()
    second, second_fp = load_gold_configuration()
    assert tuple(first) == GOLD_CONFIG_PATHS
    assert first == second
    assert first_fp == second_fp
    assert first_fp.startswith("sha256:")
    thresholds = first[GOLD_CONFIG_PATHS[2]]["policy"]
    assert thresholds["sample_threshold"] is None
    assert thresholds["freshness_threshold_days"] is None


def test_manifest_is_order_independent_and_deterministic():
    first = manifest([enrichment("b"), enrichment("a")], include_qdrant=False)
    second = manifest([enrichment("a"), enrichment("b")], include_qdrant=False)
    assert first == second
    assert first["manifest_id"].startswith("gold-input-")
    assert first["manifest_sha256"].startswith("sha256:")
    assert first["enrichment"]["run_count"] == 2
    assert first["enrichment"]["review_count"] == 4
    assert first["enrichment"]["aspect_count"] == 8
    assert candidate_ids(first) == candidate_ids(second)


@pytest.mark.parametrize(
    "normalization_row,runs,error",
    [
        (normalization(status="partial"), [enrichment()], "not committed"),
        (normalization(complete=False), [enrichment()], "incomplete"),
        (normalization(), [enrichment(status="partial")], "not committed"),
    ],
)
def test_manifest_rejects_ineligible_runs(normalization_row, runs, error):
    with pytest.raises(GoldManifestError, match=error):
        build_gold_manifest(
            normalization_row,
            runs,
            source_id="tripadvisor",
            source_release_id=SOURCE_RELEASE,
            contract_fingerprint=contract_fingerprint(),
            config_fingerprint="sha256:gold-config",
        )


def test_manifest_rejects_mixed_lineage_and_count_mismatch():
    mixed = enrichment("run-2")
    mixed["model_fingerprint"] = "sha256:other"
    with pytest.raises(GoldManifestError, match="mixed model/config"):
        manifest([enrichment(), mixed], include_qdrant=False)
    mismatch = enrichment()
    mismatch["actual_review_count"] = 1
    with pytest.raises(GoldManifestError, match="count mismatch"):
        manifest([mismatch], include_qdrant=False)


def test_manifest_rejects_qdrant_mismatch():
    generation = qdrant()
    generation["expected_input_review_count"] = 1
    with pytest.raises(GoldManifestError, match="Qdrant review"):
        manifest(generation=generation)


def review_generation(
    run_id: str, *, sentiment: str = "positive", confidence: float = 0.9
):
    return {
        "processing_run_id": run_id,
        "review_sentiment_id": "sentiment-1",
        "review_id": "review-1",
        "source_id": "tripadvisor",
        "source_release_id": SOURCE_RELEASE,
        "source_record_sk": "record-1",
        "review_text_hash": "text-hash",
        "sentiment_label": sentiment,
        "confidence": confidence,
        "model_id": "model-1",
        "model_fingerprint": "sha256:model",
        "config_fingerprint": "sha256:enrichment",
    }


def aspect_generation(run_id: str, *, sentiment: str = "positive"):
    return {
        "processing_run_id": run_id,
        "aspect_sentiment_id": "aspect-1",
        "review_sentiment_id": "sentiment-1",
        "review_id": "review-1",
        "source_id": "tripadvisor",
        "source_release_id": SOURCE_RELEASE,
        "source_record_sk": "record-1",
        "segment_ordinal": 0,
        "segment_start_char": 0,
        "segment_end_char": 4,
        "aspect": "service",
        "segment_text_hash": "segment-hash",
        "sentiment_label": sentiment,
        "confidence": 0.9,
        "model_id": "model-1",
        "model_fingerprint": "sha256:model",
        "config_fingerprint": "sha256:enrichment",
    }


def test_generation_dedup_prefers_frozen_generation_and_records_supersession():
    result = reconcile_generations(
        [review_generation("historical"), review_generation("frozen")],
        [aspect_generation("historical"), aspect_generation("frozen")],
        frozen_processing_run_ids=("frozen",),
    )
    assert result["raw_candidate_review_count"] == 2
    assert result["effective_review_count"] == 1
    assert result["raw_candidate_aspect_count"] == 2
    assert result["effective_aspect_count"] == 1
    assert result["selected_reviews"][0]["processing_run_id"] == "frozen"
    assert result["superseded_generation_counts"] == {"review": 1, "aspect": 1}


@pytest.mark.parametrize(
    "review_change,aspect_change", [("negative", None), (None, "negative")]
)
def test_generation_dedup_fails_closed_on_conflicting_predictions(
    review_change, aspect_change
):
    reviews = [review_generation("frozen"), review_generation("historical")]
    aspects = [aspect_generation("frozen"), aspect_generation("historical")]
    if review_change:
        reviews[1]["sentiment_label"] = review_change
    if aspect_change:
        aspects[1]["sentiment_label"] = aspect_change
    with pytest.raises(GoldDeduplicationError, match="conflicting"):
        reconcile_generations(reviews, aspects, frozen_processing_run_ids=("frozen",))


def test_manifest_reconciles_qdrant_to_effective_not_raw_counts():
    runs = [enrichment("a"), enrichment("b")]
    evidence = reconcile_generations(
        [review_generation("a"), review_generation("b")],
        [aspect_generation("a"), aspect_generation("b")],
        frozen_processing_run_ids=("b",),
    )
    evidence["raw_candidate_review_count"] = 4
    evidence["raw_candidate_aspect_count"] = 8
    generation = qdrant()
    generation["expected_input_review_count"] = 1
    generation["expected_input_aspect_count"] = 1
    generated = build_gold_manifest(
        normalization(),
        runs,
        source_id="tripadvisor",
        source_release_id=SOURCE_RELEASE,
        contract_fingerprint=contract_fingerprint(),
        config_fingerprint="sha256:gold-config",
        qdrant_generation=generation,
        deduplication=evidence,
    )
    assert generated["enrichment"]["raw_candidate_review_count"] == 4
    assert generated["enrichment"]["effective_review_count"] == 1


def test_manifest_pins_a_separate_versioned_geography_snapshot():
    generated = manifest(include_qdrant=False)
    with_geo = build_gold_manifest(
        normalization(),
        [enrichment()],
        source_id="tripadvisor",
        source_release_id=SOURCE_RELEASE,
        contract_fingerprint=contract_fingerprint(),
        config_fingerprint="sha256:gold-config",
        geography_snapshot={
            "table": "travel.silver.place",
            "snapshot_id": "snapshot-geo",
            "taxonomy_version": "province-rules-v1.2",
            "taxonomy_fingerprint": "sha256:taxonomy",
        },
    )
    assert with_geo["geography_snapshot"]["snapshot_id"] == "snapshot-geo"
    assert with_geo["manifest_id"] != generated["manifest_id"]


def test_legal_run_transitions_and_complete_validation():
    run = CandidateRun(manifest())
    run.transition("building")
    run.record_snapshot("dim_entity", "snapshot-1")
    run.validate(("dim_entity",))
    run.commit(("dim_entity",))
    assert run.status == "committed"


def test_illegal_transition_and_incomplete_candidate_fail_closed():
    with pytest.raises(GoldRunStateError, match="illegal"):
        transition_status("planned", "committed")
    run = CandidateRun(manifest())
    run.transition("building")
    with pytest.raises(GoldRunStateError, match="incomplete"):
        run.validate(("dim_entity",))


def test_commit_revalidates_required_snapshots_and_cannot_be_bypassed():
    run = CandidateRun(manifest())
    run.transition("building")
    run.record_snapshot("dim_entity", "snapshot-1")
    run.validate(("dim_entity",))
    run.output_snapshot_refs.clear()
    with pytest.raises(GoldRunStateError, match="incomplete"):
        run.commit(("dim_entity",))
    with pytest.raises(GoldRunStateError, match=r"use commit\(\)"):
        run.transition("committed")


def test_partial_run_can_resume_but_failed_and_committed_are_terminal():
    assert transition_status("building", "partial") == "partial"
    assert transition_status("partial", "building") == "building"
    with pytest.raises(GoldRunStateError):
        transition_status("failed", "building")
    with pytest.raises(GoldRunStateError):
        transition_status("committed", "building")


def test_committed_replay_requires_exact_contract_and_live_snapshots():
    item = manifest()
    run_id, release_id = candidate_ids(item)
    existing = {
        "gold_run_id": run_id,
        "release_id": release_id,
        "input_manifest_id": item["manifest_id"],
        "input_manifest_sha256": item["manifest_sha256"],
        "contract_fingerprint": item["contract_fingerprint"],
        "config_fingerprint": item["config_fingerprint"],
        "status": "committed",
        "output_snapshot_refs": {"dim_entity": "snapshot-1"},
    }
    assert committed_replay_is_noop(
        existing,
        manifest=item,
        required_tables=("dim_entity",),
        snapshot_exists=lambda table, snapshot: (
            (table, snapshot) == ("dim_entity", "snapshot-1")
        ),
    )
    conflict = deepcopy(existing)
    conflict["config_fingerprint"] = "sha256:other"
    with pytest.raises(GoldManifestError, match="config_fingerprint"):
        committed_replay_is_noop(
            conflict,
            manifest=item,
            required_tables=("dim_entity",),
            snapshot_exists=lambda *_: True,
        )
