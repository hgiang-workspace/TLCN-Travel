from __future__ import annotations

import pytest

from travel_intelligence.qdrant.publication import (
    CorpusPublicationError,
    InMemoryCorpusPublicationLedger,
    PublicationConflict,
    build_corpus_plan,
    generation_eligibility_reasons,
    publication_token,
    transition_status,
    validate_committed_manifest,
)

MODEL = ("minilm", "revision-1", "model-fp", "config-fp")
COLLECTIONS = ("travel_reviews_v1", "travel_review_aspects_v1")
SNAPSHOTS = {
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


def normalization(release="release-1", *, complete=True):
    return {
        "run_id": release,
        "source_id": "tripadvisor",
        "status": "committed",
        "table_snapshot_refs": SNAPSHOTS if complete else {"review": "snapshot-review"},
    }


def enrichment(run="enrichment-1", release="release-1", *, status="committed"):
    return {
        "processing_run_id": run,
        "source_id": "tripadvisor",
        "source_release_id": release,
        "status": status,
        "expected_review_count": 1,
        "actual_review_count": 1,
        "expected_aspect_count": 1,
        "actual_aspect_count": 1,
    }


def generation(
    value="generation-1",
    *,
    start="2026-01-01T00:00:00",
    release="release-1",
    enrich_run="enrichment-1",
    status="committed",
    lineage=True,
    model=MODEL,
    review_points=1,
    aspect_points=1,
    provenance="synthetic",
):
    return {
        "index_generation": value,
        "source_id": "tripadvisor",
        "source_release_id": release,
        "enrichment_run_id": enrich_run,
        "embedding_model_id": model[0],
        "embedding_model_revision": model[1],
        "embedding_model_fingerprint": model[2],
        "embedding_config_fingerprint": model[3],
        "status": status,
        "expected_review_point_count": review_points,
        "actual_review_point_count": review_points,
        "expected_aspect_point_count": aspect_points,
        "actual_aspect_point_count": aspect_points,
        "review_collection": COLLECTIONS[0],
        "aspect_collection": COLLECTIONS[1],
        "started_at": start,
        "lineage_consistent": lineage,
        "provenance_class": provenance,
    }


def member(
    generation_id="generation-1",
    *,
    review_id="review-1",
    source_record="source-1",
    review_points=1,
    aspect_points=1,
):
    return {
        "source_id": "tripadvisor",
        "source_record_sk": source_record,
        "source_row_index": 0,
        "review_id": review_id,
        "review_ordinal": 0,
        "index_generation": generation_id,
        "source_release_id": "release-1",
        "enrichment_run_id": "enrichment-1",
        "review_point_count": review_points,
        "aspect_point_count": aspect_points,
    }


def plan(generations, members, *, base=None, qdrant=None, expected_model=None):
    return build_corpus_plan(
        generations,
        [normalization()],
        [enrichment()],
        members,
        base_corpus_id=base,
        qdrant_point_counts=qdrant
        or {
            row["index_generation"]: {
                "review": row["actual_review_point_count"],
                "aspect": row["actual_aspect_point_count"],
            }
            for row in generations
        },
        result_counts={"enrichment-1": {"review": 1, "aspect": 1}},
        expected_model=expected_model,
    )


def test_single_eligible_generation_has_deterministic_manifest():
    first = plan([generation()], [member()])
    second = plan([generation()], [member()])
    assert first.corpus_id == second.corpus_id
    assert first.generations == ("generation-1",)
    assert first.expected_review_point_count == 1
    assert first.expected_aspect_point_count == 1


def test_multiple_compatible_generations_form_one_union():
    rows = [generation(), generation("generation-2", start="2026-01-02T00:00:00")]
    members = [
        member(),
        member("generation-2", review_id="review-2", source_record="source-2"),
    ]
    result = plan(rows, members, expected_model=MODEL)
    assert result.generations == ("generation-1", "generation-2")
    assert len(result.members) == 2
    assert result.expected_review_point_count == 2


def test_duplicate_occurrence_uses_newest_generation_deterministically():
    rows = [generation(), generation("generation-2", start="2026-01-02T00:00:00")]
    result = plan(rows, [member(), member("generation-2")])
    assert result.generations == ("generation-2",)
    assert result.members[0].index_generation == "generation-2"
    assert any(
        item["reasons"] == ["all_occurrences_superseded"]
        for item in result.excluded_generations
    )


@pytest.mark.parametrize(
    "kwargs,reason",
    [
        ({"status": "failed"}, "index_generation_not_committed"),
        ({"status": "partial"}, "index_generation_not_committed"),
        ({"lineage": False}, "index_lineage_inconsistent"),
        ({"provenance": "unknown"}, "provenance_not_permitted"),
    ],
)
def test_invalid_generations_are_excluded(kwargs, reason):
    row = generation(**kwargs)
    result = plan([row], [member()])
    assert result.generations == ()
    assert reason in result.excluded_generations[0]["reasons"]


def test_snapshot_incomplete_and_historical_t04c_lineage_are_fail_closed():
    reasons = generation_eligibility_reasons(
        generation(lineage=False),
        [normalization(complete=False)],
        [enrichment()],
        qdrant_point_counts={"generation-1": {"review": 1, "aspect": 1}},
        result_counts={"enrichment-1": {"review": 1, "aspect": 1}},
    )
    assert "normalization_snapshot_incomplete" in reasons
    assert "index_lineage_inconsistent" in reasons


def test_invalid_upstream_lineage_is_excluded():
    row = generation(release="other-release")
    result = plan([row], [member()])
    assert (
        "normalization_release_missing_or_ambiguous"
        in result.excluded_generations[0]["reasons"]
    )


def test_mixed_model_configuration_is_excluded():
    rows = [
        generation(),
        generation("generation-2", model=("other", "rev", "fp", "cfg")),
    ]
    members = [
        member(),
        member("generation-2", review_id="review-2", source_record="source-2"),
    ]
    result = plan(rows, members, expected_model=MODEL)
    assert result.generations == ("generation-1",)
    assert (
        "incompatible_embedding_model_or_config"
        in result.excluded_generations[0]["reasons"]
    )


def test_missing_or_orphaned_qdrant_points_are_excluded():
    result = plan(
        [generation()],
        [member()],
        qdrant={"generation-1": {"review": 0, "aspect": 1}},
    )
    assert result.generations == ()
    assert (
        "qdrant_review_point_count_mismatch"
        in result.excluded_generations[0]["reasons"]
    )


def test_partial_publication_is_invisible_and_previous_corpus_survives():
    ledger = InMemoryCorpusPublicationLedger()
    first = plan([generation()], [member()])
    ledger.publish(first)
    second = plan(
        [generation("generation-2", start="2026-01-02T00:00:00")],
        [member("generation-2", review_id="review-2", source_record="source-2")],
        base=first.corpus_id,
    )
    ledger.manifests[second.corpus_id] = second.manifest(
        publication_token=publication_token(second.corpus_id, first.corpus_id),
        status="publishing",
    )
    assert ledger.active()["corpus_id"] == first.corpus_id


def test_publication_is_idempotent_and_recovery_resumes():
    ledger = InMemoryCorpusPublicationLedger()
    item = plan([generation()], [member()])
    token = publication_token(item.corpus_id, None)
    ledger.manifests[item.corpus_id] = item.manifest(
        publication_token=token, status="publishing"
    )
    ledger.memberships[item.corpus_id] = item.member_rows
    committed = ledger.publish(item, token=token)
    replayed = ledger.publish(item, token=token)
    assert committed == replayed
    assert ledger.active()["corpus_id"] == item.corpus_id


def test_concurrent_publication_conflict_requires_replanning():
    ledger = InMemoryCorpusPublicationLedger()
    first = plan([generation()], [member()])
    second = plan(
        [generation("generation-2", start="2026-01-02T00:00:00")],
        [member("generation-2", review_id="review-2", source_record="source-2")],
    )
    ledger.publish(first)
    with pytest.raises(PublicationConflict):
        ledger.publish(second)


def test_empty_eligible_corpus_can_be_published_without_points():
    empty = build_corpus_plan([], [], [], [], qdrant_point_counts={}, result_counts={})
    ledger = InMemoryCorpusPublicationLedger()
    committed = ledger.publish(empty)
    assert committed["member_count"] == 0
    assert ledger.active()["corpus_id"] == empty.corpus_id


def test_manifest_fingerprint_and_counts_are_reader_gates():
    item = plan([generation()], [member()])
    manifest = item.manifest(publication_token="token", status="committed")
    manifest["actual_review_point_count"] = 0
    with pytest.raises(CorpusPublicationError):
        validate_committed_manifest(manifest, item.member_rows)


def test_status_transitions_reject_invalid_rewrites():
    transition_status("planned", "publishing")
    transition_status("publishing", "publishing")
    transition_status("publishing", "committed")
    with pytest.raises(CorpusPublicationError):
        transition_status("committed", "failed")
