import pytest

from travel_intelligence.common.configuration import (
    ConfigurationError,
    configuration_fingerprint,
    default_config_paths,
    load_validated,
    resolve_config_path,
)
from travel_intelligence.sentiment.contracts import (
    REVIEW_ASPECT_SENTIMENT_TABLE,
    REVIEW_SENTIMENT_TABLE,
    aspect_sentiment_id,
    build_aspect_sentiment_key,
    review_sentiment_id,
    text_hash,
)
from travel_intelligence.silver.tripadvisor_source import (
    TRIPADVISOR_REQUIRED_RELEASE_TABLES,
    select_tripadvisor_reviews,
    validate_tripadvisor_release,
)


def _committed_run(**overrides):
    run = {
        "source_id": "tripadvisor",
        "status": "committed",
        "table_snapshot_refs": {
            table: f"snapshot-{table}" for table in TRIPADVISOR_REQUIRED_RELEASE_TABLES
        },
    }
    run.update(overrides)
    return run


def _review(**overrides):
    row = {
        "review_id": "review-1",
        "review_ordinal": 0,
        "review_text": "Rất đẹp",
        "source_record_sk": "source-sk-1",
        "source_id": "tripadvisor",
        "batch_id": "batch-1",
        "source_row_index": 7,
        "source_record_id": "ta-1",
        "place_id": "place-1",
        "source_uri": "https://example.test/place/1",
    }
    row.update(overrides)
    return row


def test_all_root_configs_load_from_any_cwd_and_preserve_taxonomy():
    paths = default_config_paths()
    assert set(paths) == {
        "pipeline", "aspect_keywords", "visobert", "embedding", "qdrant"
    }
    for kind, path in paths.items():
        value, fingerprint = load_validated(kind, path)
        assert fingerprint.startswith("sha256:")
        assert value
    aspect, _ = load_validated("aspect_keywords", paths["aspect_keywords"])
    assert tuple(aspect["aspects"]) == (
        "food", "service", "ambiance", "cleanliness", "price", "location", "speed"
    )
    assert aspect["matching"]["prefer_longest_match"] is False


def test_config_fingerprint_is_order_independent():
    assert configuration_fingerprint({"b": 2, "a": 1}) == configuration_fingerprint(
        {"a": 1, "b": 2}
    )


def test_invalid_config_and_missing_path_are_rejected():
    missing = "configs/missing.yaml"
    with pytest.raises(ConfigurationError, match="does not exist"):
        resolve_config_path(missing)
    with pytest.raises(ConfigurationError, match="required key"):
        load_validated("pipeline", "configs/models/visobert.yaml")


def test_source_contract_requires_complete_committed_evidence():
    with pytest.raises(ValueError, match="not committed and complete"):
        validate_tripadvisor_release(
            _committed_run(status="partial", table_snapshot_refs=None)
        )
    incomplete = _committed_run()
    del incomplete["table_snapshot_refs"]["review"]
    with pytest.raises(ValueError, match="not committed and complete"):
        validate_tripadvisor_release(incomplete)
    with pytest.raises(ValueError, match="not a TripAdvisor"):
        validate_tripadvisor_release(_committed_run(source_id="foody"))


def test_tripadvisor_filter_preserves_nested_lineage_and_replay_key():
    rows = [
        _review(),
        _review(),  # duplicate join occurrence; same logical review key
        _review(review_id="review-2", review_ordinal=1, review_text=""),
        _review(source_id="foody", review_id="foody-1"),
        _review(batch_id="uncommitted"),
    ]
    selected = select_tripadvisor_reviews(
        rows,
        {"batch-1": _committed_run(), "uncommitted": _committed_run(status="failed")},
    )
    assert len(selected) == 1
    assert selected[0].source_record_sk == "source-sk-1"
    assert selected[0].source_row_index == 7
    assert selected[0].review_ordinal == 0


def test_missing_review_lineage_is_rejected():
    row = _review()
    del row["source_row_index"]
    with pytest.raises(ValueError, match="occurrence lineage"):
        select_tripadvisor_reviews([row], {"batch-1": _committed_run()})


def test_sentiment_and_aspect_ids_are_deterministic_and_nested():
    review_id = review_sentiment_id("review-1", "model-v1", "config-v1")
    assert review_id == review_sentiment_id("review-1", "model-v1", "config-v1")
    assert review_id != review_sentiment_id("review-1", "model-v2", "config-v1")
    segment = build_aspect_sentiment_key(
        parent_review_sentiment_id=review_id,
        segment_ordinal=0,
        aspect="ambiance",
        matched_keyword="đẹp",
        segment_text="Rất đẹp",
        model_fingerprint="model-v1",
        config_fingerprint="config-v1",
    )
    assert segment.startswith("sha256:")
    assert segment == aspect_sentiment_id(
        review_id, 0, "ambiance", "đẹp", text_hash("Rất đẹp"), "model-v1", "config-v1"
    )
    assert "review_sentiment_id STRING NOT NULL" in REVIEW_ASPECT_SENTIMENT_TABLE
    assert "review_sentiment_id STRING NOT NULL" in REVIEW_SENTIMENT_TABLE


def test_contract_sql_is_tripadvisor_committed_only_and_uses_correct_batch_join():
    from travel_intelligence.silver.tripadvisor_source import TRIPADVISOR_REVIEW_SQL

    assert "sr.source_id = 'tripadvisor'" in TRIPADVISOR_REVIEW_SQL
    assert "cb.bronze_batch_id = sr.batch_id" in TRIPADVISOR_REVIEW_SQL
    assert "status = 'committed'" in TRIPADVISOR_REVIEW_SQL
    assert "social_comment" not in TRIPADVISOR_REVIEW_SQL
    assert "foody" not in TRIPADVISOR_REVIEW_SQL
