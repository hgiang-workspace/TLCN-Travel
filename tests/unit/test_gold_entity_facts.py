from __future__ import annotations

import pytest

from travel_intelligence.gold.entity_facts import GoldFactError, derive_entity_facts


def review(review_id: str, label: str = "positive"):
    return {
        "review_sentiment_id": f"sentiment-{review_id}",
        "review_id": review_id,
        "source_id": "tripadvisor",
        "source_release_id": "silver-release",
        "source_record_sk": f"record-{review_id}",
        "place_id": "entity-1",
        "sentiment_label": label,
        "confidence": 0.8,
    }


def aspect(review_id: str, aspect_name: str = "service"):
    return {
        "aspect_sentiment_id": f"aspect-{review_id}-{aspect_name}",
        "review_sentiment_id": f"sentiment-{review_id}",
        "review_id": review_id,
        "source_id": "tripadvisor",
        "source_release_id": "silver-release",
        "source_record_sk": f"record-{review_id}",
        "place_id": "entity-1",
        "aspect": aspect_name,
        "sentiment_label": "positive",
        "confidence": 0.8,
    }


def derive(reviews, aspects):
    return derive_entity_facts(
        reviews,
        aspects,
        entity_by_id={"entity-1": {"entity_sk": "sk-1"}},
        release_id="release-1",
        gold_run_id="run-1",
        contract_version="gold-contract-v1",
        config_fingerprint="sha256:config",
        source_release_id="silver-release",
        reporting_period="silver-release",
        model_id="model",
        model_fingerprint="sha256:model",
        enrichment_config_fingerprint="sha256:enrichment",
        built_at="2026-01-01T00:00:00Z",
    )


def test_sentiment_and_aspect_counts_keep_distinct_denominators():
    result = derive(
        [review("1", "positive"), review("2", "negative")],
        [aspect("1"), aspect("1", "price"), aspect("2")],
    )
    sentiment = {
        row["sentiment_label"]: row for row in result["fact_entity_sentiment_summary"]
    }
    assert sentiment["positive"]["eligible_review_count"] == 2
    assert sentiment["positive"]["sentiment_percentage"] == 50
    service = next(
        row
        for row in result["fact_entity_aspect_summary"]
        if row["aspect"] == "service"
    )
    assert service["mention_count"] == 2
    assert service["distinct_review_count"] == 2
    assert service["review_percentage"] == 100


def test_fact_derivation_fails_for_duplicate_review_or_unlinked_aspect():
    with pytest.raises(GoldFactError, match="duplicate"):
        derive([review("1"), review("1")], [])
    dangling = aspect("1")
    with pytest.raises(GoldFactError, match="parent"):
        derive([review("2")], [dangling])


def test_fact_derivation_fails_for_invalid_label_and_confidence():
    invalid = review("1", "mixed")
    with pytest.raises(GoldFactError, match="unsupported"):
        derive([invalid], [])
    invalid = review("1")
    invalid["confidence"] = 1.1
    with pytest.raises(GoldFactError, match="outside"):
        derive([invalid], [])
