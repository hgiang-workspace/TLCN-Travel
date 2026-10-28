from __future__ import annotations

import pytest

from travel_intelligence.gold.domain_mapping import load_domain_mapping_contract
from travel_intelligence.gold.market_facts import (
    MarketFactError,
    derive_market_and_coverage_facts,
)


def _context():
    return {
        "release_id": "gold-release-test",
        "gold_run_id": "run-test",
        "contract_version": "gold-contract-v1",
        "config_fingerprint": "sha256:test",
        "source_release_id": "tripadvisor-release-test",
        "built_at": "2026-10-28T00:00:00Z",
        "source_id": "tripadvisor",
        "source_sk": "source-tripadvisor",
        "reporting_period": "tripadvisor-release-test",
        "processed_at": "2026-10-28T00:00:00Z",
        "domain_sks": {
            "destination": "domain-destination",
            "food_dining": "domain-food",
            "transportation": "domain-transportation",
        },
        "geography_sks": {"vn-province:test": "geo-test"},
    }


def _inputs():
    entities = [
        {"entity_id": "spa", "entity_type_raw": "Spa"},
        {
            "entity_id": "mixed",
            "entity_type_raw": "Chuyến tham quan nhiều ngày • Taxi & xe đưa đón",
        },
        {"entity_id": "excluded", "entity_type_raw": "Cho thuê tiện ích"},
        {"entity_id": "unreviewed", "entity_type_raw": "Bãi biển"},
    ]
    sources = [
        {"entity_id": row["entity_id"], "source_id": "tripadvisor"}
        for row in entities
    ]
    geographies = [
        {
            "entity_id": row["entity_id"],
            "geo_id": "vn-province:test",
            "geography_sk": "geo-test",
        }
        for row in entities
    ]
    sentiment = [
        {
            "entity_id": "spa",
            "sentiment_label": "positive",
            "sentiment_count": 1,
            "eligible_review_count": 2,
        },
        {
            "entity_id": "spa",
            "sentiment_label": "neutral",
            "sentiment_count": 1,
            "eligible_review_count": 2,
        },
    ]
    aspects = [
        {"entity_id": "spa", "mention_count": 2},
        {"entity_id": "spa", "mention_count": 1},
    ]
    return entities, sources, geographies, sentiment, aspects


def _derive(*, geographies=None):
    entities, sources, default_geographies, sentiment, aspects = _inputs()
    return derive_market_and_coverage_facts(
        entities,
        sources,
        default_geographies if geographies is None else geographies,
        sentiment,
        aspects,
        load_domain_mapping_contract(),
        context=_context(),
    )


def test_coverage_retains_excluded_unreviewed_and_entities_without_evidence():
    facts = _derive()
    coverage = facts["fact_data_coverage"]
    assert len(coverage) == 1
    row = coverage[0]
    assert row["observed_entity_count"] == 4
    assert row["mapped_entity_count"] == 2
    assert row["excluded_entity_count"] == 1
    assert row["unreviewed_or_unmapped_entity_count"] == 1
    assert row["entity_count_with_sentiment_evidence"] == 1
    assert row["entity_count_with_aspect_evidence"] == 1


def test_market_aggregates_independent_evidence_without_fact_join_multiplication():
    facts = _derive()
    rows = facts["fact_market_summary"]
    by_metric = {}
    for row in rows:
        metric_id = row["metric_id"]
        by_metric[metric_id] = by_metric.get(metric_id, 0) + row["numerator"]
    assert by_metric["entity_count"] == 2
    assert by_metric["entity_count_with_sentiment_evidence"] == 1
    assert by_metric["entity_count_with_aspect_evidence"] == 1
    assert by_metric["review_evidence_count"] == 2
    assert by_metric["positive_review_count"] == 1
    assert by_metric["neutral_review_count"] == 1
    assert by_metric["aspect_mention_count"] == 3
    assert len({row["market_summary_fact_sk"] for row in rows}) == len(rows)


def test_null_geography_is_retained_in_coverage_and_omitted_from_domain_market():
    entities, _, geographies, _, _ = _inputs()
    geographies[0] = {
        "entity_id": entities[0]["entity_id"],
        "geo_id": None,
        "geography_sk": None,
    }
    facts = _derive(geographies=geographies)
    assert sum(row["observed_entity_count"] for row in facts["fact_data_coverage"]) == 4
    assert sum(
        row["numerator"]
        for row in facts["fact_market_summary"]
        if row["metric_id"] == "entity_count"
    ) == 1


def test_rejects_duplicate_or_incomplete_target_bridge_grains():
    _, sources, geographies, _, _ = _inputs()
    with pytest.raises(MarketFactError, match="source bridge"):
        _derive_with_bridges(sources + [sources[0]], geographies)
    with pytest.raises(MarketFactError, match="geography bridge"):
        _derive_with_bridges(sources, geographies[:-1])


def test_aggregation_is_deterministic_for_repeated_inputs():
    assert _derive() == _derive()


def _derive_with_bridges(sources, geographies):
    entities, _, _, sentiment, aspects = _inputs()
    return derive_market_and_coverage_facts(
        entities, sources, geographies, sentiment, aspects,
        load_domain_mapping_contract(), context=_context(),
    )
