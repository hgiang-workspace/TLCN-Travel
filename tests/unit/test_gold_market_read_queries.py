from __future__ import annotations

import pytest

from travel_intelligence.gold.market_facts import METRICS
from travel_intelligence.gold.market_read_queries import (
    DATA_COVERAGE_QUERY,
    DOMAIN_ANALYSIS_QUERY,
    MARKET_METRIC_DEFINITIONS,
    MARKET_OVERVIEW_QUERY,
    province_comparison_query,
)


def test_metric_inventory_exactly_matches_persisted_metric_contract():
    definitions = {metric.metric_id: metric for metric in MARKET_METRIC_DEFINITIONS}
    assert tuple(definitions) == METRICS
    assert definitions["review_evidence_count"].unit == "reviews"
    assert definitions["aspect_mention_count"].unit == "aspect_mentions"
    assert definitions["positive_review_count"].denominator_metric_id == (
        "review_evidence_count"
    )
    assert all(metric.additivity == "semi_additive" for metric in definitions.values())


@pytest.mark.parametrize(
    "query",
    (MARKET_OVERVIEW_QUERY, DOMAIN_ANALYSIS_QUERY, DATA_COVERAGE_QUERY),
)
def test_static_queries_are_parameter_only_and_release_scoped(query):
    assert query.sql.count("?") == len(query.parameters)
    assert query.parameters == ("release_id", "source_id", "reporting_period")
    assert "WHERE" in query.sql
    assert ".release_id = r.release_id" in query.sql
    assert "gold_build_run" not in query.sql
    assert "snapshot" not in query.sql.lower()


def test_overview_keeps_market_and_coverage_aggregations_independent():
    sql = MARKET_OVERVIEW_QUERY.sql
    assert "coverage AS (" in sql
    assert "market AS (" in sql
    assert "FROM coverage CROSS JOIN market" in sql
    assert "fact_entity_sentiment_summary" not in sql
    assert "fact_entity_aspect_summary" not in sql
    assert "NULLIF(collected_entity_count, 0)" in sql


def test_domain_query_sums_counts_and_never_averages_ratios():
    sql = DOMAIN_ANALYSIS_QUERY.sql
    assert "SUM(IF(m.metric_id = 'entity_count'" in sql
    assert "AVG(" not in sql
    assert "metric_value" not in sql
    assert "GROUP BY m.domain_id" in sql


def test_coverage_query_recomputes_ratios_from_counts():
    sql = DATA_COVERAGE_QUERY.sql
    assert "c.mapped_entity_ratio" not in sql
    assert "CAST(c.mapped_entity_count AS DOUBLE)" in sql
    assert "NULLIF(c.observed_entity_count, 0)" in sql


def test_province_query_uses_bounded_placeholders_and_one_metric():
    query = province_comparison_query(3)
    assert query.sql.count("?") == len(query.parameters) == 8
    assert query.parameters[-3:] == (
        "province_id[0]",
        "province_id[1]",
        "province_id[2]",
    )
    assert "m.metric_id = ?" in query.sql
    assert "m.geo_id IN (?, ?, ?)" in query.sql
    with pytest.raises(ValueError, match="between 1 and 20"):
        province_comparison_query(0)
    with pytest.raises(ValueError, match="between 1 and 20"):
        province_comparison_query(21)
