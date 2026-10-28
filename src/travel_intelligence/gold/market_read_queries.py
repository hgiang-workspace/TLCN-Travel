"""Read-only Trino query contracts for candidate Gold market analytics.

The SQL uses DB-API qmark parameters. Callers must bind values in ``parameters``
order and must resolve an allowed release before execution. Nothing in this
module selects or publishes a release, opens a connection, or writes data.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from travel_intelligence.gold.market_facts import METRICS

Additivity = Literal["additive", "semi_additive", "non_additive"]


@dataclass(frozen=True)
class MarketMetricDefinition:
    metric_id: str
    meaning: str
    source_measure: str
    unit: str
    additivity: Additivity
    denominator_metric_id: str | None = None
    frontend_safe: bool = True
    warning: str | None = None


MARKET_METRIC_DEFINITIONS: tuple[MarketMetricDefinition, ...] = (
    MarketMetricDefinition(
        "entity_count",
        "Approved mapped entities in the selected market slice.",
        "one mapped dim_entity member per source/province/domain/period",
        "entities",
        "semi_additive",
        warning="Do not sum across processing periods.",
    ),
    MarketMetricDefinition(
        "entity_count_with_sentiment_evidence",
        "Mapped entities with at least one eligible sentiment review.",
        "fact_entity_sentiment_summary.eligible_review_count > 0",
        "entities",
        "semi_additive",
        denominator_metric_id="entity_count",
        warning="A nonzero count is evidence coverage, not representativeness.",
    ),
    MarketMetricDefinition(
        "entity_count_with_aspect_evidence",
        "Mapped entities with at least one eligible aspect mention.",
        "sum(fact_entity_aspect_summary.mention_count) > 0 per entity",
        "entities",
        "semi_additive",
        denominator_metric_id="entity_count",
        warning="A nonzero count does not imply complete aspect annotation.",
    ),
    MarketMetricDefinition(
        "review_evidence_count",
        "Effective deduplicated review occurrences with sentiment evidence.",
        "fact_entity_sentiment_summary.eligible_review_count once per entity",
        "reviews",
        "semi_additive",
        warning="Processing-period evidence count; not review-event-time activity.",
    ),
    MarketMetricDefinition(
        "positive_review_count",
        "Effective reviews classified positive.",
        "fact_entity_sentiment_summary.sentiment_count where label=positive",
        "reviews",
        "semi_additive",
        denominator_metric_id="review_evidence_count",
    ),
    MarketMetricDefinition(
        "neutral_review_count",
        "Effective reviews classified neutral.",
        "fact_entity_sentiment_summary.sentiment_count where label=neutral",
        "reviews",
        "semi_additive",
        denominator_metric_id="review_evidence_count",
    ),
    MarketMetricDefinition(
        "negative_review_count",
        "Effective reviews classified negative.",
        "fact_entity_sentiment_summary.sentiment_count where label=negative",
        "reviews",
        "semi_additive",
        denominator_metric_id="review_evidence_count",
    ),
    MarketMetricDefinition(
        "aspect_mention_count",
        "Effective deduplicated aspect-segment predictions.",
        "sum(fact_entity_aspect_summary.mention_count)",
        "aspect_mentions",
        "semi_additive",
        warning="Mentions are not distinct reviews and must not be treated as such.",
    ),
)

if tuple(metric.metric_id for metric in MARKET_METRIC_DEFINITIONS) != METRICS:
    raise RuntimeError("market read semantics do not match persisted metric IDs")


@dataclass(frozen=True)
class ReadQuery:
    sql: str
    parameters: tuple[str, ...]


_REQUEST = """request(release_id, source_id, reporting_period) AS (
    VALUES (?, ?, ?)
)"""


MARKET_OVERVIEW_QUERY = ReadQuery(
    sql=f"""
WITH {_REQUEST},
coverage AS (
    SELECT
        SUM(c.observed_entity_count) AS collected_entity_count,
        SUM(c.mapped_entity_count) AS mapped_entity_count,
        SUM(c.excluded_entity_count) AS excluded_entity_count,
        SUM(c.unreviewed_or_unmapped_entity_count) AS unreviewed_entity_count,
        SUM(c.entity_count_with_sentiment_evidence) AS sentiment_entity_count,
        SUM(c.entity_count_with_aspect_evidence) AS aspect_entity_count,
        MIN(c.data_status) AS data_status
    FROM travel.gold.fact_data_coverage c
    CROSS JOIN request r
    WHERE c.release_id = r.release_id
      AND c.source_id = r.source_id
      AND c.reporting_period = r.reporting_period
),
market AS (
    SELECT
        COALESCE(SUM(IF(m.metric_id = 'review_evidence_count', m.numerator, 0)), 0)
            AS review_evidence_count,
        COALESCE(SUM(IF(m.metric_id = 'aspect_mention_count', m.numerator, 0)), 0)
            AS aspect_mention_count,
        COUNT(DISTINCT IF(m.metric_id = 'entity_count', m.domain_id, NULL))
            AS available_domain_count
    FROM travel.gold.fact_market_summary m
    CROSS JOIN request r
    WHERE m.release_id = r.release_id
      AND m.source_id = r.source_id
      AND m.reporting_period = r.reporting_period
)
SELECT coverage.*, market.*,
    CAST(mapped_entity_count AS DOUBLE) / NULLIF(collected_entity_count, 0)
        AS mapping_coverage_ratio,
    CAST(sentiment_entity_count AS DOUBLE) / NULLIF(collected_entity_count, 0)
        AS sentiment_entity_coverage_ratio,
    CAST(aspect_entity_count AS DOUBLE) / NULLIF(collected_entity_count, 0)
        AS aspect_entity_coverage_ratio
FROM coverage CROSS JOIN market
""".strip(),
    parameters=("release_id", "source_id", "reporting_period"),
)


DOMAIN_ANALYSIS_QUERY = ReadQuery(
    sql=f"""
WITH {_REQUEST}
SELECT m.domain_id,
    SUM(IF(m.metric_id = 'entity_count', m.numerator, 0)) AS entity_count,
    SUM(IF(m.metric_id = 'entity_count_with_sentiment_evidence', m.numerator, 0))
        AS entity_count_with_sentiment_evidence,
    SUM(IF(m.metric_id = 'entity_count_with_aspect_evidence', m.numerator, 0))
        AS entity_count_with_aspect_evidence,
    SUM(IF(m.metric_id = 'review_evidence_count', m.numerator, 0))
        AS review_evidence_count,
    SUM(IF(m.metric_id = 'positive_review_count', m.numerator, 0))
        AS positive_review_count,
    SUM(IF(m.metric_id = 'neutral_review_count', m.numerator, 0))
        AS neutral_review_count,
    SUM(IF(m.metric_id = 'negative_review_count', m.numerator, 0))
        AS negative_review_count,
    SUM(IF(m.metric_id = 'aspect_mention_count', m.numerator, 0))
        AS aspect_mention_count,
    MIN(m.data_status) AS data_status
FROM travel.gold.fact_market_summary m
CROSS JOIN request r
WHERE m.release_id = r.release_id
  AND m.source_id = r.source_id
  AND m.reporting_period = r.reporting_period
GROUP BY m.domain_id
ORDER BY m.domain_id
""".strip(),
    parameters=("release_id", "source_id", "reporting_period"),
)


DATA_COVERAGE_QUERY = ReadQuery(
    sql=f"""
WITH {_REQUEST}
SELECT c.geo_id, g.geo_name,
    c.observed_entity_count AS collected_entity_count,
    c.mapped_entity_count,
    c.excluded_entity_count,
    c.unreviewed_or_unmapped_entity_count AS unreviewed_entity_count,
    c.entity_count_with_sentiment_evidence AS sentiment_entity_count,
    c.entity_count_with_aspect_evidence AS aspect_entity_count,
    CAST(c.mapped_entity_count AS DOUBLE) / NULLIF(c.observed_entity_count, 0)
        AS mapping_coverage_ratio,
    CAST(c.entity_count_with_sentiment_evidence AS DOUBLE)
        / NULLIF(c.observed_entity_count, 0) AS sentiment_entity_coverage_ratio,
    CAST(c.entity_count_with_aspect_evidence AS DOUBLE)
        / NULLIF(c.observed_entity_count, 0) AS aspect_entity_coverage_ratio,
    c.data_status, c.status_reason
FROM travel.gold.fact_data_coverage c
CROSS JOIN request r
LEFT JOIN travel.gold.dim_geography g
  ON g.release_id = c.release_id AND g.geography_sk = c.geography_sk
WHERE c.release_id = r.release_id
  AND c.source_id = r.source_id
  AND c.reporting_period = r.reporting_period
ORDER BY c.geo_id
""".strip(),
    parameters=("release_id", "source_id", "reporting_period"),
)


def province_comparison_query(province_count: int) -> ReadQuery:
    """Return a bounded, parameter-only comparison query for selected provinces."""
    if not 1 <= province_count <= 20:
        raise ValueError("province comparison requires between 1 and 20 provinces")
    placeholders = ", ".join("?" for _ in range(province_count))
    sql = f"""
WITH {_REQUEST}
SELECT m.geo_id, g.geo_name, m.domain_id, m.metric_id,
    m.numerator AS value, m.denominator, m.sample_size, m.data_status
FROM travel.gold.fact_market_summary m
CROSS JOIN request r
LEFT JOIN travel.gold.dim_geography g
  ON g.release_id = m.release_id AND g.geography_sk = m.geography_sk
WHERE m.release_id = r.release_id
  AND m.source_id = r.source_id
  AND m.reporting_period = r.reporting_period
  AND m.domain_id = ?
  AND m.metric_id = ?
  AND m.geo_id IN ({placeholders})
ORDER BY m.geo_id, m.metric_id
""".strip()
    return ReadQuery(
        sql=sql,
        parameters=(
            "release_id",
            "source_id",
            "reporting_period",
            "domain_id",
            "metric_id",
            *tuple(f"province_id[{index}]" for index in range(province_count)),
        ),
    )


__all__ = [
    "DATA_COVERAGE_QUERY",
    "DOMAIN_ANALYSIS_QUERY",
    "MARKET_METRIC_DEFINITIONS",
    "MARKET_OVERVIEW_QUERY",
    "MarketMetricDefinition",
    "ReadQuery",
    "province_comparison_query",
]
