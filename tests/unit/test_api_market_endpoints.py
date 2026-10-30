from __future__ import annotations

from typing import Any

from fastapi.testclient import TestClient

from travel_intelligence.api.config import ApiSettings
from travel_intelligence.api.database import TrinoUnavailableError
from travel_intelligence.api.main import create_app

RELEASE_ID = "gold-release-test"
PERIOD = "source-release-period"
DA_NANG = "vn-province:4f858848735ff411ba23b4bf"
HA_NOI = "vn-province:95be0df1c5ca8ae8973711d5"
MISSING = "vn-province:000000000000000000000000"
SNAPSHOTS = {
    "dim_entity": "101",
    "fact_data_coverage": "102",
    "fact_market_summary": "103",
}


def settings() -> ApiSettings:
    return ApiSettings(
        trino_host="trino",
        trino_port=8080,
        trino_user="travel_api",
        trino_catalog="travel",
        trino_schema="gold",
        trino_http_scheme="http",
        trino_request_timeout_seconds=10,
        gold_release_id=RELEASE_ID,
        cors_origins=("http://localhost:5173",),
    )


class AnalyticsDatabase:
    def __init__(
        self, *, fail_analytics: bool = False, empty_analytics: bool = False
    ) -> None:
        self.fail_analytics = fail_analytics
        self.empty_analytics = empty_analytics
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    def fetch_all(self, sql: str, parameters=()):
        bound = tuple(parameters)
        self.calls.append((sql, bound))
        if "gold_build_run" in sql:
            return [
                {
                    "release_id": RELEASE_ID,
                    "status": "partial",
                    "output_snapshot_refs": SNAPSHOTS,
                }
            ]
        if "$snapshots" in sql:
            return [{"snapshot_id": bound[0]}]
        if "count(*) AS row_count" in sql:
            table = next(name for name in SNAPSHOTS if name in sql)
            return [
                {
                    "row_count": {
                        "dim_entity": 4000,
                        "fact_data_coverage": 24,
                        "fact_market_summary": 448,
                    }[table]
                }
            ]
        if "SELECT DISTINCT source_id, reporting_period" in sql:
            return [{"source_id": "tripadvisor", "reporting_period": PERIOD}]
        if self.fail_analytics:
            raise TrinoUnavailableError("internal host and SQL details")
        if self.empty_analytics:
            return []
        if "coverage AS (" in sql:
            return [_overview_row()]
        if "GROUP BY m.domain_id" in sql:
            return [
                _domain_row("destination", 1722),
                _domain_row("food_dining", 108),
                _domain_row("transportation", 143),
            ]
        if "GROUP BY m.geo_id" in sql:
            if DA_NANG not in bound:
                return []
            return [
                {
                    "geo_id": DA_NANG,
                    "domain_id": "destination" if "m.domain_id = ?" in sql else None,
                    "metric_id": "entity_count",
                    "value": 294 if "m.domain_id = ?" in sql else 311,
                    "denominator": None,
                    "sample_size": 294,
                    "data_status": "insufficient_data",
                }
            ]
        if "c.observed_entity_count AS collected_entity_count" in sql:
            return [_coverage_row(DA_NANG, 500), _coverage_row(HA_NOI, 600)]
        raise AssertionError(f"unexpected SQL: {sql}")


def _overview_row():
    return {
        "collected_entity_count": 4000,
        "mapped_entity_count": 1973,
        "excluded_entity_count": 47,
        "unreviewed_entity_count": 1980,
        "sentiment_entity_count": 2531,
        "aspect_entity_count": 2466,
        "data_status": "insufficient_data",
        "review_evidence_count": 8749,
        "aspect_mention_count": 32137,
        "available_domain_count": 3,
        "mapping_coverage_ratio": 0.49325,
        "sentiment_entity_coverage_ratio": 0.63275,
        "aspect_entity_coverage_ratio": 0.6165,
    }


def _domain_row(domain_id: str, entity_count: int):
    return {
        "domain_id": domain_id,
        "entity_count": entity_count,
        "entity_count_with_sentiment_evidence": entity_count - 10,
        "entity_count_with_aspect_evidence": entity_count - 20,
        "review_evidence_count": entity_count * 4,
        "positive_review_count": 1,
        "neutral_review_count": entity_count * 4 - 2,
        "negative_review_count": 1,
        "aspect_mention_count": entity_count * 10,
        "data_status": "insufficient_data",
    }


def _coverage_row(province_id: str, collected: int):
    return {
        "geo_id": province_id,
        "collected_entity_count": collected,
        "mapped_entity_count": collected // 2,
        "excluded_entity_count": 5,
        "unreviewed_entity_count": collected - collected // 2 - 5,
        "sentiment_entity_count": collected // 3,
        "aspect_entity_count": collected // 4,
        "mapping_coverage_ratio": (collected // 2) / collected,
        "sentiment_entity_coverage_ratio": (collected // 3) / collected,
        "aspect_entity_coverage_ratio": (collected // 4) / collected,
        "data_status": "insufficient_data",
        "status_reason": "thresholds_not_approved",
    }


def test_overview_reconciles_baseline_and_uses_pinned_snapshots():
    database = AnalyticsDatabase()
    response = TestClient(create_app(settings(), database)).get(
        "/api/v1/market/overview"
    )
    assert response.status_code == 200
    assert response.json() == {
        "collected_entity_count": 4000,
        "mapped_entity_count": 1973,
        "excluded_entity_count": 47,
        "unreviewed_entity_count": 1980,
        "entity_count_with_sentiment_evidence": 2531,
        "entity_count_with_aspect_evidence": 2466,
        "mapping_coverage_ratio": 0.49325,
        "sentiment_entity_coverage_ratio": 0.63275,
        "aspect_entity_coverage_ratio": 0.6165,
        "review_evidence_count": 8749,
        "aspect_mention_count": 32137,
        "support_status": "insufficient_data",
    }
    analytics_sql, parameters = database.calls[-1]
    assert analytics_sql.count("FOR VERSION AS OF ?") == 2
    assert parameters == (RELEASE_ID, "tripadvisor", PERIOD, 102, 103)


def test_domains_returns_all_controlled_domains_and_accommodation_no_data():
    response = TestClient(create_app(settings(), AnalyticsDatabase())).get(
        "/api/v1/market/domains"
    )
    assert response.status_code == 200
    rows = response.json()
    assert [row["domain_id"] for row in rows] == [
        "destination",
        "accommodation",
        "food_dining",
        "transportation",
    ]
    assert rows[0]["entity_count"] == 1722
    assert rows[1]["support_status"] == "no_data"
    assert rows[1]["entity_count"] is None
    assert sum(row["entity_count"] or 0 for row in rows) == 1973


def test_province_comparison_preserves_order_and_returns_explicit_no_data():
    client = TestClient(create_app(settings(), AnalyticsDatabase()))
    response = client.get(
        "/api/v1/market/provinces/compare",
        params=[
            ("province_id", MISSING),
            ("province_id", DA_NANG),
            ("metric_id", "entity_count"),
            ("domain_id", "destination"),
        ],
    )
    assert response.status_code == 200
    rows = response.json()
    assert [row["province_id"] for row in rows] == [MISSING, DA_NANG]
    assert rows[0]["count"] is None
    assert rows[0]["support_status"] == "no_data"
    assert rows[1] == {
        "province_id": DA_NANG,
        "domain_id": "destination",
        "metric_id": "entity_count",
        "unit": "entities",
        "count": 294,
        "support_status": "insufficient_data",
    }


def test_province_comparison_without_domain_aggregates_disjoint_domains():
    database = AnalyticsDatabase()
    response = TestClient(create_app(settings(), database)).get(
        "/api/v1/market/provinces/compare",
        params={"province_id": DA_NANG, "metric_id": "entity_count"},
    )
    assert response.status_code == 200
    assert response.json()[0]["count"] == 311
    sql, parameters = database.calls[-1]
    assert "AND m.domain_id = ?" not in sql
    assert "SUM(m.numerator)" in sql
    assert parameters == (
        RELEASE_ID,
        "tripadvisor",
        PERIOD,
        103,
        "entity_count",
        DA_NANG,
    )


def test_coverage_ratios_and_missing_province_semantics():
    response = TestClient(create_app(settings(), AnalyticsDatabase())).get(
        "/api/v1/data/coverage",
        params=[("province_id", MISSING), ("province_id", DA_NANG)],
    )
    assert response.status_code == 200
    missing, observed = response.json()
    assert missing["support_status"] == "no_data"
    assert missing["collected_entity_count"] is None
    assert observed["collected_entity_count"] == 500
    assert observed["mapping_coverage_ratio"] == 0.5
    assert observed["support_status"] == "insufficient_data"


def test_invalid_metric_domain_province_and_count_return_422():
    client = TestClient(create_app(settings(), AnalyticsDatabase()))
    base = "/api/v1/market/provinces/compare"
    invalid_metric = client.get(
        base, params={"province_id": DA_NANG, "metric_id": "rating"}
    )
    assert invalid_metric.status_code == 422
    invalid_domain = client.get(
        base,
        params={
            "province_id": DA_NANG,
            "metric_id": "entity_count",
            "domain_id": "spa",
        },
    )
    assert invalid_domain.status_code == 422
    invalid_province = client.get(
        base,
        params={"province_id": "da-nang", "metric_id": "entity_count"},
    )
    assert invalid_province.status_code == 422
    too_many = [("province_id", DA_NANG)] * 21 + [("metric_id", "entity_count")]
    assert client.get(base, params=too_many).status_code == 422


def test_invalid_source_period_and_trino_failure_are_safe():
    client = TestClient(create_app(settings(), AnalyticsDatabase()))
    invalid = client.get("/api/v1/market/overview", params={"source_id": "other"})
    assert invalid.status_code == 422
    failed = TestClient(
        create_app(settings(), AnalyticsDatabase(fail_analytics=True))
    ).get("/api/v1/market/overview")
    assert failed.status_code == 503
    assert failed.json() == {"detail": "Gold analytics dependencies are unavailable"}
    assert "internal host" not in failed.text


def test_empty_gold_results_return_no_data_without_fabricated_zeros():
    client = TestClient(
        create_app(settings(), AnalyticsDatabase(empty_analytics=True))
    )
    overview = client.get("/api/v1/market/overview")
    assert overview.status_code == 200
    assert overview.json()["support_status"] == "no_data"
    assert overview.json()["collected_entity_count"] is None
    domains = client.get("/api/v1/market/domains")
    assert domains.status_code == 200
    assert all(row["support_status"] == "no_data" for row in domains.json())
    coverage = client.get("/api/v1/data/coverage")
    assert coverage.status_code == 200
    assert coverage.json() == []
