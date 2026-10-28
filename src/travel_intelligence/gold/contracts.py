"""Physical Gold Iceberg contracts approved for TASK-007B validation.

The contracts are DDL definitions only. Importing this module never creates a
namespace, table, or product release.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

GOLD_CONTRACT_VERSION = "gold-contract-v1"
PRODUCT_NAMESPACE = "gold"
VALIDATION_NAMESPACE = "gold_contract_validation_t07b"


@dataclass(frozen=True)
class GoldTable:
    name: str
    grain: str
    columns_sql: str
    partition_sql: str | None = None

    def qualified_name(self, namespace: str = PRODUCT_NAMESPACE) -> str:
        return f"travel.{namespace}.{self.name}"

    def create_sql(self, namespace: str = PRODUCT_NAMESPACE) -> str:
        partition = (
            f" PARTITIONED BY ({self.partition_sql})" if self.partition_sql else ""
        )
        return (
            f"CREATE TABLE IF NOT EXISTS {self.qualified_name(namespace)} "
            f"({self.columns_sql}) USING iceberg{partition} "
            "TBLPROPERTIES ("
            "'format-version'='2', "
            "'write.parquet.compression-codec'='zstd')"
        )


_LINEAGE = """release_id STRING NOT NULL, gold_run_id STRING NOT NULL,
contract_version STRING NOT NULL, config_fingerprint STRING NOT NULL,
source_release_id STRING NOT NULL"""

GOLD_TABLES: tuple[GoldTable, ...] = (
    GoldTable(
        "dim_entity",
        "one release-scoped current snapshot row per canonical entity",
        f"""entity_sk STRING NOT NULL, entity_id STRING NOT NULL,
        entity_name STRING NOT NULL, entity_type_raw STRING,
        product_domain_id STRING, address STRING, district STRING,
        province_geo_id STRING, latitude DOUBLE, longitude DOUBLE,
        identity_status STRING NOT NULL, identity_method STRING NOT NULL,
        source_updated_at TIMESTAMP, valid_from TIMESTAMP NOT NULL,
        valid_to TIMESTAMP, is_current BOOLEAN NOT NULL, {_LINEAGE},
        built_at TIMESTAMP NOT NULL""",
        "release_id",
    ),
    GoldTable(
        "dim_geography",
        "one release-scoped row per canonical geographic unit",
        f"""geography_sk STRING NOT NULL, geo_id STRING NOT NULL,
        geo_name STRING NOT NULL, geo_level STRING NOT NULL,
        parent_geo_id STRING, coverage_tier STRING,
        resolution_status STRING NOT NULL, valid_from TIMESTAMP NOT NULL,
        valid_to TIMESTAMP, is_current BOOLEAN NOT NULL, {_LINEAGE},
        built_at TIMESTAMP NOT NULL""",
        "release_id",
    ),
    GoldTable(
        "dim_domain",
        "one controlled product-domain member per contract version",
        f"""domain_sk STRING NOT NULL, domain_id STRING NOT NULL,
        domain_name STRING NOT NULL, mapping_status STRING NOT NULL,
        is_active BOOLEAN NOT NULL, {_LINEAGE}, built_at TIMESTAMP NOT NULL""",
    ),
    GoldTable(
        "dim_source",
        "one governed source per release",
        f"""source_sk STRING NOT NULL, source_id STRING NOT NULL,
        source_name STRING NOT NULL, source_type STRING,
        is_eligible BOOLEAN NOT NULL, exclusion_reason STRING,
        {_LINEAGE}, built_at TIMESTAMP NOT NULL""",
        "release_id",
    ),
    GoldTable(
        "dim_date",
        "one calendar date",
        """date_sk INT NOT NULL, calendar_date DATE NOT NULL,
        calendar_year INT NOT NULL, calendar_quarter INT NOT NULL,
        calendar_month INT NOT NULL, month_start DATE NOT NULL,
        contract_version STRING NOT NULL, built_at TIMESTAMP NOT NULL""",
    ),
    GoldTable(
        "bridge_entity_source",
        "one release-scoped source-entity mapping assertion",
        f"""entity_source_sk STRING NOT NULL, entity_sk STRING NOT NULL,
        entity_id STRING NOT NULL, source_sk STRING NOT NULL,
        source_id STRING NOT NULL, source_record_sk STRING NOT NULL,
        source_record_id STRING, mapping_id STRING NOT NULL,
        mapping_status STRING NOT NULL, mapping_method STRING NOT NULL,
        mapping_confidence DECIMAL(6,5), mapping_version STRING NOT NULL,
        {_LINEAGE}, built_at TIMESTAMP NOT NULL""",
        "release_id, source_id",
    ),
    GoldTable(
        "bridge_entity_geography",
        "one release-scoped resolved entity-geography relationship",
        f"""entity_geography_sk STRING NOT NULL, entity_sk STRING NOT NULL,
        entity_id STRING NOT NULL, geography_sk STRING NOT NULL,
        geo_id STRING NOT NULL, relationship_type STRING NOT NULL,
        resolution_status STRING NOT NULL, resolution_method STRING,
        rule_version STRING, {_LINEAGE}, built_at TIMESTAMP NOT NULL""",
        "release_id",
    ),
    GoldTable(
        "fact_entity_rating_snapshot",
        "release x entity x source x source rating observation",
        f"""entity_rating_fact_sk STRING NOT NULL, entity_sk STRING NOT NULL,
        entity_id STRING NOT NULL, source_sk STRING NOT NULL,
        source_id STRING NOT NULL, place_rating_id STRING NOT NULL,
        source_record_sk STRING NOT NULL, rating DECIMAL(10,4),
        rating_scale STRING, review_count BIGINT, observed_at TIMESTAMP,
        observed_at_raw STRING, aggregation_eligible BOOLEAN NOT NULL,
        data_status STRING NOT NULL, {_LINEAGE}, built_at TIMESTAMP NOT NULL""",
        "release_id, source_id",
    ),
    GoldTable(
        "fact_entity_sentiment_summary",
        "release x entity x reporting period x sentiment label",
        f"""entity_sentiment_fact_sk STRING NOT NULL,
        entity_sk STRING NOT NULL, entity_id STRING NOT NULL,
        reporting_period STRING NOT NULL, time_basis STRING NOT NULL,
        sentiment_label STRING NOT NULL, sentiment_count BIGINT NOT NULL,
        eligible_review_count BIGINT NOT NULL,
        sentiment_percentage DECIMAL(9,4),
        average_confidence DOUBLE, minimum_confidence DOUBLE,
        maximum_confidence DOUBLE, model_id STRING NOT NULL,
        model_fingerprint STRING NOT NULL,
        enrichment_config_fingerprint STRING NOT NULL,
        data_status STRING NOT NULL, evidence_set_id STRING NOT NULL,
        {_LINEAGE}, built_at TIMESTAMP NOT NULL""",
        "release_id",
    ),
    GoldTable(
        "fact_entity_aspect_summary",
        "release x entity x reporting period x aspect x sentiment label",
        f"""entity_aspect_fact_sk STRING NOT NULL,
        entity_sk STRING NOT NULL, entity_id STRING NOT NULL,
        reporting_period STRING NOT NULL, time_basis STRING NOT NULL,
        aspect STRING NOT NULL, sentiment_label STRING NOT NULL,
        mention_count BIGINT NOT NULL, distinct_review_count BIGINT NOT NULL,
        eligible_review_count BIGINT NOT NULL,
        review_percentage DECIMAL(9,4),
        average_confidence DOUBLE, model_id STRING NOT NULL,
        model_fingerprint STRING NOT NULL,
        enrichment_config_fingerprint STRING NOT NULL,
        data_status STRING NOT NULL, evidence_set_id STRING NOT NULL,
        {_LINEAGE}, built_at TIMESTAMP NOT NULL""",
        "release_id",
    ),
    GoldTable(
        "fact_market_summary",
        "release x geography x domain x reporting period x metric",
        f"""market_summary_fact_sk STRING NOT NULL,
        geography_sk STRING NOT NULL, geo_id STRING NOT NULL,
        domain_sk STRING, domain_id STRING,
        source_sk STRING NOT NULL, source_id STRING NOT NULL,
        reporting_period STRING NOT NULL, time_basis STRING NOT NULL,
        metric_id STRING NOT NULL, numerator BIGINT,
        denominator BIGINT, metric_value DECIMAL(18,4),
        sample_size BIGINT NOT NULL, data_status STRING NOT NULL,
        evidence_set_id STRING, {_LINEAGE}, built_at TIMESTAMP NOT NULL""",
        "release_id",
    ),
    GoldTable(
        "fact_market_sentiment_trend",
        "release x geography x domain x time bucket x sentiment label",
        f"""market_trend_fact_sk STRING NOT NULL,
        geography_sk STRING NOT NULL, geo_id STRING NOT NULL,
        domain_sk STRING, domain_id STRING,
        period_start DATE, reporting_period STRING NOT NULL,
        time_basis STRING NOT NULL, sentiment_label STRING NOT NULL,
        sentiment_count BIGINT NOT NULL, eligible_review_count BIGINT NOT NULL,
        sentiment_percentage DECIMAL(9,4), data_status STRING NOT NULL,
        model_fingerprint STRING NOT NULL,
        enrichment_config_fingerprint STRING NOT NULL,
        {_LINEAGE}, built_at TIMESTAMP NOT NULL""",
        "release_id",
    ),
    GoldTable(
        "fact_market_aspect_summary",
        "release x geography x domain x period x aspect x sentiment label",
        f"""market_aspect_fact_sk STRING NOT NULL,
        geography_sk STRING NOT NULL, geo_id STRING NOT NULL,
        domain_sk STRING, domain_id STRING,
        reporting_period STRING NOT NULL, time_basis STRING NOT NULL,
        aspect STRING NOT NULL, sentiment_label STRING NOT NULL,
        mention_count BIGINT NOT NULL, distinct_review_count BIGINT NOT NULL,
        eligible_review_count BIGINT NOT NULL,
        review_percentage DECIMAL(9,4), data_status STRING NOT NULL,
        model_fingerprint STRING NOT NULL,
        enrichment_config_fingerprint STRING NOT NULL,
        {_LINEAGE}, built_at TIMESTAMP NOT NULL""",
        "release_id",
    ),
    GoldTable(
        "fact_data_coverage",
        "release x geography x domain x source x reporting period",
        f"""data_coverage_fact_sk STRING NOT NULL,
        geography_sk STRING, geo_id STRING,
        domain_sk STRING, domain_id STRING,
        source_sk STRING NOT NULL, source_id STRING NOT NULL,
        reporting_period STRING NOT NULL, time_basis STRING NOT NULL,
        observed_entity_count BIGINT NOT NULL,
        mapped_entity_count BIGINT NOT NULL,
        excluded_entity_count BIGINT NOT NULL,
        unreviewed_or_unmapped_entity_count BIGINT NOT NULL,
        mapped_entity_ratio DECIMAL(9,4),
        entity_count_with_sentiment_evidence BIGINT NOT NULL,
        entity_count_with_aspect_evidence BIGINT NOT NULL,
        eligible_review_count BIGINT NOT NULL,
        aspect_mention_count BIGINT NOT NULL,
        collected_at TIMESTAMP, processed_at TIMESTAMP,
        freshness_at TIMESTAMP, coverage_tier STRING,
        data_status STRING NOT NULL, status_reason STRING NOT NULL,
        threshold_version STRING NOT NULL, {_LINEAGE},
        built_at TIMESTAMP NOT NULL""",
        "release_id, source_id",
    ),
    GoldTable(
        "gold_build_run",
        "one candidate Gold build attempt",
        """gold_run_id STRING NOT NULL, release_id STRING NOT NULL,
        source_id STRING NOT NULL, source_release_id STRING NOT NULL,
        input_manifest_id STRING NOT NULL,
        input_manifest_sha256 STRING NOT NULL,
        input_manifest_uri STRING,
        contract_version STRING NOT NULL,
        contract_fingerprint STRING NOT NULL,
        config_fingerprint STRING NOT NULL,
        status STRING NOT NULL, expected_table_counts MAP<STRING,BIGINT> NOT NULL,
        actual_table_counts MAP<STRING,BIGINT> NOT NULL,
        output_snapshot_refs MAP<STRING,STRING>,
        exclusion_counts MAP<STRING,BIGINT> NOT NULL,
        started_at TIMESTAMP NOT NULL, finished_at TIMESTAMP,
        error_summary STRING""",
        "source_id, days(started_at)",
    ),
)

GOLD_TABLE_BY_NAME = {table.name: table for table in GOLD_TABLES}


def contract_fingerprint() -> str:
    value = [
        {
            "name": table.name,
            "grain": table.grain,
            "columns_sql": " ".join(table.columns_sql.split()),
            "partition_sql": table.partition_sql,
        }
        for table in GOLD_TABLES
    ]
    body = json.dumps(
        {"version": GOLD_CONTRACT_VERSION, "tables": value},
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return "sha256:" + hashlib.sha256(body).hexdigest()


def validation_ddl() -> tuple[str, ...]:
    return (
        f"CREATE NAMESPACE IF NOT EXISTS travel.{VALIDATION_NAMESPACE}",
        *(table.create_sql(VALIDATION_NAMESPACE) for table in GOLD_TABLES),
    )


__all__ = [
    "GOLD_CONTRACT_VERSION",
    "GOLD_TABLES",
    "GOLD_TABLE_BY_NAME",
    "GoldTable",
    "PRODUCT_NAMESPACE",
    "VALIDATION_NAMESPACE",
    "contract_fingerprint",
    "validation_ddl",
]
