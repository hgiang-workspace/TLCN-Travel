"""Contract-only Silver foundation helpers.

This module intentionally does not normalize source payloads. It provides the
approved table DDL, Bronze v2 eligibility checks, and release-boundary checks
used by the first Spark foundation task.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

BRONZE_CONTRACT_VERSION = "bronze-contract-v2"
BRONZE_SCHEMA_VERSION = "bronze-v2"
SILVER_NAMESPACE = "silver"
NORMALIZATION_VERSION = "silver-foundation-v1"


@dataclass(frozen=True)
class SilverTable:
    name: str
    columns_sql: str
    partition_sql: str | None = None

    def create_sql(self) -> str:
        partition = (
            f" PARTITIONED BY ({self.partition_sql})" if self.partition_sql else ""
        )
        return (
            f"CREATE TABLE IF NOT EXISTS travel.{SILVER_NAMESPACE}.{self.name} "
            f"({self.columns_sql}) USING iceberg{partition} "
            "TBLPROPERTIES ("
            "'format-version'='2', 'write.parquet.compression-codec'='zstd')"
        )


_COMMON = "source_record_sk STRING NOT NULL, quality_codes ARRAY<STRING> NOT NULL"

SILVER_TABLES: tuple[SilverTable, ...] = (
    SilverTable(
        "source_record",
        """source_record_sk STRING NOT NULL, source_id STRING NOT NULL,
        source_row_index BIGINT NOT NULL, record_type STRING NOT NULL,
        source_record_id STRING, source_uri STRING, batch_id STRING NOT NULL,
        payload_hash STRING NOT NULL, bronze_object_path STRING NOT NULL,
        duplicate_status STRING NOT NULL, duplicate_of_source_record_sk STRING,
        source_collected_at_raw STRING, ingested_at TIMESTAMP NOT NULL,
        normalization_version STRING NOT NULL, quality_codes ARRAY<STRING> NOT NULL,
        processed_at TIMESTAMP NOT NULL, ingest_date DATE NOT NULL""",
        "source_id, days(ingest_date)",
    ),
    SilverTable(
        "normalization_run",
        """run_id STRING NOT NULL, source_id STRING NOT NULL,
        bronze_batch_id STRING NOT NULL, normalization_version STRING NOT NULL,
        attempt INT NOT NULL, status STRING NOT NULL, input_row_count BIGINT NOT NULL,
        accepted_row_count BIGINT NOT NULL, rejected_row_count BIGINT NOT NULL,
        nested_input_count BIGINT NOT NULL, target_counts MAP<STRING,BIGINT> NOT NULL,
        input_schema_fingerprint STRING NOT NULL, target_schema_fingerprint STRING,
        started_at TIMESTAMP NOT NULL, finished_at TIMESTAMP,
        table_snapshot_refs MAP<STRING,STRING>, error_summary STRING""",
        "source_id, days(started_at)",
    ),
    SilverTable(
        "place",
        """place_id STRING NOT NULL, place_name STRING NOT NULL,
        place_type STRING, province STRING, province_raw STRING,
        province_normalized STRING, province_geo_id STRING,
        province_canonical STRING, province_resolution_status STRING,
        province_match_method STRING, province_rule_version STRING,
        district STRING, address STRING,
        latitude DOUBLE, longitude DOUBLE, identity_status STRING NOT NULL,
        identity_method STRING NOT NULL, created_at TIMESTAMP NOT NULL,
        updated_at TIMESTAMP NOT NULL""",
    ),
    SilverTable(
        "source_entity_mapping",
        """mapping_id STRING NOT NULL, source_record_sk STRING NOT NULL,
        place_id STRING, mapping_method STRING NOT NULL, confidence DECIMAL(6,5),
        decision_status STRING NOT NULL, evidence_ref STRING,
        mapping_version STRING NOT NULL, created_at TIMESTAMP NOT NULL,
        updated_at TIMESTAMP NOT NULL""",
        "days(updated_at)",
    ),
    SilverTable(
        "place_rating",
        """place_rating_id STRING NOT NULL, source_record_sk STRING NOT NULL,
        place_id STRING, source_id STRING NOT NULL, observed_at TIMESTAMP,
        observed_at_raw STRING, rating DECIMAL(10,4), rating_scale STRING,
        review_count BIGINT, rating_quality DECIMAL(10,4), rating_price DECIMAL(10,4),
        rating_service DECIMAL(10,4), rating_location DECIMAL(10,4),
        rating_space DECIMAL(10,4), price_min DECIMAL(18,4), price_max DECIMAL(18,4),
        currency STRING, quality_codes ARRAY<STRING> NOT NULL""",
        "source_id",
    ),
    SilverTable(
        "review",
        """review_id STRING NOT NULL, source_record_sk STRING NOT NULL,
        place_id STRING, review_ordinal BIGINT NOT NULL,
        source_review_index_raw STRING, key_review STRING, review_text STRING,
        detail_review STRING, rating DECIMAL(10,4), language_raw STRING,
        quality_codes ARRAY<STRING> NOT NULL""",
        None,
    ),
    SilverTable(
        "social_content",
        """content_id STRING NOT NULL, source_record_sk STRING NOT NULL,
        platform STRING NOT NULL, source_video_id STRING, content_type STRING,
        description STRING, keyword_raw STRING, province_raw STRING,
        hashtags ARRAY<STRING>, posted_date_raw STRING, crawl_time_raw STRING,
        source_url STRING, reported_comment_count BIGINT,
        collected_comment_count BIGINT NOT NULL,
        quality_codes ARRAY<STRING> NOT NULL""",
        "platform",
    ),
    SilverTable(
        "social_comment",
        """comment_id STRING NOT NULL, content_id STRING NOT NULL,
        source_record_sk STRING NOT NULL, comment_ordinal BIGINT NOT NULL,
        source_comment_id_raw STRING, comment_text STRING, like_count BIGINT,
        posted_time_raw STRING, quality_codes ARRAY<STRING> NOT NULL""",
        None,
    ),
    SilverTable(
        "social_engagement",
        """engagement_id STRING NOT NULL, content_id STRING NOT NULL,
        source_record_sk STRING NOT NULL, observed_at_raw STRING,
        view_count BIGINT, like_count BIGINT, comment_count_reported BIGINT,
        share_count BIGINT, total_engagement BIGINT,
        quality_codes ARRAY<STRING> NOT NULL""",
        None,
    ),
    SilverTable(
        "tourism_supply",
        """supply_observation_id STRING NOT NULL, source_record_sk STRING NOT NULL,
        place_id STRING, availability_raw STRING, availability BOOLEAN,
        value_raw STRING, value DECIMAL(18,4), unit STRING, observed_at_raw STRING,
        observed_at TIMESTAMP, quality_codes ARRAY<STRING> NOT NULL""",
    ),
    SilverTable(
        "transport_route",
        """transport_id STRING NOT NULL, source_record_sk STRING NOT NULL,
        origin STRING NOT NULL, destination STRING NOT NULL,
        operator_name STRING NOT NULL, transport_type STRING, vehicle_type STRING,
        departure_time_raw STRING, arrival_time_raw STRING, duration_min INT,
        price_vnd DECIMAL(18,2), crawl_date_raw STRING, crawl_date DATE,
        source STRING NOT NULL, source_url STRING,
        quality_codes ARRAY<STRING> NOT NULL""",
        "source",
    ),
)


def validate_bronze_manifest(manifest: Mapping[str, Any]) -> None:
    """Reject legacy, partial, or incomplete Bronze manifests explicitly."""
    required = {
        "status",
        "contract_version",
        "schema_version",
        "source_id",
        "batch_id",
        "parquet_key",
        "row_count",
        "payload_hashes",
    }
    missing = sorted(required - manifest.keys())
    if missing:
        raise ValueError(
            "Bronze manifest missing required fields: " + ", ".join(missing)
        )
    if manifest["status"] != "success":
        raise ValueError("Bronze manifest is not successful")
    if manifest["contract_version"] != BRONZE_CONTRACT_VERSION:
        raise ValueError("Bronze v1 or unknown contract is not eligible for Silver v2")
    if manifest["schema_version"] != BRONZE_SCHEMA_VERSION:
        raise ValueError("Bronze schema version is not eligible for Silver v2")
    if not isinstance(manifest["payload_hashes"], list):
        raise ValueError("Bronze manifest payload_hashes must be a list")
    if manifest["row_count"] != len(manifest["payload_hashes"]):
        raise ValueError("Bronze manifest row count/hash count mismatch")


def validate_release_tables(
    required_tables: Sequence[str], committed_tables: Mapping[str, str]
) -> None:
    """Require every target table snapshot before a release is visible."""
    missing = [name for name in required_tables if name not in committed_tables]
    if missing:
        raise ValueError(
            "Silver release incomplete; missing tables: " + ", ".join(missing)
        )
    if any(not committed_tables[name] for name in required_tables):
        raise ValueError("Silver release contains an empty table snapshot reference")


def release_is_visible(
    status: str,
    required_tables: Sequence[str],
    table_snapshot_refs: Mapping[str, str] | None,
) -> bool:
    """Return whether a ledger row is eligible for downstream consumption.

    Physical rows from a partial multi-table write may exist in Iceberg.  They
    become visible as a product release only after the ledger is committed and
    records every required table snapshot.
    """
    if status != "committed" or table_snapshot_refs is None:
        return False
    try:
        validate_release_tables(required_tables, table_snapshot_refs)
    except ValueError:
        return False
    return True


def committed_replay_is_noop(
    existing_run: Mapping[str, Any] | None,
    *,
    source_id: str,
    batch_id: str,
    normalization_version: str,
    input_schema_fingerprint: str,
    target_schema_fingerprint: str,
    required_tables: Sequence[str],
    snapshot_exists: Callable[[str, str], bool],
) -> bool:
    """Validate an existing ledger row and decide whether replay is a no-op.

    Partial runs deliberately return ``False`` so the caller can retry the
    deterministic per-table writes. A committed row is skippable only when its
    identity, fingerprints, required references, and referenced snapshots all
    match the requested release.
    """
    if existing_run is None:
        return False

    expected = {
        "source_id": source_id,
        "bronze_batch_id": batch_id,
        "normalization_version": normalization_version,
        "input_schema_fingerprint": input_schema_fingerprint,
        "target_schema_fingerprint": target_schema_fingerprint,
    }
    conflicts = [
        field for field, value in expected.items() if existing_run.get(field) != value
    ]
    if conflicts:
        raise ValueError(
            "Existing normalization run conflicts with requested release: "
            + ", ".join(conflicts)
        )

    status = existing_run.get("status")
    if status != "committed":
        if status in {"started", "partial", "failed"}:
            return False
        raise ValueError(f"Existing normalization run has unknown status: {status}")

    refs = existing_run.get("table_snapshot_refs")
    if not release_is_visible(status, required_tables, refs):
        raise ValueError("Existing committed release has incomplete snapshot evidence")
    missing_snapshots = [
        table for table in required_tables if not snapshot_exists(table, refs[table])
    ]
    if missing_snapshots:
        raise ValueError(
            "Existing committed release references missing snapshots: "
            + ", ".join(missing_snapshots)
        )
    return True


def execute_table_writes(
    tables: Sequence[str],
    write_table: Callable[[str], str],
    *,
    fail_after_table: str | None = None,
) -> dict[str, str]:
    """Execute ordered per-table writes with deterministic failure injection."""
    results = {}
    for table in tables:
        results[table] = write_table(table)
        if fail_after_table == table:
            raise RuntimeError(f"injected failure after {table}")
    return results


@dataclass
class ReleaseLedger:
    """In-memory contract model for the eventual normalization_run ledger."""

    required_tables: tuple[str, ...]
    table_snapshot_refs: dict[str, str] = field(default_factory=dict)
    status: str = "partial"

    def record_table_commit(self, table: str, snapshot_id: str) -> None:
        if table not in self.required_tables:
            raise ValueError(f"table is not part of this release: {table}")
        if not snapshot_id:
            raise ValueError("an Iceberg snapshot reference is required")
        self.table_snapshot_refs[table] = snapshot_id

    def commit(self) -> None:
        validate_release_tables(self.required_tables, self.table_snapshot_refs)
        self.status = "committed"

