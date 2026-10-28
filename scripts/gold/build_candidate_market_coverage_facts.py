#!/usr/bin/env python3
# ruff: noqa: E501
"""Dry-run or write minimal T07-E2C market and coverage candidate facts."""

from __future__ import annotations

import argparse
import json
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from build_candidate_dimensions_and_bridges import _rows, build_spark

from travel_intelligence.gold.domain_mapping import load_domain_mapping_contract
from travel_intelligence.gold.market_facts import derive_market_and_coverage_facts
from travel_intelligence.gold.spark_store import SparkGoldStore

RELEASE_ID = "gold-release-9bfd00eeae2748684dbc34e34aab27620f6babed6c8ad8ecfbf8f8fb558c6854"
MAPPING_FINGERPRINT = "sha256:c9433a33b2b188d18bd7db0a4242c9b47b5d59ac7ebf0c023370a7efd55331aa"
SCHEMA_ADDITIONS = {
    "fact_market_summary": "source_sk STRING, source_id STRING",
    "fact_data_coverage": "mapped_entity_count BIGINT, excluded_entity_count BIGINT, unreviewed_or_unmapped_entity_count BIGINT, mapped_entity_ratio DECIMAL(9,4), entity_count_with_sentiment_evidence BIGINT, entity_count_with_aspect_evidence BIGINT",
}


def _rows_for_release(spark: Any, table: str) -> list[dict[str, Any]]:
    return _rows(spark.table(f"travel.gold.{table}").where(f"release_id = '{RELEASE_ID}'"))


def _derive(spark: Any, root: Path):
    contract = load_domain_mapping_contract(root)
    if contract["artifact_fingerprint"] != MAPPING_FINGERPRINT:
        raise RuntimeError("unexpected mapping artifact fingerprint")
    ledger = _rows_for_release(spark, "gold_build_run")
    if len(ledger) != 1 or ledger[0]["status"] != "partial":
        raise RuntimeError("candidate ledger must be unique and partial")
    ledger = ledger[0]
    entities = _rows_for_release(spark, "dim_entity")
    source_rows = _rows_for_release(spark, "dim_source")
    if len(source_rows) != 1 or source_rows[0]["source_id"] != "tripadvisor":
        raise RuntimeError("candidate source scope is not one TripAdvisor source")
    geographies = _rows_for_release(spark, "dim_geography")
    domains = _rows_for_release(spark, "dim_domain")
    context = {
        "release_id": RELEASE_ID, "gold_run_id": ledger["gold_run_id"],
        "contract_version": ledger["contract_version"],
        "config_fingerprint": ledger["config_fingerprint"],
        "source_release_id": ledger["source_release_id"], "built_at": ledger["started_at"],
        "source_id": "tripadvisor", "source_sk": source_rows[0]["source_sk"],
        "reporting_period": ledger["source_release_id"],
        "processed_at": ledger["finished_at"] or ledger["started_at"],
        "domain_sks": {row["domain_id"]: row["domain_sk"] for row in domains},
        "geography_sks": {row["geo_id"]: row["geography_sk"] for row in geographies},
    }
    facts = derive_market_and_coverage_facts(
        entities, _rows_for_release(spark, "bridge_entity_source"),
        _rows_for_release(spark, "bridge_entity_geography"),
        _rows_for_release(spark, "fact_entity_sentiment_summary"),
        _rows_for_release(spark, "fact_entity_aspect_summary"), contract, context=context,
    )
    coverage = facts["fact_data_coverage"]
    market = facts["fact_market_summary"]
    if sum(row["observed_entity_count"] for row in coverage) != 4000:
        raise RuntimeError("coverage entity total does not reconcile")
    if sum(row["mapped_entity_count"] for row in coverage) != 1973:
        raise RuntimeError("coverage mapped entity total does not reconcile")
    if sum(row["excluded_entity_count"] for row in coverage) != 47:
        raise RuntimeError("coverage excluded entity total does not reconcile")
    if sum(row["unreviewed_or_unmapped_entity_count"] for row in coverage) != 1980:
        raise RuntimeError("coverage unreviewed total does not reconcile")
    entity_metrics = [row for row in market if row["metric_id"] == "entity_count"]
    if sum(row["numerator"] for row in entity_metrics) != 1973:
        raise RuntimeError("market entity total does not reconcile")
    if len({row["data_coverage_fact_sk"] for row in coverage}) != len(coverage):
        raise RuntimeError("duplicate coverage target grain")
    if len({row["market_summary_fact_sk"] for row in market}) != len(market):
        raise RuntimeError("duplicate market target grain")
    return ledger, facts


def _ensure_schema(spark: Any) -> None:
    for table, additions in SCHEMA_ADDITIONS.items():
        existing = {field.name for field in spark.table(f"travel.gold.{table}").schema}
        needed = [item for item in additions.split(", ") if item.split()[0] not in existing]
        if needed:
            spark.sql(f"ALTER TABLE travel.gold.{table} ADD COLUMNS ({', '.join(needed)})")


def _same_rows(existing, expected, key: str) -> bool:
    if len(existing) != len(expected):
        return False
    return {str(row[key]): _canonical_row(row) for row in existing} == {
        str(row[key]): _canonical_row(row) for row in expected
    }


def _canonical_row(row: dict[str, Any]) -> tuple[tuple[str, str], ...]:
    """Compare Iceberg values semantically, ignoring decimal display scale."""
    return tuple(
        sorted((str(key), _canonical_value(value)) for key, value in row.items())
    )


def _canonical_value(value: Any) -> str:
    if isinstance(value, Decimal):
        return f"decimal:{value.normalize()}"
    if isinstance(value, (datetime, date)):
        return f"time:{value.isoformat()}"
    if value is None:
        return "null"
    return f"{type(value).__name__}:{value}"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--project-root", type=Path, default=Path("."))
    args = parser.parse_args()
    spark = build_spark()
    try:
        ledger, facts = _derive(spark, args.project_root)
        report = {
            "coverage_rows": len(facts["fact_data_coverage"]),
            "market_rows": len(facts["fact_market_summary"]),
            "coverage_entities": sum(row["observed_entity_count"] for row in facts["fact_data_coverage"]),
        }
        if not args.apply:
            print(json.dumps({"mode": "dry_run", **report}, sort_keys=True, default=str))
            return 0
        _ensure_schema(spark)
        store = SparkGoldStore(spark)
        snapshots = dict(ledger["output_snapshot_refs"] or {})
        changed = {}
        for table, key in (("fact_data_coverage", "data_coverage_fact_sk"), ("fact_market_summary", "market_summary_fact_sk")):
            expected = facts[table]
            existing = _rows_for_release(spark, table)
            if not _same_rows(existing, expected, key):
                store.upsert(table, expected)
                snapshots[table] = store.snapshot_id(table)
                changed[table] = len(expected)
        if changed:
            store.upsert("gold_build_run", [{**ledger, "output_snapshot_refs": snapshots}])
        print(json.dumps({"mode": "apply", **report, "changed": changed, "snapshots": snapshots}, sort_keys=True, default=str))
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
