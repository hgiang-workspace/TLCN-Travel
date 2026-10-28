#!/usr/bin/env python3
"""Dry-run or apply approved exact domain mappings to one Gold candidate."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from build_candidate_dimensions_and_bridges import _rows, build_spark

from travel_intelligence.gold.domain_mapping import load_domain_mapping_contract
from travel_intelligence.gold.entity_domain_mapping import (
    assert_expected_candidate_counts,
    plan_entity_domain_mapping,
)
from travel_intelligence.gold.spark_store import SparkGoldStore

RELEASE_ID = (
    "gold-release-9bfd00eeae2748684dbc34e34aab27620f6babed6c8ad8ecfbf8f8fb558c6854"
)
APPROVED_MAPPING_FINGERPRINT = (
    "sha256:c9433a33b2b188d18bd7db0a4242c9b47b5d59ac7ebf0c023370a7efd55331aa"
)


def _candidate_rows(
    spark: Any,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    ledger_rows = _rows(
        spark.table("travel.gold.gold_build_run").where(
            f"release_id = '{RELEASE_ID}'"
        )
    )
    if len(ledger_rows) != 1 or ledger_rows[0]["status"] != "partial":
        raise RuntimeError("candidate ledger is not one partial release")
    entities = _rows(
        spark.table("travel.gold.dim_entity").where(f"release_id = '{RELEASE_ID}'")
    )
    bridges = _rows(
        spark.table("travel.gold.bridge_entity_source").where(
            f"release_id = '{RELEASE_ID}'"
        )
    )
    return ledger_rows[0], entities, bridges


def _post_write_checks(spark: Any) -> dict[str, int]:
    entities = _rows(
        spark.table("travel.gold.dim_entity").where(f"release_id = '{RELEASE_ID}'")
    )
    bridges = _rows(
        spark.table("travel.gold.bridge_entity_source").where(
            f"release_id = '{RELEASE_ID}'"
        )
    )
    if len(entities) != 4000 or len({row["entity_id"] for row in entities}) != 4000:
        raise RuntimeError("post-write entity grain changed")
    if {row["entity_id"] for row in bridges} != {row["entity_id"] for row in entities}:
        raise RuntimeError("post-write source bridge integrity failed")
    sentiment = spark.table("travel.gold.fact_entity_sentiment_summary").where(
        f"release_id = '{RELEASE_ID}'"
    )
    aspects = spark.table("travel.gold.fact_entity_aspect_summary").where(
        f"release_id = '{RELEASE_ID}'"
    )
    if sentiment.count() != 3313 or aspects.count() != 19577:
        raise RuntimeError("sentiment or aspect fact count changed")
    return {"entity_count": len(entities), "bridge_count": len(bridges)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="write after preflight")
    parser.add_argument("--project-root", type=Path, default=Path("."))
    args = parser.parse_args()
    contract = load_domain_mapping_contract(args.project_root)
    if contract["artifact_fingerprint"] != APPROVED_MAPPING_FINGERPRINT:
        raise RuntimeError("mapping artifact fingerprint is not the approved baseline")
    spark = build_spark()
    try:
        ledger, entities, bridges = _candidate_rows(spark)
        plan = plan_entity_domain_mapping(
            entities, bridges, contract, release_id=RELEASE_ID
        )
        assert_expected_candidate_counts(plan.report)
        before_snapshot = dict(ledger["output_snapshot_refs"] or {}).get("dim_entity")
        result: dict[str, Any] = {
            "mode": "apply" if args.apply else "dry_run",
            "release_id": RELEASE_ID,
            "mapping_fingerprint": contract["artifact_fingerprint"],
            "entity_snapshot_before": before_snapshot,
            "report": plan.report,
        }
        if not args.apply:
            print(json.dumps(result, sort_keys=True, default=str))
            return 0
        store = SparkGoldStore(spark)
        if plan.updated_rows:
            store.upsert("dim_entity", plan.updated_rows)
            after_snapshot = store.snapshot_id("dim_entity")
            snapshots = dict(ledger["output_snapshot_refs"] or {})
            snapshots["dim_entity"] = after_snapshot
            store.upsert(
                "gold_build_run",
                [{**ledger, "output_snapshot_refs": snapshots}],
            )
        else:
            after_snapshot = before_snapshot
        result["entity_snapshot_after"] = after_snapshot
        result["changed_entity_count"] = len(plan.updated_rows)
        result["post_write"] = _post_write_checks(spark)
        print(json.dumps(result, sort_keys=True, default=str))
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
