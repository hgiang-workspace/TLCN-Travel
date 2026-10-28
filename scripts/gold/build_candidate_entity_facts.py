# ruff: noqa: E501
"""Populate non-active T07-D entity sentiment/aspect candidate facts."""

from __future__ import annotations

import json
from typing import Any

from build_candidate_dimensions_and_bridges import (
    _frozen_run_ids,
    _minio,
    _read_snapshot,
    _rows,
    build_spark,
)

from travel_intelligence.gold.entity_facts import derive_entity_facts
from travel_intelligence.gold.spark_store import SparkGoldStore

RELEASE_ID = (
    "gold-release-9bfd00eeae2748684dbc34e34aab27620f6babed6c8ad8ecfbf8f8fb558c6854"
)


def _manifest(client: Any, uri: str) -> dict[str, Any]:
    bucket, key = uri.removeprefix("s3://").split("/", 1)
    response = client.get_object(bucket, key)
    try:
        return json.loads(response.read())
    finally:
        response.close()
        response.release_conn()


def main() -> int:
    spark = build_spark()
    try:
        ledger_rows = _rows(
            spark.table("travel.gold.gold_build_run").where(
                f"release_id = '{RELEASE_ID}'"
            )
        )
        if len(ledger_rows) != 1 or ledger_rows[0]["status"] != "partial":
            raise RuntimeError(
                "approved T07-C candidate ledger is not partial and unique"
            )
        ledger = ledger_rows[0]
        client = _minio()
        manifest = _manifest(client, ledger["input_manifest_uri"])
        runs = manifest["enrichment"]["runs"]
        frozen = _frozen_run_ids(client)
        review_rows: list[dict[str, Any]] = []
        aspect_rows: list[dict[str, Any]] = []
        for run in runs:
            review_rows.extend(
                _read_snapshot(
                    spark,
                    "travel.silver.review_sentiment",
                    run["review_snapshot_id"],
                    run["processing_run_id"],
                )
            )
            aspect_rows.extend(
                _read_snapshot(
                    spark,
                    "travel.silver.review_aspect_sentiment",
                    run["aspect_snapshot_id"],
                    run["processing_run_id"],
                )
            )
        from travel_intelligence.gold.deduplication import reconcile_generations

        effective = reconcile_generations(
            review_rows, aspect_rows, frozen_processing_run_ids=frozen
        )
        if (
            effective["effective_review_count"]
            != manifest["enrichment"]["effective_review_count"]
        ):
            raise RuntimeError("effective review count does not match manifest")
        if (
            effective["effective_aspect_count"]
            != manifest["enrichment"]["effective_aspect_count"]
        ):
            raise RuntimeError("effective aspect count does not match manifest")
        selected_review_by_id = {
            row["review_sentiment_id"]: row for row in effective["selected_reviews"]
        }
        selected_aspects = [
            {
                **row,
                "place_id": selected_review_by_id[row["review_sentiment_id"]][
                    "place_id"
                ],
            }
            for row in effective["selected_aspects"]
        ]
        entities = _rows(
            spark.table("travel.gold.dim_entity").where(f"release_id = '{RELEASE_ID}'")
        )
        entity_by_id = {row["entity_id"]: row for row in entities}
        enrichment = manifest["enrichment"]
        facts = derive_entity_facts(
            effective["selected_reviews"],
            selected_aspects,
            entity_by_id=entity_by_id,
            release_id=RELEASE_ID,
            gold_run_id=ledger["gold_run_id"],
            contract_version=ledger["contract_version"],
            config_fingerprint=ledger["config_fingerprint"],
            source_release_id=ledger["source_release_id"],
            reporting_period=ledger["source_release_id"],
            model_id=enrichment["model_id"],
            model_fingerprint=enrichment["model_fingerprint"],
            enrichment_config_fingerprint=enrichment["config_fingerprint"],
            built_at=ledger["started_at"],
        )
        store = SparkGoldStore(spark)
        table_names = ("fact_entity_sentiment_summary", "fact_entity_aspect_summary")
        for table in table_names:
            store.upsert(table, facts[table])
        snapshots = dict(ledger["output_snapshot_refs"] or {})
        actual_counts = dict(ledger["actual_table_counts"] or {})
        expected_counts = dict(ledger["expected_table_counts"] or {})
        for table in table_names:
            count = store.release_count(table, RELEASE_ID)
            if count != len(facts[table]):
                raise RuntimeError(f"fact reconciliation failed for {table}")
            expected_counts[table] = len(facts[table])
            actual_counts[table] = count
            snapshots[table] = store.snapshot_id(table)
        store.upsert(
            "gold_build_run",
            [
                {
                    **ledger,
                    "status": "partial",
                    "expected_table_counts": expected_counts,
                    "actual_table_counts": actual_counts,
                    "output_snapshot_refs": snapshots,
                }
            ],
        )
        print(
            json.dumps(
                {
                    "status": "partial_candidate_entity_facts",
                    "release_id": RELEASE_ID,
                    "effective_reviews": effective["effective_review_count"],
                    "effective_aspects": effective["effective_aspect_count"],
                    "fact_counts": {
                        table: actual_counts[table] for table in table_names
                    },
                    "snapshots": {table: snapshots[table] for table in table_names},
                },
                sort_keys=True,
            )
        )
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
