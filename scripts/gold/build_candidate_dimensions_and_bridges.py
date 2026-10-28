#!/usr/bin/env python3
# ruff: noqa: E501
"""Build the non-active T07-C candidate dimension/bridge slice.

The script pins every Silver input read to the manifest snapshots. It writes no
facts, serving view, active-release pointer or Qdrant object.
"""

from __future__ import annotations

import json
import os
from typing import Any

from minio import Minio

from travel_intelligence.common.spark import configure_spark_builder
from travel_intelligence.gold.configuration import load_gold_configuration
from travel_intelligence.gold.contracts import (
    GOLD_CONTRACT_VERSION,
    contract_fingerprint,
)
from travel_intelligence.gold.deduplication import reconcile_generations
from travel_intelligence.gold.dimensions import (
    project_date_dimension,
    project_dimensions_and_bridges,
)
from travel_intelligence.gold.manifest import build_gold_manifest, candidate_ids
from travel_intelligence.gold.spark_store import SparkGoldStore
from travel_intelligence.silver.province import load_province_rules

SOURCE_ID = "tripadvisor"
SOURCE_NAME = "TripAdvisor"
SOURCE_RELEASE_ID = "891ef41b88338d85650159e037aa84286068b9823dabbe96a5fe5a63ea0e2a24"
EXTRA_RUNS = {
    "67801540bc58eb948f28dfcb278bb24a94c063b634a6975598c1d871852d3844",
    "f1f34dbee67c7014dca29fa226c56321104203e1ae47f992137d502a734c3fc9",
}


def required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"missing required environment variable: {name}")
    return value


def build_spark() -> Any:
    from pyspark.sql import SparkSession

    return (
        configure_spark_builder(SparkSession.builder.appName("t07c-gold-dimensions"))
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.travel", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.travel.type", "jdbc")
        .config(
            "spark.sql.catalog.travel.uri",
            "jdbc:postgresql://postgres:5432/iceberg_catalog",
        )
        .config(
            "spark.sql.catalog.travel.warehouse", "s3://travel-lakehouse/warehouse/"
        )
        .config("spark.sql.catalog.travel.jdbc.user", required("ICEBERG_DB_USER"))
        .config(
            "spark.sql.catalog.travel.jdbc.password", required("ICEBERG_DB_PASSWORD")
        )
        .config("spark.sql.catalog.travel.jdbc.schema-version", "V1")
        .config(
            "spark.sql.catalog.travel.io-impl", "org.apache.iceberg.aws.s3.S3FileIO"
        )
        .config("spark.sql.catalog.travel.s3.endpoint", "http://minio:9000")
        .config("spark.sql.catalog.travel.s3.region", "us-east-1")
        .config("spark.sql.catalog.travel.s3.path-style-access", "true")
        .config(
            "spark.sql.catalog.travel.s3.access-key-id", required("MINIO_ROOT_USER")
        )
        .config(
            "spark.sql.catalog.travel.s3.secret-access-key",
            required("MINIO_ROOT_PASSWORD"),
        )
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def _rows(frame: Any) -> list[dict[str, Any]]:
    return [row.asDict(recursive=True) for row in frame.collect()]


def _read_snapshot(
    spark: Any, table: str, snapshot_id: str, run_id: str
) -> list[dict[str, Any]]:
    return _rows(
        spark.read.option("snapshot-id", snapshot_id)
        .table(table)
        .where(f"processing_run_id = '{run_id}'")
    )


def _current_snapshot_id(spark: Any, table: str) -> str:
    rows = spark.sql(
        f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1"
    ).collect()
    if not rows:
        raise RuntimeError(f"missing current snapshot for {table}")
    return str(rows[0][0])


def _minio() -> Minio:
    return Minio(
        os.environ.get("MINIO_ENDPOINT", "minio:9000"),
        access_key=required("MINIO_ROOT_USER"),
        secret_key=required("MINIO_ROOT_PASSWORD"),
        secure=False,
    )


def _frozen_run_ids(client: Minio) -> set[str]:
    key = (
        "qdrant-input-manifests/"
        "qdrant-input-786a7fda45e59a98a55c324105c5a858933871f6ffd5707daab8720aec76a0af.json"
    )
    response = client.get_object(
        os.environ.get("MINIO_BUCKET", "travel-lakehouse"), key
    )
    try:
        payload = json.loads(response.read())
    finally:
        response.close()
        response.release_conn()
    run_ids = {str(row["processing_run_id"]) for row in payload["processing_runs"]}
    if len(run_ids) != 18 or run_ids & EXTRA_RUNS:
        raise RuntimeError("frozen T07-A Qdrant manifest has unexpected run membership")
    return run_ids


def _write_manifest(client: Minio, manifest: dict[str, Any]) -> str:
    from io import BytesIO

    body = json.dumps(
        manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str
    ).encode("utf-8")
    bucket = os.environ.get("MINIO_BUCKET", "travel-lakehouse")
    key = f"gold-input-manifests/{manifest['manifest_id']}.json"
    try:
        existing = client.get_object(bucket, key)
        try:
            if existing.read() != body:
                raise RuntimeError("existing immutable Gold manifest content differs")
        finally:
            existing.close()
            existing.release_conn()
    except Exception as error:
        if "NoSuchKey" not in str(error) and "NoSuchObject" not in str(error):
            raise
        client.put_object(
            bucket, key, BytesIO(body), len(body), content_type="application/json"
        )
    return f"s3://{bucket}/{key}"


def main() -> int:
    _, config_fp = load_gold_configuration(os.environ.get("PROJECT_ROOT", "/workspace"))
    spark = build_spark()
    try:
        normalization = _rows(
            spark.table("travel.silver.normalization_run").where(
                f"run_id = '{SOURCE_RELEASE_ID}'"
            )
        )
        if len(normalization) != 1:
            raise RuntimeError("selected normalization release is not unique")
        normalization_row = normalization[0]
        enrichment_runs = _rows(
            spark.table("travel.silver.review_enrichment_run").where(
                f"source_release_id = '{SOURCE_RELEASE_ID}' AND status = 'committed'"
            )
        )
        if len(enrichment_runs) != 20:
            raise RuntimeError("candidate does not contain exactly 20 committed runs")
        client = _minio()
        frozen_run_ids = _frozen_run_ids(client)
        if not frozen_run_ids.issubset(
            {row["processing_run_id"] for row in enrichment_runs}
        ):
            raise RuntimeError(
                "frozen manifest runs are absent from the current ledger"
            )
        review_rows: list[dict[str, Any]] = []
        aspect_rows: list[dict[str, Any]] = []
        for run in enrichment_runs:
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
        reconciliation = reconcile_generations(
            review_rows, aspect_rows, frozen_processing_run_ids=frozen_run_ids
        )
        manifest_evidence = {
            key: value
            for key, value in reconciliation.items()
            if key not in {"selected_reviews", "selected_aspects"}
        }
        qdrant = _rows(
            spark.table("travel.silver.review_index_run").where(
                "index_generation = 'f3c9da889c4e372d10cccb81c412d559380f16e281666eecb1da60de0f215fe4'"
            )
        )
        if len(qdrant) != 1:
            raise RuntimeError("frozen Qdrant generation is not unique")
        geography_rules = load_province_rules()
        geography_snapshot_id = _current_snapshot_id(spark, "travel.silver.place")
        manifest = build_gold_manifest(
            normalization_row,
            enrichment_runs,
            source_id=SOURCE_ID,
            source_release_id=SOURCE_RELEASE_ID,
            contract_fingerprint=contract_fingerprint(),
            config_fingerprint=config_fp,
            qdrant_generation=qdrant[0],
            deduplication=manifest_evidence,
            geography_snapshot={
                "table": "travel.silver.place",
                "snapshot_id": geography_snapshot_id,
                "taxonomy_version": geography_rules.version,
                "taxonomy_fingerprint": geography_rules.fingerprint,
            },
        )
        manifest_uri = _write_manifest(client, manifest)
        gold_run_id, release_id = candidate_ids(manifest)
        snapshots = normalization_row["table_snapshot_refs"]
        source_records = _rows(
            spark.read.option("snapshot-id", snapshots["source_record"])
            .table("travel.silver.source_record")
            .where(f"batch_id = '{normalization_row['bronze_batch_id']}'")
        )
        source_by_sk = {row["source_record_sk"]: row for row in source_records}
        mappings = [
            row
            for row in _rows(
                spark.read.option(
                    "snapshot-id", snapshots["source_entity_mapping"]
                ).table("travel.silver.source_entity_mapping")
            )
            if row["source_record_sk"] in source_by_sk
        ]
        for row in mappings:
            row["source_record_id"] = source_by_sk[row["source_record_sk"]].get(
                "source_record_id"
            )
        place_ids = {row["place_id"] for row in mappings if row.get("place_id")}
        entity_places = [
            row
            for row in _rows(
                spark.read.option("snapshot-id", snapshots["place"]).table(
                    "travel.silver.place"
                )
            )
            if row["place_id"] in place_ids
        ]
        geography_places = {
            row["place_id"]: row
            for row in _rows(
                spark.read.option("snapshot-id", geography_snapshot_id).table(
                    "travel.silver.place"
                )
            )
            if row["place_id"] in place_ids
        }
        places = []
        geography_fields = (
            "province_raw",
            "province_normalized",
            "province_geo_id",
            "province_canonical",
            "province_resolution_status",
            "province_match_method",
            "province_rule_version",
        )
        for entity_place in entity_places:
            geo_place = geography_places.get(entity_place["place_id"])
            if geo_place is None:
                raise RuntimeError("corrected geography snapshot is missing an entity")
            for field in ("place_name", "province"):
                if entity_place.get(field) != geo_place.get(field):
                    raise RuntimeError(
                        "geography snapshot changes frozen entity/source attributes"
                    )
            places.append(
                {
                    **entity_place,
                    **{field: geo_place.get(field) for field in geography_fields},
                }
            )
        built_at = normalization_row["finished_at"]
        projection = project_dimensions_and_bridges(
            places,
            mappings,
            context={
                "release_id": release_id,
                "gold_run_id": gold_run_id,
                "contract_version": GOLD_CONTRACT_VERSION,
                "config_fingerprint": config_fp,
                "source_release_id": SOURCE_RELEASE_ID,
            },
            valid_from=built_at,
            built_at=built_at,
            source_id=SOURCE_ID,
            source_name=SOURCE_NAME,
        )
        dates = [row["started_at"].date() for row in enrichment_runs]
        projection["dim_date"] = project_date_dimension(
            min(dates),
            max(dates),
            contract_version=GOLD_CONTRACT_VERSION,
            built_at=built_at,
        )
        store = SparkGoldStore(spark)
        store.create_contract_tables()
        stale_builds = _rows(
            spark.table("travel.gold.gold_build_run").where(
                f"source_release_id = '{SOURCE_RELEASE_ID}' AND status = 'building' "
                f"AND gold_run_id <> '{gold_run_id}'"
            )
        )
        for stale in stale_builds:
            store.upsert(
                "gold_build_run",
                [
                    {
                        **stale,
                        "status": "failed",
                        "finished_at": built_at,
                        "error_summary": (
                            "superseded: frozen place snapshot lacked geography schema; "
                            "no active release was published"
                        ),
                    }
                ],
            )
        table_names = (
            "dim_entity",
            "dim_geography",
            "dim_domain",
            "dim_source",
            "dim_date",
            "bridge_entity_source",
            "bridge_entity_geography",
        )
        expected_counts = {name: len(projection[name]) for name in table_names}
        ledger_base = {
            "gold_run_id": gold_run_id,
            "release_id": release_id,
            "source_id": SOURCE_ID,
            "source_release_id": SOURCE_RELEASE_ID,
            "input_manifest_id": manifest["manifest_id"],
            "input_manifest_sha256": manifest["manifest_sha256"],
            "input_manifest_uri": manifest_uri,
            "contract_version": GOLD_CONTRACT_VERSION,
            "contract_fingerprint": contract_fingerprint(),
            "config_fingerprint": config_fp,
            "expected_table_counts": expected_counts,
            "actual_table_counts": {},
            "output_snapshot_refs": {},
            "exclusion_counts": projection["exclusion_counts"],
            "started_at": built_at,
            "finished_at": None,
            "error_summary": None,
        }
        store.upsert("gold_build_run", [{**ledger_base, "status": "building"}])
        snapshots_out = {}
        for table_name in table_names:
            store.upsert(table_name, projection[table_name])
            snapshots_out[table_name] = store.snapshot_id(table_name)
        actual_counts = {
            name: store.release_count(name, release_id)
            for name in table_names
            if name != "dim_date"
        }
        actual_counts["dim_date"] = len(projection["dim_date"])
        if actual_counts != expected_counts:
            raise RuntimeError(
                f"candidate dimension reconciliation failed: {actual_counts}"
            )
        store.upsert(
            "gold_build_run",
            [
                {
                    **ledger_base,
                    "status": "partial",
                    "actual_table_counts": actual_counts,
                    "output_snapshot_refs": snapshots_out,
                    "finished_at": built_at,
                }
            ],
        )
        print(
            json.dumps(
                {
                    "status": "partial_candidate_dimensions",
                    "gold_run_id": gold_run_id,
                    "release_id": release_id,
                    "manifest_id": manifest["manifest_id"],
                    "raw_counts": {
                        "reviews": reconciliation["raw_candidate_review_count"],
                        "aspects": reconciliation["raw_candidate_aspect_count"],
                    },
                    "effective_counts": {
                        "reviews": reconciliation["effective_review_count"],
                        "aspects": reconciliation["effective_aspect_count"],
                    },
                    "table_counts": actual_counts,
                    "snapshots": snapshots_out,
                },
                default=str,
                sort_keys=True,
            )
        )
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
