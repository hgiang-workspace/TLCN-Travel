#!/usr/bin/env python3
"""Read-only TASK-005A inventory against the configured runtime."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
from typing import Any

from travel_intelligence.backfill.inventory import (
    build_inventory_report,
    human_summary,
)
from travel_intelligence.qdrant.provisioning import (
    build_http_client,
    load_provisioning_configs,
)


def required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Required environment variable is missing: {name}")
    return value


def build_spark() -> Any:
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName("t05a-tripadvisor-inventory")
        .master("local[2]")
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
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def table_rows(spark: Any, table: str) -> list[dict[str, Any]]:
    """Read a table without creating, updating, or deleting anything."""
    if not spark.catalog.tableExists(table):
        return []
    return [row.asDict(recursive=True) for row in spark.table(table).collect()]


def review_rows(spark: Any) -> list[dict[str, Any]]:
    if not spark.catalog.tableExists("travel.silver.review"):
        return []
    if not spark.catalog.tableExists("travel.silver.source_record"):
        return []
    query = """
        SELECT r.review_id, r.source_record_sk, r.review_ordinal, r.place_id,
               r.review_text, r.detail_review, sr.source_id, sr.batch_id,
               sr.source_row_index
        FROM travel.silver.review r
        JOIN travel.silver.source_record sr
          ON sr.source_record_sk = r.source_record_sk
    """
    return [row.asDict(recursive=True) for row in spark.sql(query).collect()]


def bronze_manifests() -> list[dict[str, Any]]:
    from minio import Minio

    endpoint = os.environ.get("MINIO_ENDPOINT", "minio:9000")
    client = Minio(
        endpoint,
        access_key=required("MINIO_ROOT_USER"),
        secret_key=required("MINIO_ROOT_PASSWORD"),
        secure=os.environ.get("MINIO_SECURE", "false").lower() == "true",
    )
    bucket = os.environ.get("MINIO_BUCKET", "travel-lakehouse")
    rows: list[dict[str, Any]] = []
    for item in client.list_objects(
        bucket, prefix="bronze/_manifests/tripadvisor/", recursive=True
    ):
        if not item.object_name.endswith(".json"):
            continue
        response = client.get_object(bucket, item.object_name)
        try:
            value = json.loads(response.read().decode("utf-8"))
        finally:
            response.close()
            response.release_conn()
        if isinstance(value, dict):
            rows.append({**value, "manifest_key": item.object_name})
    return rows


def qdrant_collections() -> list[dict[str, Any]]:
    qdrant, _, _ = load_provisioning_configs()
    client = build_http_client(qdrant)
    client.health()
    rows = []
    for role in ("reviews", "review_aspects"):
        name = qdrant["collections"][role]
        collection = client.collection(name)
        if collection is None:
            rows.append({"role": role, "name": name, "status": "missing"})
            continue
        config = collection.get("config", {}).get("params", {})
        rows.append(
            {
                "role": role,
                "name": name,
                "status": collection.get("status"),
                "points_count": collection.get("points_count"),
                "vectors": config.get("vectors"),
                "payload_schema": collection.get("payload_schema", {}),
            }
        )
    return rows


def synthetic_fixture_hashes(root: Path) -> tuple[str, ...]:
    if not root.is_dir():
        return ()
    return tuple(
        "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(root.glob("tripadvisor*.json"))
    )


def collect_report(catalog_root: Path):
    spark = build_spark()
    try:
        manifests = bronze_manifests()
        report = build_inventory_report(
            catalog_root=catalog_root,
            manifests=manifests,
            normalization_releases=table_rows(spark, "travel.silver.normalization_run"),
            source_records=table_rows(spark, "travel.silver.source_record"),
            reviews=review_rows(spark),
            sentiment_rows=table_rows(spark, "travel.silver.review_sentiment"),
            aspect_rows=table_rows(spark, "travel.silver.review_aspect_sentiment"),
            enrichment_runs=table_rows(spark, "travel.silver.review_enrichment_run"),
            index_runs=table_rows(spark, "travel.silver.review_index_run"),
            qdrant_collections=qdrant_collections(),
            runtime_status="verified",
            synthetic_input_hashes=synthetic_fixture_hashes(
                Path(os.environ.get("T05A_SYNTHETIC_FIXTURE_ROOT", "tests/fixtures"))
            ),
        )
        return report
    finally:
        spark.stop()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog-root", type=Path, default=Path("docs/data-catalog"))
    parser.add_argument("--json-out", type=Path)
    parser.add_argument("--summary-out", type=Path)
    args = parser.parse_args()
    report = collect_report(args.catalog_root)
    machine = report.to_json()
    summary = human_summary(report)
    if args.json_out:
        args.json_out.write_text(machine, encoding="utf-8")
    if args.summary_out:
        args.summary_out.write_text(summary, encoding="utf-8")
    print(summary, end="")
    print(machine, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
