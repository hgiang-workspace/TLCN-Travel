#!/usr/bin/env python3
"""Plan or explicitly execute the TASK-003 TripAdvisor Silver backfill."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
from typing import Any

from travel_intelligence.backfill.normalization import (
    execute_plan,
    human_summary,
    plan_backfill,
    subprocess_runner,
)
from travel_intelligence.common.spark import configure_spark_builder


def required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Required environment variable is missing: {name}")
    return value


def bronze_manifests() -> list[dict[str, Any]]:
    from minio import Minio

    client = Minio(
        os.environ.get("MINIO_ENDPOINT", "minio:9000"),
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


def build_spark() -> Any:
    from pyspark.sql import SparkSession

    return (
        configure_spark_builder(
            SparkSession.builder.appName("t05c-tripadvisor-normalization-plan")
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.travel", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.travel.type", "jdbc")
        .config("spark.sql.catalog.travel.uri", "jdbc:postgresql://postgres:5432/iceberg_catalog")
        .config("spark.sql.catalog.travel.warehouse", "s3://travel-lakehouse/warehouse/")
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
    if not spark.catalog.tableExists(table):
        return []
    return [row.asDict(recursive=True) for row in spark.table(table).collect()]


def synthetic_fixture_hashes(root: Path) -> tuple[str, ...]:
    if not root.is_dir():
        return ()
    return tuple(
        "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(root.glob("tripadvisor*.json"))
    )


def collect_plan(args: argparse.Namespace):
    spark = build_spark()
    try:
        return plan_backfill(
            bronze_manifests(),
            table_rows(spark, "travel.silver.normalization_run"),
            synthetic_input_hashes=synthetic_fixture_hashes(args.fixture_root),
            batch_ids=args.batch_id,
            max_batches=args.max_batches,
        )
    finally:
        spark.stop()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json-out", type=Path)
    parser.add_argument("--fixture-root", type=Path, default=Path("tests/fixtures"))
    parser.add_argument("--batch-id", action="append", default=[])
    parser.add_argument("--max-batches", type=int)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--confirm-writes", action="store_true")
    parser.add_argument(
        "--normalizer-script",
        default="/opt/t03c/tripadvisor_foody_normalize.py",
    )
    args = parser.parse_args()
    if args.execute and not args.confirm_writes:
        parser.error("--execute requires --confirm-writes")
    plan = collect_plan(args)
    result: dict[str, Any] = {"plan": plan.to_dict(), "mode": "dry-run"}
    if args.execute:
        execution = execute_plan(
            plan,
            confirm_writes=args.confirm_writes,
            runner=subprocess_runner(
                script=args.normalizer_script,
                bucket=os.environ.get("MINIO_BUCKET", "travel-lakehouse"),
            ),
        )
        result["mode"] = "execute"
        result["execution"] = execution.to_dict()
    print(human_summary(plan, mode=result["mode"]), end="")
    output = json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    if args.json_out:
        args.json_out.write_text(output, encoding="utf-8")
    print(output, end="")
    return 1 if result.get("execution", {}).get("failed_batch_ids") else 0


if __name__ == "__main__":
    raise SystemExit(main())
