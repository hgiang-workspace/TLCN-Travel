#!/usr/bin/env python3
"""Plan or explicitly execute the TASK-004 TripAdvisor sentiment backfill."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
from typing import Any

from travel_intelligence.backfill.sentiment import (
    VISOBERT_MODEL_ID,
    VISOBERT_MODEL_REVISION,
    execute_sentiment_plan,
    human_summary,
    plan_sentiment_backfill,
    subprocess_runner,
)
from travel_intelligence.common.configuration import (
    default_config_paths,
    load_validated,
)
from travel_intelligence.common.spark import configure_spark_builder
from travel_intelligence.silver.tripadvisor_source import (
    TRIPADVISOR_REVIEW_SQL,
    select_tripadvisor_reviews,
)


def required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Required environment variable is missing: {name}")
    return value


def build_spark() -> Any:
    from pyspark.sql import SparkSession

    return (
        configure_spark_builder(
            SparkSession.builder.appName("t05d-tripadvisor-sentiment-plan")
        )
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
            "spark.sql.catalog.travel.warehouse",
            "s3://travel-lakehouse/warehouse/",
        )
        .config("spark.sql.catalog.travel.jdbc.user", required("ICEBERG_DB_USER"))
        .config(
            "spark.sql.catalog.travel.jdbc.password",
            required("ICEBERG_DB_PASSWORD"),
        )
        .config("spark.sql.catalog.travel.jdbc.schema-version", "V1")
        .config(
            "spark.sql.catalog.travel.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        .config("spark.sql.catalog.travel.s3.endpoint", "http://minio:9000")
        .config("spark.sql.catalog.travel.s3.region", "us-east-1")
        .config("spark.sql.catalog.travel.s3.path-style-access", "true")
        .config(
            "spark.sql.catalog.travel.s3.access-key-id",
            required("MINIO_ROOT_USER"),
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


def snapshot_exists(spark: Any, table: str, snapshot_id: str) -> bool:
    rows = spark.sql(
        f"SELECT snapshot_id FROM travel.silver.{table}.snapshots"
    ).collect()
    return any(str(row.snapshot_id) == str(snapshot_id) for row in rows)


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


def synthetic_fixture_hashes(root: Path) -> tuple[str, ...]:
    if not root.is_dir():
        return ()
    return tuple(
        "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(root.glob("tripadvisor*.json"))
    )


def collect_plan(args: argparse.Namespace):
    paths = default_config_paths()
    pipeline, pipeline_fp = load_validated("pipeline", paths["pipeline"])
    aspects, aspect_fp = load_validated("aspect_keywords", paths["aspect_keywords"])
    model_config, _ = load_validated("visobert", paths["visobert"])
    config_fingerprint = hashlib.sha256(
        f"{pipeline_fp}|{aspect_fp}".encode()
    ).hexdigest()
    if model_config["model"]["id"] != VISOBERT_MODEL_ID:
        raise RuntimeError("configured ViSoBERT model does not match TASK-005D")
    if model_config["model"]["revision"] != VISOBERT_MODEL_REVISION:
        raise RuntimeError("configured ViSoBERT revision does not match TASK-005D")
    spark = build_spark()
    try:
        normalization_runs = table_rows(spark, "travel.silver.normalization_run")
        committed = {
            str(run["bronze_batch_id"]): run
            for run in normalization_runs
            if run.get("source_id") == "tripadvisor"
        }
        rows = [
            row.asDict(recursive=True)
            for row in spark.sql(TRIPADVISOR_REVIEW_SQL).collect()
        ]
        occurrences = select_tripadvisor_reviews(
            rows,
            committed,
            snapshot_exists=lambda table, snapshot: snapshot_exists(
                spark, table, snapshot
            ),
        )
        selected_keys = set(args.occurrence_key)
        if selected_keys:
            occurrences = tuple(
                item
                for item in occurrences
                if occurrence_argument(item) in selected_keys
            )
        return plan_sentiment_backfill(
            occurrences,
            manifests=bronze_manifests(),
            normalization_runs=normalization_runs,
            enrichment_runs=table_rows(spark, "travel.silver.review_enrichment_run"),
            sentiment_rows=table_rows(spark, "travel.silver.review_sentiment"),
            aspect_rows=table_rows(spark, "travel.silver.review_aspect_sentiment"),
            model_id=VISOBERT_MODEL_ID,
            model_revision=VISOBERT_MODEL_REVISION,
            config_fingerprint=config_fingerprint,
            synthetic_input_hashes=synthetic_fixture_hashes(args.fixture_root),
            max_occurrences=args.max_occurrences,
            snapshot_exists=lambda table, snapshot: snapshot_exists(
                spark, table, snapshot
            ),
        )
    finally:
        spark.stop()


def occurrence_argument(occurrence: Any) -> str:
    return "|".join(
        (
            occurrence.source_record_sk,
            occurrence.batch_id,
            occurrence.review_id,
            str(occurrence.source_row_index),
            str(occurrence.review_ordinal),
        )
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture-root", type=Path, default=Path("tests/fixtures"))
    parser.add_argument("--json-out", type=Path)
    parser.add_argument("--occurrence-key", action="append", default=[])
    parser.add_argument("--max-occurrences", type=int)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--confirm-writes", action="store_true")
    parser.add_argument(
        "--enrichment-script",
        default="/opt/t04c/tripadvisor_enrich.py",
    )
    args = parser.parse_args()
    if args.execute and not args.confirm_writes:
        parser.error("--execute requires --confirm-writes")
    plan = collect_plan(args)
    result: dict[str, Any] = {"mode": "dry-run", "plan": plan.to_dict()}
    if args.execute:
        result["mode"] = "execute"
        result["execution"] = execute_sentiment_plan(
            plan,
            confirm_writes=args.confirm_writes,
            runner=subprocess_runner(script=args.enrichment_script),
        ).to_dict()
    print(human_summary(plan, mode=result["mode"]), end="")
    output = json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    if args.json_out:
        args.json_out.write_text(output, encoding="utf-8")
    print(output, end="")
    return 1 if result.get("execution", {}).get("failed_occurrence_keys") else 0


if __name__ == "__main__":
    raise SystemExit(main())
