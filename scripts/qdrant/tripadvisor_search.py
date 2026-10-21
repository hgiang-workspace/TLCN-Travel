# ruff: noqa: E501
"""Publication-gated semantic search over committed TripAdvisor indexes."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

from travel_intelligence.common.configuration import (
    default_config_paths,
    load_validated,
)
from travel_intelligence.qdrant.indexing import MiniLMEmbedder
from travel_intelligence.qdrant.provisioning import build_http_client
from travel_intelligence.qdrant.retrieval import SemanticRetriever


def required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Required environment variable is missing: {name}")
    return value


def build_spark():
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName("t04e-tripadvisor-search")
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
        .getOrCreate()
    )


def committed_ledger_rows(spark: Any) -> list[dict[str, Any]]:
    normalization = {
        str(row["run_id"]): row.asDict(recursive=True)
        for row in spark.table("travel.silver.normalization_run").collect()
        if row["source_id"] == "tripadvisor" and row["status"] == "committed"
    }
    enrichment = [
        row.asDict(recursive=True)
        for row in spark.table("travel.silver.review_enrichment_run").collect()
        if row["source_id"] == "tripadvisor" and row["status"] == "committed"
    ]
    result: list[dict[str, Any]] = []
    for row in spark.table("travel.silver.review_index_run").collect():
        candidate = row.asDict(recursive=True)
        if (
            candidate.get("status") != "committed"
            or candidate.get("source_id") != "tripadvisor"
        ):
            continue
        if (
            candidate["expected_review_point_count"]
            != candidate["actual_review_point_count"]
        ):
            continue
        if (
            candidate["expected_aspect_point_count"]
            != candidate["actual_aspect_point_count"]
        ):
            continue
        release = normalization.get(str(candidate["source_release_id"]))
        refs = (release or {}).get("table_snapshot_refs") or {}
        required_refs = {
            "source_record",
            "place",
            "place_rating",
            "review",
            "source_entity_mapping",
            "normalization_run",
        }
        matching = [
            item
            for item in enrichment
            if item.get("source_release_id") == candidate["source_release_id"]
            and item.get("processing_run_id") == candidate["enrichment_run_id"]
            and int(item["expected_review_count"]) == int(item["actual_review_count"])
            and int(item["expected_aspect_count"]) == int(item["actual_aspect_count"])
        ]
        if release is None or not required_refs.issubset(refs) or len(matching) != 1:
            continue
        processing_id = str(candidate["enrichment_run_id"]).replace("'", "''")
        source_release = str(candidate["source_release_id"]).replace("'", "''")
        review_count = spark.sql(
            "SELECT COUNT(*) AS n FROM travel.silver.review_sentiment "
            f"WHERE source_id='tripadvisor' AND source_release_id='{source_release}' AND processing_run_id='{processing_id}'"
        ).first()["n"]
        aspect_count = spark.sql(
            "SELECT COUNT(*) AS n FROM travel.silver.review_aspect_sentiment "
            f"WHERE source_id='tripadvisor' AND source_release_id='{source_release}' AND processing_run_id='{processing_id}'"
        ).first()["n"]
        candidate["lineage_consistent"] = int(review_count) == int(
            matching[0]["actual_review_count"]
        ) and int(aspect_count) == int(matching[0]["actual_aspect_count"])
        if candidate["lineage_consistent"]:
            result.append(candidate)
    return result


def parse_filters(values: list[str]) -> dict[str, str]:
    filters = {}
    for value in values:
        if "=" not in value:
            raise ValueError("filters must use field=value")
        field, item = value.split("=", 1)
        if not field or not item:
            raise ValueError("filters must use non-empty field=value")
        filters[field] = item
    return filters


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("query")
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument("--score-threshold", type=float)
    parser.add_argument(
        "--collection", choices=("both", "reviews", "review_aspects"), default="both"
    )
    parser.add_argument("--filter", action="append", default=[])
    parser.add_argument("--offset", type=int, default=0)
    args = parser.parse_args()
    paths = default_config_paths()
    embedding, embedding_fp = load_validated("embedding", paths["embedding"])
    qdrant, _ = load_validated("qdrant", paths["qdrant"])
    spark = build_spark()
    try:
        ledger_rows = committed_ledger_rows(spark)
        embedder = MiniLMEmbedder(
            embedding, embedding_fp, Path(__file__).resolve().parents[2]
        )
        client = build_http_client(qdrant)
        client.health()
        result = SemanticRetriever(
            client,
            embedder,
            ledger_rows,
            qdrant["collections"],
        ).search(
            args.query,
            top_k=args.top_k,
            score_threshold=args.score_threshold,
            collection=args.collection,
            filters=parse_filters(args.filter),
            offset=args.offset,
        )
        print(json.dumps(result, ensure_ascii=False, sort_keys=True, default=str))
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
