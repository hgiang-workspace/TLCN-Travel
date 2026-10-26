        # ruff: noqa: E501
"""Embed committed TripAdvisor sentiment rows and publish a Qdrant generation."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path
from typing import Any

from travel_intelligence.common.configuration import (
    default_config_paths,
    load_validated,
)
from travel_intelligence.common.spark import configure_spark_builder
from travel_intelligence.qdrant.indexing import (
    MiniLMEmbedder,
    SparkIndexRunLedger,
    build_aspect_points,
    build_review_points,
    generation_id,
    reconcile_points,
    upsert_with_retry,
)
from travel_intelligence.qdrant.manifest import build_manifest, write_manifest
from travel_intelligence.qdrant.provisioning import (
    build_http_client,
    provision,
)


def required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Required environment variable is missing: {name}")
    return value


def build_spark():
    from pyspark.sql import SparkSession

    return (
        configure_spark_builder(SparkSession.builder.appName("t04d-tripadvisor-index"))
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.travel", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.travel.type", "jdbc")
        .config("spark.sql.catalog.travel.uri", "jdbc:postgresql://postgres:5432/iceberg_catalog")
        .config("spark.sql.catalog.travel.warehouse", "s3://travel-lakehouse/warehouse/")
        .config("spark.sql.catalog.travel.jdbc.user", required("ICEBERG_DB_USER"))
        .config("spark.sql.catalog.travel.jdbc.password", required("ICEBERG_DB_PASSWORD"))
        .config("spark.sql.catalog.travel.jdbc.schema-version", "V1")
        .config("spark.sql.catalog.travel.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.travel.s3.endpoint", "http://minio:9000")
        .config("spark.sql.catalog.travel.s3.region", "us-east-1")
        .config("spark.sql.catalog.travel.s3.path-style-access", "true")
        .config("spark.sql.catalog.travel.s3.access-key-id", required("MINIO_ROOT_USER"))
        .config("spark.sql.catalog.travel.s3.secret-access-key", required("MINIO_ROOT_PASSWORD"))
        .getOrCreate()
    )


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _committed_input(spark: Any, source_release_id: str | None, enrichment_run_id: str | None):
    normalization = {
        str(row["run_id"]): row.asDict(recursive=True)
        for row in spark.table("travel.silver.normalization_run").collect()
        if row["source_id"] == "tripadvisor" and row["status"] == "committed"
    }
    runs = [row.asDict(recursive=True) for row in spark.table("travel.silver.review_enrichment_run").collect()]
    eligible_runs = [
        row for row in runs
        if row.get("source_id") == "tripadvisor"
        and row.get("status") == "committed"
        and int(row["expected_review_count"]) == int(row["actual_review_count"])
        and int(row["expected_aspect_count"]) == int(row["actual_aspect_count"])
        and str(row.get("source_release_id")) in normalization
    ]
    if source_release_id:
        eligible_runs = [row for row in eligible_runs if row["source_release_id"] == source_release_id]
    if enrichment_run_id:
        eligible_runs = [row for row in eligible_runs if row["processing_run_id"] == enrichment_run_id]
    if not eligible_runs:
        raise RuntimeError("no complete committed TripAdvisor enrichment generation found")
    source_releases = {str(run["source_release_id"]) for run in eligible_runs}
    if len(source_releases) != 1:
        raise RuntimeError("input selector matched multiple source releases")
    release = normalization[next(iter(source_releases))]
    refs = release.get("table_snapshot_refs") or {}
    required_refs = {"source_record", "place", "place_rating", "review", "source_entity_mapping", "normalization_run"}
    if not required_refs.issubset(refs):
        raise RuntimeError("selected normalization release lacks complete snapshot evidence")
    review_counts = {
        str(row["processing_run_id"]): int(row["observed_count"])
        for row in spark.sql(
            "SELECT processing_run_id, COUNT(DISTINCT review_sentiment_id) AS observed_count "
            "FROM travel.silver.review_sentiment GROUP BY processing_run_id"
        ).collect()
    }
    aspect_counts = {
        str(row["processing_run_id"]): int(row["observed_count"])
        for row in spark.sql(
            "SELECT processing_run_id, COUNT(DISTINCT aspect_sentiment_id) AS observed_count "
            "FROM travel.silver.review_aspect_sentiment GROUP BY processing_run_id"
        ).collect()
    }
    excluded_runs = [
        {
            "processing_run_id": str(run["processing_run_id"]),
            "reason": "physical_result_count_mismatch",
            "expected_review_count": int(run["expected_review_count"]),
            "observed_review_count": review_counts.get(str(run["processing_run_id"]), 0),
            "expected_aspect_count": int(run["expected_aspect_count"]),
            "observed_aspect_count": aspect_counts.get(str(run["processing_run_id"]), 0),
        }
        for run in eligible_runs
        if review_counts.get(str(run["processing_run_id"]), 0) != int(run["expected_review_count"])
        or aspect_counts.get(str(run["processing_run_id"]), 0) != int(run["expected_aspect_count"])
    ]
    eligible_runs = [
        run
        for run in eligible_runs
        if str(run["processing_run_id"])
        not in {item["processing_run_id"] for item in excluded_runs}
    ]
    if not eligible_runs:
        raise RuntimeError("no committed enrichment run has reconciled physical results")
    run_ids = ", ".join(_sql_literal(str(run["processing_run_id"])) for run in eligible_runs)
    predicate = (
        "rs.source_id = 'tripadvisor' AND rs.source_release_id = "
        + _sql_literal(next(iter(source_releases)))
        + " AND rs.processing_run_id IN (" + run_ids + ")"
    )
    reviews = [
        row.asDict(recursive=True)
        for row in spark.sql(
            "SELECT rs.*, r.review_text, p.province_raw, p.province_normalized, "
            "p.province_geo_id, p.province_canonical, "
            "p.province_resolution_status, p.province_match_method, "
            "p.province_rule_version "
            "FROM travel.silver.review_sentiment rs "
            "JOIN travel.silver.review r ON rs.review_id=r.review_id "
            "AND rs.source_record_sk=r.source_record_sk "
            "LEFT JOIN travel.silver.place p ON rs.place_id=p.place_id "
            f"WHERE {predicate}"
        ).collect()
    ]
    aspect_predicate = predicate.replace("rs.", "a.")
    aspects = [
        row.asDict(recursive=True)
        for row in spark.sql(
            f"SELECT a.*, rs.place_id, r.review_text, p.province_raw, "
            "p.province_normalized, p.province_geo_id, p.province_canonical, "
            "p.province_resolution_status, p.province_match_method, "
            "p.province_rule_version "
            "FROM travel.silver.review_aspect_sentiment a "
            "JOIN travel.silver.review_sentiment rs "
            "ON a.review_sentiment_id = rs.review_sentiment_id "
            "JOIN travel.silver.review r "
            "ON rs.review_id = r.review_id AND rs.source_record_sk = r.source_record_sk "
            "LEFT JOIN travel.silver.place p ON rs.place_id = p.place_id "
            f"WHERE {aspect_predicate}"
        ).collect()
    ]
    if len(reviews) != sum(int(run["expected_review_count"]) for run in eligible_runs):
        raise RuntimeError("committed enrichment review counts do not reconcile with result tables")
    if len(aspects) != sum(int(run["expected_aspect_count"]) for run in eligible_runs):
        raise RuntimeError("committed enrichment counts do not reconcile with result tables")
    return eligible_runs, reviews, aspects, excluded_runs


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-release-id")
    parser.add_argument("--enrichment-run-id")
    parser.add_argument(
        "--limit",
        type=int,
        help="Smoke-test only: embed this many reviews and their aspects without publishing a ledger generation.",
    )
    parser.add_argument("--fail-after", choices=("reviews", "aspects"), help=argparse.SUPPRESS)
    parser.add_argument("--index-generation-suffix", help=argparse.SUPPRESS)
    args = parser.parse_args()
    paths = default_config_paths()
    embedding_config, embedding_fp = load_validated("embedding", paths["embedding"])
    qdrant_config, _ = load_validated("qdrant", paths["qdrant"])
    spark = build_spark()
    ledger = SparkIndexRunLedger(spark)
    try:
        # Evolve the existing publication ledger before any model loading or
        # vector generation. This keeps schema failures fail-fast and cheap.
        ledger.create_table()
        runs, review_rows, aspect_rows, excluded_runs = _committed_input(
            spark, args.source_release_id, args.enrichment_run_id
        )
        if args.limit is not None:
            if args.limit <= 0:
                raise ValueError("--limit must be positive")
            review_rows = sorted(
                review_rows, key=lambda row: str(row["review_sentiment_id"])
            )[: args.limit]
            selected_review_ids = {
                str(row["review_sentiment_id"]) for row in review_rows
            }
            aspect_rows = [
                row
                for row in aspect_rows
                if str(row["review_sentiment_id"]) in selected_review_ids
            ]
            embedder = MiniLMEmbedder(
                embedding_config, embedding_fp, Path(__file__).resolve().parents[2]
            )
            smoke_id = f"smoke-limit-{args.limit}"
            generation = generation_id(
                str(runs[0]["source_release_id"]),
                smoke_id,
                embedder.model_fingerprint,
                embedding_fp,
            )
            reviews_by_id = {
                str(row["review_sentiment_id"]): row for row in review_rows
            }
            review_points = build_review_points(
                review_rows,
                embedder,
                generation=generation,
                input_manifest_id=smoke_id,
            )
            aspect_points = build_aspect_points(
                aspect_rows,
                embedder,
                generation=generation,
                reviews_by_id=reviews_by_id,
                input_manifest_id=smoke_id,
            )
            client = build_http_client(qdrant_config)
            client.health()
            provision(client, qdrant_config=qdrant_config, embedding_config=embedding_config)
            batch_size = int(qdrant_config["indexing"]["batch_size"])
            upsert_with_retry(
                client, qdrant_config["collections"]["reviews"], review_points,
                batch_size=batch_size,
            )
            upsert_with_retry(
                client, qdrant_config["collections"]["review_aspects"], aspect_points,
                batch_size=batch_size,
            )
            print(json.dumps({
                "status": "smoke_loaded",
                "review_input_count": len(review_rows),
                "aspect_input_count": len(aspect_rows),
                "review_point_count": reconcile_points(
                    client, qdrant_config["collections"]["reviews"], review_points
                ),
                "aspect_point_count": reconcile_points(
                    client, qdrant_config["collections"]["review_aspects"], aspect_points
                ),
                "index_generation": generation,
                "warning": "smoke load is not recorded in review_index_run and is not an active release",
            }))
            return 0
        manifest = build_manifest(
            source_id="tripadvisor",
            source_release_id=str(runs[0]["source_release_id"]),
            runs=runs,
        )
        from minio import Minio

        minio_client = Minio(
            os.environ.get("MINIO_ENDPOINT", "minio:9000"),
            access_key=required("MINIO_ROOT_USER"),
            secret_key=required("MINIO_ROOT_PASSWORD"),
            secure=os.environ.get("MINIO_SECURE", "false").lower() == "true",
        )
        manifest_uri, manifest_sha256 = write_manifest(
            minio_client,
            bucket=os.environ.get("MINIO_BUCKET", "travel-lakehouse"),
            prefix=os.environ.get("QDRANT_MANIFEST_PREFIX", "qdrant-input-manifests"),
            manifest=manifest,
        )
        embedder = MiniLMEmbedder(embedding_config, embedding_fp, Path(__file__).resolve().parents[2])
        generation = generation_id(
            str(runs[0]["source_release_id"]), str(manifest["manifest_id"]),
            embedder.model_fingerprint, embedding_fp,
        )
        if args.index_generation_suffix:
            generation = hashlib.sha256(
                f"{generation}|{args.index_generation_suffix}".encode()
            ).hexdigest()
        reviews_by_id = {str(row["review_sentiment_id"]): row for row in review_rows}
        review_points = build_review_points(
            review_rows, embedder, generation=generation,
            input_manifest_id=str(manifest["manifest_id"]),
        )
        aspect_points = build_aspect_points(
            aspect_rows, embedder, generation=generation, reviews_by_id=reviews_by_id,
            input_manifest_id=str(manifest["manifest_id"]),
        )
        row = {
            "index_generation": generation,
            "source_id": "tripadvisor",
            "source_release_id": str(runs[0]["source_release_id"]),
            "enrichment_run_id": str(runs[0]["processing_run_id"]) if len(runs) == 1 else None,
            "embedding_model_id": embedder.model_id,
            "embedding_model_revision": embedder.revision,
            "embedding_model_fingerprint": embedder.model_fingerprint,
            "embedding_config_fingerprint": embedding_fp,
            "expected_review_point_count": len(review_points),
            "expected_aspect_point_count": len(aspect_points),
            "review_collection": qdrant_config["collections"]["reviews"],
            "aspect_collection": qdrant_config["collections"]["review_aspects"],
            "input_manifest_uri": manifest_uri,
            "input_manifest_sha256": manifest_sha256,
            "input_manifest_id": str(manifest["manifest_id"]),
            "input_run_count": len(runs),
            "expected_input_review_count": int(manifest["expected_input_review_count"]),
            "expected_input_aspect_count": int(manifest["expected_input_aspect_count"]),
        }
        prior = ledger.get(generation)
        if prior and prior.get("status") == "committed":
            print(json.dumps({**prior, "replayed": True}, default=str))
            return 0
        ledger.begin(row)
        client = build_http_client(qdrant_config)
        client.health()
        provision(client, qdrant_config=qdrant_config, embedding_config=embedding_config)
        batch_size = int(qdrant_config["indexing"]["batch_size"])
        try:
            upsert_with_retry(client, row["review_collection"], review_points, batch_size=batch_size)
            if args.fail_after == "reviews":
                raise RuntimeError("controlled failure after review point upsert")
            upsert_with_retry(client, row["aspect_collection"], aspect_points, batch_size=batch_size)
            if args.fail_after == "aspects":
                raise RuntimeError("controlled failure after aspect point upsert")
            actual_reviews = reconcile_points(client, row["review_collection"], review_points)
            actual_aspects = reconcile_points(client, row["aspect_collection"], aspect_points)
            ledger.finish(row, actual_reviews, actual_aspects)
        except Exception as error:
            review_count = sum(client.point(row["review_collection"], item[0]) is not None for item in review_points)
            aspect_count = sum(client.point(row["aspect_collection"], item[0]) is not None for item in aspect_points)
            ledger.fail(row, str(error), review_count, aspect_count)
            raise
        print(json.dumps({
            **row,
            "status": "committed",
            "actual_review_point_count": actual_reviews,
            "actual_aspect_point_count": actual_aspects,
            "excluded_runs": excluded_runs,
            "replayed": False,
        }, default=str))
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
