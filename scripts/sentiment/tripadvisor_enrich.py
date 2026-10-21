# ruff: noqa: E501
"""Run T04-C against a committed TripAdvisor Silver release only."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path

from travel_intelligence.common.configuration import (
    default_config_paths,
    load_validated,
)
from travel_intelligence.sentiment.enrichment import (
    AspectSegmenter,
    ViSoBERTClassifier,
    enrich_reviews,
    input_fingerprint,
    publish_enrichment,
)
from travel_intelligence.sentiment.spark_store import SparkIcebergEnrichmentStore
from travel_intelligence.silver.tripadvisor_source import (
    TRIPADVISOR_REVIEW_SQL,
    select_tripadvisor_reviews,
)


def required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Required environment variable is missing: {name}")
    return value


def build_spark():
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName("t04c-tripadvisor-sentiment")
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


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source-release-id",
        help="normalization_run.run_id; defaults to the latest committed TripAdvisor release",
    )
    parser.add_argument("--processing-run-id")
    parser.add_argument(
        "--occurrence-key",
        action="append",
        help="source_record_sk|batch_id|review_id|source_row_index|review_ordinal",
    )
    parser.add_argument(
        "--fail-after", choices=("reviews", "aspects"), help=argparse.SUPPRESS
    )
    args = parser.parse_args()

    paths = default_config_paths()
    pipeline, pipeline_fp = load_validated("pipeline", paths["pipeline"])
    aspects, aspect_fp = load_validated("aspect_keywords", paths["aspect_keywords"])
    model_config, model_config_fp = load_validated("visobert", paths["visobert"])
    spark = build_spark()
    try:
        runs = [
            row.asDict(recursive=True)
            for row in spark.table("travel.silver.normalization_run").collect()
        ]
        committed = {
            str(run["bronze_batch_id"]): run
            for run in runs
            if run.get("source_id") == "tripadvisor"
            and run.get("status") == "committed"
        }
        if args.source_release_id:
            committed = {
                batch: run
                for batch, run in committed.items()
                if str(run.get("run_id")) == args.source_release_id
            }
        if not committed:
            raise RuntimeError(
                "no committed TripAdvisor release with complete source evidence was found"
            )
        rows = [
            row.asDict(recursive=True)
            for row in spark.sql(TRIPADVISOR_REVIEW_SQL).collect()
        ]
        occurrences = select_tripadvisor_reviews(rows, committed)
        if args.occurrence_key:
            wanted = set(args.occurrence_key)
            occurrences = tuple(
                occurrence
                for occurrence in occurrences
                if "|".join(
                    (
                        occurrence.source_record_sk,
                        occurrence.batch_id,
                        occurrence.review_id,
                        str(occurrence.source_row_index),
                        str(occurrence.review_ordinal),
                    )
                )
                in wanted
            )
        if not occurrences:
            raise RuntimeError(
                "the selected committed TripAdvisor release contains no eligible review text"
            )
        classifier = ViSoBERTClassifier.from_config(
            model_config,
            model_config_fp,
            repository_root=Path(__file__).resolve().parents[2],
        )
        segmenter = AspectSegmenter(pipeline, aspects)
        run_id = (
            args.processing_run_id
            or hashlib.sha256(
                f"{occurrences[0].source_release_id}|{classifier.model_fingerprint}|{pipeline_fp}|{aspect_fp}".encode()
            ).hexdigest()
        )
        batch = enrich_reviews(
            occurrences,
            classifier,
            segmenter,
            config_fingerprint=hashlib.sha256(
                f"{pipeline_fp}|{aspect_fp}".encode()
            ).hexdigest(),
            processing_run_id=run_id,
        )
        store = SparkIcebergEnrichmentStore(spark)
        store.create_tables()
        result = publish_enrichment(
            store,
            batch,
            source_release_id=occurrences[0].source_release_id,
            model_id=classifier.model_id,
            model_fingerprint=classifier.model_fingerprint,
            config_fingerprint=hashlib.sha256(
                f"{pipeline_fp}|{aspect_fp}".encode()
            ).hexdigest(),
            input_fingerprint=input_fingerprint(occurrences),
            processing_run_id=run_id,
            fail_after=args.fail_after,
        )
        print(
            json.dumps({**result, "excluded_count": len(batch.exclusions)}, default=str)
        )
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
