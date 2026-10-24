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
    EnrichmentError,
    ViSoBERTClassifier,
    enrich_reviews,
    input_fingerprint,
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
    parser.add_argument("--batch-size", type=int, default=128)
    parser.add_argument("--chunk-size", type=int, default=1000)
    parser.add_argument("--max-chunks", type=int)
    parser.add_argument("--partitions", type=int, default=32)
    parser.add_argument(
        "--occurrence-key",
        action="append",
        help="source_record_sk|batch_id|review_id|source_row_index|review_ordinal",
    )
    parser.add_argument(
        "--fail-after", choices=("reviews", "aspects"), help=argparse.SUPPRESS
    )
    args = parser.parse_args()
    if args.batch_size < 1 or args.chunk_size < 1 or args.partitions < 1 or (args.max_chunks is not None and args.max_chunks < 1):
        parser.error("--batch-size, --chunk-size, --partitions and --max-chunks must be positive")

    paths = default_config_paths()
    pipeline, pipeline_fp = load_validated("pipeline", paths["pipeline"])
    aspects, aspect_fp = load_validated("aspect_keywords", paths["aspect_keywords"])
    model_config, model_config_fp = load_validated("visobert", paths["visobert"])
    spark = build_spark()
    try:
        from pyspark.sql import functions as F

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
        if not args.source_release_id:
            latest = max(
                committed.values(),
                key=lambda run: (str(run.get("finished_at") or ""), str(run["run_id"])),
            )
            committed = {
                batch: run
                for batch, run in committed.items()
                if run["run_id"] == latest["run_id"]
            }
        batch_ids = tuple(committed)
        review_frame = spark.sql(TRIPADVISOR_REVIEW_SQL).where(
            F.col("batch_id").isin(*batch_ids)
        ).repartition(args.partitions)
        occurrences = select_tripadvisor_reviews(
            (row.asDict(recursive=True) for row in review_frame.toLocalIterator()),
            committed,
        )
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
        store = SparkIcebergEnrichmentStore(spark)
        store.create_tables()
        config_fingerprint = hashlib.sha256(
            f"{pipeline_fp}|{aspect_fp}".encode()
        ).hexdigest()
        prior_runs = {
            str(row["processing_run_id"]): row.asDict(recursive=True)
            for row in spark.table("travel.silver.review_enrichment_run").collect()
        }
        chunk_count = (len(occurrences) + args.chunk_size - 1) // args.chunk_size
        chunks_to_run = min(chunk_count, args.max_chunks) if args.max_chunks else chunk_count
        results = []
        for chunk_index, start in enumerate(range(0, len(occurrences), args.chunk_size), start=1):
            if chunk_index > chunks_to_run:
                break
            chunk = occurrences[start : start + args.chunk_size]
            expected_aspects = sum(len(segmenter.segment(item.review_text)) for item in chunk)
            run_id = args.processing_run_id if args.processing_run_id and chunk_count == 1 else hashlib.sha256(
                f"{chunk[0].source_release_id}|{classifier.model_fingerprint}|{pipeline_fp}|{aspect_fp}|chunk:{chunk_index}:{chunk_count}".encode()
            ).hexdigest()
            prior = prior_runs.get(run_id)
            if (
                prior
                and prior.get("status") == "committed"
                and int(prior.get("expected_review_count", -1)) == len(chunk)
                and int(prior.get("actual_review_count", -1)) == len(chunk)
                and int(prior.get("expected_aspect_count", -1)) == expected_aspects
                and int(prior.get("actual_aspect_count", -1)) == expected_aspects
            ):
                results.append({**prior, "replayed": True})
                print(json.dumps({**prior, "replayed": True}, default=str), flush=True)
                continue
            run = {
                "processing_run_id": run_id,
                "source_id": "tripadvisor",
                "source_release_id": chunk[0].source_release_id,
                "model_id": classifier.model_id,
                "model_fingerprint": classifier.model_fingerprint,
                "config_fingerprint": config_fingerprint,
                "input_fingerprint": input_fingerprint(chunk),
                "status": "partial",
                "expected_review_count": len(chunk),
                "expected_aspect_count": expected_aspects,
            }
            store.begin(run)
            exclusions = 0
            try:
                batch = enrich_reviews(
                    chunk,
                    classifier,
                    segmenter,
                    config_fingerprint=config_fingerprint,
                    processing_run_id=run_id,
                )
                exclusions = len(batch.exclusions)
                store.upsert_reviews(batch.review_rows)
                if args.fail_after == "reviews":
                    raise EnrichmentError("controlled failure after review write")
                store.upsert_aspects(batch.aspect_rows)
                if args.fail_after == "aspects":
                    raise EnrichmentError("controlled failure after aspect write")
                reconciliation = store.reconcile(run)
                if (
                    reconciliation["review_count"] != len(chunk)
                    or reconciliation["aspect_count"] != expected_aspects
                ):
                    raise EnrichmentError("enrichment result reconciliation failed")
                store.commit(run, reconciliation)
                result = {**run, **reconciliation, "status": "committed", "chunk_index": chunk_index, "chunk_count": chunk_count, "excluded_count": exclusions, "replayed": False}
                results.append(result)
                print(json.dumps(result, default=str), flush=True)
            except Exception as error:
                store.fail(run, str(error))
                raise
        print(json.dumps({"source_release_id": occurrences[0].source_release_id, "chunk_count": chunk_count, "processed_chunks": chunks_to_run, "committed_chunks": len(results)}, default=str), flush=True)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
