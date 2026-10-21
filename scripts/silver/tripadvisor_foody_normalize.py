"""Manifest-driven synthetic TripAdvisor/Foody Bronze v2 to Silver driver."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import tempfile
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from travel_intelligence.silver.foundation import (
    NORMALIZATION_VERSION,
    SILVER_NAMESPACE,
    SILVER_TABLES,
    committed_replay_is_noop,
    execute_table_writes,
    validate_bronze_manifest,
    validate_release_tables,
)
from travel_intelligence.silver.normalization import (
    bronze_parquet_timestamp,
    nested_input_count,
    normalize_foody_row,
    normalize_tiktok_row,
    normalize_tripadvisor_row,
    normalize_vexere_row,
    reconcile_manifest_nested_count,
)


def required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Required environment variable is missing: {name}")
    return value


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("t03c-tripadvisor-foody-silver")
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
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def _download_with_iceberg_io(
    spark: SparkSession, bucket: str, key: str, destination: str
) -> None:
    jvm = spark._jvm
    properties = jvm.java.util.HashMap()
    properties.put("s3.endpoint", "http://minio:9000")
    properties.put("s3.region", "us-east-1")
    properties.put("s3.path-style-access", "true")
    properties.put("s3.access-key-id", required("MINIO_ROOT_USER"))
    properties.put("s3.secret-access-key", required("MINIO_ROOT_PASSWORD"))
    file_io = jvm.org.apache.iceberg.aws.s3.S3FileIO()
    input_stream = None
    output_stream = None
    try:
        file_io.initialize(properties)
        input_file = file_io.newInputFile(f"s3://{bucket}/{key}")
        input_stream = input_file.newStream()
        # Use single-argument Java constructors. Paths.get(String, String...)
        # is exposed as a varargs overload Py4J cannot resolve reliably here.
        output_stream = jvm.java.io.FileOutputStream(jvm.java.io.File(destination))
        input_stream.transferTo(output_stream)
    finally:
        if output_stream is not None:
            output_stream.close()
        if input_stream is not None:
            input_stream.close()
        file_io.close()


def _manifest(spark: SparkSession, bucket: str, key: str) -> dict[str, Any]:
    with tempfile.NamedTemporaryFile(suffix=".json") as local_file:
        _download_with_iceberg_io(spark, bucket, key, local_file.name)
        local_file.seek(0)
        result = json.load(local_file)
    validate_bronze_manifest(result)
    return result


def _bronze_rows(spark: SparkSession, bucket: str, manifest: dict[str, Any]):
    with tempfile.NamedTemporaryFile(suffix=".parquet") as local_file:
        _download_with_iceberg_io(
            spark, bucket, manifest["parquet_key"], local_file.name
        )
        frame = spark.read.parquet(local_file.name)
        return _validate_bronze_rows(frame, manifest)


def _validate_bronze_rows(frame, manifest: dict[str, Any]):
    expected = {
        "source_id",
        "record_type",
        "source_record_id",
        "source_uri",
        "batch_id",
        "ingested_at",
        "source_collected_at_raw",
        "source_file",
        "payload_hash",
        "payload_json",
        "source_row_index",
    }
    if set(frame.columns) != expected:
        raise ValueError("Bronze object does not have the exact v2 columns")
    if frame.schema["source_row_index"].dataType != LongType():
        raise ValueError("Bronze source_row_index is not BIGINT")
    rows = frame.collect()
    if len(rows) != manifest["row_count"]:
        raise ValueError("Bronze row count does not match manifest")
    if any(row.batch_id != manifest["batch_id"] for row in rows):
        raise ValueError("Bronze row batch identity does not match manifest")
    if any(row.source_row_index is None for row in rows):
        raise ValueError("Bronze source_row_index contains null")
    indices = [row.source_row_index for row in rows]
    if len(set(indices)) != len(indices):
        raise ValueError("Bronze source_row_index is not unique")
    rows = sorted(rows, key=lambda row: row.source_row_index)
    hashes = [row.payload_hash for row in rows]
    if hashes != manifest["payload_hashes"]:
        raise ValueError("Bronze payload hashes do not match manifest")
    return rows


def _schemas() -> dict[str, StructType]:
    quality = ArrayType(StringType(), containsNull=False)
    source = StructType(
        [
            StructField("source_record_sk", StringType(), False),
            StructField("source_id", StringType(), False),
            StructField("source_row_index", LongType(), False),
            StructField("record_type", StringType(), False),
            StructField("source_record_id", StringType(), True),
            StructField("source_uri", StringType(), True),
            StructField("batch_id", StringType(), False),
            StructField("payload_hash", StringType(), False),
            StructField("bronze_object_path", StringType(), False),
            StructField("duplicate_status", StringType(), False),
            StructField("duplicate_of_source_record_sk", StringType(), True),
            StructField("source_collected_at_raw", StringType(), True),
            StructField("ingested_at", TimestampType(), False),
            StructField("normalization_version", StringType(), False),
            StructField("quality_codes", quality, False),
            StructField("processed_at", TimestampType(), False),
            StructField("ingest_date", DateType(), False),
        ]
    )
    place = StructType(
        [
            StructField("place_id", StringType(), False),
            StructField("place_name", StringType(), False),
            StructField("place_type", StringType(), True),
            StructField("province", StringType(), True),
            StructField("district", StringType(), True),
            StructField("address", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("identity_status", StringType(), False),
            StructField("identity_method", StringType(), False),
            StructField("created_at", TimestampType(), False),
            StructField("updated_at", TimestampType(), False),
        ]
    )
    mapping = StructType(
        [
            StructField("mapping_id", StringType(), False),
            StructField("source_record_sk", StringType(), False),
            StructField("place_id", StringType(), True),
            StructField("mapping_method", StringType(), False),
            StructField("confidence", DecimalType(6, 5), True),
            StructField("decision_status", StringType(), False),
            StructField("evidence_ref", StringType(), True),
            StructField("mapping_version", StringType(), False),
            StructField("created_at", TimestampType(), False),
            StructField("updated_at", TimestampType(), False),
        ]
    )
    rating = StructType(
        [
            StructField("place_rating_id", StringType(), False),
            StructField("source_record_sk", StringType(), False),
            StructField("place_id", StringType(), True),
            StructField("source_id", StringType(), False),
            StructField("observed_at", TimestampType(), True),
            StructField("observed_at_raw", StringType(), True),
            StructField("rating", DecimalType(10, 4), True),
            StructField("rating_scale", StringType(), True),
            StructField("review_count", LongType(), True),
            StructField("rating_quality", DecimalType(10, 4), True),
            StructField("rating_price", DecimalType(10, 4), True),
            StructField("rating_service", DecimalType(10, 4), True),
            StructField("rating_location", DecimalType(10, 4), True),
            StructField("rating_space", DecimalType(10, 4), True),
            StructField("price_min", DecimalType(18, 4), True),
            StructField("price_max", DecimalType(18, 4), True),
            StructField("currency", StringType(), True),
            StructField("quality_codes", quality, False),
        ]
    )
    review = StructType(
        [
            StructField("review_id", StringType(), False),
            StructField("source_record_sk", StringType(), False),
            StructField("place_id", StringType(), True),
            StructField("review_ordinal", LongType(), False),
            StructField("source_review_index_raw", StringType(), True),
            StructField("key_review", StringType(), True),
            StructField("review_text", StringType(), True),
            StructField("detail_review", StringType(), True),
            StructField("rating", DecimalType(10, 4), True),
            StructField("language_raw", StringType(), True),
            StructField("quality_codes", quality, False),
        ]
    )
    run = StructType(
        [
            StructField("run_id", StringType(), False),
            StructField("source_id", StringType(), False),
            StructField("bronze_batch_id", StringType(), False),
            StructField("normalization_version", StringType(), False),
            StructField("attempt", IntegerType(), False),
            StructField("status", StringType(), False),
            StructField("input_row_count", LongType(), False),
            StructField("accepted_row_count", LongType(), False),
            StructField("rejected_row_count", LongType(), False),
            StructField("nested_input_count", LongType(), False),
            StructField("target_counts", MapType(StringType(), LongType()), False),
            StructField("input_schema_fingerprint", StringType(), False),
            StructField("target_schema_fingerprint", StringType(), True),
            StructField("started_at", TimestampType(), False),
            StructField("finished_at", TimestampType(), True),
            StructField(
                "table_snapshot_refs", MapType(StringType(), StringType()), True
            ),
            StructField("error_summary", StringType(), True),
        ]
    )
    social_content = StructType(
        [
            StructField("content_id", StringType(), False),
            StructField("source_record_sk", StringType(), False),
            StructField("platform", StringType(), False),
            StructField("source_video_id", StringType(), True),
            StructField("content_type", StringType(), True),
            StructField("description", StringType(), True),
            StructField("keyword_raw", StringType(), True),
            StructField("province_raw", StringType(), True),
            StructField("hashtags", ArrayType(StringType()), True),
            StructField("posted_date_raw", StringType(), True),
            StructField("crawl_time_raw", StringType(), True),
            StructField("source_url", StringType(), True),
            StructField("reported_comment_count", LongType(), True),
            StructField("collected_comment_count", LongType(), False),
            StructField("quality_codes", quality, False),
        ]
    )
    social_comment = StructType(
        [
            StructField("comment_id", StringType(), False),
            StructField("content_id", StringType(), False),
            StructField("source_record_sk", StringType(), False),
            StructField("comment_ordinal", LongType(), False),
            StructField("source_comment_id_raw", StringType(), True),
            StructField("comment_text", StringType(), True),
            StructField("like_count", LongType(), True),
            StructField("posted_time_raw", StringType(), True),
            StructField("quality_codes", quality, False),
        ]
    )
    social_engagement = StructType(
        [
            StructField("engagement_id", StringType(), False),
            StructField("content_id", StringType(), False),
            StructField("source_record_sk", StringType(), False),
            StructField("observed_at_raw", StringType(), True),
            StructField("view_count", LongType(), True),
            StructField("like_count", LongType(), True),
            StructField("comment_count_reported", LongType(), True),
            StructField("share_count", LongType(), True),
            StructField("total_engagement", LongType(), True),
            StructField("quality_codes", quality, False),
        ]
    )
    transport = StructType(
        [
            StructField("transport_id", StringType(), False),
            StructField("source_record_sk", StringType(), False),
            StructField("origin", StringType(), False),
            StructField("destination", StringType(), False),
            StructField("operator_name", StringType(), False),
            StructField("transport_type", StringType(), True),
            StructField("vehicle_type", StringType(), True),
            StructField("departure_time_raw", StringType(), True),
            StructField("arrival_time_raw", StringType(), True),
            StructField("duration_min", IntegerType(), True),
            StructField("price_vnd", DecimalType(18, 2), True),
            StructField("crawl_date_raw", StringType(), True),
            StructField("crawl_date", DateType(), True),
            StructField("source", StringType(), False),
            StructField("source_url", StringType(), True),
            StructField("quality_codes", quality, False),
        ]
    )
    return {
        "normalization_run": run,
        "source_record": source,
        "place": place,
        "source_entity_mapping": mapping,
        "place_rating": rating,
        "review": review,
        "social_content": social_content,
        "social_comment": social_comment,
        "social_engagement": social_engagement,
        "transport_route": transport,
    }


def _decimalize(
    record: dict[str, Any], schema: StructType
) -> dict[str, Any]:
    result = dict(record)
    for field in schema.fields:
        if (
            isinstance(field.dataType, DecimalType)
            and result.get(field.name) is not None
        ):
            result[field.name] = Decimal(str(result[field.name]))
    return result


def _merge(
    spark: SparkSession,
    table: str,
    records: list[dict[str, Any]],
    schema: StructType,
    key: str,
) -> str:
    if not records:
        return "empty"
    view = f"t03c_{table}"
    frame = spark.createDataFrame(
        [_decimalize(record, schema) for record in records], schema=schema
    )
    frame.createOrReplaceTempView(view)
    target = f"travel.{SILVER_NAMESPACE}.{table}"
    columns = [field.name for field in schema.fields]
    insert_columns = ", ".join(columns)
    insert_values = ", ".join(f"s.{column}" for column in columns)
    spark.sql(
        f"MERGE INTO {target} t USING {view} s ON t.{key}=s.{key} "
        f"WHEN MATCHED THEN UPDATE SET * "
        f"WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})"
    )
    return f"{table}-verified"


def _snapshot_ref(spark: SparkSession, table: str) -> str | None:
    rows = spark.sql(
        f"SELECT snapshot_id FROM travel.{SILVER_NAMESPACE}.{table}.snapshots "
        "ORDER BY committed_at DESC LIMIT 1"
    ).collect()
    return str(rows[0].snapshot_id) if rows else None


def _snapshot_exists(spark: SparkSession, table: str, snapshot_id: str) -> bool:
    rows = spark.sql(
        f"SELECT snapshot_id FROM travel.{SILVER_NAMESPACE}.{table}.snapshots"
    ).collect()
    return any(str(row.snapshot_id) == snapshot_id for row in rows)


def _existing_run(spark: SparkSession, batch_id: str) -> dict[str, Any] | None:
    frame = spark.table(f"travel.{SILVER_NAMESPACE}.normalization_run")
    rows = frame.where(
        (frame.bronze_batch_id == batch_id)
        & (frame.normalization_version == NORMALIZATION_VERSION)
    ).collect()
    if len(rows) > 1:
        raise ValueError(
            "Multiple normalization ledger rows exist for the requested batch/version"
        )
    return rows[0].asDict(recursive=True) if rows else None


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest-key", required=True)
    parser.add_argument("--bucket", default="travel-lakehouse")
    parser.add_argument("--fail-after-table")
    args = parser.parse_args()
    if (
        not args.manifest_key.strip()
        or "<" in args.manifest_key
        or ">" in args.manifest_key
        or args.manifest_key.startswith("manifest-key")
        or not args.manifest_key.endswith(".json")
    ):
        parser.error(
            "--manifest-key must be a concrete .json success-manifest object key; "
            "do not pass a placeholder"
        )
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        manifest = _manifest(spark, args.bucket, args.manifest_key)
        if manifest["source_id"] not in {"tripadvisor", "foody", "tiktok", "vexere"}:
            raise ValueError("T03-D accepts only TikTok, Vexere, TripAdvisor, or Foody")
        for table in SILVER_TABLES:
            spark.sql(table.create_sql())
        schemas = _schemas()
        target_tables = {
            "tripadvisor": (
                "source_record",
                "place",
                "place_rating",
                "review",
                "source_entity_mapping",
            ),
            "foody": (
                "source_record",
                "place",
                "place_rating",
                "source_entity_mapping",
            ),
            "tiktok": (
                "source_record",
                "social_content",
                "social_engagement",
                "social_comment",
            ),
            "vexere": ("source_record", "transport_route"),
        }[manifest["source_id"]]
        tables = target_tables
        input_fingerprint = hashlib.sha256(
            json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        target_fingerprint = hashlib.sha256(
            json.dumps(
                {name: schema.json() for name, schema in schemas.items()},
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        existing_run = _existing_run(spark, manifest["batch_id"])
        required_tables = (*tables, "normalization_run")
        if committed_replay_is_noop(
            existing_run,
            source_id=manifest["source_id"],
            batch_id=manifest["batch_id"],
            normalization_version=NORMALIZATION_VERSION,
            input_schema_fingerprint=input_fingerprint,
            target_schema_fingerprint=target_fingerprint,
            required_tables=required_tables,
            snapshot_exists=lambda table, snapshot: _snapshot_exists(
                spark, table, snapshot
            ),
        ):
            print(
                json.dumps(
                    {
                        "batch_id": manifest["batch_id"],
                        "status": "already_committed",
                        "tables": existing_run["table_snapshot_refs"],
                    }
                )
            )
            return

        rows = _bronze_rows(spark, args.bucket, manifest)
        normalizer = {
            "tripadvisor": normalize_tripadvisor_row,
            "foody": normalize_foody_row,
            "tiktok": normalize_tiktok_row,
            "vexere": normalize_vexere_row,
        }[manifest["source_id"]]
        records: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            row_values = row.asDict()
            row_values["ingested_at"] = bronze_parquet_timestamp(
                row_values["ingested_at"]
            )
            converted = SimpleNamespace(**row_values)
            normalized = normalizer(converted, manifest["parquet_key"])
            for table, values in normalized.items():
                records.setdefault(table, []).extend(values)
        computed_nested_count = nested_input_count(manifest["source_id"], records)
        reconcile_manifest_nested_count(manifest, computed_nested_count)
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        run_id = hashlib.sha256(
            f"{manifest['source_id']}|{manifest['batch_id']}|{NORMALIZATION_VERSION}|1".encode()
        ).hexdigest()
        run = {
            "run_id": run_id,
            "source_id": manifest["source_id"],
            "bronze_batch_id": manifest["batch_id"],
            "normalization_version": NORMALIZATION_VERSION,
            "attempt": 1,
            "status": "partial",
            "input_row_count": len(rows),
            "accepted_row_count": len(rows),
            "rejected_row_count": 0,
            "nested_input_count": computed_nested_count,
            "target_counts": {
                table: len(records.get(table, []))
                for table in records
            },
            "input_schema_fingerprint": input_fingerprint,
            "target_schema_fingerprint": target_fingerprint,
            "started_at": now,
            "finished_at": None,
            "table_snapshot_refs": None,
            "error_summary": None,
        }
        _merge(
            spark,
            "normalization_run",
            [run],
            schemas["normalization_run"],
            "run_id",
        )
        def write_table(table: str) -> str:
            key = {
                "source_record": "source_record_sk",
                "place": "place_id",
                "place_rating": "place_rating_id",
                "review": "review_id",
                "source_entity_mapping": "mapping_id",
                "social_content": "content_id",
                "social_engagement": "engagement_id",
                "social_comment": "comment_id",
                "transport_route": "transport_id",
            }[table]
            return _merge(spark, table, records.get(table, []), schemas[table], key)

        execute_table_writes(
            tables,
            write_table,
            fail_after_table=args.fail_after_table,
        )
        release_refs = {
            table: snapshot
            for table in tables
            if records.get(table)
            and (snapshot := _snapshot_ref(spark, table)) is not None
        }
        run_snapshot = _snapshot_ref(spark, "normalization_run")
        if run_snapshot is not None:
            release_refs["normalization_run"] = run_snapshot
        # The required set is the source-specific target contract, not the
        # keys already present in the evidence map.  Passing release_refs.keys
        # here would make an incomplete release validate itself.
        validate_release_tables(required_tables, release_refs)
        run.update(
            status="committed",
            finished_at=datetime.now(timezone.utc).replace(tzinfo=None),
            table_snapshot_refs=release_refs,
        )
        _merge(
            spark,
            "normalization_run",
            [run],
            schemas["normalization_run"],
            "run_id",
        )
        print(
            json.dumps(
                {
                    "batch_id": manifest["batch_id"],
                    "status": "committed",
                    "tables": release_refs,
                }
            )
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
