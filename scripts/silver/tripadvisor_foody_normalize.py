"""Manifest-driven Bronze v2 to Silver driver with bounded driver memory."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import tempfile
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from typing import Any, Iterator

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
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

from travel_intelligence.common.spark import configure_spark_builder
from travel_intelligence.silver.foundation import (
    NORMALIZATION_VERSION,
    SILVER_NAMESPACE,
    SILVER_TABLES,
    committed_replay_is_noop,
    validate_bronze_manifest,
    validate_release_tables,
)
from travel_intelligence.silver.normalization import (
    bronze_parquet_timestamp,
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
        configure_spark_builder(
            SparkSession.builder.appName("t03c-tripadvisor-foody-silver")
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
        .config("spark.sql.session.timeZone", "UTC")
        # MERGE must keep the normalized source distributed.  Broadcasting a
        # wide source frame collects it through the driver block manager.
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .config("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1")
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


@contextmanager
def _bronze_frame(
    spark: SparkSession, bucket: str, manifest: dict[str, Any]
) -> Iterator[Any]:
    with tempfile.NamedTemporaryFile(suffix=".parquet") as local_file:
        _download_with_iceberg_io(
            spark, bucket, manifest["parquet_key"], local_file.name
        )
        frame = spark.read.parquet(local_file.name)
        _validate_bronze_frame(frame, manifest)
        yield frame


def _validate_bronze_frame(frame, manifest: dict[str, Any]) -> None:
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
    if frame.count() != manifest["row_count"]:
        raise ValueError("Bronze row count does not match manifest")
    if frame.filter(frame.batch_id != manifest["batch_id"]).limit(1).count():
        raise ValueError("Bronze row batch identity does not match manifest")
    if frame.filter(frame.source_row_index.isNull()).limit(1).count():
        raise ValueError("Bronze source_row_index contains null")
    index_counts = frame.agg(
        F.count("source_row_index").alias("non_null_count"),
        F.countDistinct("source_row_index").alias("distinct_count"),
    ).collect()[0]
    if index_counts.non_null_count != index_counts.distinct_count:
        raise ValueError("Bronze source_row_index is not unique")
    expected_hashes = manifest["payload_hashes"]
    ordered_hashes = frame.select("source_row_index", "payload_hash").orderBy(
        "source_row_index"
    )
    for position, row in enumerate(ordered_hashes.toLocalIterator()):
        if row.payload_hash != expected_hashes[position]:
            raise ValueError("Bronze payload hashes do not match manifest")


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
            StructField("province_raw", StringType(), True),
            StructField("province_normalized", StringType(), True),
            StructField("province_geo_id", StringType(), True),
            StructField("province_canonical", StringType(), True),
            StructField("province_resolution_status", StringType(), True),
            StructField("province_match_method", StringType(), True),
            StructField("province_rule_version", StringType(), True),
            StructField("province_correction_id", StringType(), True),
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


def _ensure_place_province_columns(spark: SparkSession) -> None:
    """Add canonical province columns to an existing Iceberg place table."""
    target = f"travel.{SILVER_NAMESPACE}.place"
    existing = set(spark.table(target).columns)
    additions = (
        ("province_raw", "STRING"),
        ("province_normalized", "STRING"),
        ("province_geo_id", "STRING"),
        ("province_canonical", "STRING"),
        ("province_resolution_status", "STRING"),
        ("province_match_method", "STRING"),
        ("province_rule_version", "STRING"),
        ("province_correction_id", "STRING"),
    )
    missing = [
        f"{name} {data_type}" for name, data_type in additions if name not in existing
    ]
    if missing:
        spark.sql(f"ALTER TABLE {target} ADD COLUMNS ({', '.join(missing)})")


def _decimalize(record: dict[str, Any], schema: StructType) -> dict[str, Any]:
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


def _normalized_table_frame(
    frame,
    normalizer,
    table: str,
    parquet_key: str,
    schema: StructType,
):
    """Normalize one target table on Spark workers without driver collection."""
    decimal_fields = tuple(
        field.name for field in schema.fields if isinstance(field.dataType, DecimalType)
    )

    def normalize_partition(rows):
        for row in rows:
            row_values = row.asDict()
            row_values["ingested_at"] = bronze_parquet_timestamp(
                row_values["ingested_at"]
            )
            converted = SimpleNamespace(**row_values)
            normalized = normalizer(converted, parquet_key)
            for record in normalized.get(table, ()):
                result = dict(record)
                for field in decimal_fields:
                    if result.get(field) is not None:
                        result[field] = Decimal(str(result[field]))
                yield result

    return frame.sparkSession.createDataFrame(
        frame.rdd.mapPartitions(normalize_partition), schema=schema
    )


def _merge_frame(
    spark: SparkSession,
    table: str,
    frame,
    key: str,
) -> str:
    view = f"t03c_{table}"
    frame.createOrReplaceTempView(view)
    target = f"travel.{SILVER_NAMESPACE}.{table}"
    columns = frame.columns
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
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=256,
        help="maximum Bronze rows held by the Python driver per normalization chunk",
    )
    args = parser.parse_args()
    if args.chunk_size < 1:
        parser.error("--chunk-size must be positive")
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
        _ensure_place_province_columns(spark)
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

        normalizer = {
            "tripadvisor": normalize_tripadvisor_row,
            "foody": normalize_foody_row,
            "tiktok": normalize_tiktok_row,
            "vexere": normalize_vexere_row,
        }[manifest["source_id"]]
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
            "input_row_count": manifest["row_count"],
            "accepted_row_count": 0,
            "rejected_row_count": 0,
            "nested_input_count": 0,
            "target_counts": {},
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

        table_keys = {
            "source_record": "source_record_sk",
            "place": "place_id",
            "place_rating": "place_rating_id",
            "review": "review_id",
            "source_entity_mapping": "mapping_id",
            "social_content": "content_id",
            "social_engagement": "engagement_id",
            "social_comment": "comment_id",
            "transport_route": "transport_id",
        }
        target_counts: dict[str, int] = {}
        failure_injected = False

        with _bronze_frame(spark, args.bucket, manifest) as frame:
            for table in tables:
                normalized_frame = _normalized_table_frame(
                    frame,
                    normalizer,
                    table,
                    manifest["parquet_key"],
                    schemas[table],
                )
                count = normalized_frame.count()
                target_counts[table] = count
                if count:
                    _merge_frame(spark, table, normalized_frame, table_keys[table])
                if args.fail_after_table == table and not failure_injected:
                    failure_injected = True
                    raise RuntimeError(f"injected failure after {table}")

        processed_rows = manifest["row_count"]
        computed_nested_count = {
            "tripadvisor": target_counts.get("review", 0),
            "tiktok": target_counts.get("social_comment", 0),
            "foody": 0,
            "vexere": 0,
        }[manifest["source_id"]]
        reconcile_manifest_nested_count(manifest, computed_nested_count)
        run.update(
            accepted_row_count=processed_rows,
            nested_input_count=computed_nested_count,
            target_counts=target_counts,
        )
        _merge(
            spark,
            "normalization_run",
            [run],
            schemas["normalization_run"],
            "run_id",
        )
        release_refs = {
            table: snapshot
            for table in tables
            if target_counts.get(table, 0)
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
