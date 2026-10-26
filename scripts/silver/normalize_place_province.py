"""Backfill rule-based province fields on the existing Silver place table."""

from __future__ import annotations

import json
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from travel_intelligence.common.spark import configure_spark_builder
from travel_intelligence.silver.province import normalize_province


def required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Required environment variable is missing: {name}")
    return value


def build_spark() -> SparkSession:
    return (
        configure_spark_builder(
            SparkSession.builder.appName("silver-place-province-normalize")
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
        .config("spark.sql.catalog.travel.warehouse", "s3://travel-lakehouse/warehouse/")
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
        .getOrCreate()
    )


def ensure_columns(spark: SparkSession) -> None:
    target = "travel.silver.place"
    existing = set(spark.table(target).columns)
    additions = (
        ("province_raw", "STRING"),
        ("province_normalized", "STRING"),
        ("province_geo_id", "STRING"),
        ("province_canonical", "STRING"),
        ("province_resolution_status", "STRING"),
        ("province_match_method", "STRING"),
        ("province_rule_version", "STRING"),
    )
    missing = [
        f"{name} {data_type}"
        for name, data_type in additions
        if name not in existing
    ]
    if missing:
        spark.sql(f"ALTER TABLE {target} ADD COLUMNS ({', '.join(missing)})")


def main() -> int:
    spark = build_spark()
    try:
        ensure_columns(spark)
        source = spark.table("travel.silver.place").select("place_id", "province")
        schema = StructType(
            [
                StructField("place_id", StringType(), False),
                StructField("province_raw", StringType(), True),
                StructField("province_normalized", StringType(), True),
                StructField("province_geo_id", StringType(), True),
                StructField("province_canonical", StringType(), True),
                StructField("province_resolution_status", StringType(), False),
                StructField("province_match_method", StringType(), False),
                StructField("province_rule_version", StringType(), False),
            ]
        )

        def map_row(row):
            result = normalize_province(row["province"])
            return (
                row["place_id"],
                result["province_raw"],
                result["province_normalized"],
                result["province_geo_id"],
                result["province_canonical"],
                result["province_resolution_status"],
                result["province_match_method"],
                result["province_rule_version"],
            )

        mapped = spark.createDataFrame(source.rdd.map(map_row), schema=schema)
        mapped.createOrReplaceTempView("place_province_mapping")
        spark.sql(
            """
            MERGE INTO travel.silver.place target
            USING place_province_mapping source
            ON target.place_id = source.place_id
            WHEN MATCHED THEN UPDATE SET
              target.province_raw = source.province_raw,
              target.province_normalized = source.province_normalized,
              target.province_geo_id = source.province_geo_id,
              target.province_canonical = source.province_canonical,
              target.province_resolution_status = source.province_resolution_status,
              target.province_match_method = source.province_match_method,
              target.province_rule_version = source.province_rule_version
            """
        )
        summary = {
            row["province_resolution_status"]: row["count"]
            for row in mapped.groupBy("province_resolution_status")
            .count()
            .collect()
        }
        print(
            json.dumps(
                {"status": "committed", "place_count": mapped.count(), **summary}
            )
        )
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
