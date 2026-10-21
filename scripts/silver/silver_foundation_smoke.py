"""Create and verify the synthetic Silver foundation tables through Spark."""

from __future__ import annotations

import os
import re

from pyspark.sql import SparkSession

from travel_intelligence.silver.foundation import SILVER_NAMESPACE, SILVER_TABLES


def required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Required environment variable is missing: {name}")
    return value


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("t03b-silver-foundation")
        .master("local[2]")
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


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS travel.{SILVER_NAMESPACE}")
        for table in SILVER_TABLES:
            spark.sql(table.create_sql())
            expected_columns = re.findall(
                r"(?m)(?:^|,\s*)([a-z_][a-z0-9_]*)\s+"
                r"(?=(?:STRING|BIGINT|INT|DOUBLE|BOOLEAN|DECIMAL|TIMESTAMP|DATE|ARRAY|MAP))",
                table.columns_sql,
            )
            actual_columns = spark.table(
                f"travel.{SILVER_NAMESPACE}.{table.name}"
            ).columns
            if actual_columns != expected_columns:
                raise RuntimeError(
                    f"Silver schema mismatch for {table.name}: "
                    f"expected {expected_columns}, got {actual_columns}"
                )
        actual = {
            row.tableName
            for row in spark.sql(f"SHOW TABLES IN travel.{SILVER_NAMESPACE}").collect()
        }
        expected = {table.name for table in SILVER_TABLES}
        if not expected.issubset(actual):
            raise RuntimeError(
                f"Silver foundation tables missing: {sorted(expected - actual)}"
            )
        print(f"T03-B Silver foundation tables verified: {len(expected)}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
