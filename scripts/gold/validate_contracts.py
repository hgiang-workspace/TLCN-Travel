#!/usr/bin/env python3
"""Create and inspect TASK-007B contracts in an isolated Iceberg namespace."""

from __future__ import annotations

import json
import os

from travel_intelligence.common.spark import configure_spark_builder
from travel_intelligence.gold.configuration import load_gold_configuration
from travel_intelligence.gold.contracts import (
    GOLD_TABLES,
    VALIDATION_NAMESPACE,
    contract_fingerprint,
    validation_ddl,
)


def required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"missing required environment variable: {name}")
    return value


def build_spark():
    from pyspark.sql import SparkSession

    return (
        configure_spark_builder(
            SparkSession.builder.appName("t07b-gold-contract-validation")
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
        .getOrCreate()
    )


def main() -> int:
    _, config_fingerprint = load_gold_configuration(
        os.environ.get("PROJECT_ROOT", "/workspace")
    )
    spark = build_spark()
    try:
        for statement in validation_ddl():
            spark.sql(statement)
        observed = {}
        for table in GOLD_TABLES:
            fields = spark.table(table.qualified_name(VALIDATION_NAMESPACE)).schema
            observed[table.name] = [
                {
                    "name": field.name,
                    "type": field.dataType.simpleString(),
                    "nullable": field.nullable,
                }
                for field in fields
            ]
        print(
            json.dumps(
                {
                    "status": "validated",
                    "namespace": f"travel.{VALIDATION_NAMESPACE}",
                    "table_count": len(observed),
                    "tables": observed,
                    "contract_fingerprint": contract_fingerprint(),
                    "config_fingerprint": config_fingerprint,
                },
                sort_keys=True,
            )
        )
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
