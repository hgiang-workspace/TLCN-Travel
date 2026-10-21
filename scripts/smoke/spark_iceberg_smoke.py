import os
import sys

from pyspark.sql import SparkSession


def required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Required environment variable is missing: {name}")
    return value


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("g01-iceberg-smoke")
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
        .config("spark.sql.catalog.travel.warehouse", "s3://travel-lakehouse/warehouse/")
        .config("spark.sql.catalog.travel.jdbc.user", required_env("ICEBERG_DB_USER"))
        .config(
            "spark.sql.catalog.travel.jdbc.password",
            required_env("ICEBERG_DB_PASSWORD"),
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
            required_env("MINIO_ROOT_USER"),
        )
        .config(
            "spark.sql.catalog.travel.s3.secret-access-key",
            required_env("MINIO_ROOT_PASSWORD"),
        )
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def initial(spark: SparkSession) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS travel.integration_test")
    spark.sql(
        """
        CREATE TABLE travel.integration_test.iceberg_smoke (
            id BIGINT,
            entity_name STRING,
            score INT,
            batch_id STRING
        ) USING iceberg
        TBLPROPERTIES ('format-version' = '2')
        """
    )
    spark.sql(
        """
        INSERT INTO travel.integration_test.iceberg_smoke VALUES
            (1, 'Da Lat', 4, 'b001'),
            (2, 'Hoi An', 5, 'b001'),
            (3, 'Da Nang', 3, 'b001')
        """
    )


def update(spark: SparkSession) -> None:
    spark.sql(
        "UPDATE travel.integration_test.iceberg_smoke SET score = 5 WHERE id = 3"
    )


def append(spark: SparkSession) -> None:
    spark.sql(
        """
        INSERT INTO travel.integration_test.iceberg_smoke VALUES
            (4, 'Hue', 4, 'b002')
        """
    )


def verify(spark: SparkSession) -> None:
    spark.sql(
        """
        SELECT id, entity_name, score, batch_id
        FROM travel.integration_test.iceberg_smoke
        ORDER BY id
        """
    ).show(truncate=False)


def main() -> None:
    actions = {"initial", "update", "append", "verify"}
    if len(sys.argv) != 2 or sys.argv[1] not in actions:
        raise SystemExit("usage: spark_iceberg_smoke.py {initial|update|append|verify}")

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        globals()[sys.argv[1]](spark)
        print(f"G01 Spark action completed: {sys.argv[1]}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
