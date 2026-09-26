"""Spark utilities cho sentiment pipeline.

Lazy import pyspark để có thể import module này mà không cần Spark cài đặt.
"""

from __future__ import annotations

from typing import Any


def create_spark_session(
    app_name: str = "tlcn-sentiment",
    master: str = "local[*]",
    executor_memory: str = "4g",
    driver_memory: str = "2g",
    default_parallelism: int = 4,
    extra_configs: dict[str, str] | None = None,
):
    """Tạo SparkSession với config chuẩn cho pipeline."""
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(app_name).master(master)

    configs = {
        "spark.executor.memory": executor_memory,
        "spark.driver.memory": driver_memory,
        "spark.default.parallelism": str(default_parallelism),
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    }
    if extra_configs:
        configs.update(extra_configs)

    for k, v in configs.items():
        builder = builder.config(k, v)

    return builder.getOrCreate()


def read_jsonl_spark(spark, path: str, schema_name: str):
    """Đọc JSONL với schema định sẵn."""
    from ingestion.sentiment.schemas import to_spark_struct
    return spark.read.schema(to_spark_struct(schema_name)).json(path)


def write_parquet_spark(df, path: str, mode: str = "overwrite", partition_by: list[str] | None = None):
    """Ghi DataFrame ra Parquet."""
    writer = df.write.mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(path)


def map_in_pandas_udf(
    df,
    func,
    schema_name: str,
    *,
    barrier: bool = True,
):
    """Wrapper mapInPandas với schema output."""
    from ingestion.sentiment.schemas import to_spark_struct
    output_schema = to_spark_struct(schema_name)
    return df.mapInPandas(func, schema=output_schema, barrier=barrier)


__all__ = [
    "create_spark_session",
    "read_jsonl_spark",
    "write_parquet_spark",
    "map_in_pandas_udf",
]