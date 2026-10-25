"""Shared Spark master and driver-network configuration."""

from __future__ import annotations

import os
from typing import Any


def configure_spark_builder(builder: Any) -> Any:
    """Configure local development or the Compose Spark Standalone cluster."""
    master = os.environ.get("SPARK_MASTER_URL", "local[2]")
    configured = builder.master(master)
    if not master.startswith("spark://"):
        return configured

    python_path = os.environ.get("PYTHONPATH", "/workspace/src")
    return (
        configured.config(
            "spark.driver.bindAddress",
            os.environ.get("SPARK_DRIVER_BIND_ADDRESS", "0.0.0.0"),
        )
        .config("spark.driver.host", os.environ.get("SPARK_DRIVER_HOST", "spark"))
        .config(
            "spark.driver.port", os.environ.get("SPARK_DRIVER_PORT", "37001")
        )
        .config(
            "spark.blockManager.port",
            os.environ.get("SPARK_BLOCK_MANAGER_PORT", "37002"),
        )
        .config(
            "spark.cores.max", os.environ.get("SPARK_APPLICATION_CORES", "2")
        )
        .config(
            "spark.executor.cores", os.environ.get("SPARK_EXECUTOR_CORES", "2")
        )
        .config(
            "spark.executor.memory",
            os.environ.get("SPARK_EXECUTOR_MEMORY", "2g"),
        )
        .config("spark.executor.instances", "1")
        .config("spark.executorEnv.PYTHONPATH", python_path)
        .config("spark.pyspark.python", "/usr/bin/python3")
        .config("spark.pyspark.driver.python", "/usr/bin/python3")
    )


__all__ = ["configure_spark_builder"]
