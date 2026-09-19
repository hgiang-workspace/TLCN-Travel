"""Silver assets - Bronze → Silver ETL using PySpark.

Asset graph:
  bronze_tripadvisor → silver_tripadvisor → silver_entity
"""

from pathlib import Path
from dagster import asset, Output, MetadataValue

PROJECT_ROOT = Path("/home/hgiang/TLCN")


@asset
def silver_tripadvisor(bronze_tripadvisor: dict) -> dict:
    """Transform Bronze Tripadvisor data to Silver via PySpark.

    Calls spark/jobs/bronze_to_silver.py via spark-submit.
    """
    import subprocess

    job_path = str(PROJECT_ROOT / "spark" / "jobs" / "bronze_to_silver.py")

    result = subprocess.run(
        ["python", job_path],
        capture_output=True,
        text=True,
        timeout=600,
    )

    if result.returncode != 0:
        raise RuntimeError(f"Spark job failed: {result.stderr}")

    yield Output(
        {"status": "completed", "job": "bronze_to_silver"},
        metadata={
            "status": MetadataValue.text("completed"),
            "job": MetadataValue.text("bronze_to_silver"),
        },
    )


@asset
def silver_entity(silver_tripadvisor: dict) -> dict:
    """Extract entities from Silver data via PySpark.

    Calls spark/jobs/entity_processing.py via spark-submit.
    """
    import subprocess

    job_path = str(PROJECT_ROOT / "spark" / "jobs" / "entity_processing.py")

    result = subprocess.run(
        ["python", job_path],
        capture_output=True,
        text=True,
        timeout=600,
    )

    if result.returncode != 0:
        raise RuntimeError(f"Spark job failed: {result.stderr}")

    yield Output(
        {"status": "completed", "job": "entity_processing"},
        metadata={
            "status": MetadataValue.text("completed"),
            "job": MetadataValue.text("entity_processing"),
        },
    )
