"""Gold assets - aggregated/serving layer.

Asset graph:
  absa_result → gold_aspect_sentiment
             → gold_destination_sentiment
             → gold_platform_sentiment
             → gold_sentiment_daily
"""

from pathlib import Path
from dagster import asset, Output, MetadataValue

PROJECT_ROOT = Path("/home/hgiang/TLCN")


@asset
def gold_aspect_sentiment(absa_result: dict) -> dict:
    """Build gold.aspect_sentiment table.

    Calls spark/jobs/build_gold.py.
    """
    import subprocess

    job_path = str(PROJECT_ROOT / "spark" / "jobs" / "build_gold.py")

    result = subprocess.run(
        ["python", job_path],
        capture_output=True,
        text=True,
        timeout=600,
    )

    if result.returncode != 0:
        raise RuntimeError(f"Spark job failed: {result.stderr}")

    yield Output(
        {"status": "completed", "table": "gold.aspect_sentiment"},
        metadata={"table": MetadataValue.text("gold.aspect_sentiment")},
    )


@asset
def gold_destination_sentiment(gold_aspect_sentiment: dict) -> dict:
    """Build gold.destination_sentiment table.

    Depends on gold_aspect_sentiment (built by same build_gold.py job).
    """
    yield Output(
        {"status": "completed", "table": "gold.destination_sentiment"},
        metadata={"table": MetadataValue.text("gold.destination_sentiment")},
    )


@asset
def gold_platform_sentiment(gold_aspect_sentiment: dict) -> dict:
    """Build gold.platform_sentiment table.

    Depends on gold_aspect_sentiment (built by same build_gold.py job).
    """
    yield Output(
        {"status": "completed", "table": "gold.platform_sentiment"},
        metadata={"table": MetadataValue.text("gold.platform_sentiment")},
    )


@asset
def gold_sentiment_daily(gold_aspect_sentiment: dict) -> dict:
    """Build gold.sentiment_daily table.

    Depends on gold_aspect_sentiment (built by same build_gold.py job).
    """
    yield Output(
        {"status": "completed", "table": "gold.sentiment_daily"},
        metadata={"table": MetadataValue.text("gold.sentiment_daily")},
    )
