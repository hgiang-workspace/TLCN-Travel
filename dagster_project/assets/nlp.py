"""NLP assets - sentiment analysis, NER, ABSA pipeline.

Asset graph:
  silver_social_post → sentiment_dataset
                    → ner_dataset
                          ↓
                      NLP inference
                          ↓
                      ABSA output → absa_result
"""

from pathlib import Path
from dagster import asset, Output, MetadataValue

PROJECT_ROOT = Path("/home/hgiang/TLCN")


@asset
def sentiment_dataset(silver_tripadvisor: dict) -> dict:
    """Build sentiment ML dataset from Silver data.

    Calls spark/jobs/build_ml_dataset.py.
    """
    import subprocess

    job_path = str(PROJECT_ROOT / "spark" / "jobs" / "build_ml_dataset.py")

    result = subprocess.run(
        ["python", job_path],
        capture_output=True,
        text=True,
        timeout=600,
    )

    if result.returncode != 0:
        raise RuntimeError(f"Spark job failed: {result.stderr}")

    yield Output(
        {"status": "completed", "job": "build_ml_dataset"},
        metadata={"status": MetadataValue.text("completed")},
    )


@asset
def ner_dataset(silver_entity: dict) -> dict:
    """NER dataset is built as part of build_ml_dataset.py.

    This asset depends on silver_entity to ensure entity processing is done first.
    """
    # NER dataset is built together with sentiment dataset in build_ml_dataset.py
    # This asset exists to represent the dependency in the asset graph
    yield Output(
        {"status": "completed", "note": "built_by_build_ml_dataset"},
        metadata={"status": MetadataValue.text("completed")},
    )


@asset
def absa_result(sentiment_dataset: dict) -> dict:
    """Run NLP inference / ABSA on the ML dataset.

    Placeholder for future NLP model inference.
    Currently skips if no model is available.
    """
    nlp_dir = PROJECT_ROOT / "nlp"
    inference_dir = nlp_dir / "inference"

    if not inference_dir.exists():
        yield Output(
            {"status": "skipped", "reason": "nlp/inference/ not yet implemented"},
            metadata={"status": MetadataValue.text("skipped")},
        )
        return

    # Future: run NLP inference here
    yield Output(
        {"status": "completed", "job": "absa_inference"},
        metadata={"status": MetadataValue.text("completed")},
    )
