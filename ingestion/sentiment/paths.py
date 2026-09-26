"""Quy ước đường dẫn cho pipeline sentiment."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ingestion.sentiment.config import get, resolve_path

STAGING_COMMENTS = "_staging/comments"
STAGING_SENTENCES = "_staging/sentences"
STAGING_PREDICTIONS = "_staging/predictions_raw"
QUARANTINE = "_quarantine"
REPORTS = "_reports"


@dataclass(frozen=True)
class SentimentPaths:
    root: Path
    staging_comments: Path
    staging_sentences: Path
    staging_predictions: Path
    quarantine: Path
    reports: Path
    run_log: Path
    model_registry: Path

    def comments_dir(self, source: str) -> Path:
        return self.staging_comments / source

    def sentences_dir(self, source: str) -> Path:
        return self.staging_sentences / source

    def predictions_dir(self, source: str, model_version: str | None = None) -> Path:
        base = self.staging_predictions / source
        return base if not model_version else base / f"model_version={model_version}"

    def final_dir(self, source: str) -> Path:
        return self.root / source

    def final_jsonl(self, source: str) -> Path:
        return self.final_dir(source) / "sentiment.jsonl"

    def final_parquet(self, source: str) -> Path:
        return self.final_dir(source) / "sentiment.parquet"

    def quarantine_file(self, source: str) -> Path:
        return self.quarantine / f"{source}.jsonl"

    def report_file(self, run_id: str) -> Path:
        return self.reports / f"{run_id}.json"

    def quality_file(self, run_id: str) -> Path:
        return self.reports / f"{run_id}_quality.json"

    def latest_quality_file(self) -> Path:
        return self.reports / "latest_quality.json"

    def log_file(self, job: str) -> Path:
        return resolve_path("logs") / f"sentiment_{job}.log"


def build_paths(cfg: dict[str, Any], output_root: str | Path | None = None) -> SentimentPaths:
    """Tạo cấu trúc thư mục và trả về các đường dẫn chuẩn."""
    root = resolve_path(output_root or get(cfg, "pipeline.output_root", "data/sentiment"))
    paths = SentimentPaths(
        root=root,
        staging_comments=root / STAGING_COMMENTS,
        staging_sentences=root / STAGING_SENTENCES,
        staging_predictions=root / STAGING_PREDICTIONS,
        quarantine=root / QUARANTINE,
        reports=root / REPORTS,
        run_log=root / REPORTS / "run_log.jsonl",
        model_registry=root / "model_registry.json",
    )
    for directory in (
        paths.root,
        paths.staging_comments,
        paths.staging_sentences,
        paths.staging_predictions,
        paths.quarantine,
        paths.reports,
    ):
        directory.mkdir(parents=True, exist_ok=True)
    return paths
