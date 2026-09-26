"""Job S4: Validate + Publish.

Đọc staging predictions, validate, ghi:
- valid -> final output (jsonl + parquet)
- invalid -> quarantine
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from ingestion.sentiment.config import load_config, get
from ingestion.sentiment.io_local import read_jsonl, write_jsonl
from ingestion.sentiment.obs.run_log import write_run_log
from ingestion.sentiment.paths import build_paths
from ingestion.sentiment.quality import validate_batch, build_quality_config


def run_s4_local(
    *,
    source: str,
    run_id: str,
    model_version: str | None = None,
    paths=None,
    cfg=None,
) -> dict[str, Any]:
    """Chạy S4 ở chế độ local."""
    cfg = cfg or load_config()
    paths = paths or build_paths(cfg)

    # Input: staging predictions
    pred_version = model_version or get(cfg, "model.version", "visobert")
    in_dir = paths.predictions_dir(source, pred_version)
    in_files = list(in_dir.glob("*.jsonl"))
    if not in_files:
        raise FileNotFoundError(f"Không có file input S3: {in_dir}")

    quality_cfg = build_quality_config(cfg)
    all_valid: list[dict[str, Any]] = []
    all_quarantined: list[dict[str, Any]] = []

    for in_file in in_files:
        for row in read_jsonl(in_file):
            # Chỉ xử lý prediction của run_id hiện tại
            if row.get("run_id") != run_id:
                continue
            valid, quarantined = validate_batch([row], quality_cfg)
            all_valid.extend(valid)
            all_quarantined.extend(quarantined)

    # Output valid -> final
    final_dir = paths.final_dir(source)
    final_jsonl = paths.final_jsonl(source)
    final_parquet = paths.final_parquet(source)

    valid_written = write_jsonl(final_jsonl, all_valid)

    # Try write parquet if pyarrow available
    parquet_written = 0
    try:
        import pandas as pd
        if all_valid:
            df = pd.DataFrame(all_valid)
            df.to_parquet(final_parquet, index=False)
            parquet_written = len(df)
    except ImportError:
        pass

    # Output quarantined
    quarantine_file = paths.quarantine_file(source)
    quarantine_written = write_jsonl(quarantine_file, all_quarantined)

    return {
        "valid": valid_written,
        "quarantined": quarantine_written,
        "parquet_written": parquet_written,
        "final_jsonl": str(final_jsonl),
        "final_parquet": str(final_parquet) if parquet_written > 0 else "",
        "quarantine_file": str(quarantine_file),
    }


def main():
    parser = argparse.ArgumentParser(description="S4 - Validate + Publish")
    parser.add_argument("--source", required=True, choices=["tripadvisor", "tiktok", "foody"])
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--model-version", default=None)
    parser.add_argument("--local", action="store_true")
    parser.add_argument("--config", default=None)
    args = parser.parse_args()

    cfg = load_config(args.config)
    paths = build_paths(cfg)

    started = ""
    try:
        if args.local or True:
            result = run_s4_local(
                source=args.source,
                run_id=args.run_id,
                model_version=args.model_version,
                paths=paths,
                cfg=cfg,
            )
        else:
            raise NotImplementedError("Spark mode chưa implement")

        write_run_log(paths, job="S4", run_id=args.run_id, status="success",
                      rows_in=result["valid"] + result["quarantined"], rows_out=result["valid"],
                      started_at=started, params={"source": args.source, "model_version": args.model_version},
                      metrics={"quarantined": result["quarantined"], "parquet_written": result["parquet_written"]})
        print(json.dumps({"status": "success", **result}, ensure_ascii=False))
    except Exception as e:
        write_run_log(paths, job="S4", run_id=args.run_id, status="failed",
                      error=str(e), started_at=started)
        print(json.dumps({"status": "failed", "error": str(e)}, ensure_ascii=False), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()