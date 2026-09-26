"""Job S5: Quality Report.

Đọc final output + quarantine, tính metric, ghi báo cáo JSON.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from ingestion.sentiment.config import load_config
from ingestion.sentiment.io_local import read_jsonl
from ingestion.sentiment.obs.run_log import write_run_log
from ingestion.sentiment.paths import build_paths
from ingestion.sentiment.report import compute_metrics, compute_label_consistency


def run_s5_local(
    *,
    source: str,
    run_id: str,
    paths=None,
    cfg=None,
) -> dict[str, Any]:
    """Chạy S5 ở chế độ local."""
    cfg = cfg or load_config()
    paths = paths or build_paths(cfg)

    # Read final output
    final_jsonl = paths.final_jsonl(source)
    quarantine_file = paths.quarantine_file(source)

    if not final_jsonl.exists():
        raise FileNotFoundError(f"Không có final output: {final_jsonl}")

    valid_rows = read_jsonl(final_jsonl)
    quarantined_rows = read_jsonl(quarantine_file) if quarantine_file.exists() else []

    # Compute metrics
    metrics = compute_metrics(valid_rows, quarantined_rows)
    consistency = compute_label_consistency(valid_rows)

    # Combine
    report = {
        "run_id": run_id,
        "source": source,
        "metrics": metrics,
        "rating_consistency": consistency,
        "generated_at": Path(__file__).stat().st_mtime,  # placeholder
    }

    # Write report
    report_file = paths.report_file(run_id)
    quality_file = paths.quality_file(run_id)
    latest_quality = paths.latest_quality_file()

    for f in (report_file, quality_file, latest_quality):
        f.parent.mkdir(parents=True, exist_ok=True)
        with open(f, "w", encoding="utf-8") as handle:
            json.dump(report, handle, ensure_ascii=False, indent=2)

    return {
        "report_file": str(report_file),
        "quality_file": str(quality_file),
        "metrics": metrics,
        "consistency": consistency,
    }


def main():
    parser = argparse.ArgumentParser(description="S5 - Quality Report")
    parser.add_argument("--source", required=True, choices=["tripadvisor", "tiktok", "foody"])
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--local", action="store_true")
    parser.add_argument("--config", default=None)
    args = parser.parse_args()

    cfg = load_config(args.config)
    paths = build_paths(cfg)

    started = ""
    try:
        if args.local or True:
            result = run_s5_local(
                source=args.source,
                run_id=args.run_id,
                paths=paths,
                cfg=cfg,
            )
        else:
            raise NotImplementedError("Spark mode chưa implement")

        write_run_log(paths, job="S5", run_id=args.run_id, status="success",
                      rows_in=result["metrics"]["total_input"], rows_out=result["metrics"]["valid"],
                      started_at=started, params={"source": args.source})
        print(json.dumps({"status": "success", **result}, ensure_ascii=False))
    except Exception as e:
        write_run_log(paths, job="S5", run_id=args.run_id, status="failed",
                      error=str(e), started_at=started)
        print(json.dumps({"status": "failed", "error": str(e)}, ensure_ascii=False), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()