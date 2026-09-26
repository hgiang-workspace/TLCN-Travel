"""Pipeline runner: chạy liên tiếp S1→S2→S3→S4→S5.

Usage:
    python -m ingestion.sentiment.run_pipeline --source tripadvisor --run-id run_20240101
    python -m ingestion.sentiment.run_pipeline --source tripadvisor --run-id run_20240101 --from S2 --to S4
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


JOBS = [
    ("S1", "ingestion.sentiment.jobs.s1_adapter"),
    ("S2", "ingestion.sentiment.jobs.s2_sentence"),
    ("S3", "ingestion.sentiment.jobs.s3_infer"),
    ("S4", "ingestion.sentiment.jobs.s4_validate"),
    ("S5", "ingestion.sentiment.jobs.s5_report"),
]

JOB_ORDER = {job[0]: i for i, job in enumerate(JOBS)}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def run_job(module: str, args: list[str], capture: bool = True, env=None) -> dict[str, Any]:
    """Chạy 1 job qua subprocess, trả về parsed JSON output."""
    cmd = [sys.executable, "-m", module] + args
    result = subprocess.run(cmd, capture_output=capture, text=True, timeout=3600, env=env)
    if result.returncode != 0:
        return {"status": "failed", "error": result.stderr or result.stdout, "returncode": result.returncode}
    try:
        return json.loads(result.stdout.strip().split("\n")[-1])
    except json.JSONDecodeError:
        return {"status": "unknown", "raw": result.stdout}


def run_pipeline(
    *,
    source: str,
    run_id: str,
    from_job: str = "S1",
    to_job: str = "S5",
    config: str | None = None,
    limit: int | None = None,
    model_version: str | None = None,
    allow_stub: bool = False,
) -> dict[str, Any]:
    """Chạy pipeline từ from_job đến to_job (inclusive)."""
    # Validate job names
    if from_job not in JOB_ORDER or to_job not in JOB_ORDER:
        raise ValueError(f"Job không hợp lệ. Hỗ trợ: {list(JOB_ORDER.keys())}")
    if JOB_ORDER[from_job] > JOB_ORDER[to_job]:
        raise ValueError(f"from_job ({from_job}) phải <= to_job ({to_job})")

    # Build common args
    common_args = ["--source", source, "--run-id", run_id, "--local"]
    if config:
        common_args += ["--config", config]
    if limit:
        common_args += ["--limit", str(limit)]
    if model_version:
        common_args += ["--model-version", model_version]

    # Set provider stub env if needed
    env = None
    if allow_stub:
        import os
        env = os.environ.copy()
        env["TLCN_SENTIMENT_PROVIDER"] = "stub"

    results: dict[str, Any] = {"run_id": run_id, "source": source, "jobs": {}}
    overall_success = True

    for job_name, module in JOBS:
        if JOB_ORDER[job_name] < JOB_ORDER[from_job] or JOB_ORDER[job_name] > JOB_ORDER[to_job]:
            results["jobs"][job_name] = {"status": "skipped"}
            continue

        print(f"\n{'='*60}")
        print(f"Running {job_name} ({module})...")
        print(f"{'='*60}")

        # Chỉ S1 nhận --limit
        job_args = common_args.copy()
        if job_name != "S1":
            # Remove --limit and its value for other jobs
            filtered = []
            skip_next = False
            for i, a in enumerate(job_args):
                if skip_next:
                    skip_next = False
                    continue
                if a == "--limit":
                    skip_next = True
                    continue
                filtered.append(a)
            job_args = filtered
        if job_name in ("S3", "S4") and model_version:
            job_args += ["--model-version", model_version]

        start = time.time()
        job_result = run_job(module, job_args, capture=True, env=env)
        duration = round(time.time() - start, 2)

        job_result["duration_seconds"] = duration
        results["jobs"][job_name] = job_result

        print(f"{job_name}: {job_result.get('status', 'unknown')} ({duration}s)")
        if job_result.get("status") != "success":
            overall_success = False
            print(f"  Error: {job_result.get('error', 'Unknown')}")
            break  # Dừng pipeline khi job fail

    results["overall_status"] = "success" if overall_success else "failed"
    results["finished_at"] = utc_now_iso()

    # Write summary
    summary_path = Path(f"data/sentiment/_reports/{run_id}_pipeline_summary.json")
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    return results


def main():
    parser = argparse.ArgumentParser(description="Run sentiment pipeline S1→S5")
    parser.add_argument("--source", required=True, choices=["tripadvisor", "tiktok", "foody"])
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--from", dest="from_job", default="S1", choices=["S1", "S2", "S3", "S4", "S5"])
    parser.add_argument("--to", dest="to_job", default="S5", choices=["S1", "S2", "S3", "S4", "S5"])
    parser.add_argument("--config", default=None)
    parser.add_argument("--limit", type=int, default=None, help="Giới hạn số comment (cho S1)")
    parser.add_argument("--model-version", default=None)
    parser.add_argument("--allow-stub", action="store_true", help="Cho phép stub provider (test offline)")
    args = parser.parse_args()

    try:
        result = run_pipeline(
            source=args.source,
            run_id=args.run_id,
            from_job=args.from_job,
            to_job=args.to_job,
            config=args.config,
            limit=args.limit,
            model_version=args.model_version,
            allow_stub=args.allow_stub,
        )
        print(json.dumps({"status": result["overall_status"], "summary": f"data/sentiment/_reports/{args.run_id}_pipeline_summary.json"}, ensure_ascii=False))
        sys.exit(0 if result["overall_status"] == "success" else 1)
    except Exception as e:
        print(json.dumps({"status": "failed", "error": str(e)}, ensure_ascii=False), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()