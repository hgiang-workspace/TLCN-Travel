"""Job S6: Aspect Detection.

Đọc staging sentences (S2), chạy aspect detection, ghi ra staging aspects.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from ingestion.sentiment.aspects import run_s6_aspect_detection, load_aspect_keywords
from ingestion.sentiment.config import load_config, get
from ingestion.sentiment.io_local import read_jsonl, write_jsonl
from ingestion.sentiment.obs.run_log import write_run_log
from ingestion.sentiment.paths import build_paths


def run_s6_local(
    *,
    source: str,
    run_id: str,
    paths=None,
    cfg=None,
) -> dict[str, Any]:
    """Chạy S6 ở chế độ local."""
    cfg = cfg or load_config()
    paths = paths or build_paths(cfg)

    # Check if aspects enabled
    if not get(cfg, "aspects.enabled", True):
        return {"status": "skipped", "reason": "aspects.enabled=false", "rows_in": 0, "rows_out": 0}

    # Input: staging sentences (S2 output)
    in_dir = paths.sentences_dir(source)
    in_files = list(in_dir.glob("*.jsonl"))
    if not in_files:
        raise FileNotFoundError(f"Không có file input S2: {in_dir}")

    # Load aspect keywords
    keywords = load_aspect_keywords()

    s2_rows: list[dict[str, Any]] = []
    for in_file in in_files:
        for row in read_jsonl(in_file):
            if row.get("run_id") != run_id:
                continue
            s2_rows.append(row)

    if not s2_rows:
        return {"sentences_in": 0, "aspects_out": 0, "output": ""}

    # Run aspect detection + aspect-aware segmentation
    aspects_cfg = get(cfg, "aspects", {}) or {}
    s6_rows = run_s6_aspect_detection(
        s2_rows, run_id=run_id, keywords=keywords, cfg=aspects_cfg
    )

    # Output: staging aspects
    out_dir = paths.aspects_dir(source)
    out_file = out_dir / "part-000.jsonl"
    written = write_jsonl(out_file, s6_rows)

    clauses_out = len({(r["sentence_id"], r.get("clause_idx", 0)) for r in s6_rows})

    return {
        "sentences_in": len(s2_rows),
        "aspects_out": written,
        "segments_out": written,
        "clauses_out": clauses_out,
        "output": str(out_file),
    }


def main():
    parser = argparse.ArgumentParser(description="S6 - Aspect Detection")
    parser.add_argument("--source", required=True, choices=["tripadvisor", "tiktok", "foody"])
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--local", action="store_true")
    parser.add_argument("--config", default=None)
    args = parser.parse_args()

    cfg = load_config(args.config)
    paths = build_paths(cfg)

    try:
        if args.local or True:
            result = run_s6_local(
                source=args.source,
                run_id=args.run_id,
                paths=paths,
                cfg=cfg,
            )
        else:
            raise NotImplementedError("Spark mode chưa implement")

        if result.get("status") == "skipped":
            write_run_log(paths, job="S6", run_id=args.run_id, status="skipped",
                          rows_in=0, rows_out=0, params={"source": args.source, "reason": result.get("reason")})
            print(json.dumps({"status": "skipped", **result}, ensure_ascii=False))
            return

        write_run_log(paths, job="S6", run_id=args.run_id, status="success",
                      rows_in=result["sentences_in"], rows_out=result["aspects_out"],
                      params={"source": args.source})
        print(json.dumps({"status": "success", **result}, ensure_ascii=False))
    except Exception as e:
        write_run_log(paths, job="S6", run_id=args.run_id, status="failed", error=str(e))
        print(json.dumps({"status": "failed", "error": str(e)}, ensure_ascii=False), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()