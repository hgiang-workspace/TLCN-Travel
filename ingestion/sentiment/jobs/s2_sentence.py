"""Job S2: Split Sentences + Dedupe.

Đọc staging comments, tách câu, dedupe theo sentence_hash, ghi ra staging sentences.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from ingestion.sentiment.builders import build_s2_rows, sentence_hash
from ingestion.sentiment.config import load_config, get
from ingestion.sentiment.io_local import read_jsonl, write_jsonl
from ingestion.sentiment.obs.run_log import write_run_log
from ingestion.sentiment.paths import build_paths
from ingestion.sentiment.text import split_sentences


def run_s2_local(
    *,
    source: str,
    run_id: str,
    paths=None,
    cfg=None,
) -> dict[str, Any]:
    """Chạy S2 ở chế độ local."""
    cfg = cfg or load_config()
    paths = paths or build_paths(cfg)

    # Input: staging comments
    in_dir = paths.comments_dir(source)
    in_files = list(in_dir.glob("*.jsonl"))
    if not in_files:
        raise FileNotFoundError(f"Không có file input S1: {in_dir}")

    text_cfg = get(cfg, "text", {})
    s2_rows: list[dict[str, Any]] = []
    total_comments = 0

    for in_file in in_files:
        for row in read_jsonl(in_file):
            # Chỉ xử lý comment của run_id hiện tại
            if row.get("run_id") != run_id:
                continue
            total_comments += 1
            sentences = split_sentences(row["normalized_text"], text_cfg)
            s2_rows.extend(build_s2_rows(row, sentences=sentences, text_cfg=text_cfg, run_id=run_id))

    # Output: staging sentences
    out_dir = paths.sentences_dir(source)
    out_file = out_dir / "part-000.jsonl"
    written = write_jsonl(out_file, s2_rows)

    return {
        "comments_in": total_comments,
        "sentences_out": written,
        "output": str(out_file),
    }


def main():
    parser = argparse.ArgumentParser(description="S2 - Split Sentences + Dedupe")
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
            result = run_s2_local(
                source=args.source,
                run_id=args.run_id,
                paths=paths,
                cfg=cfg,
            )
        else:
            raise NotImplementedError("Spark mode chưa implement")

        write_run_log(paths, job="S2", run_id=args.run_id, status="success",
                      rows_in=result["comments_in"], rows_out=result["sentences_out"],
                      started_at=started, params={"source": args.source})
        print(json.dumps({"status": "success", **result}, ensure_ascii=False))
    except Exception as e:
        write_run_log(paths, job="S2", run_id=args.run_id, status="failed",
                      error=str(e), started_at=started)
        print(json.dumps({"status": "failed", "error": str(e)}, ensure_ascii=False), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()