"""Job S1: Adapter + Normalize.

Đọc raw JSON từ data/raw/<source>/, chạy adapter, normalize, ghi ra staging comments.
Chạy được ở chế độ local (--local) hoặc Spark (--master yarn/k8s).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from ingestion.sentiment.adapters import get_adapter
from ingestion.sentiment.builders import build_s1_row
from ingestion.sentiment.config import load_config, get
from ingestion.sentiment.io_local import write_jsonl, read_jsonl
from ingestion.sentiment.obs.run_log import write_run_log
from ingestion.sentiment.paths import build_paths
from ingestion.sentiment.text import normalize_text


def run_s1_local(
    *,
    source: str,
    run_id: str,
    limit: int | None = None,
    paths=None,
    cfg=None,
) -> dict[str, Any]:
    """Chạy S1 ở chế độ local (không Spark)."""
    cfg = cfg or load_config()
    paths = paths or build_paths(cfg)

    # Input file
    raw_root = Path(get(cfg, "pipeline.raw_root", "data/raw"))
    input_file = raw_root / source / f"{source}_comments.json"
    if not input_file.exists():
        # Thử tên file cũ cho TripAdvisor
        if source == "tripadvisor":
            input_file = raw_root / "tripadvisor" / "tripadvisor_comments.json"
        elif source == "foody":
            input_file = raw_root / "foody" / "foody_comments.json"

    if not input_file.exists():
        raise FileNotFoundError(f"Không tìm thấy file input: {input_file}")

    # Load raw
    with open(input_file, "r", encoding="utf-8") as f:
        raw_data = json.load(f)

    # Adapter
    adapter = get_adapter(source)
    comments = adapter(raw_data)
    if limit:
        comments = comments[:limit]

    # Process
    text_cfg = get(cfg, "text", {})
    s1_rows: list[dict[str, Any]] = []
    for c in comments:
        norm = normalize_text(c.raw_text, text_cfg)
        if not norm:
            continue  # bỏ comment rỗng sau normalize
        s1_rows.append(build_s1_row(c, normalized_text=norm, run_id=run_id))

    # Output
    out_dir = paths.comments_dir(source)
    out_file = out_dir / "part-000.jsonl"
    written = write_jsonl(out_file, s1_rows)

    return {
        "rows_in": len(comments),
        "rows_out": written,
        "output": str(out_file),
    }


def main():
    parser = argparse.ArgumentParser(description="S1 - Adapter + Normalize")
    parser.add_argument("--source", required=True, choices=["tripadvisor", "tiktok", "foody"])
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--local", action="store_true", help="Chạy local (không Spark)")
    parser.add_argument("--config", default=None)
    args = parser.parse_args()

    cfg = load_config(args.config)
    paths = build_paths(cfg)

    started = ""
    try:
        if args.local or True:  # Mặc định local nếu không có Spark
            result = run_s1_local(
                source=args.source,
                run_id=args.run_id,
                limit=args.limit,
                paths=paths,
                cfg=cfg,
            )
        else:
            # TODO: Spark implementation
            raise NotImplementedError("Spark mode chưa implement")

        write_run_log(paths, job="S1", run_id=args.run_id, status="success",
                      rows_in=result["rows_in"], rows_out=result["rows_out"],
                      started_at=started, params={"source": args.source, "limit": args.limit})
        print(json.dumps({"status": "success", **result}, ensure_ascii=False))
    except Exception as e:
        write_run_log(paths, job="S1", run_id=args.run_id, status="failed",
                      error=str(e), started_at=started)
        print(json.dumps({"status": "failed", "error": str(e)}, ensure_ascii=False), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()