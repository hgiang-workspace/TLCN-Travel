"""Job S3: Inference ViSoBERT.

Đọc staging sentences, chạy inference, ghi ra staging predictions.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

from ingestion.sentiment.config import load_config, get
from ingestion.sentiment.io_local import read_jsonl, write_jsonl
from ingestion.sentiment.obs.run_log import write_run_log
from ingestion.sentiment.paths import build_paths
from ingestion.sentiment.inference import predict_batch


def run_s3_local(
    *,
    source: str,
    run_id: str,
    model_version: str | None = None,
    limit: int | None = None,
    paths=None,
    cfg=None,
) -> dict[str, Any]:
    """Chạy S3 ở chế độ local."""
    cfg = cfg or load_config()
    paths = paths or build_paths(cfg)

    # Model dir
    model_dir = Path(get(cfg, "model.local_dir", "models/visobert"))
    if model_version:
        model_dir = model_dir / model_version

    # Provider: env var override > config > default
    provider = os.environ.get("TLCN_SENTIMENT_PROVIDER") or get(cfg, "model.provider", "transformers")
    # Cho phép stub provider không cần model directory
    if provider != "stub" and not model_dir.exists():
        raise FileNotFoundError(f"Model chưa bootstrap: {model_dir}. Chạy S0 trước.")

    # Input: staging sentences
    in_dir = paths.sentences_dir(source)
    in_files = list(in_dir.glob("*.jsonl"))
    if not in_files:
        raise FileNotFoundError(f"Không có file input S2: {in_dir}")

    # Read sentences
    sentences: list[dict[str, Any]] = []
    for in_file in in_files:
        for row in read_jsonl(in_file):
            # Chỉ xử lý câu của run_id hiện tại
            if row.get("run_id") != run_id:
                continue
            sentences.append(row)
            if limit and len(sentences) >= limit:
                break
        if limit and len(sentences) >= limit:
            break

    if not sentences:
        return {"sentences_in": 0, "predictions_out": 0, "output": ""}

    texts = [s["sentence_text"] for s in sentences]

    # Inference
    batch_size = get(cfg, "model.batch_size", 32)
    max_length = get(cfg, "model.max_length", 256)
    # Sử dụng provider đã xác định ở trên (ưu tiên env var)
    # provider = get(cfg, "model.provider", "transformers")  # ĐÃ LẤY TỪ ENV VAR

    preds = predict_batch(texts, str(model_dir), batch_size=batch_size, max_length=max_length, provider=provider)

    # Merge
    out_rows: list[dict[str, Any]] = []
    for s, p in zip(sentences, preds):
        out_rows.append({
            **s,
            "label": p["label"],
            "score": p["score"],
            "label_id": p["label_id"],
            "model_version": get(cfg, "model.version", os.path.basename(str(model_dir))),
            "run_id": run_id,
        })

    # Output: staging predictions
    out_dir = paths.predictions_dir(source, get(cfg, "model.version"))
    out_file = out_dir / "part-000.jsonl"
    written = write_jsonl(out_file, out_rows)

    return {
        "sentences_in": len(sentences),
        "predictions_out": written,
        "output": str(out_file),
    }


def main():
    parser = argparse.ArgumentParser(description="S3 - ViSoBERT Inference")
    parser.add_argument("--source", required=True, choices=["tripadvisor", "tiktok", "foody"])
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--model-version", default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--local", action="store_true")
    parser.add_argument("--config", default=None)
    args = parser.parse_args()

    cfg = load_config(args.config)
    paths = build_paths(cfg)

    started = ""
    try:
        if args.local or True:
            result = run_s3_local(
                source=args.source,
                run_id=args.run_id,
                model_version=args.model_version,
                limit=args.limit,
                paths=paths,
                cfg=cfg,
            )
        else:
            raise NotImplementedError("Spark mode chưa implement")

        write_run_log(paths, job="S3", run_id=args.run_id, status="success",
                      rows_in=result["sentences_in"], rows_out=result["predictions_out"],
                      started_at=started, params={"source": args.source, "model_version": args.model_version, "limit": args.limit})
        print(json.dumps({"status": "success", **result}, ensure_ascii=False))
    except Exception as e:
        write_run_log(paths, job="S3", run_id=args.run_id, status="failed",
                      error=str(e), started_at=started)
        print(json.dumps({"status": "failed", "error": str(e)}, ensure_ascii=False), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()