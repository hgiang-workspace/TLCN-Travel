"""Job S7: ABSA Assembly + Publish.

Đọc staging aspects (S6) + staging predictions (S3), assemble ABSA, validate, ghi final output.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from ingestion.sentiment.aspects import assemble_absa, load_aspect_keywords
from ingestion.sentiment.config import load_config, get
from ingestion.sentiment.io_local import read_jsonl, write_jsonl
from ingestion.sentiment.obs.run_log import write_run_log
from ingestion.sentiment.paths import build_paths
from ingestion.sentiment.quality import validate_absa_batch, build_absa_quality_config
from ingestion.sentiment.report import compute_absa_metrics


def run_s7_local(
    *,
    source: str,
    run_id: str,
    model_version: str | None = None,
    paths=None,
    cfg=None,
) -> dict[str, Any]:
    """Chạy S7 ở chế độ local."""
    cfg = cfg or load_config()
    paths = paths or build_paths(cfg)

    # Check if aspects enabled
    if not get(cfg, "aspects.enabled", True):
        return {"status": "skipped", "reason": "aspects.enabled=false", "valid": 0, "quarantined": 0}

    # Input: staging aspects (S6 output)
    aspects_dir = paths.aspects_dir(source)
    aspects_files = list(aspects_dir.glob("*.jsonl"))
    if not aspects_files:
        raise FileNotFoundError(f"Không có file input S6: {aspects_dir}")

    # Granularity quyết định nguồn sentiment
    granularity = get(cfg, "aspects.granularity", "aspect_span")
    is_segment_level = granularity in ("aspect_span", "segment", "aspect")

    # Read S6 aspects
    s6_rows: list[dict[str, Any]] = []
    for in_file in aspects_files:
        for row in read_jsonl(in_file):
            if row.get("run_id") != run_id:
                continue
            s6_rows.append(row)

    if not s6_rows:
        return {"aspects_in": 0, "valid": 0, "quarantined": 0, "final_jsonl": "", "final_parquet": ""}

    if not is_segment_level:
        # Đường cũ (sentence/clause): join sentiment mức câu từ S3 (predictions_raw)
        pred_version = model_version or get(cfg, "model.version", "visobert")
        pred_dir = paths.predictions_dir(source, pred_version)
        pred_files = list(pred_dir.glob("*.jsonl"))
        if not pred_files:
            raise FileNotFoundError(f"Không có file input S3: {pred_dir}")

        pred_rows: list[dict[str, Any]] = []
        for in_file in pred_files:
            for row in read_jsonl(in_file):
                if row.get("run_id") != run_id:
                    continue
                pred_rows.append(row)

        # Build sentiment map: sentence_id -> (label, score, model_version)
        sentiment_map = {}
        for p in pred_rows:
            sentiment_map[p["sentence_id"]] = (
                p["label"], p["score"], p.get("model_version", "visobert"),
            )

        # Gán sentiment vào S6 rows (tạm, sẽ dùng trong assemble)
        for s6 in s6_rows:
            sid = s6["sentence_id"]
            if sid in sentiment_map:
                label, score, mv = sentiment_map[sid]
                s6["_sentiment_label"] = label
                s6["_sentiment_score"] = score
                s6["_model_version"] = mv
            else:
                s6["_sentiment_label"] = "neutral"
                s6["_sentiment_score"] = 0.5
                s6["_model_version"] = "visobert"

    # Model dir + provider
    model_dir = Path(get(cfg, "model.local_dir", "models/visobert"))
    if model_version:
        model_dir = model_dir / model_version

    # Provider: env var override > config > default (like S3)
    import os
    provider = os.environ.get("TLCN_SENTIMENT_PROVIDER") or get(cfg, "model.provider", "transformers")
    batch_size = get(cfg, "model.batch_size", 32)
    max_length = get(cfg, "model.max_length", 256)
    text_cfg = get(cfg, "text", {})

    s7_rows = assemble_absa(
        s6_rows,
        model_dir=str(model_dir),
        granularity=granularity,
        batch_size=batch_size,
        max_length=max_length,
        provider=provider,
        text_cfg=text_cfg,
    )

    # Validate ABSA
    absa_qcfg = build_absa_quality_config(cfg)
    valid_rows, quarantined_rows = validate_absa_batch(s7_rows, absa_qcfg)

    # Output valid -> final
    final_dir = paths.final_dir(source)
    final_jsonl = paths.absa_jsonl(source)
    final_parquet = paths.absa_parquet(source)

    valid_written = write_jsonl(final_jsonl, valid_rows)

    # Try write parquet if pyarrow available
    parquet_written = 0
    try:
        import pandas as pd
        if valid_rows:
            df = pd.DataFrame(valid_rows)
            df.to_parquet(final_parquet, index=False)
            parquet_written = len(df)
    except ImportError:
        pass

    # Output quarantined
    quarantine_file = paths.quarantine_file(source)
    quarantine_written = write_jsonl(quarantine_file, quarantined_rows)

    # Compute metrics
    s2_dir = paths.sentences_dir(source)
    s2_files = list(s2_dir.glob("*.jsonl"))
    s2_total = 0
    for f in s2_files:
        for row in read_jsonl(f):
            if row.get("run_id") == run_id:
                s2_total += 1

    metrics = compute_absa_metrics(valid_rows, quarantined_rows, s2_total)

    # Write ABSA quality report
    absa_quality_file = paths.absa_quality_file(run_id)
    absa_quality_file.parent.mkdir(parents=True, exist_ok=True)
    with open(absa_quality_file, "w", encoding="utf-8") as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)

    unique_segments = len({str(r.get("segment_hash") or r.get("segment_text")) for r in s6_rows})

    return {
        "aspects_in": len(s6_rows),
        "segments_in": len(s6_rows),
        "unique_segments": unique_segments,
        "valid": valid_written,
        "quarantined": quarantine_written,
        "parquet_written": parquet_written,
        "final_jsonl": str(final_jsonl),
        "final_parquet": str(final_parquet) if parquet_written > 0 else "",
        "quarantine_file": str(quarantine_file),
        "metrics": metrics,
    }


def main():
    parser = argparse.ArgumentParser(description="S7 - ABSA Publish")
    parser.add_argument("--source", required=True, choices=["tripadvisor", "tiktok", "foody"])
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--model-version", default=None)
    parser.add_argument("--local", action="store_true")
    parser.add_argument("--config", default=None)
    args = parser.parse_args()

    cfg = load_config(args.config)
    paths = build_paths(cfg)

    try:
        if args.local or True:
            result = run_s7_local(
                source=args.source,
                run_id=args.run_id,
                model_version=args.model_version,
                paths=paths,
                cfg=cfg,
            )
        else:
            raise NotImplementedError("Spark mode chưa implement")

        if result.get("status") == "skipped":
            write_run_log(paths, job="S7", run_id=args.run_id, status="skipped",
                          rows_in=0, rows_out=0, params={"source": args.source, "reason": result.get("reason")})
            print(json.dumps({"status": "skipped", **result}, ensure_ascii=False))
            return

        write_run_log(paths, job="S7", run_id=args.run_id, status="success",
                      rows_in=result["aspects_in"], rows_out=result["valid"],
                      params={"source": args.source, "model_version": args.model_version},
                      metrics={
                          "quarantined": result["quarantined"],
                          "parquet_written": result["parquet_written"],
                          "segments_in": result.get("segments_in", 0),
                          "unique_segments": result.get("unique_segments", 0),
                      })
        print(json.dumps({"status": "success", **result}, ensure_ascii=False))
    except Exception as e:
        write_run_log(paths, job="S7", run_id=args.run_id, status="failed", error=str(e))
        print(json.dumps({"status": "failed", "error": str(e)}, ensure_ascii=False), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()