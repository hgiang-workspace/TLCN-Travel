"""Job S0: Bootstrap model ViSoBERT.

Tải model từ HuggingFace Hub, lưu về local (models/), ghi registry.
Chạy 1 lần hoặc khi cần cập nhật version.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from ingestion.sentiment.config import load_config, get
from ingestion.sentiment.obs.run_log import write_run_log
from ingestion.sentiment.paths import build_paths


def run_s0_local(
    *,
    run_id: str,
    force: bool = False,
    paths=None,
    cfg=None,
) -> dict[str, Any]:
    """Tải model về local (chỉ chạy khi có transformers)."""
    cfg = cfg or load_config()
    paths = paths or build_paths(cfg)

    model_name = get(cfg, "model.name", "5CD-AI/Vietnamese-Sentiment-visobert")
    local_dir = Path(get(cfg, "model.local_dir", "models/visobert"))

    # Check nếu đã có model
    if local_dir.exists() and not force:
        config_file = local_dir / "config.json"
        if config_file.exists():
            return {
                "status": "exists",
                "model": model_name,
                "local_dir": str(local_dir),
            }

    # Thử tải model (cần transformers + torch)
    try:
        from transformers import AutoModelForSequenceClassification, AutoTokenizer, AutoConfig
    except ImportError as e:
        return {
            "status": "skipped",
            "reason": f"Thiếu transformers/torch: {e}",
            "model": model_name,
            "local_dir": str(local_dir),
        }

    local_dir.mkdir(parents=True, exist_ok=True)

    print(f"Downloading {model_name} ...")
    config = AutoConfig.from_pretrained(model_name)
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModelForSequenceClassification.from_pretrained(model_name)

    # Save local
    config.save_pretrained(local_dir)
    tokenizer.save_pretrained(local_dir)
    model.save_pretrained(local_dir)

    # Write registry
    registry = {
        "model_name": model_name,
        "local_dir": str(local_dir),
        "id2label": config.id2label,
        "label2id": config.label2id,
        "num_labels": config.num_labels,
        "bootstrap_run_id": run_id,
        "bootstrapped_at": Path(__file__).stat().st_mtime,
    }
    with open(paths.model_registry, "w", encoding="utf-8") as f:
        json.dump(registry, f, ensure_ascii=False, indent=2)

    return {
        "status": "downloaded",
        "model": model_name,
        "local_dir": str(local_dir),
        "num_labels": config.num_labels,
        "id2label": config.id2label,
    }


def main():
    parser = argparse.ArgumentParser(description="S0 - Bootstrap ViSoBERT Model")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--force", action="store_true", help="Tải lại model")
    parser.add_argument("--local", action="store_true")
    parser.add_argument("--config", default=None)
    args = parser.parse_args()

    cfg = load_config(args.config)
    paths = build_paths(cfg)

    started = ""
    try:
        result = run_s0_local(run_id=args.run_id, force=args.force, paths=paths, cfg=cfg)

        write_run_log(paths, job="S0", run_id=args.run_id, status="success",
                      rows_in=0, rows_out=1 if result.get("status") == "downloaded" else 0,
                      started_at=started, params={"model": get(cfg, "model.name"), "force": args.force})
        print(json.dumps({"status": "success", **result}, ensure_ascii=False))
    except Exception as e:
        write_run_log(paths, job="S0", run_id=args.run_id, status="failed",
                      error=str(e), started_at=started)
        print(json.dumps({"status": "failed", "error": str(e)}, ensure_ascii=False), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()