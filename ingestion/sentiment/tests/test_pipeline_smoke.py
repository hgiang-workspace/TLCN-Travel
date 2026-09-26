"""Tests cho pipeline jobs (smoke test offline với stub)."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from ingestion.sentiment.config import load_config
from ingestion.sentiment.paths import build_paths
from ingestion.sentiment.jobs.s1_adapter import run_s1_local
from ingestion.sentiment.jobs.s2_sentence import run_s2_local
from ingestion.sentiment.jobs.s3_infer import run_s3_local
from ingestion.sentiment.jobs.s4_validate import run_s4_local
from ingestion.sentiment.jobs.s5_report import run_s5_local


class TestPipelineSmoke:
    """Smoke test chạy full pipeline S1→S5 với stub provider."""

    @pytest.fixture
    def temp_env(self, monkeypatch, tmp_path):
        """Setup môi trường temp với config và raw data giả."""
        # Tạo thư mục temp
        raw_dir = tmp_path / "data" / "raw" / "tripadvisor"
        raw_dir.mkdir(parents=True)

        # Raw data mẫu (format thực tế từ data/raw/tripadvisor/tripadvisor_comments.json)
        raw_data = [{
            "record_id": "p1",
            "entity_name": "Test Hotel",
            "province": "Hà Nội",
            "rating": 4.5,
            "reviews": [
                {"index": 1, "detail_review": "Phòng sạch, view đẹp. Nhân viên thân thiện!", "key_review": "Tuyệt vời"},
                {"index": 2, "detail_review": "Không gian ồn ào. Dịch vụ kém.", "key_review": "Tệ"},
            ]
        }]

        with open(raw_dir / "tripadvisor_comments.json", "w", encoding="utf-8") as f:
            json.dump(raw_data, f, ensure_ascii=False)

        # Config temp
        config_dir = tmp_path / "configs" / "ingestion"
        config_dir.mkdir(parents=True)
        config_content = f"""
pipeline:
  output_root: "{tmp_path}/data/sentiment"
  raw_root: "{tmp_path}/data/raw"
model:
  name: "5CD-AI/Vietnamese-Sentiment-visobert"
  version: "visobert"
  local_dir: "{tmp_path}/models/visobert"
  batch_size: 2
  max_length: 128
  provider: "stub"
text:
  lowercase: true
  use_underthesea: false
quality:
  min_score: 0.3
  min_sentence_len: 3
  required_labels: ["negative", "neutral", "positive"]
"""
        config_file = config_dir / "sentiment.yaml"
        with open(config_file, "w", encoding="utf-8") as f:
            f.write(config_content)

        # Set env
        monkeypatch.setenv("TLCN_PROJECT_ROOT", str(tmp_path))
        monkeypatch.setenv("TLCN_SENTIMENT_CONFIG", str(config_file))
        monkeypatch.setenv("TLCN_SENTIMENT_PROVIDER", "stub")

        yield tmp_path

    def test_s1_adapter(self, temp_env):
        cfg = load_config()
        paths = build_paths(cfg)
        result = run_s1_local(source="tripadvisor", run_id="test_run", paths=paths, cfg=cfg, limit=10)
        assert "rows_out" in result
        assert result["rows_out"] > 0

    def test_s2_sentence(self, temp_env):
        cfg = load_config()
        paths = build_paths(cfg)
        # Chạy S1 trước
        run_s1_local(source="tripadvisor", run_id="test_run", paths=paths, cfg=cfg, limit=10)
        # Chạy S2
        result = run_s2_local(source="tripadvisor", run_id="test_run", paths=paths, cfg=cfg)
        assert result["sentences_out"] > 0

    def test_s3_infer(self, temp_env):
        cfg = load_config()
        paths = build_paths(cfg)
        # Chạy S1 + S2
        run_s1_local(source="tripadvisor", run_id="test_run", paths=paths, cfg=cfg, limit=10)
        run_s2_local(source="tripadvisor", run_id="test_run", paths=paths, cfg=cfg)
        # Chạy S3 (stub provider)
        result = run_s3_local(source="tripadvisor", run_id="test_run", paths=paths, cfg=cfg)
        assert result["predictions_out"] > 0

    def test_s4_validate(self, temp_env):
        cfg = load_config()
        paths = build_paths(cfg)
        run_s1_local(source="tripadvisor", run_id="test_run", paths=paths, cfg=cfg, limit=10)
        run_s2_local(source="tripadvisor", run_id="test_run", paths=paths, cfg=cfg)
        run_s3_local(source="tripadvisor", run_id="test_run", paths=paths, cfg=cfg)
        result = run_s4_local(source="tripadvisor", run_id="test_run", paths=paths, cfg=cfg)
        assert result["valid"] > 0

    def test_s5_report(self, temp_env):
        cfg = load_config()
        paths = build_paths(cfg)
        run_s1_local(source="tripadvisor", run_id="test_run", paths=paths, cfg=cfg, limit=10)
        run_s2_local(source="tripadvisor", run_id="test_run", paths=paths, cfg=cfg)
        run_s3_local(source="tripadvisor", run_id="test_run", paths=paths, cfg=cfg)
        run_s4_local(source="tripadvisor", run_id="test_run", paths=paths, cfg=cfg)
        result = run_s5_local(source="tripadvisor", run_id="test_run", paths=paths, cfg=cfg)
        assert "report_file" in result
        assert result["metrics"]["total_input"] > 0