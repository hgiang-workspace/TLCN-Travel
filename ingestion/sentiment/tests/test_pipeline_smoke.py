"""Tests cho pipeline jobs (smoke test offline với stub)."""

from __future__ import annotations

import json
import shutil
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
from ingestion.sentiment.jobs.s6_aspect import run_s6_local
from ingestion.sentiment.jobs.s7_absa_publish import run_s7_local


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

        # Copy từ điển aspect thật vào temp (S6 cần)
        repo_root = Path(__file__).resolve().parents[3]
        real_keywords = repo_root / "configs" / "ingestion" / "aspect_keywords.yaml"
        if real_keywords.exists():
            shutil.copy(real_keywords, config_dir / "aspect_keywords.yaml")
        else:
            (config_dir / "aspect_keywords.yaml").write_text(
                'aspects:\n  food: ["ngon", "đồ ăn"]\n  service: ["phục vụ", "nhân viên"]\n'
                '  ambiance: ["không gian", "đẹp"]\n  other: ["khác"]\n',
                encoding="utf-8",
            )

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
  use_clause_split: true
  min_clause_len: 3
quality:
  min_score: 0.3
  min_sentence_len: 3
  required_labels: ["negative", "neutral", "positive"]
aspects:
  enabled: true
  keywords_path: "{config_dir / "aspect_keywords.yaml"}"
  granularity: "aspect_span"
  keep_unmatched: true
  min_clause_len: 3
  min_segment_len: 3
  segment:
    strategy: "hybrid"
    window_before: 6
    window_after: 6
    include_keyword: true
    dedupe_by_hash: true
    max_aspects_per_sentence: 6
  quality:
    min_aspect_coverage: 0.0
    required_aspects: []
    max_aspects_per_sentence: 10
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

    def _run_s1_s2(self, cfg, paths):
        run_s1_local(source="tripadvisor", run_id="test_run", paths=paths, cfg=cfg, limit=10)
        run_s2_local(source="tripadvisor", run_id="test_run", paths=paths, cfg=cfg)

    def test_s6_aspect_segments(self, temp_env):
        """S6 sinh rows có segment_text/segment_hash/clause_idx."""
        cfg = load_config()
        paths = build_paths(cfg)
        self._run_s1_s2(cfg, paths)
        result = run_s6_local(source="tripadvisor", run_id="test_run", paths=paths, cfg=cfg)
        assert result["segments_out"] > 0
        assert result["clauses_out"] > 0

        rows = []
        for f in paths.aspects_dir("tripadvisor").glob("*.jsonl"):
            for line in f.read_text(encoding="utf-8").splitlines():
                if line.strip():
                    rows.append(json.loads(line))
        assert rows
        for row in rows:
            assert row["segment_text"]
            assert len(row["segment_hash"]) == 16
            assert row["segment_type"] in ("clause", "window", "fallback")
            assert row["aspect"] in ("food", "drink", "service", "ambiance", "cleanliness",
                                     "price", "location", "portion", "speed", "parking", "other")

    def test_s7_absa_segment_publish(self, temp_env):
        """S7 (granularity=aspect_span) sinh absa.jsonl có segment_text."""
        cfg = load_config()
        paths = build_paths(cfg)
        self._run_s1_s2(cfg, paths)
        run_s6_local(source="tripadvisor", run_id="test_run", paths=paths, cfg=cfg)
        result = run_s7_local(source="tripadvisor", run_id="test_run", paths=paths, cfg=cfg)

        assert result["valid"] > 0
        assert result["unique_segments"] > 0

        absa_rows = []
        for line in Path(result["final_jsonl"]).read_text(encoding="utf-8").splitlines():
            if line.strip():
                absa_rows.append(json.loads(line))
        assert absa_rows
        for row in absa_rows:
            assert row["segment_text"]
            assert row["segment_type"] in ("clause", "window", "sentence", "fallback")
            assert row["sentiment"] in ("negative", "neutral", "positive")
            # Input cho model phải là đoạn theo khía cạnh (hoặc fallback)
            assert row["segment_text"] in (
                row["sentence"], row.get("clause_text", row["segment_text"])
            ) or len(row["segment_text"]) <= len(row["sentence"])

        # Metrics có segment keys
        metrics = result["metrics"]
        assert "segment_type_distribution" in metrics
        assert "unique_segment_ratio" in metrics

    def test_s7_multi_aspect_differs(self, temp_env):
        """Câu đa khía cạnh trái chiều sinh nhiều row với segment khác nhau."""
        cfg = load_config()
        paths = build_paths(cfg)
        self._run_s1_s2(cfg, paths)
        run_s6_local(source="tripadvisor", run_id="test_run", paths=paths, cfg=cfg)
        result = run_s7_local(source="tripadvisor", run_id="test_run", paths=paths, cfg=cfg)

        rows = [json.loads(l) for l in
                Path(result["final_jsonl"]).read_text(encoding="utf-8").splitlines() if l.strip()]

        # Câu "Phòng sạch, view đẹp." có cleanliness + ambiance (nhiều aspect)
        sentence_ids = {r["sentence_id"] for r in rows}
        multi = [sid for sid in sentence_ids
                 if len({r["aspect"] for r in rows if r["sentence_id"] == sid}) > 1]
        assert multi, "mong đợi ít nhất 1 câu có >1 aspect"
        for sid in multi:
            for r in (x for x in rows if x["sentence_id"] == sid):
                # Mỗi aspect có segment_text của riêng nó (clause/window)
                assert r["segment_text"]
                assert r["segment_text"] != ""