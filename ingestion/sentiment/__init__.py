"""Sentiment enrichment cho comment ở tầng Ingestion (ViSoBERT).

Package này CHỈ thuộc tầng Ingestion: đọc file thô do các collector sinh ra,
suy luận cảm xúc, ghi kết quả ra file. Không dùng Iceberg/Silver/ML/Gold/Dagster.

Cấu trúc:
    config.py        nạp configs/ingestion/sentiment.yaml
    paths.py         quy ước đường dẫn data/sentiment/**
    spark.py         tạo SparkSession + StructType (pyspark import lazy)
    schemas.py       Comment dataclass + danh sách field từng tầng
    builders.py      hàm thuần dựng row cho S1/S2
    io_local.py      đọc/ghi bảng ở chế độ local (jsonl, không cần Spark/pyarrow)
    labels.py        nhãn chuẩn + map nhãn từ model
    adapters/        adapter theo từng nguồn (tripadvisor, tiktok)
    text/            chuẩn hoá text + tách câu (+ pandas UDF)
    inference/       ViSoBERT: get_model/predict_batch/mapInPandas UDF
    quality/         rule kiểm định + quarantine
    report/          metric chất lượng
    jobs/            S0..S5 (script __main__)
    run_pipeline.py  chạy liên tiếp S1..S5
"""

from __future__ import annotations

PIPELINE_VERSION = "1.0.0"

__all__ = ["PIPELINE_VERSION"]
