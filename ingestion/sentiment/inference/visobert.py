"""ViSoBERT inference module.

- get_model(): tải model/tokenizer (cache theo module)
- predict_batch(): batch inference trên list[str] -> list[dict]
- map_udf(): pandas UDF cho Spark mapInPandas
"""

from __future__ import annotations

import os
from functools import lru_cache
from typing import Any

# Lazy imports - chỉ import khi thực sự gọi
_MODEL = None
_TOKENIZER = None
_PROVIDER = None
_DEVICE = None


def _get_device():
    global _DEVICE
    if _DEVICE is not None:
        return _DEVICE
    try:
        import torch
        _DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
    except ImportError:
        _DEVICE = "cpu"
    return _DEVICE


def get_model(
    model_dir: str,
    *,
    provider: str = "transformers",
) -> tuple[Any, Any, Any]:
    """Tải model + tokenizer + label_provider.

    Args:
        model_dir: thư mục model local (đã bootstrap qua S0)
        provider: "transformers" | "stub" (stub dùng cho test offline)

    Returns:
        (model, tokenizer, label_provider)
    """
    global _MODEL, _TOKENIZER, _PROVIDER

    if _MODEL is not None and _TOKENIZER is not None:
        return _MODEL, _TOKENIZER, _PROVIDER

    if provider == "stub":
        from ingestion.sentiment.labels import build_stub_provider
        _PROVIDER = build_stub_provider()

        class StubModel:
            def __call__(self, **kwargs):
                batch_size = kwargs.get("input_ids").shape[0]
                # Trả về logits deterministic dựa trên hash
                import numpy as np
                logits = np.zeros((batch_size, 3), dtype=np.float32)
                for i in range(batch_size):
                    h = hash(tuple(kwargs["input_ids"][i].tolist()))
                    logits[i, h % 3] = 1.0
                return type("Output", (), {"logits": logits})()

            def to(self, device):
                return self

            def eval(self):
                return self

        class StubTokenizer:
            def __call__(self, texts, padding=True, truncation=True, max_length=256, return_tensors="pt"):
                import numpy as np
                batch_size = len(texts)
                return {
                    "input_ids": np.random.randint(0, 1000, (batch_size, max_length), dtype=np.int64),
                    "attention_mask": np.ones((batch_size, max_length), dtype=np.int64),
                }

        _MODEL = StubModel()
        _TOKENIZER = StubTokenizer()
        return _MODEL, _TOKENIZER, _PROVIDER

    # Real transformers
    try:
        import torch
        from transformers import AutoModelForSequenceClassification, AutoTokenizer
        from ingestion.sentiment.labels import build_label_provider
    except ImportError as e:
        raise RuntimeError(f"Cần cài transformers + torch: {e}")

    device = _get_device()
    tokenizer = AutoTokenizer.from_pretrained(model_dir, local_files_only=True)
    model = AutoModelForSequenceClassification.from_pretrained(model_dir, local_files_only=True)
    model.to(device)
    model.eval()

    # Load config để lấy id2label
    import json
    config_path = Path(model_dir) / "config.json"
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    _MODEL = model
    _TOKENIZER = tokenizer
    _PROVIDER = build_label_provider(config)

    return _MODEL, _TOKENIZER, _PROVIDER


def predict_batch(
    texts: list[str],
    model_dir: str,
    *,
    batch_size: int = 32,
    max_length: int = 256,
    provider: str = "transformers",
) -> list[dict[str, Any]]:
    """Chạy inference trên batch câu.

    Returns:
        List dict: {label, score, label_id} cho mỗi câu.
    """
    model, tokenizer, provider_obj = get_model(model_dir, provider=provider)
    
    if provider == "stub":
        # Stub path - không cần torch
        import numpy as np
        results: list[dict[str, Any]] = []
        
        for i in range(0, len(texts), batch_size):
            batch = texts[i:i + batch_size]
            inputs = tokenizer(batch, padding=True, truncation=True, max_length=max_length, return_tensors="pt")
            
            outputs = model(**inputs)
            logits = outputs.logits
            
            # Softmax numpy
            exp_logits = np.exp(logits - np.max(logits, axis=-1, keepdims=True))
            probs = exp_logits / np.sum(exp_logits, axis=-1, keepdims=True)
            pred_ids = np.argmax(probs, axis=-1).tolist()
            pred_scores = np.max(probs, axis=-1).tolist()
            
            for pid, score in zip(pred_ids, pred_scores):
                label = provider_obj.from_int(pid)
                results.append({
                    "label": label.value,
                    "score": float(score),
                    "label_id": int(pid),
                })
        return results
    
    # Real transformers path
    import torch
    device = _get_device()
    results: list[dict[str, Any]] = []

    for i in range(0, len(texts), batch_size):
        batch = texts[i:i + batch_size]
        inputs = tokenizer(
            batch,
            padding=True,
            truncation=True,
            max_length=max_length,
            return_tensors="pt",
        )
        inputs = {k: v.to(device) for k, v in inputs.items()}

        with torch.no_grad():
            outputs = model(**inputs)
            logits = outputs.logits
            probs = torch.softmax(logits, dim=-1)
            pred_ids = torch.argmax(probs, dim=-1).cpu().tolist()
            pred_scores = torch.max(probs, dim=-1).values.cpu().tolist()

        for pid, score in zip(pred_ids, pred_scores):
            label = provider_obj.from_int(pid)
            results.append({
                "label": label.value,
                "score": float(score),
                "label_id": int(pid),
            })

    return results


def map_udf(
    pdf: "pandas.DataFrame",
    *,
    model_dir: str,
    batch_size: int = 32,
    max_length: int = 256,
    provider: str = "transformers",
) -> "pandas.DataFrame":
    """Pandas UDF cho Spark mapInPandas.

    Input columns: sentence_id, sentence_text, sentence_hash, ...
    Output columns: input columns + label, score, label_id, model_version
    """
    import pandas as pd

    texts = pdf["sentence_text"].tolist()
    preds = predict_batch(texts, model_dir, batch_size=batch_size, max_length=max_length, provider=provider)

    out = pdf.copy()
    out["label"] = [p["label"] for p in preds]
    out["score"] = [p["score"] for p in preds]
    out["label_id"] = [p["label_id"] for p in preds]
    out["model_version"] = os.path.basename(model_dir)

    return out


__all__ = ["get_model", "predict_batch", "map_udf"]