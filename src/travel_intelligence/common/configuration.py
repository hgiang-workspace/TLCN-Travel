"Root-relative YAML configuration loading and validation."

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import yaml

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
CONFIG_ROOT = REPOSITORY_ROOT / "configs"
REQUIRED_ASPECTS = (
    "food", "service", "ambiance", "cleanliness", "price", "location", "speed",
)


class ConfigurationError(ValueError):
    "Raised when a required configuration is absent or malformed."


def resolve_config_path(path: str | Path, *, root: Path | None = None) -> Path:
    "Resolve a config path independently of the caller's working directory."
    candidate = Path(path)
    base = root or REPOSITORY_ROOT
    if not candidate.is_absolute():
        candidate = base / candidate
    candidate = candidate.resolve()
    try:
        candidate.relative_to(base.resolve())
    except ValueError as error:
        raise ConfigurationError(
            "Configuration path escapes repository root"
        ) from error
    if not candidate.is_file():
        raise ConfigurationError(f"Configuration file does not exist: {candidate}")
    return candidate


def _required(mapping: dict[str, Any], key: str, path: Path) -> Any:
    if key not in mapping:
        raise ConfigurationError(f"{path}: missing required key {key!r}")
    return mapping[key]


def load_yaml(path: str | Path, *, root: Path | None = None) -> dict[str, Any]:
    resolved = resolve_config_path(path, root=root)
    with resolved.open(encoding="utf-8") as stream:
        value = yaml.safe_load(stream)
    if not isinstance(value, dict):
        raise ConfigurationError(f"{resolved}: root must be a mapping")
    return value


def validate_pipeline_config(value: dict[str, Any], path: Path | None = None) -> None:
    name = str(path or "pipeline.yaml")
    for key in ("version", "text", "segmentation", "negation", "quality", "references"):
        _required(value, key, Path(name))
    text = value["text"]
    segmentation = value["segmentation"]
    quality = value["quality"]
    references = value["references"]
    if not isinstance(text, dict) or not isinstance(segmentation, dict):
        raise ConfigurationError(f"{name}: text and segmentation must be mappings")
    if not isinstance(quality, dict) or not isinstance(references, dict):
        raise ConfigurationError(f"{name}: quality and references must be mappings")
    for key in ("lowercase", "use_underthesea", "use_clause_split", "min_clause_len"):
        _required(text, key, Path(name))
    for key in (
        "strategy", "window_before", "window_after", "include_keyword",
        "dedupe_by_hash", "max_aspects_per_sentence",
    ):
        _required(segmentation, key, Path(name))
    for key in (
        "min_score", "max_score", "min_sentence_len", "max_sentence_len",
        "required_labels", "label_distribution_min_pct",
    ):
        _required(quality, key, Path(name))
    for key in ("model", "aspect_keywords"):
        _required(references, key, Path(name))
    if segmentation["strategy"] not in {"clause", "window", "hybrid"}:
        raise ConfigurationError(f"{name}: unsupported segmentation strategy")
    if (
        not isinstance(quality["required_labels"], list)
        or not quality["required_labels"]
    ):
        raise ConfigurationError(f"{name}: required_labels must be non-empty")


def validate_aspect_keywords(value: dict[str, Any], path: Path | None = None) -> None:
    name = str(path or "aspect_keywords.yaml")
    matching = _required(value, "matching", Path(name))
    aspects = _required(value, "aspects", Path(name))
    if not isinstance(matching, dict) or not isinstance(aspects, dict):
        raise ConfigurationError(f"{name}: matching and aspects must be mappings")
    if tuple(aspects) != REQUIRED_ASPECTS:
        raise ConfigurationError(f"{name}: taxonomy must be {REQUIRED_ASPECTS!r}")
    for aspect in REQUIRED_ASPECTS:
        keywords = aspects[aspect]
        if (
            not isinstance(keywords, list)
            or not keywords
            or not all(isinstance(item, str) and item for item in keywords)
        ):
            raise ConfigurationError(
                f"{name}: {aspect} keywords must be non-empty strings"
            )
    for key in ("lowercase", "word_boundary", "prefer_longest_match"):
        _required(matching, key, Path(name))


def validate_visobert_config(value: dict[str, Any], path: Path | None = None) -> None:
    name = str(path or "visobert.yaml")
    model = _required(value, "model", Path(name))
    inference = _required(value, "inference", Path(name))
    labels = _required(value, "classification", Path(name))
    if not all(isinstance(item, dict) for item in (model, inference, labels)):
        raise ConfigurationError(
            f"{name}: model, inference and classification must be mappings"
        )
    for key in ("id", "revision", "local_artifact_path", "fingerprint_generation"):
        _required(model, key, Path(name))
    for key in ("provider", "device", "batch_size", "max_length"):
        _required(inference, key, Path(name))
    if not isinstance(labels.get("labels"), list) or not labels["labels"]:
        raise ConfigurationError(f"{name}: classification.labels must be non-empty")


def validate_embedding_config(value: dict[str, Any], path: Path | None = None) -> None:
    name = str(path or "embedding.yaml")
    model = _required(value, "model", Path(name))
    vector = _required(value, "vector", Path(name))
    if not isinstance(model, dict) or not isinstance(vector, dict):
        raise ConfigurationError(f"{name}: model and vector must be mappings")
    for key in ("id", "status"):
        _required(model, key, Path(name))
    for key in ("dimension", "distance"):
        _required(vector, key, Path(name))
    if model["status"] != "unconfigured" and not model.get("id"):
        raise ConfigurationError(f"{name}: configured embedding requires model.id")
    if model["status"] == "configured" and (
        not isinstance(vector["dimension"], int) or vector["dimension"] <= 0
    ):
        raise ConfigurationError(
            f"{name}: configured embedding requires a positive vector dimension"
        )
    if vector["distance"] not in {"Cosine", "Dot", "Euclid", "Manhattan"}:
        raise ConfigurationError(f"{name}: unsupported vector distance")


def validate_qdrant_config(value: dict[str, Any], path: Path | None = None) -> None:
    name = str(path or "qdrant.yaml")
    for key in ("connection", "collections", "payload_indexes", "indexing"):
        _required(value, key, Path(name))
    connection = value["connection"]
    collections = value["collections"]
    indexes = value["payload_indexes"]
    if not isinstance(collections, dict) or set(collections) != {
        "reviews", "review_aspects"
    }:
        raise ConfigurationError(f"{name}: exactly two Qdrant collections are required")
    if not all(isinstance(item, str) and item for item in collections.values()):
        raise ConfigurationError(f"{name}: collection names must be non-empty strings")
    if not isinstance(indexes, dict) or set(indexes) != set(collections):
        raise ConfigurationError(f"{name}: payload indexes must cover both collections")
    if not all(isinstance(item, dict) for item in indexes.values()):
        raise ConfigurationError(f"{name}: payload indexes must be mappings")
    allowed_index_types = {"keyword", "integer", "float", "bool", "text", "uuid"}
    if any(
        field_type not in allowed_index_types
        for item in indexes.values()
        for field_type in item.values()
    ):
        raise ConfigurationError(f"{name}: unsupported payload index type")
    if not isinstance(connection, dict) or not all(
        key in connection for key in ("url", "api_key_env")
    ):
        raise ConfigurationError(
            f"{name}: connection must use URL and environment key name"
        )
    if connection["api_key_env"] and not isinstance(connection["api_key_env"], str):
        raise ConfigurationError(f"{name}: api_key_env must be a string")
    if not isinstance(value["vector_config"], str) or not value["vector_config"]:
        raise ConfigurationError(f"{name}: vector_config must reference embedding.yaml")


def validate_config(kind: str, value: dict[str, Any], path: Path | None = None) -> None:
    validators = {
        "pipeline": validate_pipeline_config,
        "aspect_keywords": validate_aspect_keywords,
        "visobert": validate_visobert_config,
        "embedding": validate_embedding_config,
        "qdrant": validate_qdrant_config,
    }
    try:
        validator = validators[kind]
    except KeyError as error:
        raise ConfigurationError(f"Unknown configuration kind: {kind}") from error
    validator(value, path)


def configuration_fingerprint(value: dict[str, Any]) -> str:
    "Return a stable SHA-256 fingerprint of parsed YAML data."
    canonical = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def load_validated(kind: str, path: str | Path) -> tuple[dict[str, Any], str]:
    resolved = resolve_config_path(path)
    value = load_yaml(resolved)
    validate_config(kind, value, resolved)
    return value, configuration_fingerprint(value)


def default_config_paths() -> dict[str, Path]:
    return {
        "pipeline": CONFIG_ROOT / "enrichment/sentiment/pipeline.yaml",
        "aspect_keywords": CONFIG_ROOT / "enrichment/sentiment/aspect_keywords.yaml",
        "visobert": CONFIG_ROOT / "models/visobert.yaml",
        "embedding": CONFIG_ROOT / "models/embedding.yaml",
        "qdrant": CONFIG_ROOT / "serving/qdrant.yaml",
    }
