"""Publication-gated semantic retrieval for the T04-E TripAdvisor indexes."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping, Protocol

from travel_intelligence.qdrant.indexing import MiniLMEmbedder, validate_vector


class RetrievalError(RuntimeError):
    """Raised for invalid requests or unavailable/incompatible retrieval state."""


class PublicationError(RetrievalError):
    """Raised when no safe committed index generation is available."""


class SearchApi(Protocol):
    def search_points(
        self,
        name: str,
        vector: list[float],
        *,
        limit: int,
        score_threshold: float | None = None,
        query_filter: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]: ...


@dataclass(frozen=True)
class ActiveGeneration:
    index_generation: str
    source_release_id: str
    enrichment_run_id: str
    started_at: str
    expected_review_point_count: int
    expected_aspect_point_count: int
    model_id: str
    model_revision: str
    model_fingerprint: str
    config_fingerprint: str


ALLOWED_FILTERS = frozenset(
    {
        "place_id",
        "aspect",
        "sentiment",
        "source_id",
        "source_record_sk",
        "source_release_id",
        "enrichment_run_id",
        "review_id",
        "review_sentiment_id",
        "index_generation",
    }
)


def _timestamp(row: Mapping[str, Any]) -> str:
    value = row.get("started_at")
    if isinstance(value, datetime):
        return value.isoformat()
    if value is None:
        raise PublicationError("committed index generation has no started_at")
    return str(value)


def select_active_generation(
    rows: list[Mapping[str, Any]],
    *,
    model_id: str,
    model_revision: str,
    model_fingerprint: str,
    config_fingerprint: str,
) -> ActiveGeneration:
    """Select the newest safe generation; fail closed on malformed ledger rows."""
    candidates: list[ActiveGeneration] = []
    for row in rows:
        if row.get("source_id") != "tripadvisor" or row.get("status") != "committed":
            continue
        required = (
            "index_generation",
            "source_release_id",
            "enrichment_run_id",
            "expected_review_point_count",
            "actual_review_point_count",
            "expected_aspect_point_count",
            "actual_aspect_point_count",
            "embedding_model_id",
            "embedding_model_revision",
            "embedding_model_fingerprint",
            "embedding_config_fingerprint",
        )
        if any(row.get(key) in (None, "") for key in required):
            raise PublicationError("committed index ledger row is incomplete")
        if row.get("lineage_consistent") is not True:
            continue
        if int(row["expected_review_point_count"]) != int(
            row["actual_review_point_count"]
        ) or int(row["expected_aspect_point_count"]) != int(
            row["actual_aspect_point_count"]
        ):
            continue
        if (
            str(row["embedding_model_id"]) != model_id
            or str(row["embedding_model_revision"]) != model_revision
            or str(row["embedding_model_fingerprint"]) != model_fingerprint
            or str(row["embedding_config_fingerprint"]) != config_fingerprint
        ):
            continue
        candidates.append(
            ActiveGeneration(
                index_generation=str(row["index_generation"]),
                source_release_id=str(row["source_release_id"]),
                enrichment_run_id=str(row["enrichment_run_id"]),
                started_at=_timestamp(row),
                expected_review_point_count=int(row["expected_review_point_count"]),
                expected_aspect_point_count=int(row["expected_aspect_point_count"]),
                model_id=model_id,
                model_revision=model_revision,
                model_fingerprint=model_fingerprint,
                config_fingerprint=config_fingerprint,
            )
        )
    unique = {item.index_generation: item for item in candidates}
    if len(unique) != len(candidates):
        raise PublicationError(
            "index ledger contains conflicting duplicate generations"
        )
    if not unique:
        raise PublicationError("no compatible committed TripAdvisor index generation")
    return max(
        unique.values(), key=lambda item: (item.started_at, item.index_generation)
    )


def _match_filter(field: str, value: Any) -> dict[str, Any]:
    if field not in ALLOWED_FILTERS:
        raise RetrievalError(f"unsupported retrieval filter: {field}")
    if not isinstance(value, str) or not value:
        raise RetrievalError(f"filter {field} must be a non-empty string")
    return {"key": field, "match": {"value": value}}


def build_filter(
    generation: ActiveGeneration, filters: Mapping[str, str] | None
) -> dict[str, Any]:
    values = dict(filters or {})
    requested_generation = values.get("index_generation")
    if requested_generation and requested_generation != generation.index_generation:
        raise RetrievalError("retrieval may only query the active committed generation")
    values["index_generation"] = generation.index_generation
    return {
        "must": [_match_filter(key, value) for key, value in sorted(values.items())]
    }


def _normalized_query(embedder: MiniLMEmbedder, query: str) -> list[float]:
    if not isinstance(query, str) or not query.strip():
        raise RetrievalError("query must be a non-empty string")
    chunks = embedder.chunks(query)
    vectors = embedder.encode(chunks)
    if len(vectors) == 1:
        vector = vectors[0]
    else:
        vector = [
            sum(item[index] for item in vectors) / len(vectors)
            for index in range(embedder.dimension)
        ]
        norm = math.sqrt(sum(item * item for item in vector))
        if norm == 0 or not math.isfinite(norm):
            raise RetrievalError("query embedding aggregation produced an invalid norm")
        vector = [item / norm for item in vector]
    result = validate_vector(vector, embedder.dimension)
    if embedder.normalize:
        norm = math.sqrt(sum(item * item for item in result))
        if not math.isclose(norm, 1.0, rel_tol=1e-4, abs_tol=1e-4):
            raise RetrievalError(f"query embedding is not normalized: norm={norm}")
    return result


def _group_results(
    collection: str, points: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for point in points:
        payload = point.get("payload")
        if not isinstance(payload, dict):
            raise RetrievalError("Qdrant returned a point without a payload")
        parent = (
            payload.get("review_sentiment_id")
            if collection == "reviews"
            else payload.get("aspect_sentiment_id")
        )
        if not parent:
            raise RetrievalError(
                "Qdrant payload is missing its logical parent identity"
            )
        key = (collection, str(parent))
        score = float(point.get("score"))
        if not math.isfinite(score):
            raise RetrievalError("Qdrant returned a non-finite similarity score")
        item = grouped.setdefault(
            key,
            {
                "collection": collection,
                "score": score,
                "point_id": str(point.get("id")),
                "payload": payload,
                "matched_chunks": [],
            },
        )
        chunk = {
            "point_id": str(point.get("id")),
            "score": score,
            "chunk_index": payload.get(
                "chunk_index", payload.get("segment_chunk_index")
            ),
            "text": payload.get("chunk_text", payload.get("segment_text")),
            "text_hash": payload.get(
                "chunk_text_hash", payload.get("segment_chunk_text_hash")
            ),
        }
        item["matched_chunks"].append(chunk)
        if (score, str(point.get("id"))) > (item["score"], item["point_id"]):
            item["score"] = score
            item["point_id"] = str(point.get("id"))
    results = list(grouped.values())
    for item in results:
        item["matched_chunks"].sort(
            key=lambda value: (-value["score"], value["point_id"])
        )
    return results


class SemanticRetriever:
    """Reusable retrieval service; transport and ledger acquisition stay external."""

    def __init__(
        self,
        api: SearchApi,
        embedder: MiniLMEmbedder,
        ledger_rows: list[Mapping[str, Any]],
        collections: Mapping[str, str],
    ) -> None:
        self.api = api
        self.embedder = embedder
        self.ledger_rows = ledger_rows
        self.collections = dict(collections)

    def search(
        self,
        query: str,
        *,
        top_k: int = 10,
        score_threshold: float | None = None,
        collection: str = "both",
        filters: Mapping[str, str] | None = None,
        offset: int = 0,
    ) -> dict[str, Any]:
        if collection not in {"reviews", "review_aspects", "both"}:
            raise RetrievalError("collection must be reviews, review_aspects or both")
        if not 1 <= top_k <= 100:
            raise RetrievalError("top_k must be between 1 and 100")
        if offset < 0:
            raise RetrievalError("offset must be non-negative")
        if score_threshold is not None and not 0 <= score_threshold <= 1:
            raise RetrievalError("Cosine score_threshold must be between 0 and 1")
        generation = select_active_generation(
            self.ledger_rows,
            model_id=self.embedder.model_id,
            model_revision=self.embedder.revision,
            model_fingerprint=self.embedder.model_fingerprint,
            config_fingerprint=self.embedder.config_fingerprint,
        )
        vector = _normalized_query(self.embedder, query)
        query_filter = build_filter(generation, filters)
        roles = ["reviews", "review_aspects"] if collection == "both" else [collection]
        grouped: list[dict[str, Any]] = []
        for role in roles:
            raw = self.api.search_points(
                self.collections[role],
                vector,
                limit=min(100, top_k * 4 + offset),
                score_threshold=score_threshold,
                query_filter=query_filter,
            )
            grouped.extend(
                _group_results("reviews" if role == "reviews" else "aspects", raw)
            )
        grouped.sort(
            key=lambda item: (-item["score"], item["collection"], item["point_id"])
        )
        page = grouped[offset : offset + top_k]
        return {
            "query": query,
            "index_generation": generation.index_generation,
            "source_release_id": generation.source_release_id,
            "enrichment_run_id": generation.enrichment_run_id,
            "model_id": generation.model_id,
            "model_revision": generation.model_revision,
            "total": len(grouped),
            "offset": offset,
            "top_k": top_k,
            "results": page,
        }


__all__ = [
    "ActiveGeneration",
    "PublicationError",
    "RetrievalError",
    "SemanticRetriever",
    "build_filter",
    "select_active_generation",
]
