"""Idempotent Qdrant collection and payload-index provisioning.

The implementation uses Qdrant's HTTP API directly so provisioning does not
require an embedding library or download model artifacts.  It never deletes or
recreates an incompatible collection.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from typing import Any, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlparse
from urllib.request import Request, urlopen

from travel_intelligence.common.configuration import (
    ConfigurationError,
    default_config_paths,
    load_validated,
    resolve_config_path,
)

_ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")


class QdrantProvisioningError(RuntimeError):
    """Raised when Qdrant is unavailable or has an incompatible schema."""


class QdrantApi(Protocol):
    def health(self) -> None: ...

    def collection(self, name: str) -> dict[str, Any] | None: ...

    def create_collection(self, name: str, vector: dict[str, Any]) -> None: ...

    def payload_index(self, name: str, field: str, field_schema: str) -> None: ...

    def delete_collection(self, name: str) -> None: ...

    def upsert_point(
        self, name: str, point_id: str, vector: list[float], payload: dict[str, Any]
    ) -> None: ...

    def upsert_points(self, name: str, points: list[dict[str, Any]]) -> None: ...

    def point(self, name: str, point_id: str) -> dict[str, Any] | None: ...

    def search_points(
        self,
        name: str,
        vector: list[float],
        *,
        limit: int,
        score_threshold: float | None = None,
        query_filter: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]: ...


def expand_env(value: str, environ: dict[str, str] | None = None) -> str:
    environment = environ if environ is not None else os.environ

    def replace(match: re.Match[str]) -> str:
        variable, default = match.group(1), match.group(2)
        result = environment.get(variable)
        if result is None:
            result = default
        if result is None:
            raise QdrantProvisioningError(
                f"Required environment variable is missing: {variable}"
            )
        return result

    return _ENV_PATTERN.sub(replace, value)


class QdrantHttpClient:
    def __init__(
        self,
        url: str,
        *,
        api_key: str | None = None,
        timeout: float = 5.0,
    ) -> None:
        parsed = urlparse(url.rstrip("/"))
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise QdrantProvisioningError(f"Invalid Qdrant URL: {url}")
        self.base_url = url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout

    def _request(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        data = (
            json.dumps(body, ensure_ascii=False).encode("utf-8")
            if body is not None
            else None
        )
        headers = {"Accept": "application/json"}
        if body is not None:
            headers["Content-Type"] = "application/json"
        if self.api_key:
            headers["api-key"] = self.api_key
        request = Request(
            self.base_url + path,
            data=data,
            headers=headers,
            method=method,
        )
        try:
            with urlopen(request, timeout=self.timeout) as response:
                response_body = response.read().decode("utf-8")
                if not response_body:
                    return {}
                try:
                    return json.loads(response_body)
                except json.JSONDecodeError:
                    if method == "GET" and path == "/readyz":
                        return {}
                    raise
        except HTTPError as error:
            if error.code == 404:
                raise
            detail = _response_detail(error)
            raise QdrantProvisioningError(
                f"Qdrant API {method} {path} failed with HTTP {error.code}: {detail}"
            ) from error
        except (URLError, TimeoutError, OSError, json.JSONDecodeError) as error:
            raise QdrantProvisioningError(
                f"Qdrant connection failed for {method} {path}: {error}"
            ) from error

    def health(self) -> None:
        self._request("GET", "/readyz")

    def collection(self, name: str) -> dict[str, Any] | None:
        path = "/collections/" + quote(name, safe="")
        try:
            return self._request("GET", path).get("result")
        except HTTPError as error:
            if error.code == 404:
                return None
            raise

    def create_collection(self, name: str, vector: dict[str, Any]) -> None:
        path = "/collections/" + quote(name, safe="")
        self._request("PUT", path, {"vectors": vector})

    def payload_index(self, name: str, field: str, field_schema: str) -> None:
        collection = quote(name, safe="")
        self._request(
            "PUT",
            f"/collections/{collection}/index",
            {"field_name": field, "field_schema": field_schema},
        )

    def delete_collection(self, name: str) -> None:
        self._request("DELETE", "/collections/" + quote(name, safe=""))

    def upsert_point(
        self,
        name: str,
        point_id: str,
        vector: list[float],
        payload: dict[str, Any],
    ) -> None:
        collection = quote(name, safe="")
        self._request(
            "PUT",
            f"/collections/{collection}/points",
            {"points": [{"id": point_id, "vector": vector, "payload": payload}]},
        )

    def upsert_points(self, name: str, points: list[dict[str, Any]]) -> None:
        collection = quote(name, safe="")
        self._request(
            "PUT",
            f"/collections/{collection}/points",
            {"points": points},
        )

    def point(self, name: str, point_id: str) -> dict[str, Any] | None:
        collection = quote(name, safe="")
        try:
            result = self._request(
                "POST",
                f"/collections/{collection}/points",
                {"ids": [point_id], "with_payload": True, "with_vector": False},
            ).get("result", [])
        except HTTPError as error:
            if error.code == 404:
                return None
            raise
        return result[0] if result else None

    def search_points(
        self,
        name: str,
        vector: list[float],
        *,
        limit: int,
        score_threshold: float | None = None,
        query_filter: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        collection = quote(name, safe="")
        body: dict[str, Any] = {
            "vector": vector,
            "limit": limit,
            "with_payload": True,
            "with_vector": False,
        }
        if score_threshold is not None:
            body["score_threshold"] = score_threshold
        if query_filter is not None:
            body["filter"] = query_filter
        return self._request(
            "POST", f"/collections/{collection}/points/search", body
        ).get("result", [])


def _response_detail(error: HTTPError) -> str:
    try:
        return error.read().decode("utf-8")[:500]
    except (OSError, UnicodeDecodeError):
        return error.reason or "unknown error"


@dataclass(frozen=True)
class ProvisioningSummary:
    collection: str
    created: bool
    payload_indexes_created: tuple[str, ...]
    payload_indexes_existing: tuple[str, ...]


def _vector_from_collection(collection: dict[str, Any]) -> dict[str, Any]:
    try:
        vectors = collection["config"]["params"]["vectors"]
    except (KeyError, TypeError) as error:
        raise QdrantProvisioningError(
            "Qdrant collection response has no default vector configuration"
        ) from error
    if (
        not isinstance(vectors, dict)
        or "size" not in vectors
        or "distance" not in vectors
    ):
        raise QdrantProvisioningError(
            "Qdrant collection uses an unsupported or named-vector schema"
        )
    return vectors


def _payload_schema(collection: dict[str, Any]) -> dict[str, Any]:
    schema = collection.get("payload_schema", {})
    return schema if isinstance(schema, dict) else {}


def _schema_type(value: Any) -> str | None:
    if isinstance(value, str):
        return value.lower()
    if isinstance(value, dict):
        data_type = value.get("data_type", value.get("type"))
        return str(data_type).lower() if data_type else None
    return None


def _validate_vector(
    collection_name: str,
    collection: dict[str, Any],
    expected: dict[str, Any],
) -> None:
    actual = _vector_from_collection(collection)
    expected_size = expected["size"]
    expected_distance = str(expected["distance"]).lower()
    actual_distance = str(actual["distance"]).lower()
    if actual.get("size") != expected_size or actual_distance != expected_distance:
        raise QdrantProvisioningError(
            f"Collection {collection_name!r} has incompatible vector schema: "
            f"actual size={actual.get('size')}, distance={actual.get('distance')}; "
            f"expected size={expected_size}, distance={expected['distance']}. "
            "It will not be deleted or recreated."
        )
    if "datatype" in expected and actual.get("datatype", "float32").lower() != str(
        expected["datatype"]
    ).lower():
        raise QdrantProvisioningError(
            f"Collection {collection_name!r} has incompatible vector datatype; "
            "it will not be deleted or recreated."
        )


def _embedding_vector_config(embedding: dict[str, Any]) -> dict[str, Any]:
    model = embedding["model"]
    vector = embedding["vector"]
    if model.get("status") != "configured":
        raise ConfigurationError("embedding.yaml must select a configured model")
    dimension = vector.get("dimension")
    if not isinstance(dimension, int) or dimension <= 0:
        raise ConfigurationError("embedding.yaml vector.dimension must be positive")
    return {
        "size": dimension,
        "distance": vector["distance"],
        "datatype": vector["datatype"],
    }


def provision(
    api: QdrantApi,
    *,
    qdrant_config: dict[str, Any],
    embedding_config: dict[str, Any],
) -> tuple[ProvisioningSummary, ...]:
    expected_vector = _embedding_vector_config(embedding_config)
    summaries = []
    for role in ("reviews", "review_aspects"):
        name = qdrant_config["collections"][role]
        current = api.collection(name)
        created = current is None
        if created:
            api.create_collection(name, expected_vector)
            current = api.collection(name)
            if current is None:
                raise QdrantProvisioningError(
                    f"Qdrant did not return collection after creation: {name}"
                )
        else:
            _validate_vector(name, current, expected_vector)
        created_indexes = []
        existing_indexes = []
        schema = _payload_schema(current)
        for field, field_type in qdrant_config["payload_indexes"][role].items():
            actual_type = _schema_type(schema.get(field))
            if actual_type is None:
                api.payload_index(name, field, field_type)
                created_indexes.append(field)
            elif actual_type != field_type.lower():
                raise QdrantProvisioningError(
                    f"Collection {name!r} payload index {field!r} has type "
                    f"{actual_type!r}, expected {field_type!r}; it will not be changed."
                )
            else:
                existing_indexes.append(field)
        summaries.append(
            ProvisioningSummary(
                name,
                created,
                tuple(created_indexes),
                tuple(existing_indexes),
            )
        )
    return tuple(summaries)


def load_provisioning_configs() -> tuple[dict[str, Any], dict[str, Any], str]:
    paths = default_config_paths()
    qdrant, _ = load_validated("qdrant", paths["qdrant"])
    embedding_path = resolve_config_path(qdrant["vector_config"])
    embedding, fingerprint = load_validated("embedding", embedding_path)
    return qdrant, embedding, fingerprint


def build_http_client(
    qdrant_config: dict[str, Any],
    *,
    environ: dict[str, str] | None = None,
    timeout: float = 5.0,
) -> QdrantHttpClient:
    environment = environ if environ is not None else os.environ
    connection = qdrant_config["connection"]
    url = expand_env(connection["url"], environment)
    key_name = connection.get("api_key_env")
    api_key = environment.get(key_name) if key_name else None
    return QdrantHttpClient(url, api_key=api_key, timeout=timeout)


def provision_from_environment() -> tuple[ProvisioningSummary, ...]:
    qdrant, embedding, _ = load_provisioning_configs()
    client = build_http_client(qdrant)
    client.health()
    return provision(
        client,
        qdrant_config=qdrant,
        embedding_config=embedding,
    )


def summary_json(
    summaries: tuple[ProvisioningSummary, ...],
    *,
    embedding_fingerprint: str | None = None,
) -> str:
    return json.dumps(
        {
            "status": "ok",
            "embedding_config_fingerprint": embedding_fingerprint,
            "collections": [
                {
                    "name": summary.collection,
                    "created": summary.created,
                    "payload_indexes_created": list(summary.payload_indexes_created),
                    "payload_indexes_existing": list(summary.payload_indexes_existing),
                }
                for summary in summaries
            ],
        },
        sort_keys=True,
    )


__all__ = [
    "ProvisioningSummary",
    "QdrantHttpClient",
    "QdrantProvisioningError",
    "build_http_client",
    "expand_env",
    "load_provisioning_configs",
    "provision",
    "provision_from_environment",
    "summary_json",
]
