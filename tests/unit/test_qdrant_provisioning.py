import copy

import pytest

from travel_intelligence.common.configuration import (
    default_config_paths,
    load_validated,
)
from travel_intelligence.qdrant.provisioning import (
    QdrantProvisioningError,
    expand_env,
    provision,
)


class FakeQdrant:
    def __init__(self):
        self.collections = {}
        self.created = []
        self.deleted = []

    def health(self):
        return None

    def collection(self, name):
        value = self.collections.get(name)
        return copy.deepcopy(value) if value else None

    def create_collection(self, name, vector):
        self.created.append(name)
        self.collections[name] = {
            "config": {"params": {"vectors": dict(vector)}},
            "payload_schema": {},
        }

    def payload_index(self, name, field, field_schema):
        self.collections[name]["payload_schema"][field] = {
            "data_type": field_schema
        }

    def delete_collection(self, name):
        self.deleted.append(name)
        del self.collections[name]

    def upsert_point(self, name, point_id, vector, payload):
        self.collections[name].setdefault("points", {})[point_id] = payload

    def point(self, name, point_id):
        payload = self.collections[name].get("points", {}).get(point_id)
        return {"payload": payload} if payload is not None else None


def _configs():
    paths = default_config_paths()
    qdrant, _ = load_validated("qdrant", paths["qdrant"])
    embedding, _ = load_validated("embedding", paths["embedding"])
    return qdrant, embedding


def test_provisioning_creates_exactly_two_collections_and_indexes():
    qdrant, embedding = _configs()
    fake = FakeQdrant()
    first = provision(fake, qdrant_config=qdrant, embedding_config=embedding)
    assert [item.collection for item in first] == [
        "travel_reviews_v1",
        "travel_review_aspects_v1",
    ]
    assert fake.created == [
        "travel_reviews_v1",
        "travel_review_aspects_v1",
    ]
    assert fake.collections["travel_reviews_v1"]["config"]["params"]["vectors"] == {
        "size": 384,
        "distance": "Cosine",
        "datatype": "float32",
    }
    assert set(
        fake.collections["travel_review_aspects_v1"]["payload_schema"]
    ) == set(qdrant["payload_indexes"]["review_aspects"])


def test_repeated_provisioning_is_noop_for_existing_compatible_collections():
    qdrant, embedding = _configs()
    fake = FakeQdrant()
    provision(fake, qdrant_config=qdrant, embedding_config=embedding)
    fake.created.clear()
    second = provision(fake, qdrant_config=qdrant, embedding_config=embedding)
    assert fake.created == []
    assert all(not item.created for item in second)
    assert all(item.payload_indexes_created == () for item in second)


def test_incompatible_vector_schema_fails_without_delete_or_recreate():
    qdrant, embedding = _configs()
    fake = FakeQdrant()
    provision(fake, qdrant_config=qdrant, embedding_config=embedding)
    fake.collections["travel_reviews_v1"]["config"]["params"]["vectors"]["size"] = 768
    with pytest.raises(QdrantProvisioningError, match="incompatible vector schema"):
        provision(fake, qdrant_config=qdrant, embedding_config=embedding)
    assert fake.deleted == []
    assert fake.created == ["travel_reviews_v1", "travel_review_aspects_v1"]


def test_incompatible_payload_index_fails_without_silent_change():
    qdrant, embedding = _configs()
    fake = FakeQdrant()
    provision(fake, qdrant_config=qdrant, embedding_config=embedding)
    fake.collections["travel_reviews_v1"]["payload_schema"]["sentiment"][
        "data_type"
    ] = "integer"
    with pytest.raises(QdrantProvisioningError, match="payload index"):
        provision(fake, qdrant_config=qdrant, embedding_config=embedding)


def test_environment_url_expansion_and_missing_required_variable():
    default_url = "$" + "{QDRANT_URL:-http://qdrant:6333}"
    required_url = "$" + "{REQUIRED_QDRANT_URL}"
    assert expand_env(default_url, {}) == "http://qdrant:6333"
    assert expand_env(default_url, {"QDRANT_URL": "x"}) == "x"
    with pytest.raises(QdrantProvisioningError, match="environment variable"):
        expand_env(required_url, {})
