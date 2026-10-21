#!/usr/bin/env python3
"""Run non-destructive Qdrant provisioning and persistence checks.

The optional persistence collection is a temporary, operator-named collection.
Run this command before and after a Qdrant restart with the same name.  The
production-named collections are never deleted by this script.
"""

from __future__ import annotations

import argparse
import sys

from travel_intelligence.qdrant.provisioning import (
    QdrantProvisioningError,
    _schema_type,
    _validate_vector,
    build_http_client,
    load_provisioning_configs,
    provision,
)

PERSISTENCE_POINT_ID = "00000000-0000-4000-8000-000000000004"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--persistence-collection",
        help="temporary collection to check across a container restart",
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="delete only the explicitly named temporary persistence collection",
    )
    args = parser.parse_args()
    if args.cleanup and not args.persistence_collection:
        parser.error("--cleanup requires --persistence-collection")
    try:
        qdrant, embedding, fingerprint = load_provisioning_configs()
        client = build_http_client(qdrant)
        client.health()
        first = provision(
            client, qdrant_config=qdrant, embedding_config=embedding
        )
        second = provision(
            client, qdrant_config=qdrant, embedding_config=embedding
        )
        if any(summary.created for summary in second):
            raise QdrantProvisioningError(
                "Provisioning replay created a production collection"
            )
        for role, name in qdrant["collections"].items():
            metadata = client.collection(name)
            if metadata is None:
                raise QdrantProvisioningError(f"Collection disappeared: {name}")
            expected_indexes = qdrant["payload_indexes"][role]
            schema = metadata.get("payload_schema", {})
            missing = [
                field
                for field, field_type in expected_indexes.items()
                if _schema_type(schema.get(field)) != field_type
            ]
            if missing:
                raise QdrantProvisioningError(
                    f"Collection {name} is missing required payload indexes: {missing}"
                )
        persistence = None
        if args.persistence_collection:
            vector = {
                "size": embedding["vector"]["dimension"],
                "distance": embedding["vector"]["distance"],
                "datatype": embedding["vector"]["datatype"],
            }
            existing = client.collection(args.persistence_collection)
            if existing is None:
                client.create_collection(args.persistence_collection, vector)
            else:
                _validate_vector(args.persistence_collection, existing, vector)
            point_before = client.point(
                args.persistence_collection, PERSISTENCE_POINT_ID
            )
            if point_before is None:
                client.upsert_point(
                    args.persistence_collection,
                    PERSISTENCE_POINT_ID,
                    [0.0] * vector["size"],
                    {"test_resource": "task004b-persistence-smoke"},
                )
            point_after = client.point(
                args.persistence_collection, PERSISTENCE_POINT_ID
            )
            if point_after is None:
                raise QdrantProvisioningError(
                    "Persistence smoke point was not readable after upsert"
                )
            persistence = {
                "collection": args.persistence_collection,
                "point": PERSISTENCE_POINT_ID,
                "preexisting": point_before is not None,
                "present": True,
            }
            if args.cleanup:
                client.delete_collection(args.persistence_collection)
                persistence["cleaned_up"] = True
        print(
            {
                "status": "ok",
                "embedding_config_fingerprint": fingerprint,
                "collections_checked": sorted(qdrant["collections"].values()),
                "replay_created_collections": [
                    summary.collection for summary in second if summary.created
                ],
                "persistence": persistence,
                "initial_provisioning": [
                    {
                        "name": summary.collection,
                        "created": summary.created,
                        "indexes_created": list(summary.payload_indexes_created),
                    }
                    for summary in first
                ],
            }
        )
        return 0
    except Exception as error:
        print(f"Qdrant smoke test failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
