#!/usr/bin/env python3
"""Publish a validated TripAdvisor snapshot to MinIO."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from minio import Minio

from travel_intelligence.ingestion.bronze_writer import MinioObjectStore, publish_bronze
from travel_intelligence.ingestion.tripadvisor import adapt_tripadvisor


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("snapshot", type=Path)
    parser.add_argument(
        "--endpoint", default=os.environ.get("MINIO_ENDPOINT", "127.0.0.1:9000")
    )
    parser.add_argument(
        "--bucket", default=os.environ.get("S3_BUCKET", "travel-lakehouse")
    )
    parser.add_argument("--secure", action="store_true")
    parser.add_argument(
        "--allow-local-source",
        action="store_true",
        help="allow a snapshot outside tests/fixtures after explicit review",
    )
    args = parser.parse_args()
    fixture_root = Path("tests/fixtures").resolve()
    if args.snapshot.resolve().parent != fixture_root and not args.allow_local_source:
        parser.error(
            "snapshot must be directly under tests/fixtures unless "
            "--allow-local-source is provided"
        )
    result = adapt_tripadvisor(args.snapshot)
    client = Minio(
        args.endpoint,
        access_key=os.environ["MINIO_ROOT_USER"],
        secret_key=os.environ["MINIO_ROOT_PASSWORD"],
        secure=args.secure,
    )
    publication = publish_bronze(MinioObjectStore(client, args.bucket), result)
    print(publication)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

