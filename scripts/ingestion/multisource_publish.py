#!/usr/bin/env python3
"""Publish a validated Foody, TikTok or Vexere snapshot to MinIO."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from minio import Minio

from travel_intelligence.ingestion.bronze_writer import MinioObjectStore, publish_bronze
from travel_intelligence.ingestion.multisource import (
    adapt_foody,
    adapt_tiktok,
    adapt_vexere,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("source", choices=("foody", "tiktok", "vexere"))
    parser.add_argument("snapshot", type=Path)
    parser.add_argument(
        "--endpoint", default=os.environ.get("MINIO_ENDPOINT", "127.0.0.1:9000")
    )
    parser.add_argument(
        "--bucket", default=os.environ.get("S3_BUCKET", "travel-lakehouse")
    )
    parser.add_argument(
        "--allow-local-source",
        action="store_true",
        help="allow a snapshot outside tests/fixtures after explicit review",
    )
    args = parser.parse_args()
    if args.snapshot.resolve().parent != Path("tests/fixtures").resolve():
        if not args.allow_local_source:
            parser.error(
                "snapshot must be directly under tests/fixtures unless "
                "--allow-local-source is provided"
            )
    adapter = {"foody": adapt_foody, "tiktok": adapt_tiktok, "vexere": adapt_vexere}[
        args.source
    ]
    result = adapter(args.snapshot)
    client = Minio(
        args.endpoint,
        access_key=os.environ["MINIO_ROOT_USER"],
        secret_key=os.environ["MINIO_ROOT_PASSWORD"],
        secure=False,
    )
    print(publish_bronze(MinioObjectStore(client, args.bucket), result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
