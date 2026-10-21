#!/usr/bin/env python3
"""Validate a local TripAdvisor snapshot without MinIO or Parquet writes."""

from __future__ import annotations

import argparse

from travel_intelligence.ingestion.tripadvisor import adapt_tripadvisor, format_dry_run


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("snapshot", help="path to a local TripAdvisor JSON snapshot")
    args = parser.parse_args()
    try:
        result = adapt_tripadvisor(args.snapshot)
    except ValueError as exc:
        parser.error(str(exc))
    print(format_dry_run(result))
    return 0 if result.rejected_records == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())

