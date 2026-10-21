#!/usr/bin/env python3
"""Provision the two TASK-004 TripAdvisor retrieval collections."""

from __future__ import annotations

import sys

from travel_intelligence.qdrant.provisioning import (
    load_provisioning_configs,
    provision_from_environment,
    summary_json,
)


def main() -> int:
    try:
        _, _, fingerprint = load_provisioning_configs()
        summaries = provision_from_environment()
    except Exception as error:
        print(f"Qdrant provisioning failed: {error}", file=sys.stderr)
        return 1
    print(summary_json(summaries, embedding_fingerprint=fingerprint))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
