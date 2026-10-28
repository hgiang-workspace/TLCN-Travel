"""Export the pinned T07-E2A TripAdvisor category review inventory.

This is a read-only Trino audit utility.  It writes only the requested local
review artifact and cannot assign domains or write to Iceberg.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

from travel_intelligence.gold.domain_mapping import build_category_inventory

CANDIDATE_RELEASE_ID = (
    "gold-release-9bfd00eeae2748684dbc34e34aab27620f6babed6c8ad8ecfbf8f8fb558c6854"
)
SOURCE_RELEASE_ID = "891ef41b88338d85650159e037aa84286068b9823dabbe96a5fe5a63ea0e2a24"
SOURCE_ID = "tripadvisor"
QUERY = f"""
SELECT e.entity_id, e.entity_type_raw, s.source_id, e.release_id, e.source_release_id
FROM travel.gold.dim_entity e
JOIN travel.gold.bridge_entity_source s
  ON s.release_id = e.release_id AND s.entity_id = e.entity_id
WHERE e.release_id = '{CANDIDATE_RELEASE_ID}'
  AND s.source_id = '{SOURCE_ID}'
ORDER BY e.entity_id
"""


def trino_rows(endpoint: str) -> list[dict[str, object]]:
    """Run the fixed, read-only query through Trino's public statement API."""
    request = Request(
        endpoint.rstrip("/") + "/v1/statement",
        data=QUERY.encode("utf-8"),
        headers={
            "Content-Type": "text/plain; charset=utf-8",
            "X-Trino-User": "t07e2a-readonly-audit",
            "X-Trino-Catalog": "travel",
            "X-Trino-Schema": "gold",
        },
        method="POST",
    )
    rows: list[dict[str, object]] = []
    columns: list[str] | None = None
    with urlopen(request, timeout=30) as response:  # nosec B310: explicit Trino URL
        payload = json.load(response)
    while True:
        if payload.get("error"):
            raise RuntimeError(f"Trino query failed: {payload['error']}")
        if payload.get("columns"):
            columns = [str(column["name"]) for column in payload["columns"]]
        if payload.get("data"):
            if columns is None:
                raise RuntimeError("Trino returned data before columns")
            rows.extend(
                dict(zip(columns, value, strict=True)) for value in payload["data"]
            )
        next_uri = payload.get("nextUri")
        if not next_uri:
            return rows
        with urlopen(str(next_uri), timeout=30) as response:  # nosec B310: Trino cursor
            payload = json.load(response)


def build_export(rows: list[dict[str, object]]) -> dict[str, Any]:
    """Validate the pinned input and render the full category review queue."""
    if len(rows) != 4000:
        raise ValueError(f"expected 4000 candidate entities, found {len(rows)}")
    if {
        str(row["release_id"])
        for row in rows
    } != {CANDIDATE_RELEASE_ID} or {
        str(row["source_release_id"])
        for row in rows
    } != {SOURCE_RELEASE_ID}:
        raise ValueError("candidate lineage does not match the pinned release")
    inventory = build_category_inventory(rows, source_id=SOURCE_ID)
    raw_counts = Counter(
        "" if row["entity_type_raw"] is None else str(row["entity_type_raw"])
        for row in rows
    )
    raw_category_count = sum(len(row.raw_categories) for row in inventory)
    if raw_category_count != 582:
        raise ValueError(f"expected 582 raw categories, found {raw_category_count}")
    return {
        "artifact_version": "t07e2a-tripadvisor-category-inventory-v1",
        "candidate_release_id": CANDIDATE_RELEASE_ID,
        "source_release_id": SOURCE_RELEASE_ID,
        "source_id": SOURCE_ID,
        "entity_count": len(rows),
        "distinct_raw_category_count": raw_category_count,
        "distinct_normalized_category_count": len(inventory),
        "raw_categories": [
            {
                "raw_category": raw_category,
                "normalized_category": (
                    None
                    if not raw_category.strip()
                    else next(
                        item.normalized_category
                        for item in inventory
                        if raw_category in item.raw_categories
                    )
                ),
                "entity_count": entity_count,
            }
            for raw_category, entity_count in sorted(
                raw_counts.items(), key=lambda item: (-item[1], item[0])
            )
        ],
        "categories": [
            {
                "normalized_category": row.normalized_category,
                "entity_count": row.entity_count,
                "raw_categories": list(row.raw_categories),
                "default_mapping_state": "unmapped",
            }
            for row in sorted(
                inventory,
                key=lambda row: (-row.entity_count, row.normalized_category or ""),
            )
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trino-url", default="http://127.0.0.1:8080")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("docs/tasks/TASK-007E2A-tripadvisor-category-inventory.json"),
    )
    args = parser.parse_args()
    artifact = build_export(trino_rows(args.trino_url))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(artifact, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(
        f"wrote {args.output}: {artifact['entity_count']} entities, "
        f"{artifact['distinct_raw_category_count']} raw categories, "
        f"{artifact['distinct_normalized_category_count']} normalized categories"
    )


if __name__ == "__main__":
    main()
