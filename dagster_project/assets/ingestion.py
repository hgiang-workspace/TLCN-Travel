"""Ingestion assets - entity-based data collection.

Asset graph:
  entity_registry → bronze_tripadvisor
"""

import json
from pathlib import Path
from dagster import asset, Output, MetadataValue

PROJECT_ROOT = Path("/home/hgiang/TLCN")


@asset
def bronze_tripadvisor(entity_registry: dict) -> dict:
    """Collect Tripadvisor data for entities and write to Bronze.

    Calls ingestion/collectors/tripadvisor/collector.py for each entity.
    Writes raw JSONL to MinIO Bronze path (or local temp for MVP).
    """
    import sys
    sys.path.insert(0, str(PROJECT_ROOT))
    from ingestion.orchestrator import (
        load_entity_registry,
        get_entities_for_source,
        dispatch_to_collector,
        write_to_bronze,
    )

    registry_path = str(PROJECT_ROOT / "discovery" / "outputs" / "entity_registry.json")
    registry = load_entity_registry(registry_path)
    entities = get_entities_for_source(registry, "tripadvisor")

    if not entities:
        yield Output(
            {"records_collected": 0, "output_path": ""},
            metadata={"status": MetadataValue.text("no_entities")},
        )
        return

    config = {
        "base_url": "https://www.tripadvisor.com",
        "rate_limit": {"requests_per_second": 2},
    }

    records = dispatch_to_collector("tripadvisor", entities, config)
    valid_records = [r for r in records if r.get("record", {}).get("content_raw", "").strip()]
    output_path = write_to_bronze(valid_records, "tripadvisor")

    yield Output(
        {"records_collected": len(valid_records), "output_path": output_path},
        metadata={
            "records_collected": MetadataValue.int(len(valid_records)),
            "output_path": MetadataValue.text(output_path),
            "entities_processed": MetadataValue.int(len(entities)),
        },
    )
