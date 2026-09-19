"""Discovery assets - entity discovery pipeline.

Asset graph:
  province_seeds ------+
                       +---> entity_candidates_vn
  category_seeds ------+            |
                            entity_candidates_en
                                   |
                             entity_mapped
                                   |
                            entity_registry
"""

import json
import asyncio
from pathlib import Path
from dagster import asset, Output, MetadataValue

PROJECT_ROOT = Path("/home/hgiang/TLCN")

OUTPUT_DIR = PROJECT_ROOT / "discovery" / "outputs"
VN_OUTPUT = OUTPUT_DIR / "entity_registry_vn.json"
EN_OUTPUT = OUTPUT_DIR / "entity_registry.json"


def _load_output(output_path):
    if not output_path.exists():
        return []
    with open(output_path, "r", encoding="utf-8") as f:
        return json.load(f)


def _run_vn_scraper(max_provinces=None):
    import sys
    sys.path.insert(0, str(PROJECT_ROOT))
    from discovery.discovery_vn import TripAdvisorVNDiscovery

    async def _run():
        scraper = TripAdvisorVNDiscovery(headless=True, max_provinces=max_provinces)
        await scraper.run()
        return [c.model_dump() for c in scraper.registry]

    return asyncio.run(_run())


def _run_en_scraper(max_provinces=None):
    import sys
    sys.path.insert(0, str(PROJECT_ROOT))
    from discovery.discovery import TripAdvisorDiscovery

    async def _run():
        scraper = TripAdvisorDiscovery(headless=True, max_provinces=max_provinces)
        await scraper.run()
        return [c.model_dump() for c in scraper.registry]

    return asyncio.run(_run())


@asset
def province_seeds():
    import yaml
    config_path = PROJECT_ROOT / "configs" / "provinces.yaml"
    with open(config_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    provinces = data["provinces"]
    yield Output(
        provinces,
        metadata={"count": MetadataValue.int(len(provinces)), "path": str(config_path)},
    )


@asset
def category_seeds():
    import yaml
    config_path = PROJECT_ROOT / "configs" / "categories.yaml"
    with open(config_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    categories = data["categories"]
    yield Output(
        categories,
        metadata={"count": MetadataValue.int(len(categories)), "path": str(config_path)},
    )


@asset
def entity_candidates_vn(province_seeds, category_seeds):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    existing = _load_output(VN_OUTPUT)
    if existing:
        yield Output(
            existing,
            metadata={
                "source": MetadataValue.text("cached"),
                "count": MetadataValue.int(len(existing)),
                "path": str(VN_OUTPUT),
            },
        )
        return
    results = _run_vn_scraper()
    with open(VN_OUTPUT, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    yield Output(
        results,
        metadata={
            "source": MetadataValue.text("scraped"),
            "count": MetadataValue.int(len(results)),
            "path": str(VN_OUTPUT),
        },
    )


@asset
def entity_candidates_en(province_seeds, category_seeds):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    existing = _load_output(EN_OUTPUT)
    if existing:
        yield Output(
            existing,
            metadata={
                "source": MetadataValue.text("cached"),
                "count": MetadataValue.int(len(existing)),
                "path": str(EN_OUTPUT),
            },
        )
        return
    results = _run_en_scraper()
    with open(EN_OUTPUT, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    yield Output(
        results,
        metadata={
            "source": MetadataValue.text("scraped"),
            "count": MetadataValue.int(len(results)),
            "path": str(EN_OUTPUT),
        },
    )


@asset
def entity_mapped(entity_candidates_vn, entity_candidates_en):
    from dagster_project.discovery.name_mapper import map_entity_names

    mapped = map_entity_names(
        vn_entities=entity_candidates_vn,
        en_entities=entity_candidates_en,
        threshold=60.0,
    )
    matched_count = sum(1 for e in mapped if e.get("name_en"))
    yield Output(
        mapped,
        metadata={
            "total": MetadataValue.int(len(mapped)),
            "matched": MetadataValue.int(matched_count),
            "unmatched": MetadataValue.int(len(mapped) - matched_count),
        },
    )


@asset
def entity_registry(entity_mapped):
    import sys
    sys.path.insert(0, str(PROJECT_ROOT))
    from discovery.resolvers.entity_registry import EntityRegistry

    registry_path = str(OUTPUT_DIR / "entity_registry.json")
    registry = EntityRegistry(registry_path)

    for entity in entity_mapped:
        entity_id = entity.get("candidate_id", "")
        name = entity.get("name_vn") or entity.get("name", "")
        entity_type = entity.get("entity_type", "")
        province = entity.get("province", "")

        registry.get_or_create_entity(
            canonical_name=name,
            entity_type=entity_type,
            province=province,
        )
        if entity.get("source"):
            registry.add_source_mapping(
                entity_id=entity_id,
                source=entity["source"],
                source_entity_id=entity.get("source_entity_id", ""),
            )

    yield Output(
        {"entity_count": registry.count(), "path": registry_path},
        metadata={
            "entity_count": MetadataValue.int(registry.count()),
            "path": MetadataValue.text(registry_path),
        },
    )


@asset
def discovery_export_csv(entity_candidates_vn, entity_candidates_en, entity_mapped):
    """Export discovery results to CSV files."""
    import csv

    def write_csv(data, path):
        if not data:
            return 0
        all_keys = list(dict.fromkeys(k for record in data for k in record.keys()))
        with open(path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=all_keys)
            writer.writeheader()
            writer.writerows(data)
        return len(data)

    vn_csv = OUTPUT_DIR / "entity_registry_vn.csv"
    en_csv = OUTPUT_DIR / "entity_registry.csv"
    mapped_csv = OUTPUT_DIR / "entity_mapped.csv"

    vn_count = write_csv(entity_candidates_vn, vn_csv)
    en_count = write_csv(entity_candidates_en, en_csv)
    mapped_count = write_csv(entity_mapped, mapped_csv)

    yield Output(
        {
            "vn_rows": vn_count,
            "en_rows": en_count,
            "mapped_rows": mapped_count,
        },
        metadata={
            "vn_csv": MetadataValue.text(str(vn_csv)),
            "en_csv": MetadataValue.text(str(en_csv)),
            "mapped_csv": MetadataValue.text(str(mapped_csv)),
        },
    )
