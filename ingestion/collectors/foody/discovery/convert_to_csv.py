#!/usr/bin/env python3
"""
Convert Foody restaurant data + TripAdvisor entity registry to keywords.csv

Combines 2 data sources:
1. Foody restaurant data (data/raw/foody/foody_quan_an.json) - name + province
2. TripAdvisor entity registry VN (discovery/outputs/entity_registry_vn.csv) - name + province

Output: keywords.csv with columns (STT, Key_word) where Key_word = name + province
"""

import csv
import json
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[4]  # ingestion/collectors/foody/discovery -> repo root

# FOODY_JSON = 0 #BASE_DIR / "data" / "raw" / "foody" / "foody_quan_an.json"
TRIPADVISOR_CSV = BASE_DIR / "discovery" / "outputs" / "entity_registry_vn.csv"
OUTPUT_CSV = BASE_DIR / "ingestion" / "collectors" / "foody" / "discovery" / "keywords.csv"


# def load_foody_data() -> list[dict]:
#     """Load Foody restaurant data from JSON."""
#     if not FOODY_JSON.exists():
#         print(f"Warning: {FOODY_JSON} not found")
#         return []

#     with open(FOODY_JSON, "r", encoding="utf-8") as f:
#         data = json.load(f)

#     results = []
#     for item in data:
#         name = item.get("name", "").strip()
#         province = item.get("province", "").strip()
#         if name and province:
#             results.append({"name": name, "province": province, "source": "foody"})

#     print(f"Loaded {len(results)} restaurants from Foody")
#     return results


def load_tripadvisor_data() -> list[dict]:
    """Load TripAdvisor entity registry from CSV (Vietnamese version)."""
    if not TRIPADVISOR_CSV.exists():
        print(f"Warning: {TRIPADVISOR_CSV} not found")
        return []

    results = []
    with open(TRIPADVISOR_CSV, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row.get("name", "").strip()
            province = row.get("province", "").strip()
            if name and province:
                results.append({"name": name, "province": province, "source": "tripadvisor_vn"})

    print(f"Loaded {len(results)} entities from TripAdvisor VN")
    return results


def combine_and_deduplicate(ta_data: list[dict]) -> list[dict]:
    """Combine and deduplicate by (name, province) pair."""
    seen = set()
    combined = []

    for item in  ta_data:
        key = (item["name"].lower(), item["province"].lower())
        if key not in seen:
            seen.add(key)
            combined.append(item)

    print(f"Total unique items after deduplication: {len(combined)}")
    return combined


def write_keywords_csv(items: list[dict], output_path: Path) -> None:
    """Write keywords.csv with STT and Key_word (name + province)."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["STT", "Key_word"])

        for idx, item in enumerate(items, start=1):
            keyword = f"{item['name']} {item['province']}"
            writer.writerow([idx, keyword])

    print(f"Written {len(items)} keywords to {output_path}")


def main() -> None:
    print("=" * 60)
    print("Converting Foody + TripAdvisor data to keywords.csv")
    print("=" * 60)

    # Load both sources
    # foody_data = load_foody_data()
    ta_data = load_tripadvisor_data()

    # Combine and deduplicate
    combined = combine_and_deduplicate(ta_data)

    # Sort by province then name for consistent ordering
    combined.sort(key=lambda x: (x["province"].lower(), x["name"].lower()))

    # Write output
    write_keywords_csv(combined, OUTPUT_CSV)

    print("=" * 60)
    print("Done!")
    print("=" * 60)


if __name__ == "__main__":
    main()