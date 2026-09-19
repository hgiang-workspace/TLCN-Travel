"""Convert discovery JSON outputs to CSV."""

import json
import csv
from pathlib import Path

OUTPUT_DIR = Path(__file__).parent / "outputs"


def json_to_csv(json_path: Path, csv_path: Path):
    """Convert a JSON file to CSV."""
    if not json_path.exists():
        print(f"File not found: {json_path}")
        return

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not data:
        print(f"Empty data in {json_path}")
        return

    # Get all keys from all records
    all_keys = list(dict.fromkeys(k for record in data for k in record.keys()))

    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=all_keys)
        writer.writeheader()
        writer.writerows(data)

    print(f"Converted: {json_path.name} -> {csv_path.name} ({len(data)} rows)")


def main():
    # Convert VN entities
    json_to_csv(
        OUTPUT_DIR / "entity_registry_vn.json",
        OUTPUT_DIR / "entity_registry_vn.csv",
    )

    # Convert EN entities
    json_to_csv(
        OUTPUT_DIR / "entity_registry.json",
        OUTPUT_DIR / "entity_registry.csv",
    )


if __name__ == "__main__":
    main()
