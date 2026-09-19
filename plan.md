# Plan: Import Discovery Results as Dagster Assets

## Goal

- Keep `discovery/` folder as-is, call functions from Dagster
- Add entity name mapping phase (EN ↔ VN)
- Drop Airflow

---

## Current Discovery Flow

```
discovery/discovery.py      → outputs/entity_registry.json     (EN)
discovery/discovery_vn.py   → outputs/entity_registry_vn.json  (VN)
```

---

## New Asset Graph

```
province_seeds ─────┐
                    ├──→ entity_candidates_vn ──→ entity_candidates_en ──→ entity_mapped
category_seeds ─────┘                                                        ↓
                                                                     entity_registry
                                                                            ↓
                                                                   bronze_tripadvisor
```

---

## Phase 1: Create `dagster_project/assets/discovery.py`

**Asset: `province_seeds`**
- Load from `configs/provinces.yaml`
- Return `list[dict]`

**Asset: `category_seeds`**
- Load from `configs/categories.yaml`
- Return `list[str]`

**Asset: `entity_candidates_vn`**
- Depends on: `province_seeds`, `category_seeds`
- Call `discovery_vn.TripAdvisorVNDiscovery` for VN source
- Output: list of entity dicts from VN scraper

**Asset: `entity_candidates_en`**
- Depends on: `province_seeds`, `category_seeds`
- Call `discovery.TripAdvisorDiscovery` for EN source
- Output: list of entity dicts from EN scraper

**Asset: `entity_mapped`**
- Depends on: `entity_candidates_vn`, `entity_candidates_en`
- Map VN entity names ↔ EN entity names using fuzzy matching
- Output: merged list with both name_vn and name_en fields

**Asset: `entity_registry`**
- Depends on: `entity_mapped`
- Create/update `EntityRegistry` (JSON)
- Persist to `discovery/outputs/entity_registry.json`

---

## Phase 2: Create `dagster_project/discovery/name_mapper.py`

**New file: `dagster_project/discovery/name_mapper.py`**

Purpose: Map Vietnamese entity names to English names (and vice versa)

```python
def map_entity_names(
    vn_entities: list[dict],
    en_entities: list[dict],
    threshold: float = 60.0,
) -> list[dict]:
    """
    Match VN entities with EN entities by:
    1. Same province + fuzzy name match
    2. Same source_url domain pattern
    3. Same entity_type

    Returns merged list with:
    - name_vn: Vietnamese name
    - name_en: English name (or None if no match)
    - province, entity_type, source_url, etc.
    """
```

**Matching strategy:**
1. Group both lists by province
2. For each VN entity, find best EN entity match in same province
3. Use `rapidfuzz.fuzz.token_sort_ratio` for name similarity
4. If score >= threshold → map them together
5. If no match → keep name_vn only, name_en = None

---

## Phase 3: Create `dagster_project/discovery/__init__.py`

Empty init file for import convenience.

---

## Phase 4: Update `dagster_project/assets/discovery.py`

**Rewrite all 5 assets to call discovery/ functions directly:**

```python
import sys
from pathlib import Path

PROJECT_ROOT = Path("/home/hgiang/TLCN")
sys.path.insert(0, str(PROJECT_ROOT))

from discovery.discovery_vn import TripAdvisorVNDiscovery
from discovery.discovery import TripAdvisorDiscovery
```

**Asset implementations:**
- `province_seeds`: Load YAML
- `category_seeds`: Load YAML
- `entity_candidates_vn`: Instantiate `TripAdvisorVNDiscovery`, call `run()`, read output JSON
- `entity_candidates_en`: Instantiate `TripAdvisorDiscovery`, call `run()`, read output JSON
- `entity_mapped`: Call `map_entity_names(vn, en)`
- `entity_registry`: Call `EntityRegistry` from `discovery.resolvers.entity_registry`

---

## Phase 5: Merge Configs

**Update `configs/provinces.yaml`**
- Merge all 34 provinces from `discovery/seeds/provinces.yaml`
- Keep dict format: `{name, code}`

**Update `configs/categories.yaml`**
- Merge hierarchical categories from `discovery/seeds/categories.yaml`

---

## Phase 6: Update `dagster_project/definitions.py`

- Import all 6 discovery assets
- Register with resources

---

## Phase 7: Cleanup

**Delete `airflow/` folder**

---

## File Changes Summary

| File | Action |
|------|--------|
| `dagster_project/discovery/__init__.py` | Create |
| `dagster_project/discovery/name_mapper.py` | Create |
| `dagster_project/assets/discovery.py` | Rewrite |
| `configs/provinces.yaml` | Update (merge 34 provinces) |
| `configs/categories.yaml` | Update (merge hierarchical) |
| `dagster_project/definitions.py` | Update imports |
| `airflow/` | Delete |

---

## Dependencies

Add to `requirement.txt`:
```
rapidfuzz>=3.0.0
```

---

## Execution Order

1. Create `dagster_project/discovery/__init__.py`
2. Create `dagster_project/discovery/name_mapper.py`
3. Rewrite `dagster_project/assets/discovery.py`
4. Merge configs (provinces + categories)
5. Update `dagster_project/definitions.py`
6. Delete `airflow/`
7. Test: `dagster dev`
