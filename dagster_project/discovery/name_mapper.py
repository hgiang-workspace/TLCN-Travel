"""Name Mapper - Map Vietnamese entity names to English names (and vice versa)."""

from rapidfuzz import fuzz
from typing import Optional


def normalize_name(name: str) -> str:
    """Normalize entity name for comparison."""
    if not name:
        return ""
    name = name.lower().strip()
    # Remove common prefixes/suffixes
    for prefix in ["di tích", "bãi biển", "núi", "hồ", "đền", "chùa", "công viên", "khu du lịch"]:
        if name.startswith(prefix):
            name = name[len(prefix):].strip()
    return name


def find_best_match(
    target: str,
    candidates: list[dict],
    threshold: float = 60.0,
    province: Optional[str] = None,
) -> Optional[dict]:
    """Find best matching candidate by name similarity.

    Args:
        target: Name to match against
        candidates: List of candidate dicts with 'name' field
        threshold: Minimum similarity score (0-100)
        province: Optional province filter

    Returns:
        Best matching candidate dict or None
    """
    if not target or not candidates:
        return None

    target_norm = normalize_name(target)
    best_match = None
    best_score = 0

    for candidate in candidates:
        cand_name = candidate.get("name", "")
        cand_province = candidate.get("province", "")

        # Filter by province if specified
        if province and cand_province.lower() != province.lower():
            continue

        cand_norm = normalize_name(cand_name)

        # Use token_sort_ratio for better fuzzy matching
        score = fuzz.token_sort_ratio(target_norm, cand_norm)

        if score > best_score and score >= threshold:
            best_score = score
            best_match = candidate

    return best_match


def map_entity_names(
    vn_entities: list[dict],
    en_entities: list[dict],
    threshold: float = 60.0,
) -> list[dict]:
    """Map Vietnamese entities with English entities.

    Matching strategy:
    1. Group both lists by province
    2. For each VN entity, find best EN entity match in same province
    3. Use fuzzy name matching with threshold

    Args:
        vn_entities: List of VN entity dicts
        en_entities: List of EN entity dicts
        threshold: Minimum similarity score (0-100)

    Returns:
        Merged list with name_vn, name_en fields
    """
    # Group EN entities by province for faster lookup
    en_by_province = {}
    for en_ent in en_entities:
        prov = en_ent.get("province", "")
        if prov not in en_by_province:
            en_by_province[prov] = []
        en_by_province[prov].append(en_ent)

    mapped = []

    for vn_ent in vn_entities:
        vn_name = vn_ent.get("name", "")
        province = vn_ent.get("province", "")

        # Find matching EN entity in same province
        en_candidates = en_by_province.get(province, [])
        match = find_best_match(vn_name, en_candidates, threshold, province)

        if match:
            # Merge: take VN as primary, add EN name
            merged = {**vn_ent}
            merged["name_vn"] = vn_name
            merged["name_en"] = match.get("name", "")
            merged["source_url_en"] = match.get("source_url", "")
            merged["match_score"] = fuzz.token_sort_ratio(
                normalize_name(vn_name),
                normalize_name(match.get("name", ""))
            )
        else:
            # No match: keep VN only
            merged = {**vn_ent}
            merged["name_vn"] = vn_name
            merged["name_en"] = None
            merged["source_url_en"] = None
            merged["match_score"] = 0

        mapped.append(merged)

    return mapped


def create_entity_id(name: str, province: str) -> str:
    """Create a stable entity ID from name and province."""
    name_norm = name.lower().strip().replace(" ", "_")
    province_norm = province.lower().strip().replace(" ", "_")
    return f"{province_norm}__{name_norm}"
