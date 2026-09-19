"""Entity Candidate Schema - unified schema for Discovery results."""

from dataclasses import dataclass, asdict


@dataclass
class EntityCandidate:
    candidate_id: str
    source: str
    source_entity_id: str
    name: str
    entity_type: str
    province: str
    district: str = ""
    address: str = ""
    latitude: float = 0.0
    longitude: float = 0.0
    url: str = ""
    discovered_by: str = "province_category"
    discovered_at: str = ""
