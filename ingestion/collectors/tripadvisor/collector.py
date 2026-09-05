from pydantic import BaseModel, Field, validator
from typing import Optional, Dict, Any
import json
import re
from datetime import datetime

class TripadvisorReview(BaseModel):
    record_id: str = Field(..., description="Unique record identifier")
    platform: str = Field("tripadvisor", description="Source platform")
    data_type: str = Field(..., description="Type of data: review, rating, etc.")
    content: Dict[str, Any] = Field(..., description="Content with raw_text")
    location: Dict[str, Any] = Field(..., description="Location information")
    category: str = Field(..., description="Review category")
    engagement: Dict[str, int] = Field(..., description="Engagement metrics")
    rating: Optional[float] = Field(None, description="Rating score")
    scraped_at: str = Field(..., description="ISO timestamp of scraping")

    @validator('rating')
    def rating_must_be_valid(cls, v):
        if v is not None and (v < 1 or v > 5):
            raise ValueError('Rating must be between 1 and 5')
        return v

    @validator('scraped_at')
    def scraped_at_must_be_iso(cls, v):
        try:
            datetime.fromisoformat(v.replace('Z', '+00:00'))
        except (ValueError, AttributeError):
            raise ValueError('scraped_at must be ISO format')
        return v

    def to_jsonl(self) -> str:
        return json.dumps({
            "record_id": self.record_id,
            "platform": self.platform,
            "data_type": self.data_type,
            "content_raw": self.content.get("raw_text", ""),
            "location_province": self.location.get("province", ""),
            "location_place": self.location.get("specific_place", ""),
            "category": self.category,
            "engagement_like": self.engagement.get("like_count", 0),
            "engagement_comment": self.engagement.get("comment_count", 0),
            "engagement_share": self.engagement.get("share_count", 0),
            "rating": self.rating,
            "scraped_at": self.scraped_at
        }, ensure_ascii=False)

class TripadvisorCollector:
    def __init__(self, config: Dict[str, Any]):
        self.config = config

    def collect(self, query: str = "") -> list[TripadvisorReview]:
        # Collector framework - actual scraping logic in parser
        raise NotImplementedError

    def validate(self, data: dict) -> TripadvisorReview:
        return TripadvisorReview(**data)