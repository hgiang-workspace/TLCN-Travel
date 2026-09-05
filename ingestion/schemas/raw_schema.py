from pydantic import BaseModel, Field, validator
from typing import Optional, Dict, Any
from datetime import datetime

class RawTripadvisorReview(BaseModel):
    """Raw schema for Tripadvisor review data before validation."""
    record_id: str = Field(..., description="Unique record identifier")
    platform: str = Field("tripadvisor", description="Source platform")
    data_type: str = Field(..., description="Type of data")
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

    class Config:
        str_strip_whitespace = True
        arbitrary_types_allowed = True