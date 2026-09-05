import json
import re
from datetime import datetime
from typing import List, Optional, Dict, Any

from .collector import TripadvisorReview

class TripadvisorParser:
    @staticmethod
    def parse_raw_review(raw_data: Dict[str, Any]) -> TripadvisorReview:
        record_id = raw_data.get("id", "")
        if not record_id:
            record_id = f"ta_{hash(str(raw_data)) % 1000000}"

        content_raw = raw_data.get("content", "")
        if isinstance(content_raw, str):
            content = {"raw_text": content_raw}
        elif isinstance(content_raw, dict):
            content = content_raw
        else:
            content = {"raw_text": str(content_raw)}

        location = raw_data.get("location", {})
        if isinstance(location, str):
            location = {"specific_place": location}

        engagement = raw_data.get("engagement", {})
        if isinstance(engagement, int):
            engagement = {"like_count": engagement, "comment_count": 0, "share_count": 0}
        elif not isinstance(engagement, dict):
            engagement = {"like_count": 0, "comment_count": 0, "share_count": 0}

        like_count = engagement.get("like_count", 0)
        comment_count = engagement.get("comment_count", 0)
        share_count = engagement.get("share_count", 0)

        category = raw_data.get("category", "travel")
        if not category:
            category = "travel"

        rating = raw_data.get("rating")
        if rating is not None:
            try:
                rating = float(rating)
                if rating < 1 or rating > 5:
                    rating = None
            except (ValueError, TypeError):
                rating = None

        scraped_at = raw_data.get("scraped_at")
        if not scraped_at:
            scraped_at = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")

        try:
            datetime.fromisoformat(str(scraped_at).replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            scraped_at = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")

        return TripadvisorReview(
            record_id=record_id,
            platform="tripadvisor",
            data_type="review",
            content=content,
            location=location,
            category=category,
            engagement={
                "like_count": like_count,
                "comment_count": comment_count,
                "share_count": share_count
            },
            rating=rating,
            scraped_at=str(scraped_at)
        )

    @staticmethod
    def parse_reviews_batch(raw_data: List[Dict[str, Any]]) -> List[TripadvisorReview]:
        return [TripadvisorParser.parse_raw_review(item) for item in raw_data]

    @staticmethod
    def to_jsonl(review: TripadvisorReview) -> str:
        return review.to_jsonl()

    @staticmethod
    def to_jsonl_batch(reviews: List[TripadvisorReview]) -> str:
        return "\n".join([TripadvisorParser.to_jsonl(r) for r in reviews])