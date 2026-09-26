"""Unit test schema mới (mục 9.2 của plan — Phase 1/7)."""

import json
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[4]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from ingestion.schemas.raw_schema import (
    TripadvisorCommentRecord,
    TripadvisorImage,
    TripadvisorReviewItem,
)


def test_derived_fields():
    rec = TripadvisorCommentRecord(
        record_id="ta_abc123", entity_name="Núi Ngũ Hành Sơn", province="Da Nang",
        source_url="https://www.tripadvisor.com.vn/Attraction_Review-g298085-d454980-Reviews-...",
        rating=4.5,
        images=[TripadvisorImage(
            index=1,
            original_url="https://media-cdn.tripadvisor.com/media/photo-o/1a/2b/3c/x.jpg",
            thumbnail_url="https://media-cdn.tripadvisor.com/media/photo-s/1a/2b/3c/x.jpg",
            downloaded=True,
            local_path="data/raw/tripadvisor/images/nui-ngu-hanh-son-abc12345/01.jpg",
            bytes_original=94_631, bytes_compressed=10_337, width=1600, height=1067)],
        reviews=[TripadvisorReviewItem(index=1, key_review="Tuyệt vời", detail_review="Cảnh đẹp,  nên đi  sớm"),
                 TripadvisorReviewItem(index=2, key_review="", detail_review="Đông vào cuối tuần")],
    )
    assert rec.review_count == 2 and rec.images_downloaded == 1
    assert rec.key_review == "Tuyệt vời"
    assert rec.detail_review == "Cảnh đẹp, nên đi sớm\n\nĐông vào cuối tuần"
    assert rec.local_image_paths == ["data/raw/tripadvisor/images/nui-ngu-hanh-son-abc12345/01.jpg"]
    assert rec.images[0].compression_ratio == 0.1092
    assert rec.status == "ok" and json.loads(rec.to_jsonl())["review_count"] == 2


def test_rating_validation():
    import pytest
    with pytest.raises(Exception):
        TripadvisorCommentRecord(record_id="ta_x", source_url="https://x", rating=9.0)
