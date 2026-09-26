"""
Raw schema (Bronze) — nguồn duy nhất cho data contract phase ingestion.

Cấu trúc mới đầy đủ nhất cho TripAdvisor comment crawler.

Luồng dữ liệu:
    registry (bước 1) → source_url (2) → rating (3) → gallery (4,5)
    → ảnh nén local (6,7) → tab Reviews (8) → reviewCard (9,10) → JSON (11)
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

CRAWLER_VERSION = "1.0.0"
CrawlStatus = Literal["ok", "partial", "blocked", "error"]


class TripadvisorImage(BaseModel):
    """1 ảnh trong gallery (bước 5 + 6 + 7)."""

    model_config = ConfigDict(str_strip_whitespace=True)

    index: int = Field(..., ge=1, description="Vị trí trong gallery (1-based)")
    original_url: str = Field("", description="URL bản gốc (photo-o) dùng để tải")
    thumbnail_url: str = Field("", description="URL thô lấy từ div[data-testid=tile_gallery] img")
    downloaded: bool = Field(False, description="True nếu đã lưu file .jpg local")
    local_path: Optional[str] = Field(
        None, description="Path tương đối từ repo root, vd data/raw/tripadvisor/images/<slug>-d<id>/01.jpg"
    )
    bytes_original: Optional[int] = Field(None, ge=0, description="Bytes trước khi nén")
    bytes_compressed: Optional[int] = Field(None, ge=0, description="Bytes sau khi nén")
    width: Optional[int] = Field(None, ge=1, description="Chiều rộng sau nén")
    height: Optional[int] = Field(None, ge=1, description="Chiều cao sau nén")

    @property
    def compression_ratio(self) -> Optional[float]:
        """bytes_compressed / bytes_original — dùng để log/kiểm tra bước 6."""
        if not self.bytes_original or self.bytes_compressed is None:
            return None
        return round(self.bytes_compressed / self.bytes_original, 4)


class TripadvisorReviewItem(BaseModel):
    """1 review trong trang địa điểm (bước 9 + 10)."""

    model_config = ConfigDict(str_strip_whitespace=True)

    index: int = Field(..., ge=1, description="Thứ tự review trên trang (1-based)")
    key_review: str = Field("", description="Tiêu đề review — h3")
    detail_review: str = Field("", description="Nội dung review — div.fIrGe span")

    @field_validator("key_review", "detail_review")
    @classmethod
    def normalize_text(cls, value: str) -> str:
        """Gộp mọi khoảng trắng/xuống dòng về 1 space."""
        return " ".join((value or "").split())


class TripadvisorCommentRecord(BaseModel):
    """Record raw/bronze: 1 entity TripAdvisor + rating + ảnh + review."""

    model_config = ConfigDict(str_strip_whitespace=True)

    # ---------- bước 1 + 2: định danh & entity ----------
    record_id: str = Field(..., min_length=1, description="Số thứ tự đánh từ 1 theo vị trí trong file JSON (stt trong file)")
    entity_name: str = Field("", description="name trong registry")
    entity_type: str = Field("", description="entity_type trong registry")
    province: str = Field("", description="province trong registry")
    rank: Optional[int] = Field(None, ge=1, description="rank trong registry")
    source_url: str = Field("", description="URL trang Attraction_Review đã truy cập")
    source: str = Field("tripadvisor", description="Nguồn dữ liệu (hằng số)")

    # ---------- bước 3 ----------
    rating: Optional[float] = Field(None, description="Rating tổng của địa điểm, thang 1..5")

    # ---------- bước 5 + 6 + 7 ----------
    images: List[TripadvisorImage] = Field(default_factory=list, description="Chi tiết từng ảnh")
    original_image_urls: List[str] = Field(default_factory=list, description="Toàn bộ URL ảnh gallery")
    local_image_paths: List[str] = Field(default_factory=list, description="File .jpg đã nén trên disk")
    images_downloaded: int = Field(0, ge=0, description="Số ảnh tải thành công (≤ max_images)")

    # ---------- bước 9 + 10 ----------
    key_review: str = Field("", description="Tiêu đề review đầu tiên (h3)")
    detail_review: str = Field("", description="Nối nội dung review bằng \n\n (2 newline)")
    reviews: List[TripadvisorReviewItem] = Field(default_factory=list, description="Tối đa 10 review")
    review_count: int = Field(0, ge=0, description="len(reviews)")

    # ---------- vận hành / audit ----------
    status: CrawlStatus = Field("ok", description="ok | partial | blocked | error")
    error: str = Field("", description="Thông báo lỗi hoặc selector thiếu")
    missing_fields: List[str] = Field(default_factory=list, description="Field không lấy được, vd ['rating','detail_review']")
    crawler_version: str = Field(CRAWLER_VERSION, description="Version crawler tạo record")
    scraped_at: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(timespec="seconds"),
        description="Thời điểm crawl, ISO-8601 UTC",
    )

    # ---------- validators ----------
    @field_validator("rating")
    @classmethod
    def rating_must_be_valid(cls, value: Optional[float]) -> Optional[float]:
        if value is None:
            return None
        if not 1.0 <= value <= 5.0:
            raise ValueError("rating phải nằm trong khoảng 1..5")
        return value

    @field_validator("scraped_at")
    @classmethod
    def scraped_at_must_be_iso(cls, value: str) -> str:
        try:
            datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except (ValueError, AttributeError) as exc:
            raise ValueError("scraped_at phải là ISO-8601") from exc
        return value

    @model_validator(mode="after")
    def sync_derived_fields(self) -> "TripadvisorCommentRecord":
        """Đồng bộ mọi field dẫn xuất để JSON không bao giờ tự mâu thuẫn."""
        self.review_count = len(self.reviews)
        self.images_downloaded = sum(1 for img in self.images if img.downloaded)
        self.local_image_paths = [img.local_path for img in self.images if img.local_path]
        self.original_image_urls = self.original_image_urls or [
            img.original_url for img in self.images if img.original_url
        ]
        if not self.key_review and self.reviews:
            self.key_review = self.reviews[0].key_review
        if not self.detail_review and self.reviews:
            self.detail_review = "\n\n".join(r.detail_review for r in self.reviews if r.detail_review)
        if self.status == "ok" and not self.reviews:
            self.status = "partial"
        return self

    # ---------- serialize ----------
    def to_record(self) -> dict:
        """dict đúng thứ tự field (ghi JSON array)."""
        return self.model_dump()

    def to_jsonl(self) -> str:
        """1 dòng JSONL cho các consumer dạng dòng (giữ tương thích bước 11)."""
        return json.dumps(self.to_record(), ensure_ascii=False)


__all__ = ["CRAWLER_VERSION", "CrawlStatus", "TripadvisorImage",
           "TripadvisorReviewItem", "TripadvisorCommentRecord"]
