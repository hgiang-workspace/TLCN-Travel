"""Adapters cho từng nguồn dữ liệu.

Mỗi adapter chuyển đổi format thô (raw JSON) sang Comment dataclass chuẩn.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Comment:
    """Định dạng comment chuẩn sau adapter."""

    source: str              # "tripadvisor" | "tiktok" | "foody"
    comment_id: str          # ID duy nhất của comment (trong source)
    place_id: str            # ID địa điểm (để join ngược)
    place_name: str          # Tên địa điểm
    raw_text: str            # Nội dung gốc (chưa normalize)
    rating: float | None     # Rating 1-5 nếu có
    collected_at: str        # ISO timestamp thu thập
    meta: dict[str, Any]     # Các trường bổ sung (author, url, etc.)


def extract_comments_tripadvisor(raw: dict[str, Any]) -> list[Comment]:
    """Trích xuất comment từ file tripadvisor_comments.json.

    Cấu trúc file thực tế (từ data/raw/tripadvisor/tripadvisor_comments.json):
    - Top-level: list of place records
    - Mỗi place: record_id, entity_name, province, rating, reviews (list), source_url, source
    - Mỗi review: index, key_review, detail_review
    """
    result: list[Comment] = []

    if not isinstance(raw, list):
        return result

    for place in raw:
        if not isinstance(place, dict):
            continue

        place_id = str(place.get("record_id", ""))
        place_name = str(place.get("entity_name", ""))
        place_rating = place.get("rating")
        try:
            place_rating = float(place_rating) if place_rating is not None else None
        except (ValueError, TypeError):
            place_rating = None

        reviews = place.get("reviews", [])
        if not isinstance(reviews, list):
            continue

        for rev in reviews:
            if not isinstance(rev, dict):
                continue
            # Ưu tiên detail_review, fallback key_review
            content = rev.get("detail_review") or rev.get("key_review") or ""
            if not content or not isinstance(content, str):
                continue

            review_index = rev.get("index")
            review_id = str(review_index) if review_index is not None else f"ta_{place_id}_{len(result)}"

            result.append(Comment(
                source="tripadvisor",
                comment_id=review_id,
                place_id=place_id,
                place_name=place_name,
                raw_text=content,
                rating=place_rating,  # Rating ở cấp place
                collected_at="",  # Không có timestamp review
                meta={
                    "province": place.get("province"),
                    "source_url": place.get("source_url"),
                    "source": place.get("source"),
                    "review_key": rev.get("key_review"),
                },
            ))
    return result


def extract_comments_tiktok(raw: dict[str, Any]) -> list[Comment]:
    """Trích xuất comment từ file tiktok raw (khi crawler chạy xong).

    Cấu trúc mong đợi (từ crawler.py):
    - Top-level: list of place/video records
    - Mỗi item: aweme_id, desc, statistics, comments (list)
    - Mỗi comment: cid, text, create_time, user, digg_count...
    """
    result: list[Comment] = []

    if not isinstance(raw, list):
        return result

    for item in raw:
        if not isinstance(item, dict):
            continue

        aweme_id = str(item.get("aweme_id", ""))
        place_name = str(item.get("desc", "") or item.get("title", "") or f"tiktok_{aweme_id}")
        comments = item.get("comments", [])
        if not isinstance(comments, list):
            continue

        for cmt in comments:
            if not isinstance(cmt, dict):
                continue
            text = cmt.get("text") or cmt.get("content") or ""
            if not text or not isinstance(text, str):
                continue

            cid = str(cmt.get("cid", cmt.get("comment_id", "")))
            create_time = cmt.get("create_time", cmt.get("createTime", ""))

            result.append(Comment(
                source="tiktok",
                comment_id=cid or f"tt_{aweme_id}_{len(result)}",
                place_id=aweme_id,
                place_name=place_name,
                raw_text=text,
                rating=None,  # TikTok không có rating
                collected_at=str(create_time),
                meta={
                    "author_id": cmt.get("user", {}).get("uid") if isinstance(cmt.get("user"), dict) else None,
                    "author_name": cmt.get("user", {}).get("nickname") if isinstance(cmt.get("user"), dict) else None,
                    "digg_count": cmt.get("digg_count"),
                    "reply_count": cmt.get("reply_comment_count"),
                },
            ))
    return result


ADAPTERS = {
    "tripadvisor": extract_comments_tripadvisor,
    "tiktok": extract_comments_tiktok,
}


def get_adapter(source: str):
    """Lấy function adapter theo source."""
    if source not in ADAPTERS:
        raise ValueError(f"Không có adapter cho source: {source}. Hỗ trợ: {list(ADAPTERS.keys())}")
    return ADAPTERS[source]


__all__ = ["Comment", "extract_comments_tripadvisor", "extract_comments_tiktok", "get_adapter", "ADAPTERS"]