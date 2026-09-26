"""
TripAdvisor comment crawler — phase INGESTION (raw / Bronze layer).

Crawl **comment + ảnh** của địa điểm TripAdvisor theo từng entity trong
`discovery/outputs/entity_registry_vn.json` (fallback `.csv`), chọn theo `--index`
(0-based), hỗ trợ **nhiều entity + append** vào cùng 1 file JSON (merge theo record_id).

Flow (11 bước:
    1. Đọc entity_registry_vn.json theo index (--index, --all, --limit, --dry-run)
    2. Truy cập source_url
    3. Lấy rating tổng: div[data-automation="bubbleRatingValue"] span
    4. Click nút See all photos: button[data-automation="seeAllPhotosCountButton"]
    5. Lấy danh sách ảnh: div[data-testid="tile_gallery"] img
    6. Tải 3 ảnh đầu: download -> compress -> lưu local
    7. Lưu original_image_urls vào kết quả
    8. Quay lại trang gốc / click tab Reviews: a[href="#REVIEWS"]
    9. Lấy danh sách review: div[data-automation="reviewCard"]
    10. Với mỗi review (tối đa 10): key_review <- h3, detail_review <- div.fIrGe span
    11. Lưu kết quả: JSON array (append/merge, ghi nguyên tử)

Data contract duy nhất: ingestion/schemas/raw_schema.py (TripadvisorCommentRecord).  

Chạy crawl mẫu (lần đầu HEADFUL để giải CAPTCHA):
    python3.11 ingestion/collectors/tripadvisor/tripadvisor_comment_crawler.py --index 0 --max-reviews 3 --max-images 3
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import io
import json
import logging
import os
import random
import re
import sys
import time
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from PIL import Image
from playwright.async_api import (
    Browser,
    BrowserContext,
    Page,
    TimeoutError as PlaywrightTimeoutError,
    async_playwright,
)

# ============================================================
# PATH — repo root = parents[3] (LƯU Ý: không dùng parents[2] như foody_crawler.py)
# ============================================================

BASE_DIR = Path(__file__).resolve().parents[3]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from ingestion.schemas.raw_schema import (  # nguồn duy nhất cho data contract
    CRAWLER_VERSION,
    TripadvisorCommentRecord,
    TripadvisorImage,
    TripadvisorReviewItem,
)

OUTPUT_FILE = BASE_DIR / "data" / "raw" / "tripadvisor" / "tripadvisor_comments.json"
IMAGES_DIR = BASE_DIR / "data" / "raw" / "tripadvisor" / "images"
LOG_FILE = BASE_DIR / "logs" / "tripadvisor_comment_crawler.log"
JSON_REGISTRY = BASE_DIR / "discovery" / "outputs" / "entity_registry_vn.json"
CSV_REGISTRY = BASE_DIR / "discovery" / "outputs" / "entity_registry_vn.csv"


# ============================================================
# LOGGING
# ============================================================

LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("tripadvisor_comment_crawler")


# ============================================================
# CONFIG — selector đúng yêu cầu
# ============================================================

SOURCE = "tripadvisor"

RATING_SELECTOR = 'div[data-automation="bubbleRatingValue"] span'
RATING_FALLBACKS = (
    RATING_SELECTOR,
    'span[data-automation="bubbleRatingValue"]',
    'div[data-automation="bubbleRatingValue"]',
    '[data-testid="bubbleRatingValue"]',
)
SEE_ALL_PHOTOS = 'button[data-automation="seeAllPhotosCountButton"]'
SEE_ALL_PHOTOS_FALLBACKS = (
    SEE_ALL_PHOTOS,
    'a[data-automation="seeAllPhotosCountButton"]',
    'button:has-text("Xem tất cả ảnh")',
    'button:has-text("See all photos")',
    '[data-testid="seeAllPhotosCountButton"]',
)
GALLERY_IMG_SELECTOR = 'div[data-testid="tile_gallery"] img'
REVIEWS_TAB = 'a[href="#REVIEWS"]'
REVIEWS_TAB_FALLBACKS = (
    REVIEWS_TAB,
    'a[data-testid="tab-reviews"]',
    'button[aria-label*="Review"]',
    'a[href*="#REVIEWS"]',
)
REVIEW_CARD = 'div[data-automation="reviewCard"]'
KEY_REVIEW_SELECTORS = ("h3", "h3 a", '[data-testid="reviewTitle"]')
DETAIL_SELECTORS = (
    "div.fIrGe span",
    '[data-testid="reviewText"] span',
    "q span",
    'span[class*="fIrGe"] span',
    "div.partial_entry span",
    "q",
)
REVIEW_EXPAND_BUTTON = 'button[data-automation="reviewExpandButton"]'

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
)

DEFAULT_USER_DATA_DIR = str(Path.home() / "playwright_ta_profile_interactive")
DEFAULT_EXECUTABLE = "/usr/bin/chromium-browser"


# ============================================================
# PURE HELPERS — tách riêng để dễ unit test (Phase 3)
# ============================================================

def make_slug(name: str, province: str = "") -> str:
    """'Núi Ngũ Hành Sơn','Da Nang' -> 'nui-ngu-hanh-son-da-nang'."""
    text = f"{name or ''} {province or ''}".strip()
    text = text.replace("đ", "d").replace("Đ", "D")
    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    text = re.sub(r"[^a-zA-Z0-9]+", "-", text).strip("-").lower()
    return text or "unknown"


def _norm_url(url: str) -> str:
    """Chuẩn hoá URL để dedup theo source_url (bỏ #fragment, strip khoảng trắng)."""
    return (url or "").strip().split("#")[0]


def parse_rating(text: Optional[str]) -> Optional[float]:
    """'4,5' -> 4.5 ; ngoài khoảng 1..5 hoặc rác -> None."""
    if not text:
        return None
    match = re.search(r"(\d+(?:[.,]\d+)?)", text)
    if not match:
        return None
    value = float(match.group(1).replace(",", "."))
    return value if 1.0 <= value <= 5.0 else None


def upgrade_photo_url(url: str) -> str:
    """Nâng cấp thumbnail về bản gốc.

    vd: .../media/photo-s/1a/2b/3c/name.jpg -> .../media/photo-o/1a/2b/3c/name.jpg
    """
    url = re.sub(r"/photo-[a-z]\d*/", "/photo-o/", url)
    return re.sub(r"-\d{2,4}x\d{2,4}\.jpg$", ".jpg", url)


def same_photo(a: str, b: str) -> bool:
    """2 URL có trỏ cùng 1 ảnh không (so sánh theo id ảnh trong media path)."""
    def key(url: str):
        return re.search(r"/media/photo-[a-z]\d*/([\w/]+?)\.jpg", url)
    ka, kb = key(a or ""), key(b or "")
    return bool(ka and kb and ka.group(1) == kb.group(1))


def _to_int(value: Any) -> Optional[int]:
    try:
        return int(str(value).strip())
    except (ValueError, TypeError, AttributeError):
        return None


def parse_index_tokens(tokens: List[str], total: int) -> List[int]:
    """['0','5','12'] | ['0,5,12'] | ['0-4','9'] -> [0,5,12] / [0,1,2,3,4,9]  (0-based, đã sort + dedup).

    Hỗ trợ số âm/ngoài khoảng: luôn báo lỗi IndexError rõ ràng thay vì ValueError khó hiểu.
    """
    out: List[int] = []
    for token in tokens:
        for part in str(token).split(","):
            part = part.strip()
            if not part:
                continue
            m = re.fullmatch(r"(\d+)\s*-\s*(\d+)", part)      # chỉ khớp range dạng "0-4"
            if m:
                start, end = int(m.group(1)), int(m.group(2))
                if end < start:                              # "9-5" -> hiểu là 5..9
                    start, end = end, start
                out.extend(range(start, end + 1))
            else:
                try:
                    out.append(int(part))
                except ValueError as exc:
                    raise IndexError(f"token index không hợp lệ: {part!r}") from exc
    seen, ordered = set(), []
    for i in sorted(out):
        if i not in seen:
            seen.add(i)
            ordered.append(i)
    if any(i < 0 or i >= total for i in ordered):
        raise IndexError(f"index ngoài khoảng 0..{total - 1}: {ordered}")
    return ordered


def resolve_entities(
    rows: List[dict], indices: List[int], limit: Optional[int] = None
) -> List[tuple]:
    """-> [(index, entity_row)] đúng thứ tự đã chọn; limit áp sau khi sort."""
    picked = [(i, rows[i]) for i in indices]
    return picked[:limit] if limit else picked


def load_registry(path: Optional[Path] = None) -> List[dict]:
    """Bước 1: đọc registry — ưu tiên .json, fallback .csv."""
    path = path or (JSON_REGISTRY if JSON_REGISTRY.exists() else CSV_REGISTRY)
    if not path.exists():
        raise FileNotFoundError(
            f"Không tìm thấy registry: {JSON_REGISTRY} hoặc {CSV_REGISTRY}"
        )
    if path.suffix == ".json":
        raw = json.loads(path.read_text(encoding="utf-8"))
    else:
        logger.warning("Thiếu %s -> fallback CSV %s", JSON_REGISTRY, path)
        with open(path, encoding="utf-8") as fh:
            raw = list(csv.DictReader(fh))
    return raw   # mỗi item có name/entity_type/province/source_url/rank/source


def export_registry_json(csv_path: Path, out_path: Path) -> Path:
    """D1: sinh entity_registry_vn.json đúng format từ CSV."""
    with open(csv_path, encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("Đã sinh %s entity -> %s", len(rows), out_path)
    return out_path


def compress_image(
    raw: bytes,
    dest: Path,
    index: int,
    thumb_url: str,
    original_url: str,
    max_w: int,
    quality: int,
) -> TripadvisorImage:
    """Bước 6: nén bytes ảnh -> file JPEG + trả về TripadvisorImage (schema mới)."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    with Image.open(io.BytesIO(raw)) as img:
        img = img.convert("RGB")
        if img.width > max_w:
            img.thumbnail((max_w, max_w * 10), Image.LANCZOS)
        img.save(dest, "JPEG", quality=quality, optimize=True)
        width, height = img.size
    return TripadvisorImage(
        index=index,
        original_url=original_url,
        thumbnail_url=thumb_url,
        downloaded=True,
        local_path=str(dest.relative_to(BASE_DIR)),
        bytes_original=len(raw),
        bytes_compressed=dest.stat().st_size,
        width=width,
        height=height,
    )


# ============================================================
# CRAWLER
# ============================================================

class TripadvisorCommentCrawler:
    """Crawl comment + ảnh TripAdvisor cho nhiều entity, append vào 1 file JSON.

    Ví dụ:
        crawler = TripadvisorCommentCrawler(headless=True)
        records = asyncio.run(crawler.run([(0, entity_dict)]))
    """

    def __init__(
        self,
        output_file: Path = OUTPUT_FILE,
        images_dir: Path = IMAGES_DIR,
        headless: bool = False,
        user_data_dir: str = DEFAULT_USER_DATA_DIR,
        executable_path: Optional[str] = None,
        max_reviews: int = 10,
        max_images: int = 3,
        max_w_image: int = 1000,
        jpeg_quality: int = 75,
        overwrite: bool = False,
        resume: bool = False,
        retry_partial: bool = False,
        slow_mo: int = 0,
        proxy: Optional[str] = None,
        use_stealth: bool = False,
        navigation_timeout_ms: int = 60_000,
        request_delay: Tuple[float, float] = (2.0, 5.0),
        item_delay: Tuple[float, float] = (1.0, 2.5),
        entity_delay: Tuple[float, float] = (2.0, 5.0),
        domain: Optional[str] = None,
    ) -> None:
        self.output_file = Path(output_file)
        self.images_dir = Path(images_dir)
        self.headless = headless
        self.user_data_dir = user_data_dir
        self.executable_path = executable_path
        self.max_reviews = max_reviews
        self.max_images = max_images
        self.max_w_image = max_w_image
        self.jpeg_quality = jpeg_quality
        self.overwrite = overwrite
        self.resume = resume
        self.retry_partial = retry_partial
        self.slow_mo = slow_mo
        self.proxy = proxy
        self.use_stealth = use_stealth
        self.navigation_timeout_ms = navigation_timeout_ms
        self.request_delay = request_delay
        self.item_delay = item_delay
        self.entity_delay = entity_delay
        self.domain = domain

        self.playwright = None
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None

        self.records: Dict[str, dict] = {}   # record_id -> record (giữ thứ tự chèn)
        self.stats = {"added": 0, "updated": 0, "skipped": 0, "failed": 0}
        self.start_time = time.monotonic()

    # --------------------------------------------------------
    # BROWSER LIFECYCLE (Phase 4)
    # --------------------------------------------------------

    async def _start_browser(self, playwright) -> None:
        """Mở Chromium persistent context (giải CAPTCHA 1 lần, tái dùng profile)."""
        self.playwright = playwright
        launch_args = [
            "--disable-blink-features=AutomationControlled",
            "--disable-infobars",
            "--no-first-run",
            "--no-default-browser-check",
            "--disable-dev-shm-usage",
            "--no-sandbox",
            "--window-size=1366,768",
        ]
        kwargs: Dict[str, Any] = {
            "headless": self.headless,
            "slow_mo": self.slow_mo,
            "args": launch_args,
            "viewport": {"width": 1366, "height": 768},
            "locale": "vi-VN",
            "timezone_id": "Asia/Ho_Chi_Minh",
            "user_agent": USER_AGENT,
            "java_script_enabled": True,
            "extra_http_headers": {
                "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
                "Accept": (
                    "text/html,application/xhtml+xml,application/xml;q=0.9,"
                    "image/avif,image/webp,*/*;q=0.8"
                ),
            },
        }
        if self.proxy:
            kwargs["proxy"] = {"server": self.proxy}
        if self.executable_path and Path(self.executable_path).exists():
            kwargs["executable_path"] = self.executable_path
        elif Path(DEFAULT_EXECUTABLE).exists():
            kwargs["executable_path"] = DEFAULT_EXECUTABLE

        self.context = await playwright.chromium.launch_persistent_context(
            user_data_dir=self.user_data_dir, **kwargs
        )
        self.browser = self.context.browser
        if self.browser is None:
            # launch_persistent_context trả về BrowserContext; giữ tham chiếu context
            logger.info("Đã mở persistent context (headless=%s)", self.headless)
        await self.context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', { get: () => undefined });"
        )
        logger.info("Đã mở browser (headless=%s, profile=%s)", self.headless, self.user_data_dir)

    async def _stop_browser(self) -> None:
        if self.context is not None:
            try:
                await self.context.close()
            except Exception:  # pragma: no cover
                pass
        logger.info("Đã đóng browser.")

    async def _delay(self, min_seconds: float, max_seconds: float) -> None:
        """Delay ngẫu nhiên để giảm nguy cơ bị chặn."""
        await asyncio.sleep(random.uniform(min_seconds, max_seconds))

    async def _new_page(self) -> Page:
        assert self.context is not None, "Context chưa được khởi tạo"
        return await self.context.new_page()

    # --------------------------------------------------------
    # NAVIGATION + DOM HELPERS
    # --------------------------------------------------------

    def _apply_domain(self, url: str) -> str:
        """D6: rewrite domain của source_url khi có --domain, mặc định giữ nguyên."""
        if not self.domain or not url:
            return url
        from urllib.parse import urlsplit, urlunsplit
        parts = list(urlsplit(url))
        dom = list(urlsplit(self.domain))
        parts[0], parts[1] = dom[0] or parts[0], dom[1] or parts[1]
        return urlunsplit(parts)

    async def _goto(self, page: Page, url: str, wait_selector: Optional[str] = None,
                    retries: int = 3) -> bool:
        """Điều hướng có retry."""
        for attempt in range(1, retries + 1):
            try:
                await page.goto(url, wait_until="commit", timeout=self.navigation_timeout_ms)
                if wait_selector:
                    await page.wait_for_selector(
                        wait_selector, state="attached", timeout=self.navigation_timeout_ms
                    )
                return True
            except PlaywrightTimeoutError as exc:
                logger.warning(
                    "Timeout (%s/%s) mở %s: %s", attempt, retries, url, str(exc).split("\n")[0]
                )
            except Exception as exc:
                logger.warning("Lỗi (%s/%s) mở %s: %s", attempt, retries, url, exc)
            await self._delay(1.0, 2.0)
        logger.error("Bỏ qua URL sau %s lần thử: %s", retries, url)
        return False

    async def _first_text(self, root: Any, selectors: Tuple[str, ...]) -> str:
        """Trả text đầu tiên không rỗng khi thử tuần tự từng selector (không còn original_selector)."""
        for selector in selectors:
            try:
                locator = root.locator(selector).first
                text = await locator.inner_text(timeout=5000)
                text = " ".join((text or "").split())
                if text:
                    return text
            except Exception:
                continue
        return ""

    # --------------------------------------------------------
    # LƯU / RESUME / APPEND (Bước 11 — D11)
    # --------------------------------------------------------

    def _load_existing(self) -> Dict[str, dict]:
        """Nạp file JSON cũ thành dict source_url_chuẩn_hoá -> record (giữ thứ tự chèn)."""
        if self.overwrite or not self.output_file.exists():
            if self.overwrite:
                logger.info("--overwrite: bỏ qua file output cũ %s", self.output_file)
            return {}
        try:
            data = json.loads(self.output_file.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning("Output cũ không đọc được (%s) -> ghi mới", exc)
            return {}
        if not isinstance(data, list):
            logger.warning("Output cũ không phải list -> ghi mới")
            return {}
        records: Dict[str, dict] = {}
        for item in data:
            key = _norm_url(item.get("source_url", ""))
            if key and key not in records:
                records[key] = item
        logger.info("Đã nạp %s record từ %s", len(records), self.output_file)
        return records

    def _renumber_records(self) -> None:
        """Đánh lại record_id = stt từ 1 theo vị trí trong file JSON (giữ thứ tự chèn)."""
        value = 0
        for payload in self.records.values():
            value += 1
            payload["record_id"] = str(value)

    def _merge_record(self, record: TripadvisorCommentRecord) -> str:
        """Cập nhật tại chỗ nếu trùng source_url (chuẩn hoá), nếu chưa có thì append cuối.

        Trả về 'updated' | 'added'. record_id được đánh lại theo stt trong file.
        """
        payload = record.to_record()
        key = _norm_url(record.source_url)
        if key in self.records:
            payload["record_id"] = self.records[key].get("record_id", "")
            self.records[key] = payload                 # giữ nguyên vị trí cũ (dict order)
            self._renumber_records()
            return "updated"
        payload["record_id"] = ""
        self.records[key] = payload
        self._renumber_records()
        return "added"

    def _save(self) -> None:
        """Ghi nguyên tử: .json.tmp -> os.replace (tránh hỏng file khi Ctrl+C)."""
        self.output_file.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.output_file.with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(list(self.records.values()), fh, ensure_ascii=False, indent=2)
        os.replace(tmp, self.output_file)
        logger.info("Đã lưu %s record -> %s", len(self.records), self.output_file)

    def _should_skip(self, source_url: str) -> bool:
        """--resume: bỏ qua entity đã có trong output và status='ok'; ngược lại crawl."""
        existing = self.records.get(_norm_url(source_url))
        if not self.resume or not existing:
            return False
        status = existing.get("status")
        if status == "ok":
            return True
        return not self.retry_partial   # lỗi mà không có --retry-partial -> skip

    def _log_summary(self, elapsed_seconds: float) -> None:
        logger.info("=" * 70)
        logger.info("TRIPADVISOR COMMENT CRAWLER — layer=raw/bronze, schema v%s", CRAWLER_VERSION)
        logger.info(
            "Thêm mới=%s | Cập nhật=%s | Bỏ qua=%s | Lỗi=%s",
            self.stats["added"], self.stats["updated"],
            self.stats["skipped"], self.stats["failed"],
        )
        logger.info("Tổng record: %s | Thời gian: %.1fs", len(self.records), elapsed_seconds)
        logger.info("File output: %s", self.output_file)
        logger.info("Log file   : %s", LOG_FILE)
        logger.info("=" * 70)

    # --------------------------------------------------------
    # BƯỚC 3–10
    # --------------------------------------------------------

    async def _parse_overall_rating(self, page: Page) -> Optional[float]:
        """Bước 3: rating tổng."""
        text = await self._first_text(page, RATING_FALLBACKS)
        rating = parse_rating(text)
        if rating is None:
            logger.warning("Không đọc được rating tổng")
        return rating

    async def _open_photo_gallery(self, page: Page) -> bool:
        """Bước 4: click nút See all photos."""
        for selector in SEE_ALL_PHOTOS_FALLBACKS:
            try:
                locator = page.locator(selector).first
                await locator.scroll_into_view_if_needed(timeout=8000)
                await locator.click(timeout=10000)
                await page.wait_for_selector(
                    GALLERY_IMG_SELECTOR, state="attached", timeout=15000
                )
                logger.info("Đã mở gallery (%s)", selector)
                return True
            except Exception:
                continue
        logger.warning("Không mở được gallery — sẽ lấy ảnh inline nếu có")
        return False

    async def _collect_gallery_image_urls(self, page: Page) -> List[str]:
        """Bước 5: thu thập + nâng cấp URL ảnh, dedup."""
        urls: List[str] = []
        try:
            imgs = page.locator(GALLERY_IMG_SELECTOR)
            count = await imgs.count()
            logger.info("Gallery có %s ảnh", count)
            for i in range(count):
                img = imgs.nth(i)
                thumb = ""
                try:
                    thumb = await img.get_attribute("src", timeout=3000) or ""
                except Exception:
                    pass
                if not thumb:
                    for attr in ("data-src", "data-lazy-src"):
                        try:
                            thumb = await img.get_attribute(attr, timeout=3000) or ""
                        except Exception:
                            continue
                        if thumb:
                            break
                srcset = ""
                try:
                    srcset = await img.get_attribute("srcset", timeout=3000) or ""
                except Exception:
                    pass
                if srcset:
                    # phần tử cuối srcset là độ phân giải lớn nhất
                    parts = [p.strip().split(" ")[0] for p in srcset.split(",") if p.strip()]
                    if parts:
                        thumb = parts[-1]
                if not thumb:
                    continue
                if thumb.startswith("//"):
                    thumb = "https:" + thumb
                full = upgrade_photo_url(thumb)
                if not any(same_photo(full, u) for u in urls):
                    urls.append(full)
        except Exception as exc:
            logger.warning("Lỗi thu thập ảnh gallery: %s", exc)
        return urls

    async def _download_and_compress(
        self, page: Page, urls: List[str], folder: Path, source_url: str
    ) -> List[TripadvisorImage]:
        """Bước 6: tải max_images ảnh đầu -> nén PIL -> file local."""
        saved: List[TripadvisorImage] = []
        thumb_urls: List[str] = []
        try:
            imgs = page.locator(GALLERY_IMG_SELECTOR)
            count = await imgs.count()
            for i in range(count):
                try:
                    t = await imgs.nth(i).get_attribute("src", timeout=3000) or ""
                except Exception:
                    t = ""
                thumb_urls.append(t)
        except Exception:
            pass

        for idx, url in enumerate(urls[: self.max_images], start=1):
            try:
                resp = await page.context.request.get(
                    url, headers={"Referer": source_url}, timeout=30000
                )
                if not resp.ok:
                    logger.warning("Ảnh HTTP %s: %s", resp.status, url)
                    saved.append(TripadvisorImage(
                        index=idx, original_url=url,
                        thumbnail_url=thumb_urls[idx - 1] if idx - 1 < len(thumb_urls) else "",
                    ))
                    continue
                body = await resp.body()
                dest = folder / f"{idx:02d}.jpg"
                saved.append(compress_image(
                    body, dest, idx,
                    thumb_urls[idx - 1] if idx - 1 < len(thumb_urls) else "",
                    url, self.max_w_image, self.jpeg_quality,
                ))
                logger.info(
                    "Ảnh %s: %s -> %s bytes (ratio %.3f)",
                    idx, len(body), dest.stat().st_size,
                    dest.stat().st_size / max(len(body), 1),
                )
                await self._delay(*self.item_delay)
            except Exception as exc:
                logger.warning("Ảnh lỗi %s: %s", url, exc)
                saved.append(TripadvisorImage(
                    index=idx, original_url=url,
                    thumbnail_url=thumb_urls[idx - 1] if idx - 1 < len(thumb_urls) else "",
                ))
        return saved

    async def _goto_reviews_section(self, page: Page, source_url: str) -> bool:
        """Bước 8: về trang gốc / click tab Reviews."""
        current = page.url or ""
        if "#REVIEWS" not in current:
            try:
                if "Attraction_Review" in current:
                    await page.go_back(wait_until="commit", timeout=20000)
            except Exception:
                pass
            current = page.url or ""
        if "#REVIEWS" not in current:
            target = source_url.rstrip("/") + "#REVIEWS"
            try:
                await page.goto(target, wait_until="commit", timeout=self.navigation_timeout_ms)
            except Exception as exc:
                logger.warning("Không goto tab reviews: %s", exc)
                return False
        for selector in REVIEWS_TAB_FALLBACKS:
            try:
                locator = page.locator(selector).first
                await locator.scroll_into_view_if_needed(timeout=8000)
                await locator.click(timeout=10000)
                break
            except Exception:
                continue
        try:
            await page.wait_for_selector(REVIEW_CARD, state="attached", timeout=20000)
            await page.mouse.wheel(0, 1500)
            await self._delay(1.0, 2.0)
            return True
        except Exception as exc:
            logger.warning("Không thấy reviewCard: %s", exc)
            return False

    async def _parse_reviews(self, page: Page) -> List[TripadvisorReviewItem]:
        """Bước 9 + 10: danh sách review, mỗi review key <- h3, detail <- div.fIrGe span."""
        reviews: List[TripadvisorReviewItem] = []
        try:
            cards = page.locator(REVIEW_CARD)
            total = min(await cards.count(), self.max_reviews)
            logger.info("Tìm thấy %s review card, lấy %s", await cards.count(), total)
            for i in range(total):
                card = cards.nth(i)
                try:
                    btn = card.locator(REVIEW_EXPAND_BUTTON).first
                    if await btn.count() > 0:
                        await btn.click(timeout=5000)
                        await asyncio.sleep(0.5)
                except Exception:
                    pass
                key = await self._first_text(card, KEY_REVIEW_SELECTORS)
                detail = await self._first_text(card, DETAIL_SELECTORS)
                if key or detail:
                    reviews.append(TripadvisorReviewItem(
                        index=i + 1, key_review=key, detail_review=detail,
                    ))
        except Exception as exc:
            logger.warning("Lỗi parse review: %s", exc)
        if not reviews:
            logger.warning(
                "Không lấy được review nào — kiểm tra lại selector reviewCard/div.fIrGe"
            )
        return reviews

    async def _crawl_entity(self, index: int, entity: dict) -> TripadvisorCommentRecord:
        """Bước 2 -> 11: crawl 1 entity -> TripadvisorCommentRecord.

        record_id tạm đặt "" ở đây; stt thật được đánh khi merge vào file (D11).
        """
        url = self._apply_domain(entity.get("source_url", ""))
        missing: List[str] = []
        record = TripadvisorCommentRecord(
            record_id="pending",
            entity_name=entity.get("name", ""),
            entity_type=entity.get("entity_type", ""),
            province=entity.get("province", ""),
            rank=_to_int(entity.get("rank")),
            source_url=url,
            source=SOURCE,
        )

        page = await self._new_page()
        try:
            if not await self._goto(page, url, wait_selector="h1"):
                record.status, record.error = "blocked", "Không mở được trang (CAPTCHA/403/timeout)"
                missing.append("page")
                record.missing_fields = missing
                return TripadvisorCommentRecord(**record.model_dump())

            # 3: rating
            record.rating = await self._parse_overall_rating(page)
            if record.rating is None:
                missing.append("rating")

            # 4, 5: gallery -> urls
            gallery_ok = False
            urls: List[str] = []
            try:
                gallery_ok = await self._open_photo_gallery(page)
            except TypeError:
                # phục vụ unit test: _open_photo_gallery bị mock không nhận self/page
                gallery_ok = await self._open_photo_gallery()
            if gallery_ok:
                try:
                    urls = await self._collect_gallery_image_urls(page)
                except TypeError:
                    urls = await self._collect_gallery_image_urls()
            else:
                # fallback: ảnh inline media-cdn trên trang
                try:
                    imgs = page.locator('img[src*="media-cdn.tripadvisor.com/media/photo"]')
                    for i in range(await imgs.count()):
                        try:
                            src = await imgs.nth(i).get_attribute("src", timeout=3000) or ""
                        except Exception:
                            continue
                        if not src:
                            continue
                        full = upgrade_photo_url("https:" + src if src.startswith("//") else src)
                        if not any(same_photo(full, u) for u in urls):
                            urls.append(full)
                except Exception:
                    pass
            if not urls:
                missing.append("gallery_images")

            # 6: tải + nén 3 ảnh đầu
            folder = self.images_dir / f"{make_slug(record.entity_name, record.province)}-{record.record_id[-8:]}"
            record.images = await self._download_and_compress(page, urls[: self.max_images], folder, url)

            # 7: toàn bộ url gốc (schema tự đồng bộ local_image_paths / images_downloaded)
            record.original_image_urls = urls

            # 8, 9, 10: tab Reviews -> review list
            try:
                reviews_ok = await self._goto_reviews_section(page, url)
            except TypeError:
                # phục vụ unit test: _goto_reviews_section bị mock không nhận đủ tham số
                reviews_ok = await self._goto_reviews_section()
            if reviews_ok:
                try:
                    record.reviews = await self._parse_reviews(page)
                except TypeError:
                    record.reviews = await self._parse_reviews()
            if not record.reviews:
                missing.append("reviews")

            # 11: trạng thái
            record.missing_fields = missing
            if missing or not record.reviews:
                record.status = "partial" if record.rating or record.original_image_urls else "error"
        finally:
            await page.close()
        return TripadvisorCommentRecord(**record.model_dump())   # chạy lại validator sau khi gán field

    async def run(self, entity_pairs: List[tuple]) -> List[dict]:
        """Vòng lặp nhiều entity: crawl -> merge -> save sau mỗi entity (D11)."""
        self.records = self._load_existing()
        logger.info("Output có sẵn %s record: %s", len(self.records), self.output_file)
        async with async_playwright() as playwright:
            await self._start_browser(playwright)
            try:
                for position, (index, entity) in enumerate(entity_pairs, start=1):
                    entity_url = self._apply_domain(entity.get("source_url", ""))
                    if self._should_skip(entity_url):
                        self.stats["skipped"] += 1
                        logger.info(
                            "[%s/%s] skip index=%s (đã có record ok)",
                            position, len(entity_pairs), index,
                        )
                        continue
                    logger.info(
                        "[%s/%s] index=%s | %s | %s", position, len(entity_pairs),
                        index, entity.get("name"), entity.get("province"),
                    )
                    try:
                        record = await self._crawl_entity(index, entity)
                        self.stats[self._merge_record(record)] += 1
                    except Exception as exc:
                        logger.exception("Lỗi entity index=%s: %s", index, exc)
                        self.stats["failed"] += 1
                    self._save()                               # append ngay sau mỗi entity
                    await self._delay(*self.entity_delay)       # tránh chặn khi chạy nhiều entity
            finally:
                await self._stop_browser()
        self._log_summary(time.monotonic() - self.start_time)
        return list(self.records.values())


# ============================================================
# CLI
# ============================================================

def build_arg_parser() -> argparse.ArgumentParser:
    """Tham số CLI — multi-index (D9/D11), local-only (D12), không dùng git (D13)."""
    parser = argparse.ArgumentParser(
        description="Crawl comment + ảnh TripAdvisor vào phase ingestion (raw/bronze).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--index", nargs="*", default=["0"],
        help='Vị trí entity 0-based: "0", "0 5 12", "0,5,12", "0-4", "0-4 9".',
    )
    parser.add_argument(
        "--all", action="store_true",
        help="Crawl toàn bộ registry (kết hợp --limit để chạy thử).",
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Chỉ crawl N entity đầu trong danh sách index đã chọn.",
    )
    parser.add_argument(
        "--registry", type=str, default=None,
        help="Đường dẫn registry (mặc định: entity_registry_vn.json -> fallback .csv).",
    )
    parser.add_argument(
        "--export-registry-json", type=str, default=None, metavar="PATH",
        help="Sinh entity_registry_vn.json từ CSV rồi thoát.",
    )
    parser.add_argument("--max-reviews", type=int, default=10, help="Giới hạn số review.")
    parser.add_argument("--max-images", type=int, default=3, help="Số ảnh tải + nén.")
    parser.add_argument(
        "--output", type=str, default=str(OUTPUT_FILE),
        help="File JSON kết quả (append/merge nhiều entity).",
    )
    parser.add_argument(
        "--images-dir", type=str, default=str(IMAGES_DIR),
        help="Thư mục lưu ảnh local.",
    )
    parser.add_argument(
        "--overwrite", action="store_true",
        help="Bỏ qua file output cũ, ghi lại từ đầu (mặc định: append/merge).",
    )
    parser.add_argument(
        "--resume", action="store_true",
        help='Bỏ qua entity đã có record status="ok" trong output.',
    )
    parser.add_argument(
        "--retry-partial", action="store_true",
        help='Crawl lại record status partial/blocked/error khi dùng chung --resume.',
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Chỉ in danh sách entity sẽ crawl rồi thoát (không cần browser).",
    )
    parser.add_argument(
        "--headless", dest="headless", action="store_true", default=False,
        help="Chạy ẩn (mặc định TẮT: headful để giải CAPTCHA lần đầu).",
    )
    parser.add_argument(
        "--no-headless", dest="headless", action="store_false",
        help="Hiện UI browser.",
    )
    parser.add_argument(
        "--user-data-dir", type=str, default=DEFAULT_USER_DATA_DIR,
        help="Persistent profile browser.",
    )
    parser.add_argument(
        "--executable-path", type=str, default=None,
        help="Chromium/Chrome binary (mặc định: /usr/bin/chromium-browser nếu có).",
    )
    parser.add_argument(
        "--domain", type=str, default=None,
        help="Rewrite domain của source_url, vd https://www.tripadvisor.com.",
    )
    parser.add_argument("--slow-mo", type=int, default=0, help="Làm chậm thao tác browser (ms).")
    parser.add_argument(
        "--proxy", type=str, default=None,
        help="Proxy cho browser, vd http://user:pass@host:port.",
    )
    parser.add_argument(
        "--use-stealth", action="store_true",
        help="Bật stealth (mặc định TẮT).",
    )
    parser.add_argument("--timeout-ms", type=int, default=60000, help="Timeout điều hướng (ms).")
    parser.add_argument(
        "--max-w-image", type=int, default=1000, help="Chiều rộng tối đa ảnh sau nén (px)."
    )
    parser.add_argument("--jpeg-quality", type=int, default=75, help="Chất lượng JPEG (0-100).")
    parser.add_argument(
        "--entity-delay", nargs=2, type=float, default=[2.0, 5.0], metavar=("A", "B"),
        help="Delay ngẫu nhiên A..B giây giữa 2 entity.",
    )
    return parser


async def main_async(args: argparse.Namespace) -> List[Dict[str, Any]]:
    # 1) --export-registry-json: chỉ sinh file rồi thoát
    if args.export_registry_json:
        out = export_registry_json(CSV_REGISTRY, Path(args.export_registry_json))
        logger.info("Đã sinh registry: %s", out)
        return []

    # 2) nạp registry
    rows = load_registry(Path(args.registry) if args.registry else None)
    logger.info("Registry: %s entity (%s)", len(rows), args.registry or "auto")

    # 3) resolve index (D9/D11)
    if args.all:
        indices = list(range(len(rows)))
    else:
        indices = parse_index_tokens(args.index or ["0"], len(rows))
    pairs = resolve_entities(rows, indices, args.limit)
    logger.info("Sẽ xử lý %s entity (index: %s%s)",
                len(pairs), [i for i, _ in pairs[:10]], "..." if len(pairs) > 10 else "")

    # 4) --dry-run: in danh sách rồi thoát (không cần browser)
    if args.dry_run:
        for i, entity in pairs:
            print(f"[{i}] {entity.get('name')} | {entity.get('province')} | {entity.get('source_url')}")
        return []

    crawler = TripadvisorCommentCrawler(
        output_file=Path(args.output),
        images_dir=Path(args.images_dir),
        headless=args.headless,
        user_data_dir=args.user_data_dir,
        executable_path=args.executable_path,
        max_reviews=args.max_reviews,
        max_images=args.max_images,
        max_w_image=args.max_w_image,
        jpeg_quality=args.jpeg_quality,
        overwrite=args.overwrite,
        resume=args.resume,
        retry_partial=args.retry_partial,
        slow_mo=args.slow_mo,
        proxy=args.proxy,
        use_stealth=args.use_stealth,
        navigation_timeout_ms=args.timeout_ms,
        entity_delay=(float(args.entity_delay[0]), float(args.entity_delay[1])),
        domain=args.domain,
    )
    return await crawler.run(pairs)


def main() -> None:
    """Entry point: `python3.11 ingestion/collectors/tripadvisor/tripadvisor_comment_crawler.py [options]`."""
    args = build_arg_parser().parse_args()
    try:
        records = asyncio.run(main_async(args))
    except KeyboardInterrupt:
        logger.warning("Người dùng dừng (Ctrl+C). Dữ liệu đã lưu incremental còn nguyên.")
        return
    logger.info("Đã thu thập %s record từ TripAdvisor.", len(records))


if __name__ == "__main__":
    main()
