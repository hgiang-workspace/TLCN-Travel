"""
Foody.vn crawler — phase INGESTION (raw / Bronze layer).

Crawl danh sách **quán ăn** (`entity_type = "quan an"`) theo từng tỉnh/thành và lưu ra JSON.

Flow (bám đúng yêu cầu):
    1. Truy cập https://www.foody.vn
    2. Click element chọn tỉnh/thành `#head-province .rn-nav-name`
       (div có `ng-click="Show()"`) để mở danh sách tỉnh/thành.
    3. Lần lượt click từng thẻ `#popupLocation a[data-id]`
       (ví dụ `<a data-id="217" href="/ho-chi-minh"><label>TP. HCM</label></a>`).
    4. Sau khi chọn mỗi tỉnh, thêm `/food/quan-an` vào URL hiện tại rồi truy cập
       (`/ho-chi-minh` -> `/ho-chi-minh/food/quan-an`).
    5. Ở trang danh sách: duyệt mọi `div.row-item.filter-result-item`, lấy link chi tiết từ
       `a[data-bind="attr: { href: DetailUrl, title: Name }"]`
       (fallback: `a.ri-avatar`, `.resname h2 a` — link server-render sẵn).
    6. Ở trang chi tiết: lấy `h1` trong `.main-information.disableSection` và toàn bộ
       rating trong `.microsite-points-summary`
       (Chất lượng / Giá cả / Phục vụ / Không gian / Vị trí, điểm trung bình, số bình luận).
    7. Trong cùng trang chi tiết: lấy từ khối `.disableSection`:
       địa chỉ (`streetAddress` + `addressLocality` + `addressRegion`),
       giờ mở/đóng cửa, giá thấp nhất — cao nhất (`priceRange`).
    8. Click `a.linkmap` (Bản đồ) để mở bản đồ, rồi lấy lat/lng từ element Google Maps:
       URL ảnh có dạng `!4m2!1x{lat_e7}!2x{lng_e7}` -> chia cho `10_000_000`.

Output: `data/raw/foody/foody_quan_an.json` (đổi bằng `--output`).
Log:    `logs/foody_crawler.log`.

Chạy test nhanh:
    python3.11 ingestion/foody/foody_crawler.py --max-provinces 1 --max-items 2

Chạy đầy đủ:
    python3.11 ingestion/foody/foody_crawler.py --provinces "TP. HCM,Hà Nội" --max-items 50
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import random
import re
import time
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlsplit

from playwright.async_api import (
    Browser,
    BrowserContext,
    Page,
    TimeoutError as PlaywrightTimeoutError,
    async_playwright,
)
from pydantic import BaseModel, Field

try:  # playwright-stealth là optional: thiếu cũng vẫn crawl được
    from playwright_stealth import stealth_async
except ImportError:  # pragma: no cover
    stealth_async = None


# ============================================================
# PATH — theo convention của repo (xem ingestion/transport/vexere_crawler.py)
# ============================================================

BASE_DIR = Path(__file__).resolve().parents[2]
OUTPUT_FILE = BASE_DIR / "data" / "raw" / "foody" / "foody_quan_an.json"
LOG_FILE = BASE_DIR / "logs" / "foody_crawler.log"


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
logger = logging.getLogger("foody_crawler")


# ============================================================
# CONFIG
# ============================================================

SOURCE = "foody"
ENTITY_TYPE = "quan an"
BASE_URL = "https://www.foody.vn"
LISTING_PATH = "food/quan-an"          # bước 4: đường dẫn category "Quán ăn"

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
)

# Bước 2 + 3: selector dropdown tỉnh/thành (AngularJS render sau khi click)
PROVINCE_TOGGLE_SELECTOR = "#head-province .rn-nav-name"
PROVINCE_POPUP_SELECTOR = "#popupLocation"
PROVINCE_ANCHOR_SELECTOR = "#popupLocation a[data-id]"

# Bước 5: item danh sách + các selector link chi tiết (ưu tiên theo thứ tự)
LIST_ITEM_SELECTOR = "div.row-item.filter-result-item"
DETAIL_LINK_SELECTORS = (
    'a[data-bind*="DetailUrl"]',   # đúng yêu cầu: a[data-bind="attr: { href: DetailUrl ...}"]
    "a.ri-avatar",                 # link server-render sẵn trong item
    ".resname h2 a",               # fallback cuối (có thể là trang thương hiệu -> sẽ bị lọc)
)

# Bước 6: tên + rating
NAME_SELECTORS = (
    ".main-information.disableSection h1",
    ".main-information h1",
    "h1[itemprop='name']",
    "h1",
)
RATING_SUMMARY_SELECTOR = ".microsite-points-summary"
RATING_GROUP_SELECTOR = ".microsite-top-points"
RATING_AVG_SELECTOR = ".microsite-point-avg"
REVIEW_COUNT_SELECTOR = ".microsite-review-count"

# Map label (đã bỏ dấu) -> field trong schema
RATING_LABEL_MAP: Dict[str, Tuple[str, ...]] = {
    "rating_quality": ("chat luong",),
    "rating_price": ("gia ca", "gia"),
    "rating_service": ("phuc vu",),
    "rating_space": ("khong gian",),
    "rating_location": ("vi tri",),
}
# Fallback theo thứ tự DOM quan sát được khi label bị thiếu/đổi chữ
RATING_FALLBACK_ORDER = (
    "rating_quality",
    "rating_price",
    "rating_location",
    "rating_service",
    "rating_space",
)

# Bước 7: địa chỉ / giờ / giá
ADDRESS_SCOPES = (".disableSection", ".microsite-res-info")
HOURS_SELECTORS = (
    ".micro-timesopen",
    ".microsite-res-info .new-detail-info-area",
    ".res-common-price",
)
PRICE_SELECTOR = "span[itemprop='priceRange']"
PRICE_FALLBACK_SELECTOR = ".res-common-minmaxprice"

# Bước 8: bản đồ
MAP_LINK_SELECTORS = ("a.linkmap:has-text('Bản đồ')", "a.linkmap")

# JS kiểm tra AngularJS đã compile xong dropdown tỉnh/thành chưa
# (popup render bằng ng-repeat + AJAX nên href ban đầu còn là "{{item.Url}}")
ANGULAR_READY_JS = (
    "() => !!window.angular && !!document.querySelector('#head-province.ng-scope')"
)
PROVINCE_ANCHORS_READY_JS = """() => {
    const anchors = Array.from(document.querySelectorAll('#popupLocation a[data-id]'));
    return anchors.filter(a => {
        const href = a.getAttribute('href') || '';
        return href.startsWith('/') && !href.includes('{{');
    }).length;
}"""

# Regex dùng chung
SESSION_PREFIX_RE = re.compile(r"^/\(S\([^)]*\)\)")            # bỏ session-id trong URL foody
TIME_RANGE_RE = re.compile(r"(\d{1,2}:\d{2})\s*[-–—]\s*(\d{1,2}:\d{2})")
PRICE_TOKEN_RE = re.compile(r"(\d[\d.,]*)\s*(k|nghìn|ngàn|triệu|tr)?", re.IGNORECASE)
MAP_IMG_COORD_RE = re.compile(r"!4m2!1x(-?\d+)!2x(-?\d+)")      # toạ độ * 10_000_000
MAP_EMBED_COORD_RE = re.compile(r"[?&]q=(-?\d+\.\d+),(-?\d+\.\d+)")
URL_AT_COORD_RE = re.compile(r"@(-?\d+\.\d+),(-?\d+\.\d+)")

# Bounding box Việt Nam — dùng để cảnh báo toạ độ bất thường
VN_BBOX = {"lat_min": 8.0, "lat_max": 24.0, "lng_min": 102.0, "lng_max": 110.5}

# Path bị loại khỏi kết quả vì không phải trang chi tiết 1 quán ăn
EXCLUDED_LINK_PARTS = ("/thuong-hieu/", "/nearBy", "/binh-luan", "/hinh-anh", "/menu")


# ============================================================
# DATA MODEL — đúng data contract đã chốt (21 field, giữ nguyên thứ tự)
# ============================================================

class FoodyEntity(BaseModel):
    """1 quán ăn crawl từ Foody.vn."""

    name: str = Field("", description="Tên quán ăn (thẻ h1 trang chi tiết)")
    entity_type: str = Field(ENTITY_TYPE, description='Cố định "quan an"')
    province: str = Field("", description="Tỉnh/thành chọn ở dropdown")
    address: str = Field("", description="Số nhà + đường + phường (streetAddress)")
    district: str = Field("", description="Quận/huyện (addressLocality)")
    city: str = Field("", description="Tỉnh/thành theo microdata (addressRegion)")

    rating_avg: Optional[float] = Field(None, description="Điểm trung bình (thang 10)")
    rating_quality: Optional[float] = Field(None, description="Điểm Chất lượng")
    rating_price: Optional[float] = Field(None, description="Điểm Giá cả")
    rating_service: Optional[float] = Field(None, description="Điểm Phục vụ")
    rating_space: Optional[float] = Field(None, description="Điểm Không gian")
    rating_location: Optional[float] = Field(None, description="Điểm Vị trí")
    review_count: Optional[int] = Field(None, description="Số lượng bình luận")

    open_time: str = Field("", description="Giờ mở cửa HH:MM")
    close_time: str = Field("", description="Giờ đóng cửa HH:MM")
    price_min: Optional[int] = Field(None, description="Giá thấp nhất (VND)")
    price_max: Optional[int] = Field(None, description="Giá cao nhất (VND)")

    lat: Optional[float] = Field(None, description="Vĩ độ")
    lng: Optional[float] = Field(None, description="Kinh độ")

    source_url: str = Field("", description="URL trang chi tiết")
    source: str = Field(SOURCE, description='Nguồn dữ liệu: "foody"')

    def to_record(self) -> Dict[str, Any]:
        """Dict đúng thứ tự field của contract để ghi JSON."""
        return self.model_dump()



# ============================================================
# PURE HELPERS — tách riêng để dễ unit test
# ============================================================

def strip_accents(text: str) -> str:
    """'Chất lượng' -> 'chat luong' (phục vụ so khớp label rating)."""
    if not text:
        return ""
    normalized = unicodedata.normalize("NFD", text.replace("đ", "d").replace("Đ", "D"))
    return "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn").lower().strip()


def normalize_whitespace(text: str) -> str:
    """Gộp mọi khoảng trắng/xuống dòng về 1 space."""
    return re.sub(r"\s+", " ", (text or "")).strip()


def normalize_path(href: Optional[str]) -> str:
    """Chuẩn hoá href: bỏ session-id Foody `/(S(xxx))/`, đảm bảo bắt đầu bằng '/'."""
    href = (href or "").strip()
    if not href:
        return ""
    href = SESSION_PREFIX_RE.sub("", href)
    if href.startswith("//"):
        href = "/" + href.lstrip("/")
    if not href.startswith("/"):
        href = "/" + href
    return href


def province_slug_of(url_or_path: str) -> str:
    """Lấy slug tỉnh/thành từ URL/path: '/ho-chi-minh/food/quan-an' -> 'ho-chi-minh'."""
    path = normalize_path(urlsplit(url_or_path).path)
    parts = [p for p in path.split("/") if p]
    return parts[0] if parts else ""


def to_float(value: Any) -> Optional[float]:
    """'7.5' -> 7.5; '_._' / '' / None -> None."""
    if value is None:
        return None
    text = str(value).strip().replace(",", ".")
    if not text:
        return None
    match = re.search(r"-?\d+(?:\.\d+)?", text)
    if not match:
        return None
    try:
        return float(match.group(0))
    except ValueError:
        return None


def to_int(value: Any) -> Optional[int]:
    """'1,459' -> 1459; '12.345' -> 12345."""
    if value is None:
        return None
    digits = re.sub(r"[^\d]", "", str(value))
    if not digits:
        return None
    try:
        return int(digits)
    except ValueError:
        return None


def parse_opening_hours(raw: str) -> Tuple[str, str]:
    """'Đang mở cửa 09:30 - 20:30' -> ('09:30', '20:30').

    Lưu ý: nếu quán có nhiều khung giờ trong ngày, chỉ lấy khung ĐẦU TIÊN hiển thị.
    """
    match = TIME_RANGE_RE.search(raw or "")
    if not match:
        return "", ""
    return match.group(1), match.group(2)


def parse_price_range(raw: str) -> Tuple[Optional[int], Optional[int]]:
    """'50.000đ - 110.000đ' -> (50000, 110000); '25k - 50k' -> (25000, 50000)."""
    if not raw:
        return None, None

    prices: List[int] = []
    for number, unit in PRICE_TOKEN_RE.findall(raw):
        cleaned = number.replace(".", "").replace(",", "").replace(" ", "")
        if not cleaned.isdigit():
            continue
        value = int(cleaned)
        unit_key = (unit or "").lower()
        if unit_key in ("k", "nghìn", "ngàn"):
            value *= 1_000
        elif unit_key in ("triệu", "tr"):
            value *= 1_000_000
        if value > 0:
            prices.append(value)

    if not prices:
        return None, None
    if len(prices) == 1:
        return prices[0], None
    return min(prices), max(prices)


def parse_address_text(raw: str) -> Dict[str, str]:
    """Fallback: '736 Sư Vạn Hạnh, P. 12, Quận 10, TP. HCM'
    -> {'address': '736 Sư Vạn Hạnh, P. 12', 'district': 'Quận 10', 'city': 'TP. HCM'}."""
    parts = [normalize_whitespace(p) for p in (raw or "").split(",") if normalize_whitespace(p)]
    if len(parts) >= 3:
        return {"address": ", ".join(parts[:-2]), "district": parts[-2], "city": parts[-1]}
    if len(parts) == 2:
        return {"address": parts[0], "district": parts[1], "city": ""}
    if len(parts) == 1:
        return {"address": parts[0], "district": "", "city": ""}
    return {"address": "", "district": "", "city": ""}


def is_vn_coordinate(lat: Optional[float], lng: Optional[float]) -> bool:
    """Kiểm tra toạ độ nằm trong bounding box Việt Nam (chỉ dùng để cảnh báo)."""
    if lat is None or lng is None:
        return False
    return (
        VN_BBOX["lat_min"] <= lat <= VN_BBOX["lat_max"]
        and VN_BBOX["lng_min"] <= lng <= VN_BBOX["lng_max"]
    )




# ============================================================
# CRAWLER
# ============================================================

class FoodyCrawler:
    """Quản lý toàn bộ flow crawl Foody.vn bằng Playwright (async).

    Ví dụ:
        crawler = FoodyCrawler(headless=True, max_provinces=1, max_items=5)
        records = asyncio.run(crawler.run())
    """

    def __init__(
        self,
        output_file: Path = OUTPUT_FILE,
        headless: bool = False,
        max_provinces: Optional[int] = None,
        max_items: Optional[int] = None,
        max_pages: int = 0,
        provinces: Optional[List[str]] = None,
        resume: bool = False,
        slow_mo: int = 0,
        navigation_timeout_ms: int = 60_000,
        map_wait_timeout_ms: int = 8_000,
        request_delay: Tuple[float, float] = (1.5, 3.5),
        item_delay: Tuple[float, float] = (1.0, 2.5),
        proxy: Optional[str] = None,
        use_stealth: bool = False,
    ) -> None:
        self.output_file = Path(output_file)
        self.headless = headless
        self.max_provinces = max_provinces     # giới hạn số tỉnh (test trước)
        self.max_items = max_items             # giới hạn số quán / tỉnh (test trước)
        self.max_pages = max_pages             # 0 = không giới hạn (crawl hết)
        self.province_filter = [p.strip() for p in (provinces or []) if p.strip()]
        self.resume = resume
        self.slow_mo = slow_mo
        self.navigation_timeout_ms = navigation_timeout_ms
        self.map_wait_timeout_ms = map_wait_timeout_ms
        self.request_delay = request_delay
        self.item_delay = item_delay
        self.proxy = proxy
        self.use_stealth = use_stealth

        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None

        self.records: List[FoodyEntity] = []
        self.seen_urls: set = set()
        self.stats = {
            "provinces_done": 0,
            "items_found": 0,
            "items_saved": 0,
            "items_failed": 0,
            "items_skipped": 0,
            "coord_sources": {},
        }

    # --------------------------------------------------------
    # BROWSER LIFECYCLE
    # --------------------------------------------------------

    async def _start_browser(self, playwright) -> None:
        """Mở Chromium + context giả lập người dùng thật (giảm nguy cơ bị chặn)."""
        launch_args = [
            "--disable-blink-features=AutomationControlled",
            "--disable-infobars",
            "--no-first-run",
            "--no-default-browser-check",
            "--disable-dev-shm-usage",
            "--no-sandbox",
            "--window-size=1366,768",
        ]

        self.browser = await playwright.chromium.launch(
            headless=self.headless,
            slow_mo=self.slow_mo,
            args=launch_args,
            proxy={"server": self.proxy} if self.proxy else None,
            ignore_default_args=["--enable-automation"],
        )

        self.context = await self.browser.new_context(
            viewport={"width": 1366, "height": 768},
            locale="vi-VN",
            timezone_id="Asia/Ho_Chi_Minh",
            user_agent=USER_AGENT,
            java_script_enabled=True,
            extra_http_headers={
                "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
                "Accept": (
                    "text/html,application/xhtml+xml,application/xml;q=0.9,"
                    "image/avif,image/webp,*/*;q=0.8"
                ),
            },
        )
        self.context.set_default_timeout(self.navigation_timeout_ms)

        # Ẩn dấu hiệu automation
        await self.context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', { get: () => undefined });"
        )
        logger.info("Đã mở Chromium (headless=%s)", self.headless)

    async def _stop_browser(self) -> None:
        if self.context is not None:
            try:
                await self.context.close()
            except Exception:  # pragma: no cover
                pass
        if self.browser is not None:
            try:
                await self.browser.close()
            except Exception:  # pragma: no cover
                pass
        logger.info("Đã đóng Chromium.")

    async def _delay(self, min_seconds: float, max_seconds: float) -> None:
        """Delay ngẫu nhiên để giảm nguy cơ bị chặn (rate limit)."""
        await asyncio.sleep(random.uniform(min_seconds, max_seconds))

    async def _new_page(self) -> Page:
        """Tạo tab mới (tuỳ chọn áp stealth nếu `--use-stealth`).

        Lưu ý quan trọng: trên foody.vn, `playwright_stealth` làm hỏng bootstrap
        jQuery/AngularJS (`JSERR: utils is not defined`, `window.jQuery === false`)
        nên dropdown tỉnh/thành không bao giờ render. Vì vậy mặc định TẮT stealth;
        chỉ bật khi thực sự cần và phải kiểm tra lại dropdown.
        """
        assert self.context is not None, "Context chưa được khởi tạo"
        page = await self.context.new_page()
        if self.use_stealth and stealth_async is not None:
            try:
                await stealth_async(page)
                logger.warning("Đã bật playwright_stealth (có thể làm hỏng AngularJS của foody).")
            except Exception as exc:  # pragma: no cover
                logger.debug("stealth_async bỏ qua: %s", exc)
        return page

    # --------------------------------------------------------
    # NAVIGATION + DOM HELPERS
    # --------------------------------------------------------

    async def _goto(
        self,
        page: Page,
        url: str,
        wait_selector: Optional[str] = None,
        retries: int = 3,
    ) -> bool:
        """Điều hướng có retry.

        Dùng `wait_until="commit"` vì foody.vn giữ long-polling nên
        `domcontentloaded`/`load` rất dễ timeout (đã kiểm chứng thực tế).
        """
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

    async def _text(self, root: Any, selector: str, default: str = "") -> str:
        """Lấy text an toàn (root = Page hoặc Locator)."""
        try:
            locator = root.locator(selector).first
            if await locator.count() == 0:
                return default
            return normalize_whitespace(await locator.inner_text(timeout=5_000))
        except Exception:
            return default

    async def _attr(self, root: Any, selector: str, name: str, default: str = "") -> str:
        """Lấy attribute an toàn (dùng cho meta[itemprop=...] -> đọc `content`)."""
        try:
            locator = root.locator(selector).first
            if await locator.count() == 0:
                return default
            return normalize_whitespace(await locator.get_attribute(name) or default)
        except Exception:
            return default

    async def _first_text(self, root: Any, selectors: Tuple[str, ...]) -> str:
        """Thử lần lượt nhiều selector, trả về text đầu tiên lấy được."""
        for selector in selectors:
            value = await self._text(root, selector)
            if value:
                return value
        return ""


    # --------------------------------------------------------
    # BƯỚC 1 + 2: TRANG CHỦ & DROPDOWN TỈNH/THÀNH
    # --------------------------------------------------------

    async def _open_home(self, page: Page) -> bool:
        """Bước 1: truy cập https://www.foody.vn (+ chờ AngularJS khởi tạo LocationCtrl)."""
        ok = await self._goto(page, BASE_URL, wait_selector=PROVINCE_TOGGLE_SELECTOR, retries=4)
        if not ok:
            return False
        await self._wait_for_angular(page)
        await self._delay(1.0, 2.0)
        return True

    async def _wait_for_angular(self, page: Page, timeout_ms: int = 30000) -> bool:
        """Chờ AngularJS compile xong `#head-province`.

        Rất quan trọng: nếu click quá sớm thì `ng-click="Show()"` chưa được gắn
        và dropdown sẽ không mở (href vẫn là template `{{item.Url}}`).
        """
        try:
            await page.wait_for_function(ANGULAR_READY_JS, timeout=timeout_ms)
            return True
        except Exception as exc:
            logger.warning("AngularJS chưa sẵn sàng: %s", str(exc).split("\n")[0])
            return False

    async def _compiled_province_anchors(self, page: Page) -> int:
        """Số thẻ tỉnh/thành đã render thật (href đã hết `{{ }})`."""
        try:
            return int(await page.evaluate(PROVINCE_ANCHORS_READY_JS))
        except Exception:
            return 0

    async def _force_open_popup(self, page: Page) -> bool:
        """Fallback: gọi trực tiếp `Show()` trên Angular scope của `#head-province`."""
        try:
            opened = await page.evaluate(
                """() => {
                    if (!window.angular) return false;
                    const el = document.getElementById('head-province');
                    if (!el) return false;
                    const scope = window.angular.element(el).scope();
                    if (!scope || typeof scope.Show !== 'function') return false;
                    scope.Show();
                    scope.$apply();
                    return true;
                }"""
            )
            return bool(opened)
        except Exception:
            return False

    async def _open_province_popup(self, page: Page, attempts: int = 4) -> bool:
        """Bước 2: click `#head-province .rn-nav-name` (div `ng-click="Show()"`) để mở danh sách.

        Có retry vì popup được render bất đồng bộ (AJAX `GetPopupLocation` + ng-repeat).
        """
        for attempt in range(1, attempts + 1):
            if await self._compiled_province_anchors(page) > 0:
                return True

            toggle = page.locator(PROVINCE_TOGGLE_SELECTOR).first
            if await toggle.count() == 0:
                logger.error("Không tìm thấy nút chọn tỉnh/thành: %s", PROVINCE_TOGGLE_SELECTOR)
                return False

            try:
                await toggle.click(timeout=15_000)
            except Exception as exc:
                logger.debug("Click nút tỉnh/thành lỗi (lần %s): %s", attempt, exc)
                await self._force_open_popup(page)

            try:
                await page.wait_for_function(PROVINCE_ANCHORS_READY_JS, timeout=15_000)
                await self._delay(0.5, 1.2)  # chờ render đủ danh sách
                return True
            except Exception:
                logger.warning("Dropdown tỉnh/thành chưa render (lần %s/%s)", attempt, attempts)
                # Popup có thể chưa mở được do Angular chưa sẵn sàng -> thử lại
                await self._force_open_popup(page)
                await self._wait_for_angular(page, timeout_ms=10_000)
                await self._delay(1.0, 2.0)

        logger.error("Không mở được dropdown tỉnh/thành sau %s lần thử.", attempts)
        return False

    async def _load_provinces(self, page: Page) -> List[Dict[str, str]]:
        """Bước 2 + 3: mở dropdown và đọc toàn bộ thẻ tỉnh/thành (`a[data-id]`).

        Trả về list: {"id", "name", "path", "slug"} — dedup theo slug.
        """
        if not await self._open_home(page):
            return []
        if not await self._open_province_popup(page):
            return []

        anchors = page.locator(PROVINCE_ANCHOR_SELECTOR)
        total = await anchors.count()
        logger.info("Dropdown tỉnh/thành: tìm thấy %s thẻ <a data-id>", total)

        provinces: List[Dict[str, str]] = []
        seen_slugs = set()
        started_collecting = False  # Bắt đầu thu thập từ Hải Dương (/hai-duong)

        for index in range(total):
            anchor = anchors.nth(index)
            href = normalize_path(await anchor.get_attribute("href"))
            if not href or href == "/":
                continue

            slug = province_slug_of(href)
            if not slug or slug in seen_slugs:
                continue

            label = await self._text(anchor, "label") or slug
            data_id = (await anchor.get_attribute("data-id")) or ""

            # Bỏ qua các tỉnh trước Hải Dương
            if not started_collecting:
                if slug == "binh-dinh":
                    started_collecting = True
                    logger.info("Đã đến Bình Định/. (%s) -> bắt đầu thu thập tỉnh/thành", slug)
                else:
                    logger.debug("Bỏ qua tỉnh/thành trước Hải Dương: %s (%s)", label, slug)
                    continue

            # Dừng lại khi gặp Thailand (data-id="76") hoặc Bangkok (/bangkok)
            if data_id == "76" and "thailand" in label.lower():
                logger.info("Gặp Thailand (data-id=76) -> dừng lấy tỉnh/thành")
                break
            if slug == "bangkok":
                logger.info("Gặp Bangkok (/bangkok) -> dừng lấy tỉnh/thành")
                break

            seen_slugs.add(slug)
            provinces.append(
                {
                    "id": data_id,
                    "name": label,
                    "path": f"/{slug}",
                    "slug": slug,
                }
            )

        logger.info("Đọc được %s tỉnh/thành (sau dedup)", len(provinces))
        return self._apply_province_filter(provinces)

    def _apply_province_filter(self, provinces: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """Lọc theo `--provinces` (khớp tên không dấu hoặc slug) + cắt theo `--max-provinces`."""
        if self.province_filter:
            wanted = {strip_accents(p.replace("-", " ")) for p in self.province_filter}
            provinces = [
                p
                for p in provinces
                if strip_accents(p["name"]) in wanted
                or strip_accents(p["slug"].replace("-", " ")) in wanted
            ]
            logger.info("Lọc theo --provinces -> còn %s tỉnh/thành", len(provinces))

        if self.max_provinces:
            provinces = provinces[: self.max_provinces]
            logger.info(
                "Giới hạn --max-provinces=%s -> %s tỉnh/thành", self.max_provinces, len(provinces)
            )

        return provinces

    # --------------------------------------------------------
    # BƯỚC 3 + 4: CHỌN TỈNH -> GHÉP /food/quan-an -> TRUY CẬP
    # --------------------------------------------------------

    async def _select_province(self, page: Page, province: Dict[str, str]) -> bool:
        """Bước 3: click đúng thẻ tỉnh/thành trong dropdown."""
        if not await self._open_home(page):
            return False
        if not await self._open_province_popup(page):
            return False

        # Ưu tiên khớp theo href (đúng thẻ <a data-id href="/ho-chi-minh">)
        anchor = page.locator(f'{PROVINCE_POPUP_SELECTOR} a[href="{province["path"]}"]').first
        if await anchor.count() == 0 and province.get("id"):
            anchor = page.locator(f'{PROVINCE_POPUP_SELECTOR} a[data-id="{province["id"]}"]').first
        if await anchor.count() == 0:
            anchor = page.locator(
                f'{PROVINCE_POPUP_SELECTOR} a:has(label:text-is("{province["name"]}"))'
            ).first
        if await anchor.count() == 0:
            logger.error("Không click được thẻ tỉnh/thành: %s", province["name"])
            return False

        try:
            await anchor.scroll_into_view_if_needed(timeout=10_000)
            await anchor.click(timeout=15_000)
            await page.wait_for_load_state("commit", timeout=self.navigation_timeout_ms)
        except Exception as exc:
            logger.warning("Click tỉnh %s lỗi (vẫn thử đi tiếp): %s", province["name"], exc)

        await self._delay(0.8, 1.8)
        logger.info("Bước 3 OK — đã chọn tỉnh/thành: %s (%s)", province["name"], province["path"])
        return True

    def _build_listing_url(self, current_url: str, page_no: int = 1) -> str:
        """Bước 4: thêm `/food/quan-an` vào URL tỉnh hiện tại.

        '/ho-chi-minh' -> 'https://www.foody.vn/ho-chi-minh/food/quan-an'
        (page_no > 1 -> thêm '?page=N' để phân trang danh sách quán ăn).
        """
        slug = province_slug_of(current_url)
        url = urljoin(BASE_URL, f"{slug}/{LISTING_PATH}")
        if page_no > 1:
            url = f"{url}?page={page_no}"
        return url

    # --------------------------------------------------------
    # BƯỚC 5: TRANG DANH SÁCH -> LINK CHI TIẾT
    # --------------------------------------------------------

    async def _detail_href(self, item: Any) -> str:
        """Lấy href chi tiết trong 1 item danh sách.

        Ưu tiên `a[data-bind*="DetailUrl"]` (đúng yêu cầu), fallback sang link
        server-render sẵn trong item. Bỏ qua link không phải trang chi tiết
        (thương hiệu, nearBy, bình luận, ...).
        """
        for selector in DETAIL_LINK_SELECTORS:
            href = normalize_path(await self._attr(item, selector, "href"))
            if not href:
                continue
            if any(part in href for part in EXCLUDED_LINK_PARTS):
                continue
            if len([p for p in href.split("/") if p]) < 2:  # cần dạng /{tỉnh}/{slug}
                continue
            return href
        return ""

    async def _crawl_listing(self, page: Page, province: Dict[str, str]) -> List[Dict[str, Any]]:
        """Bước 5: duyệt các trang danh sách quán ăn và thu link chi tiết.

        Hỗ trợ 2 chế độ:
        - Pagination (mặc định): truy cập ?page=N
        - Scroll loading: lướt xuống click nút 'Xem tiếp kết quả' (id=scrollLoadingPage)
        """
        links: List[Dict[str, Any]] = []
        seen = set()

        listing_url = self._build_listing_url(province["path"], 1)
        logger.info("   Bước 5 — mở danh sách: %s", listing_url)
        if not await self._goto(page, listing_url, wait_selector=LIST_ITEM_SELECTOR):
            return links

        await self._delay(*self.request_delay)

        # Xác định chế độ load more
        load_more_selector = "#scrollLoadingPage a[rel='next']"
        has_load_more = await page.locator(load_more_selector).count() > 0
        logger.info("   Load more button: %s", "có" if has_load_more else "không")

        page_count = 1
        no_new_items_count = 0
        max_no_new = 3  # dừng sau 3 lần không có item mới
        unlimited_pages = self.max_pages <= 0  # 0 hoặc âm = không giới hạn

        while True:
            if self.max_items is not None and len(links) >= self.max_items:
                break

            # Chỉ check max_pages khi có giới hạn (> 0)
            if not unlimited_pages and page_count > self.max_pages:
                logger.info("   Đạt max_pages=%s -> dừng", self.max_pages)
                break

            items = page.locator(LIST_ITEM_SELECTOR)
            count = await items.count()
            logger.info("   Lần load %s: %s item `%s`", page_count, count, LIST_ITEM_SELECTOR)

            if count == 0:
                break

            new_items_this_round = 0
            for index in range(count):
                if self.max_items is not None and len(links) >= self.max_items:
                    break
                item = items.nth(index)
                href = await self._detail_href(item)
                if not href:
                    continue

                full_url = urljoin(BASE_URL, href)
                if full_url in seen:
                    continue

                seen.add(full_url)
                links.append(
                    {
                        "url": full_url,
                        "name": await self._text(item, ".resname h2 a"),
                        "province": province["name"],
                    }
                )
                new_items_this_round += 1

            logger.info("   -> %s link mới (tổng: %s)", new_items_this_round, len(links))

            if new_items_this_round == 0:
                no_new_items_count += 1
                if no_new_items_count >= max_no_new:
                    logger.info("   Không có item mới sau %s lần -> dừng", max_no_new)
                    break
            else:
                no_new_items_count = 0

            # Nếu đã đủ item hoặc đạt max_pages -> dừng
            if self.max_items is not None and len(links) >= self.max_items:
                break
            # Chỉ check max_pages khi CÓ giới hạn (> 0)
            if not unlimited_pages and page_count >= self.max_pages:
                break

            # Thử load thêm: click nút 'Xem tiếp kết quả' hoặc chuyển trang pagination
            if has_load_more:
                try:
                    # Scroll đến nút load more
                    load_more_btn = page.locator(load_more_selector).first
                    await load_more_btn.scroll_into_view_if_needed(timeout=10_000)
                    await self._delay(0.5, 1.0)

                    # Click nút
                    await load_more_btn.click(timeout=15_000)
                    logger.info("   -> Đã click 'Xem tiếp kết quả', chờ load...")

                    # Chờ item mới xuất hiện (đếm item hiện tại, chờ tăng)
                    old_count = count
                    for _ in range(30):  # timeout ~15s
                        await self._delay(0.3, 0.5)
                        new_count = await page.locator(LIST_ITEM_SELECTOR).count()
                        if new_count > old_count:
                            logger.info("   -> Load thêm được %s item mới", new_count - old_count)
                            break
                    else:
                        logger.warning("   -> Timeout chờ item mới, thử click lại lần sau")

                except Exception as exc:
                    logger.warning("   -> Lỗi click load more: %s", exc)
                    # Fallback: thử pagination
                    has_load_more = False
                    page_count += 1
                    listing_url = self._build_listing_url(province["path"], page_count)
                    logger.info("   Fallback pagination -> trang %s: %s", page_count, listing_url)
                    if not await self._goto(page, listing_url, wait_selector=LIST_ITEM_SELECTOR):
                        break
                    await self._delay(*self.request_delay)
            else:
                # Pagination mode
                page_count += 1
                listing_url = self._build_listing_url(province["path"], page_count)
                logger.info("   Pagination -> trang %s: %s", page_count, listing_url)
                if not await self._goto(page, listing_url, wait_selector=LIST_ITEM_SELECTOR):
                    break
                await self._delay(*self.request_delay)

        self.stats["items_found"] += len(links)
        logger.info(
            "   Tổng link chi tiết lấy được của %s: %s (giới hạn --max-items=%s)",
            province["name"],
            len(links),
            self.max_items,
        )
        return links


    # --------------------------------------------------------
    # BƯỚC 6: TÊN QUÁN + RATING
    # --------------------------------------------------------

    async def _parse_name(self, page: Page) -> str:
        """Bước 6: tên quán = `h1` nằm trong `.main-information.disableSection`."""
        return await self._first_text(page, NAME_SELECTORS)

    async def _parse_ratings(self, page: Page) -> Dict[str, Any]:
        """Bước 6: đọc `.microsite-points-summary`.

        Gồm: điểm trung bình (`.microsite-point-avg`), số bình luận
        (`.microsite-review-count`) và 5 điểm chi tiết
        (Chất lượng / Giá cả / Vị trí / Phục vụ / Không gian).
        """
        ratings: Dict[str, Any] = {
            "rating_avg": None,
            "rating_quality": None,
            "rating_price": None,
            "rating_service": None,
            "rating_space": None,
            "rating_location": None,
            "review_count": None,
        }

        summary = page.locator(RATING_SUMMARY_SELECTOR).first
        if await summary.count() == 0:
            logger.debug("Không thấy %s", RATING_SUMMARY_SELECTOR)
            return ratings

        ratings["rating_avg"] = to_float(await self._text(summary, RATING_AVG_SELECTOR))
        ratings["review_count"] = to_int(await self._text(summary, REVIEW_COUNT_SELECTOR))

        groups = summary.locator(RATING_GROUP_SELECTOR)
        group_count = await groups.count()
        used_fields = set()

        for index in range(group_count):
            group = groups.nth(index)
            label = strip_accents(await self._text(group, ".label"))
            value = to_float(await self._text(group, ".avg-txt-highlight"))

            target = None
            for field, aliases in RATING_LABEL_MAP.items():
                if label and any(alias in label for alias in aliases):
                    target = field
                    break
            # Label bị đổi chữ/thiếu -> dùng thứ tự DOM đã quan sát được
            if target is None and not label and index < len(RATING_FALLBACK_ORDER):
                target = RATING_FALLBACK_ORDER[index]

            if target and target not in used_fields:
                ratings[target] = value
                used_fields.add(target)

        return ratings

    # --------------------------------------------------------
    # BƯỚC 7: ĐỊA CHỈ / GIỜ MỞ CỬA / KHOẢNG GIÁ
    # --------------------------------------------------------

    async def _parse_address(self, page: Page) -> Dict[str, str]:
        """Bước 7: địa chỉ trong khối `.disableSection` (microdata schema.org/PostalAddress)."""
        address = {"address": "", "district": "", "city": ""}

        scope = page.locator(".disableSection").first
        if await scope.count() == 0:
            scope = page

        address["address"] = await self._text(scope, "span[itemprop='streetAddress']")
        address["district"] = await self._text(scope, "span[itemprop='addressLocality']")
        address["city"] = await self._text(scope, "span[itemprop='addressRegion']")

        if not any(address.values()):
            # Fallback: tách chuỗi địa chỉ đầy đủ "…, Quận 10, TP. HCM"
            raw = await self._text(page, ".res-common-add")
            address = parse_address_text(raw)

        return address

    async def _parse_opening_hours(self, page: Page) -> Dict[str, str]:
        """Bước 7: giờ mở/đóng cửa (khung giờ đầu tiên hiển thị)."""
        hours = {"open_time": "", "close_time": ""}

        raw = await self._first_text(page, HOURS_SELECTORS)
        if not raw:
            # Meta/JSON-LD dạng itemprop="openingHours"
            raw = await self._attr(page, "meta[itemprop='openingHours']", "content")
        if not raw:
            raw = await self._text(page, "[itemprop='openingHours']")

        hours["open_time"], hours["close_time"] = parse_opening_hours(raw)
        return hours

    async def _parse_price_range(self, page: Page) -> Dict[str, Optional[int]]:
        """Bước 7: giá thấp nhất / cao nhất từ `priceRange`."""
        raw = await self._text(page, PRICE_SELECTOR)
        if not raw:
            raw = await self._text(page, PRICE_FALLBACK_SELECTOR)

        price_min, price_max = parse_price_range(raw)
        return {"price_min": price_min, "price_max": price_max}


    # --------------------------------------------------------
    # BƯỚC 8: CLICK "BẢN ĐỒ" -> LẤY LAT/LNG
    # --------------------------------------------------------

    async def _click_map_link(self, page: Page) -> bool:
        """Bước 8: click thẻ `<a class="linkmap">Bản đồ</a>` để mở bản đồ."""
        for selector in MAP_LINK_SELECTORS:
            link = page.locator(selector).first
            if await link.count() == 0:
                continue
            try:
                await link.scroll_into_view_if_needed(timeout=10_000)
                await link.click(timeout=15_000)
                await self._delay(1.0, 2.0)  # chờ Google Maps render element
                logger.debug("Đã click link bản đồ: %s", selector)
                return True
            except Exception as exc:
                logger.debug("Click link bản đồ lỗi (%s): %s", selector, exc)
        return False

    async def _coords_from_map_images(self, page: Page) -> Optional[Tuple[float, float]]:
        """Quét mọi `img` trong TẤT CẢ frame + HTML tìm pattern `!4m2!1x{lat}!2x{lng}`."""
        candidates: List[str] = []

        for frame in page.frames:
            try:
                srcs = await frame.eval_on_selector_all(
                    "img", "els => els.map(e => e.currentSrc || e.src || '')"
                )
                candidates.extend(srcs)
            except Exception:
                continue

        # Phòng trường hợp ảnh được chèn trực tiếp vào DOM chính bằng JS
        try:
            candidates.append(await page.content())
        except Exception:
            pass

        for src in candidates:
            match = MAP_IMG_COORD_RE.search(src or "")
            if match:
                lat_e7, lng_e7 = int(match.group(1)), int(match.group(2))
                return round(lat_e7 / 10_000_000, 7), round(lng_e7 / 10_000_000, 7)
        return None

    def _coords_from_embed_iframe(self, page: Page) -> Optional[Tuple[float, float]]:
        """Fallback: iframe Google Maps embed có dạng `…/maps/embed/v1/place?…&q=lat,lng`."""
        for frame in page.frames:
            match = MAP_EMBED_COORD_RE.search(frame.url or "")
            if match:
                return float(match.group(1)), float(match.group(2))
        return None

    async def _coords_from_geo_microdata(self, page: Page) -> Optional[Tuple[float, float]]:
        """Fallback: microdata `span[itemprop=geo]` (foody render sẵn ở server)."""
        raw_lat = await self._attr(page, "meta[itemprop='latitude']", "content")
        raw_lng = await self._attr(page, "meta[itemprop='longitude']", "content")
        if not raw_lat or not raw_lng:
            raw_lat = await self._text(page, "span[itemprop='latitude']")
            raw_lng = await self._text(page, "span[itemprop='longitude']")
        lat, lng = to_float(raw_lat), to_float(raw_lng)
        if lat is None or lng is None:
            return None
        return lat, lng

    def _coords_from_url(self, page: Page) -> Optional[Tuple[float, float]]:
        """Fallback cuối: toạ độ nhúng trong URL (`@lat,lng`)."""
        match = URL_AT_COORD_RE.search(page.url or "")
        if match:
            return float(match.group(1)), float(match.group(2))
        return None

    async def _extract_coordinates(self, page: Page) -> Tuple[Optional[float], Optional[float], str]:
        """Bước 8: click bản đồ rồi lấy lat/lng, kèm chuỗi fallback (trả cả nguồn lấy được)."""
        await self._click_map_link(page)

        # (1) Chờ ảnh Google Maps xuất hiện pattern `!4m2!1x…!2x…`
        deadline = time.monotonic() + self.map_wait_timeout_ms / 1000
        while time.monotonic() < deadline:
            coords = await self._coords_from_map_images(page)
            if coords:
                self._log_coords(coords, "map-img")
                await self._close_map_popup(page)
                return coords[0], coords[1], "map-img"
            await asyncio.sleep(0.5)

        # (2) iframe Google Maps embed (?q=lat,lng)
        coords = self._coords_from_embed_iframe(page)
        if coords:
            self._log_coords(coords, "map-iframe")
            await self._close_map_popup(page)
            return coords[0], coords[1], "map-iframe"

        # (3) microdata geo có sẵn từ server
        coords = await self._coords_from_geo_microdata(page)
        if coords:
            self._log_coords(coords, "geo-meta")
            await self._close_map_popup(page)
            return coords[0], coords[1], "geo-meta"

        # (4) toạ độ trong URL
        coords = self._coords_from_url(page)
        if coords:
            self._log_coords(coords, "url")
            await self._close_map_popup(page)
            return coords[0], coords[1], "url"

        logger.warning("Không lấy được toạ độ: %s", page.url)
        await self._close_map_popup(page)
        return None, None, ""

    def _log_coords(self, coords: Tuple[float, float], source: str) -> None:
        """Ghi log + thống kê nguồn toạ độ."""
        lat, lng = coords
        self.stats["coord_sources"][source] = self.stats["coord_sources"].get(source, 0) + 1
        if not is_vn_coordinate(lat, lng):
            logger.warning("Toạ độ ngoài bounding box Việt Nam (%s): %s, %s", source, lat, lng)
        else:
            logger.debug("Toạ độ (%s): %s, %s", source, lat, lng)

    async def _close_map_popup(self, page: Page) -> None:
        """Đóng popup bản đồ để không che các thao tác sau."""
        try:
            await page.keyboard.press("Escape")
        except Exception:
            pass


    # --------------------------------------------------------
    # GỘP BƯỚC 6 + 7 + 8: PARSE 1 TRANG CHI TIẾT
    # --------------------------------------------------------

    async def _parse_detail(self, page: Page, province: str, source_url: str) -> FoodyEntity:
        """Parse toàn bộ thông tin của 1 quán ăn trong trang chi tiết."""
        entity = FoodyEntity(
            entity_type=ENTITY_TYPE,
            province=province,
            source_url=source_url,
            source=SOURCE,
        )

        entity.name = await self._parse_name(page)

        for key, value in (await self._parse_ratings(page)).items():
            setattr(entity, key, value)
        for key, value in (await self._parse_address(page)).items():
            setattr(entity, key, value)
        for key, value in (await self._parse_opening_hours(page)).items():
            setattr(entity, key, value)
        for key, value in (await self._parse_price_range(page)).items():
            setattr(entity, key, value)

        entity.lat, entity.lng, _ = await self._extract_coordinates(page)
        return entity

    # --------------------------------------------------------
    # ĐIỀU PHỐI: 1 TỈNH
    # --------------------------------------------------------

    async def _crawl_province(
        self,
        province: Dict[str, str],
        index: int,
        total: int,
    ) -> int:
        """Crawl 1 tỉnh: chọn tỉnh (3+4) -> danh sách (5) -> từng trang chi tiết (6+7+8)."""
        logger.info("-" * 70)
        logger.info("[%s/%s] TỈNH/THÀNH: %s (%s)", index, total, province["name"], province["slug"])

        listing_page = await self._new_page()
        detail_page = await self._new_page()
        saved = 0

        try:
            if not await self._select_province(listing_page, province):
                return 0

            # Bước 4: lấy URL tỉnh hiện tại rồi ghép thêm /food/quan-an
            logger.info("   Bước 4 — URL danh sách: %s", self._build_listing_url(listing_page.url))

            links = await self._crawl_listing(listing_page, province)
            if not links:
                logger.warning("   ⚠️ Không có quán ăn nào cho %s", province["name"])
                return 0

            for position, link in enumerate(links, start=1):
                url = link["url"]
                if url in self.seen_urls:
                    self.stats["items_skipped"] += 1
                    logger.info("   [%s/%s] Bỏ qua (đã có): %s", position, len(links), url)
                    continue

                try:
                    logger.info("   [%s/%s] Chi tiết: %s", position, len(links), url)
                    if not await self._goto(detail_page, url, wait_selector="h1"):
                        raise RuntimeError("Không mở được trang chi tiết")

                    await self._delay(*self.request_delay)
                    entity = await self._parse_detail(detail_page, province["name"], url)

                    if not entity.name:
                        raise RuntimeError("Không lấy được tên quán (h1 rỗng)")

                    self.records.append(entity)
                    self.seen_urls.add(url)
                    saved += 1
                    self.stats["items_saved"] += 1
                    logger.info(
                        "   [OK] %s | %s | rating=%s | giá=%s-%s | toạ độ=%s,%s",
                        entity.name,
                        entity.district or "?",
                        entity.rating_avg,
                        entity.price_min,
                        entity.price_max,
                        entity.lat,
                        entity.lng,
                    )
                except Exception as exc:
                    self.stats["items_failed"] += 1
                    logger.error("   [X] Lỗi quán %s: %s", url, exc)

                await self._delay(*self.item_delay)
        finally:
            for page in (detail_page, listing_page):
                try:
                    await page.close()
                except Exception:
                    pass

        return saved


    # --------------------------------------------------------
    # LƯU JSON / RESUME
    # --------------------------------------------------------

    def _load_existing(self) -> None:
        """`--resume`: nạp file JSON cũ để không crawl lại (dedup theo source_url)."""
        if not self.output_file.exists():
            logger.info("--resume: chưa có file %s, sẽ crawl mới", self.output_file)
            return
        try:
            with open(self.output_file, "r", encoding="utf-8") as handle:
                data = json.load(handle)
        except Exception as exc:
            logger.warning("--resume: không đọc được %s (%s), bỏ qua", self.output_file, exc)
            return

        if not isinstance(data, list):
            logger.warning("--resume: file %s không phải list, bỏ qua", self.output_file)
            return

        for item in data:
            try:
                entity = FoodyEntity(**item)
            except Exception:
                continue
            if not entity.source_url or entity.source_url in self.seen_urls:
                continue
            self.seen_urls.add(entity.source_url)
            self.records.append(entity)

        logger.info("--resume: nạp lại %s record từ %s", len(self.records), self.output_file)

    def _save(self, final: bool = False) -> None:
        """Ghi JSON: incremental sau mỗi tỉnh, final khi kết thúc."""
        if not self.records:
            logger.warning("Chưa có record nào để lưu.")
            return

        self.output_file.parent.mkdir(parents=True, exist_ok=True)
        payload = [entity.to_record() for entity in self.records]

        with open(self.output_file, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)

        logger.info(
            "%s %s record -> %s",
            "Đã lưu (final)" if final else "Đã lưu (incremental)",
            len(payload),
            self.output_file,
        )

    # --------------------------------------------------------
    # RUN
    # --------------------------------------------------------

    async def run(self) -> List[Dict[str, Any]]:
        """Chạy toàn bộ flow crawl."""
        if self.resume:
            self._load_existing()

        logger.info("=" * 70)
        logger.info("FOODY CRAWLER — entity_type=%s, layer=raw/bronze", ENTITY_TYPE)
        logger.info(
            "Giới hạn: max_provinces=%s | max_items/tỉnh=%s | max_pages=%s",
            self.max_provinces,
            self.max_items,
            self.max_pages,
        )
        logger.info("Output: %s", self.output_file)
        logger.info("=" * 70)

        start = time.monotonic()

        async with async_playwright() as playwright:
            await self._start_browser(playwright)
            try:
                home_page = await self._new_page()
                provinces = await self._load_provinces(home_page)
                await home_page.close()

                if not provinces:
                    logger.error("Không lấy được danh sách tỉnh/thành — dừng crawl.")
                    return []

                for index, province in enumerate(provinces, start=1):
                    try:
                        await self._crawl_province(province, index, len(provinces))
                    except Exception as exc:
                        logger.error("[%s] Lỗi cả tỉnh %s: %s", index, province["name"], exc)
                    finally:
                        self.stats["provinces_done"] += 1
                        self._save()  # incremental: không mất dữ liệu nếu bị ngắt
                    await self._delay(*self.request_delay)
            finally:
                await self._stop_browser()

        self._save(final=True)
        self._log_summary(time.monotonic() - start)
        return [entity.to_record() for entity in self.records]

    def _log_summary(self, elapsed_seconds: float) -> None:
        """Tổng kết + tỉ lệ field được điền (đánh giá chất lượng dữ liệu)."""
        logger.info("=" * 70)
        logger.info("HOÀN TẤT sau %.1fs", elapsed_seconds)
        logger.info("Tỉnh/thành đã xử lý   : %s", self.stats["provinces_done"])
        logger.info("Link tìm thấy         : %s", self.stats["items_found"])
        logger.info("Record đã lưu         : %s", self.stats["items_saved"])
        logger.info("Record lỗi            : %s", self.stats["items_failed"])
        logger.info("Record bỏ qua         : %s", self.stats["items_skipped"])
        logger.info("Nguồn toạ độ          : %s", self.stats["coord_sources"])
        logger.info("Tổng record trong file: %s", len(self.records))

        if self.records:
            payload = [entity.to_record() for entity in self.records]
            total = len(payload)
            for field in payload[0].keys():
                filled = sum(1 for row in payload if row.get(field) not in (None, "", []))
                logger.info("   - %-16s %5.1f%% (%s/%s)", field, 100 * filled / total, filled, total)

        logger.info("File output: %s", self.output_file)
        logger.info("Log file   : %s", LOG_FILE)
        logger.info("=" * 70)



# ============================================================
# CLI
# ============================================================

def build_arg_parser() -> argparse.ArgumentParser:
    """Tham số CLI — có sẵn các cờ giới hạn để test trước khi crawl full."""
    parser = argparse.ArgumentParser(
        description="Crawl quán ăn từ Foody.vn vào phase ingestion (raw/bronze).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--headless",
        dest="headless",
        action="store_true",
        default=False,
        help="Chạy ẩn (mặc định, phù hợp server/CI).",
    )
    parser.add_argument(
        "--no-headless",
        dest="headless",
        action="store_false",
        help="Hiện UI browser (dùng khi cần quan sát/gỡ lỗi).",
    )
    parser.add_argument("--max-provinces", type=int, default=None, help="Giới hạn số tỉnh/thành.")
    parser.add_argument("--max-items", type=int, default=None, help="Giới hạn số quán mỗi tỉnh (None = không giới hạn).")
    parser.add_argument("--max-pages", type=int, default=0, help="Số lần load trang/lượt click 'Xem tiếp' (0 = không giới hạn, crawl hết).")
    parser.add_argument(
        "--provinces",
        type=str,
        default=None,
        help='Chỉ crawl một số tỉnh, cách nhau dấu phẩy. Ví dụ: "TP. HCM,Hà Nội".',
    )
    parser.add_argument(
        "--output",
        type=str,
        default=str(OUTPUT_FILE),
        help="Đường dẫn file JSON output.",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Nạp file output cũ và bỏ qua các quán đã có (dedup theo source_url).",
    )
    parser.add_argument("--slow-mo", type=int, default=0, help="Làm chậm thao tác browser (ms).")
    parser.add_argument(
        "--proxy",
        type=str,
        default=None,
        help="Proxy cho browser, ví dụ http://user:pass@host:port.",
    )
    parser.add_argument(
        "--use-stealth",
        action="store_true",
        help=(
            "Bật playwright_stealth (mặc định TẮT: stealth làm hỏng AngularJS "
            "của foody.vn nên dropdown tỉnh/thành không render)."
        ),
    )
    return parser


async def main_async(args: argparse.Namespace) -> List[Dict[str, Any]]:
    crawler = FoodyCrawler(
        output_file=Path(args.output),
        headless=args.headless,
        max_provinces=args.max_provinces,
        max_items=args.max_items,
        max_pages=args.max_pages,
        provinces=args.provinces.split(",") if args.provinces else None,
        resume=args.resume,
        slow_mo=args.slow_mo,
        proxy=args.proxy,
        use_stealth=args.use_stealth,
    )
    return await crawler.run()


def main() -> None:
    """Entry point: `python3.11 ingestion/foody/foody_crawler.py [options]`."""
    args = build_arg_parser().parse_args()
    try:
        records = asyncio.run(main_async(args))
    except KeyboardInterrupt:
        logger.warning("Người dùng dừng (Ctrl+C). Dữ liệu đã lưu incremental còn nguyên.")
        return
    logger.info("Đã thu thập %s record từ Foody.vn.", len(records))


if __name__ == "__main__":
    main()

