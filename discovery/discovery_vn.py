import asyncio
import json
import random
import re
from pathlib import Path
from typing import List, Optional
from urllib.parse import urljoin

import yaml
from pydantic import BaseModel
from playwright.async_api import async_playwright, Browser, Page, TimeoutError as PlaywrightTimeoutError

try:
    from playwright_stealth import stealth_async
except ImportError:
    stealth_async = None


class EntityCandidate(BaseModel):
    name: str
    entity_type: Optional[str] = None
    province: str
    source_url: Optional[str] = None
    rank: Optional[int] = None
    source: str = "tripadvisor_vn"


class TripAdvisorVNDiscovery:
    BASE_URL = "https://www.tripadvisor.com.vn"

    def __init__(
        self,
        seeds_dir: str = "seeds",
        output_file: str = "outputs/entity_registry_vn.json",
        headless: bool = False,
        max_provinces: Optional[int] = None,
    ):
        self.seeds_dir = Path(seeds_dir)
        self.output_file = Path(output_file)
        self.headless = headless
        self.max_provinces = max_provinces
        self.browser: Optional[Browser] = None
        self.registry: List[EntityCandidate] = []

        self.output_file.parent.mkdir(parents=True, exist_ok=True)

    def load_provinces(self) -> List[str]:
        with open(self.seeds_dir / "provinces.yaml", encoding="utf-8") as f:
            provinces = yaml.safe_load(f) or []
        if self.max_provinces:
            provinces = provinces[: self.max_provinces]
        return provinces

    async def start_browser(self):
        playwright = await async_playwright().start()

        self.browser = await playwright.chromium.launch(
            headless=False,                    # Bắt buộc
            channel="chrome",                  # Dùng Chrome thật đã cài trên máy
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-infobars",
                "--no-first-run",
                "--no-default-browser-check",
                "--disable-dev-shm-usage",
                "--no-sandbox",
                "--window-size=1366,768",
            ],
            ignore_default_args=["--enable-automation"],  # Quan trọng
        )


    async def run(self):
        provinces = self.load_provinces()
        print(f"Bắt đầu discovery {len(provinces)} tỉnh trên TripAdvisor.vn\n")

        await self.start_browser()

        try:
            context = await self.browser.new_context(
                viewport={"width": 1366, "height": 768},
                locale="vi-VN",
                timezone_id="Asia/Ho_Chi_Minh",
                user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
                java_script_enabled=True,
                extra_http_headers={
                    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                },
            )

            # Ẩn webdriver
            await context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
            """)

            page = await context.new_page()

            # Thử vào trang chủ trước
            print("→ Đang mở tripadvisor.com.vn ...")
            await page.goto(self.BASE_URL, wait_until="domcontentloaded", timeout=60000)
            await self.human_delay(3000, 5000)

            # Kiểm tra có bị chặn không
            content = await page.content()
            if "enable JS" in content or "ad blocker" in content.lower():
                print("❌ Vẫn bị chặn ngay từ trang chủ.")
                print("→ Khuyến nghị: Đổi IP hoặc dùng proxy residential.")
                return

            print("✅ Vào được trang chủ thành công.")

            for province in provinces:
                try:
                    attractions_url = await self.search_province(page, province)
                    if not attractions_url:
                        continue

                    candidates = await self.scrape_attractions(page, attractions_url, province)
                    self.add_candidates(candidates)

                    await self.human_delay(5000, 9000)

                except Exception as e:
                    print(f"❌ Lỗi với {province}: {e}")
                    continue

            self.save()

        finally:
            await self.stop_browser()

    async def stop_browser(self):
        if self.browser:
            await self.browser.close()

    async def human_delay(self, min_ms=800, max_ms=2500):
        await asyncio.sleep(random.uniform(min_ms / 1000, max_ms / 1000))

    async def search_province(self, page: Page, province: str) -> Optional[str]:
        print(f"🔍 Searching: {province}")

        await page.goto(self.BASE_URL, wait_until="domcontentloaded", timeout=60000)
        await self.human_delay(1500, 2800)

        # Ô search (trên bản .vn thường dùng placeholder tiếng Việt hoặc giống bản .com)
        search_input = page.locator(
            'input[placeholder*="Địa điểm"], '
            'input[placeholder*="Places to go"], '
            'input[placeholder*="Tìm kiếm"], '
            'input[role="searchbox"]'
        ).first

        await search_input.wait_for(state="visible", timeout=15000)
        await search_input.click()
        await self.human_delay(300, 600)

        await search_input.fill("")
        for char in province:
            await search_input.type(char, delay=random.randint(70, 160))
        await self.human_delay(900, 1500)

        # Đợi typeahead
        typeahead = page.locator('div#typeahead_results, div[data-test-attribute="typeahead-results"]')
        try:
            await typeahead.wait_for(state="visible", timeout=8000)
        except PlaywrightTimeoutError:
            print(f"   ⚠️ Typeahead không hiện với {province}")
            return None

        options = typeahead.locator('a[role="option"]')
        count = await options.count()

        geo_code = None
        slug = None

        for i in range(count):
            option = options.nth(i)
            href = await option.get_attribute("href")
            text = (await option.inner_text()).lower()

            if href and "/Tourism-g" in href and province.lower() in text:
                match = re.search(r"/Tourism-(g\d+)-(.+?)-Vacations\.html", href)
                if match:
                    geo_code = match.group(1)
                    slug = match.group(2)
                    print(f"   → Tìm thấy: {geo_code} | {slug}")
                    break

        if not geo_code or not slug:
            print(f"   ⚠️ Không tìm thấy geo code cho {province}")
            return None

        attractions_url = f"{self.BASE_URL}/Attractions-{geo_code}-Activities-oa0-{slug}.html"
        print(f"   → Attractions URL: {attractions_url}")
        return attractions_url

    async def scrape_attractions(self, page: Page, attractions_url: str, province: str) -> List[EntityCandidate]:
        print(f"   📥 Scraping attractions...")
        await page.goto(attractions_url, wait_until="domcontentloaded", timeout=60000)
        await self.human_delay(2000, 3500)

        candidates = []
        page_num = 1

        while True:
            print(f"   → Đang lấy trang {page_num}...")

            for _ in range(2):
                await page.mouse.wheel(0, random.randint(600, 1000))
                await self.human_delay(600, 1100)

            articles = page.locator("article.GTuVU.XJlaI")
            count = await articles.count()
            print(f"     Tìm thấy {count} article")

            for i in range(count):
                article = articles.nth(i)
                try:
                    # Tên
                    name_el = article.locator("h3.biGQs div.XfVdV")
                    name_raw = (await name_el.inner_text()).strip() if await name_el.count() else None

                    if not name_raw:
                        name_el = article.locator("h3")
                        name_raw = (await name_el.inner_text()).strip() if await name_el.count() else None

                    # entity_type
                    entity_type = None
                    type_container = article.locator("div.alPVI.eNNhq.PgLKC.tnGGX.yzLvM")
                    if await type_container.count() > 0:
                        type_el = type_container.locator("div.biGQs._P.VImYz.ZNjnF").first
                        if await type_el.count() > 0:
                            entity_type = (await type_el.inner_text()).strip()

                    # Link
                    link_el = article.locator("a[href*='Attraction_Review']").first
                    href = await link_el.get_attribute("href") if await link_el.count() else None
                    source_url = urljoin(self.BASE_URL, href) if href else None

                    if not name_raw:
                        continue

                    rank = None
                    name = name_raw
                    match = re.match(r"^(\d+)\.\s*(.+)$", name_raw)
                    if match:
                        rank = int(match.group(1))
                        name = match.group(2).strip()

                    candidate = EntityCandidate(
                        name=name,
                        entity_type=entity_type,
                        province=province,
                        source_url=source_url,
                        rank=rank,
                        source="tripadvisor_vn",
                    )

                    if not any(c.name == candidate.name and c.province == candidate.province for c in candidates):
                        candidates.append(candidate)

                except Exception as e:
                    print(f"     ⚠️ Lỗi article {i}: {e}")
                    continue

            # Nút Next page
            next_btn = page.locator(
                'a[data-smoke-attr="pagination-next-arrow"], '
                'a[aria-label="Next page"], '
                'a[aria-label="Trang tiếp theo"]'
            )

            if await next_btn.count() == 0:
                print("   → Không còn trang tiếp theo.")
                break

            try:
                class_attr = await next_btn.first.get_attribute("class") or ""
                if "disabled" in class_attr.lower():
                    print("   → Nút Next đã disabled.")
                    break

                await next_btn.first.scroll_into_view_if_needed()
                await self.human_delay(600, 1200)

                print("   → Bấm Next page...")
                await next_btn.first.click(timeout=10000)
                await page.wait_for_load_state("domcontentloaded")
                await self.human_delay(2500, 4000)

                page_num += 1
                if page_num > 20:
                    print("   → Đã đến giới hạn 20 trang.")
                    break

            except Exception as e:
                print(f"   ⚠️ Không bấm được Next: {e}")
                break

        print(f"   ✅ Tổng cộng lấy được {len(candidates)} entity từ {province}")
        return candidates

    def is_duplicate(self, candidate: EntityCandidate) -> bool:
        name_norm = candidate.name.lower().strip()
        for existing in self.registry:
            if existing.province == candidate.province and existing.name.lower().strip() == name_norm:
                return True
        return False

    def add_candidates(self, candidates: List[EntityCandidate]):
        added = 0
        for c in candidates:
            if not self.is_duplicate(c):
                self.registry.append(c)
                added += 1
        print(f"   ✅ Thêm {added} entity mới")

    def save(self):
        data = [c.model_dump() for c in self.registry]
        with open(self.output_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"\n💾 Đã lưu {len(self.registry)} entity → {self.output_file}")

    async def run(self):
        provinces = self.load_provinces()
        print(f"Bắt đầu discovery {len(provinces)} tỉnh trên TripAdvisor.vn\n")

        await self.start_browser()

        try:
            context = await self.browser.new_context(
                locale="vi-VN",
                viewport={"width": 1366, "height": 768},
                user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
            )
            page = await context.new_page()

            if stealth_async:
                await stealth_async(page)

            for province in provinces:
                try:
                    attractions_url = await self.search_province(page, province)
                    if not attractions_url:
                        continue

                    candidates = await self.scrape_attractions(page, attractions_url, province)
                    self.add_candidates(candidates)

                    await self.human_delay(4000, 8000)

                except Exception as e:
                    print(f"❌ Lỗi với {province}: {e}")
                    continue

            self.save()

        finally:
            await self.stop_browser()


if __name__ == "__main__":
    discovery = TripAdvisorVNDiscovery(
        headless=False,
        max_provinces=2,  # test trước
    )
    asyncio.run(discovery.run())