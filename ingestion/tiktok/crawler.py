import os
import re
import json
import time
import random
import logging
import hashlib
from datetime import datetime
from urllib.parse import quote
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ============================================================
# PATH
# ============================================================
BASE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
)
TIKTOK_DIR = os.path.join(BASE_DIR, "ingestion", "tiktok")
RAW_DIR = os.path.join(BASE_DIR, "data", "raw", "tiktok")
LOG_DIR = os.path.join(BASE_DIR, "logs")
KEYWORD_FILE = os.path.join(TIKTOK_DIR, "keyword.csv")
OUTPUT_JSON = os.path.join(RAW_DIR, "tiktok_metadata_comments.json")

os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# ============================================================
# LOGGING
# ============================================================
LOG_FILE = os.path.join(LOG_DIR, "tiktok_comments_crawler.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ============================================================
# CONFIG
# ============================================================
MAX_VIDEOS_PER_KEYWORD = 20
MAX_COMMENTS_PER_VIDEO = 50
SEARCH_SCROLL_ROUNDS = 8
COMMENT_SCROLL_ROUNDS = 60
MIN_SLEEP = 2
MAX_SLEEP = 5

# ============================================================
# HELPER
# ============================================================
def random_sleep(minimum=MIN_SLEEP, maximum=MAX_SLEEP):
    time.sleep(random.uniform(minimum, maximum))

def parse_count(text):
    if not text:
        return None
    text = str(text).strip().replace(",", "").replace(" ", "")
    if not text:
        return None
    try:
        suffix = text[-1].upper()
        if suffix == "K":
            return int(float(text[:-1]) * 1_000)
        if suffix == "M":
            return int(float(text[:-1]) * 1_000_000)
        if suffix == "B":
            return int(float(text[:-1]) * 1_000_000_000)
        return int(float(text))
    except Exception:
        return None

def extract_video_id(url):
    if not url:
        return None
    match = re.search(r"/video/(\d+)", url)
    return match.group(1) if match else None

def clean_text(text):
    if not text:
        return ""
    return re.sub(r"\s+", " ", str(text)).strip()

def make_comment_id(author, text, posted_time):
    raw = f"{author or ''}|{text or ''}|{posted_time or ''}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()

# ============================================================
# DRIVER
# ============================================================
def create_driver():
    options = Options()
    options.add_argument("--start-maximized")
    options.add_argument("--disable-notifications")
    options.add_argument("--lang=vi-VN")
    options.add_argument("--disable-popup-blocking")
    # options.add_argument("--headless=new")  # bỏ comment nếu muốn chạy ẩn
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(60)
    return driver

# ============================================================
# SEARCH VIDEO URL
# ============================================================
def search_video_urls(driver, keyword):
    logger.info("Searching: %s", keyword)
    url = "https://www.tiktok.com/search?q=" + quote(keyword)
    driver.get(url)
    time.sleep(6)

    video_urls = set()
    last_count = 0

    for round_no in range(SEARCH_SCROLL_ROUNDS):
        links = driver.find_elements(By.CSS_SELECTOR, 'a[href*="/video/"]')
        for link in links:
            try:
                href = link.get_attribute("href")
                if not href:
                    continue
                match = re.search(r"(https://www\.tiktok\.com/[^\"']+/video/\d+)", href)
                if match:
                    video_urls.add(match.group(1))
            except Exception:
                continue

        logger.info("Search round %d | videos: %d", round_no + 1, len(video_urls))

        if len(video_urls) >= MAX_VIDEOS_PER_KEYWORD:
            break

        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(random.uniform(3, 5))

        if len(video_urls) == last_count:
            break
        last_count = len(video_urls)

    return list(video_urls)[:MAX_VIDEOS_PER_KEYWORD]

# ============================================================
# GET ELEMENT TEXT
# ============================================================
def get_text(driver, selectors):
    for selector in selectors:
        try:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            for element in elements:
                text = clean_text(element.text)
                if text:
                    return text
        except Exception:
            continue
    return None

# ============================================================
# VIDEO METADATA
# ============================================================
def get_video_metadata(driver, keyword, province, category, video_url):
    logger.info("Open video: %s", video_url)
    driver.get(video_url)
    time.sleep(random.uniform(4, 7))

    video_id = extract_video_id(driver.current_url)

    author = get_text(driver, [
        '[data-e2e="browse-username"]',
        '[data-e2e="video-author-uniqueid"]',
        'h2[data-e2e="video-author-uniqueid"]',
        'a[href*="/@"] span'
    ])

    description = get_text(driver, [
        '[data-e2e="browse-video-desc"]',
        '[data-e2e="video-desc"]',
        'div[data-e2e="browse-video-desc"]'
    ])

    like_count = get_text(driver, [
        '[data-e2e="browse-like-count"]',
        '[data-e2e="like-count"]'
    ])
    comment_count = get_text(driver, [
        '[data-e2e="browse-comment-count"]',
        '[data-e2e="comment-count"]'
    ])
    share_count = get_text(driver, [
        '[data-e2e="share-count"]',
        '[data-e2e="browse-share-count"]'
    ])

    # View count thường không hiện ổn định
    view_count = None

    hashtags = re.findall(r"#\S+", description) if description else []

    posted_date = get_text(driver, [
        'span[data-e2e="browser-nickname"] + span',
        'time',
        '[data-e2e="browser-nickname"]'
    ])

    metadata = {
        "video_id": video_id,
        "video_url": video_url,
        "keyword": keyword,
        "province": province,
        "category": category,
        "author": author,
        "description": description,
        "hashtags": hashtags,
        "view_count": parse_count(view_count),
        "like_count": parse_count(like_count),
        "comment_count": parse_count(comment_count),
        "share_count": parse_count(share_count),
        "posted_date": posted_date,
        "crawl_time": datetime.now().isoformat(),
        "comments": [],
        "comments_crawled": 0
    }
    return metadata

# ============================================================
# OPEN COMMENT PANEL
# ============================================================
def open_comment_panel(driver):
    logger.info("Opening TikTok comment panel...")

    selectors = [
        '[data-e2e="comment-icon"]',
        '[data-e2e="comment-count"]',
        '[data-e2e="browse-comment-icon"]',
        '[data-e2e="browse-comment-count"]',
        'button[aria-label*="comment" i]',
        'button[aria-label*="bình luận" i]',
        'button[aria-label*="comments" i]'
    ]

    clicked = False
    for selector in selectors:
        try:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            logger.info("Selector %s -> %d elements", selector, len(elements))
            for element in elements:
                try:
                    if not element.is_displayed():
                        continue
                    driver.execute_script(
                        "arguments[0].scrollIntoView({block: 'center'});", element
                    )
                    time.sleep(0.8)
                    try:
                        element.click()
                    except Exception:
                        driver.execute_script("arguments[0].click();", element)
                    logger.info("Clicked comment button: %s", selector)
                    clicked = True
                    break
                except Exception as e:
                    logger.debug("Click failed: %s", e)
            if clicked:
                break
        except Exception:
            continue

    time.sleep(3)

    # Kiểm tra comment đã load chưa
    comment_selectors = [
        'p[data-e2e="comment-level-1"]',
        '[data-e2e="comment-level-1"]',
        '[data-e2e="comment-item"]',
        '[data-e2e="comment-content"]'
    ]
    for selector in comment_selectors:
        try:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            if elements:
                logger.info("Comment DOM loaded: %s | %d elements", selector, len(elements))
                return True
        except Exception:
            continue

    # Wait thêm
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'p[data-e2e="comment-level-1"]'))
        )
        logger.info("Comment DOM loaded after wait.")
        return True
    except Exception:
        logger.warning("Comment DOM not detected.")
        return False

# ============================================================
# FIND COMMENT ELEMENTS (cải thiện)
# ============================================================
def find_comment_elements(driver):
    selectors = [
        'p[data-e2e="comment-level-1"]',
        '[data-e2e="comment-level-1"]',
        '[data-e2e="comment-item"]',
        'div[class*="CommentItemContainer"]',
        'div[class*="DivCommentItemContainer"]',
        'div[class*="CommentItem"]'
    ]
    all_elements = []
    for selector in selectors:
        try:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            for el in elements:
                if el not in all_elements and el.is_displayed():
                    all_elements.append(el)
        except Exception:
            continue
    return all_elements

# ============================================================
# PARSE COMMENT
# ============================================================
def parse_comment_element(element):
    try:
        content = element.find_elements(
            By.CSS_SELECTOR,
            'span[data-e2e="comment-level-1"]'
        )

        if not content:
            return None

        text = clean_text(content[0].text)

        if not text:
            return None

        comment_id = (
            element.get_attribute("data-comment-id")
            or element.get_attribute("data-id")
            or hashlib.md5(text.encode("utf-8")).hexdigest()
        )

        return {
            "comment_id": comment_id,
            "text": text
        }

    except Exception as e:
        logger.debug("Parse comment error: %s", e)
        return None




# ============================================================
# SCROLL COMMENT CONTAINER (đã sửa mạnh)
# ============================================================
def scroll_comment_container(driver):
    try:
        result = driver.execute_script("""
            // 1. Tìm comment bằng nhiều selector
            const selectors = [
                'p[data-e2e="comment-level-1"]',
                '[data-e2e="comment-level-1"]',
                '[data-e2e="comment-item"]',
                'div[class*="CommentItemContainer"]',
                'div[class*="DivCommentItemContainer"]',
                'div[class*="CommentItem"]'
            ];

            let comments = [];
            for (const sel of selectors) {
                comments = Array.from(document.querySelectorAll(sel));
                if (comments.length > 0) break;
            }

            if (comments.length === 0) {
                return "NO_COMMENT";
            }

            // 2. Tìm container scrollable gần nhất
            let el = comments[comments.length - 1];
            for (let i = 0; i < 25 && el; i++) {
                el = el.parentElement;
                if (!el) break;

                const style = window.getComputedStyle(el);
                const overflowY = style.overflowY;
                const scrollHeight = el.scrollHeight;
                const clientHeight = el.clientHeight;

                if ((overflowY === 'auto' || overflowY === 'scroll' || overflowY === 'overlay')
                    && scrollHeight > clientHeight + 80) {

                    // Scroll xuống đáy
                    el.scrollTop = el.scrollHeight;

                    // Trigger wheel event để TikTok load thêm
                    el.dispatchEvent(new WheelEvent('wheel', {
                        deltaY: 900,
                        bubbles: true,
                        cancelable: true
                    }));

                    return "COMMENT_CONTAINER";
                }
            }

            // 3. Fallback: scroll toàn trang + wheel
            window.scrollTo(0, document.body.scrollHeight);
            document.documentElement.scrollTop = document.documentElement.scrollHeight;

            document.body.dispatchEvent(new WheelEvent('wheel', {
                deltaY: 1200,
                bubbles: true
            }));

            return "PAGE";
        """)
        return result
    except Exception as e:
        logger.warning("Scroll container failed: %s", e)
        return "ERROR"

# ============================================================
# CLICK "XEM THÊM" NẾU CÓ
# ============================================================
def click_view_more_comments(driver):
    try:
        # Các nút "Xem thêm" / "View more" thường gặp
        xpaths = [
            '//*[contains(text(), "Xem thêm") or contains(text(), "View more") or contains(text(), "Load more") or contains(text(), "See more")]',
            '//button[contains(., "Xem thêm") or contains(., "View more")]',
            '//div[contains(@class, "ViewMore") or contains(@class, "view-more")]'
        ]
        for xp in xpaths:
            btns = driver.find_elements(By.XPATH, xp)
            for btn in btns:
                if btn.is_displayed():
                    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
                    time.sleep(0.5)
                    driver.execute_script("arguments[0].click();", btn)
                    logger.info("Clicked 'View more' button")
                    time.sleep(1.5)
                    return True
    except Exception:
        pass
    return False

# ============================================================
# GET COMMENTS (đã cải thiện)
# ============================================================
def get_comments(driver, max_comments=50):
    logger.info("======================================")
    logger.info("START COMMENT CRAWLING")
    logger.info("======================================")

    comments = {}

    if not open_comment_panel(driver):
        logger.warning("Could not open/load comment section.")
        return []

    no_new_rounds = 0
    previous_dom_count = 0

    for round_no in range(COMMENT_SCROLL_ROUNDS):
        # Thử click "Xem thêm" trước
        click_view_more_comments(driver)

        elements = find_comment_elements(driver)
        logger.info("Round %d | DOM comments: %d", round_no + 1, len(elements))

        old_count = len(comments)

        for element in elements:
            comment = parse_comment_element(element)
            if not comment:
                continue
            cid = comment["comment_id"]
            if cid not in comments:
                comments[cid] = comment
                logger.info(
                    "Comment %d/%d: %s",
                    len(comments), max_comments, comment["text"][:100]
                )
            if len(comments) >= max_comments:
                break

        if len(comments) >= max_comments:
            logger.info("Reached maximum %d comments.", max_comments)
            break

        # Kiểm tra có comment mới không
        if len(comments) == old_count:
            no_new_rounds += 1
        else:
            no_new_rounds = 0

        if no_new_rounds >= 12:
            logger.info("No new comments after 12 rounds → stop.")
            break

        # Kiểm tra DOM growth
        current_dom_count = len(elements)
        logger.info("DOM growth: %d → %d", previous_dom_count, current_dom_count)
        previous_dom_count = current_dom_count

        # Scroll
        scroll_mode = scroll_comment_container(driver)
        logger.info("Scroll mode: %s", scroll_mode)

        time.sleep(random.uniform(2.8, 4.5))

    result = list(comments.values())[:max_comments]
    logger.info("======================================")
    logger.info("COMMENT RESULT: %d/%d", len(result), max_comments)
    logger.info("======================================")
    return result

# ============================================================
# LOAD KEYWORDS
# ============================================================
def load_keywords():
    if not os.path.exists(KEYWORD_FILE):
        raise FileNotFoundError(f"Missing keyword file: {KEYWORD_FILE}")
    df = pd.read_csv(KEYWORD_FILE)
    required = ["Keyword TikTok", "province", "category"]
    for col in required:
        if col not in df.columns:
            raise ValueError(f"Missing column: {col}")
    return df.fillna("")

# ============================================================
# SAVE JSON
# ============================================================
def save_json(records):
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    logger.info("Saved JSON: %s", OUTPUT_JSON)

# ============================================================
# MAIN
# ============================================================
def main():
    logger.info("======================================")
    logger.info("TikTok metadata + comments crawler")
    logger.info("======================================")

    df = load_keywords()
    driver = create_driver()
    all_records = []

    try:
        for _, row in df.iterrows():
            keyword = str(row["Keyword TikTok"]).strip()
            province = str(row["province"]).strip()
            category = str(row["category"]).strip()

            if not keyword:
                continue

            video_urls = search_video_urls(driver, keyword)
            logger.info("Keyword '%s': %d videos", keyword, len(video_urls))

            for index, video_url in enumerate(video_urls, start=1):
                logger.info("[%d/%d] %s", index, len(video_urls), video_url)
                try:
                    metadata = get_video_metadata(
                        driver, keyword, province, category, video_url
                    )
                    random_sleep()

                    comments = get_comments(driver, MAX_COMMENTS_PER_VIDEO)
                    metadata["comments"] = comments
                    metadata["comments_crawled"] = len(comments)

                    all_records.append(metadata)
                    save_json(all_records)          # lưu ngay sau mỗi video

                    logger.info("Done: %d comments", len(comments))
                    random_sleep()
                except Exception:
                    logger.exception("Failed video: %s", video_url)

    finally:
        driver.quit()

    save_json(all_records)
    logger.info("Finished. Videos: %d", len(all_records))

# ============================================================
# RUN
# ============================================================
if __name__ == "__main__":
    main()