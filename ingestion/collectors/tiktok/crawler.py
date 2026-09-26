import os
import re
import json
import time
import random
import logging
import hashlib
from datetime import datetime
from urllib.parse import quote
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import (
    TimeoutException,
    StaleElementReferenceException,
    ElementClickInterceptedException,
)

# ============================================================
# PATH
# ============================================================
BASE_DIR = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "..",
        ".."
    )
)

TIKTOK_DIR = os.path.join(
    BASE_DIR,
    "ingestion",
    "tiktok"
)

RAW_DIR = os.path.join(
    BASE_DIR,
    "data",
    "raw",
    "tiktok"
)

LOG_DIR = os.path.join(
    BASE_DIR,
    "logs"
)

# === ĐÃ ĐỔI TÊN FILE ===
KEYWORD_FILE = os.path.join(
    TIKTOK_DIR,
    "key_word.csv"          # <-- trước là keyword.csv
)

OUTPUT_JSON = os.path.join(
    RAW_DIR,
    "tiktok_metadata_comments.json"
)

LOG_FILE = os.path.join(
    LOG_DIR,
    "tiktok_comments_crawler.log"
)

# ============================================================
# CONFIG
# ============================================================
MAX_VIDEOS_PER_KEYWORD = 3
MAX_COMMENTS_PER_VIDEO = 10
SEARCH_SCROLL_ROUNDS = 8
COMMENT_SCROLL_ROUNDS = 40
MIN_SLEEP = 2.5
MAX_SLEEP = 5.0

# Thời gian chờ bạn giải CAPTCHA thủ công (giây)
CAPTCHA_WAIT_TIMEOUT = 300          # 5 phút
CAPTCHA_CHECK_INTERVAL = 5          # kiểm tra lại mỗi 5s

# ============================================================
# PROVINCES
# ============================================================
PROVINCES = [
    "An Giang",
    "Bà Rịa - Vũng Tàu",
    "Bắc Ninh",
    "Cà Mau",
    "Cao Bằng",
    "Cần Thơ",
    "Đà Nẵng",
    "Đắk Lắk",
    "Điện Biên",
    "Đồng Nai",
    "Đồng Tháp",
    "Gia Lai",
    "Hà Nội",
    "Hà Tĩnh",
    "Hải Phòng",
    "Hồ Chí Minh",
    "Huế",
    "Hưng Yên",
    "Khánh Hòa",
    "Lai Châu",
    "Lạng Sơn",
    "Lào Cai",
    "Lâm Đồng",
    "Nghệ An",
    "Ninh Bình",
    "Phú Thọ",
    "Quảng Ngãi",
    "Quảng Ninh",
    "Quảng Trị",
    "Sơn La",
    "Tây Ninh",
    "Thanh Hóa",
    "Thái Nguyên",
    "Tuyên Quang",
    "Vĩnh Long",
]

# ============================================================
# CREATE FOLDERS
# ============================================================
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# ============================================================
# LOGGER
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("tiktok_crawler")

# ============================================================
# BASIC UTILS
# ============================================================
def random_sleep(minimum=MIN_SLEEP, maximum=MAX_SLEEP):
    time.sleep(random.uniform(minimum, maximum))


def clean_text(value):
    if value is None:
        return ""
    value = str(value)
    value = value.replace("\ufeff", "")
    value = value.replace("\xa0", " ")
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def parse_count(text):
    if text is None:
        return None
    text = clean_text(text)
    if not text:
        return None

    text_upper = text.upper()
    match = re.search(r"([\d.,]+)\s*([KMB])", text_upper)
    if match:
        number_text = match.group(1)
        suffix = match.group(2)
        try:
            number_text = number_text.replace(",", ".")
            number = float(number_text)
            multiplier = {"K": 1_000, "M": 1_000_000, "B": 1_000_000_000}
            return int(number * multiplier[suffix])
        except Exception:
            return None

    digits = re.sub(r"[^\d]", "", text)
    if not digits:
        return None
    try:
        return int(digits)
    except Exception:
        return None


def make_comment_id(author, text):
    raw = clean_text(author) + "|" + clean_text(text)
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def extract_video_id(url):
    if not url:
        return ""
    match = re.search(r"/video/(\d+)", url)
    if match:
        return match.group(1)
    return ""


def extract_province(keyword):
    keyword = clean_text(keyword)
    for province in sorted(PROVINCES, key=len, reverse=True):
        if keyword.lower().endswith(province.lower()):
            return province
    return ""


# ============================================================
# CAPTCHA HANDLING
# ============================================================
def is_captcha_present(driver):
    """
    Kiểm tra xem có CAPTCHA TikTok đang hiện không.
    """
    captcha_selectors = [
        # Các class/id phổ biến của TikTok captcha
        '[class*="captcha"]',
        '[class*="Captcha"]',
        '[id*="captcha"]',
        '[id*="Captcha"]',
        '.captcha_verify_container',
        '.secsdk-captcha-drag-icon',
        '[class*="secsdk-captcha"]',
        'div[class*="verify"]',
        'iframe[src*="captcha"]',
        # Text tiếng Việt / Anh
        '//*[contains(text(), "Kéo") and contains(text(), "hình")]',
        '//*[contains(text(), "Drag the")]',
        '//*[contains(text(), "Xác minh")]',
        '//*[contains(text(), "Verify")]',
        '//*[contains(text(), "puzzle")]',
    ]

    for selector in captcha_selectors:
        try:
            if selector.startswith("//"):
                elements = driver.find_elements(By.XPATH, selector)
            else:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)

            for el in elements:
                try:
                    if el.is_displayed():
                        return True
                except Exception:
                    continue
        except Exception:
            continue
    return False


def wait_for_captcha_manual(driver, timeout=CAPTCHA_WAIT_TIMEOUT):
    """
    Nếu phát hiện CAPTCHA → chờ người dùng giải thủ công.
    Trả về True nếu CAPTCHA biến mất (đã giải xong), False nếu timeout.
    """
    if not is_captcha_present(driver):
        return True

    logger.warning("=" * 60)
    logger.warning("!!! CAPTCHA DETECTED !!!")
    logger.warning("Hãy kéo hình xác nhận trên trình duyệt.")
    logger.warning(f"Bot sẽ chờ tối đa {timeout} giây...")
    logger.warning("=" * 60)

    # Beep (Windows) hoặc print rõ
    try:
        import winsound
        winsound.Beep(1000, 800)
    except Exception:
        print("\a")  # terminal bell

    start = time.time()
    while time.time() - start < timeout:
        if not is_captcha_present(driver):
            logger.info("CAPTCHA đã được giải. Tiếp tục crawl...")
            time.sleep(2)
            return True
        remaining = int(timeout - (time.time() - start))
        if remaining % 30 == 0:  # log mỗi 30s
            logger.info(f"Vẫn đang chờ CAPTCHA... còn {remaining}s")
        time.sleep(CAPTCHA_CHECK_INTERVAL)

    logger.error("Hết thời gian chờ CAPTCHA. Bỏ qua bước này.")
    return False


# ============================================================
# KEYWORD LOADER
# ============================================================
def clean_cell(value):
    value = str(value).strip()
    value = value.replace("\ufeff", "")
    if len(value) >= 2 and value.startswith('"') and value.endswith('"'):
        value = value[1:-1]
    return value.strip()


def detect_delimiter(header):
    candidates = ["\t", ",", ";", "|"]
    best = None
    best_count = 0
    for delimiter in candidates:
        count = header.count(delimiter)
        if count > best_count:
            best = delimiter
            best_count = count
    return best


def load_keywords():
    if not os.path.exists(KEYWORD_FILE):
        raise FileNotFoundError(f"Missing keyword file: {KEYWORD_FILE}")

    logger.info("Loading keyword file: %s", KEYWORD_FILE)

    with open(KEYWORD_FILE, "r", encoding="utf-8-sig", errors="replace") as file:
        lines = [line.rstrip("\r\n") for line in file]

    lines = [line for line in lines if line.strip()]
    if not lines:
        raise ValueError("Keyword file rong.")

    delimiter = detect_delimiter(lines[0])
    if delimiter is None:
        raise ValueError("Khong xac dinh duoc delimiter.")

    header = [clean_cell(x) for x in lines[0].split(delimiter)]
    header_lower = [x.lower() for x in header]
    logger.info("Columns: %s", header)

    keyword_index = None
    for i, column in enumerate(header_lower):
        if column in ["key_word", "keyword", "keyword tiktok", "key word"]:
            keyword_index = i
            break

    if keyword_index is None:
        for i, column in enumerate(header_lower):
            if "key" in column and "word" in column:
                keyword_index = i
                break

    if keyword_index is None:
        raise ValueError("Khong tim thay cot keyword.")

    province_index = None
    category_index = None
    for i, column in enumerate(header_lower):
        if column == "province":
            province_index = i
        if column == "category":
            category_index = i

    records = []
    for line_number, line in enumerate(lines[1:], start=2):
        try:
            parts = line.split(delimiter)
            if keyword_index >= len(parts):
                continue

            if keyword_index == len(header) - 1:
                keyword = delimiter.join(parts[keyword_index:])
            else:
                keyword = parts[keyword_index]

            keyword = clean_cell(keyword)
            if not keyword:
                continue

            if province_index is not None and province_index < len(parts):
                province = clean_cell(parts[province_index])
            else:
                province = extract_province(keyword)

            if category_index is not None and category_index < len(parts):
                category = clean_cell(parts[category_index])
            else:
                category = "travel"

            records.append({
                "keyword": keyword,
                "province": province,
                "category": category
            })
        except Exception as e:
            logger.warning("Cannot parse line %d: %s", line_number, e)

    logger.info("Loaded %d keywords", len(records))
    for item in records[:5]:
        logger.info(
            "keyword='%s' | province='%s' | category='%s'",
            item["keyword"], item["province"], item["category"]
        )
    return records


# ============================================================
# CREATE DRIVER
# ============================================================
def create_driver():
    """
    Ưu tiên dùng undetected_chromedriver nếu có,
    giảm khả năng bị TikTok phát hiện bot.
    """
    try:
        import undetected_chromedriver as uc
        options = uc.ChromeOptions()
        options.add_argument("--start-maximized")
        options.add_argument("--disable-notifications")
        options.add_argument("--disable-popup-blocking")
        options.add_argument("--lang=vi-VN")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-sandbox")
        # Không headless vì cần nhìn CAPTCHA
        driver = uc.Chrome(options=options)
        logger.info("Using undetected_chromedriver")
    except Exception as e:
        logger.warning("undetected_chromedriver không có, dùng selenium thường: %s", e)
        options = Options()
        options.add_argument("--start-maximized")
        options.add_argument("--disable-notifications")
        options.add_argument("--disable-popup-blocking")
        options.add_argument("--lang=vi-VN")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-sandbox")
        driver = webdriver.Chrome(options=options)

    driver.set_page_load_timeout(60)
    return driver


# ============================================================
# SEARCH VIDEOS
# ============================================================
def search_video_urls(driver, keyword):
    logger.info("Searching: %s", keyword)
    url = "https://www.tiktok.com/search?q=" + quote(keyword)

    try:
        driver.get(url)
    except Exception as e:
        logger.error("Cannot open search page: %s", e)
        return []

    time.sleep(5)
    wait_for_captcha_manual(driver)   # <-- kiểm tra CAPTCHA ngay sau khi vào search

    video_urls = []
    unchanged = 0
    previous_count = 0

    for round_number in range(SEARCH_SCROLL_ROUNDS):
        try:
            if is_captcha_present(driver):
                if not wait_for_captcha_manual(driver):
                    break

            elements = driver.find_elements(By.CSS_SELECTOR, 'a[href*="/video/"]')
            for element in elements:
                try:
                    href = element.get_attribute("href")
                    if not href:
                        continue
                    href = href.split("?")[0]
                    if not re.search(r"/video/\d+", href):
                        continue
                    if href not in video_urls:
                        video_urls.append(href)
                    if len(video_urls) >= MAX_VIDEOS_PER_KEYWORD:
                        logger.info("Found %d videos.", MAX_VIDEOS_PER_KEYWORD)
                        return video_urls[:MAX_VIDEOS_PER_KEYWORD]
                except StaleElementReferenceException:
                    continue

            if len(video_urls) == previous_count:
                unchanged += 1
            else:
                unchanged = 0
            previous_count = len(video_urls)

            if unchanged >= 3:
                break

            driver.execute_script(
                "window.scrollBy(0, arguments[0]);",
                random.randint(1200, 2200)
            )
            time.sleep(random.uniform(2.2, 3.5))
        except Exception as e:
            logger.warning("Search round error: %s", e)

    return video_urls[:MAX_VIDEOS_PER_KEYWORD]


# ============================================================
# FIND TEXT
# ============================================================
def find_first_text(driver, selectors):
    for selector in selectors:
        try:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            for element in elements:
                try:
                    text = clean_text(element.text)
                    if text:
                        return text
                except Exception:
                    continue
        except Exception:
            continue
    return ""


# ============================================================
# VIDEO METADATA
# ============================================================
def get_video_metadata(driver, video_url, keyword, province, category):
    video_id = extract_video_id(video_url)
    logger.info("Opening video: %s", video_id)

    try:
        driver.get(video_url)
    except Exception as e:
        logger.warning("Cannot load video: %s", e)

    time.sleep(5)
    wait_for_captcha_manual(driver)   # <-- kiểm tra CAPTCHA sau khi mở video

    try:
        WebDriverWait(driver, 10).until(
            lambda d: d.find_elements(By.TAG_NAME, "body")
        )
    except TimeoutException:
        pass

    author = find_first_text(driver, [
        'a[data-e2e="browse-username"]',
        'a[data-e2e="video-author-uniqueid"]',
        'a[href*="/@"]'
    ])

    description = find_first_text(driver, [
        '[data-e2e="browse-video-desc"]',
        'h1[data-e2e="browse-video-desc"]',
        '[data-e2e="video-desc"]'
    ])

    like_text = find_first_text(driver, [
        '[data-e2e="browse-like-count"]',
        '[data-e2e="like-count"]'
    ])

    comment_text = find_first_text(driver, [
        '[data-e2e="browse-comment-count"]',
        '[data-e2e="comment-count"]'
    ])

    share_text = find_first_text(driver, [
        '[data-e2e="share-count"]',
        '[data-e2e="browse-share-count"]'
    ])

    view_text = find_first_text(driver, [
        '[data-e2e="browse-view-count"]',
        '[data-e2e="view-count"]'
    ])

    hashtags = []
    try:
        elements = driver.find_elements(By.CSS_SELECTOR, 'a[href*="/tag/"]')
        for element in elements:
            text = clean_text(element.text)
            if text:
                if not text.startswith("#"):
                    text = "#" + text
                if text not in hashtags:
                    hashtags.append(text)
    except Exception:
        pass

    posted_date = None
    try:
        elements = driver.find_elements(By.CSS_SELECTOR, 'meta[itemprop="uploadDate"]')
        if elements:
            posted_date = elements[0].get_attribute("content")
    except Exception:
        pass

    if not posted_date:
        try:
            elements = driver.find_elements(By.CSS_SELECTOR, 'meta[property="article:published_time"]')
            if elements:
                posted_date = elements[0].get_attribute("content")
        except Exception:
            pass

    return {
        "video_id": video_id,
        "video_url": video_url,
        "keyword": keyword,
        "province": province,
        "category": category,
        "author": author,
        "description": description,
        "hashtags": hashtags,
        "view_count": parse_count(view_text),
        "like_count": parse_count(like_text),
        "comment_count": parse_count(comment_text),
        "share_count": parse_count(share_text),
        "posted_date": posted_date,
        "crawl_time": datetime.now().isoformat(),
        "comments": []
    }


# ============================================================
# OPEN COMMENT PANEL
# ============================================================
def open_comment_panel(driver):
    logger.info("Opening comment panel...")

    if is_captcha_present(driver):
        if not wait_for_captcha_manual(driver):
            return False

    selectors = [
        '[data-e2e="comment-icon"]',
        '[data-e2e="comment-icon"] button',
        'button[aria-label*="comment" i]',
        'button[aria-label*="bình luận" i]',
    ]

    for selector in selectors:
        try:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            for element in elements:
                try:
                    driver.execute_script(
                        "arguments[0].scrollIntoView({block: 'center'});",
                        element
                    )
                    time.sleep(1)
                    driver.execute_script("arguments[0].click();", element)
                    time.sleep(3)
                    return True
                except Exception:
                    continue
        except Exception:
            continue

    logger.warning("Comment button not found.")
    return False


# ============================================================
# FIND COMMENT TEXT ELEMENTS  (đã cải thiện)
# ============================================================
def find_comment_elements(driver):
    # Ưu tiên selector chính thức + một số class hay gặp hiện tại
    selectors = [
        'div[data-e2e="comment-item"]',
        'div[class*="CommentItemContainer"]',
        'div[class*="DivCommentObjectWrapper"]',
        'div[class*="CommentItem"]',
        'div[class*="CommentObject"]',
        'p[data-e2e^="comment-level-"]',          # fallback cũ
    ]

    results = []
    seen = set()

    for selector in selectors:
        try:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            for el in elements:
                try:
                    # tạo key để tránh lấy trùng element
                    key = el.id or (el.get_attribute("outerHTML") or "")[:120]
                except Exception:
                    key = str(id(el))
                if key not in seen:
                    seen.add(key)
                    results.append(el)
        except Exception:
            continue

    return results


# ============================================================
# PARSE COMMENT  (đã sửa sạch sẽ)
# ============================================================
def parse_comment_element(element):
    try:
        # --- 1. Author ---
        author = ""
        try:
            author_els = element.find_elements(
                By.CSS_SELECTOR,
                'a[data-e2e*="comment-username"], a[href*="/@"]'
            )
            if author_els:
                author = clean_text(author_els[0].text)
        except Exception:
            pass

        # --- 2. Comment text (ưu tiên selector sạch) ---
        text = ""
        text_selectors = [
            'p[data-e2e^="comment-level-"]',
            'span[data-e2e^="comment-level-"]',
            '[data-e2e="comment-level-1"]',
            '[data-e2e="comment-level-2"]',
            'div[class*="CommentText"]',
            'span[class*="CommentText"]',
            'p[class*="Comment"]',
            'span[class*="SpanText"]',
        ]
        for sel in text_selectors:
            try:
                text_els = element.find_elements(By.CSS_SELECTOR, sel)
                for te in text_els:
                    t = clean_text(te.text)
                    if t and t not in {"Reply", "Trả lời", "Like", "Thích", "Xem bản dịch", "See translation"}:
                        text = t
                        break
                if text:
                    break
            except Exception:
                continue

        # --- 3. Fallback: lấy raw text rồi làm sạch ---
        if not text:
            raw = clean_text(element.text)
            if not raw:
                return None

            # Bỏ author ở đầu nếu có
            if author and raw.startswith(author):
                raw = raw[len(author):].strip()

            # Loại bỏ các phần thừa phổ biến ở cuối (ngày, Trả lời X, Like...)
            raw = re.sub(r'\s+\d{1,2}-\d{1,2}(\s+\d{4})?\s*$', '', raw)          # 1-25 hoặc 12-29 2025
            raw = re.sub(r'\s+\d{4}-\d{2}-\d{2}\s*$', '', raw)                     # 2025-12-29
            raw = re.sub(r'\s*(Trả lời|Reply)\s*\d*\s*$', '', raw, flags=re.I)    # Trả lời 1 / Reply 0
            raw = re.sub(r'\s*(Thích|Like)\s*\d*\s*$', '', raw, flags=re.I)
            raw = re.sub(r'\s+\d+\s*$', '', raw)                                   # số cuối cùng lẻ
            text = clean_text(raw)

        if not text or len(text) < 2:
            return None

        # Bỏ các comment rác
        invalid = {
            "Reply", "Trả lời", "Like", "Thích",
            "Xem bản dịch", "See translation",
            "Xem thêm", "See more"
        }
        if text in invalid:
            return None

        # --- 4. Like count ---
        like_count = None
        try:
            like_els = element.find_elements(
                By.CSS_SELECTOR,
                '[data-e2e*="like-count"], [data-e2e*="comment-like"], [class*="like"]'
            )
            for le in like_els:
                lt = clean_text(le.text)
                if lt:
                    like_count = parse_count(lt)
                    if like_count is not None:
                        break
        except Exception:
            pass

        comment_id = make_comment_id(author, text)

        return {
            "comment_id": comment_id,
            "author": author,
            "text": text,
            "like_count": like_count,
            "posted_time": None
        }

    except Exception:
        return None


# ============================================================
# SCROLL COMMENT CONTAINER
# ============================================================
def scroll_comment_container(driver):
    try:
        containers = driver.find_elements(
            By.XPATH,
            "//div[.//*[contains(@data-e2e, 'comment')]]"
        )
        best_container = None
        best_height = 0

        for container in containers:
            try:
                height = driver.execute_script("return arguments[0].scrollHeight;", container)
                client_height = driver.execute_script("return arguments[0].clientHeight;", container)
                if height and client_height and height > client_height + 100:
                    if height > best_height:
                        best_height = height
                        best_container = container
            except Exception:
                continue

        if best_container:
            driver.execute_script(
                "arguments[0].scrollTop = arguments[0].scrollHeight;",
                best_container
            )
            return True
    except Exception:
        pass

    try:
        elements = driver.find_elements(By.CSS_SELECTOR, "div")
        for element in elements:
            try:
                scroll_height = driver.execute_script("return arguments[0].scrollHeight;", element)
                client_height = driver.execute_script("return arguments[0].clientHeight;", element)
                if scroll_height and client_height and scroll_height > client_height + 300:
                    driver.execute_script(
                        "arguments[0].scrollTop = arguments[0].scrollHeight;",
                        element
                    )
                    return True
            except Exception:
                continue
    except Exception:
        pass

    try:
        driver.execute_script("window.scrollBy(0, 800);")
        return True
    except Exception:
        return False


# ============================================================
# GET COMMENTS
# ============================================================
def get_comments(driver):
    logger.info("Start collecting comments...")
    comments = []
    seen_ids = set()

    if not open_comment_panel(driver):
        logger.warning("Cannot open comment panel.")
        return []

    time.sleep(4)
    wait_for_captcha_manual(driver)   # <-- kiểm tra lại sau khi mở panel

    no_new_rounds = 0

    for round_number in range(COMMENT_SCROLL_ROUNDS):
        logger.info("Comment round %d/%d", round_number + 1, COMMENT_SCROLL_ROUNDS)

        if is_captcha_present(driver):
            if not wait_for_captcha_manual(driver):
                break

        old_count = len(comments)
        elements = find_comment_elements(driver)
        logger.info("Found %d possible comment elements.", len(elements))

        for element in elements:
            if len(comments) >= MAX_COMMENTS_PER_VIDEO:
                break
            try:
                comment = parse_comment_element(element)
                if not comment:
                    continue
                comment_id = comment["comment_id"]
                if comment_id in seen_ids:
                    continue
                seen_ids.add(comment_id)
                comments.append(comment)
                logger.info(
                    "Comment %d/%d | %s | %s",
                    len(comments), MAX_COMMENTS_PER_VIDEO,
                    comment.get("author", "")[:30],
                    comment["text"][:80]
                )
            except StaleElementReferenceException:
                continue
            except Exception as e:
                logger.debug("Comment parse error: %s", e)

        if len(comments) >= MAX_COMMENTS_PER_VIDEO:
            logger.info("Reached %d comments.", MAX_COMMENTS_PER_VIDEO)
            break

        if len(comments) == old_count:
            no_new_rounds += 1
        else:
            no_new_rounds = 0

        if no_new_rounds >= 6:
            logger.info("No new comments.")
            break

        scroll_comment_container(driver)
        time.sleep(random.uniform(1.8, 3.0))

    logger.info("Collected %d/%d comments.", len(comments), MAX_COMMENTS_PER_VIDEO)
    return comments[:MAX_COMMENTS_PER_VIDEO]


# ============================================================
# SAVE / LOAD JSON
# ============================================================
def save_json(data):
    temp_file = OUTPUT_JSON + ".tmp"
    with open(temp_file, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)
    os.replace(temp_file, OUTPUT_JSON)


def load_existing_data():
    if not os.path.exists(OUTPUT_JSON):
        return []
    try:
        with open(OUTPUT_JSON, "r", encoding="utf-8") as file:
            data = json.load(file)
        if isinstance(data, list):
            return data
    except Exception as e:
        logger.warning("Cannot load old JSON: %s", e)
    return []


# ============================================================
# MAIN
# ============================================================
def main():
    logger.info("=" * 70)
    logger.info("START TIKTOK CRAWLER")
    logger.info("Videos / keyword = %d", MAX_VIDEOS_PER_KEYWORD)
    logger.info("Comments / video = %d", MAX_COMMENTS_PER_VIDEO)
    logger.info("=" * 70)

    keywords = load_keywords()
    if not keywords:
        logger.error("No keywords.")
        return

    all_data = load_existing_data()
    crawled_videos = set()
    for item in all_data:
        video_id = item.get("video_id")
        keyword = item.get("keyword")
        if video_id:
            crawled_videos.add((keyword, video_id))

    logger.info("Existing videos: %d", len(crawled_videos))

    driver = create_driver()

    try:
        for keyword_index, item in enumerate(keywords, start=1):
            keyword = item["keyword"]
            province = item["province"]
            category = item["category"]

            logger.info("")
            logger.info("=" * 70)
            logger.info("KEYWORD %d/%d", keyword_index, len(keywords))
            logger.info("Keyword: %s", keyword)
            logger.info("Province: %s", province)
            logger.info("=" * 70)

            video_urls = search_video_urls(driver, keyword)
            logger.info("Found %d videos.", len(video_urls))

            for video_index, video_url in enumerate(video_urls, start=1):
                video_id = extract_video_id(video_url)
                logger.info("")
                logger.info("VIDEO %d/%d", video_index, len(video_urls))
                logger.info("ID: %s", video_id)

                if (keyword, video_id) in crawled_videos:
                    logger.info("Already crawled -> skip.")
                    continue

                try:
                    metadata = get_video_metadata(
                        driver, video_url, keyword, province, category
                    )
                    if not metadata:
                        continue

                    comments = get_comments(driver)
                    metadata["comments"] = comments

                    all_data.append(metadata)
                    crawled_videos.add((keyword, video_id))
                    save_json(all_data)

                    logger.info(
                        "SAVED | video=%s | comments=%d",
                        video_id, len(comments)
                    )
                    random_sleep()
                except Exception as e:
                    logger.exception("Video error %s: %s", video_id, e)
                    continue

    except KeyboardInterrupt:
        logger.warning("Stopped by user.")
    except Exception as e:
        logger.exception("Crawler error: %s", e)
    finally:
        try:
            driver.quit()
        except Exception:
            pass
        logger.info("")
        logger.info("=" * 70)
        logger.info("CRAWLER FINISHED")
        logger.info("Total records: %d", len(all_data))
        logger.info("Output: %s", OUTPUT_JSON)
        logger.info("=" * 70)


if __name__ == "__main__":
    main()