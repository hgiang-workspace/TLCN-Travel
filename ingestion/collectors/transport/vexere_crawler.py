import csv
import json
import logging
import re
import time
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    WebDriverException,
    TimeoutException,
    InvalidSessionIdException,
)


# ============================================================
# PATH
# ============================================================

CURRENT_DIR = Path(__file__).resolve().parent

ROUTE_FILE = CURRENT_DIR / "transport_routes.csv"
OUTPUT_FILE = CURRENT_DIR / "data" / "raw" / "transport" / "vexere_transport.json"
LOG_FILE = CURRENT_DIR / "logs" / "vexere_crawler.log"
DEBUG_DIR = CURRENT_DIR / "logs" / "debug_html"


# ============================================================
# CONFIG
# ============================================================

WAIT_TIME = 5
MAX_RECORDS_PER_ROUTE = 10
MAX_LOAD_MORE_CLICKS = 10
SOURCE = "vexere"

# QUAN TRỌNG: crawl NGÀY MAI, không phải hôm nay
CRAWL_DATE = (datetime.now() + timedelta(days=1)).strftime("%d-%m-%Y")

TICKET_WAIT_TIMEOUT = 20
ROUTE_DELAY = 3


# ============================================================
# LOGGING
# ============================================================

LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
DEBUG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


# ============================================================
# LOAD ROUTES
# ============================================================

def load_routes():
    if not ROUTE_FILE.exists():
        raise FileNotFoundError(f"Không tìm thấy file route: {ROUTE_FILE}")

    routes = []
    with open(ROUTE_FILE, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        required = {"origin", "destination", "url"}
        if not required.issubset(set(reader.fieldnames or [])):
            raise ValueError("CSV phải có các cột: origin,destination,url")

        for row in reader:
            origin = (row.get("origin") or "").strip()
            destination = (row.get("destination") or "").strip()
            url = (row.get("url") or "").strip()
            ttype = (row.get("transport_type") or "bus").strip().lower()

            if not origin or not destination or not url:
                continue
            if ttype not in ("bus", "flight", "train"):
                ttype = "bus"

            routes.append({
                "origin": origin,
                "destination": destination,
                "url": url,
                "transport_type": ttype,
            })

    logger.info(f"Đã đọc {len(routes)} tuyến từ CSV")
    return routes


# ============================================================
# CREATE DRIVER
# ============================================================

def create_driver():
    options = Options()
    options.add_argument("--start-maximized")
    options.add_argument("--disable-notifications")
    options.add_argument("--lang=vi-VN")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_experimental_option("excludeSwitches", ["enable-logging", "enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

    # options.add_argument("--headless=new")
    # options.add_argument("--window-size=1920,1080")

    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(90)
    driver.set_script_timeout(30)
    try:
        driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {
                "source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
            },
        )
    except Exception:
        pass
    return driver


def safe_quit(driver):
    try:
        if driver:
            driver.quit()
    except Exception:
        pass


# ============================================================
# BUILD URL
# ============================================================

def build_vexere_url(base_url, crawl_date, transport_type="bus"):
    """
    Bus: date=DD-MM-YYYY + lt=1
    Flight/Train (dat-ve-*.vi): date[depart]=YYYY-MM-DD
    """
    parsed = urlparse(base_url)
    query = parse_qs(parsed.query, keep_blank_values=True)

    # Xóa date cũ (cả 2 dạng)
    for key in list(query.keys()):
        if key in ("date", "lt", "lf") or key.startswith("date["):
            query.pop(key, None)

    if transport_type == "bus":
        query["date"] = [crawl_date]  # DD-MM-YYYY
        query["lt"] = ["1"]
    else:
        # Flight / Train dùng ISO date
        # crawl_date đang là DD-MM-YYYY → chuyển YYYY-MM-DD
        try:
            d, m, y = crawl_date.split("-")
            iso = f"{y}-{m}-{d}"
        except Exception:
            iso = crawl_date
        query["date[depart]"] = [iso]
        # đảm bảo passenger mặc định
        if "passenger[adt]" not in query:
            query["passenger[adt]"] = ["1"]
        if "passenger[chd]" not in query:
            query["passenger[chd]"] = ["0"]
        if transport_type == "flight":
            if "passenger[inf]" not in query:
                query["passenger[inf]"] = ["0"]
            if "fare_class" not in query:
                query["fare_class"] = ["PT"]
        else:  # train
            for k, v in (
                ("passenger[eld]", "0"),
                ("passenger[stu]", "0"),
                ("passenger[uni]", "0"),
            ):
                if k not in query:
                    query[k] = [v]

    new_query = urlencode(query, doseq=True)
    return urlunparse((
        parsed.scheme, parsed.netloc, parsed.path,
        parsed.params, new_query, parsed.fragment,
    ))


# ============================================================
# PARSE HELPERS
# ============================================================

def parse_price(text):
    if not text:
        return None
    text = text.strip()
    matches = re.findall(r"(\d+(?:[.,]\d+)*)\s*(?:đ|₫|vnd)", text, flags=re.IGNORECASE)
    if matches:
        try:
            return int(matches[0].replace(".", "").replace(",", ""))
        except ValueError:
            pass
    numbers = re.findall(r"\d+(?:[.,]\d+)*", text)
    if numbers:
        try:
            return int(numbers[0].replace(".", "").replace(",", ""))
        except ValueError:
            pass
    return None


def parse_duration(text):
    if not text:
        return None
    text = text.lower().strip()
    hours = minutes = 0
    hour_match = re.search(r"(\d+)\s*h", text)
    minute_match = re.search(r"(\d+)\s*(?:m|p|phút)", text)
    if hour_match:
        hours = int(hour_match.group(1))
    if minute_match:
        minutes = int(minute_match.group(1))
    if hour_match or minute_match:
        return hours * 60 + minutes
    number_match = re.search(r"\d+", text)
    if number_match:
        return int(number_match.group())
    return None


def is_time(text):
    if not text:
        return False
    return bool(re.fullmatch(r"\d{1,2}:\d{2}", text.strip()))


# ============================================================
# PRICE FROM TICKET
# ============================================================

def extract_price_from_ticket(ticket):
    for sel in (".fare-sale", ".fareSmall"):
        try:
            for el in ticket.find_elements(By.CSS_SELECTOR, sel):
                text = el.text.strip()
                if text:
                    price = parse_price(text)
                    if price is not None:
                        return price
        except Exception:
            pass

    try:
        price_elements = ticket.find_elements(
            By.XPATH,
            ".//*[contains(text(), 'đ') or contains(text(), 'Đ') "
            "or contains(text(), '₫') or contains(translate(text(), 'vnd', 'VND'), 'VND')]",
        )
        for el in price_elements:
            text = el.text.strip()
            if text:
                price = parse_price(text)
                if price is not None:
                    return price
    except Exception:
        pass

    try:
        return parse_price(ticket.text.strip())
    except Exception:
        pass
    return None


# ============================================================
# POPUPS
# ============================================================

def close_popups(driver):
    try:
        for xpath in [
            "//button[contains(., 'Chấp nhận tất cả')]",
            "//button[contains(., 'Chấp nhận')]",
            "//button[contains(., 'Đồng ý')]",
            "//button[contains(., 'Accept')]",
            "//button[contains(., 'Đóng')]",
        ]:
            for b in driver.find_elements(By.XPATH, xpath):
                try:
                    if b.is_displayed():
                        b.click()
                        time.sleep(0.4)
                except Exception:
                    pass

        for sel in [
            "button[aria-label='Close']",
            ".modal-close",
            "[class*='close-btn']",
            "[class*='popup'] button",
        ]:
            for el in driver.find_elements(By.CSS_SELECTOR, sel):
                try:
                    if el.is_displayed():
                        el.click()
                        time.sleep(0.3)
                except Exception:
                    pass
    except Exception:
        pass


# ============================================================
# WAIT / COUNT / LOAD MORE
# ============================================================

def wait_for_tickets(driver, timeout=TICKET_WAIT_TIMEOUT):
    end = time.time() + timeout
    while time.time() < end:
        try:
            tickets = driver.find_elements(By.CSS_SELECTOR, "div.ticket")
            if len(tickets) >= 1:
                return True

            body = driver.find_element(By.TAG_NAME, "body").text
            if re.search(r"Kết quả[:\s]*\d+", body, re.IGNORECASE):
                time.sleep(1.5)
                tickets = driver.find_elements(By.CSS_SELECTOR, "div.ticket")
                if len(tickets) >= 1:
                    return True
                if re.search(r"Kết quả[:\s]*0\s*chuyến", body, re.IGNORECASE):
                    return False

            if any(kw in body for kw in (
                "Không tìm thấy",
                "không có chuyến",
                "Chưa có chuyến",
                "Xin lỗi bạn",
            )):
                return False
        except Exception:
            pass
        time.sleep(0.8)
    return False


def count_tickets(driver):
    try:
        return len(driver.find_elements(By.CSS_SELECTOR, "div.ticket"))
    except Exception:
        return 0


def click_load_more_trips(driver):
    xpaths = [
        "//button[contains(normalize-space(.), 'Xem thêm chuyến')]",
        "//div[contains(normalize-space(.), 'Xem thêm chuyến')]",
        "//span[contains(normalize-space(.), 'Xem thêm chuyến')]",
        "//a[contains(normalize-space(.), 'Xem thêm chuyến')]",
    ]
    for xpath in xpaths:
        try:
            for element in driver.find_elements(By.XPATH, xpath):
                try:
                    if not element.is_displayed() or not element.is_enabled():
                        continue
                    driver.execute_script(
                        "arguments[0].scrollIntoView({behavior:'instant',block:'center'});",
                        element,
                    )
                    time.sleep(0.4)
                    before = count_tickets(driver)
                    driver.execute_script("arguments[0].click();", element)
                    logger.info(f"Đã click 'Xem thêm chuyến' (trước: {before})")
                    time.sleep(2)
                    try:
                        WebDriverWait(driver, 5).until(
                            lambda d: count_tickets(d) > before
                        )
                    except Exception:
                        pass
                    logger.info(f"Ticket sau click: {count_tickets(driver)}")
                    return True
                except Exception:
                    continue
        except Exception:
            continue
    return False


def load_more_trips_until_limit(driver):
    for i in range(1, MAX_LOAD_MORE_CLICKS + 1):
        current = count_tickets(driver)
        logger.info(f"Ticket hiện tại: {current}/{MAX_RECORDS_PER_ROUTE}")
        if current >= MAX_RECORDS_PER_ROUTE:
            break
        if not click_load_more_trips(driver):
            logger.info("Không còn nút 'Xem thêm chuyến'")
            break
        logger.info(f"Click load-more lần {i}")
    logger.info(f"Tổng ticket cuối: {count_tickets(driver)}")


# ============================================================
# DEBUG DUMP
# ============================================================

def dump_debug_html(driver, origin, destination):
    try:
        safe_name = re.sub(r"[^\w\-]", "_", f"{origin}_{destination}")[:80]
        path = DEBUG_DIR / f"{safe_name}.html"
        with open(path, "w", encoding="utf-8") as f:
            f.write(driver.page_source)
        logger.info(f"Đã lưu HTML debug: {path}")
    except Exception as e:
        logger.warning(f"Không lưu được debug HTML: {e}")


# ============================================================
# EXTRACT BUS
# ============================================================

def extract_bus_tickets(driver, origin, destination, route_url):
    tickets = driver.find_elements(By.CSS_SELECTOR, "div.ticket")
    logger.info(f"Tìm thấy {len(tickets)} ticket bus")

    records = []
    seen = set()

    for index, ticket in enumerate(tickets, start=1):
        try:
            operator_name = ""
            try:
                operator_name = ticket.find_element(By.CSS_SELECTOR, ".bus-name").text.strip()
            except Exception:
                pass

            vehicle_type = ""
            try:
                vehicle_type = ticket.find_element(By.CSS_SELECTOR, ".seat-type").text.strip()
            except Exception:
                pass

            departure_time = ""
            try:
                departure_time = ticket.find_element(
                    By.CSS_SELECTOR, ".from-to .content.from .hour"
                ).text.strip()
            except Exception:
                try:
                    hours = ticket.find_elements(By.CSS_SELECTOR, ".hour")
                    if hours:
                        departure_time = hours[0].text.strip()
                except Exception:
                    pass

            arrival_time = ""
            try:
                arrival_time = ticket.find_element(
                    By.CSS_SELECTOR, ".from-to .content.to .hour"
                ).text.strip()
            except Exception:
                try:
                    hours = ticket.find_elements(By.CSS_SELECTOR, ".hour")
                    if len(hours) >= 2:
                        arrival_time = hours[1].text.strip()
                except Exception:
                    pass

            duration_text = ""
            try:
                duration_text = ticket.find_element(By.CSS_SELECTOR, ".duration").text.strip()
            except Exception:
                pass

            duration_min = parse_duration(duration_text)
            price_vnd = extract_price_from_ticket(ticket)

            if not is_time(departure_time):
                logger.warning(f"Ticket {index}: thiếu giờ đi ({departure_time!r})")
                continue

            if not operator_name:
                operator_name = "Unknown"
            if not vehicle_type:
                vehicle_type = "Unknown"

            unique_key = (
                origin, destination, operator_name, vehicle_type,
                departure_time, arrival_time, price_vnd,
            )
            if unique_key in seen:
                continue
            seen.add(unique_key)

            record = {
                "origin": origin,
                "destination": destination,
                "operator_name": operator_name,
                "transport_type": "bus",
                "vehicle_type": vehicle_type,
                "departure_time": departure_time,
                "arrival_time": arrival_time or "",
                "duration_min": duration_min,
                "price_vnd": price_vnd,
                "crawl_date": CRAWL_DATE,
                "source": SOURCE,
                "source_url": route_url,
            }
            records.append(record)

            price_str = f"{price_vnd:,} VND" if price_vnd else "N/A"
            logger.info(
                f"[{len(records)}] {operator_name} | {vehicle_type} | "
                f"{departure_time} -> {arrival_time} | {price_str}"
            )

            if len(records) >= MAX_RECORDS_PER_ROUTE:
                break
        except Exception as e:
            logger.warning(f"Lỗi parse ticket {index}: {e}")

    return records


# ============================================================
# CLICK SEARCH (flight / train landing pages)
# ============================================================

def click_search_button(driver):
    """Click nút Tìm kiếm trên trang flight/train."""
    try:
        for xpath in [
            "//button[normalize-space()='Tìm kiếm']",
            "//button[contains(., 'Tìm kiếm')]",
            "//button[contains(@class,'search')]",
        ]:
            btns = driver.find_elements(By.XPATH, xpath)
            for b in btns:
                try:
                    if b.is_displayed() and b.is_enabled():
                        driver.execute_script("arguments[0].click();", b)
                        logger.info("Đã click nút Tìm kiếm")
                        time.sleep(5)
                        return True
                except Exception:
                    continue
    except Exception as e:
        logger.warning(f"Không click được Tìm kiếm: {e}")
    return False


# ============================================================
# EXTRACT TRAIN
# ============================================================

def extract_train_tickets(driver, origin, destination, route_url):
    """
    Selector: [class*="TrainTicketItem__Container"]
    Text mẫu:
        21:45  Ga Sài Gòn  7h 55p  05:40  Ga Nha Trang  Tàu SNT2  Ngồi mềm Từ 367K
    """
    selectors = [
        '[class*="TrainTicketItem__Container"]',
        '[class*="TrainTicketItem__Wrapper"]',
    ]
    cards = []
    used = None
    for sel in selectors:
        found = driver.find_elements(By.CSS_SELECTOR, sel)
        if found:
            cards = found
            used = sel
            break

    if not cards:
        logger.warning(f"TRAIN | {origin} -> {destination}: không thấy TrainTicketItem")
        return []

    logger.info(f"Tìm thấy {len(cards)} train card | selector={used}")
    records = []
    seen = set()

    for index, card in enumerate(cards, start=1):
        try:
            full = card.text.strip()
            if len(full) < 20:
                continue

            times = re.findall(r"\b(\d{1,2}:\d{2})\b", full)
            departure_time = times[0] if times else ""
            arrival_time = times[1] if len(times) >= 2 else ""

            # Tàu SE2 / SNT2 / TN1 ...
            train_match = re.search(r"\b(?:Tàu\s+)?(SE\d+|SNT\d+|TN\d+|SPT\d+)\b", full, re.I)
            operator_name = f"Tàu {train_match.group(1).upper()}" if train_match else "Vietnam Railways"

            # Hạng ghế + giá thấp nhất "Từ 367K" hoặc "367.000đ"
            vehicle_type = "Unknown"
            for kw in ("Ngồi mềm", "Ngồi cứng", "Giường khoang 4", "Giường khoang 6", "Giường nằm"):
                if kw.lower() in full.lower():
                    vehicle_type = kw
                    break

            price_vnd = None
            # Ưu tiên "Từ xxxK"
            m = re.search(r"[Tt]ừ\s*(\d+(?:[.,]\d+)?)\s*[Kk]", full)
            if m:
                try:
                    price_vnd = int(float(m.group(1).replace(",", ".")) * 1000)
                except ValueError:
                    pass
            if price_vnd is None:
                price_vnd = parse_price(full)

            duration_min = parse_duration(full)

            if not is_time(departure_time):
                continue

            key = (origin, destination, operator_name, vehicle_type, departure_time, arrival_time, price_vnd)
            if key in seen:
                continue
            seen.add(key)

            record = {
                "origin": origin,
                "destination": destination,
                "operator_name": operator_name,
                "transport_type": "train",
                "vehicle_type": vehicle_type,
                "departure_time": departure_time,
                "arrival_time": arrival_time,
                "duration_min": duration_min,
                "price_vnd": price_vnd,
                "crawl_date": CRAWL_DATE,
                "source": SOURCE,
                "source_url": route_url,
            }
            records.append(record)
            logger.info(
                f"[{len(records)}] [train] {operator_name} | {vehicle_type} | "
                f"{departure_time} -> {arrival_time} | {price_vnd or 'N/A'}"
            )
            if len(records) >= MAX_RECORDS_PER_ROUTE:
                break
        except Exception as e:
            logger.warning(f"Lỗi parse train card {index}: {e}")

    return records


# ============================================================
# EXTRACT FLIGHT
# ============================================================

def extract_flight_tickets(driver, origin, destination, route_url):
    """
    Selector: [class*="FlightTicketItem__FlightTicketItemContainer"]
    Text mẫu:
        VietJet Air  Deluxe  Airbus VJ160  20:30  SGN  2h10  22:40  HAN  1.362.341đ
    """
    selectors = [
        '[class*="FlightTicketItem__FlightTicketItemContainer"]',
        '[class*="FlightTicketItem"]',
    ]
    cards = []
    used = None
    for sel in selectors:
        found = driver.find_elements(By.CSS_SELECTOR, sel)
        # lọc card có text đủ dài
        found = [c for c in found if len((c.text or "").strip()) > 40]
        if found:
            cards = found
            used = sel
            break

    if not cards:
        logger.warning(f"FLIGHT | {origin} -> {destination}: không thấy FlightTicketItem")
        return []

    logger.info(f"Tìm thấy {len(cards)} flight card | selector={used}")
    records = []
    seen = set()

    for index, card in enumerate(cards, start=1):
        try:
            full = card.text.strip()
            if len(full) < 30:
                continue

            times = re.findall(r"\b(\d{1,2}:\d{2})\b", full)
            departure_time = times[0] if times else ""
            arrival_time = times[1] if len(times) >= 2 else ""

            operator_name = "Unknown"
            for airline in (
                "Vietnam Airlines", "VietJet Air", "Vietjet Air", "Bamboo Airways",
                "Pacific Airlines", "Vietravel Airlines", "VietJet", "Bamboo",
            ):
                if airline.lower() in full.lower():
                    operator_name = airline.replace("Vietjet Air", "VietJet Air")
                    break

            # Số hiệu / loại: VJ160, Airbus ...
            vehicle_type = "Unknown"
            m = re.search(r"\b((?:VJ|VN|QH|VU|BL)\d+)\b", full)
            if m:
                vehicle_type = m.group(1)
            else:
                for kw in ("Deluxe", "SkyBoss", "Business", "Eco", "Phổ thông", "Bay thẳng"):
                    if kw.lower() in full.lower():
                        vehicle_type = kw
                        break

            price_vnd = parse_price(full)
            duration_min = parse_duration(full)

            if not is_time(departure_time):
                continue

            key = (origin, destination, operator_name, vehicle_type, departure_time, arrival_time, price_vnd)
            if key in seen:
                continue
            seen.add(key)

            record = {
                "origin": origin,
                "destination": destination,
                "operator_name": operator_name,
                "transport_type": "flight",
                "vehicle_type": vehicle_type,
                "departure_time": departure_time,
                "arrival_time": arrival_time,
                "duration_min": duration_min,
                "price_vnd": price_vnd,
                "crawl_date": CRAWL_DATE,
                "source": SOURCE,
                "source_url": route_url,
            }
            records.append(record)
            logger.info(
                f"[{len(records)}] [flight] {operator_name} | {vehicle_type} | "
                f"{departure_time} -> {arrival_time} | {price_vnd or 'N/A'}"
            )
            if len(records) >= MAX_RECORDS_PER_ROUTE:
                break
        except Exception as e:
            logger.warning(f"Lỗi parse flight card {index}: {e}")

    return records


# ============================================================
# CRAWL ONE ROUTE
# ============================================================

def crawl_route(driver, route, route_index, total_routes):
    origin = route["origin"]
    destination = route["destination"]
    base_url = route["url"]
    transport_type = route.get("transport_type", "bus")

    vexere_url = build_vexere_url(base_url, CRAWL_DATE, transport_type)

    logger.info("=" * 70)
    logger.info(
        f"ROUTE {route_index}/{total_routes}: {origin} -> {destination} [{transport_type}]"
    )
    logger.info(f"URL: {vexere_url}")

    try:
        driver.get(vexere_url)
        time.sleep(3)
        close_popups(driver)
        time.sleep(1)

        if transport_type == "bus":
            if not wait_for_tickets(driver, timeout=TICKET_WAIT_TIMEOUT):
                logger.warning(
                    f"{origin} -> {destination}: Không tìm thấy ticket "
                    f"(có thể hết chuyến ngày {CRAWL_DATE} hoặc URL sai)"
                )
                dump_debug_html(driver, origin, destination)
                return []

            logger.info(f"Ticket ban đầu: {count_tickets(driver)}")
            load_more_trips_until_limit(driver)
            records = extract_bus_tickets(driver, origin, destination, vexere_url)
        elif transport_type == "train":
            close_popups(driver)
            # Nếu URL chưa phải dat-ve-tau → cần click Tìm kiếm
            if "dat-ve-tau" not in vexere_url:
                click_search_button(driver)
                time.sleep(3)
            else:
                time.sleep(4)  # chờ list train render
            close_popups(driver)
            records = extract_train_tickets(driver, origin, destination, vexere_url)
            if not records:
                dump_debug_html(driver, origin, destination)
        elif transport_type == "flight":
            close_popups(driver)
            if "dat-ve-may-bay" not in vexere_url:
                click_search_button(driver)
                time.sleep(4)
            else:
                time.sleep(5)  # chờ list flight render
            close_popups(driver)
            records = extract_flight_tickets(driver, origin, destination, vexere_url)
            if not records:
                dump_debug_html(driver, origin, destination)
        else:
            logger.warning(f"Unknown transport_type: {transport_type}")
            records = []

        logger.info(
            f"Hoàn thành {origin} -> {destination} [{transport_type}]: {len(records)} records"
        )
        return records

    except (InvalidSessionIdException, WebDriverException) as e:
        logger.error(f"Driver lỗi trên route {origin} -> {destination}: {e}")
        raise
    except Exception as e:
        logger.exception(f"Lỗi route {origin} -> {destination}: {e}")
        return []


# ============================================================
# SAVE
# ============================================================

def save_json(records):
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    logger.info(f"Đã lưu {len(records)} records vào: {OUTPUT_FILE}")


# ============================================================
# MAIN
# ============================================================

def main():
    logger.info("=" * 70)
    logger.info("VEXERE TRANSPORT CRAWLER (fixed)")
    logger.info(f"Ngày crawl: {CRAWL_DATE} (ngày MAI)")
    logger.info(f"Max: {MAX_RECORDS_PER_ROUTE} chuyến / tuyến")
    logger.info("=" * 70)

    routes = load_routes()
    if not routes:
        logger.warning("Không có route nào trong CSV.")
        return

    driver = create_driver()
    all_records = []

    try:
        index = 0
        while index < len(routes):
            route = routes[index]
            try:
                records = crawl_route(
                    driver=driver,
                    route=route,
                    route_index=index + 1,
                    total_routes=len(routes),
                )
                all_records.extend(records)
                index += 1
                if index < len(routes):
                    time.sleep(ROUTE_DELAY)

            except (InvalidSessionIdException, WebDriverException) as e:
                logger.error(f"Chrome crash, restart driver... ({e})")
                safe_quit(driver)
                time.sleep(3)
                driver = create_driver()
                continue

    finally:
        safe_quit(driver)
        logger.info("Đã đóng Chrome driver.")

    save_json(all_records)

    by_type = {}
    for r in all_records:
        t = r.get("transport_type", "unknown")
        by_type[t] = by_type.get(t, 0) + 1

    logger.info("=" * 70)
    logger.info("CRAWLER HOÀN TẤT")
    logger.info(f"Tổng route: {len(routes)}")
    logger.info(f"Tổng records: {len(all_records)}")
    for t, cnt in sorted(by_type.items()):
        logger.info(f"  - {t}: {cnt}")
    logger.info(f"Output: {OUTPUT_FILE}")
    logger.info(f"Debug HTML (nếu fail): {DEBUG_DIR}")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()