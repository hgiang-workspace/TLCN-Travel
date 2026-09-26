import csv
import json
import logging
import re
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# ============================================================
# PATH
# ============================================================

BASE_DIR = Path(__file__).resolve().parents[2]
CURRENT_DIR = Path(__file__).resolve().parent

ROUTE_FILE = CURRENT_DIR / "transport_routes.csv"
OUTPUT_FILE = BASE_DIR / "data" / "raw" / "transport" / "vexere_transport.json"
LOG_FILE = BASE_DIR / "logs" / "vexere_crawler.log"


# ============================================================
# CONFIG
# ============================================================

WAIT_TIME = 5

# Tối đa 10 chuyến hợp lệ / tuyến
MAX_RECORDS_PER_ROUTE = 10

# Tối đa số lần click "Xem thêm chuyến"
MAX_LOAD_MORE_CLICKS = 10

SOURCE = "vexere"

# Tự động lấy ngày hiện tại theo máy
CRAWL_DATE = datetime.now().strftime("%d-%m-%Y")


# ============================================================
# LOGGING
# ============================================================

LOG_FILE.parent.mkdir(
    parents=True,
    exist_ok=True
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[
        logging.FileHandler(
            LOG_FILE,
            encoding="utf-8"
        ),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


# ============================================================
# LOAD ROUTES
# ============================================================

def load_routes():
    """
    Đọc danh sách tuyến từ transport_routes.csv.

    CSV gồm:
        origin,destination,url

    Không cần cột date.
    """

    if not ROUTE_FILE.exists():
        raise FileNotFoundError(
            f"Không tìm thấy file route: {ROUTE_FILE}"
        )

    routes = []

    with open(
        ROUTE_FILE,
        "r",
        encoding="utf-8-sig",
        newline=""
    ) as f:

        reader = csv.DictReader(f)

        required_columns = {
            "origin",
            "destination",
            "url"
        }

        if not required_columns.issubset(
            reader.fieldnames or []
        ):
            raise ValueError(
                "CSV phải có các cột: "
                "origin,destination,url"
            )

        for row in reader:

            origin = row["origin"].strip()
            destination = row["destination"].strip()
            url = row["url"].strip()

            if not origin or not destination or not url:
                continue

            routes.append({
                "origin": origin,
                "destination": destination,
                "url": url
            })

    logger.info(
        f"Đã đọc {len(routes)} tuyến từ CSV"
    )

    return routes


# ============================================================
# CREATE DRIVER
# ============================================================

def create_driver():

    options = Options()

    options.add_argument(
        "--start-maximized"
    )

    options.add_argument(
        "--disable-notifications"
    )

    options.add_argument(
        "--lang=vi-VN"
    )

    # Giảm log Chrome không cần thiết
    options.add_experimental_option(
        "excludeSwitches",
        ["enable-logging"]
    )

    driver = webdriver.Chrome(
        options=options
    )

    driver.set_page_load_timeout(
        60
    )

    return driver


# ============================================================
# BUILD VEXERE URL
# ============================================================

def build_vexere_url(
    base_url,
    crawl_date
):
    """
    CSV chỉ lưu URL gốc.

    Python tự thêm:
        date=DD-MM-YYYY
        lt=1

    Không tự thêm lf=1.
    """

    parsed = urlparse(
        base_url
    )

    query = parse_qs(
        parsed.query,
        keep_blank_values=True
    )

    # Xóa parameter cũ
    query.pop(
        "date",
        None
    )

    query.pop(
        "lt",
        None
    )

    query.pop(
        "lf",
        None
    )

    # Ngày crawl
    query["date"] = [
        crawl_date
    ]

    # Loại tìm kiếm vé xe
    query["lt"] = [
        "1"
    ]

    new_query = urlencode(
        query,
        doseq=True
    )

    final_url = urlunparse((
        parsed.scheme,
        parsed.netloc,
        parsed.path,
        parsed.params,
        new_query,
        parsed.fragment
    ))

    return final_url


# ============================================================
# PARSE PRICE
# ============================================================

def parse_price(text):
    """
    Parse giá vé Vexere.

    Hỗ trợ:

        Từ 500.000đ
        500.000đ
        765.000đ
        1.200.000đ
        Từ 1.200.000 ₫
        500000 VND

    Return:
        int hoặc None
    """

    if not text:
        return None

    text = text.strip()

    # --------------------------------------------------------
    # Cách 1:
    # Tìm số có đơn vị tiền
    # --------------------------------------------------------

    matches = re.findall(
        r"(\d+(?:[.,]\d+)*)\s*(?:đ|₫|vnd)",
        text,
        flags=re.IGNORECASE
    )

    if matches:

        try:

            # Lấy giá đầu tiên
            value = matches[0]

            # Xóa dấu phân cách hàng nghìn
            value = value.replace(
                ".",
                ""
            )

            value = value.replace(
                ",",
                ""
            )

            return int(value)

        except ValueError:
            pass

    # --------------------------------------------------------
    # Cách 2:
    # Fallback nếu chỉ có số
    # --------------------------------------------------------

    numbers = re.findall(
        r"\d+(?:[.,]\d+)*",
        text
    )

    if numbers:

        try:

            value = numbers[0]

            value = value.replace(
                ".",
                ""
            )

            value = value.replace(
                ",",
                ""
            )

            return int(value)

        except ValueError:
            pass

    return None


# ============================================================
# EXTRACT PRICE FROM TICKET
# ============================================================

def extract_price_from_ticket(ticket):
    """
    Tìm giá trong ticket.

    Thứ tự ưu tiên:

    1. .fare-sale
    2. .fareSmall
    3. Các element chứa đ / ₫ / VND
    4. Toàn bộ text của ticket

    Mục tiêu xử lý các trường hợp:
        Từ 500.000đ
        500.000đ
        765.000đ
    """

    price_text = ""

    # --------------------------------------------------------
    # Cách 1: .fare-sale
    # --------------------------------------------------------

    try:

        elements = ticket.find_elements(
            By.CSS_SELECTOR,
            ".fare-sale"
        )

        for element in elements:

            text = element.text.strip()

            if text:
                price_text = text
                price = parse_price(text)

                if price is not None:
                    return price

    except Exception:
        pass

    # --------------------------------------------------------
    # Cách 2: .fareSmall
    # --------------------------------------------------------

    try:

        elements = ticket.find_elements(
            By.CSS_SELECTOR,
            ".fareSmall"
        )

        for element in elements:

            text = element.text.strip()

            if text:
                price_text = text
                price = parse_price(text)

                if price is not None:
                    return price

    except Exception:
        pass

    # --------------------------------------------------------
    # Cách 3:
    # Tìm element chứa đơn vị tiền
    # --------------------------------------------------------

    try:

        price_elements = ticket.find_elements(
            By.XPATH,
            ".//*[contains(text(), 'đ') "
            "or contains(text(), 'Đ') "
            "or contains(text(), '₫') "
            "or contains(translate(text(), "
            "'vnd', 'VND'), 'VND')]"
        )

        for element in price_elements:

            text = element.text.strip()

            if not text:
                continue

            price = parse_price(
                text
            )

            if price is not None:
                return price

    except Exception:
        pass

    # --------------------------------------------------------
    # Cách 4:
    # Quét toàn bộ text ticket
    # --------------------------------------------------------

    try:

        full_text = ticket.text.strip()

        price = parse_price(
            full_text
        )

        if price is not None:
            return price

        # Log để debug nếu không tìm thấy
        logger.warning(
            "Không parse được giá ticket | "
            f"Text: {full_text[:300]!r}"
        )

    except Exception:
        pass

    return None


# ============================================================
# PARSE DURATION
# ============================================================

def parse_duration(text):
    """
    Chuyển duration thành phút.

    Ví dụ:
        16h      -> 960
        3h30m    -> 210
        3h30p    -> 210
        90p      -> 90
    """

    if not text:
        return None

    text = text.lower().strip()

    hours = 0
    minutes = 0

    hour_match = re.search(
        r"(\d+)\s*h",
        text
    )

    minute_match = re.search(
        r"(\d+)\s*(?:m|p)",
        text
    )

    if hour_match:
        hours = int(
            hour_match.group(1)
        )

    if minute_match:
        minutes = int(
            minute_match.group(1)
        )

    if hour_match or minute_match:

        return (
            hours * 60
            + minutes
        )

    number_match = re.search(
        r"\d+",
        text
    )

    if number_match:

        return int(
            number_match.group()
        )

    return None


# ============================================================
# CHECK TIME
# ============================================================

def is_time(text):
    """
    Kiểm tra chuỗi có dạng HH:MM.
    """

    if not text:
        return False

    return bool(
        re.fullmatch(
            r"\d{1,2}:\d{2}",
            text.strip()
        )
    )


# ============================================================
# DETECT TRANSPORT TYPE
# ============================================================

def detect_transport_type(
    vehicle_type
):
    """
    Phân loại phương tiện.

    Với Vexere hiện tại chủ yếu là bus.
    """

    if not vehicle_type:
        return "bus"

    text = vehicle_type.lower()

    if any(
        keyword in text
        for keyword in [
            "máy bay",
            "may bay",
            "flight",
            "airline",
            "plane"
        ]
    ):
        return "flight"

    if any(
        keyword in text
        for keyword in [
            "tàu hỏa",
            "tau hoa",
            "train"
        ]
    ):
        return "train"

    return "bus"


# ============================================================
# WAIT FOR TICKETS
# ============================================================

def wait_for_tickets(
    driver,
    timeout=15
):
    """
    Chờ ticket xuất hiện.
    """

    try:

        WebDriverWait(
            driver,
            timeout
        ).until(
            EC.presence_of_element_located(
                (
                    By.CSS_SELECTOR,
                    "div.ticket"
                )
            )
        )

        return True

    except Exception:

        return False


# ============================================================
# COUNT TICKETS
# ============================================================

def count_tickets(driver):
    """
    Đếm số ticket hiện tại trên trang.
    """

    try:

        tickets = driver.find_elements(
            By.CSS_SELECTOR,
            "div.ticket"
        )

        return len(tickets)

    except Exception:

        return 0


# ============================================================
# CLICK "XEM THÊM CHUYẾN"
# ============================================================

def click_load_more_trips(
    driver
):
    """
    Chỉ click:

        Xem thêm chuyến

    Không click:

        Xem thêm hãng
    """

    xpaths = [

        "//button[contains("
        "normalize-space(.), "
        "'Xem thêm chuyến')]",

        "//div[contains("
        "normalize-space(.), "
        "'Xem thêm chuyến')]",

        "//span[contains("
        "normalize-space(.), "
        "'Xem thêm chuyến')]",

        "//a[contains("
        "normalize-space(.), "
        "'Xem thêm chuyến')]"
    ]

    for xpath in xpaths:

        try:

            elements = driver.find_elements(
                By.XPATH,
                xpath
            )

            for element in elements:

                try:

                    if not element.is_displayed():
                        continue

                    if not element.is_enabled():
                        continue

                    # Scroll tới nút
                    driver.execute_script(
                        """
                        arguments[0].scrollIntoView({
                            behavior: 'instant',
                            block: 'center'
                        });
                        """,
                        element
                    )

                    time.sleep(
                        0.5
                    )

                    before_count = count_tickets(
                        driver
                    )

                    # Click
                    driver.execute_script(
                        "arguments[0].click();",
                        element
                    )

                    logger.info(
                        "Đã click "
                        "'Xem thêm chuyến' "
                        f"(ticket trước: {before_count})"
                    )

                    # Chờ DOM cập nhật
                    time.sleep(
                        2
                    )

                    try:

                        WebDriverWait(
                            driver,
                            5
                        ).until(
                            lambda d:
                            count_tickets(d)
                            > before_count
                        )

                    except Exception:
                        pass

                    after_count = count_tickets(
                        driver
                    )

                    logger.info(
                        "Ticket sau khi click: "
                        f"{after_count}"
                    )

                    return True

                except Exception:

                    continue

        except Exception:

            continue

    return False


# ============================================================
# LOAD ENOUGH TRIPS
# ============================================================

def load_more_trips_until_limit(
    driver
):
    """
    Click "Xem thêm chuyến".

    Dừng khi:

    - Có >= 10 ticket
    - Không còn nút
    - Quá số lần click cho phép
    """

    logger.info(
        "Bắt đầu kiểm tra nút "
        "'Xem thêm chuyến'..."
    )

    for click_number in range(
        1,
        MAX_LOAD_MORE_CLICKS + 1
    ):

        current_count = count_tickets(
            driver
        )

        logger.info(
            f"Ticket hiện tại: "
            f"{current_count}/"
            f"{MAX_RECORDS_PER_ROUTE}"
        )

        # Đủ ticket
        if current_count >= MAX_RECORDS_PER_ROUTE:

            logger.info(
                "Đã đủ số chuyến yêu cầu."
            )

            break

        clicked = click_load_more_trips(
            driver
        )

        if not clicked:

            logger.info(
                "Không tìm thấy nút "
                "'Xem thêm chuyến'."
            )

            break

        logger.info(
            "Đã click "
            f"'Xem thêm chuyến' "
            f"lần {click_number}"
        )

    final_count = count_tickets(
        driver
    )

    logger.info(
        "Tổng ticket sau khi load thêm: "
        f"{final_count}"
    )


# ============================================================
# EXTRACT TRIP CARDS
# ============================================================

def extract_trip_cards(
    driver,
    origin,
    destination,
    route_url
):
    """
    Lấy tối đa 10 chuyến hợp lệ.

    Không giới hạn theo hãng.

    Duyệt nhiều hơn 10 ticket nếu cần
    để tránh trường hợp ticket lỗi làm
    thiếu số record.
    """

    tickets = driver.find_elements(
        By.CSS_SELECTOR,
        "div.ticket"
    )

    logger.info(
        f"Tìm thấy {len(tickets)} "
        "ticket trên trang"
    )

    records = []
    seen = set()

    # Không cắt tickets[:10] ở đây.
    # Duyệt cho tới khi đủ 10 record hợp lệ.

    for index, ticket in enumerate(
        tickets,
        start=1
    ):

        try:

            # =================================================
            # OPERATOR
            # =================================================

            operator_name = ""

            try:

                operator_name = ticket.find_element(
                    By.CSS_SELECTOR,
                    ".bus-name"
                ).text.strip()

            except Exception:
                pass


            # =================================================
            # VEHICLE TYPE
            # =================================================

            vehicle_type = ""

            try:

                vehicle_type = ticket.find_element(
                    By.CSS_SELECTOR,
                    ".seat-type"
                ).text.strip()

            except Exception:
                pass


            # =================================================
            # DEPARTURE TIME
            # =================================================

            departure_time = ""

            try:

                departure_time = ticket.find_element(
                    By.CSS_SELECTOR,
                    ".from-to "
                    ".content.from "
                    ".hour"
                ).text.strip()

            except Exception:
                pass


            # =================================================
            # ARRIVAL TIME
            # =================================================

            arrival_time = ""

            try:

                arrival_time = ticket.find_element(
                    By.CSS_SELECTOR,
                    ".from-to "
                    ".content.to "
                    ".hour"
                ).text.strip()

            except Exception:
                pass


            # =================================================
            # DURATION
            # =================================================

            duration_text = ""

            try:

                duration_text = ticket.find_element(
                    By.CSS_SELECTOR,
                    ".duration"
                ).text.strip()

            except Exception:
                pass

            duration_min = parse_duration(
                duration_text
            )


            # =================================================
            # PRICE
            # =================================================

            price_vnd = extract_price_from_ticket(
                ticket
            )


            # =================================================
            # VALIDATION
            # =================================================

            if not is_time(
                departure_time
            ):

                logger.warning(
                    f"Ticket {index}: "
                    "thiếu giờ đi"
                )

                continue


            if not is_time(
                arrival_time
            ):

                logger.warning(
                    f"Ticket {index}: "
                    "thiếu giờ đến"
                )

                continue


            if not operator_name:

                operator_name = "Unknown"


            if not vehicle_type:

                vehicle_type = "Unknown"


            # =================================================
            # UNIQUE KEY
            # =================================================

            unique_key = (
                origin,
                destination,
                operator_name,
                vehicle_type,
                departure_time,
                arrival_time,
                price_vnd
            )

            if unique_key in seen:
                continue

            seen.add(
                unique_key
            )


            # =================================================
            # TRANSPORT TYPE
            # =================================================

            transport_type = detect_transport_type(
                vehicle_type
            )


            # =================================================
            # RECORD
            # =================================================

            record = {
                "origin": origin,
                "destination": destination,
                "operator_name": operator_name,
                "transport_type": transport_type,
                "vehicle_type": vehicle_type,
                "departure_time": departure_time,
                "arrival_time": arrival_time,
                "duration_min": duration_min,
                "price_vnd": price_vnd,
                "crawl_date": CRAWL_DATE,
                "source": SOURCE,
                "source_url": route_url
            }

            records.append(
                record
            )


            # =================================================
            # LOG
            # =================================================

            if price_vnd is not None:

                logger.info(
                    f"[{len(records)}] "
                    f"{operator_name} | "
                    f"{vehicle_type} | "
                    f"{departure_time} -> "
                    f"{arrival_time} | "
                    f"{price_vnd:,} VND"
                )

            else:

                logger.warning(
                    f"[{len(records)}] "
                    f"{operator_name} | "
                    f"{vehicle_type} | "
                    f"{departure_time} -> "
                    f"{arrival_time} | "
                    f"Price N/A"
                )


            # =================================================
            # ĐỦ 10 RECORD
            # =================================================

            if len(records) >= MAX_RECORDS_PER_ROUTE:

                break


        except Exception as e:

            logger.warning(
                f"Lỗi parse ticket "
                f"{index}: {e}"
            )


    return records


# ============================================================
# CRAWL ONE ROUTE
# ============================================================

def crawl_route(
    driver,
    route,
    route_index,
    total_routes
):
    """
    Crawl một tuyến.
    """

    origin = route["origin"]
    destination = route["destination"]
    base_url = route["url"]

    vexere_url = build_vexere_url(
        base_url,
        CRAWL_DATE
    )

    logger.info(
        "=" * 70
    )

    logger.info(
        f"ROUTE {route_index}/"
        f"{total_routes}: "
        f"{origin} -> {destination}"
    )

    logger.info(
        f"URL: {vexere_url}"
    )

    try:

        # ----------------------------------------------------
        # OPEN PAGE
        # ----------------------------------------------------

        driver.get(
            vexere_url
        )

        time.sleep(
            3
        )


        # ----------------------------------------------------
        # WAIT INITIAL TICKETS
        # ----------------------------------------------------

        if not wait_for_tickets(
            driver,
            timeout=15
        ):

            logger.warning(
                f"{origin} -> {destination}: "
                "Không tìm thấy ticket ban đầu"
            )

            return []


        initial_count = count_tickets(
            driver
        )

        logger.info(
            f"Ticket ban đầu: "
            f"{initial_count}"
        )


        # ----------------------------------------------------
        # LOAD MORE
        # ----------------------------------------------------

        load_more_trips_until_limit(
            driver
        )


        # ----------------------------------------------------
        # EXTRACT
        # ----------------------------------------------------

        records = extract_trip_cards(
            driver=driver,
            origin=origin,
            destination=destination,
            route_url=vexere_url
        )


        logger.info(
            f"Hoàn thành "
            f"{origin} -> {destination}: "
            f"{len(records)} records"
        )

        return records


    except Exception as e:

        logger.exception(
            f"Lỗi route "
            f"{origin} -> {destination}: "
            f"{e}"
        )

        return []


# ============================================================
# SAVE JSON
# ============================================================

def save_json(
    records
):
    """
    Lưu dữ liệu thành JSON.
    """

    OUTPUT_FILE.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    with open(
        OUTPUT_FILE,
        "w",
        encoding="utf-8"
    ) as f:

        json.dump(
            records,
            f,
            ensure_ascii=False,
            indent=2
        )

    logger.info(
        f"Đã lưu {len(records)} "
        "records vào:"
    )

    logger.info(
        str(OUTPUT_FILE)
    )


# ============================================================
# MAIN
# ============================================================

def main():

    logger.info(
        "=" * 70
    )

    logger.info(
        "VEXERE TRANSPORT CRAWLER"
    )

    logger.info(
        f"Ngày crawl: {CRAWL_DATE}"
    )

    logger.info(
        "Phạm vi: TP.HCM -> "
        "các tỉnh/thành"
    )

    logger.info(
        "Phương tiện: bus"
    )

    logger.info(
        f"Max: "
        f"{MAX_RECORDS_PER_ROUTE} "
        "chuyến / tuyến"
    )

    logger.info(
        "Nút được click: "
        "'Xem thêm chuyến'"
    )

    logger.info(
        "Không click: "
        "'Xem thêm hãng'"
    )

    logger.info(
        "=" * 70
    )


    # --------------------------------------------------------
    # LOAD ROUTES
    # --------------------------------------------------------

    routes = load_routes()

    if not routes:

        logger.warning(
            "Không có route nào trong CSV."
        )

        return


    # --------------------------------------------------------
    # CREATE DRIVER
    # --------------------------------------------------------

    driver = create_driver()

    all_records = []


    try:

        # ----------------------------------------------------
        # CRAWL SEQUENTIALLY
        # ----------------------------------------------------

        for index, route in enumerate(
            routes,
            start=1
        ):

            records = crawl_route(
                driver=driver,
                route=route,
                route_index=index,
                total_routes=len(routes)
            )

            all_records.extend(
                records
            )

            # Delay giữa các route
            if index < len(routes):

                time.sleep(
                    2
                )


    finally:

        driver.quit()

        logger.info(
            "Đã đóng Chrome driver."
        )


    # --------------------------------------------------------
    # SAVE
    # --------------------------------------------------------

    save_json(
        all_records
    )


    # --------------------------------------------------------
    # SUMMARY
    # --------------------------------------------------------

    logger.info(
        "=" * 70
    )

    logger.info(
        "CRAWLER HOÀN TẤT"
    )

    logger.info(
        f"Tổng route: "
        f"{len(routes)}"
    )

    logger.info(
        f"Tổng records: "
        f"{len(all_records)}"
    )

    logger.info(
        f"Output: "
        f"{OUTPUT_FILE}"
    )

    logger.info(
        "=" * 70
    )


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":
    main()