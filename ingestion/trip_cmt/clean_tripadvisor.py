import json
from pathlib import Path
from collections import Counter


# ============================================================
# PATH
# ============================================================

BASE_DIR = Path(__file__).resolve().parent

INPUT_FILE = BASE_DIR / "tripadvisor_comments.json"
OUTPUT_FILE = BASE_DIR / "tripadvisor_comments_CLEANED.json"
OUTPUT_NLP_FILE = BASE_DIR / "tripadvisor_comments_FOR_NLP.json"


# ============================================================
# CLEAN VALUE
# ============================================================

def clean_value(value, stats):
    if value is None:
        stats["null_removed"] += 1
        return None

    if isinstance(value, str):
        cleaned = value.strip()
        if cleaned != value: stats["string_cleaned"] += 1
        if cleaned == "":
            stats["empty_removed"] += 1
            return None
        return cleaned

    if isinstance(value, list):
        cleaned_list = []
        for item in value:
            cleaned_item = clean_value(item, stats)
            if cleaned_item is not None:
                cleaned_list.append(cleaned_item)
        if len(cleaned_list) == 0:
            stats["empty_removed"] += 1
            return None
        return cleaned_list

    if isinstance(value, dict):
        cleaned_dict = {}
        for key, item in value.items():
            clean_key = key.strip()
            if clean_key != key: stats["key_cleaned"] += 1
            cleaned_item = clean_value(item, stats)
            if cleaned_item is not None:
                cleaned_dict[clean_key] = cleaned_item
        if len(cleaned_dict) == 0:
            stats["empty_removed"] += 1
            return None
        return cleaned_dict

    return value


# ============================================================
# CLEAN TRIPADVISOR
# ============================================================

def clean_tripadvisor_data():
    print("=" * 70)
    print("TRIPADVISOR DATA CLEANING")
    print("=" * 70)

    if not INPUT_FILE.exists():
        print(f"Không tìm thấy file: {INPUT_FILE}")
        return

    print(f"\nInput file: {INPUT_FILE}")
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        print("File JSON phải có dạng list.")
        return

    print(f"Số lượng record ban đầu: {len(data)}")

    stats = {
        "total_records": len(data), "cleaned_records": 0, "removed_records": 0,
        "null_removed": 0, "empty_removed": 0, "string_cleaned": 0, "key_cleaned": 0
    }
    cleaned_data = []

    # --- BƯỚC 1: LÀM SẠCH CƠ BẢN ---
    for item in data:
        if not isinstance(item, dict):
            stats["removed_records"] += 1
            continue

        cleaned_item = {}
        for key, value in item.items():
            clean_key = key.strip()
            if clean_key != key: stats["key_cleaned"] += 1

            cleaned_value = clean_value(value, stats)
            if cleaned_value is not None:
                cleaned_item[clean_key] = cleaned_value

        if len(cleaned_item) > 0:
            cleaned_data.append(cleaned_item)
            stats["cleaned_records"] += 1
        else:
            stats["removed_records"] += 1

    # ========================================================
    # SAVE FILE TỔNG HỢP (Cho thống kê mô tả)
    # ========================================================
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(cleaned_data, f, ensure_ascii=False, indent=2)

    # ========================================================
    # BƯỚC 2: LỌC DỮ LIỆU CHO NLP (Natural Language Processing)
    # ========================================================
    nlp_data = []
    for item in cleaned_data:
        # Kiểm tra xem có nội dung review thực tế hay không
        has_detail_review = isinstance(item.get("detail_review"), str) and len(item.get("detail_review", "").strip()) > 0
        has_reviews_list = isinstance(item.get("reviews"), list) and len(item.get("reviews")) > 0
        
        # Chỉ giữ lại nếu có ít nhất 1 trong 2 điều kiện trên
        if has_detail_review or has_reviews_list:
            nlp_data.append(item)

    with open(OUTPUT_NLP_FILE, "w", encoding="utf-8") as f:
        json.dump(nlp_data, f, ensure_ascii=False, indent=2)

    # ========================================================
    # PRINT STATISTICS
    # ========================================================
    print("\n" + "=" * 70)
    print("CLEANING SUMMARY")
    print("=" * 70)
    print(f"Records ban đầu        : {stats['total_records']}")
    print(f"Records sau làm sạch   : {stats['cleaned_records']}")
    print(f"Records bị loại        : {stats['removed_records']}")
    print("\nCác trường đã xử lý:")
    print(f"   - Null bị loại          : {stats['null_removed']}")
    print(f"   - Giá trị rỗng bị loại  : {stats['empty_removed']}")
    print(f"   - String được strip     : {stats['string_cleaned']}")
    print(f"   - Key được strip        : {stats['key_cleaned']}")
    
    print(f"\nOutput tổng hợp (Thống kê): {OUTPUT_FILE}")
    print(f" Output cho NLP (Đã lọc review rỗng): {OUTPUT_NLP_FILE}")
    print(f"   ➔ Số record giữ lại cho phân tích văn bản: {len(nlp_data)} / {len(cleaned_data)}")

  
    print("\n" + "=" * 70)
    print("HOÀN TẤT LÀM SẠCH TRIPADVISOR")
    print("=" * 70)


if __name__ == "__main__":
    clean_tripadvisor_data()