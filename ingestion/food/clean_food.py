import json
import statistics
from pathlib import Path


# ============================================================
# PATH
# ============================================================

BASE_DIR = Path(__file__).resolve().parent

INPUT_FILE = BASE_DIR / "foody_quan_an.json"
OUTPUT_FILE = BASE_DIR / "foody_quan_an_CLEANED.json"


# ============================================================
# CLEAN FUNCTION
# ============================================================

def clean_value(value):
    """Làm sạch value: String, List, None, Number/Boolean"""
    if isinstance(value, str):
        value = value.strip()
        return None if value == "" else value
    elif isinstance(value, list):
        cleaned_list = []
        for item in value:
            if isinstance(item, str):
                item = item.strip()
                if item != "": cleaned_list.append(item)
            elif item is not None:
                cleaned_list.append(item)
        return None if len(cleaned_list) == 0 else cleaned_list
    elif value is None:
        return None
    return value


# ============================================================
# CLEAN FOODY
# ============================================================

def clean_foody_data():
    print("=" * 70)
    print("FOODY DATA CLEANING & IMPUTATION")
    print("=" * 70)

    if not INPUT_FILE.exists():
        print(f"Không tìm thấy file: {INPUT_FILE}")
        return

    print(f"\nInput file: {INPUT_FILE}")
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        print("JSON không có dạng list.")
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

            if value is None:
                stats["null_removed"] += 1
                continue
            elif isinstance(value, str):
                clean_value_result = value.strip()
                if clean_value_result != value: stats["string_cleaned"] += 1
                if clean_value_result == "":
                    stats["empty_removed"] += 1
                    continue
                cleaned_item[clean_key] = clean_value_result
            elif isinstance(value, list):
                clean_list = [x.strip() if isinstance(x, str) else x for x in value if x is not None and (not isinstance(x, str) or x.strip() != "")]
                if len(clean_list) == 0:
                    stats["empty_removed"] += 1
                    continue
                cleaned_item[clean_key] = clean_list
            else:
                cleaned_item[clean_key] = value

        if len(cleaned_item) > 0:
            cleaned_data.append(cleaned_item)
            stats["cleaned_records"] += 1
        else:
            stats["removed_records"] += 1

    # --- BƯỚC 2: ĐIỀN KHUYẾT (IMPUTATION) CHO CÁC CỘT RATING ---
    print("\nĐang xử lý điền khuyết (Median Imputation) cho các trường Rating...")
    rating_cols = ['rating_avg', 'rating_quality', 'rating_price', 'rating_service', 'rating_location']
    imputation_count = 0

    for col in rating_cols:
        # Lấy tất cả giá trị hợp lệ (là số) của cột này
        valid_values = [item[col] for item in cleaned_data if col in item and isinstance(item[col], (int, float))]
        
        if valid_values:
            median_val = round(statistics.median(valid_values), 1)
            # Điền giá trị median vào các record bị thiếu
            for item in cleaned_data:
                if col not in item or item[col] is None:
                    item[col] = median_val
                    imputation_count += 1
            print(f"   - {col:18}: Điền khuyết bằng Median = {median_val}")

    # ========================================================
    # SAVE
    # ========================================================
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(cleaned_data, f, ensure_ascii=False, indent=2)

    # ========================================================
    # PRINT STATISTICS
    # ========================================================
    print("\n" + "=" * 70)
    print("CLEANING & IMPUTATION SUMMARY")
    print("=" * 70)
    print(f"Records ban đầu       : {stats['total_records']}")
    print(f"Records sau làm sạch  : {stats['cleaned_records']}")
    print(f"Records bị loại       : {stats['removed_records']}")
    print(f"Số giá trị Rating được điền khuyết: {imputation_count}")
    print("\nCác trường đã xử lý:")
    print(f"   - Null bị loại         : {stats['null_removed']}")
    print(f"   - Giá trị rỗng bị loại : {stats['empty_removed']}")
    print(f"   - String được strip    : {stats['string_cleaned']}")
    print(f"   - Key được strip       : {stats['key_cleaned']}")
    print(f"\nOutput: {OUTPUT_FILE}")

    print("\n" + "=" * 70)
    print("HOÀN TẤT LÀM SẠCH FOODY")
    print("=" * 70)


if __name__ == "__main__":
    clean_foody_data()