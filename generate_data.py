# =============================================================================
# generate_data.py
# Sinh dữ liệu mẫu giả lập hệ thống thương mại điện tử.
#
# Mục đích: thay thế database thực trong môi trường lab.
# Output  : 3 file CSV tại thư mục data/raw/
#             - orders.csv       (500 đơn hàng, có chứa duplicate cố ý)
#             - customers.csv    (200 khách hàng)
#             - products.csv     (50 sản phẩm)
#
# Chạy   : python generate_data.py
# =============================================================================

import os
import random
import pandas as pd
from faker import Faker
from datetime import datetime, timedelta

fake = Faker("vi_VN")   # locale tiếng Việt cho tên, địa chỉ
random.seed(42)

# ---------------------------------------------------------------------------
# Đường dẫn output
# ---------------------------------------------------------------------------
RAW_DIR = "data/raw"
os.makedirs(RAW_DIR, exist_ok=True)

# ---------------------------------------------------------------------------
# 1. Customers (200 khách hàng)
# ---------------------------------------------------------------------------
def generate_customers(n: int = 200) -> pd.DataFrame:
    records = []
    for i in range(1, n + 1):
        records.append({
            "customer_id": f"CUST-{i:04d}",
            "name"       : fake.name(),
            "email"      : fake.email(),
            "phone"      : fake.phone_number(),
            "city"       : fake.city(),
            "region"     : random.choice(["North", "Central", "South"]),
            "created_at" : fake.date_between(start_date="-3y", end_date="today").isoformat(),
        })
    return pd.DataFrame(records)

# ---------------------------------------------------------------------------
# 2. Products (50 sản phẩm)
# ---------------------------------------------------------------------------
CATEGORIES = ["Electronics", "Fashion", "Home & Living", "Sports", "Beauty"]

def generate_products(n: int = 50) -> pd.DataFrame:
    records = []
    for i in range(1, n + 1):
        category = random.choice(CATEGORIES)
        records.append({
            "product_id"  : f"PROD-{i:04d}",
            "name"        : f"{fake.word().capitalize()} {category} {i}",
            "category"    : category,
            "price"       : round(random.uniform(50_000, 5_000_000), -3),   # VNĐ
            "stock"       : random.randint(0, 500),
        })
    return pd.DataFrame(records)

# ---------------------------------------------------------------------------
# 3. Orders (500 đơn + ~10% duplicate để demo dedup ở Silver layer)
# ---------------------------------------------------------------------------
ORDER_STATUSES = ["pending", "confirmed", "shipped", "delivered", "cancelled"]

def generate_orders(
    customers: pd.DataFrame,
    products : pd.DataFrame,
    n        : int = 500,
    dup_rate : float = 0.10,
) -> pd.DataFrame:
    """
    Sinh đơn hàng.
    dup_rate: tỷ lệ bản ghi trùng lặp (mô phỏng lỗi double-insert từ hệ thống).
    """
    records = []
    base_date = datetime(2024, 1, 1)

    for i in range(1, n + 1):
        order_date = base_date + timedelta(days=random.randint(0, 364))
        records.append({
            "order_id"   : f"ORD-{i:06d}",
            "customer_id": random.choice(customers["customer_id"].tolist()),
            "product_id" : random.choice(products["product_id"].tolist()),
            "quantity"   : random.randint(1, 10),
            "unit_price" : random.choice(products["price"].tolist()),
            "status"     : random.choice(ORDER_STATUSES),
            "order_date" : order_date.date().isoformat(),
            "ingested_at": datetime.now().isoformat(timespec="seconds"),
        })

    df = pd.DataFrame(records)

    # Tạo bản ghi trùng lặp cố ý (mô phỏng vấn đề thực tế)
    n_dup = int(n * dup_rate)
    duplicates = df.sample(n=n_dup, random_state=42).copy()
    duplicates["ingested_at"] = (
        datetime.now() + timedelta(minutes=5)
    ).isoformat(timespec="seconds")   # timestamp khác nhau → không bị drop bởi dropDuplicates đơn giản

    df_with_dups = pd.concat([df, duplicates], ignore_index=True)
    return df_with_dups.sample(frac=1, random_state=42).reset_index(drop=True)  # shuffle

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("Đang sinh dữ liệu mẫu...")

    customers = generate_customers(200)
    products  = generate_products(50)
    orders    = generate_orders(customers, products, n=500, dup_rate=0.10)

    customers.to_csv(f"{RAW_DIR}/customers.csv", index=False, encoding="utf-8")
    products.to_csv(f"{RAW_DIR}/products.csv",   index=False, encoding="utf-8")
    orders.to_csv(f"{RAW_DIR}/orders.csv",        index=False, encoding="utf-8")

    print(f"✅ customers : {len(customers):,} rows  →  {RAW_DIR}/customers.csv")
    print(f"✅ products  : {len(products):,} rows  →  {RAW_DIR}/products.csv")
    print(f"✅ orders    : {len(orders):,} rows  →  {RAW_DIR}/orders.csv")
    print(f"   (trong đó ~{int(500 * 0.10)} bản ghi là duplicate cố ý để demo dedup)")
