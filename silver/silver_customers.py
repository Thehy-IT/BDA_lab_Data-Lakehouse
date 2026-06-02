# =============================================================================
# silver/silver_customers.py
# Layer SILVER — Làm sạch bảng customers từ Bronze.
#
# Các bước:
#   1. Đọc customers.csv trực tiếp (trong lab, customers không có Bronze Delta).
#   2. Chuẩn hoá: trim whitespace, lower email, chuẩn hoá region.
#   3. Kiểm tra null, loại bỏ bản ghi thiếu thông tin bắt buộc.
#   4. Ghi vào Silver Delta (overwrite vì bảng dimension ít thay đổi).
#
# Chạy : python -m silver.silver_customers
# =============================================================================

import os
import sys

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DateType

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from config.spark_session import get_spark

# ---------------------------------------------------------------------------
# Đường dẫn
# ---------------------------------------------------------------------------
SOURCE_PATH  = "data/raw/customers.csv"
SILVER_PATH  = "data/lakehouse/silver/customers"

VALID_REGIONS = {"North", "Central", "South"}


def run(source_path: str = SOURCE_PATH, silver_path: str = SILVER_PATH) -> None:
    spark = get_spark("Silver_Customers")

    print("=" * 60)
    print("[Silver] Bắt đầu xử lý customers")
    print("=" * 60)

    df_raw = (
        spark.read
        .option("header", "true")
        .option("encoding", "utf-8-sig")
        .csv(source_path)
    )
    print(f"[Silver] Đọc được {df_raw.count():,} customers từ CSV")

    # --- Chuẩn hoá ---
    df_clean = (
        df_raw
        .withColumn("name",        F.trim(F.col("name")))
        .withColumn("email",       F.lower(F.trim(F.col("email"))))
        .withColumn("phone",       F.regexp_replace("phone", r"\s+", ""))
        .withColumn("city",        F.trim(F.col("city")))
        .withColumn("region",      F.trim(F.col("region")))
        .withColumn("created_date",F.to_date("created_at", "yyyy-MM-dd"))
        .drop("created_at")
    )

    # --- Validate ---
    df_valid = (
        df_clean
        .filter(F.col("customer_id").isNotNull())
        .filter(F.col("email").isNotNull() & F.col("email").contains("@"))
        .filter(F.col("region").isin(list(VALID_REGIONS)))
    )

    n_raw   = df_raw.count()
    n_valid = df_valid.count()
    print(f"[Silver] Sau validate : {n_valid:,}  (loại {n_raw - n_valid:,} bản ghi lỗi)")

    # Thêm timestamp xử lý
    df_final = df_valid.withColumn("_silver_updated_at", F.current_timestamp())

    # Dimension table → overwrite toàn bộ khi refresh
    (
        df_final.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(silver_path)
    )

    print(f"[Silver] ✅ Đã ghi {n_valid:,} customers vào: {silver_path}")

    # Kiểm tra phân bổ theo region
    print("\n[Silver] Phân bổ theo region:")
    df_final.groupBy("region").count().orderBy("region").show()


if __name__ == "__main__":
    run()
