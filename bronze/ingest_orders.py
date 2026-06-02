# =============================================================================
# bronze/ingest_orders.py
# Layer BRONZE — Ingestion đơn hàng từ CSV vào Delta table.
#
# Nguyên tắc Bronze:
#   - Dữ liệu được nạp "nguyên trạng" (as-is), KHÔNG làm sạch.
#   - Ghi theo mode append-only → giữ toàn bộ lịch sử, kể cả bản duplicate.
#   - Thêm cột metadata (ingested_at, source_file) để audit sau này.
#
# Chạy : python -m bronze.ingest_orders
# =============================================================================

import os
import sys
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, TimestampType,
)

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from config.spark_session import get_spark

# ---------------------------------------------------------------------------
# Đường dẫn
# ---------------------------------------------------------------------------
SOURCE_PATH = "data/raw/orders.csv"
BRONZE_PATH = "data/lakehouse/bronze/orders"

# ---------------------------------------------------------------------------
# Schema tường minh — Bronze vẫn nên khai báo schema để phát hiện
# lỗi format sớm, tránh "schema drift" âm thầm.
# ---------------------------------------------------------------------------
ORDERS_SCHEMA = StructType([
    StructField("order_id",    StringType(),  nullable=True),
    StructField("customer_id", StringType(),  nullable=True),
    StructField("product_id",  StringType(),  nullable=True),
    StructField("quantity",    IntegerType(), nullable=True),
    StructField("unit_price",  DoubleType(),  nullable=True),
    StructField("status",      StringType(),  nullable=True),
    StructField("order_date",  StringType(),  nullable=True),   # giữ là string, parse ở Silver
    StructField("ingested_at", StringType(),  nullable=True),
])


def ingest_orders(source_path: str = SOURCE_PATH, bronze_path: str = BRONZE_PATH) -> int:
    """
    Đọc CSV đơn hàng → thêm metadata → ghi vào Bronze Delta table.

    Trả về
    ------
    int : Số bản ghi đã nạp vào Bronze (bao gồm cả duplicate).
    """
    spark = get_spark("Bronze_IngestOrders")

    print(f"[Bronze] Đọc dữ liệu nguồn: {source_path}")
    df_raw = (
        spark.read
        .option("header", "true")
        .option("encoding", "utf-8")
        .schema(ORDERS_SCHEMA)
        .csv(source_path)
    )

    # Thêm metadata cho mục đích audit / lineage
    df_with_meta = df_raw.withColumns({
        "_bronze_ingested_at": F.lit(datetime.now().isoformat(timespec="seconds")),
        "_source_file"       : F.lit(os.path.basename(source_path)),
    })

    row_count = df_with_meta.count()
    print(f"[Bronze] Số bản ghi đọc được (bao gồm duplicate): {row_count:,}")

    # Ghi append-only — KHÔNG ghi đè, KHÔNG dedup ở đây
    (
        df_with_meta.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(bronze_path)
    )

    print(f"[Bronze] ✅ Đã ghi vào: {bronze_path}")
    print(f"[Bronze]    Delta table có thể xem lịch sử bằng: DESCRIBE HISTORY delta.`{bronze_path}`")
    return row_count


if __name__ == "__main__":
    ingest_orders()
