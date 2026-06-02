# =============================================================================
# silver/silver_orders.py
# Layer SILVER — Làm sạch và dedup đơn hàng từ Bronze.
#
# Các bước xử lý:
#   1. Đọc Bronze orders Delta table.
#   2. Ép kiểu, parse ngày tháng, chuẩn hoá status.
#   3. Loại bỏ duplicate bằng window function (giữ bản ghi mới nhất).
#   4. Validate dữ liệu cơ bản (null check, range check).
#   5. Upsert vào Silver bằng Delta MERGE INTO (ACID).
#
# Điểm quan trọng:
#   - MERGE INTO đảm bảo idempotent: chạy lại nhiều lần → kết quả không đổi.
#   - Schema enforcement bật ở Silver: ghi sẽ thất bại nếu schema sai.
#
# Chạy : python -m silver.silver_orders
# =============================================================================

import os
import sys

from pyspark.sql import functions as F, Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, DateType, TimestampType,
)
from delta.tables import DeltaTable

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from config.spark_session import get_spark

# ---------------------------------------------------------------------------
# Đường dẫn
# ---------------------------------------------------------------------------
BRONZE_PATH = "data/lakehouse/bronze/orders"
SILVER_PATH = "data/lakehouse/silver/orders"

# ---------------------------------------------------------------------------
# Schema tường minh cho Silver — strict hơn Bronze
# ---------------------------------------------------------------------------
SILVER_SCHEMA = StructType([
    StructField("order_id",          StringType(),    nullable=False),
    StructField("customer_id",       StringType(),    nullable=False),
    StructField("product_id",        StringType(),    nullable=False),
    StructField("quantity",          IntegerType(),   nullable=False),
    StructField("unit_price",        DoubleType(),    nullable=False),
    StructField("total_amount",      DoubleType(),    nullable=False),   # cột tính toán mới
    StructField("status",            StringType(),    nullable=False),
    StructField("order_date",        DateType(),      nullable=False),
    StructField("_silver_updated_at",TimestampType(), nullable=False),
])

# Các giá trị status hợp lệ
VALID_STATUSES = {"pending", "confirmed", "shipped", "delivered", "cancelled"}


def _clean_and_deduplicate(spark, bronze_path: str):
    """
    Bước 1–3: Đọc Bronze → clean → dedup.
    Trả về DataFrame đã sạch, sẵn sàng upsert vào Silver.
    """
    df_bronze = spark.read.format("delta").load(bronze_path)

    # --- Ép kiểu & tính toán ---
    df_typed = (
        df_bronze
        .withColumn("order_date",   F.to_date("order_date", "yyyy-MM-dd"))
        .withColumn("quantity",     F.col("quantity").cast(IntegerType()))
        .withColumn("unit_price",   F.col("unit_price").cast(DoubleType()))
        .withColumn("total_amount", F.col("quantity") * F.col("unit_price"))
        .withColumn("status",       F.lower(F.trim(F.col("status"))))
    )

    # --- Validate: lọc bỏ bản ghi lỗi ---
    df_valid = (
        df_typed
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("customer_id").isNotNull())
        .filter(F.col("quantity") > 0)
        .filter(F.col("unit_price") > 0)
        .filter(F.col("status").isin(list(VALID_STATUSES)))
        .filter(F.col("order_date").isNotNull())
    )

    n_bronze = df_bronze.count()
    n_valid  = df_valid.count()
    print(f"[Silver] Bronze rows   : {n_bronze:,}")
    print(f"[Silver] Sau validate  : {n_valid:,}  (loại {n_bronze - n_valid:,} bản ghi lỗi)")

    # --- Dedup bằng Window function ---
    # Với mỗi order_id, chỉ giữ bản ghi có ingested_at mới nhất.
    # Cách này đúng hơn dropDuplicates vì xử lý được duplicate có timestamp khác nhau.
    window_spec = Window.partitionBy("order_id").orderBy(F.col("ingested_at").desc())
    df_deduped = (
        df_valid
        .withColumn("_row_num", F.row_number().over(window_spec))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num", "ingested_at", "_bronze_ingested_at", "_source_file")
        .withColumn("_silver_updated_at", F.current_timestamp())
    )

    n_deduped = df_deduped.count()
    print(f"[Silver] Sau dedup     : {n_deduped:,}  (loại {n_valid - n_deduped:,} duplicate)")
    return df_deduped


def _merge_into_silver(spark, df_clean, silver_path: str) -> None:
    """
    Bước 5: ACID Upsert (MERGE INTO) vào Silver Delta table.

    Chiến lược:
    - MATCHED     → UPDATE (cập nhật nếu đơn hàng đã tồn tại)
    - NOT MATCHED → INSERT (thêm mới nếu chưa có)
    """
    # Lần đầu: tạo bảng Silver nếu chưa tồn tại
    if not os.path.exists(silver_path):
        print(f"[Silver] Khởi tạo Silver table lần đầu: {silver_path}")
        df_clean.write.format("delta").mode("overwrite").save(silver_path)
        return

    silver_table = DeltaTable.forPath(spark, silver_path)

    (
        silver_table.alias("target")
        .merge(
            df_clean.alias("source"),
            condition="target.order_id = source.order_id"
        )
        .whenMatchedUpdate(set={
            "quantity"          : "source.quantity",
            "unit_price"        : "source.unit_price",
            "total_amount"      : "source.total_amount",
            "status"            : "source.status",
            "order_date"        : "source.order_date",
            "_silver_updated_at": "source._silver_updated_at",
        })
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"[Silver] ✅ MERGE INTO hoàn tất: {silver_path}")


def run(bronze_path: str = BRONZE_PATH, silver_path: str = SILVER_PATH) -> None:
    spark = get_spark("Silver_Orders")

    print("=" * 60)
    print("[Silver] Bắt đầu xử lý orders: Bronze → Silver")
    print("=" * 60)

    df_clean = _clean_and_deduplicate(spark, bronze_path)
    _merge_into_silver(spark, df_clean, silver_path)

    # Kiểm tra kết quả & in lịch sử version
    silver_table = DeltaTable.forPath(spark, silver_path)
    history = silver_table.history(3).select("version", "timestamp", "operation")
    print("\n[Silver] Lịch sử 3 version gần nhất (Time Travel):")
    history.show(truncate=False)

    final_count = spark.read.format("delta").load(silver_path).count()
    print(f"[Silver] Tổng số đơn hàng sạch trong Silver: {final_count:,}")


if __name__ == "__main__":
    run()
