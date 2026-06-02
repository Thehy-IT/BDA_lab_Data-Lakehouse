# =============================================================================
# gold/gold_fact_orders.py
# Layer GOLD — Bảng Fact chứa đơn hàng đã được enrich đầy đủ thông tin.
#
# Join Silver orders + Silver customers + products CSV
# → tạo bảng fact_orders dạng Star Schema, sẵn sàng cho BI / báo cáo.
#
# Cột bổ sung tại Gold:
#   - customer_name, region       (từ customers)
#   - product_name, category      (từ products)
#   - order_year, order_month     (partition keys, tối ưu query theo thời gian)
#   - is_high_value               (business rule: đơn > 1 triệu VNĐ)
#
# Chạy : python -m gold.gold_fact_orders
# =============================================================================

import os
import sys

from pyspark.sql import functions as F

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from config.spark_session import get_spark

# ---------------------------------------------------------------------------
# Đường dẫn
# ---------------------------------------------------------------------------
SILVER_ORDERS_PATH    = "data/lakehouse/silver/orders"
SILVER_CUSTOMERS_PATH = "data/lakehouse/silver/customers"
PRODUCTS_CSV_PATH     = "data/raw/products.csv"
GOLD_FACT_PATH        = "data/lakehouse/gold/fact_orders"


def run(
    silver_orders_path   : str = SILVER_ORDERS_PATH,
    silver_customers_path: str = SILVER_CUSTOMERS_PATH,
    products_csv_path    : str = PRODUCTS_CSV_PATH,
    gold_path            : str = GOLD_FACT_PATH,
) -> None:
    spark = get_spark("Gold_FactOrders")

    print("=" * 60)
    print("[Gold] Bắt đầu xây dựng fact_orders")
    print("=" * 60)

    # --- Đọc Silver ---
    df_orders = spark.read.format("delta").load(silver_orders_path)
    df_customers = spark.read.format("delta").load(silver_customers_path)

    # --- Đọc Products (dimension nhỏ, đọc thẳng từ CSV) ---
    df_products = (
        spark.read
        .option("header", "true")
        .option("encoding", "utf-8")
        .csv(products_csv_path)
        .select("product_id", "name", "category")
        .withColumnRenamed("name", "product_name")
    )

    # --- Join để enrich ---
    df_fact = (
        df_orders
        # JOIN customers (left join: giữ đơn dù customer bị xoá)
        .join(
            df_customers.select("customer_id", "name", "region")
                        .withColumnRenamed("name", "customer_name"),
            on="customer_id",
            how="left",
        )
        # JOIN products
        .join(df_products, on="product_id", how="left")
        # Thêm cột phân tích
        .withColumn("order_year",    F.year("order_date"))
        .withColumn("order_month",   F.month("order_date"))
        .withColumn("order_quarter", F.quarter("order_date"))
        .withColumn(
            "is_high_value",
            F.when(F.col("total_amount") >= 1_000_000, True).otherwise(False)
        )
        .withColumn("_gold_created_at", F.current_timestamp())
        # Sắp xếp cột cho dễ đọc
        .select(
            "order_id", "order_date", "order_year", "order_month", "order_quarter",
            "customer_id", "customer_name", "region",
            "product_id", "product_name", "category",
            "quantity", "unit_price", "total_amount", "is_high_value",
            "status", "_gold_created_at",
        )
    )

    row_count = df_fact.count()
    print(f"[Gold] Tổng đơn hàng trong fact_orders: {row_count:,}")

    # --- Ghi Gold (partition theo năm-tháng để tối ưu query) ---
    (
        df_fact.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("order_year", "order_month")
        .save(gold_path)
    )
    print(f"[Gold] ✅ Đã ghi fact_orders: {gold_path}")

    # --- Preview kết quả ---
    print("\n[Gold] Mẫu 5 bản ghi fact_orders:")
    df_fact.select(
        "order_id", "customer_name", "region",
        "product_name", "category", "total_amount", "is_high_value", "status"
    ).show(5, truncate=30)

    print("\n[Gold] Thống kê nhanh theo status:")
    df_fact.groupBy("status").agg(
        F.count("*").alias("so_don"),
        F.round(F.sum("total_amount") / 1e6, 2).alias("doanh_thu_trieu_vnd")
    ).orderBy("status").show()


if __name__ == "__main__":
    run()
