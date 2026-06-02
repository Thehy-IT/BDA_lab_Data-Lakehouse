# =============================================================================
# gold/gold_agg_revenue.py
# Layer GOLD — Bảng tổng hợp doanh thu theo ngày, region, category.
#
# Đây là bảng mà Analytics team dùng trực tiếp:
#   - Dashboard doanh thu theo thời gian
#   - So sánh hiệu suất theo khu vực
#   - Phân tích danh mục sản phẩm bán chạy
#
# Input : gold/fact_orders (đã được enrich đầy đủ)
# Output: gold/agg_revenue_daily
#
# Chạy : python -m gold.gold_agg_revenue
# =============================================================================

import os
import sys

from pyspark.sql import functions as F

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from config.spark_session import get_spark

# ---------------------------------------------------------------------------
# Đường dẫn
# ---------------------------------------------------------------------------
GOLD_FACT_PATH = "data/lakehouse/gold/fact_orders"
GOLD_AGG_PATH  = "data/lakehouse/gold/agg_revenue_daily"


def run(fact_path: str = GOLD_FACT_PATH, agg_path: str = GOLD_AGG_PATH) -> None:
    spark = get_spark("Gold_AggRevenue")

    print("=" * 60)
    print("[Gold] Bắt đầu tổng hợp doanh thu: agg_revenue_daily")
    print("=" * 60)

    df_fact = (
        spark.read
        .format("delta")
        .load(fact_path)
        # Chỉ tính đơn hàng thành công (không tính cancelled)
        .filter(F.col("status") != "cancelled")
    )

    # ---------------------------------------------------------------------------
    # Tổng hợp theo ngày + region + category
    # ---------------------------------------------------------------------------
    df_agg = (
        df_fact
        .groupBy("order_date", "order_year", "order_month", "region", "category")
        .agg(
            F.count("order_id")                      .alias("so_don_hang"),
            F.sum("total_amount")                    .alias("doanh_thu"),
            F.avg("total_amount")                    .alias("gia_tri_trung_binh"),
            F.sum("quantity")                        .alias("tong_san_pham_ban"),
            F.countDistinct("customer_id")           .alias("so_khach_hang"),
            F.sum(
                F.when(F.col("is_high_value"), 1).otherwise(0)
            )                                        .alias("so_don_gia_tri_cao"),
        )
        .withColumn("doanh_thu",           F.round("doanh_thu", 0))
        .withColumn("gia_tri_trung_binh",  F.round("gia_tri_trung_binh", 0))
        .withColumn("_gold_created_at",    F.current_timestamp())
        .orderBy("order_date", "region", "category")
    )

    row_count = df_agg.count()
    print(f"[Gold] Tổng số tổ hợp (ngày × region × category): {row_count:,}")

    # --- Ghi Gold ---
    (
        df_agg.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("order_year", "order_month")
        .save(agg_path)
    )
    print(f"[Gold] ✅ Đã ghi agg_revenue_daily: {agg_path}")

    # ---------------------------------------------------------------------------
    # Báo cáo tóm tắt — đây là giá trị thực sự mà Analytics team nhận được
    # ---------------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("[Gold] === BÁO CÁO TÓM TẮT ===")
    print("=" * 60)

    df_result = spark.read.format("delta").load(agg_path)

    print("\n📊 Top 5 ngày có doanh thu cao nhất:")
    (
        df_result
        .groupBy("order_date")
        .agg(F.sum("doanh_thu").alias("tong_doanh_thu"))
        .orderBy(F.col("tong_doanh_thu").desc())
        .withColumn("tong_doanh_thu_trieu", F.round(F.col("tong_doanh_thu") / 1e6, 2))
        .select("order_date", "tong_doanh_thu_trieu")
        .show(5)
    )

    print("📊 Doanh thu theo Region:")
    (
        df_result
        .groupBy("region")
        .agg(
            F.sum("doanh_thu").alias("tong"),
            F.sum("so_don_hang").alias("so_don"),
        )
        .withColumn("doanh_thu_trieu", F.round(F.col("tong") / 1e6, 2))
        .select("region", "doanh_thu_trieu", "so_don")
        .orderBy(F.col("doanh_thu_trieu").desc())
        .show()
    )

    print("📊 Doanh thu theo Category:")
    (
        df_result
        .groupBy("category")
        .agg(F.sum("doanh_thu").alias("tong"))
        .withColumn("doanh_thu_trieu", F.round(F.col("tong") / 1e6, 2))
        .orderBy(F.col("doanh_thu_trieu").desc())
        .show()
    )


if __name__ == "__main__":
    run()
