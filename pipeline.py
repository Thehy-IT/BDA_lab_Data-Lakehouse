# =============================================================================
# pipeline.py
# Pipeline chạy toàn bộ Lakehouse: Bronze → Silver → Gold (tuần tự).
#
# Đây là entry point chính của project.
# Chỉ cần chạy 1 lệnh để thực hiện toàn bộ quy trình:
#
#   python generate_data.py   (1 lần duy nhất để sinh data)
#   python pipeline.py        (chạy pipeline)
#
# Pipeline sẽ:
#   Step 0 — Kiểm tra dữ liệu đầu vào
#   Step 1 — Bronze: ingest orders CSV → Delta (append-only, giữ duplicate)
#   Step 2 — Silver: clean + dedup orders (MERGE INTO, ACID)
#   Step 3 — Silver: clean customers
#   Step 4 — Gold  : xây dựng fact_orders (join 3 bảng)
#   Step 5 — Gold  : tổng hợp agg_revenue_daily
#   Step 6 — In báo cáo tóm tắt và thông tin Time Travel
# =============================================================================

import sys
import time
import os

# Đảm bảo import được các module trong project
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from bronze.ingest_orders    import ingest_orders
from silver.silver_orders    import run as silver_orders
from silver.silver_customers import run as silver_customers
from gold.gold_fact_orders   import run as gold_fact_orders
from gold.gold_agg_revenue   import run as gold_agg_revenue
from config.spark_session    import get_spark


# ---------------------------------------------------------------------------
# Tiện ích log
# ---------------------------------------------------------------------------
def _step(n: int, total: int, title: str) -> None:
    print()
    print("=" * 65)
    print(f"  STEP {n}/{total} — {title}")
    print("=" * 65)


def _check_input_data() -> bool:
    """Kiểm tra dữ liệu raw đã được sinh chưa."""
    required = ["data/raw/orders.csv", "data/raw/customers.csv", "data/raw/products.csv"]
    missing = [f for f in required if not os.path.exists(f)]
    if missing:
        print("❌ Thiếu file dữ liệu đầu vào:")
        for f in missing:
            print(f"   - {f}")
        print("\n👉 Hãy chạy trước: python generate_data.py")
        return False
    return True


def _print_time_travel_info(spark) -> None:
    """
    In thông tin Time Travel của Silver orders — điểm nổi bật của Delta Lake.
    Đây là feature giúp trả lời câu hỏi: "Dữ liệu trông như thế nào trước khi chạy pipeline?"
    """
    from delta.tables import DeltaTable
    silver_path = "data/lakehouse/silver/orders"

    if not os.path.exists(silver_path):
        return

    print("\n📜 TIME TRAVEL — Lịch sử version của Silver orders:")
    silver_table = DeltaTable.forPath(spark, silver_path)
    silver_table.history().select("version", "timestamp", "operation", "operationMetrics").show(5, truncate=60)

    # Demo đọc version cũ (nếu có >= 2 version)
    history_count = silver_table.history().count()
    if history_count >= 2:
        print("🔍 Demo Time Travel — Đọc dữ liệu tại version 0 (trước khi MERGE):")
        df_v0 = spark.read.format("delta").option("versionAsOf", 0).load(silver_path)
        print(f"   Số bản ghi tại version 0: {df_v0.count():,}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    total_steps = 6
    start_time  = time.time()

    print("  E-COMMERCE DATA LAKEHOUSE PIPELINE")
    print("  Bronze → Silver → Gold")

    # --- Step 0: Kiểm tra đầu vào ---
    _step(0, total_steps, "Kiểm tra dữ liệu đầu vào")
    if not _check_input_data():
        sys.exit(1)
    print("✅ Đầy đủ file đầu vào")

    # --- Step 1: Bronze ---
    _step(1, total_steps, "BRONZE — Ingest orders (append-only, giữ nguyên duplicate)")
    n_bronze = ingest_orders()

    # --- Step 2: Silver orders ---
    _step(2, total_steps, "SILVER — Clean + Dedup orders (MERGE INTO ACID)")
    silver_orders()

    # --- Step 3: Silver customers ---
    _step(3, total_steps, "SILVER — Clean customers (validate + overwrite)")
    silver_customers()

    # --- Step 4: Gold fact ---
    _step(4, total_steps, "GOLD — Xây dựng fact_orders (Star Schema join)")
    gold_fact_orders()

    # --- Step 5: Gold agg ---
    _step(5, total_steps, "GOLD — Tổng hợp agg_revenue_daily")
    gold_agg_revenue()

    # --- Step 6: Summary ---
    _step(6, total_steps, "BÁO CÁO KẾT QUẢ & TIME TRAVEL")
    spark = get_spark()
    _print_time_travel_info(spark)

    elapsed = time.time() - start_time
    print("\n" + "✅ " * 20)
    print(f"  Pipeline hoàn tất trong {elapsed:.1f} giây")
    print(f"  Bronze rows ingested : {n_bronze:,} (bao gồm duplicate)")
    print(f"  Lakehouse output     : data/lakehouse/")
    print("✅ " * 20 + "\n")


if __name__ == "__main__":
    main()
