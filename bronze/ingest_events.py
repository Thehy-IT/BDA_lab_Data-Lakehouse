# =============================================================================
# bronze/ingest_events.py
# Layer BRONZE — Ingestion sự kiện người dùng qua Structured Streaming.
#
# Trong lab này, nguồn streaming được mô phỏng bằng cách đọc file JSON
# từ thư mục (socket source không ổn định; file source dễ demo hơn).
# Trong production, thay "json" source bằng Kafka source.
#
# Chạy : python -m bronze.ingest_events
#         Ctrl+C để dừng stream sau khi thấy kết quả.
# =============================================================================

import os
import sys
import json
import random
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType,
)

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from config.spark_session import get_spark

# ---------------------------------------------------------------------------
# Đường dẫn
# ---------------------------------------------------------------------------
STREAM_SOURCE_DIR  = "data/stream_input"     # thư mục Spark "theo dõi" file mới
BRONZE_EVENTS_PATH = "data/lakehouse/bronze/events"
CHECKPOINT_PATH    = "data/checkpoints/bronze_events"

# ---------------------------------------------------------------------------
# Schema sự kiện clickstream
# ---------------------------------------------------------------------------
EVENT_SCHEMA = StructType([
    StructField("event_id",   StringType(), nullable=False),
    StructField("session_id", StringType(), nullable=True),
    StructField("customer_id",StringType(), nullable=True),
    StructField("event_type", StringType(), nullable=True),   # view / add_to_cart / purchase
    StructField("product_id", StringType(), nullable=True),
    StructField("amount",     DoubleType(), nullable=True),
    StructField("event_time", StringType(), nullable=True),
])

# ---------------------------------------------------------------------------
# Sinh file JSON mẫu để stream đọc (thay thế Kafka trong lab)
# ---------------------------------------------------------------------------
EVENT_TYPES = ["page_view", "product_view", "add_to_cart", "checkout", "purchase"]

def generate_event_file(n: int = 50) -> None:
    """Tạo 1 file JSON Lines mô phỏng batch sự kiện từ frontend."""
    os.makedirs(STREAM_SOURCE_DIR, exist_ok=True)
    events = []
    base = datetime.now()
    for i in range(n):
        events.append({
            "event_id"   : f"EVT-{int(base.timestamp())}-{i:04d}",
            "session_id" : f"SESS-{random.randint(1, 50):04d}",
            "customer_id": f"CUST-{random.randint(1, 200):04d}",
            "event_type" : random.choice(EVENT_TYPES),
            "product_id" : f"PROD-{random.randint(1, 50):04d}",
            "amount"     : round(random.uniform(50_000, 2_000_000), -3),
            "event_time" : (base - timedelta(seconds=random.randint(0, 300))).isoformat(),
        })

    filename = f"{STREAM_SOURCE_DIR}/events_{int(base.timestamp())}.json"
    with open(filename, "w", encoding="utf-8") as f:
        for e in events:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")
    print(f"[Bronze] Đã tạo file stream mẫu: {filename} ({n} events)")


def ingest_events_stream() -> None:
    """
    Khởi động Structured Streaming job:
    đọc JSON từ thư mục → parse schema → ghi Delta (append).
    
    Trigger: ProcessingTime mỗi 10 giây (phù hợp lab).
    """
    spark = get_spark("Bronze_IngestEvents")

    # Sinh dữ liệu mẫu trước khi stream bắt đầu
    generate_event_file(n=100)

    print(f"[Bronze] Khởi động Structured Streaming, nguồn: {STREAM_SOURCE_DIR}")

    df_stream = (
        spark.readStream
        .format("json")
        .schema(EVENT_SCHEMA)
        .option("maxFilesPerTrigger", 1)    # xử lý từng file mỗi trigger (demo rõ hơn)
        .load(STREAM_SOURCE_DIR)
    )

    # Thêm metadata streaming
    df_enriched = df_stream.withColumns({
        "_bronze_ingested_at": F.current_timestamp(),
        "_source"            : F.lit("clickstream_stream"),
    })

    query = (
        df_enriched.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="10 seconds")
        .start(BRONZE_EVENTS_PATH)
    )

    print("[Bronze] Stream đang chạy... Nhấn Ctrl+C để dừng.")
    print(f"[Bronze] Checkpoint: {CHECKPOINT_PATH}")
    print(f"[Bronze] Output    : {BRONZE_EVENTS_PATH}")

    try:
        query.awaitTermination(timeout=60)   # tự dừng sau 60s trong lab
    except KeyboardInterrupt:
        print("\n[Bronze] Đã dừng stream.")
    finally:
        query.stop()
        print(f"[Bronze] ✅ Tổng batch đã xử lý: {query.lastProgress}")


if __name__ == "__main__":
    ingest_events_stream()
