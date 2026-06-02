# E-Commerce Data Lakehouse

**Môn học:** Data Engineering  
**Bài tập:** Homework 07 — Lakehouse Architecture  
**Vai trò mô phỏng:** Data Engineer tại công ty thương mại điện tử toàn cầu

---

## 1. Bối cảnh & Vấn đề

Công ty đang vận hành song song hai hệ thống lưu trữ dữ liệu và cả hai đều gặp vấn đề:

| Hệ thống hiện tại | Vấn đề |
|---|---|
| **Data Lake** (S3 / object storage) | Không có schema, dữ liệu lộn xộn, tồn tại nhiều bản ghi trùng lặp do lỗi double-insert từ các service |
| **Data Warehouse** (Redshift / BigQuery) | Chi phí vận hành cao, ETL chậm, khó hỗ trợ real-time; mỗi lần schema thay đổi mất nhiều ngày triển khai |

Analytics team cần dữ liệu **đáng tin cậy** và **gần real-time** để ra quyết định kinh doanh. Hệ thống hiện tại không đáp ứng được yêu cầu này.

---

## 2. Giải pháp: Data Lakehouse

**Data Lakehouse** kết hợp ưu điểm của cả hai paradigm:

```
Data Lake       →  lưu trữ rẻ (Parquet trên object storage)
Data Warehouse  →  ACID, schema enforcement, query hiệu năng cao
                   ↓
Data Lakehouse  =  cả hai, nhờ Delta Lake transaction log
```

### So sánh 3 paradigm

| Tiêu chí | Data Lake | Data Warehouse | **Data Lakehouse** |
|---|---|---|---|
| Chi phí lưu trữ | ✅ Thấp | ❌ Cao | ✅ Thấp |
| ACID transactions | ❌ Không | ✅ Có | ✅ Có |
| Schema enforcement | ❌ Schema-on-read | ✅ Schema-on-write | ✅ Có (linh hoạt hơn DWH) |
| Hỗ trợ streaming | ⚠️ Phức tạp | ❌ Khó | ✅ Native (Structured Streaming) |
| Time Travel | ❌ Không | ❌ Không | ✅ Có (versioning) |
| Upsert / MERGE | ❌ Không | ✅ Có | ✅ Có (MERGE INTO) |
| Phù hợp ML | ✅ Tốt | ⚠️ Trung bình | ✅ Tốt |

**Kết luận:** Lakehouse là lựa chọn tối ưu cho bài toán của công ty — chi phí thấp như Data Lake, nhưng đáng tin cậy và có thể query như Data Warehouse.

---

## 3. Kiến trúc hệ thống

```
[Sources]                [Bronze]              [Silver]              [Gold]
──────────               ────────              ────────              ──────
orders.csv    ──append──▶ orders Delta  ──MERGE INTO──▶ orders   ──join──▶ fact_orders
events JSON   ──stream──▶ events Delta            ──clean──▶ customers         │
customers.csv ────────────────────────────────────────────────────────▶ agg_revenue_daily
products.csv  ──────────────────────────────────────────────────────────────────────────▶

             Apache Spark (Batch + Structured Streaming)
             Delta Lake (ACID · Time Travel · Schema Enforcement · MERGE INTO)
             Object Storage: data/lakehouse/ (Parquet + transaction log)
```

### Nguyên tắc từng layer

**Bronze — Raw ingestion**
- Dữ liệu nạp nguyên trạng, không xử lý
- Append-only: giữ toàn bộ lịch sử kể cả bản ghi lỗi
- Thêm metadata: `_bronze_ingested_at`, `_source_file`

**Silver — Cleansed & Deduplicated**
- Ép kiểu chính xác (string → date, string → double)
- Dedup bằng Window function: giữ bản ghi mới nhất theo `ingested_at`
- MERGE INTO (ACID): upsert đảm bảo idempotent
- Validate: lọc null, range check, enum check

**Gold — Business Ready**
- Star schema: fact_orders join với customers, products
- Aggregation: doanh thu theo ngày × region × category
- Business rules: `is_high_value`, `order_quarter`
- Partition theo `order_year`, `order_month` để tối ưu query

---

## 4. Cấu trúc thư mục

```
ecommerce-lakehouse/
├── README.md
├── requirements.txt
├── generate_data.py          # Sinh dữ liệu mẫu (Faker)
├── pipeline.py               # Entry point: chạy toàn bộ pipeline
│
├── config/
│   └── spark_session.py      # SparkSession + Delta config (dùng chung)
│
├── bronze/
│   ├── ingest_orders.py      # CSV → Bronze Delta (append)
│   └── ingest_events.py      # JSON Stream → Bronze Delta (Structured Streaming)
│
├── silver/
│   ├── silver_orders.py      # Dedup + MERGE INTO + schema enforce
│   └── silver_customers.py   # Validate + clean customers
│
├── gold/
│   ├── gold_fact_orders.py   # Star schema fact table
│   └── gold_agg_revenue.py   # Daily revenue aggregation
│
├── notebooks/
│   └── demo.ipynb            # EDA, Time Travel, DESCRIBE HISTORY
│
└── data/                     # (tự sinh khi chạy, không commit lên git)
    ├── raw/                  # CSV gốc từ generate_data.py
    ├── stream_input/         # JSON files cho streaming demo
    ├── lakehouse/
    │   ├── bronze/
    │   ├── silver/
    │   └── gold/
    └── checkpoints/
```

---

## 5. Hướng dẫn cài đặt & chạy

### Yêu cầu hệ thống
- Python 3.10+
- Java 11 (bắt buộc cho PySpark)

### Cài đặt

```bash
# Clone hoặc giải nén project
cd ecommerce-lakehouse

# Tạo virtual environment
python -m venv .venv
source .venv/bin/activate          # Linux/Mac
# .venv\Scripts\activate           # Windows

# Cài thư viện
pip install -r requirements.txt
```

### Chạy pipeline

```bash
# Bước 1: Sinh dữ liệu mẫu (chỉ cần chạy 1 lần)
python generate_data.py

# Bước 2: Chạy toàn bộ pipeline Bronze → Silver → Gold
python pipeline.py

# (Tùy chọn) Chạy từng layer riêng lẻ
python -m bronze.ingest_orders
python -m silver.silver_orders
python -m gold.gold_fact_orders
```

### Kết quả mong đợi

```
✅ customers : 200 rows
✅ products  : 50 rows
✅ orders    : 550 rows (500 + ~50 duplicate cố ý)

[Bronze] Số bản ghi đọc được (bao gồm duplicate): 550
[Silver] Sau dedup     : 500  (loại 50 duplicate)
[Silver] ✅ MERGE INTO hoàn tất
[Gold]   ✅ fact_orders: 500 rows
[Gold]   ✅ agg_revenue_daily: ~N tổ hợp
```

---

## 6. Các tính năng Delta Lake được minh họa

### ACID Transactions — MERGE INTO
```python
# silver/silver_orders.py
silver_table.alias("target")
.merge(df_clean.alias("source"), "target.order_id = source.order_id")
.whenMatchedUpdate(set={...})
.whenNotMatchedInsertAll()
.execute()
```
Đảm bảo: nếu pipeline bị interrupt giữa chừng, dữ liệu không bị corrupt.

### Schema Enforcement
```python
# Ghi sẽ thất bại nếu DataFrame không đúng schema của bảng
df.write.format("delta").mode("append").save(silver_path)
# → AnalysisException nếu có cột sai kiểu
```

### Time Travel — Versioning
```python
# Đọc dữ liệu tại một thời điểm cụ thể trong quá khứ
df_yesterday = spark.read.format("delta") \
    .option("versionAsOf", 0) \
    .load("data/lakehouse/silver/orders")

# Xem lịch sử thay đổi
DeltaTable.forPath(spark, silver_path).history().show()
```

---

## 7. Ghi chú kỹ thuật

- **Dedup strategy:** Sử dụng Window function `ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY ingested_at DESC)` thay vì `dropDuplicates()` vì các bản ghi duplicate có `ingested_at` khác nhau — `dropDuplicates` sẽ không phát hiện được.
- **Partition strategy:** Bảng Gold partition theo `order_year`, `order_month` để Spark thực hiện partition pruning khi query theo thời gian.
- **Idempotency:** `pipeline.py` có thể chạy lại nhiều lần mà không tạo ra dữ liệu sai — nhờ MERGE INTO ở Silver và `overwrite` ở Gold.
- **Lab simplification:** Streaming dùng file JSON source thay vì Kafka; customers đọc thẳng từ CSV thay vì có Bronze Delta riêng.
