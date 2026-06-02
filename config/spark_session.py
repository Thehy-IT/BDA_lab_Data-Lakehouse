# =============================================================================
# config/spark_session.py
# Khởi tạo SparkSession với Delta Lake extensions.
# File này được dùng chung cho tất cả các layer (Bronze / Silver / Gold).
# =============================================================================

from pyspark.sql import SparkSession


def get_spark(app_name: str = "EcommerceLakehouse") -> SparkSession:
    """
    Tạo và trả về một SparkSession đã được cấu hình Delta Lake.

    Tham số
    -------
    app_name : str
        Tên hiển thị của Spark application (mặc định: "EcommerceLakehouse").

    Trả về
    ------
    SparkSession
        Instance SparkSession sẵn sàng đọc/ghi Delta table.

    Ví dụ sử dụng
    -------------
    >>> from config.spark_session import get_spark
    >>> spark = get_spark("MyJob")
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        # --- Delta Lake catalog & extensions ---
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        # --- Tối ưu cho môi trường local (lab) ---
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")   # giảm overhead khi data nhỏ
        .master("local[*]")
        .getOrCreate()
    )

    # Bật tính năng tự động merge schema khi ghi (hữu ích ở Silver layer)
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    # Tắt log INFO thừa, chỉ hiện WARNING trở lên
    spark.sparkContext.setLogLevel("WARN")

    return spark
