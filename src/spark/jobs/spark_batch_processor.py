"""
spark_batch_processor.py
Spark batch job: reads raw CSV from MinIO, transforms, writes Parquet.
"""
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType, DateType

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "stock-market-data")

spark = (
    SparkSession.builder
    .appName("StockBatchProcessor")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

schema = StructType([
    StructField("symbol", StringType()),
    StructField("date", StringType()),
    StructField("daily_open", FloatType()),
    StructField("daily_high", FloatType()),
    StructField("daily_low", FloatType()),
    StructField("daily_close", FloatType()),
    StructField("daily_volume", LongType()),
    StructField("batch_load_timestamp", StringType()),
])

raw_path = f"s3a://{MINIO_BUCKET}/raw/historical/"
df = spark.read.csv(raw_path, header=True, schema=schema)

df_clean = (
    df
    .withColumn("trade_date", F.to_date("date"))
    .withColumnRenamed("daily_open", "open_price")
    .withColumnRenamed("daily_high", "high_price")
    .withColumnRenamed("daily_low", "low_price")
    .withColumnRenamed("daily_close", "close_price")
    .withColumnRenamed("daily_volume", "volume")
    .withColumn("batch_loaded_at", F.to_timestamp("batch_load_timestamp"))
    .drop("date", "batch_load_timestamp")
    .filter(F.col("close_price") > 0)
    .filter(F.col("symbol").isNotNull())
    .dropDuplicates(["symbol", "trade_date"])
)

out_path = f"s3a://{MINIO_BUCKET}/processed/historical/"
df_clean.write.mode("overwrite").partitionBy("symbol").parquet(out_path)

print(f"Batch processing complete. Records written: {df_clean.count()}")
spark.stop()
