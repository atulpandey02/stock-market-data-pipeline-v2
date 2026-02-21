"""
spark_stream_batch_processor.py
Spark job: reads raw realtime CSV from MinIO, computes window analytics, writes Parquet.
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType, TimestampType

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "stock-market-data")

spark = (
    SparkSession.builder
    .appName("StockStreamBatchProcessor")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

schema = StructType([
    StructField("symbol", StringType()),
    StructField("timestamp", StringType()),
    StructField("price", FloatType()),
    StructField("open", FloatType()),
    StructField("high", FloatType()),
    StructField("low", FloatType()),
    StructField("volume", LongType()),
])

raw_path = f"s3a://{MINIO_BUCKET}/raw/realtime/"
df = spark.read.csv(raw_path, header=True, schema=schema)

df = df.withColumn("event_time", F.to_timestamp("timestamp")).drop("timestamp")

# 15-minute window aggregations
window_15m = F.window("event_time", "15 minutes")
window_1h = F.window("event_time", "1 hour")

agg_15m = (
    df.groupBy("symbol", window_15m)
    .agg(
        F.avg("price").alias("ma_15m"),
        F.stddev("price").alias("volatility_15m"),
        F.sum("volume").alias("volume_sum_15m"),
    )
    .withColumn("window_start", F.col("window.start"))
    .withColumn("window_end", F.col("window.end"))
    .drop("window")
)

agg_1h = (
    df.groupBy("symbol", window_1h)
    .agg(F.avg("price").alias("ma_1h"))
    .withColumn("window_start_1h", F.col("window.start"))
    .drop("window")
)

result = (
    agg_15m.join(
        agg_1h,
        (agg_15m.symbol == agg_1h.symbol) &
        (agg_15m.window_start >= agg_1h.window_start_1h) &
        (agg_15m.window_start < F.col("window_start_1h") + F.expr("INTERVAL 1 HOUR")),
        how="left"
    )
    .drop(agg_1h.symbol)
    .drop("window_start_1h")
    .withColumn("load_timestamp", F.current_timestamp())
)

out_path = f"s3a://{MINIO_BUCKET}/processed/realtime/"
result.write.mode("append").partitionBy("symbol").parquet(out_path)

print(f"Stream batch processing complete. Records: {result.count()}")
spark.stop()
