"""
load_to_snowflake.py
Loads processed batch Parquet files from MinIO into Snowflake.
"""
import logging
import os
import io
from datetime import datetime
import boto3
import pandas as pd
import snowflake.connector

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "stock-market-data")

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE", "SYSADMIN")

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS STOCKMARKETBATCH.PUBLIC.DAILY_STOCK_METRICS (
    SYMBOL          VARCHAR(10),
    DATE            DATE,
    DAILY_OPEN      FLOAT,
    DAILY_HIGH      FLOAT,
    DAILY_LOW       FLOAT,
    DAILY_CLOSE     FLOAT,
    DAILY_VOLUME    BIGINT,
    BATCH_LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
"""

MERGE_SQL = """
MERGE INTO STOCKMARKETBATCH.PUBLIC.DAILY_STOCK_METRICS t
USING (SELECT %s AS SYMBOL, %s::DATE AS DATE, %s AS DAILY_OPEN,
              %s AS DAILY_HIGH, %s AS DAILY_LOW, %s AS DAILY_CLOSE,
              %s AS DAILY_VOLUME, CURRENT_TIMESTAMP() AS BATCH_LOAD_TIMESTAMP) s
ON t.SYMBOL = s.SYMBOL AND t.DATE = s.DATE
WHEN MATCHED THEN UPDATE SET
    t.DAILY_OPEN = s.DAILY_OPEN, t.DAILY_HIGH = s.DAILY_HIGH,
    t.DAILY_LOW = s.DAILY_LOW, t.DAILY_CLOSE = s.DAILY_CLOSE,
    t.DAILY_VOLUME = s.DAILY_VOLUME, t.BATCH_LOAD_TIMESTAMP = s.BATCH_LOAD_TIMESTAMP
WHEN NOT MATCHED THEN INSERT VALUES (
    s.SYMBOL, s.DATE, s.DAILY_OPEN, s.DAILY_HIGH, s.DAILY_LOW,
    s.DAILY_CLOSE, s.DAILY_VOLUME, s.BATCH_LOAD_TIMESTAMP
)
"""


def get_s3_client():
    return boto3.client(
        "s3", endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY, aws_secret_access_key=MINIO_SECRET_KEY,
    )


def get_snowflake_conn():
    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT, user=SNOWFLAKE_USER, password=SNOWFLAKE_PASSWORD,
        warehouse=SNOWFLAKE_WAREHOUSE, role=SNOWFLAKE_ROLE,
    )


def main():
    s3 = get_s3_client()
    conn = get_snowflake_conn()
    cursor = conn.cursor()
    cursor.execute(CREATE_TABLE_SQL)

    paginator = s3.get_paginator("list_objects_v2")
    prefix = "processed/historical/"
    total = 0

    for page in paginator.paginate(Bucket=MINIO_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if not obj["Key"].endswith(".parquet"):
                continue
            response = s3.get_object(Bucket=MINIO_BUCKET, Key=obj["Key"])
            df = pd.read_parquet(io.BytesIO(response["Body"].read()))
            for _, row in df.iterrows():
                cursor.execute(MERGE_SQL, (
                    row["symbol"], row["trade_date"], row["open_price"],
                    row["high_price"], row["low_price"], row["close_price"], row["volume"],
                ))
            total += len(df)
            logger.info(f"Loaded {len(df)} rows from {obj['Key']}")

    conn.commit()
    cursor.close()
    conn.close()
    logger.info(f"Batch load complete. Total rows upserted: {total}")


if __name__ == "__main__":
    main()
