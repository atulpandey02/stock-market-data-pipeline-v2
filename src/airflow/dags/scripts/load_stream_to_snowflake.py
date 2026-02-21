"""
load_stream_to_snowflake.py
Loads processed streaming Parquet files from MinIO into Snowflake.
"""
import logging
import os
import io
from datetime import datetime, timedelta
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
CREATE TABLE IF NOT EXISTS STOCKMARKETSTREAM.PUBLIC.REALTIME_STOCK_ANALYTICS (
    SYMBOL          VARCHAR(10),
    WINDOW_START    TIMESTAMP_NTZ,
    WINDOW_END      TIMESTAMP_NTZ,
    MA_15M          FLOAT,
    MA_1H           FLOAT,
    VOLATILITY_15M  FLOAT,
    VOLUME_SUM_15M  BIGINT,
    LOAD_TIMESTAMP  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
"""

INSERT_SQL = """
INSERT INTO STOCKMARKETSTREAM.PUBLIC.REALTIME_STOCK_ANALYTICS
    (SYMBOL, WINDOW_START, WINDOW_END, MA_15M, MA_1H, VOLATILITY_15M, VOLUME_SUM_15M, LOAD_TIMESTAMP)
VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP())
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
    prefix = "processed/realtime/"
    total = 0

    for page in paginator.paginate(Bucket=MINIO_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if not obj["Key"].endswith(".parquet"):
                continue
            response = s3.get_object(Bucket=MINIO_BUCKET, Key=obj["Key"])
            df = pd.read_parquet(io.BytesIO(response["Body"].read()))
            for _, row in df.iterrows():
                cursor.execute(INSERT_SQL, (
                    row.get("symbol"), row.get("window_start"),
                    row.get("window_end"), row.get("ma_15m"),
                    row.get("ma_1h"), row.get("volatility_15m"), row.get("volume_sum_15m"),
                ))
            total += len(df)
            logger.info(f"Loaded {len(df)} rows from {obj['Key']}")

    conn.commit()
    cursor.close()
    conn.close()
    logger.info(f"Stream load complete. Total rows inserted: {total}")


if __name__ == "__main__":
    main()
