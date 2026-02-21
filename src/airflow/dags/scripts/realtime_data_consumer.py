"""
realtime_data_consumer.py
Consumes real-time stock data from Kafka and writes to MinIO as CSV.
"""
import csv
import io
import json
import logging
import os
from datetime import datetime
from confluent_kafka import Consumer
from minio import Minio

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_REALTIME", "stock-market-realtime")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000").replace("http://", "")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "stock-market-data")
COLLECT_DURATION = int(os.getenv("COLLECT_DURATION_SECONDS", "180"))


def get_minio_client():
    return Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)


def upload_to_minio(client, records: list):
    now = datetime.utcnow()
    path = f"raw/realtime/year={now.year}/month={now.month:02d}/day={now.day:02d}/hour={now.hour:02d}/stock_data_{now.strftime('%Y%m%d_%H%M')}.csv"
    fieldnames = ["symbol", "timestamp", "price", "open", "high", "low", "volume"]
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(records)
    data = buf.getvalue().encode("utf-8")
    client.put_object(MINIO_BUCKET, path, io.BytesIO(data), len(data), content_type="text/csv")
    logger.info(f"Uploaded {len(records)} records to {path}")


def main():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVER,
        "group.id": "realtime-consumer-group",
        "auto.offset.reset": "latest",
    })
    consumer.subscribe([KAFKA_TOPIC])
    minio_client = get_minio_client()

    records = []
    start_time = datetime.utcnow()

    try:
        while (datetime.utcnow() - start_time).seconds < COLLECT_DURATION:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue
            records.append(json.loads(msg.value().decode("utf-8")))
            if len(records) % 100 == 0:
                logger.info(f"Collected {len(records)} records so far")

        if records:
            upload_to_minio(minio_client, records)
        logger.info(f"Done. Total records collected: {len(records)}")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
