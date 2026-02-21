"""
batch_data_consumer.py
Consumes batch stock data from Kafka and writes to MinIO as CSV.
"""
import csv
import io
import json
import logging
import os
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
from minio import Minio

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_BATCH", "stock-market-batch")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000").replace("http://", "")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "stock-market-data")
CONSUME_TIMEOUT = int(os.getenv("CONSUME_TIMEOUT_SECONDS", "60"))


def get_minio_client():
    return Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)


def upload_to_minio(client, records: list, symbol: str):
    now = datetime.utcnow()
    path = f"raw/historical/year={now.year}/month={now.month:02d}/day={now.day:02d}/{symbol}_{now.strftime('%H%M%S')}.csv"
    fieldnames = ["symbol", "date", "daily_open", "daily_high", "daily_low", "daily_close", "daily_volume", "batch_load_timestamp"]
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
        "group.id": "batch-consumer-group",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([KAFKA_TOPIC])
    minio_client = get_minio_client()

    records_by_symbol: dict = {}
    start_time = datetime.utcnow()

    try:
        while (datetime.utcnow() - start_time).seconds < CONSUME_TIMEOUT:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    break
                logger.error(f"Consumer error: {msg.error()}")
                continue
            record = json.loads(msg.value().decode("utf-8"))
            symbol = record["symbol"]
            records_by_symbol.setdefault(symbol, []).append(record)

        for symbol, records in records_by_symbol.items():
            upload_to_minio(minio_client, records, symbol)
    finally:
        consumer.close()
        logger.info("Consumer closed")


if __name__ == "__main__":
    main()
