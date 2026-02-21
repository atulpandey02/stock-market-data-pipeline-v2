"""
batch_data_producer.py
Fetches historical stock data and publishes to Kafka batch topic.
"""
import json
import logging
import os
from datetime import datetime, timedelta
from confluent_kafka import Producer
import yfinance as yf

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_BATCH", "stock-market-batch")
SYMBOLS = ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "TSLA", "NVDA"]
LOOKBACK_DAYS = int(os.getenv("BATCH_LOOKBACK_DAYS", "365"))


def delivery_report(err, msg):
    if err:
        logger.error(f"Delivery failed: {err}")
    else:
        logger.debug(f"Delivered to {msg.topic()} [{msg.partition()}]")


def fetch_historical_data(symbol: str, start_date: str, end_date: str):
    ticker = yf.Ticker(symbol)
    df = ticker.history(start=start_date, end=end_date)
    records = []
    for date, row in df.iterrows():
        records.append({
            "symbol": symbol,
            "date": date.strftime("%Y-%m-%d"),
            "daily_open": round(float(row["Open"]), 4),
            "daily_high": round(float(row["High"]), 4),
            "daily_low": round(float(row["Low"]), 4),
            "daily_close": round(float(row["Close"]), 4),
            "daily_volume": int(row["Volume"]),
            "batch_load_timestamp": datetime.utcnow().isoformat(),
        })
    return records


def main():
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVER})
    end_date = datetime.utcnow().strftime("%Y-%m-%d")
    start_date = (datetime.utcnow() - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")

    logger.info(f"Fetching historical data from {start_date} to {end_date}")

    total = 0
    for symbol in SYMBOLS:
        records = fetch_historical_data(symbol, start_date, end_date)
        for record in records:
            producer.produce(
                KAFKA_TOPIC,
                key=symbol,
                value=json.dumps(record),
                callback=delivery_report,
            )
        producer.flush()
        total += len(records)
        logger.info(f"Produced {len(records)} records for {symbol}")

    logger.info(f"Batch complete. Total records produced: {total}")


if __name__ == "__main__":
    main()
