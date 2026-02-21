"""
stream_data_producer.py
Produces real-time stock market data to Kafka every 2 seconds.
"""
import json
import time
import logging
import os
from datetime import datetime
from confluent_kafka import Producer
import yfinance as yf

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_REALTIME", "stock-market-realtime")
SYMBOLS = ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "TSLA", "NVDA"]
INTERVAL_SECONDS = 2


def delivery_report(err, msg):
    if err:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.debug(f"Delivered to {msg.topic()} [{msg.partition()}]")


def fetch_realtime_data(symbol: str) -> dict:
    ticker = yf.Ticker(symbol)
    data = ticker.history(period="1d", interval="1m")
    if data.empty:
        return None
    latest = data.iloc[-1]
    return {
        "symbol": symbol,
        "timestamp": datetime.utcnow().isoformat(),
        "price": round(float(latest["Close"]), 4),
        "open": round(float(latest["Open"]), 4),
        "high": round(float(latest["High"]), 4),
        "low": round(float(latest["Low"]), 4),
        "volume": int(latest["Volume"]),
    }


def main():
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVER})
    logger.info(f"Starting real-time producer for {SYMBOLS}")
    try:
        while True:
            for symbol in SYMBOLS:
                record = fetch_realtime_data(symbol)
                if record:
                    producer.produce(
                        KAFKA_TOPIC,
                        key=symbol,
                        value=json.dumps(record),
                        callback=delivery_report,
                    )
                    logger.info(f"Produced: {symbol} @ {record['price']}")
            producer.poll(0)
            time.sleep(INTERVAL_SECONDS)
    except KeyboardInterrupt:
        logger.info("Stopping producer")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
