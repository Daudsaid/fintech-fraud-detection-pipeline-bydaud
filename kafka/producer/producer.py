import os
import json
import time
import logging
from decimal import Decimal
from datetime import datetime
from sqlalchemy import create_engine, text
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DB_URL       = os.getenv("DATABASE_URL", "postgresql://daudsaid:9848@localhost:5433/fraud_db")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC        = "fraud.transactions"
BATCH_SIZE   = 500


def serialise(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Not serialisable: {type(obj)}")


def stream(limit: int = None) -> None:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v, default=serialise).encode("utf-8"),
        acks="all",
        retries=3,
    )

    engine = create_engine(DB_URL)
    query  = "SELECT * FROM raw_transactions ORDER BY transaction_dt"
    if limit:
        query += f" LIMIT {limit}"

    log.info("Streaming to topic '%s'", TOPIC)
    sent = 0

    with engine.connect() as conn:
        result = conn.execution_options(stream_results=True).execute(text(query))
        cols   = list(result.keys())
        while True:
            rows = result.fetchmany(BATCH_SIZE)
            if not rows:
                break
            for row in rows:
                producer.send(TOPIC, value=dict(zip(cols, row)))
                sent += 1
            if sent % 10_000 == 0:
                log.info("Sent %d messages", sent)

    producer.flush()
    log.info("Done. Total sent: %d", sent)


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--limit", type=int, default=10000)
    args = p.parse_args()
    stream(limit=args.limit)
