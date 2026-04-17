import os
import json
import logging
from kafka import KafkaConsumer
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DB_URL       = os.getenv("DATABASE_URL", "postgresql://daudsaid:9848@localhost:5433/fraud_db")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC        = "fraud.transactions"
GROUP_ID     = "fraud-alert-consumer"
FLUSH_EVERY  = 200

ALERTS_DDL = """
CREATE TABLE IF NOT EXISTS fraud_alerts (
    alert_id        SERIAL PRIMARY KEY,
    transaction_id  BIGINT NOT NULL,
    fraud_score     NUMERIC(6,2),
    fraud_flag      SMALLINT,
    is_fraud        SMALLINT,
    transaction_amt NUMERIC(12,2),
    card1           INTEGER,
    p_emaildomain   VARCHAR(100),
    hour_of_day     SMALLINT,
    is_night        SMALLINT,
    is_high_value   SMALLINT,
    alert_reason    TEXT,
    consumed_at     TIMESTAMP DEFAULT NOW()
);
"""


def build_reason(r: dict) -> str:
    reasons = []
    if r.get("is_high_value"):      reasons.append("high_value")
    if r.get("is_night"):           reasons.append("night_txn")
    if r.get("amt_zscore", 0) > 3:  reasons.append("unusual_amount")
    if not r.get("email_match"):    reasons.append("email_mismatch")
    if r.get("v_null_density", 0) > 0.5: reasons.append("thin_identity")
    if r.get("is_high_fraud_product"): reasons.append("high_risk_product")
    return ",".join(reasons) or "score_threshold"


def flush(engine, buffer: list) -> None:
    if not buffer:
        return
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO fraud_alerts
                (transaction_id, fraud_score, fraud_flag, is_fraud,
                 transaction_amt, card1, p_emaildomain,
                 hour_of_day, is_night, is_high_value, alert_reason)
            VALUES
                (:transaction_id, :fraud_score, :fraud_flag, :is_fraud,
                 :transaction_amt, :card1, :p_emaildomain,
                 :hour_of_day, :is_night, :is_high_value, :alert_reason)
        """), buffer)
    log.info("Flushed %d alerts", len(buffer))
    buffer.clear()


def consume() -> None:
    engine = create_engine(DB_URL, pool_pre_ping=True)
    with engine.begin() as conn:
        conn.execute(text(ALERTS_DDL))
    log.info("fraud_alerts table ready")

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    log.info("Listening on topic '%s'", TOPIC)
    buffer = []
    total  = 0

    for msg in consumer:
        r = msg.value
        if not r.get("fraud_flag"):
            continue
        buffer.append({
            "transaction_id": r.get("transaction_id"),
            "fraud_score":    r.get("fraud_score"),
            "fraud_flag":     r.get("fraud_flag"),
            "is_fraud":       r.get("is_fraud"),
            "transaction_amt": r.get("transaction_amt"),
            "card1":          r.get("card1"),
            "p_emaildomain":  r.get("p_emaildomain"),
            "hour_of_day":    r.get("hour_of_day"),
            "is_night":       r.get("is_night"),
            "is_high_value":  r.get("is_high_value"),
            "alert_reason":   build_reason(r),
        })
        total += 1
        if len(buffer) >= FLUSH_EVERY:
            flush(engine, buffer)
            log.info("Total alerts: %d", total)

    flush(engine, buffer)
    log.info("Consumer finished. Total alerts: %d", total)


if __name__ == "__main__":
    consume()
