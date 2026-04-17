import os
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
log = logging.getLogger(__name__)

DB_URL     = os.getenv("DATABASE_URL", "postgresql://daudsaid:9848@localhost:5433/fraud_db")
RAW_TABLE  = "raw_transactions"
CHUNK_SIZE = 10_000


def get_engine():
    return create_engine(DB_URL, pool_pre_ping=True)


def create_schema(engine) -> None:
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {RAW_TABLE} (
        transaction_id        BIGINT PRIMARY KEY,
        is_fraud              SMALLINT,
        transaction_dt        BIGINT,
        transaction_amt       NUMERIC(12, 2),
        product_cd            VARCHAR(10),
        card1                 INTEGER,
        card4                 VARCHAR(30),
        card6                 VARCHAR(30),
        addr1                 NUMERIC,
        addr2                 NUMERIC,
        p_emaildomain         VARCHAR(100),
        r_emaildomain         VARCHAR(100),
        device_type           VARCHAR(30),
        device_info           VARCHAR(200),
        transaction_timestamp TIMESTAMP,
        hour_of_day           SMALLINT,
        day_of_week           SMALLINT,
        is_weekend            SMALLINT,
        is_night              SMALLINT,
        amt_zscore            NUMERIC(10, 4),
        amt_log               NUMERIC(10, 4),
        is_round_amount       SMALLINT,
        is_high_value         SMALLINT,
        is_high_fraud_product SMALLINT,
        card_velocity         INTEGER,
        p_email_risky         SMALLINT,
        email_match           SMALLINT,
        has_identity          SMALLINT,
        is_mobile             SMALLINT,
        v_null_density        NUMERIC(5, 3),
        fraud_score           NUMERIC(6, 2),
        fraud_flag            SMALLINT,
        loaded_at             TIMESTAMP DEFAULT NOW()
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))
    log.info("Schema ready: %s", RAW_TABLE)


def load(df: pd.DataFrame, truncate: bool = False) -> None:
    engine = get_engine()
    create_schema(engine)

    keep = [
        "transaction_id", "is_fraud", "transaction_dt", "transaction_amt",
        "product_cd", "card1", "card4", "card6", "addr1", "addr2",
        "p_emaildomain", "r_emaildomain", "device_type", "device_info",
        "transaction_timestamp", "hour_of_day", "day_of_week",
        "is_weekend", "is_night", "amt_zscore", "amt_log",
        "is_round_amount", "is_high_value", "is_high_fraud_product",
        "card_velocity", "p_email_risky", "email_match",
        "has_identity", "is_mobile", "v_null_density",
        "fraud_score", "fraud_flag",
    ]
    df_load = df[[c for c in keep if c in df.columns]]

    if truncate:
        with engine.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE {RAW_TABLE}"))
        log.info("Table truncated")

    total  = len(df_load)
    loaded = 0
    for i in range(0, total, CHUNK_SIZE):
        chunk = df_load.iloc[i: i + CHUNK_SIZE]
        chunk.to_sql(RAW_TABLE, engine, if_exists="append", index=False, method="multi")
        loaded += len(chunk)
        log.info("Loaded %d / %d rows", loaded, total)

    log.info("Load complete: %d rows into %s", total, RAW_TABLE)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    import sys
    sys.path.insert(0, "etl/extract")
    sys.path.insert(0, "etl/transform")
    from extract import extract
    from transform import transform
    df = transform(extract())
    load(df, truncate=True)
