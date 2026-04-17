import pandas as pd
import numpy as np
import logging

log = logging.getLogger(__name__)

REFERENCE_DT = pd.Timestamp("2017-12-01")
HIGH_FRAUD_PRODUCTS = {"C", "S"}
RISKY_DOMAINS = {"gmail.com", "yahoo.com", "hotmail.com", "outlook.com"}


def transform(df: pd.DataFrame) -> pd.DataFrame:
    log.info("Starting transform on %d rows", len(df))
    df = df.copy()

    # --- Timestamps ---
    df["transaction_timestamp"] = REFERENCE_DT + pd.to_timedelta(df["TransactionDT"], unit="s")
    df["hour_of_day"] = df["transaction_timestamp"].dt.hour
    df["day_of_week"] = df["transaction_timestamp"].dt.dayofweek
    df["is_weekend"]  = df["day_of_week"].isin([5, 6]).astype(int)
    df["is_night"]    = df["hour_of_day"].between(0, 5).astype(int)

    # --- Amount features ---
    df["TransactionAmt"] = df["TransactionAmt"].fillna(0)
    amt_mean = df["TransactionAmt"].mean()
    amt_std  = df["TransactionAmt"].std()
    df["amt_zscore"]      = ((df["TransactionAmt"] - amt_mean) / (amt_std + 1e-9)).round(4)
    df["amt_log"]         = np.log1p(df["TransactionAmt"]).round(4)
    df["is_round_amount"] = (df["TransactionAmt"] % 1 == 0).astype(int)
    df["is_high_value"]   = (df["TransactionAmt"] > 500).astype(int)

    # --- Product risk ---
    df["is_high_fraud_product"] = df["ProductCD"].isin(HIGH_FRAUD_PRODUCTS).astype(int)

    # --- Card velocity ---
    df["card_velocity"] = df.groupby("card1")["TransactionID"].transform("count")

    # --- Email features ---
    df["p_email_risky"] = df["P_emaildomain"].isin(RISKY_DOMAINS).astype(int)
    df["email_match"]   = (df["P_emaildomain"] == df["R_emaildomain"]).astype(int)

    # --- Identity features ---
    df["has_identity"] = df["DeviceType"].notna().astype(int)
    df["is_mobile"]    = (df["DeviceType"] == "mobile").astype(int)

    # --- Null density across V1-V20 ---
    v_cols = [c for c in df.columns if c.startswith("V")]
    df["v_null_density"] = df[v_cols].isnull().mean(axis=1).round(3)

    # --- Rule-based fraud score (0-100) ---
    df["fraud_score"] = (
        df["amt_zscore"].clip(0, 5)       * 8   +
        df["is_night"]                    * 10  +
        df["is_weekend"]                  * 5   +
        df["is_high_value"]               * 15  +
        df["is_high_fraud_product"]       * 20  +
        df["p_email_risky"]               * 5   +
        df["v_null_density"]              * 15  +
        (1 - df["email_match"])           * 10
    ).clip(0, 100).round(2)

    df["fraud_flag"] = (df["fraud_score"] >= 40).astype(int)

    # --- Rename to snake_case ---
    df = df.rename(columns={
        "TransactionID":  "transaction_id",
        "isFraud":        "is_fraud",
        "TransactionDT":  "transaction_dt",
        "TransactionAmt": "transaction_amt",
        "ProductCD":      "product_cd",
        "P_emaildomain":  "p_emaildomain",
        "R_emaildomain":  "r_emaildomain",
        "DeviceType":     "device_type",
        "DeviceInfo":     "device_info",
    })

    log.info("Transform complete.")
    log.info("Fraud flag rate:   %.2f%%", df["fraud_flag"].mean() * 100)
    log.info("Actual fraud rate: %.2f%%", df["is_fraud"].mean() * 100)
    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    import sys
    sys.path.insert(0, "etl/extract")
    from extract import extract
    raw = transform(extract())
    print(raw[["transaction_id", "transaction_amt", "product_cd",
               "fraud_score", "fraud_flag", "is_fraud"]].head(10).to_string())
