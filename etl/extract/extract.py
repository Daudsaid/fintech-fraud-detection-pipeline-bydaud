import pandas as pd
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

TRANSACTION_PATH = Path("data/raw/train_transaction.csv")
IDENTITY_PATH    = Path("data/raw/train_identity.csv")

TX_COLS = [
    "TransactionID", "isFraud", "TransactionDT", "TransactionAmt",
    "ProductCD", "card1", "card2", "card3", "card4", "card5", "card6",
    "addr1", "addr2", "dist1", "dist2", "P_emaildomain", "R_emaildomain",
    "C1", "C2", "C3", "C4", "C5", "C6",
    "D1", "D2", "D3", "D4", "D5",
    "M1", "M2", "M3", "M4", "M5", "M6",
    "V1", "V2", "V3", "V4", "V5", "V6", "V7", "V8", "V9", "V10",
    "V11", "V12", "V13", "V14", "V15", "V16", "V17", "V18", "V19", "V20",
]

ID_COLS = [
    "TransactionID", "DeviceType", "DeviceInfo",
    "id_01", "id_02", "id_03", "id_04", "id_05",
    "id_06", "id_09", "id_10", "id_11",
    "id_12", "id_13", "id_14", "id_15",
    "id_28", "id_29", "id_31", "id_33",
    "id_35", "id_36", "id_37", "id_38",
]


def extract() -> pd.DataFrame:
    log.info("Loading transactions...")
    tx = pd.read_csv(TRANSACTION_PATH, usecols=lambda c: c in TX_COLS)
    log.info("Transactions: %d rows, %d cols", *tx.shape)

    log.info("Loading identity...")
    id_available = [c for c in ID_COLS if c in pd.read_csv(IDENTITY_PATH, nrows=0).columns]
    identity = pd.read_csv(IDENTITY_PATH, usecols=id_available)
    log.info("Identity: %d rows, %d cols", *identity.shape)

    log.info("Merging on TransactionID...")
    merged = tx.merge(identity, on="TransactionID", how="left")
    log.info("Merged: %d rows, %d cols", *merged.shape)

    return merged


if __name__ == "__main__":
    df = extract()
    print(df.head(3).to_string())
    print("\nFraud rate:", round(df["isFraud"].mean() * 100, 2), "%")
    print("Columns:", list(df.columns))
