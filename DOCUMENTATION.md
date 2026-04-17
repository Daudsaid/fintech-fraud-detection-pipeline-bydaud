# Fintech Fraud Detection Pipeline — Full Documentation

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Dataset](#2-dataset)
3. [Architecture](#3-architecture)
4. [Infrastructure](#4-infrastructure)
5. [ETL Layer](#5-etl-layer)
6. [Kafka Streaming Layer](#6-kafka-streaming-layer)
7. [dbt Modelling Layer](#7-dbt-modelling-layer)
8. [Airflow Orchestration Layer](#8-airflow-orchestration-layer)
9. [Data Model](#9-data-model)
10. [Feature Engineering](#10-feature-engineering)
11. [Fraud Scoring](#11-fraud-scoring)
12. [Key Findings](#12-key-findings)
13. [Problems Solved](#13-problems-solved)
14. [Setup Guide](#14-setup-guide)

---

## 1. Project Overview

This pipeline ingests raw financial transaction data, engineers fraud signals, loads it into PostgreSQL, streams flagged transactions through Apache Kafka, builds analytics models with dbt, and orchestrates the entire workflow with Apache Airflow.

**Goal:** Detect fraudulent transactions in a large financial dataset using a rule-based scoring system, stream alerts in real time, and serve clean aggregated models for analytics.

**Dataset:** IEEE-CIS Fraud Detection (Kaggle) — 590,540 transactions, 3.5% fraud rate.

**Stack:** Python, PostgreSQL, Apache Kafka, dbt, Apache Airflow, Docker.

---

## 2. Dataset

### Source
IEEE-CIS Fraud Detection — Kaggle Competition
https://www.kaggle.com/c/ieee-fraud-detection

### Files
| File | Rows | Columns | Description |
|---|---|---|---|
| train_transaction.csv | 590,540 | 394 | Transaction records with fraud labels |
| train_identity.csv | 144,233 | 41 | Device and identity data |

### Key Fields

**Transaction data:**
- TransactionID — unique identifier
- isFraud — ground truth label (0 or 1)
- TransactionDT — seconds offset from reference date
- TransactionAmt — transaction amount in USD
- ProductCD — product category (W, C, R, H, S)
- card1–card6 — card metadata (type, network, issuer)
- addr1, addr2 — billing address regions
- P_emaildomain, R_emaildomain — purchaser and recipient email domains
- C1–C14 — counting features (anonymised)
- D1–D15 — timedelta features (anonymised)
- M1–M9 — match features (T/F flags)
- V1–V339 — Vesta engineered features (anonymised)

**Identity data:**
- TransactionID — join key
- DeviceType — mobile or desktop
- DeviceInfo — device string
- id_01–id_38 — identity features (anonymised)

### Join Strategy
Left join on TransactionID. Only 144,233 of 590,540 transactions have identity records. A left join preserves all transactions — an inner join would silently drop 75% of data.

### Class Imbalance
3.5% fraud rate (20,663 fraudulent out of 590,540). Heavily imbalanced — handled by engineering meaningful features rather than resampling.

---

## 3. Architecture

```
┌─────────────────────────────────────────────────────┐
│                   Raw Data                          │
│   train_transaction.csv + train_identity.csv        │
│   data/raw/                                         │
└────────────────────┬────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────┐
│                  ETL Layer                          │
│                                                     │
│  etl/extract/extract.py                             │
│  • Read CSVs with usecols (77 of 434 columns)       │
│  • Left join transaction + identity                 │
│  • Return merged DataFrame                          │
│                                                     │
│  etl/transform/transform.py                         │
│  • Engineer 12 fraud signal features                │
│  • Apply rule-based fraud score (0–100)             │
│  • Flag transactions with score >= 40               │
│  • Rename columns to snake_case                     │
│                                                     │
│  etl/load/load_to_postgres.py                       │
│  • Create raw_transactions table if not exists      │
│  • Truncate and reload in 10k chunks                │
│  • Log progress every batch                         │
└────────────────────┬────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────┐
│                  PostgreSQL                         │
│                                                     │
│  Database: fraud_db                                 │
│  Port: 5433                                         │
│                                                     │
│  Tables:                                            │
│  • raw_transactions (590,540 rows)                  │
│  • fraud_alerts (Kafka consumer output)             │
│  • stg_transactions (dbt view)                      │
│  • int_flagged_transactions (dbt view)              │
│  • mart_fraud_summary (dbt table, 118 rows)         │
└──────────┬──────────────────────────┬───────────────┘
           │                          │
           ▼                          ▼
┌──────────────────────┐   ┌──────────────────────────┐
│    Kafka Layer       │   │       dbt Layer           │
│                      │   │                           │
│  producer.py         │   │  stg_transactions         │
│  Reads raw_trans     │   │  int_flagged_transactions │
│  → fraud.transactions│   │  mart_fraud_summary       │
│  topic               │   │  6 schema tests           │
│                      │   │                           │
│  consumer.py         │   └──────────────────────────┘
│  Reads topic         │
│  Filters fraud_flag=1│
│  → fraud_alerts      │
└──────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────┐
│                Apache Airflow                       │
│                                                     │
│  DAG: fraud_pipeline (@daily)                       │
│                                                     │
│  extract → transform_and_load → dbt_run → dbt_test  │
│                                                     │
│  UI: http://localhost:8082                          │
└─────────────────────────────────────────────────────┘
```

---

## 4. Infrastructure

### Docker Containers

| Container | Image | Port | Purpose |
|---|---|---|---|
| fraud-postgres | postgres:15 | 5433 | Primary data store |
| fraud-kafka | confluentinc/cp-kafka:7.6.0 | 9092 | Message broker |
| fraud-zookeeper | confluentinc/cp-zookeeper:7.6.0 | 2181 | Kafka coordination |
| fraud-airflow-postgres | postgres:15 | 5434 | Airflow metadata store |
| fraud-airflow-webserver | apache/airflow:2.9.1 | 8082 | Airflow UI |
| fraud-airflow-scheduler | apache/airflow:2.9.1 | — | DAG scheduling |

### Docker Compose Files

**docker-compose.yml** — Kafka + Zookeeper + fraud Postgres
**airflow/docker-compose.yml** — Airflow webserver, scheduler, and its own Postgres

### Environment Variables (.env)
```
DATABASE_URL=postgresql://daudsaid:9848@localhost:5433/fraud_db
KAFKA_BROKER=localhost:9092
```

### Memory Requirements
Docker Desktop must have at least 6GB allocated. The transform step loads 590k rows into pandas — this requires ~500MB inside the container. Without sufficient memory, the OS kills the process (OOM kill).

---

## 5. ETL Layer

### extract.py

**Purpose:** Load and merge the two raw CSVs into a single DataFrame.

**Key design decisions:**

1. **usecols** — Only 77 of 434 columns are loaded. The V columns (V1–V339) are mostly anonymised Vesta features useful for ML but not for our rule-based pipeline. Loading all 434 columns into pandas inside Docker caused OOM kills. usecols drops them at read time before they ever enter memory.

2. **Left join** — identity data only exists for 24% of transactions. Left join preserves all 590k transactions. Missing identity columns become NaN, which is handled downstream.

3. **Logging** — Every stage logs row and column counts so failures are easy to diagnose.

```python
TX_COLS = [
    "TransactionID", "isFraud", "TransactionDT", "TransactionAmt",
    "ProductCD", "card1", "card4", "card6", ...
]

tx = pd.read_csv(TRANSACTION_PATH, usecols=lambda c: c in TX_COLS)
identity = pd.read_csv(IDENTITY_PATH, usecols=id_available)
merged = tx.merge(identity, on="TransactionID", how="left")
```

**Output:** DataFrame with 590,540 rows and 77 columns.

---

### transform.py

**Purpose:** Engineer fraud signals and apply rule-based scoring.

**Timestamp features:**
TransactionDT is a seconds offset from a reference date (2017-12-01), not a real Unix timestamp. We convert it to derive hour_of_day, day_of_week, is_weekend, and is_night.

**Amount features:**
TransactionAmt is right-skewed (mean $135, max $31,937). We apply:
- Z-score normalisation to detect outliers
- Log transform to normalise the distribution
- Binary flags for round amounts and high-value transactions (>$500)

**Card velocity:**
Groups by card1 (card identifier) and counts transactions. A card with very high velocity in the dataset is suspicious.

**Email features:**
Checks if the purchaser email is on a common freemail domain (gmail, yahoo, hotmail, outlook). Also checks if purchaser and recipient email domains match — a mismatch is a signal of account takeover.

**Null density:**
Counts what fraction of V1–V20 Vesta features are null for each row. Rows with high null density are thin records, often associated with synthetic or fraudulent transactions.

**Output:** DataFrame with all original columns plus 12 engineered features, fraud_score, and fraud_flag.

---

### load_to_postgres.py

**Purpose:** Create the raw_transactions table and load the transformed DataFrame.

**Key design decisions:**

1. **Schema-first** — Table is created with explicit column types before loading. This avoids pandas inferring wrong types (e.g. SMALLINT vs BIGINT).

2. **Chunked loading** — 10,000 rows per batch. This keeps memory stable and provides progress logging. Loading all 590k rows in one call would either timeout or fail silently.

3. **Truncate + reload** — On each run, the table is truncated before loading. This makes the pipeline idempotent — safe to run multiple times.

4. **pool_pre_ping=True** — SQLAlchemy checks connection health before each query. Important when the container has been idle.

---

## 6. Kafka Streaming Layer

### Why Kafka?

The ETL layer handles batch loading. Kafka adds a streaming layer that simulates real-time transaction monitoring — the kind of system a real fintech company would use to flag suspicious transactions as they arrive, not hours later in a batch job.

### producer.py

**Purpose:** Read transactions from PostgreSQL and publish them to the fraud.transactions Kafka topic.

**Flow:**
1. Connect to PostgreSQL
2. Stream rows from raw_transactions using server-side cursors (stream_results=True) — avoids loading all 590k rows into memory at once
3. Serialise each row as JSON
4. Publish to fraud.transactions topic
5. Flush and close

**Key detail — serialise function:**
PostgreSQL returns Decimal and datetime objects which are not JSON serialisable by default. The serialise function handles these types explicitly.

```python
def serialise(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
```

---

### consumer.py

**Purpose:** Consume messages from fraud.transactions, filter for fraud_flag=1, and write alerts to fraud_alerts table.

**Flow:**
1. Create fraud_alerts table if not exists
2. Connect to Kafka topic with auto_offset_reset="earliest" — reads from the beginning every time
3. For each message, skip if fraud_flag != 1
4. Build an alert record with alert_reason
5. Buffer alerts and flush to Postgres every 200 records

**alert_reason logic:**
Each alert is tagged with the reason(s) it was flagged:
- high_value — transaction > $500
- night_txn — between 00:00 and 05:59
- unusual_amount — amt_zscore > 3
- email_mismatch — purchaser and recipient email domains differ
- thin_identity — v_null_density > 0.5
- high_risk_product — ProductCD is C or S

**Batching:**
Alerts are buffered and written in batches of 200 using SQLAlchemy's executemany. This is significantly faster than one INSERT per message.

---

## 7. dbt Modelling Layer

### Project Structure

```
dbt/fraud_dbt/
├── dbt_project.yml          # Project config, materialisation settings
├── profiles.yml             # Connection config
└── models/
    ├── staging/
    │   ├── sources.yml      # Declares raw_transactions and fraud_alerts as sources
    │   ├── schema.yml       # Data tests
    │   └── stg_transactions.sql
    ├── intermediate/
    │   └── int_flagged_transactions.sql
    └── marts/
        └── mart_fraud_summary.sql
```

### Materialisation Strategy

| Layer | Materialisation | Reason |
|---|---|---|
| staging | view | No storage cost, always fresh |
| intermediate | view | Filtered subset, no storage needed |
| marts | table | Aggregated, queried frequently |

### stg_transactions

A view that selects and aliases all columns from raw_transactions. Purpose is to create a clean, documented interface between the raw table and all downstream models. If the raw table schema changes, only this model needs updating.

### int_flagged_transactions

Filters stg_transactions to only rows where fraud_flag = 1. Used as the input for any model that only needs to work with suspicious transactions. Avoids repeating the filter logic across multiple models.

### mart_fraud_summary

Aggregates fraud statistics by segment. Groups by product_cd, card_network, is_night, is_weekend, and is_high_value. Computes:
- total_transactions
- actual_fraud (sum of is_fraud)
- flagged (sum of fraud_flag)
- avg_fraud_score
- avg_amount
- fraud_rate (actual_fraud / total_transactions)

Produces 118 rows covering all combinations of the grouping dimensions.

### dbt Tests

Defined in schema.yml. All 6 pass:

| Test | Column | What it checks |
|---|---|---|
| unique | transaction_id | No duplicate transactions |
| not_null | transaction_id | Every row has an ID |
| not_null | is_fraud | Ground truth label always present |
| accepted_values | is_fraud | Only 0 or 1 |
| not_null | fraud_score | Score always computed |
| accepted_values | product_cd | Only W, C, R, H, S |

---

## 8. Airflow Orchestration Layer

### DAG: fraud_pipeline

**Schedule:** @daily
**Start date:** 2024-01-01
**Catchup:** False — does not backfill historical runs

### Tasks

```
extract → transform_and_load → dbt_run → dbt_test
```

All tasks use BashOperator. This was a deliberate choice over PythonOperator — BashOperator spawns a subprocess, which means each task gets its own memory space. PythonOperator runs in the same process as the scheduler, which caused memory pressure and zombie task failures during development.

| Task | Command | Timeout | Retries |
|---|---|---|---|
| extract | python3 etl/extract/extract.py | 15 min | 2 |
| transform_and_load | python3 etl/load/load_to_postgres.py | 20 min | 2 |
| dbt_run | dbt run --profiles-dir . | 10 min | 0 |
| dbt_test | dbt test --profiles-dir . | 5 min | 0 |

### Configuration

Key Airflow settings in docker-compose.yml:
- AIRFLOW__SCHEDULER__ZOMBIE_TASK_THRESHOLD: 600 — gives tasks 10 minutes before being declared zombie (default is 5 minutes, too short for the load task)
- AIRFLOW__CORE__LOAD_EXAMPLES: false — keeps the UI clean
- DATABASE_URL passed as environment variable so the scripts connect to the correct Postgres instance

### Volume Mounts

The Airflow containers mount:
- ../airflow/dags → /opt/airflow/dags
- ../etl → /opt/airflow/etl
- ../dbt → /opt/airflow/dbt
- ../data → /opt/airflow/data

This means the DAG runs the same Python scripts that run locally — no duplication.

### dbt Inside Airflow

dbt is not installed in the base Airflow image. It must be pip installed into the container after startup. The profiles.yml inside the container must point to host.docker.internal:5433, not localhost (which inside the container refers to the container itself, not the host Mac).

---

## 9. Data Model

### raw_transactions

| Column | Type | Description |
|---|---|---|
| transaction_id | BIGINT PK | Unique transaction identifier |
| is_fraud | SMALLINT | Ground truth (0 or 1) |
| transaction_dt | BIGINT | Seconds offset from reference date |
| transaction_amt | NUMERIC(12,2) | Transaction amount USD |
| product_cd | VARCHAR(10) | Product category |
| card1 | INTEGER | Card identifier |
| card4 | VARCHAR(30) | Card network (visa, mastercard, etc) |
| card6 | VARCHAR(30) | Card type (credit, debit) |
| p_emaildomain | VARCHAR(100) | Purchaser email domain |
| r_emaildomain | VARCHAR(100) | Recipient email domain |
| device_type | VARCHAR(30) | mobile or desktop |
| transaction_timestamp | TIMESTAMP | Derived real timestamp |
| hour_of_day | SMALLINT | 0–23 |
| is_night | SMALLINT | 1 if 00:00–05:59 |
| is_weekend | SMALLINT | 1 if Saturday or Sunday |
| amt_zscore | NUMERIC(10,4) | Amount standard score |
| amt_log | NUMERIC(10,4) | Log-transformed amount |
| is_high_value | SMALLINT | 1 if amount > $500 |
| is_high_fraud_product | SMALLINT | 1 if ProductCD is C or S |
| card_velocity | INTEGER | Transaction count for this card |
| p_email_risky | SMALLINT | 1 if freemail domain |
| email_match | SMALLINT | 1 if purchaser = recipient domain |
| has_identity | SMALLINT | 1 if identity record exists |
| is_mobile | SMALLINT | 1 if mobile device |
| v_null_density | NUMERIC(5,3) | Null fraction across V1–V20 |
| fraud_score | NUMERIC(6,2) | Rule-based score 0–100 |
| fraud_flag | SMALLINT | 1 if fraud_score >= 40 |
| loaded_at | TIMESTAMP | ETL load timestamp |

### fraud_alerts

| Column | Type | Description |
|---|---|---|
| alert_id | SERIAL PK | Auto-incrementing alert ID |
| transaction_id | BIGINT | Reference to raw_transactions |
| fraud_score | NUMERIC(6,2) | Score at time of alert |
| fraud_flag | SMALLINT | Always 1 |
| is_fraud | SMALLINT | Ground truth label |
| transaction_amt | NUMERIC(12,2) | Transaction amount |
| card1 | INTEGER | Card identifier |
| p_emaildomain | VARCHAR(100) | Purchaser email domain |
| hour_of_day | SMALLINT | Hour of transaction |
| is_night | SMALLINT | Night transaction flag |
| is_high_value | SMALLINT | High value flag |
| alert_reason | TEXT | Comma-separated reason codes |
| consumed_at | TIMESTAMP | When consumer processed this |

---

## 10. Feature Engineering

### Why these features?

Each feature was chosen based on domain knowledge of fraud patterns in payments:

**Amount signals:**
Fraudsters often test cards with unusual amounts — either very small (card testing) or very large (before the card is blocked). Z-score catches outliers. Round amounts flag synthetic transactions.

**Time signals:**
Fraud rates are higher at night and on weekends because monitoring teams are smaller and cardholders are less likely to notice immediately.

**Card velocity:**
A card used hundreds of times in a dataset spanning months is suspicious. Legitimate cards have low velocity.

**Email signals:**
Freemail domains (gmail, yahoo) are easier to create fraudulently than corporate domains. A mismatch between purchaser and recipient email suggests the transaction is not between the same person.

**Product risk:**
Product C (digital goods) has 11.7% fraud rate vs 2% for Product W. Digital goods are preferred by fraudsters because they are instantly deliverable and hard to reverse.

**Null density:**
The V1–V320 Vesta features are engineered from real transaction behaviour. Rows with high null density across these features are thin records — often synthetic or from new/fraudulent accounts.

---

## 11. Fraud Scoring

### Formula

```
fraud_score = (
    amt_zscore.clip(0, 5)     * 8   +   max 40 points
    is_night                  * 10  +   10 points
    is_weekend                * 5   +   5 points
    is_high_value             * 15  +   15 points
    is_high_fraud_product     * 20  +   20 points
    p_email_risky             * 5   +   5 points
    v_null_density            * 15  +   15 points
    (1 - email_match)         * 10      10 points
).clip(0, 100)
```

Maximum possible score: 120 points before clipping to 100.

### Threshold

fraud_flag = 1 if fraud_score >= 40

This threshold was chosen to balance:
- Sensitivity (catching real fraud)
- Specificity (not flooding the alerts table)

At threshold 40, 10.6% of transactions are flagged. Actual fraud rate is 3.5%.

### Limitations

This is a rule-based system, not a machine learning model. It has no memory, does not learn from errors, and cannot detect novel fraud patterns. In production, this would be combined with a gradient boosting model (XGBoost, LightGBM) trained on the labelled data. The rule-based score serves as an interpretable baseline and a fast first-pass filter.

---

## 12. Key Findings

### Fraud by Product

| Product | Transactions | Actual Fraud | Fraud Rate |
|---|---|---|---|
| C (digital goods) | 68,519 | 8,008 | 11.7% |
| S (services) | 11,628 | 686 | 5.9% |
| H | 33,024 | 1,574 | 4.8% |
| R | 37,699 | 1,426 | 3.8% |
| W (standard) | 439,670 | 8,969 | 2.0% |

Product C has nearly 6x the fraud rate of Product W. Digital goods are the primary fraud vector.

### Fraud by Card Network (mart_fraud_summary top segments)

- C + American Express + night = 100% fraud rate (small sample)
- S + Discover = 17.8% fraud rate
- C + Visa + high value = 100% fraud rate (small sample)
- C + Visa (all) = 13.2% fraud rate — largest high-fraud segment by volume

### Time Patterns

Night transactions (00:00–05:59) consistently show higher fraud rates across all product categories. Weekend transactions show a smaller but consistent uplift.

### Identity Coverage

Only 24% of transactions have identity records. This makes identity-based features (device type, id columns) less reliable as standalone signals but useful in combination with other features.

---

## 13. Problems Solved

### Problem 1 — OOM Kill in Docker

**Symptom:** Python process killed silently inside Airflow container. No error message — just `Killed` in the bash output.

**Root cause:** pandas loading 590,540 rows × 434 columns = ~2GB RAM. Docker container had insufficient memory.

**Fix:** Used usecols in pd.read_csv() to load only 77 relevant columns. Memory dropped from ~2GB to ~200MB. Problem eliminated without changing Docker settings.

**Lesson:** Always select only the columns you need. This is standard practice in production pipelines — it is why columnar storage formats like Parquet exist.

---

### Problem 2 — Airflow Zombie Tasks

**Symptom:** Tasks showed state=failed in Airflow but executor_state=success. Tasks were completing but Airflow was marking them failed.

**Root cause:** Airflow uses a heartbeat system. Every few seconds, the worker sends a signal saying it is alive. When the OOM killer killed the Python process, the heartbeat stopped. Airflow waited 5 minutes (the default zombie threshold) then declared the task a zombie and marked it failed.

**Fix:** Increased AIRFLOW__SCHEDULER__ZOMBIE_TASK_THRESHOLD to 600 seconds. Also fixed the underlying OOM issue which eliminated the zombie problem entirely.

---

### Problem 3 — dbt Cannot Connect Inside Container

**Symptom:** dbt run inside the Airflow container failed with connection refused to localhost:5433.

**Root cause:** Inside a Docker container, localhost refers to the container itself, not the host machine. The fraud-postgres container is running on the host, not inside the Airflow container.

**Fix:** Changed the profiles.yml inside the container to use host.docker.internal:5433, which is Docker's special DNS name that resolves to the host machine from inside a container.

---

### Problem 4 — PythonOperator vs BashOperator Memory

**Symptom:** Even after fixing the OOM issue, tasks were occasionally failing under memory pressure when multiple tasks ran simultaneously.

**Root cause:** PythonOperator runs the callable in the same process as the Airflow scheduler. When the ETL task loaded 590k rows, it competed with the scheduler for memory.

**Fix:** Switched all tasks to BashOperator. This spawns a separate subprocess for each task, giving it its own memory space isolated from the scheduler. Memory is released when the subprocess exits.

---

### Problem 5 — Kafka Topic Auto-Create Warning

**Symptom:** Producer logged WARNING: Topic fraud.transactions is not available during auto-create initialization on first run.

**Root cause:** The producer tries to connect before Kafka has finished creating the topic. This is normal on first run.

**Fix:** Not a real error — Kafka retries and creates the topic automatically. Added KAFKA_AUTO_CREATE_TOPICS_ENABLE: true to the Kafka container config.

---

## 14. Setup Guide

### Prerequisites

- Docker Desktop with 6GB memory allocated
- Python 3.13
- pip packages: pandas, sqlalchemy, psycopg2-binary, kafka-python, python-dotenv, dbt-postgres

### Step 1 — Clone and install

```bash
git clone https://github.com/Daudsaid/fintech-fraud-detection-pipeline-bydaud.git
cd fintech-fraud-detection-pipeline-bydaud
pip3 install pandas sqlalchemy psycopg2-binary kafka-python python-dotenv dbt-postgres
```

### Step 2 — Download dataset

Download from https://www.kaggle.com/c/ieee-fraud-detection and place:
- train_transaction.csv → data/raw/
- train_identity.csv → data/raw/

### Step 3 — Start infrastructure

```bash
# Kafka + Zookeeper + fraud Postgres
docker compose up -d

# Airflow
docker compose -f airflow/docker-compose.yml up airflow-init
docker compose -f airflow/docker-compose.yml up -d airflow-webserver airflow-scheduler
```

### Step 4 — Configure environment

```bash
cat > .env << 'EOF'
DATABASE_URL=postgresql://daudsaid:9848@localhost:5433/fraud_db
KAFKA_BROKER=localhost:9092
EOF
```

### Step 5 — Run ETL

```bash
python3 etl/load/load_to_postgres.py
```

This runs extract → transform → load. Takes about 2.5 minutes for 590k rows.

### Step 6 — Run dbt

```bash
cd dbt/fraud_dbt
dbt run
dbt test
```

### Step 7 — Stream via Kafka

Open two terminals:

```bash
# Terminal 1
python3 kafka/producer/producer.py --limit 50000

# Terminal 2
python3 kafka/consumer/consumer.py
```

### Step 8 — Airflow

Open http://localhost:8082
Login: admin / admin
Trigger the fraud_pipeline DAG manually or wait for the daily schedule.

### Step 9 — Query results

Connect DBeaver or psql to localhost:5433, database fraud_db:

```sql
-- Check all tables
SELECT 'raw_transactions' as table_name, COUNT(*) as rows FROM raw_transactions
UNION ALL
SELECT 'fraud_alerts', COUNT(*) FROM fraud_alerts
UNION ALL
SELECT 'mart_fraud_summary', COUNT(*) FROM mart_fraud_summary;

-- Fraud by product
SELECT product_cd,
       COUNT(*) as total,
       SUM(is_fraud) as actual_fraud,
       ROUND(SUM(is_fraud)::numeric / COUNT(*) * 100, 2) as fraud_rate_pct
FROM raw_transactions
GROUP BY product_cd
ORDER BY fraud_rate_pct DESC;
```

---

*Built by Daud Abdi — github.com/Daudsaid — daudabdi.com*