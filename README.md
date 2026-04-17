---

## Key Findings

| Segment | Fraud Rate | Notes |
|---|---|---|
| Product C — Visa | 13.2% | Highest volume fraud segment |
| Product C — Mastercard | 13.0% | Consistent with Visa |
| Product S — Discover | 17.8% | Small volume, high rate |
| Product W — all networks | 2.0% | Baseline — bulk of transactions |
| Night + High Value | >13% | Cross-segment elevated risk |
| Discover card overall | Elevated | Appears in 4 of top 10 fraud segments |

Overall fraud rate: **3.5%** (20,663 / 590,540 transactions)

---

## Proof

### Airflow — All 4 Tasks Green
![Airflow DAG](assets/airflow_dag_success.png)

### PostgreSQL — Table Row Counts
![DBeaver Table Counts](assets/dbeaver_table_counts.png)

### Fraud Analysis by Product
![Fraud Analysis](assets/dbeaver_fraud_analysis.png)

### dbt Run and Test — PASS=6 WARN=0 ERROR=0
![dbt Tests](assets/dbt_run_and_test.png)

### Kafka — Producer and Consumer Live
![Kafka](assets/kafka_consumer.png)

---

## Setup

### Prerequisites
- Docker Desktop (6GB memory allocated)
- Python 3.13
- dbt-postgres

### Start Infrastructure

docker compose up -d
docker compose -f airflow/docker-compose.yml up -d

### Run ETL

python3 etl/load/load_to_postgres.py

### Stream via Kafka

Terminal 1: python3 kafka/producer/producer.py --limit 50000
Terminal 2: python3 kafka/consumer/consumer.py

### Run dbt

cd dbt/fraud_dbt
dbt run
dbt test

### Airflow

http://localhost:8082
Username: admin
Password: admin

---

## Dataset

IEEE-CIS Fraud Detection — Kaggle Competition
https://www.kaggle.com/c/ieee-fraud-detection

590,540 transactions | 144,233 identity records | 3.5% fraud rate | 434 raw features