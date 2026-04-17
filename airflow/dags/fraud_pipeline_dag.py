from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "daud",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "email_on_failure": False,
}

with DAG(
    dag_id="fraud_pipeline",
    default_args=default_args,
    description="IEEE-CIS fraud detection: extract → transform → load → dbt",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["fraud", "etl", "dbt"],
) as dag:

    extract = BashOperator(
        task_id="extract",
        bash_command="cd /opt/airflow && python3 etl/extract/extract.py 2>&1",
        execution_timeout=timedelta(minutes=15),
        retries=2,
    )

    transform_and_load = BashOperator(
        task_id="transform_and_load",
        bash_command="cd /opt/airflow && python3 etl/load/load_to_postgres.py 2>&1",
        execution_timeout=timedelta(minutes=20),
        retries=2,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt/fraud_dbt && dbt run --profiles-dir /opt/airflow/dbt/fraud_dbt 2>&1",
        execution_timeout=timedelta(minutes=10),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt/fraud_dbt && dbt test --profiles-dir /opt/airflow/dbt/fraud_dbt 2>&1",
        execution_timeout=timedelta(minutes=5),
    )

    extract >> transform_and_load >> dbt_run >> dbt_test
