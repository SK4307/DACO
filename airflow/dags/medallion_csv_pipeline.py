from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_ROOT = os.environ.get("DW_PROJECT_ROOT", "/opt/airflow/dags/repo")
DBT_PROJECT_DIR = f"{PROJECT_ROOT}/dbt"


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="medallion_csv_pipeline",
    description="Load CSV files to SQL Server Bronze, then run dbt Silver and Gold models.",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["sqlserver", "csv", "dbt", "medallion"],
) as dag:
    ingest_csv_to_bronze = BashOperator(
        task_id="ingest_csv_to_bronze",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python scripts/ingest_csv_to_bronze.py --config config/sources.yml"
        ),
    )

    dbt_run = BashOperator(
        task_id="dbt_run_silver_gold",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            "dbt run --profiles-dir . --select silver gold"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            "dbt test --profiles-dir ."
        ),
    )

    ingest_csv_to_bronze >> dbt_run >> dbt_test
