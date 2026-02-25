from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys

sys.path.insert(
    0, "/opt/airflow/scripts"
)  # para que Airflow encuentre consumer_batch.py

from consumer_batch import run_consumer_batch

with DAG(
    dag_id="consumer_dag",
    start_date=datetime(2026, 2, 25),
    schedule_interval=None,  # manual trigger
    catchup=False,
) as dag:

    run_batch = PythonOperator(
        task_id="run_consumer_batch", python_callable=run_consumer_batch
    )
