from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

with DAG(
    dag_id="kafka_to_elastic_dag",
    start_date=datetime(2026, 2, 24),
    schedule_interval="*/5 * * * *",  # cada 5 minutos
    catchup=False,
    retries=3,
    retry_delay=timedelta(minutes=1),
    tags=["kafka", "elastic"],
) as dag:

    run_consumer = BashOperator(
        task_id="consume_online_purchases",
        bash_command="python /opt/airflow/scripts/consumer.py",
    )
