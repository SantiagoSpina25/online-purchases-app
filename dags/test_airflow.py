from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="test_airflow",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    test_script = BashOperator(
        task_id="run_test_script",
        bash_command="python /opt/airflow/scripts/test_script.py",
    )
