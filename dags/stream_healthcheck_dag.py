from datetime import datetime
import os
import subprocess

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Comando que devuelve 0 cuando el stream funciona correctamente.
HEALTHCHECK_CMD = os.getenv("STREAM_HEALTHCHECK_CMD")


def check_stream_process() -> None:
    result = subprocess.run(
        HEALTHCHECK_CMD,
        shell=True,
        text=True,
        capture_output=True,
    )

    if result.returncode != 0:
        output = (result.stdout or result.stderr or "no output").strip()
        raise AirflowException(
            f"Stream healthcheck failed with command: {HEALTHCHECK_CMD}. Output: {output}"
        )


with DAG(
    dag_id="stream_healthcheck",
    start_date=datetime(2026, 3, 1),
    schedule="*/5 * * * *",
    catchup=False,
    tags=["streaming", "monitoring"],
) as dag:
    healthcheck_stream = PythonOperator(
        task_id="healthcheck_stream",
        python_callable=check_stream_process,
    )

    healthcheck_stream
