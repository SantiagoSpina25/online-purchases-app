from datetime import datetime
import os

from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum


DEFAULT_HISTORICAL_TO_ELASTIC_CMD = """
export PYSPARK_SUBMIT_ARGS="--packages org.elasticsearch:elasticsearch-spark-30_2.12:8.11.1 pyspark-shell" && \
python /opt/airflow/scripts/parquet_historical_to_elastic.py
""".strip()


LOCAL_TZ = pendulum.timezone("Europe/Madrid")


with DAG(
    dag_id="parquet_historical_to_elastic",
    start_date=datetime(2026, 3, 3, tzinfo=LOCAL_TZ),
    schedule="0 23 * * *",
    catchup=False,
    tags=["historical", "elasticsearch"],
) as dag:
    run_parquet_historical_to_elastic = BashOperator(
        task_id="run_parquet_historical_to_elastic",
        bash_command=os.getenv(
            "HISTORICAL_TO_ELASTIC_CMD",
            DEFAULT_HISTORICAL_TO_ELASTIC_CMD,
        ),
    )

    run_parquet_historical_to_elastic
