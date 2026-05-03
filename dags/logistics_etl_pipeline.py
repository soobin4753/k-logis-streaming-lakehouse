from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "logistics",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="logistics_etl_pipeline",
    description="Process raw parquet, build metrics and mart",
    default_args=default_args,
    start_date=datetime(2026, 5, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["logistics", "etl", "spark", "mart"],
) as dag:

    etl_processed = BashOperator(
        task_id="etl_processed_delivery_events",
        bash_command=r"""
docker exec spark-master bash -c '
cd /app/spark

/opt/spark/bin/spark-submit etl_processed_delivery_events.py
'
""",
    )

    build_metrics = BashOperator(
        task_id="build_shipment_metrics",
        bash_command=r"""
docker exec spark-master bash -c '
cd /app/spark

/opt/spark/bin/spark-submit \
--conf spark.jars.ivy=/tmp/.ivy2 \
--packages org.postgresql:postgresql:42.7.3 \
build_shipment_metrics.py
'
""",
    )

    build_mart = BashOperator(
        task_id="build_data_mart",
        bash_command=r"""
docker exec spark-master bash -c '
cd /app/spark

/opt/spark/bin/spark-submit \
--conf spark.jars.ivy=/tmp/.ivy2 \
--packages org.postgresql:postgresql:42.7.3 \
build_data_mart.py
'
""",
    )

    etl_processed >> build_metrics >> build_mart