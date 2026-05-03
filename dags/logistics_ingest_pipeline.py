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
    dag_id="logistics_ingest_pipeline",
    description="Reset DB/storage, run Spark streaming, produce 1000 shipments, wait flush, then stop streaming",
    default_args=default_args,
    start_date=datetime(2026, 5, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["logistics", "ingest", "streaming", "kafka"],
) as dag:

    reset_postgres = BashOperator(
        task_id="reset_postgres_tables",
        bash_command=r"""
docker exec logistics-postgres psql -U postgres -d logistics -v ON_ERROR_STOP=1 -c "
TRUNCATE TABLE
    mart_risk_summary,
    mart_hub_performance,
    mart_region_delay,
    shipment_metrics,
    dispatch,
    shipment
RESTART IDENTITY CASCADE;
"
""",
    )

    reset_storage = BashOperator(
        task_id="reset_storage_files",
        bash_command=r"""
docker exec -u root spark-master bash -c '
echo "===== reset parquet/checkpoint/log files ====="

mkdir -p /app/storage/raw
mkdir -p /app/storage/processed
mkdir -p /app/storage/checkpoint

rm -rf /app/storage/raw/* || true
rm -rf /app/storage/processed/* || true
rm -rf /app/storage/checkpoint/* || true
rm -rf /app/storage/metrics || true
rm -rf /app/storage/mart || true
rm -f /app/storage/streaming_raw.log || true

mkdir -p /app/storage/raw
mkdir -p /app/storage/processed
mkdir -p /app/storage/checkpoint

chmod -R 777 /app/storage || true

echo "===== storage reset completed ====="
'
""",
    )

    start_streaming = BashOperator(
        task_id="start_spark_streaming",
        bash_command=r"""
docker exec spark-master bash -c '
pkill -f streaming_raw_delivery_events.py || true
'

docker exec -d spark-master bash -c '
cd /app/spark

nohup /opt/spark/bin/spark-submit \
--conf spark.jars.ivy=/tmp/.ivy2 \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
streaming_raw_delivery_events.py \
> /app/storage/streaming_raw.log 2>&1 &
'

echo "Spark streaming started"
sleep 45
""",
    )

    run_producer = BashOperator(
        task_id="run_producer_1000_shipments",
        bash_command=r"""
cd /opt/airflow/project
python -m producer.producer
""",
    )

    wait_streaming_flush = BashOperator(
        task_id="wait_streaming_flush",
        bash_command="""
echo "Waiting for Spark streaming to consume remaining Kafka events..."
sleep 90
""",
    )

    stop_streaming = BashOperator(
        task_id="stop_spark_streaming",
        trigger_rule="all_done",
        bash_command=r"""
docker exec spark-master bash -c '
pkill -f streaming_raw_delivery_events.py || true
'

echo "Spark streaming stopped"
sleep 5
""",
    )

    reset_postgres >> reset_storage >> start_streaming >> run_producer >> wait_streaming_flush >> stop_streaming