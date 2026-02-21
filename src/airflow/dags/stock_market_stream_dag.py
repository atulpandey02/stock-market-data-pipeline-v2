"""
stock_market_stream_dag.py
Real-time streaming pipeline: produce → consume → Spark → Snowflake.
Runs every 30 minutes for data collection and micro-batch loading.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="stock_streaming_pipeline",
    default_args=default_args,
    description="Real-time streaming pipeline: Kafka → Spark → Snowflake",
    schedule_interval="*/30 * * * *",
    catchup=False,
    tags=["streaming", "stock-market"],
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")

    cleanup_processes = BashOperator(
        task_id="cleanup_processes",
        bash_command="pkill -f stream_data_producer.py || true && pkill -f realtime_data_consumer.py || true",
    )

    collect_streaming_data = BashOperator(
        task_id="collect_streaming_data",
        bash_command=(
            "python /opt/airflow/dags/scripts/stream_data_producer.py & "
            "python /opt/airflow/dags/scripts/realtime_data_consumer.py"
        ),
    )

    spark_analytics_processing = BashOperator(
        task_id="spark_analytics_processing",
        bash_command=(
            "spark-submit --master spark://spark-master:7077 "
            "/opt/spark-apps/jobs/spark_stream_batch_processor.py"
        ),
    )

    load_to_snowflake = BashOperator(
        task_id="load_to_snowflake",
        bash_command="python /opt/airflow/dags/scripts/load_stream_to_snowflake.py",
    )

    pipeline_summary = BashOperator(
        task_id="pipeline_summary",
        bash_command='echo "Streaming pipeline complete at $(date)"',
    )

    final_cleanup = BashOperator(
        task_id="final_cleanup",
        bash_command="pkill -f stream_data_producer.py || true",
    )

    (
        start
        >> cleanup_processes
        >> collect_streaming_data
        >> spark_analytics_processing
        >> load_to_snowflake
        >> pipeline_summary
        >> final_cleanup
    )
