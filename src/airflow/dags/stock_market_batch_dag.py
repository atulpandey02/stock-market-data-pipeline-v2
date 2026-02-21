"""
stock_market_batch_dag.py
Daily batch pipeline: ingest → process → load to Snowflake → dbt transforms.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="stock_market_batch_pipeline",
    default_args=default_args,
    description="End-to-end batch pipeline: Kafka → Spark → Snowflake → dbt",
    schedule_interval="0 2 * * *",
    catchup=False,
    tags=["batch", "stock-market"],
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")

    fetch_historical_data = BashOperator(
        task_id="fetch_historical_data",
        bash_command="python /opt/airflow/dags/scripts/batch_data_producer.py",
    )

    consume_historical_data = BashOperator(
        task_id="consume_historical_data",
        bash_command="python /opt/airflow/dags/scripts/batch_data_consumer.py",
    )

    process_data = BashOperator(
        task_id="spark_process_batch",
        bash_command=(
            "spark-submit --master spark://spark-master:7077 "
            "--jars /opt/spark-jars/snowflake-jdbc.jar "
            "/opt/spark-apps/jobs/spark_batch_processor.py"
        ),
    )

    load_historical_to_snowflake = BashOperator(
        task_id="load_historical_to_snowflake",
        bash_command="python /opt/airflow/dags/scripts/load_to_snowflake.py",
    )

    # ── Trigger dbt transformation DAG after Snowflake load ──────────────────
    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt_transformations",
        trigger_dag_id="dbt_transformation_pipeline",
        wait_for_completion=True,
        poke_interval=30,
    )

    process_complete = EmptyOperator(task_id="process_complete")

    (
        start
        >> fetch_historical_data
        >> consume_historical_data
        >> process_data
        >> load_historical_to_snowflake
        >> trigger_dbt
        >> process_complete
    )
