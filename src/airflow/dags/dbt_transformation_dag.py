"""
dbt_transformation_dag.py

Dedicated Airflow DAG for dbt transformations.
Runs after both the batch and stream Snowflake loads complete.

Schedule: Daily at 06:00 UTC (after overnight batch load)
Also triggered externally by stock_market_batch_dag on completion.

Flow:
    dbt_deps → dbt_source_freshness
                    ↓
              dbt_run_staging
                    ↓
          dbt_run_intermediate  (implicit via ephemeral models)
                    ↓
              dbt_run_marts
                    ↓
              dbt_test_all
                    ↓
             dbt_generate_docs
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

# ── Constants ──────────────────────────────────────────────────────────────────
DBT_DIR = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"
DBT_TARGET = "prod"  # change to 'dev' for testing

DBT_CMD_BASE = (
    f"cd {DBT_DIR} && "
    f"dbt {{subcommand}} "
    f"--profiles-dir {DBT_PROFILES_DIR} "
    f"--target {DBT_TARGET}"
)

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dbt_transformation_pipeline",
    default_args=default_args,
    description="dbt transformation layer: staging → marts → tests → docs",
    schedule_interval="0 6 * * *",   # daily at 06:00 UTC
    catchup=False,
    tags=["dbt", "snowflake", "transformation"],
    max_active_runs=1,
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id="start")

    # ── 1. Install/update dbt packages ────────────────────────────────────────
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=DBT_CMD_BASE.format(subcommand="deps"),
    )

    # ── 2. Check source freshness (warns if data is stale) ────────────────────
    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=DBT_CMD_BASE.format(subcommand="source freshness"),
        # Warn only — don't block pipeline if source is slightly stale
        # Remove the next line to make staleness a hard failure
    )

    # ── 3. Run staging models ──────────────────────────────────────────────────
    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=DBT_CMD_BASE.format(
            subcommand="run --select tag:staging"
        ),
    )

    # ── 4. Run mart models (intermediate is ephemeral, runs inline) ────────────
    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=DBT_CMD_BASE.format(
            subcommand="run --select tag:mart"
        ),
    )

    # ── 5. Run all dbt tests ───────────────────────────────────────────────────
    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=DBT_CMD_BASE.format(
            subcommand="test --select tag:staging"
        ),
    )

    dbt_test_marts = BashOperator(
        task_id="dbt_test_marts",
        bash_command=DBT_CMD_BASE.format(
            subcommand="test --select tag:mart"
        ),
    )

    # ── 6. Generate & upload docs ─────────────────────────────────────────────
    dbt_generate_docs = BashOperator(
        task_id="dbt_generate_docs",
        bash_command=DBT_CMD_BASE.format(subcommand="docs generate"),
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ── 7. Completion ─────────────────────────────────────────────────────────
    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ── DAG wiring ────────────────────────────────────────────────────────────
    (
        start
        >> dbt_deps
        >> dbt_source_freshness
        >> dbt_run_staging
        >> dbt_test_staging
        >> dbt_run_marts
        >> dbt_test_marts
        >> dbt_generate_docs
        >> end
    )
