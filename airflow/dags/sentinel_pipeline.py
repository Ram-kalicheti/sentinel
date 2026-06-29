"""sentinel batch orchestration over the streaming bronze trigger, silver, gold, then dbt"""
import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.databricks.operators.databricks import (
    DatabricksSubmitRunOperator,
)

CLUSTER_ID = "0619-073738-jadx852a"
USER_PREFIX = "/Users/sitharamkalicheti@zohomail.com"

NOTEBOOKS = {
    "streaming_trigger": f"{USER_PREFIX}/sentinel_bronze_ingest",
    "silver": f"{USER_PREFIX}/sentinel/sentinel_silver",
    "gold": f"{USER_PREFIX}/sentinel/sentinel_gold",
}


def alert_on_failure(context):
    """so a failed task is observable rather than silent"""
    ti = context["task_instance"]
    print(json.dumps({
        "event": "task_failure",
        "dag_id": ti.dag_id,
        "task_id": ti.task_id,
        "run_id": context["run_id"],
        "try_number": ti.try_number,
        "exception": str(context.get("exception")),
    }))


default_args = {
    "owner": "sentinel",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "on_failure_callback": alert_on_failure,
}


def databricks_task(task_id):
    """existing-cluster submit keeps compute on databricks and airflow purely orchestrating"""
    return DatabricksSubmitRunOperator(
        task_id=task_id,
        databricks_conn_id="databricks_default",
        existing_cluster_id=CLUSTER_ID,
        notebook_task={"notebook_path": NOTEBOOKS[task_id]},
    )


with DAG(
    dag_id="sentinel_pipeline",
    description="batch path orchestrating bronze trigger, silver, gold, dbt",
    start_date=datetime(2026, 6, 27),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["sentinel"],
) as dag:

    streaming_trigger = databricks_task("streaming_trigger")
    silver = databricks_task("silver")
    gold = databricks_task("gold")

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "/home/airflow/dbt-venv/bin/dbt clean && "
            "/home/airflow/dbt-venv/bin/dbt run && "
            "/home/airflow/dbt-venv/bin/dbt test"
        ),
        env={"DBT_PROFILES_DIR": "/home/airflow/.dbt"},
        append_env=True,
    )

    streaming_trigger >> silver >> gold >> dbt_build
