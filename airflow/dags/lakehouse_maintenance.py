"""Daily 03:00: compaction, manifest rewrite, snapshot expiry, orphan cleanup, per schema."""

from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, chain
from alerts import failure_email

DBT = "/opt/dbt-venv/bin/dbt"
# Own target and log dirs per DAG: at 03:00 both DAGs run dbt at the same time.
DBT_DIRS = (
    "--project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt"
    " --target-path /tmp/dbt/lakehouse_maintenance/target"
    " --log-path /tmp/dbt/lakehouse_maintenance/logs"
)

with DAG(
    dag_id="lakehouse_maintenance",
    description="daily Iceberg maintenance: compaction, manifests, snapshot expiry, orphan files",
    schedule="0 3 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        # A streaming commit between a procedure's read and its commit fails it; rerunning is safe.
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "execution_timeout": timedelta(minutes=30),
        "on_failure_callback": failure_email(),
    },
):
    # One schema at a time: each compaction is a Spark job sharing the Thrift Server's 2 cores.
    chain(
        *[
            BashOperator(
                task_id=f"maintain_{schema}",
                bash_command=(
                    f"{DBT} run-operation maintain_schema --args '{{schema: {schema}}}' {DBT_DIRS}"
                ),
            )
            for schema in ("bronze", "silver", "gold")
        ]
    )
