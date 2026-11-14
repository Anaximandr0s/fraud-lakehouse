"""Every 10 min: dbt source freshness (warns only) → dbt run → dbt test, excluding the ml tag."""

from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, chain
from alerts import failure_email

DBT = "/opt/dbt-venv/bin/dbt"
# Own target and log dirs per DAG: at 03:00 both DAGs run dbt at the same time.
DBT_DIRS = (
    "--project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt"
    " --target-path /tmp/dbt/gold_refresh/target --log-path /tmp/dbt/gold_refresh/logs"
)

with DAG(
    dag_id="gold_refresh",
    description="rebuilds the Gold tables with dbt every 10 min (freshness, run, test)",
    schedule="*/10 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "execution_timeout": timedelta(minutes=8),
        "on_failure_callback": failure_email(),
    },
):
    chain(
        BashOperator(task_id="source_freshness", bash_command=f"{DBT} source freshness {DBT_DIRS}"),
        BashOperator(task_id="run", bash_command=f"{DBT} run --exclude tag:ml {DBT_DIRS}"),
        BashOperator(task_id="test", bash_command=f"{DBT} test --exclude tag:ml {DBT_DIRS}"),
    )
