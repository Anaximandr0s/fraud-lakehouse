# 0005 — Airflow standalone, dbt in its own venv, maintenance through dbt

**Context.** Two DAGs (`gold_refresh` every 10 min, `lakehouse_maintenance` daily) send SQL to spark-thrift.
The laptop budget allows one 2 GB container for orchestration, and the project runs Python 3.10 everywhere.

**Decision.** One container runs `airflow standalone` (scheduler, DAG processor, triggerer, API server) with
LocalExecutor and the Postgres `airflow` database. Pinned to `apache/airflow:3.3.2-python3.10`: Airflow 3.4
needs Python 3.11. dbt-spark lives in `/opt/dbt-venv`, installed from `uv.lock`, and `BashOperator` calls it, so
its dependencies never meet Airflow's. Maintenance is the dbt macro `maintain_schema` (`dbt run-operation`),
which calls the Iceberg procedures for every table of a schema: no Hive client or Spark in Airflow.
Failure emails use `SmtpNotifier` (`email_on_failure` is deprecated in Airflow 3.3) with an `smtp_default`
connection built from `.env`. Login: the admin password `standalone` generates (`make airflow-password`).

**Consequences.** Not for production: one process group, no high availability, a new password when the container
is recreated. Airflow 3.4+ waits for a move to Python 3.11 across the project.
