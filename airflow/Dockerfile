ARG AIRFLOW_IMAGE
ARG UV_IMAGE
FROM ${UV_IMAGE} AS uv

FROM ${AIRFLOW_IMAGE}
USER 0
COPY --from=uv /uv /usr/local/bin/uv
COPY pyproject.toml uv.lock /tmp/project/
# dbt gets its own venv, pinned by uv.lock, so its dependencies never touch Airflow's.
RUN uv export --project /tmp/project --frozen --only-group dbt --no-emit-project -o /tmp/dbt.txt \
    && uv venv --python /usr/local/bin/python /opt/dbt-venv \
    && uv pip install --python /opt/dbt-venv/bin/python --no-cache -r /tmp/dbt.txt \
    && rm -r /tmp/project /tmp/dbt.txt
COPY dbt/ /opt/airflow/dbt/
COPY airflow/dags/ /opt/airflow/dags/
USER 50000
