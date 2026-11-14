ARG SPARK_IMAGE
ARG UV_IMAGE
FROM ${UV_IMAGE} AS uv

FROM ${SPARK_IMAGE} AS spark
COPY jars/spark/*.jar /opt/spark/jars/
COPY spark/conf/spark-defaults.conf /opt/spark/conf/
# Created here so the named volumes mounted on them are owned by the spark user, not root.
RUN mkdir /opt/spark/recovery /opt/spark/checkpoints
COPY src/fraud_lakehouse/ /opt/fraud-lakehouse/fraud_lakehouse/
COPY spark/jobs/ /opt/spark/jobs/
ENV PYTHONPATH=/opt/fraud-lakehouse

# Same Spark, jars and Python as the cluster, plus JupyterLab, numpy and pandas from uv.lock.
FROM spark AS jupyter
USER 0
COPY --from=uv /uv /usr/local/bin/uv
COPY pyproject.toml uv.lock /tmp/project/
RUN uv export --project /tmp/project --frozen --only-group notebook --no-emit-project -o /tmp/notebook.txt \
    && uv pip install --system --python /usr/bin/python3 --no-cache -r /tmp/notebook.txt \
    && rm -r /tmp/project /tmp/notebook.txt
ENV PYTHONPATH=/opt/fraud-lakehouse:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip
# A named user with the host's uid: notebooks saved in the bind mount stay editable on the host, and Hadoop's
# login, which needs a user name, works.
ARG HOST_UID=1000
ARG HOST_GID=1000
RUN groupadd --gid "${HOST_GID}" notebook && useradd --uid "${HOST_UID}" --gid "${HOST_GID}" --create-home notebook
USER ${HOST_UID}
