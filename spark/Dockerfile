ARG SPARK_IMAGE
FROM ${SPARK_IMAGE}

COPY jars/spark/*.jar /opt/spark/jars/
COPY spark/conf/spark-defaults.conf /opt/spark/conf/
# Created here so the named volume mounted on it is owned by the spark user, not root.
RUN mkdir /opt/spark/recovery
