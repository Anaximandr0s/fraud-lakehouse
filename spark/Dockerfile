ARG SPARK_IMAGE
FROM ${SPARK_IMAGE}

COPY jars/spark/*.jar /opt/spark/jars/
COPY spark/conf/spark-defaults.conf /opt/spark/conf/
# Created here so the named volumes mounted on them are owned by the spark user, not root.
RUN mkdir /opt/spark/recovery /opt/spark/checkpoints
COPY src/fraud_lakehouse/ /opt/fraud-lakehouse/fraud_lakehouse/
COPY spark/jobs/ /opt/spark/jobs/
ENV PYTHONPATH=/opt/fraud-lakehouse
