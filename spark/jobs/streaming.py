"""Kafka → bronze.transactions_raw → silver.transactions, one foreachBatch every 30 s."""

import logging
import os
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession

from fraud_lakehouse.config import StreamingConfig
from fraud_lakehouse.schemas import BRONZE_TABLE, REJECTED_TABLE, SILVER_TABLE
from fraud_lakehouse.silver import to_bronze, to_silver

log = logging.getLogger("streaming")

DDL = [
    "CREATE DATABASE IF NOT EXISTS bronze",
    "CREATE DATABASE IF NOT EXISTS silver",
    f"""CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} (
        value STRING, kafka_topic STRING, kafka_partition INT, kafka_offset BIGINT,
        kafka_ts TIMESTAMP, ingest_ts TIMESTAMP
    ) USING iceberg PARTITIONED BY (days(ingest_ts))""",
    f"""CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
        trans_num STRING, trans_ts TIMESTAMP, event_ts TIMESTAMP, card_hash STRING,
        merchant STRING, category STRING, amt DECIMAL(10,2), gender STRING, city STRING,
        state STRING, zip STRING, lat DOUBLE, long DOUBLE, merch_lat DOUBLE, merch_long DOUBLE,
        city_pop INT, job STRING, age INT, distance_km DOUBLE, hour_of_day INT, day_of_week INT,
        is_fraud INT, ingest_ts TIMESTAMP
    ) USING iceberg PARTITIONED BY (months(trans_ts))
    TBLPROPERTIES ('format-version' = '2', 'write.merge.mode' = 'merge-on-read')""",
    f"""CREATE TABLE IF NOT EXISTS {REJECTED_TABLE} (
        raw_value STRING, reason STRING, ingest_ts TIMESTAMP
    ) USING iceberg""",
]

# Insert only: a replayed event is identical to the stored one, so nothing needs updating.
MERGE = f"""
    MERGE INTO {SILVER_TABLE} t USING silver_batch s ON t.trans_num = s.trans_num
    WHEN NOT MATCHED THEN INSERT *
"""


def write_batch(kafka_batch: DataFrame, batch_id: int, salt: str) -> None:
    # Persisted so Bronze and Silver come from one read of Kafka with one ingest_ts.
    bronze = to_bronze(kafka_batch, datetime.now(timezone.utc)).persist()
    try:
        if bronze.isEmpty():
            return
        bronze.writeTo(BRONZE_TABLE).append()
        silver, rejected = to_silver(bronze, salt)
        rejected.writeTo(REJECTED_TABLE).append()
        silver.createOrReplaceTempView("silver_batch")
        bronze.sparkSession.sql(MERGE)
        log.info("batch %d: %d Kafka records", batch_id, bronze.count())
    finally:
        bronze.unpersist()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    config = StreamingConfig.from_env(os.environ)
    spark = SparkSession.builder.appName("spark-streaming").getOrCreate()
    for statement in DDL:
        spark.sql(statement)

    kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.bootstrap)
        .option("subscribe", config.topic)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", config.max_offsets_per_trigger)
        # Offsets already deleted by Kafka retention mean lost events: stop instead of skipping.
        .option("failOnDataLoss", "true")
        .load()
    )
    query = (
        kafka.writeStream.foreachBatch(
            lambda batch, batch_id: write_batch(batch, batch_id, config.hash_salt)
        )
        .option("checkpointLocation", config.checkpoint_dir)
        .trigger(processingTime="30 seconds")
        .start()
    )
    # A failed query raises here and ends the process, so Docker restarts the container.
    query.awaitTermination()


if __name__ == "__main__":
    main()
