from datetime import datetime

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from fraud_lakehouse.schemas import CATEGORIES, CSV_COLUMNS, SILVER_COLUMNS

CORRUPT = "_corrupt_record"
EVENT_SCHEMA = StructType(
    [StructField(c, StringType()) for c in [*CSV_COLUMNS, "event_ts", CORRUPT]]
)
EARTH_RADIUS_KM = 6371.0
COORDINATES = ("lat", "long", "merch_lat", "merch_long")


# Checked in order: an event is rejected with the reason of the first rule it fails.
def rules() -> list[tuple[str, Column]]:
    return [
        ("malformed_json", F.col(CORRUPT).isNull() & F.col("value").isNotNull()),
        ("trans_num_null", F.col("trans_num").isNotNull()),
        ("trans_ts_invalid", F.col("trans_ts").isNotNull()),
        ("amt_not_positive", F.col("amt") > 0),
        ("category_unknown", F.col("category").isin(CATEGORIES)),
        (
            "coordinates_out_of_range",
            F.col("lat").between(-90, 90)
            & F.col("merch_lat").between(-90, 90)
            & F.col("long").between(-180, 180)
            & F.col("merch_long").between(-180, 180),
        ),
        ("is_fraud_invalid", F.col("is_fraud").isin(0, 1)),
    ]


def to_bronze(kafka: DataFrame, ingest_ts: datetime) -> DataFrame:
    """Kafka source rows → Bronze columns, with one ingest_ts for the whole batch."""
    return kafka.select(
        F.col("value").cast("string").alias("value"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_ts"),
        F.lit(ingest_ts).cast("timestamp").alias("ingest_ts"),
    )


def to_silver(bronze: DataFrame, salt: str) -> tuple[DataFrame, DataFrame]:
    """Returns (Silver rows, one per trans_num; rejected rows with their reason)."""
    checked = parse(bronze).withColumn("reason", reject_reason())
    rejected = checked.where(F.col("reason").isNotNull()).select(
        F.col("value").alias("raw_value"), "reason", "ingest_ts"
    )
    valid = checked.where(F.col("reason").isNull())
    return dedup(enrich(clean(valid, salt))).select(*SILVER_COLUMNS), rejected


def parse(bronze: DataFrame) -> DataFrame:
    event = F.from_json("value", EVENT_SCHEMA, {"columnNameOfCorruptRecord": CORRUPT})
    return bronze.withColumn("e", event).select(
        "value",
        "kafka_partition",
        "kafka_offset",
        "ingest_ts",
        F.col(f"e.{CORRUPT}").alias(CORRUPT),
        "e.trans_num",
        F.to_timestamp("e.trans_date_trans_time", "yyyy-MM-dd HH:mm:ss").alias("trans_ts"),
        F.to_timestamp("e.event_ts").alias("event_ts"),
        "e.cc_num",
        "e.merchant",
        "e.category",
        F.col("e.amt").cast("decimal(10,2)").alias("amt"),
        "e.gender",
        "e.city",
        "e.state",
        # The CSV drops leading zeros from zip codes.
        F.lpad("e.zip", 5, "0").alias("zip"),
        *[F.col(f"e.{c}").cast("double").alias(c) for c in COORDINATES],
        F.col("e.city_pop").cast("int").alias("city_pop"),
        "e.job",
        F.to_date("e.dob").alias("dob"),
        # Checked as text: a cast to int would truncate "1.9" to a valid 1.
        F.when(F.col("e.is_fraud").isin("0", "1"), F.col("e.is_fraud").cast("int")).alias(
            "is_fraud"
        ),
    )


def reject_reason() -> Column:
    reason = F.lit(None).cast("string")
    for name, passes in reversed(rules()):
        reason = F.when(~F.coalesce(passes, F.lit(False)), name).otherwise(reason)
    return reason


def clean(df: DataFrame, salt: str) -> DataFrame:
    return df.withColumn("merchant", F.regexp_replace("merchant", "^fraud_", "")).withColumn(
        "card_hash", F.sha2(F.concat(F.lit(salt), F.col("cc_num")), 256)
    )


def enrich(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("age", F.floor(F.months_between("trans_ts", "dob") / 12).cast("int"))
        .withColumn("distance_km", haversine_km("lat", "long", "merch_lat", "merch_long"))
        .withColumn("hour_of_day", F.hour("trans_ts"))
        .withColumn("day_of_week", F.dayofweek("trans_ts"))
    )


def haversine_km(lat1: str, lon1: str, lat2: str, lon2: str) -> Column:
    dlat = F.radians(F.col(lat2) - F.col(lat1))
    dlon = F.radians(F.col(lon2) - F.col(lon1))
    a = (
        F.sin(dlat / 2) ** 2
        + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(dlon / 2) ** 2
    )
    return 2 * EARTH_RADIUS_KM * F.asin(F.sqrt(a))


def dedup(df: DataFrame) -> DataFrame:
    """Keeps the first Kafka record of each trans_num: MERGE fails if two source rows match one."""
    first = Window.partitionBy("trans_num").orderBy("kafka_partition", "kafka_offset")
    return df.withColumn("n", F.row_number().over(first)).where("n = 1").drop("n")
