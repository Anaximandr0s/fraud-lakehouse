import json
from datetime import datetime, timezone
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from fraud_lakehouse.schemas import CSV_COLUMNS, SILVER_COLUMNS
from fraud_lakehouse.silver import to_bronze, to_silver

INGEST_TS = datetime(2026, 11, 14, 9, 0, tzinfo=timezone.utc)

EVENT = {
    "trans_date_trans_time": "2019-01-01 00:00:18",
    "cc_num": "2703186189652095",
    "merchant": "fraud_Rippin, Kub and Mann",
    "category": "misc_net",
    "amt": "4.97",
    "first": "Jennifer",
    "last": "Banks",
    "gender": "F",
    "street": "561 Perry Cove",
    "city": "Moravian Falls",
    "state": "NC",
    "zip": "1257",
    "lat": "36.0788",
    "long": "-81.1781",
    "city_pop": "3495",
    "job": "Psychologist, counselling",
    "dob": "1988-03-09",
    "trans_num": "0b242abb623afc578575680df30655b9",
    "unix_time": "1325376018",
    "merch_lat": "36.011293",
    "merch_long": "-82.048315",
    "is_fraud": "0",
    "event_ts": "2026-11-13T20:52:35.459+00:00",
}


def kafka_batch(spark, values, offsets=None):
    offsets = offsets or list(range(len(values)))
    rows = [
        (v.encode(), "transactions", 0, o, datetime(2026, 11, 13, tzinfo=timezone.utc))
        for v, o in zip(values, offsets, strict=True)
    ]
    return spark.createDataFrame(
        rows, "value binary, topic string, partition int, offset long, timestamp timestamp"
    )


def silver_of(spark, *events, salt="salt"):
    batch = to_bronze(kafka_batch(spark, [json.dumps(e) for e in events]), INGEST_TS)
    valid, rejected = to_silver(batch, salt)
    return valid.collect(), rejected.collect()


def test_to_bronze_keeps_the_raw_value_and_kafka_metadata(spark):
    bronze = to_bronze(kafka_batch(spark, ['{"a": 1}'], offsets=[42]), INGEST_TS).collect()

    assert bronze[0].asDict() == {
        "value": '{"a": 1}',
        "kafka_topic": "transactions",
        "kafka_partition": 0,
        "kafka_offset": 42,
        "kafka_ts": datetime(2026, 11, 13),
        "ingest_ts": datetime(2026, 11, 14, 9, 0),
    }


def test_to_silver_types_cleans_and_enriches_a_valid_event(spark):
    valid, rejected = silver_of(spark, EVENT)

    assert rejected == []
    row = valid[0]
    assert row.__fields__ == SILVER_COLUMNS
    assert row.trans_ts == datetime(2019, 1, 1, 0, 0, 18)
    assert row.event_ts == datetime(2026, 11, 13, 20, 52, 35, 459000)
    assert row.merchant == "Rippin, Kub and Mann"
    assert row.amt == Decimal("4.97")
    assert row.zip == "01257"
    assert row.age == 30
    assert row.distance_km == pytest.approx(78.6, abs=0.1)
    assert row.hour_of_day == 0
    assert row.day_of_week == 3
    assert row.is_fraud == 0
    assert row.ingest_ts == datetime(2026, 11, 14, 9, 0)


def test_card_hash_is_salted_sha256_of_cc_num(spark):
    first = silver_of(spark, EVENT, salt="a")[0][0].card_hash
    second = silver_of(spark, EVENT, salt="b")[0][0].card_hash

    assert len(first) == 64
    assert first != second
    assert "2703186189652095" not in first


def test_age_counts_full_years_only(spark):
    event = {**EVENT, "dob": "1988-01-01", "trans_date_trans_time": "2019-12-31 23:59:59"}

    assert silver_of(spark, event)[0][0].age == 31


@pytest.mark.parametrize(
    ("change", "reason"),
    [
        ({"trans_num": None}, "trans_num_null"),
        ({"trans_date_trans_time": "not a date"}, "trans_ts_invalid"),
        ({"trans_date_trans_time": "2019-1-1 00:00:18"}, "trans_ts_invalid"),
        ({"amt": "0"}, "amt_not_positive"),
        ({"amt": "abc"}, "amt_not_positive"),
        ({"category": "crypto"}, "category_unknown"),
        ({"lat": "91"}, "coordinates_out_of_range"),
        ({"merch_long": "-181"}, "coordinates_out_of_range"),
        ({"is_fraud": "2"}, "is_fraud_invalid"),
        ({"is_fraud": "1.9"}, "is_fraud_invalid"),
    ],
)
def test_to_silver_rejects_invalid_events_with_a_reason(spark, change, reason):
    valid, rejected = silver_of(spark, {**EVENT, **change})

    assert valid == []
    assert [(r.reason, r.ingest_ts) for r in rejected] == [(reason, datetime(2026, 11, 14, 9, 0))]
    assert json.loads(rejected[0].raw_value)["trans_num"] == change.get(
        "trans_num", EVENT["trans_num"]
    )


def test_to_silver_rejects_malformed_json(spark):
    batch = to_bronze(kafka_batch(spark, ['{"trans_num": "x", broken']), INGEST_TS)

    valid, rejected = to_silver(batch, "salt")

    assert valid.collect() == []
    assert [(r.raw_value, r.reason) for r in rejected.collect()] == [
        ('{"trans_num": "x", broken', "malformed_json")
    ]


def test_to_silver_rejects_an_empty_kafka_value_as_malformed(spark):
    batch = to_bronze(kafka_batch(spark, [""]), INGEST_TS).withColumn("value", F.lit(None))

    _, rejected = to_silver(batch, "salt")

    assert [r.reason for r in rejected.collect()] == ["malformed_json"]


def test_to_silver_keeps_one_row_per_trans_num_in_a_batch(spark):
    values = [json.dumps(EVENT), json.dumps({**EVENT, "amt": "9.99"})]
    batch = to_bronze(kafka_batch(spark, values, offsets=[7, 3]), INGEST_TS)

    valid, _ = to_silver(batch, "salt")

    assert [r.amt for r in valid.collect()] == [Decimal("9.99")]


def test_silver_drops_personal_columns():
    assert set(CSV_COLUMNS) - set(SILVER_COLUMNS) == {
        "cc_num",
        "first",
        "last",
        "street",
        "dob",
        "unix_time",
        "trans_date_trans_time",
    }
