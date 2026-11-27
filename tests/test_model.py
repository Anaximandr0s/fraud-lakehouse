from datetime import datetime
from decimal import Decimal

import pytest
from pyspark.ml.linalg import Vectors

from fraud_lakehouse.model import (
    FEATURES,
    SEED,
    best_threshold,
    check_splits,
    evaluate,
    fraud_probability,
    pipeline,
    prepare,
    time_split,
    top_features,
    with_class_weight,
)

ML_COLUMNS = (
    "trans_num string, trans_ts timestamp, category string, gender string, age int,"
    " amt decimal(10,2), log_amt double, hour_of_day int, day_of_week int, is_night boolean,"
    " distance_km double, log_city_pop double, card_tx_count_1h long, card_tx_count_24h long,"
    " card_amt_ratio double, secs_since_last_tx long, is_fraud int"
)


def ml_row(n, is_fraud=0, ratio=1.5, secs=60, category="travel", gender="F"):
    ts = datetime(2019, 1, 1 + n % 28, n % 24)
    return (
        f"t{n}",
        ts,
        category,
        gender,
        40,
        Decimal(10 + n),
        2.3,
        ts.hour,
        3,
        ts.hour < 6,
        12.5,
        8.1,
        0,
        2,
        ratio,
        secs,
        is_fraud,
    )


def test_prepare_fills_missing_card_history_and_encodes_gender(spark):
    df = spark.createDataFrame([ml_row(1, ratio=None, secs=None, gender="M")], ML_COLUMNS)

    row = prepare(df).collect()[0]

    assert row.card_amt_ratio == 1.0
    assert row.secs_since_last_tx == -1.0
    assert row.is_male == 1.0
    assert row.is_night in (0.0, 1.0)
    assert all(getattr(row, f) is not None for f in FEATURES)


def test_class_weight_balances_the_two_classes(spark):
    rows = [ml_row(i, is_fraud=int(i < 2)) for i in range(10)]

    weighted = with_class_weight(prepare(spark.createDataFrame(rows, ML_COLUMNS))).collect()

    fraud_total = sum(r.weight for r in weighted if r.is_fraud == 1)
    legit_total = sum(r.weight for r in weighted if r.is_fraud == 0)
    assert fraud_total == pytest.approx(legit_total)
    assert fraud_total + legit_total == pytest.approx(len(rows))


def test_time_split_keeps_the_latest_rows_for_validation(spark):
    df = spark.createDataFrame([ml_row(i) for i in range(20)], ML_COLUMNS)

    fit, validation = time_split(df, validation_fraction=0.25)

    assert fit.count() + validation.count() == 20
    assert fit.agg({"trans_ts": "max"}).first()[0] < validation.agg({"trans_ts": "min"}).first()[0]


def scored(spark, pairs):
    return spark.createDataFrame(
        [(Vectors.dense([1 - p, p]), label) for p, label in pairs], ["probability", "is_fraud"]
    )


def test_evaluate_counts_precision_and_recall_at_the_threshold(spark):
    # At 0.5: flagged = 0.9 (fraud), 0.8 (legit), 0.6 (fraud); missed fraud = 0.2.
    pairs = [(0.9, 1), (0.8, 0), (0.6, 1), (0.2, 1), (0.1, 0), (0.05, 0)]

    metrics = evaluate(scored(spark, pairs), threshold=0.5)

    assert metrics["precision"] == pytest.approx(2 / 3)
    assert metrics["recall"] == pytest.approx(2 / 3)
    assert 0 < metrics["pr_auc"] <= 1


def test_best_threshold_maximises_f1(spark):
    pairs = [(0.95, 1), (0.9, 1), (0.85, 1), (0.6, 0), (0.55, 0), (0.3, 0), (0.1, 0)]

    assert best_threshold(scored(spark, pairs)) == pytest.approx(0.61)


def test_best_threshold_can_go_above_095_where_class_weights_push_frauds(spark):
    pairs = [(0.995, 1), (0.99, 1), (0.975, 0), (0.97, 0), (0.6, 0)]

    assert best_threshold(scored(spark, pairs)) == pytest.approx(0.98)


def test_pipeline_trains_on_the_prepared_features(spark):
    from pyspark.ml.classification import LogisticRegression

    rows = [
        ml_row(i, is_fraud=int(i % 5 == 0), category=["travel", "home"][i % 2]) for i in range(40)
    ]
    df = with_class_weight(prepare(spark.createDataFrame(rows, ML_COLUMNS)))

    model = pipeline(LogisticRegression(labelCol="is_fraud", weightCol="weight")).fit(df)

    assert "probability" in model.transform(df).columns


def test_top_features_names_the_gbt_importances(spark):
    from pyspark.ml.classification import GBTClassifier

    rows = [
        ml_row(i, is_fraud=int(i % 4 == 0), category=["travel", "home"][i % 2]) for i in range(40)
    ]
    df = prepare(spark.createDataFrame(rows, ML_COLUMNS))
    model = pipeline(GBTClassifier(labelCol="is_fraud", maxIter=3, seed=SEED)).fit(df)

    top = top_features(model, model.transform(df), 3)

    assert len(top) == 3
    assert all(isinstance(name, str) and 0 <= weight <= 1 for name, weight in top)
    assert [w for _, w in top] == sorted((w for _, w in top), reverse=True)


def test_check_splits_names_the_missing_split_and_the_fix():
    with pytest.raises(ValueError, match=r"test.*RATE"):
        check_splits({"train": 37146})


def test_check_splits_accepts_both_splits():
    check_splits({"train": 10, "test": 5})


def test_fraud_probability_is_the_second_class_probability(spark):
    df = spark.createDataFrame([(Vectors.dense([0.3, 0.7]),)], ["probability"])

    assert fraud_probability(df).first().p_fraud == pytest.approx(0.7)
