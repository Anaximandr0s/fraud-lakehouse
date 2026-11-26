"""Fraud model on gold.ml_features: features, class weights, time split, Spark ML pipeline."""

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

LABEL = "is_fraud"
SEED = 42
NUMERIC = [
    "amt",
    "log_amt",
    "hour_of_day",
    "day_of_week",
    "is_night",
    "age",
    "is_male",
    "distance_km",
    "log_city_pop",
    "card_tx_count_1h",
    "card_tx_count_24h",
    "card_amt_ratio",
    "secs_since_last_tx",
]
FEATURES = [*NUMERIC, "category"]


def check_splits(rows_per_split: dict[str, int]) -> None:
    """Fails with the fix when a split is empty, instead of a cryptic error mid-training."""
    missing = [split for split in ("train", "test") if not rows_per_split.get(split)]
    if missing:
        raise ValueError(
            f"gold.ml_features has no {' or '.join(missing)} rows. The test split starts at"
            " test_split_ts (dbt/dbt_project.yml), late in the replay: let the producer get past"
            " it (RATE=500 in .env replays the CSV in about 1 h), then run make ml-features."
        )


def prepare(df: DataFrame) -> DataFrame:
    """Numeric features as doubles, with values for a card's first transactions (no history)."""
    return df.select(
        "trans_num",
        "trans_ts",
        "category",
        F.col(LABEL).cast("int").alias(LABEL),
        *[F.col(c).cast("double").alias(c) for c in NUMERIC if c not in {"is_male", "is_night"}],
        F.col("is_night").cast("double").alias("is_night"),
        (F.col("gender") == "M").cast("double").alias("is_male"),
    ).fillna({"card_amt_ratio": 1.0, "secs_since_last_tx": -1.0})


def with_class_weight(df: DataFrame) -> DataFrame:
    """Weights each class to half the total: frauds are 0.5 % of rows and would be ignored."""
    counts = {row[LABEL]: row["count"] for row in df.groupBy(LABEL).count().collect()}
    total = sum(counts.values())
    weight = F.when(F.col(LABEL) == 1, total / (2 * counts[1])).otherwise(total / (2 * counts[0]))
    return df.withColumn("weight", weight)


def time_split(df: DataFrame, validation_fraction: float) -> tuple[DataFrame, DataFrame]:
    """The latest rows become validation, like the test split comes after the train split."""
    seconds = F.unix_timestamp("trans_ts")
    # Relative error 0: an exact quantile, so every run puts the same rows in validation.
    cutoff = df.select(seconds.alias("s")).approxQuantile("s", [1 - validation_fraction], 0.0)[0]
    return df.where(seconds < cutoff), df.where(seconds >= cutoff)


def pipeline(classifier) -> Pipeline:
    return Pipeline(
        stages=[
            StringIndexer(
                inputCol="category", outputCol="category_index", stringOrderType="alphabetAsc"
            ),
            OneHotEncoder(inputCol="category_index", outputCol="category_vec"),
            VectorAssembler(inputCols=[*NUMERIC, "category_vec"], outputCol="features"),
            classifier,
        ]
    )


def fraud_probability(predictions: DataFrame) -> DataFrame:
    return predictions.withColumn("p_fraud", vector_to_array("probability")[1])


def top_features(model: PipelineModel, predictions: DataFrame, n: int) -> list[tuple[str, float]]:
    """The n largest tree importances, named from the assembled vector's metadata."""
    attrs = predictions.schema["features"].metadata["ml_attr"]["attrs"]
    names = {a["idx"]: a["name"] for group in attrs.values() for a in group}
    importances = model.stages[-1].featureImportances.toArray()
    ranked = sorted(enumerate(importances), key=lambda pair: pair[1], reverse=True)
    return [(names[i], float(weight)) for i, weight in ranked[:n]]
