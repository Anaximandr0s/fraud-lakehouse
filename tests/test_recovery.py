from fraud_lakehouse.recovery import (
    Counts,
    new_offsets_sql,
    offset_gaps,
    parse_rows,
    problems,
)

BEFORE = Counts(bronze=1000, silver=990, duplicates=0, rejected=10)
GROWN = Counts(bronze=1600, silver=1590, duplicates=0, rejected=10)


def test_growth_without_loss_is_a_clean_recovery():
    assert problems(BEFORE, GROWN, expect_growth=True, silver_missing=0, gaps=0) == []


def test_fewer_rows_is_data_loss():
    after = Counts(bronze=1000, silver=980, duplicates=0, rejected=10)

    assert problems(BEFORE, after, expect_growth=False, silver_missing=0, gaps=0) == [
        "silver lost rows: 990 → 980"
    ]


def test_new_rows_cannot_hide_rows_that_disappeared():
    assert problems(BEFORE, GROWN, expect_growth=True, silver_missing=4, gaps=0) == [
        "4 silver rows from before the restart are missing"
    ]


def test_skipped_kafka_offsets_are_data_loss():
    assert problems(BEFORE, GROWN, expect_growth=True, silver_missing=0, gaps=7) == [
        "7 kafka offsets after the restart never reached bronze"
    ]


def test_duplicates_in_silver_are_reported():
    after = Counts(bronze=1600, silver=1590, duplicates=3, rejected=10)

    assert problems(BEFORE, after, expect_growth=True, silver_missing=0, gaps=0) == [
        "silver has 3 duplicate trans_num"
    ]


def test_no_new_row_while_the_producer_sends_means_streaming_did_not_resume():
    assert problems(BEFORE, BEFORE, expect_growth=True, silver_missing=0, gaps=0) == [
        "bronze did not grow after the restart: streaming did not resume"
    ]


def test_a_replay_grows_bronze_only_and_is_a_clean_recovery():
    after = Counts(bronze=1600, silver=990, duplicates=0, rejected=10)

    assert problems(BEFORE, after, expect_growth=True, silver_missing=0, gaps=0) == []


def test_rows_are_read_from_beeline_output_behind_its_prompt():
    out = (
        "0: jdbc:hive2://localhost:10000> 2528357,2497000,0,12\n0: jdbc:hive2://localhost:10000> \n"
    )

    assert parse_rows(out) == [[2528357, 2497000, 0, 12]]


def test_new_offsets_are_selected_per_partition_above_the_previous_maximum():
    assert new_offsets_sql({0: 10, 1: 20}) == (
        "SELECT kafka_partition, count(DISTINCT kafka_offset), max(kafka_offset)"
        " FROM bronze.transactions_raw"
        " WHERE (kafka_partition = 0 AND kafka_offset > 10)"
        " OR (kafka_partition = 1 AND kafka_offset > 20)"
        " GROUP BY kafka_partition"
    )


def test_offset_gaps_count_offsets_missing_after_the_previous_maximum():
    before = {0: 10, 1: 20, 2: 30}
    after = [[0, 5, 15], [1, 3, 25]]

    assert offset_gaps(before, after) == 2
