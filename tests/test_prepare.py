import pytest

from fraud_lakehouse.prepare import DataError, checked, concat_at_split, without_index
from fraud_lakehouse.schemas import CSV_COLUMNS

RAW_HEADER = ["", *CSV_COLUMNS]


def row(ts: str, trans_num: str) -> list[str]:
    values = dict.fromkeys(CSV_COLUMNS, "x")
    values["trans_date_trans_time"] = ts
    values["trans_num"] = trans_num
    return [values[c] for c in CSV_COLUMNS]


def raw(index: int, ts: str, trans_num: str) -> list[str]:
    return [str(index), *row(ts, trans_num)]


def test_without_index_drops_the_index_column():
    rows = iter([RAW_HEADER, raw(0, "2019-01-01 00:00:18", "a")])

    assert list(without_index(rows)) == [row("2019-01-01 00:00:18", "a")]


def test_without_index_rejects_an_unexpected_header():
    rows = iter([CSV_COLUMNS, row("2019-01-01 00:00:18", "a")])

    with pytest.raises(DataError, match="header"):
        list(without_index(rows))


def test_concat_at_split_yields_train_then_test():
    train = iter([row("2019-01-01 00:00:18", "a")])
    test = iter([row("2020-06-21 12:14:25", "b")])

    rows = list(concat_at_split(train, test, "2020-06-21 12:14:25"))

    assert [r[CSV_COLUMNS.index("trans_num")] for r in rows] == ["a", "b"]


def test_concat_at_split_rejects_a_test_file_starting_elsewhere():
    train = iter([row("2019-01-01 00:00:18", "a")])
    test = iter([row("2020-06-22 00:00:00", "b")])

    with pytest.raises(DataError, match="split"):
        list(concat_at_split(train, test, "2020-06-21 12:14:25"))


def test_checked_passes_valid_rows_through():
    rows = [row("2019-01-01 00:00:18", "a"), row("2019-01-01 00:00:18", "b")]

    assert list(checked(iter(rows), expected_rows=2)) == rows


@pytest.mark.parametrize(
    ("rows", "error"),
    [
        ([row("2019-01-02 00:00:00", "a"), row("2019-01-01 00:00:00", "b")], "order"),
        ([row("2019-01-01 00:00:00", "a"), row("2019-01-02 00:00:00", "a")], "duplicate"),
        ([row("2019-01-01 00:00:00", "a"), ["too", "short"]], "columns"),
        ([row("2019-01-01 00:00:00", "a")], "rows"),
    ],
)
def test_checked_rejects_invalid_data(rows, error):
    with pytest.raises(DataError, match=error):
        list(checked(iter(rows), expected_rows=2))
