from collections.abc import Iterator

from fraud_lakehouse.schemas import CSV_COLUMNS

TS = CSV_COLUMNS.index("trans_date_trans_time")
TRANS_NUM = CSV_COLUMNS.index("trans_num")


class DataError(ValueError):
    pass


def without_index(rows: Iterator[list[str]]) -> Iterator[list[str]]:
    """Checks a raw Kaggle header and drops its unnamed index column; yields data rows only."""
    header = next(rows)
    if header != ["", *CSV_COLUMNS]:
        raise DataError(f"unexpected header: {header}")
    for row in rows:
        yield row[1:]


def concat_at_split(
    train: Iterator[list[str]], test: Iterator[list[str]], split_ts: str
) -> Iterator[list[str]]:
    yield from train
    first = next(test)
    if first[TS] != split_ts:
        raise DataError(f"test file starts at {first[TS]}, expected split {split_ts}")
    yield first
    yield from test


def checked(rows: Iterator[list[str]], expected_rows: int) -> Iterator[list[str]]:
    """Yields rows unchanged; fails on a bad width, a timestamp out of order,
    a duplicate trans_num, or a final count other than expected_rows."""
    seen: set[str] = set()
    last_ts = ""
    for count, row in enumerate(rows, start=1):
        if len(row) != len(CSV_COLUMNS):
            raise DataError(f"row {count}: {len(row)} columns, expected {len(CSV_COLUMNS)}")
        if row[TS] < last_ts:
            raise DataError(f"row {count}: {row[TS]} breaks the order after {last_ts}")
        if row[TRANS_NUM] in seen:
            raise DataError(f"row {count}: duplicate trans_num {row[TRANS_NUM]}")
        seen.add(row[TRANS_NUM])
        last_ts = row[TS]
        yield row
    if len(seen) != expected_rows:
        raise DataError(f"{len(seen)} rows, expected {expected_rows}")
