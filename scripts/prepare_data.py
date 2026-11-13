"""Merges fraudTrain.csv and fraudTest.csv from the Kaggle zip into one CSV sorted by time.

Usage: prepare_data.py ZIP OUTPUT
"""

import csv
import io
import logging
import sys
import zipfile
from collections.abc import Iterator
from contextlib import ExitStack
from pathlib import Path

from fraud_lakehouse.prepare import TS, DataError, checked, concat_at_split, without_index
from fraud_lakehouse.schemas import CSV_COLUMNS, TEST_SPLIT_TS, TOTAL_ROWS

log = logging.getLogger("prepare_data")


def by_time(row: list[str]) -> str:
    return row[TS]


def read_csv(stack: ExitStack, raw: io.BufferedIOBase) -> Iterator[list[str]]:
    return csv.reader(stack.enter_context(io.TextIOWrapper(raw, encoding="utf-8", newline="")))


def is_valid(output: Path) -> bool:
    if not output.exists():
        return False
    with output.open(newline="") as f:
        rows = csv.reader(f)
        if next(rows, None) != CSV_COLUMNS:
            return False
        try:
            for _ in checked(rows, TOTAL_ROWS):
                pass
        except DataError as e:
            log.warning("%s is invalid, rebuilding: %s", output, e)
            return False
    return True


def prepare(archive: Path, output: Path) -> None:
    part = output.with_suffix(".part")
    frauds = 0
    with ExitStack() as stack:
        zf = stack.enter_context(zipfile.ZipFile(archive))
        # fraudTrain.csv has 1,858 rows out of time order; fraudTest.csv is sorted.
        train = iter(sorted(without_index(read_csv(stack, zf.open("fraudTrain.csv"))), key=by_time))
        test = without_index(read_csv(stack, zf.open("fraudTest.csv")))
        out = stack.enter_context(part.open("w", newline=""))
        writer = csv.writer(out, lineterminator="\n")
        writer.writerow(CSV_COLUMNS)
        for row in checked(concat_at_split(train, test, TEST_SPLIT_TS), TOTAL_ROWS):
            writer.writerow(row)
            frauds += row[-1] == "1"
    part.replace(output)
    log.info("wrote %s: %d rows, %d frauds", output, TOTAL_ROWS, frauds)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    archive, output = Path(sys.argv[1]), Path(sys.argv[2])
    if is_valid(output):
        log.info("%s is already valid", output)
        return
    prepare(archive, output)


if __name__ == "__main__":
    main()
