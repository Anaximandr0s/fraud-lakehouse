"""Verdict of a down/up recovery test from table counts taken before and after."""

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class Counts:
    bronze: int
    silver: int
    duplicates: int
    rejected: int


def problems(
    before: Counts, after: Counts, expect_growth: bool, silver_missing: int, gaps: int
) -> list[str]:
    found = []
    for table in ("bronze", "silver", "rejected"):
        if getattr(after, table) < getattr(before, table):
            found.append(f"{table} lost rows: {getattr(before, table)} → {getattr(after, table)}")
    # Counts alone could hide a loss behind new rows.
    if silver_missing:
        found.append(f"{silver_missing} silver rows from before the restart are missing")
    if gaps:
        found.append(f"{gaps} kafka offsets after the restart never reached bronze")
    if after.duplicates:
        found.append(f"silver has {after.duplicates} duplicate trans_num")
    # Bronze appends every event, even a replayed one that Silver's MERGE skips.
    if expect_growth and after.bronze <= before.bronze:
        found.append("bronze did not grow after the restart: streaming did not resume")
    return found


def parse_rows(beeline_output: str) -> list[list[int]]:
    # Without a terminal, beeline prints its prompt on the same line as the result.
    rows = re.findall(r"(-?\d+(?:,-?\d+)*)\s*$", beeline_output, flags=re.MULTILINE)
    return [[int(v) for v in row.split(",")] for row in rows]


def new_offsets_sql(max_offsets: dict[int, int]) -> str:
    where = " OR ".join(
        f"(kafka_partition = {p} AND kafka_offset > {m})" for p, m in sorted(max_offsets.items())
    )
    return (
        "SELECT kafka_partition, count(DISTINCT kafka_offset), max(kafka_offset)"
        f" FROM bronze.transactions_raw WHERE {where} GROUP BY kafka_partition"
    )


def offset_gaps(max_before: dict[int, int], new_rows: list[list[int]]) -> int:
    """Streaming reads each partition in order, so offsets after the restart must be contiguous."""
    return sum(newest - max_before[p] - distinct for p, distinct, newest in new_rows)
