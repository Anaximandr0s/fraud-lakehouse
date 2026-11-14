"""Stops every container (volumes kept), starts the same services again, checks nothing was lost."""

import logging
import subprocess
import sys
import time
from pathlib import Path

from fraud_lakehouse.recovery import Counts, new_offsets_sql, offset_gaps, parse_rows, problems

ROOT = Path(__file__).resolve().parent.parent
COUNT_SQL = (
    "SELECT (SELECT count(*) FROM bronze.transactions_raw),"
    " (SELECT count(*) FROM silver.transactions),"
    " (SELECT count(*) - count(DISTINCT trans_num) FROM silver.transactions),"
    " (SELECT count(*) FROM silver.transactions_rejected)"
)
SNAPSHOT_SQL = (
    "SELECT snapshot_id FROM silver.transactions.snapshots ORDER BY committed_at DESC LIMIT 1"
)
MAX_OFFSETS_SQL = (
    "SELECT kafka_partition, max(kafka_offset) FROM bronze.transactions_raw"
    " GROUP BY kafka_partition"
)
MISSING_SQL = (
    "SELECT count(*) FROM silver.transactions VERSION AS OF {snapshot} b"
    " LEFT ANTI JOIN silver.transactions a ON a.trans_num = b.trans_num"
)
GROWTH_TIMEOUT_SECONDS = 300

log = logging.getLogger("recovery")


def sh(*args: str) -> str:
    return subprocess.run(args, cwd=ROOT, check=True, stdout=subprocess.PIPE, text=True).stdout


def sql(query: str) -> list[list[int]]:
    beeline = "exec -T spark-thrift /opt/spark/bin/beeline --silent=true --outputformat=csv2"
    args = f"docker compose {beeline} --showHeader=false -u jdbc:hive2://localhost:10000".split()
    return parse_rows(sh(*args, "-e", query))


def counts() -> Counts:
    return Counts(*sql(COUNT_SQL)[-1])


def producer_still_sending() -> bool:
    # No position file yet means the producer has not checkpointed: it starts at row 0.
    read = "cat /state/position 2>/dev/null || echo 0"
    position = int(sh("docker", "compose", "exec", "-T", "producer", "sh", "-c", read))
    with (ROOT / "data" / "fraud_all.csv").open("rb") as f:
        total = sum(1 for _ in f) - 1
    return position < total


def restart(services: list[str]) -> None:
    started = time.monotonic()
    sh("docker", "compose", "--profile", "*", "down")
    log.info(
        "down in %.0f s; starting %d services again", time.monotonic() - started, len(services)
    )
    # up ignores --profile '*' (down accepts it), so every profile is named.
    names = sh("docker", "compose", "config", "--profiles").split()
    profiles = [flag for name in names for flag in ("--profile", name)]
    sh("docker", "compose", *profiles, *"up -d --wait --wait-timeout 300".split(), *services)
    log.info("every service healthy %.0f s after down", time.monotonic() - started)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    running = "docker compose --profile * ps --services --status running".split()
    services = sh(*running).split()
    expect_growth = "producer" in services and producer_still_sending()
    before = counts()
    snapshot = sql(SNAPSHOT_SQL)[-1][0]
    max_offsets = dict(sql(MAX_OFFSETS_SQL))
    log.info(
        "before: %s, silver snapshot %d, producer sending: %s", before, snapshot, expect_growth
    )

    restart(services)

    after = counts()
    deadline = time.monotonic() + GROWTH_TIMEOUT_SECONDS
    while expect_growth and after.bronze <= before.bronze and time.monotonic() < deadline:
        time.sleep(30)
        after = counts()
    silver_missing = sql(MISSING_SQL.format(snapshot=snapshot))[-1][0]
    gaps = offset_gaps(max_offsets, sql(new_offsets_sql(max_offsets)))
    log.info(
        "after:  %s, silver rows missing %d, kafka offset gaps %d", after, silver_missing, gaps
    )

    found = problems(before, after, expect_growth, silver_missing, gaps)
    for problem in found:
        log.error(problem)
    if found:
        sys.exit(1)
    log.info("recovered: no row lost, no offset gap, no duplicate in silver")


if __name__ == "__main__":
    main()
