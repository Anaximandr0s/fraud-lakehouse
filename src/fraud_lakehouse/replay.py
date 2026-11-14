"""Replays the CSV into Kafka at a fixed rate, resuming from a saved row position."""

import csv
import logging
import time
from datetime import datetime, timezone
from itertools import islice
from pathlib import Path
from typing import Any

from prometheus_client import CollectorRegistry, Counter, Gauge

from fraud_lakehouse.config import ProducerConfig
from fraud_lakehouse.events import send_time, to_event
from fraud_lakehouse.schemas import CSV_COLUMNS

log = logging.getLogger("producer")

CHECKPOINT_SECONDS = 1


class ReplayMetrics:
    def __init__(self, registry: CollectorRegistry) -> None:
        self.rows_sent = Counter(
            "producer_rows_sent", "CSV rows handed to Kafka", registry=registry
        )
        self.position = Gauge(
            "producer_position",
            "CSV rows acknowledged by Kafka, saved for resume",
            registry=registry,
        )
        # Lets the "no streaming input" alert stay silent once the whole CSV has been replayed.
        self.finished = Gauge(
            "producer_finished", "1 once the end of the CSV is reached", registry=registry
        )


def read_position(path: Path) -> int:
    return int(path.read_text()) if path.exists() else 0


def save_position(path: Path, position: int) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(str(position))
    tmp.replace(path)


def on_delivery(err: Any, _msg: Any) -> None:
    if err is not None:
        raise RuntimeError(f"Kafka delivery failed: {err}")


def checkpoint(
    producer: Any, config: ProducerConfig, metrics: ReplayMetrics, position: int
) -> None:
    # Saved only once Kafka has acknowledged every event before it: a crash resends, never skips.
    unsent = producer.flush(10)
    if unsent:
        raise RuntimeError(f"{unsent} events not acknowledged by Kafka")
    save_position(config.position_file, position)
    metrics.position.set(position)
    config.heartbeat_file.touch()


def replay(producer: Any, config: ProducerConfig, metrics: ReplayMetrics) -> None:
    """Sends every row after the saved position; producer is a confluent_kafka.Producer."""
    position = read_position(config.position_file)
    metrics.position.set(position)
    metrics.finished.set(0)
    log.info("starting at row %d, %s events/s", position, config.rate)
    with config.data_file.open(newline="") as f:
        rows = csv.reader(f)
        if next(rows) != CSV_COLUMNS:
            raise ValueError(f"unexpected header in {config.data_file}")
        start = last_checkpoint = time.monotonic()
        try:
            for sent, row in enumerate(islice(rows, position, None)):
                delay = send_time(start, sent, config.rate) - time.monotonic()
                if delay > 0:
                    time.sleep(delay)
                key, value = to_event(row, datetime.now(timezone.utc))
                producer.produce(config.topic, value, key, on_delivery=on_delivery)
                producer.poll(0)
                position += 1
                metrics.rows_sent.inc()
                if time.monotonic() - last_checkpoint >= CHECKPOINT_SECONDS:
                    checkpoint(producer, config, metrics, position)
                    last_checkpoint = time.monotonic()
                if position % 10_000 == 0:
                    log.info("sent row %d", position)
        except SystemExit:
            checkpoint(producer, config, metrics, position)
            raise
        # Not in a finally: after a failed delivery, saving the position would skip that event.
        checkpoint(producer, config, metrics, position)
    metrics.finished.set(1)
    log.info("end of file at row %d, idle", position)
