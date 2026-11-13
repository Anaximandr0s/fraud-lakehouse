"""Replays data/fraud_all.csv into Kafka at RATE events/s, resuming from its saved position."""

import csv
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from itertools import islice
from pathlib import Path

from confluent_kafka import KafkaException, Producer

from fraud_lakehouse.config import ProducerConfig
from fraud_lakehouse.events import send_time, to_event
from fraud_lakehouse.schemas import CSV_COLUMNS

log = logging.getLogger("producer")

CHECKPOINT_SECONDS = 1
IDLE_HEARTBEAT_SECONDS = 30


def read_position(path: Path) -> int:
    return int(path.read_text()) if path.exists() else 0


def save_position(path: Path, position: int) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(str(position))
    tmp.replace(path)


def on_delivery(err, _msg) -> None:
    if err is not None:
        raise KafkaException(err)


def checkpoint(producer: Producer, config: ProducerConfig, position: int) -> None:
    # Saved only once Kafka has acknowledged every event before it: a crash resends, never skips.
    unsent = producer.flush(10)
    if unsent:
        raise RuntimeError(f"{unsent} events not acknowledged by Kafka")
    save_position(config.position_file, position)
    config.heartbeat_file.touch()


def replay(producer: Producer, config: ProducerConfig) -> None:
    position = read_position(config.position_file)
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
                if time.monotonic() - last_checkpoint >= CHECKPOINT_SECONDS:
                    checkpoint(producer, config, position)
                    last_checkpoint = time.monotonic()
                if position % 10_000 == 0:
                    log.info("sent row %d", position)
        except SystemExit:
            checkpoint(producer, config, position)
            raise
        # Not in a finally: after a failed delivery, saving the position would skip that event.
        checkpoint(producer, config, position)
    log.info("end of file at row %d, idle", position)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    # As PID 1, Python ignores SIGTERM by default; exiting runs the final checkpoint.
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    config = ProducerConfig.from_env(os.environ)
    replay(Producer({"bootstrap.servers": config.bootstrap}), config)
    # Exiting would make Docker restart it in a loop; the heartbeat keeps it healthy while idle.
    while True:
        config.heartbeat_file.touch()
        time.sleep(IDLE_HEARTBEAT_SECONDS)


if __name__ == "__main__":
    main()
