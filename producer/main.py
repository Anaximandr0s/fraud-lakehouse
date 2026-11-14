"""Replays data/fraud_all.csv into Kafka at RATE events/s and serves its metrics on METRICS_PORT."""

import logging
import os
import signal
import sys
import time

from confluent_kafka import Producer
from prometheus_client import REGISTRY, start_http_server

from fraud_lakehouse.config import ProducerConfig
from fraud_lakehouse.replay import ReplayMetrics, replay

IDLE_HEARTBEAT_SECONDS = 30


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    # As PID 1, Python ignores SIGTERM by default; exiting runs the final checkpoint.
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    config = ProducerConfig.from_env(os.environ)
    metrics = ReplayMetrics(REGISTRY)
    start_http_server(config.metrics_port)
    replay(Producer({"bootstrap.servers": config.bootstrap}), config, metrics)
    # Exiting would make Docker restart it in a loop; the heartbeat keeps it healthy while idle.
    while True:
        config.heartbeat_file.touch()
        time.sleep(IDLE_HEARTBEAT_SECONDS)


if __name__ == "__main__":
    main()
