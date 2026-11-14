import csv
import json

import pytest
from prometheus_client import CollectorRegistry

from fraud_lakehouse.config import ProducerConfig
from fraud_lakehouse.replay import ReplayMetrics, replay
from fraud_lakehouse.schemas import CSV_COLUMNS


class FakeKafka:
    """Records produced events; stops the process (like SIGTERM) on the given call if asked."""

    def __init__(self, exit_on_call: int | None = None) -> None:
        self.sent: list[tuple[bytes, bytes]] = []
        self.exit_on_call = exit_on_call

    def produce(self, topic, value, key, on_delivery=None):
        if self.exit_on_call == len(self.sent) + 1:
            raise SystemExit(0)
        self.sent.append((key, value))

    def poll(self, timeout):
        return 0

    def flush(self, timeout):
        return 0


@pytest.fixture
def config(tmp_path):
    data = tmp_path / "fraud_all.csv"
    with data.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(CSV_COLUMNS)
        for i in range(5):
            row = dict.fromkeys(CSV_COLUMNS, "x")
            row["cc_num"] = f"card{i}"
            row["trans_num"] = f"t{i}"
            writer.writerow([row[c] for c in CSV_COLUMNS])
    return ProducerConfig(
        bootstrap="unused",
        topic="transactions",
        rate=1_000_000,
        data_file=data,
        position_file=tmp_path / "position",
        heartbeat_file=tmp_path / "heartbeat",
        metrics_port=8000,
    )


@pytest.fixture
def metrics():
    return ReplayMetrics(CollectorRegistry())


def test_replay_sends_every_row_then_reports_finished(config, metrics):
    kafka = FakeKafka()

    replay(kafka, config, metrics)

    assert [k for k, _ in kafka.sent] == [f"card{i}".encode() for i in range(5)]
    assert json.loads(kafka.sent[0][1])["trans_num"] == "t0"
    assert config.position_file.read_text() == "5"
    assert metrics.rows_sent._value.get() == 5
    assert metrics.position._value.get() == 5
    assert metrics.finished._value.get() == 1


def test_replay_resumes_after_the_saved_position(config, metrics):
    config.position_file.write_text("3")
    kafka = FakeKafka()

    replay(kafka, config, metrics)

    assert [k for k, _ in kafka.sent] == [b"card3", b"card4"]
    assert metrics.rows_sent._value.get() == 2
    assert metrics.position._value.get() == 5
    assert metrics.finished._value.get() == 1


def test_a_stop_before_the_end_saves_the_position_and_is_not_finished(config, metrics):
    kafka = FakeKafka(exit_on_call=3)

    with pytest.raises(SystemExit):
        replay(kafka, config, metrics)

    assert config.position_file.read_text() == "2"
    assert metrics.position._value.get() == 2
    assert metrics.finished._value.get() == 0
