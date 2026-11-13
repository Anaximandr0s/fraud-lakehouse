import pytest

from fraud_lakehouse.config import ProducerConfig

ENV = {
    "KAFKA_BOOTSTRAP": "kafka:9092",
    "KAFKA_TOPIC": "transactions",
    "RATE": "20",
    "DATA_FILE": "/data/fraud_all.csv",
    "POSITION_FILE": "/state/position",
    "HEARTBEAT_FILE": "/tmp/heartbeat",
}


def test_from_env_reads_every_setting():
    config = ProducerConfig.from_env(ENV)

    assert config.bootstrap == "kafka:9092"
    assert config.rate == 20.0


@pytest.mark.parametrize("rate", ["0", "-5"])
def test_from_env_rejects_a_non_positive_rate(rate):
    with pytest.raises(ValueError, match="RATE"):
        ProducerConfig.from_env({**ENV, "RATE": rate})


def test_from_env_fails_on_a_missing_setting():
    env = {k: v for k, v in ENV.items() if k != "KAFKA_TOPIC"}

    with pytest.raises(KeyError, match="KAFKA_TOPIC"):
        ProducerConfig.from_env(env)
