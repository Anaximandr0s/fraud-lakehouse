from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ProducerConfig:
    bootstrap: str
    topic: str
    rate: float
    data_file: Path
    position_file: Path
    heartbeat_file: Path

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> "ProducerConfig":
        rate = float(env["RATE"])
        if rate <= 0:
            raise ValueError(f"RATE must be positive, got {rate}")
        return cls(
            bootstrap=env["KAFKA_BOOTSTRAP"],
            topic=env["KAFKA_TOPIC"],
            rate=rate,
            data_file=Path(env["DATA_FILE"]),
            position_file=Path(env["POSITION_FILE"]),
            heartbeat_file=Path(env["HEARTBEAT_FILE"]),
        )
