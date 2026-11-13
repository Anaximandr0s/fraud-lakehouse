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


@dataclass(frozen=True)
class StreamingConfig:
    bootstrap: str
    topic: str
    checkpoint_dir: str
    max_offsets_per_trigger: int
    hash_salt: str

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> "StreamingConfig":
        max_offsets = int(env["MAX_OFFSETS_PER_TRIGGER"])
        if max_offsets <= 0:
            raise ValueError(f"MAX_OFFSETS_PER_TRIGGER must be positive, got {max_offsets}")
        # Without a salt, card_hash could be reversed by hashing every possible card number.
        if not env["HASH_SALT"]:
            raise ValueError("HASH_SALT must not be empty")
        return cls(
            bootstrap=env["KAFKA_BOOTSTRAP"],
            topic=env["KAFKA_TOPIC"],
            checkpoint_dir=env["CHECKPOINT_DIR"],
            max_offsets_per_trigger=max_offsets,
            hash_salt=env["HASH_SALT"],
        )
