import json
from datetime import datetime

from fraud_lakehouse.schemas import CSV_COLUMNS


def to_event(row: list[str], sent_at: datetime) -> tuple[bytes, bytes]:
    """Returns (key, value): key is cc_num, value is the CSV row as JSON strings plus event_ts."""
    event = dict(zip(CSV_COLUMNS, row, strict=True))
    event["event_ts"] = sent_at.isoformat(timespec="milliseconds")
    return event["cc_num"].encode(), json.dumps(event).encode()


def send_time(start: float, sent: int, rate: float) -> float:
    return start + sent / rate
