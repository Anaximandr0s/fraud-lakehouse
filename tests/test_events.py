import json
from datetime import datetime, timezone

import pytest

from fraud_lakehouse.events import send_time, to_event
from fraud_lakehouse.schemas import CSV_COLUMNS


def test_to_event_keys_by_cc_num_and_keeps_csv_strings():
    row = [f"v{i}" for i in range(len(CSV_COLUMNS))]
    row[CSV_COLUMNS.index("cc_num")] = "2703186189652095"
    row[CSV_COLUMNS.index("zip")] = "01257"
    sent_at = datetime(2026, 11, 13, 10, 0, 0, 123456, tzinfo=timezone.utc)

    key, value = to_event(row, sent_at)

    event = json.loads(value)
    assert key == b"2703186189652095"
    assert event["zip"] == "01257"
    assert event["event_ts"] == "2026-11-13T10:00:00.123+00:00"
    assert list(event) == [*CSV_COLUMNS, "event_ts"]


@pytest.mark.parametrize(("sent", "expected"), [(0, 100.0), (20, 101.0), (30, 101.5)])
def test_send_time_spaces_events_at_the_rate(sent, expected):
    assert send_time(start=100.0, sent=sent, rate=20) == expected
