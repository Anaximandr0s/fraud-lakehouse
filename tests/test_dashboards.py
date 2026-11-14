import itertools

import pytest

from fraud_lakehouse.dashboards import (
    DASHBOARDS,
    GRID_WIDTH,
    card_payload,
    dashcards_payload,
    database_payload,
)


def cells(card):
    return {
        (row, col)
        for row in range(card.row, card.row + card.height)
        for col in range(card.col, card.col + card.width)
    }


@pytest.mark.parametrize("dashboard", DASHBOARDS, ids=lambda d: d.name)
def test_cards_fit_the_grid_without_overlapping(dashboard):
    for card in dashboard.cards:
        assert card.width > 0 and card.height > 0
        assert 0 <= card.col and card.col + card.width <= GRID_WIDTH
    for a, b in itertools.combinations(dashboard.cards, 2):
        assert not cells(a) & cells(b), f"{a.name} overlaps {b.name}"


def test_live_reads_silver_and_the_others_read_gold():
    live, *gold_dashboards = DASHBOARDS
    assert live.schema == "silver"
    assert all("silver.transactions" in c.sql and "gold." not in c.sql for c in live.cards)
    for dashboard in gold_dashboards:
        assert dashboard.schema == "gold"
        assert all("gold." in c.sql and "silver." not in c.sql for c in dashboard.cards)


def test_names_are_unique_because_the_setup_script_finds_objects_by_name():
    names = [d.name for d in DASHBOARDS] + [c.name for d in DASHBOARDS for c in d.cards]
    assert len(names) == len(set(names))


def test_card_payload_is_a_native_query_in_the_collection():
    card = DASHBOARDS[0].cards[0]

    payload = card_payload(card, database_id=2, collection_id=5)

    assert payload["name"] == card.name
    assert payload["collection_id"] == 5
    assert payload["display"] == card.display
    assert payload["dataset_query"] == {
        "type": "native",
        "database": 2,
        "native": {"query": card.sql},
    }


def test_dashcards_payload_places_each_card_where_it_is_declared():
    dashboard = DASHBOARDS[0]
    card_ids = {c.name: i for i, c in enumerate(dashboard.cards, start=10)}

    dashcards = dashcards_payload(dashboard, card_ids)

    assert [d["card_id"] for d in dashcards] == list(card_ids.values())
    first = dashboard.cards[0]
    assert {k: dashcards[0][k] for k in ("row", "col", "size_x", "size_y")} == {
        "row": first.row,
        "col": first.col,
        "size_x": first.width,
        "size_y": first.height,
    }
    assert len({d["id"] for d in dashcards}) == len(dashcards)
    assert all(d["id"] < 0 for d in dashcards)


def test_database_payload_syncs_one_schema_and_never_scans_column_values():
    payload = database_payload("silver", "spark-thrift", 10000)

    assert payload["name"] == "Lakehouse silver"
    assert payload["engine"] == "sparksql"
    assert payload["details"]["host"] == "spark-thrift"
    assert payload["details"]["dbname"] == "silver"
    assert payload["is_full_sync"] is False
    assert payload["is_on_demand"] is False
