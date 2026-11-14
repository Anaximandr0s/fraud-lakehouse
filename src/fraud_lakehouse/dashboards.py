"""The Metabase dashboards of docs/architecture.md §6 as data, and their API payloads."""

from dataclasses import dataclass, field
from typing import Any

GRID_WIDTH = 24
LAST_HOUR = "event_ts >= current_timestamp() - INTERVAL 1 HOUR"


def percent(*columns: str) -> dict[str, Any]:
    """Shows fraud_rate (stored as a 0–1 fraction) as a percentage."""
    return {
        "column_settings": {
            f'["name","{c}"]': {"number_style": "percent", "decimals": 2} for c in columns
        }
    }


@dataclass(frozen=True)
class Card:
    name: str
    sql: str
    display: str
    row: int
    col: int
    width: int
    height: int
    settings: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Dashboard:
    name: str
    schema: str
    description: str
    cards: tuple[Card, ...]


LIVE = Dashboard(
    name="Live",
    schema="silver",
    description="Silver, last hour of replay (event_ts). Open with #refresh=60.",
    cards=(
        Card(
            name="Live: last hour",
            sql=f"""SELECT
    count(*) AS transactions,
    sum(is_fraud) AS frauds,
    sum(is_fraud) / count(*) AS fraud_rate,
    sum(CASE WHEN is_fraud = 1 THEN amt ELSE 0 END) AS fraud_amount
FROM silver.transactions
WHERE {LAST_HOUR}""",
            display="table",
            row=0,
            col=0,
            width=24,
            height=3,
            settings=percent("fraud_rate"),
        ),
        Card(
            name="Live: transactions and frauds per minute",
            sql=f"""SELECT
    date_trunc('MINUTE', event_ts) AS minute,
    count(*) AS transactions,
    sum(is_fraud) AS frauds
FROM silver.transactions
WHERE {LAST_HOUR}
GROUP BY 1
ORDER BY 1""",
            display="line",
            row=3,
            col=0,
            width=24,
            height=6,
            settings={
                "graph.dimensions": ["minute"],
                "graph.metrics": ["transactions", "frauds"],
                "series_settings": {"frauds": {"axis": "right"}},
            },
        ),
        Card(
            name="Live: recent frauds",
            sql=f"""SELECT
    event_ts,
    trans_ts,
    merchant,
    category,
    amt,
    state,
    left(card_hash, 8) AS card
FROM silver.transactions
WHERE is_fraud = 1 AND {LAST_HOUR}
ORDER BY event_ts DESC
LIMIT 20""",
            display="table",
            row=9,
            col=0,
            width=24,
            height=8,
        ),
    ),
)


def rate_by(name: str, table: str, dimension: str, order_by: str, display: str, row: int, col: int):
    return Card(
        name=name,
        sql=f"SELECT {dimension}, fraud_rate, tx_count FROM gold.{table} ORDER BY {order_by}",
        display=display,
        row=row,
        col=col,
        width=12,
        height=7,
        settings={
            "graph.dimensions": [dimension],
            "graph.metrics": ["fraud_rate"],
            **percent("fraud_rate"),
        },
    )


PATTERNS = Dashboard(
    name="Fraud patterns",
    schema="gold",
    description="Gold, rebuilt by dbt every 10 min. Open with #refresh=600.",
    cards=(
        rate_by(
            "Patterns: fraud rate by category",
            "fraud_by_category",
            "category",
            "fraud_rate DESC",
            "row",
            0,
            0,
        ),
        rate_by(
            "Patterns: fraud rate by hour of day",
            "fraud_by_hour_of_day",
            "hour_of_day",
            "hour_of_day",
            "bar",
            0,
            12,
        ),
        Card(
            name="Patterns: fraud rate by state",
            sql="SELECT state, fraud_rate, tx_count FROM gold.fraud_by_state",
            display="map",
            row=7,
            col=0,
            width=24,
            height=9,
            settings={
                "map.type": "region",
                "map.region": "us_states",
                "map.dimension": "state",
                "map.metric": "fraud_rate",
                **percent("fraud_rate"),
            },
        ),
        rate_by(
            "Patterns: fraud rate by amount band",
            "fraud_by_amount_band",
            "amount_band",
            "band_order",
            "bar",
            16,
            0,
        ),
        rate_by(
            "Patterns: fraud rate by age band",
            "fraud_by_age_band",
            "age_band",
            "band_order",
            "bar",
            16,
            12,
        ),
    ),
)

RISK = Dashboard(
    name="Risk",
    schema="gold",
    description="Gold, rebuilt by dbt every 10 min.",
    cards=(
        Card(
            name="Risk: cards hit by fraud",
            sql="""SELECT count(*) AS cards_hit
FROM gold.customer_risk_profile
WHERE fraud_count > 0""",
            display="scalar",
            row=0,
            col=0,
            width=6,
            height=3,
        ),
        Card(
            name="Risk: top merchants",
            sql="""SELECT risk_rank, merchant, tx_count, fraud_count, fraud_rate
FROM gold.merchant_risk
ORDER BY risk_rank
LIMIT 20""",
            display="table",
            row=3,
            col=0,
            width=12,
            height=10,
            settings=percent("fraud_rate"),
        ),
        Card(
            name="Risk: top cards",
            sql="""SELECT left(card_hash, 8) AS card, tx_count, fraud_count, fraud_amt, last_ts
FROM gold.customer_risk_profile
WHERE fraud_count > 0
ORDER BY fraud_amt DESC
LIMIT 20""",
            display="table",
            row=3,
            col=12,
            width=12,
            height=10,
        ),
    ),
)

DASHBOARDS = (LIVE, PATTERNS, RISK)


def database_payload(schema: str, host: str, port: int) -> dict[str, Any]:
    """One connection per schema: the Spark SQL driver syncs only the tables of its dbname."""
    return {
        "engine": "sparksql",
        "name": f"Lakehouse {schema}",
        "details": {"host": host, "port": port, "dbname": schema, "user": "metabase"},
        # Never scan column values: that is a SELECT DISTINCT per column over Silver.
        "is_full_sync": False,
        "is_on_demand": False,
    }


def card_payload(card: Card, database_id: int, collection_id: int) -> dict[str, Any]:
    return {
        "name": card.name,
        "collection_id": collection_id,
        "display": card.display,
        "visualization_settings": card.settings,
        "dataset_query": {
            "type": "native",
            "database": database_id,
            "native": {"query": card.sql},
        },
    }


def dashcards_payload(dashboard: Dashboard, card_ids: dict[str, int]) -> list[dict[str, Any]]:
    """Negative ids ask Metabase to create the dashcards; the list replaces the existing ones."""
    return [
        {
            "id": -i,
            "card_id": card_ids[card.name],
            "row": card.row,
            "col": card.col,
            "size_x": card.width,
            "size_y": card.height,
        }
        for i, card in enumerate(dashboard.cards, start=1)
    ]
