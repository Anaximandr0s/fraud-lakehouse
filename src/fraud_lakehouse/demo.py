"""What each box of the demo page is, what it may do, and parsers for its live state."""

import base64
import json
import re
from dataclasses import asdict, dataclass, field

from fraud_lakehouse.dashboards import DASHBOARDS, REFRESH_SECONDS

METABASE = "http://localhost:3000"
STREAMING_PREFIX = "metrics_spark_streaming_driver_spark_streaming_transactions_"


@dataclass(frozen=True)
class Link:
    label: str
    url: str


@dataclass(frozen=True)
class Node:
    key: str
    label: str
    layer: str
    role: str
    ports: tuple[str, ...] = ()
    links: tuple[Link, ...] = ()
    profile: str | None = None
    # Only services ingestion (Kafka to Silver) never waits on; Airflow stopped pauses Gold.
    stoppable: bool = False
    container: bool = True
    details: tuple[str, ...] = field(default=())
    shell: tuple[str, ...] = ()


def minio_folder(prefix: str) -> str:
    # The console addresses a folder by the base64 of its prefix.
    return "http://localhost:9001/browser/lakehouse/" + base64.b64encode(prefix.encode()).decode()


NODES = (
    Node(
        "csv",
        "fraud_all.csv",
        "ingest",
        "Kaggle credit-card dataset (2019–2020), train and test files merged in time order.",
        container=False,
        details=("1,852,394 transactions", "0.52 % fraud", "data/fraud_all.csv, mounted read only"),
    ),
    Node(
        "producer",
        "Producer",
        "ingest",
        "Replays the CSV into Kafka at RATE events/s and resumes from its saved position.",
        ports=("8000 metrics (127.0.0.1)",),
        stoppable=True,
        details=(
            "Python, confluent-kafka",
            "Position saved once Kafka acknowledges",
            "key = cc_num",
        ),
    ),
    Node(
        "kafka",
        "Kafka",
        "ingest",
        "Event log between the producer and Spark: topic transactions, 3 partitions, 24 h.",
        ports=("9094 external listener", "9092 internal"),
        details=("KRaft, one broker", "Spark keeps its own offsets in its checkpoint"),
    ),
    Node(
        "spark-streaming",
        "Spark Streaming",
        "process",
        "Every 30 s: Kafka → Bronze (raw) → Silver (typed, deduplicated with MERGE on trans_num).",
        ports=("4040 driver UI",),
        links=(Link("Streaming query", "http://localhost:4040/StreamingQuery/"),),
        details=("One foreachBatch per micro-batch", "Checkpoint on a local volume"),
    ),
    Node(
        "spark-master",
        "Spark master",
        "platform",
        "Standalone cluster manager: gives each application at most 2 cores.",
        ports=("8080 UI", "7077 cluster"),
        links=(Link("Master UI", "http://localhost:8080"),),
    ),
    Node(
        "spark-worker",
        "Spark worker",
        "platform",
        "Runs the executors of streaming, thrift and Jupyter: 6 cores, 1920 MB.",
        ports=("8081 UI",),
        links=(Link("Worker UI", "http://localhost:8081"),),
    ),
    Node(
        "spark-thrift",
        "Thrift Server",
        "serve",
        "JDBC SQL endpoint over the Iceberg tables, used by dbt, Metabase and Beeline.",
        ports=("10000 JDBC", "4041 UI"),
        links=(Link("Thrift UI", "http://localhost:4041"),),
    ),
    Node(
        "bronze",
        "Bronze",
        "store",
        "bronze.transactions_raw: every Kafka message as received, append only.",
        container=False,
        links=(Link("Files in MinIO", minio_folder("warehouse/bronze.db/")),),
        details=("Iceberg, partitioned by day of ingestion", "Keeps replay duplicates"),
    ),
    Node(
        "silver",
        "Silver",
        "store",
        "silver.transactions: one typed row per trans_num; invalid rows go to the rejected table.",
        container=False,
        links=(Link("Files in MinIO", minio_folder("warehouse/silver.db/")),),
        details=("Iceberg, partitioned by month", "Written ordered by event_ts"),
    ),
    Node(
        "gold",
        "Gold",
        "store",
        "Aggregates and ML features built by dbt from Silver.",
        container=False,
        links=(Link("Files in MinIO", minio_folder("warehouse/gold.db/")),),
        details=("Refreshed every 10 min by Airflow", "gold.ml_features on demand"),
    ),
    Node(
        "minio",
        "MinIO",
        "platform",
        "S3-compatible object store: holds every Iceberg data and metadata file.",
        ports=("9000 S3 API", "9001 console"),
        links=(Link("Console", "http://localhost:9001/browser/lakehouse"),),
    ),
    Node(
        "hive-metastore",
        "Hive Metastore",
        "platform",
        "Iceberg catalog: maps each table name to its current metadata file.",
        ports=("9083 Thrift (internal)",),
        details=("Its own tables live in the PostgreSQL database metastore",),
        shell=("make psql db=metastore",),
    ),
    Node(
        "postgres",
        "PostgreSQL",
        "platform",
        "Databases of the metastore, Airflow and Metabase.",
        ports=("5432 (internal)",),
        shell=("make psql db=metastore", "make psql db=airflow", "make psql db=metabase"),
    ),
    Node(
        "dbt",
        "dbt",
        "process",
        "SQL models and tests that build Gold through the Thrift Server.",
        container=False,
        details=("Runs inside the Airflow container", "staging view → gold tables"),
    ),
    Node(
        "airflow",
        "Airflow",
        "process",
        "Schedules gold_refresh every 10 min and lakehouse_maintenance daily.",
        ports=("8082 UI",),
        links=(Link("DAGs", "http://localhost:8082"),),
        profile="orchestration",
        stoppable=True,
    ),
    Node(
        "metabase",
        "Metabase",
        "serve",
        "Dashboards: Live on Silver, Fraud patterns and Risk on Gold.",
        ports=("3000 UI",),
        links=(Link("All dashboards", f"{METABASE}/collection/root"),),
        profile="bi",
        stoppable=True,
    ),
    Node(
        "jupyter",
        "JupyterLab",
        "serve",
        "PySpark notebooks on the cluster: fraud model trained on gold.ml_features.",
        ports=("8888 UI",),
        links=(Link("JupyterLab", "http://localhost:8888/lab"),),
        profile="notebook",
        stoppable=True,
    ),
    Node(
        "beeline",
        "Beeline",
        "serve",
        "Command-line SQL client of the Thrift Server, from the spark-thrift image.",
        container=False,
        details=("Runs once per command, not a service",),
        shell=(
            "make beeline",
            "SELECT count(*) FROM silver.transactions;",
            "SELECT * FROM gold.fraud_by_category LIMIT 5;",
        ),
    ),
    Node(
        "prometheus",
        "Prometheus",
        "observability",
        "Scrapes Spark, the producer, MinIO and cAdvisor every 15 s.",
        ports=("9090 UI",),
        links=(Link("Targets", "http://localhost:9090/targets"),),
        profile="monitoring",
        stoppable=True,
    ),
    Node(
        "grafana",
        "Grafana",
        "observability",
        "Pipeline dashboard and six email alerts.",
        ports=("3001 UI",),
        links=(
            Link("Pipeline dashboard", "http://localhost:3001/d/pipeline"),
            Link("Alert rules", "http://localhost:3001/alerting/list"),
        ),
        profile="monitoring",
        stoppable=True,
    ),
    Node(
        "cadvisor",
        "cAdvisor",
        "observability",
        "Memory and CPU of every container, read by Prometheus.",
        ports=("8080 (internal)",),
        profile="monitoring",
        stoppable=True,
    ),
)

BY_KEY = {node.key: node for node in NODES}

GLOBAL_ACTIONS = {
    "start-all": [["make", "up-all"]],
    "stop-all": [["make", "down"]],
    "reset": [["docker", "compose", "--profile", "*", "down", "-v"]],
}


def catalog() -> list[dict]:
    return [asdict(node) for node in NODES]


def commands(action: str, key: str | None) -> list[list[str]]:
    """The fixed command lines behind one button; anything not listed is refused."""
    if action in GLOBAL_ACTIONS:
        return GLOBAL_ACTIONS[action]
    if action not in ("start", "stop"):
        raise ValueError(f"unknown action {action!r}")
    node = BY_KEY.get(key or "")
    if node is None or not node.stoppable:
        raise ValueError(f"{key} cannot be stopped or started from the demo page")
    if action == "start" and key == "jupyter":
        # Built on demand, unlike the images of make up-all; make up-notebook would also restart
        # every stopped core service, a paused producer included.
        up = "up -d --build --no-deps --wait --wait-timeout 180".split()
        return [["docker", "compose", "--profile", "notebook", *up, "jupyter"]]
    profile = ["--profile", node.profile] if node.profile else []
    if action == "stop":
        return [["docker", "compose", *profile, "stop", node.key]]
    return [["docker", "compose", *profile, "up", "-d", "--no-deps", "--wait", node.key]]


def parse_ps(text: str) -> dict[str, dict[str, str]]:
    rows = (json.loads(line) for line in text.splitlines() if line.strip())
    return {
        r["Service"]: {"state": r["State"], "health": r["Health"], "image": r["Image"]}
        for r in rows
    }


def parse_stats(text: str, project: str) -> dict[str, dict]:
    name = re.compile(rf"^{re.escape(project)}-(.+)-\d+$")
    stats = {}
    for line in text.splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        match = name.match(row["Name"])
        if match:
            stats[match.group(1)] = {
                "memory": row["MemUsage"],
                "memory_percent": float(row["MemPerc"].rstrip("%")),
                "cpu_percent": float(row["CPUPerc"].rstrip("%")),
            }
    return stats


PROMETHEUS_LINE = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{.*\})? (\S+)$")


def parse_metrics(text: str) -> dict[str, float]:
    """Prometheus text format; other lines (Spark serves plain text while starting) are skipped."""
    matches = (PROMETHEUS_LINE.match(line) for line in text.splitlines())
    return {m.group(1): float(m.group(2)) for m in matches if m}


def streaming_rates(metrics: dict[str, float]) -> dict[str, float | None]:
    return {
        "input_per_second": metrics.get(f"{STREAMING_PREFIX}inputRate_total_Value"),
        "processed_per_second": metrics.get(f"{STREAMING_PREFIX}processingRate_total_Value"),
        "batch_ms": metrics.get(f"{STREAMING_PREFIX}latency_Value"),
    }


def progress(position: int, total: int, rate: float) -> dict:
    remaining = max(total - position, 0)
    return {
        "position": position,
        "total": total,
        "percent": round(100 * position / total, 1) if total else 0.0,
        "remaining": remaining,
        "eta_seconds": round(remaining / rate) if rate > 0 and remaining else None,
    }


DATABASES = {
    "metastore": "every table name and its current Iceberg metadata file (Hive Metastore)",
    "airflow": "DAG runs and task states",
    "metabase": "users, questions and dashboards",
    "postgres": "default database, empty",
}
TABLES_SQL = """
SELECT d."NAME", t."TBL_NAME", t."TBL_TYPE",
  max(CASE WHEN p."PARAM_KEY" = 'numRows' THEN p."PARAM_VALUE" END),
  max(CASE WHEN p."PARAM_KEY" = 'numFiles' THEN p."PARAM_VALUE" END),
  max(CASE WHEN p."PARAM_KEY" = 'totalSize' THEN p."PARAM_VALUE" END),
  max(CASE WHEN p."PARAM_KEY" = 'current-snapshot-timestamp-ms' THEN p."PARAM_VALUE" END)
FROM "TBLS" t JOIN "DBS" d USING ("DB_ID") LEFT JOIN "TABLE_PARAMS" p USING ("TBL_ID")
GROUP BY 1, 2, 3 ORDER BY 1, 2
"""
DATABASES_SQL = (
    "SELECT datname, pg_database_size(datname) FROM pg_database WHERE NOT datistemplate ORDER BY 1"
)
DASHBOARDS_SQL = "SELECT id, name FROM report_dashboard WHERE NOT archived"


def psql_rows(text: str) -> list[list[str]]:
    """Rows of psql -At -F'|' output."""
    return [line.split("|") for line in text.splitlines() if line.strip()]


def number(value: str) -> int | None:
    return int(value) if value else None


def parse_tables(text: str) -> list[dict]:
    return [
        {
            "name": f"{db}.{table}",
            "type": "view" if kind == "VIRTUAL_VIEW" else "table",
            "rows": number(rows),
            "files": number(files),
            "bytes": number(size),
            "committed_ms": number(committed),
        }
        for db, table, kind, rows, files, size, committed in psql_rows(text)
    ]


def parse_databases(text: str) -> list[dict]:
    return [
        {"name": name, "bytes": int(size), "holds": DATABASES.get(name, "")}
        for name, size in psql_rows(text)
    ]


def dashboard_links(text: str) -> list[dict]:
    """Metabase links of the project dashboards, in their declared order, ids read live."""
    ids = {name: dashboard_id for dashboard_id, name in psql_rows(text)}
    return [
        {
            "label": d.name,
            "url": f"{METABASE}/dashboard/{ids[d.name]}#refresh={REFRESH_SECONDS[d.name]}",
        }
        for d in DASHBOARDS
        if d.name in ids
    ]


def silver_complete(tables: list[dict], csv_rows: int) -> bool:
    # Then a replay only sends trans_num values Silver holds: MERGE skips them, Live stays empty.
    silver = next((t["rows"] for t in tables if t["name"] == "silver.transactions"), None)
    return bool(csv_rows) and silver is not None and silver >= csv_rows
