# fraud-lakehouse

Real-time fraud detection lakehouse that runs on one laptop with Docker Compose. A producer replays a
Kaggle credit-card dataset into Kafka, Spark Structured Streaming writes Iceberg tables on MinIO, and dbt
builds Gold tables for dashboards and data science. Built as a learning project, one step at a time.

## Run

Requires [uv](https://docs.astral.sh/uv/), GNU Make and Docker Compose.

```
cp .env.example .env   # then replace the change-me passwords
make up-core           # downloads pinned jars, starts MinIO, Postgres, Hive Metastore
make help
```

MinIO console: http://localhost:9001 (login from `.env`).

## How it works

See [docs/architecture.md](docs/architecture.md): data flow, components, tables and build plan.

## Tests

`make lint test` runs ruff and pytest. CI (`.github/workflows/ci.yml`) runs the same on every push and pull request,
and validates `compose.yaml`.

## Limits

Build step 1 (of steps 0–9): storage and catalog only. Hive Metastore is pinned to 3.1.3, the newest version
Spark 3.5 can talk to ([ADR 0001](docs/adr/0001-iceberg-catalog.md)).
