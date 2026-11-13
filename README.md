# fraud-lakehouse

Real-time fraud detection lakehouse that runs on one laptop with Docker Compose. A producer replays a
Kaggle credit-card dataset into Kafka, Spark Structured Streaming writes Iceberg tables on MinIO, and dbt
builds Gold tables for dashboards and data science. Built as a learning project, one step at a time.

## Run

Requires [uv](https://docs.astral.sh/uv/) and GNU Make.

```
uv sync
make help
```

The pipeline services arrive in later build steps.

## How it works

See [docs/architecture.md](docs/architecture.md): data flow, components, tables and build plan.

## Tests

`make lint test` runs ruff and pytest. CI (`.github/workflows/ci.yml`) runs the same on every push and pull request.

## Limits

Build step 0 (of steps 0–9): repository skeleton only, no services yet.
