# fraud-lakehouse

Real-time fraud detection lakehouse that runs on one laptop with Docker Compose. A producer replays a
Kaggle credit-card dataset into Kafka, Spark Structured Streaming writes Iceberg tables on MinIO, and dbt
builds Gold tables for dashboards and data science. Built as a learning project, one step at a time.

## Run

Requires [uv](https://docs.astral.sh/uv/), GNU Make and Docker Compose.

```
cp .env.example .env   # then replace the change-me passwords
make up-core           # downloads jars and the Kaggle dataset, builds the images, starts the core services
make help
```

UIs: MinIO console http://localhost:9001 (login from `.env`), Spark master http://localhost:8080,
Spark Thrift Server http://localhost:4041, spark-streaming http://localhost:4040. `make beeline` opens a SQL shell on
the Thrift Server; `make silver-count` prints Bronze and Silver row counts.
Kafka listens on `localhost:9094` for host clients; the producer sends `RATE` events/s (`.env`, default 20) to the
`transactions` topic and resumes where it stopped. `make reset-producer` makes it replay the CSV from the start.

Postgres sets its role passwords only when its volume is first created: changing a password in `.env` afterwards
has no effect. Run `ALTER ROLE … PASSWORD …` in Postgres, or `docker compose down -v` if no data needs keeping.

If spark-streaming stays down longer than Kafka's 24 h retention, it fails on restart (`failOnDataLoss`) instead of
silently skipping the deleted events. To recover: `make reset-streaming` (deletes its checkpoint, so it restarts from
the oldest event still in Kafka), then `make reset-producer` and `make up-core` (replays the CSV; Silver ignores rows
it already has, Bronze keeps the duplicates).

## How it works

See [docs/architecture.md](docs/architecture.md): data flow, components, tables and build plan.

## Tests

`make lint test` runs ruff and pytest (Silver transforms on a local SparkSession; needs Java 17+). CI (`.github/workflows/ci.yml`) runs the same on every push and pull request,
validates `compose.yaml` and lints both Dockerfiles with hadolint.

## Limits

Build step 4 (of steps 0–9): ingestion into Kafka and streaming into Iceberg Bronze and Silver; no Gold tables yet.
Hive Metastore is pinned to 3.1.3, the newest version Spark 3.5 can talk to ([ADR 0001](docs/adr/0001-iceberg-catalog.md)).
