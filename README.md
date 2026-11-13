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
Spark Thrift Server http://localhost:4041. `make beeline` opens a SQL shell on the Thrift Server.
Kafka listens on `localhost:9094` for host clients; the producer sends `RATE` events/s (`.env`, default 20) to the
`transactions` topic and resumes where it stopped. `make reset-producer` makes it replay the CSV from the start.

Postgres sets its role passwords only when its volume is first created: changing a password in `.env` afterwards
has no effect. Run `ALTER ROLE … PASSWORD …` in Postgres, or `docker compose down -v` if no data needs keeping.

## How it works

See [docs/architecture.md](docs/architecture.md): data flow, components, tables and build plan.

## Tests

`make lint test` runs ruff and pytest. CI (`.github/workflows/ci.yml`) runs the same on every push and pull request,
validates `compose.yaml` and lints both Dockerfiles with hadolint.

## Limits

Build step 3 (of steps 0–9): storage, catalog, Spark, and ingestion into Kafka; nothing consumes the topic yet.
Until step 4, stop the producer once events show up (`docker compose stop producer`): Kafka keeps 24 h, so events
sent with no consumer are deleted unread, and the producer's position moves past them.
Hive Metastore is pinned to 3.1.3, the newest version Spark 3.5 can talk to ([ADR 0001](docs/adr/0001-iceberg-catalog.md)).
