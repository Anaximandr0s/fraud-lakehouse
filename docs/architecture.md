# Architecture — fraud-lakehouse

Real-time fraud detection lakehouse that runs on one laptop (Ubuntu, 16 GB RAM, 6 cores) with Docker Compose.
A producer replays a Kaggle credit-card dataset into Kafka; Spark Structured Streaming writes Iceberg tables
(Bronze, Silver) on S3-compatible storage; dbt builds Gold tables through the Spark Thrift Server;
Metabase, Beeline and Jupyter read Silver and Gold. Airflow schedules dbt and maintenance. Prometheus and Grafana monitor.

## 1. Data flow

```
data/fraud_all.csv → producer → Kafka topic `transactions`
  → spark-streaming (micro-batch 30 s) → bronze.transactions_raw → silver.transactions (+ silver.transactions_rejected)
  → dbt via spark-thrift, every 10 min (Airflow) → gold.*
  → Metabase (Live dashboard on Silver, others on Gold) · Beeline (SQL) · Jupyter (PySpark)
Storage: Iceberg tables on MinIO (s3a://lakehouse/warehouse), catalog = Hive Metastore backed by PostgreSQL.
```

## 2. Components

| Service | Image | Role |
|---|---|---|
| kafka | `apache/kafka` (KRaft, 1 broker) | topic `transactions`, 3 partitions, 24 h retention, key = `cc_num` |
| producer | built from `producer/` | replays `fraud_all.csv` at `RATE` events/s (default 20; `RATE=500` on first run replays everything in ~1 h); keeps original dates, adds `event_ts`; stores its position to resume after restart; at the end of the file it logs and stays idle (exiting would loop under `restart: unless-stopped`) |
| minio | `pgsty/minio` | S3 storage, bucket `lakehouse` (upstream MinIO no longer publishes images) |
| postgres | `postgres` | one instance, 3 databases: `metastore`, `airflow`, `metabase` |
| hive-metastore | `apache/hive` 3.1.3 (metastore service) | Iceberg catalog, port 9083; pinned to 3.1.3, the newest metastore Spark 3.5's Hive client accepts (ADR 0001); the pinned Postgres JDBC driver replaces the image's old one; S3A uses the image's own `hadoop-aws` |
| spark-master / spark-worker | built from `spark/` (on `apache/spark` 3.5, Python 3.10) | Spark Standalone; worker 6 cores, `SPARK_WORKER_MEMORY=1920m` |
| spark-streaming | same Spark image | long-running job: Kafka → Bronze → Silver in one `foreachBatch` |
| spark-thrift | same Spark image | JDBC SQL endpoint on port 10000, FAIR scheduler |
| airflow | built from `apache/airflow` 3, `airflow standalone`, LocalExecutor | DAGs `gold_refresh`, `lakehouse_maintenance`; dbt-spark lives in its own venv (`/opt/dbt-venv`) and is called with `BashOperator`, so its dependencies never conflict with Airflow's |
| metabase | `metabase/metabase` | dashboards over Silver and Gold (Spark SQL driver → spark-thrift) |
| jupyter | Spark image + JupyterLab | data science on Silver and Gold; same Spark and Python as the cluster |
| prometheus, grafana, cadvisor | `prom/prometheus`, `grafana/grafana`, `gcr.io/cadvisor/cadvisor` | metrics, dashboards, email alerts |

Spark settings, all in `spark/conf/spark-defaults.conf`:
- Every Spark application sets `spark.cores.max=2` (streaming, thrift, jupyter) so none starves the others.
- `spark.executor.memory=640m` (+ 384 MB overhead per executor); `spark.driver.memory` is set explicitly per
  application (streaming 512m, thrift 640m, jupyter 512m) so the driver fits its container limit.
- `spark.sql.session.timeZone=UTC`.
- S3A jars match the Hadoop bundled with Spark 3.5: `hadoop-aws` 3.3.4 and the `aws-java-sdk-bundle` version it declares.
  Versions pinned in `.env.example`.

All Python code (shared package, Spark jobs, producer, scripts, notebooks) runs on Python 3.10, the version of the Spark image.
Streaming and the producer are services (`restart: unless-stopped`), not Airflow tasks.

## 3. Dataset

Source: Kaggle `kartik2112/fraud-detection` (simulated with Sparkov). Downloaded and merged by `make data`:

```bash
curl -fsSL -o data/raw/fraud-detection.zip \
  https://www.kaggle.com/api/v1/datasets/download/kartik2112/fraud-detection
```

- No authentication: checked in step 3, the public dataset downloads without a Kaggle token. `curl -f` fails
  loudly if Kaggle ever requires one.
- The zip contains `fraudTrain.csv` (1,296,675 rows, 2019-01-01 → 2020-06-21) and `fraudTest.csv` (555,719 rows, 2020-06-21 → 2020-12-31).
- `scripts/prepare_data.py` reads both CSVs inside the zip: check the 23-column raw header, drop the unnamed index
  column, sort `fraudTrain.csv` by `trans_date_trans_time` (1,858 rows are out of order; `fraudTest.csv` is sorted;
  about 2.2 GB peak memory, once), concatenate (train ends where test starts), check 22 columns per row, time order,
  unique `trans_num`, row count = 1,852,394; write `data/fraud_all.csv` (9,651 frauds, 0.52 %).
- The two files split during 2020-06-21. The first `trans_date_trans_time` of `fraudTest.csv` is stored as
  `TEST_SPLIT_TS` in `src/fraud_lakehouse/schemas.py`; `prepare_data.py` checks it.
- `make data` is idempotent: skips the download if the zip exists, skips the merge if the output is valid.
- `data/` is gitignored.

Columns: `trans_date_trans_time, cc_num, merchant, category, amt, first, last, gender, street, city, state, zip,
lat, long, city_pop, job, dob, trans_num, unix_time, merch_lat, merch_long, is_fraud`.

Quirks:
- `unix_time` is shifted by 7 years → ignored.
- Every `merchant` starts with `fraud_` (simulator artifact, not a label) → prefix stripped.
- `zip` loses leading zeros → stored as a 5-char string.
- `is_fraud` comes from the dataset; it is the model target, never a feature.

## 4. Tables

`trans_ts` (original transaction time) drives business logic: `age`, `hour_of_day`, `day_of_week`, `ml_features`
windows, the train/test split. `event_ts` (replay time) drives the Live dashboard.

### bronze.transactions_raw — append only, partitioned by `days(ingest_ts)`
`value STRING` (raw JSON), `kafka_topic`, `kafka_partition INT`, `kafka_offset BIGINT`, `kafka_ts TIMESTAMP`, `ingest_ts TIMESTAMP`.

One `foreachBatch` appends the batch to Bronze, then merges it into Silver. If a batch is replayed after a failure,
Bronze can hold duplicate rows (acceptable for raw data); Silver stays unique thanks to `MERGE`.

### silver.transactions — one row per `trans_num`, partitioned by `months(trans_ts)`
Transformations, in order: parse JSON with a fixed schema → cast types → validate →
clean (strip `fraud_`, `card_hash = sha2(concat(HASH_SALT, cc_num), 256)`, drop `cc_num, first, last, street, dob, unix_time`) →
enrich (`age` at `trans_ts`, `distance_km` haversine, `hour_of_day`, `day_of_week`) →
deduplicate on `trans_num` within the batch (`MERGE` fails if two source rows match one target row) → `MERGE` on `trans_num`.
`HASH_SALT` comes from `.env`. Table property `write.merge.mode=merge-on-read`; nightly compaction rewrites the delete files.

Columns: `trans_num, trans_ts, event_ts, card_hash, merchant, category, amt, gender, city, state, zip, lat, long,
merch_lat, merch_long, city_pop, job, age, distance_km, hour_of_day, day_of_week, is_fraud, ingest_ts`.

Validation rules (failure → `silver.transactions_rejected(raw_value, reason, ingest_ts)`):
`trans_num` not null · `trans_ts` valid · `amt > 0` · `category` in the 14 known values ·
lat in [-90, 90], long in [-180, 180] · `is_fraud` in (0, 1).

### gold.* — built by dbt from `stg_transactions` (view over Silver)

| Model | Grain | Notes |
|---|---|---|
| `fraud_by_category` | category | |
| `fraud_by_hour_of_day` | 0–23 | |
| `fraud_by_state` | state | + mean lat/long for the map |
| `fraud_by_amount_band` | <10, 10–50, 50–100, 100–500, 500–1000, >1000 $ | |
| `fraud_by_age_band` | 18–25, 26–40, 41–60, >60 | |
| `merchant_risk` | merchant | min 100 transactions, risk rank |
| `customer_risk_profile` | card_hash | |
| `ml_features` | transaction | features + `is_fraud` + `split` (`train` if `trans_ts < TEST_SPLIT_TS`, else `test`); tag `ml` |

`ml_features` card-level features use windows over the past only, ordered by `unix_timestamp(trans_ts)`
(`range between N preceding and 1 preceding`): `card_tx_count_1h`, `card_tx_count_24h`, `card_amt_ratio`,
`secs_since_last_tx`; plus `log_amt`, `is_night`, `log_city_pop`. It is excluded from `gold_refresh` and built on demand
with `make ml-features`.

Every Gold model has `not_null` / `unique` tests in `schema.yml`. `dbt source freshness` warns (does not fail)
if Silver has no new row for 5 min.

Data science writes only to the `sandbox` schema; reads `silver` and `gold`.

## 5. Orchestration

| DAG | Schedule | Tasks |
|---|---|---|
| `gold_refresh` | every 10 min | `dbt source freshness` (warn only) → `dbt run --exclude tag:ml` → `dbt test --exclude tag:ml` |
| `lakehouse_maintenance` | daily 03:00 | per table: `rewrite_data_files` → `rewrite_manifests` → `expire_snapshots` (> 3 days) → `remove_orphan_files` |

Both DAGs only send SQL to spark-thrift; Airflow does not install Spark. Failures email `ALERT_EMAIL`.
Compaction can conflict with a concurrent streaming `MERGE` commit: the streaming batch fails, the service restarts
and replays it from its checkpoint, with no loss and no duplicate. This is accepted.

## 6. Serving

Metabase, 3 dashboards:
1. **Live** (refresh 1 min, on Silver, last hour by `event_ts`): transactions, frauds, fraud rate, fraud amount;
   per-minute line; recent frauds table.
2. **Fraud patterns** (refresh 10 min, on Gold): rate by category, hour, state (map), amount band, age band.
3. **Risk** (on Gold): top merchants, top cards, number of cards hit.

Optional (step 6): Metabase alert on the last-hour fraud rate. Frauds cluster at night, so the threshold is set
after measuring the hourly fraud rate on the data, not before.

## 7. Monitoring

Prometheus scrapes every 15 s: cAdvisor (containers), Spark native Prometheus endpoint +
`spark.sql.streaming.metricsEnabled`, MinIO `/minio/v2/metrics/cluster`.
Spark does not commit offsets to Kafka, so Kafka lag is not measured: lag = Spark processed rate vs input rate,
plus table freshness.

Grafana alerts (email): target down 1 min · processed < input rate 5 min · batch > 30 s for 5 min ·
no streaming input (`inputRowsPerSecond` = 0) 5 min · container RAM > 90 % of limit 5 min · MinIO free disk < 10 %.
Grafana dashboards and alerts are provisioned from files.

Email paths: Grafana (infrastructure) and Airflow (DAG failures); Metabase only if its optional alert is enabled.
SMTP is a Gmail account with an app password. Settings come from `.env`:
`SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASSWORD, ALERT_EMAIL`.

## 8. Ports

| Service | Host |
|---|---|
| Kafka (external listener) | 9094 |
| MinIO API / console | 9000 / 9001 |
| Spark master UI / worker UI | 8080 / 8081 |
| spark-streaming UI / spark-thrift UI | 4040 / 4041 |
| spark-thrift JDBC | 10000 |
| Airflow | 8082 |
| Metabase | 3000 |
| Grafana | 3001 |
| Prometheus | 9090 |
| Jupyter | 8888 |

## 9. Resources (memory limits)

core profile ≈ 8 GB: spark-worker 3.5 G · spark-thrift 1 G · spark-streaming 768 M · kafka 768 M ·
hive-metastore 640 M · minio 512 M · spark-master 512 M · postgres 384 M · producer 128 M.
Other profiles: `orchestration` (Airflow 2 G) · `bi` (Metabase 1 G) · `notebook` (Jupyter 1 G) ·
`monitoring` (< 0.9 G). All profiles ≈ 13 GB of limits.

spark-worker: 3 executors × (640 MB + 384 MB overhead) ≈ 3 GB, plus the worker daemon, fits in 3.5 G.
Measured in step 2 (`docker stats`, idle right after a Beeline create/insert/select on a 2-row Iceberg table):
spark-worker 674 MiB · spark-thrift 555 MiB · spark-master 225 MiB · hive-metastore 341 MiB · postgres 96 MiB ·
minio 95 MiB. The worker figure holds only the thrift executor; streaming load is measured in step 4.

`make up-all` is for demos only. Daily development runs `core` + one profile.

## 10. Build plan

One step = one branch = one PR. A step is done when its check passes and CI is green.
Every service gets its healthcheck, restart policy and memory limit in the step that adds it.

| # | Step | Check |
|---|---|---|
| 0 | Repo skeleton: README, `.gitignore`, `.env.example`, Makefile, `pyproject.toml` (uv, ruff, pytest), pre-commit, CI, LICENSE | `make lint test` passes locally; CI green |
| 1 | Storage: MinIO + bucket init, Postgres + 3 DBs, Hive Metastore. Starts with a spike: from a throwaway Spark container, create a database with an `s3a://` location and an Iceberg table. If it does not work after 1 h of effort, switch to the Iceberg JDBC catalog on Postgres. Result recorded in an ADR | spike result in ADR; bucket visible in console; catalog answers |
| 2 | Spark + Iceberg: master, worker, thrift. Iceberg tables live in the default catalog (`spark_catalog` = `SparkSessionCatalog`, type `hive`, `spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083`), so dbt and Metabase see them without a catalog prefix | Beeline: create Iceberg table, insert, select; files visible in MinIO; container memory measured |
| 3 | Data + ingestion: `make data`, Kafka, producer | `fraud_all.csv` = 1,852,394 rows; JSON events visible with `kafka-console-consumer`; Kaggle auth need checked |
| 4 | Streaming: Bronze + Silver | `count(*)` grows every 30 s; after container restart, no duplicates in Silver |
| 5 | dbt + Airflow: Gold | both DAGs green; Gold tables filled; `dbt test` passes |
| 6 | Metabase | 3 dashboards update on their own; optional fraud-rate alert after measuring the hourly rate |
| 7 | Monitoring + alerts | stopping the producer sends an email alert within 6 min |
| 8 | Data science notebooks (needs the full replay: `RATE=500` on first run) | notebook reads `gold.ml_features`, trains a model, prints PR-AUC, precision, recall on the test split |
| 9 | Hardening: check healthchecks and restart policies, `make demo`, final README | `docker compose down && up`: everything recovers, no data loss |

## 11. Decisions

- Iceberg over plain Parquet: atomic commits on S3, `MERGE` for dedup, compaction, time travel.
- Hive Metastore as catalog: shared by streaming, thrift and Jupyter; Metabase sees tables in the default catalog.
  Spike passed on Hive 3.1.3 (ADR 0001); JDBC catalog fallback not needed.
- Spark Standalone instead of YARN; Spark Thrift Server instead of HiveServer2.
- Python 3.10 everywhere: one version, the one of the Spark image, so driver and workers always match.
- Streaming: one `foreachBatch` for Bronze and Silver instead of two queries; simpler, Bronze may hold replay duplicates (ADR).
- dbt for Gold: SQL models, ordering via `ref()`, tests; runs in its own venv inside the Airflow image, no extra container.
- `airflow standalone` in one container: enough for one laptop, not for production (ADR).
- No MLflow, no Schema Registry, no Kafka Connect, no Alertmanager, no Celery, no kafka-exporter: not needed at this scale.

## 12. Out of scope

Multi-node deployment, per-user access control (would be Apache Ranger), real-time model scoring in the stream,
cloud deployment.
