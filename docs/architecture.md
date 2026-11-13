# Architecture — fraud-lakehouse

Real-time fraud detection lakehouse that runs on one laptop (Ubuntu, 16 GB RAM, 6 cores) with Docker Compose.
A producer replays a Kaggle credit-card dataset into Kafka; Spark Structured Streaming writes Iceberg tables
(Bronze, Silver) on S3-compatible storage; dbt builds Gold tables through the Spark Thrift Server;
Metabase, Beeline and Jupyter read Gold. Airflow schedules dbt and maintenance. Prometheus and Grafana monitor.

## 1. Data flow

```
data/fraud_all.csv → producer → Kafka topic `transactions`
  → spark-streaming (micro-batch 30 s) → bronze.transactions_raw → silver.transactions (+ silver.transactions_rejected)
  → dbt via spark-thrift, every 10 min (Airflow) → gold.*
  → Metabase (dashboards) · Beeline (SQL) · Jupyter (PySpark)
Storage: Iceberg tables on MinIO (s3a://lakehouse/warehouse), catalog = Hive Metastore backed by PostgreSQL.
```

## 2. Components

| Service | Image | Role |
|---|---|---|
| kafka | `apache/kafka` (KRaft, 1 broker) | topic `transactions`, 3 partitions, 24 h retention, key = `cc_num` |
| producer | built from `producer/` | replays `fraud_all.csv` at `RATE` events/s (default 20); keeps original dates, adds `event_ts`; stores its position to resume after restart |
| minio | `pgsty/minio` | S3 storage, bucket `lakehouse` (upstream MinIO no longer publishes images) |
| postgres | `postgres` | one instance, 3 databases: `metastore`, `airflow`, `metabase` |
| hive-metastore | `apache/hive` (standalone metastore) | Iceberg catalog, port 9083 |
| spark-master / spark-worker | built from `spark/` (on `apache/spark` 3.5) | Spark Standalone; worker 6 cores, 3 GB for executors |
| spark-streaming | same Spark image | long-running job: Kafka → Bronze → Silver |
| spark-thrift | same Spark image | JDBC SQL endpoint on port 10000, FAIR scheduler |
| airflow | built from `airflow/` (on `apache/airflow` 3, LocalExecutor, dbt-spark inside) | DAGs `gold_refresh`, `lakehouse_maintenance` |
| metabase | `metabase/metabase` | dashboards over Gold (Spark SQL driver → spark-thrift) |
| jupyter | `jupyter/pyspark-notebook` or Spark image + JupyterLab | data science on Silver and Gold |
| prometheus, grafana, cadvisor, kafka-exporter | official images | metrics, dashboards, email alerts |

Every Spark application sets `spark.cores.max=2` (streaming, thrift, jupyter) so none starves the others.
Streaming and the producer are services (`restart: unless-stopped`), not Airflow tasks.

## 3. Dataset

Source: Kaggle `kartik2112/fraud-detection` (simulated with Sparkov). Downloaded and merged by `make data`:

```bash
curl -L -o data/raw/fraud-detection.zip \
  https://www.kaggle.com/api/v1/datasets/download/kartik2112/fraud-detection
```

- The zip contains `fraudTrain.csv` (1,296,675 rows, 2019-01-01 → 2020-06-21) and `fraudTest.csv` (555,719 rows, 2020-06-21 → 2020-12-31).
- `scripts/prepare_data.py`: drop the unnamed index column, concatenate, sort by `trans_date_trans_time`,
  check 23 columns, unique `trans_num`, row count = 1,852,394; write `data/fraud_all.csv` (9,651 frauds, 0.52 %).
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

### bronze.transactions_raw — append only, partitioned by `days(ingest_ts)`
`value STRING` (raw JSON), `kafka_topic`, `kafka_partition INT`, `kafka_offset BIGINT`, `kafka_ts TIMESTAMP`, `ingest_ts TIMESTAMP`.

### silver.transactions — one row per `trans_num`, partitioned by `months(trans_ts)`
Transformations, in order: parse JSON with a fixed schema → cast types → validate → `MERGE` on `trans_num` →
clean (strip `fraud_`, `card_hash = sha2(cc_num, 256)`, drop `cc_num, first, last, street, dob, unix_time`) →
enrich (`age`, `distance_km` haversine, `hour_of_day`, `day_of_week`).

Columns: `trans_num, trans_ts, event_ts, card_hash, merchant, category, amt, gender, city, state, zip, lat, long,
merch_lat, merch_long, city_pop, job, age, distance_km, hour_of_day, day_of_week, is_fraud, ingest_ts`.

Validation rules (failure → `silver.transactions_rejected(raw_value, reason, ingest_ts)`):
`trans_num` not null · `trans_ts` valid · `amt > 0` · `category` in the 14 known values ·
lat in [-90, 90], long in [-180, 180] · `is_fraud` in (0, 1).

### gold.* — built by dbt from `stg_transactions` (view over Silver)

| Model | Grain | Notes |
|---|---|---|
| `fraud_kpi_minute` | minute of `event_ts`, last 24 h | incremental merge; nb_transactions, nb_fraudes, taux_fraude_pct, montant_total, montant_fraude |
| `recent_frauds` | last 100 frauds by `event_ts` | |
| `fraud_by_category` | category | |
| `fraud_by_hour_of_day` | 0–23 | |
| `fraud_by_state` | state | + mean lat/long for the map |
| `fraud_by_amount_band` | <10, 10–50, 50–100, 100–500, 500–1000, >1000 $ | |
| `fraud_by_age_band` | 18–25, 26–40, 41–60, >60 | |
| `merchant_risk` | merchant | min 100 transactions, risk rank |
| `customer_risk_profile` | card_hash | |
| `ml_features` | transaction | features + `is_fraud` + `split` (`train` if `trans_ts < '2020-06-21'`, else `test`) |

`ml_features` card-level features use windows over the past only (`range between N preceding and 1 preceding`):
`card_tx_count_1h`, `card_tx_count_24h`, `card_amt_ratio`, `secs_since_last_tx`; plus `log_amt`, `is_night`, `log_city_pop`.

Every Gold model has `not_null` / `unique` tests in `schema.yml`. `dbt source freshness` fails if Silver has no new row for 5 min.

Data science writes only to the `sandbox` schema; reads `silver` and `gold`.

## 5. Orchestration

| DAG | Schedule | Tasks |
|---|---|---|
| `gold_refresh` | every 10 min | `dbt source freshness` → `dbt run` → `dbt test` |
| `lakehouse_maintenance` | daily 03:00 | per table: `rewrite_data_files` → `rewrite_manifests` → `expire_snapshots` (> 3 days) → `remove_orphan_files` |

Both DAGs only send SQL to spark-thrift; Airflow does not install Spark. Failures email `ALERT_EMAIL`.

## 6. Serving

Metabase, 3 dashboards (all on Gold):
1. **Live** (refresh 1 min): last-hour transactions, frauds, fraud rate, fraud amount; per-minute line; recent frauds table.
2. **Fraud patterns** (refresh 10 min): rate by category, hour, state (map), amount band, age band.
3. **Risk**: top merchants, top cards, number of cards hit.
Metabase alert: last-hour fraud rate > 1 % → email.

## 7. Monitoring

Prometheus scrapes every 15 s: cAdvisor (containers), kafka-exporter (topic offsets), Spark native Prometheus
endpoint + `spark.sql.streaming.metricsEnabled`, MinIO `/minio/v2/metrics/cluster`.
Spark does not commit offsets to Kafka: lag = Spark processed rate vs input rate, plus table freshness.

Grafana alerts (email): target down 1 min · processed < input rate 5 min · batch > 30 s for 5 min ·
no Kafka input 5 min · container RAM > 90 % of limit 5 min · MinIO free disk < 10 %.
Grafana dashboards and alerts are provisioned from files.

SMTP settings (Grafana, Metabase, Airflow) come from `.env`: `SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASSWORD, ALERT_EMAIL`.

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
Other profiles: `orchestration` (Airflow 1.5 G) · `bi` (Metabase 1 G) · `notebook` (Jupyter 768 M) ·
`monitoring` (≈ 0.9 G). All profiles ≈ 12 GB of limits.

## 10. Build plan

One step = one branch = one PR. A step is done when its check passes and CI is green.

| # | Step | Check |
|---|---|---|
| 0 | Repo skeleton: README, `.gitignore`, `.env.example`, Makefile, `pyproject.toml` (uv, ruff, pytest), pre-commit, CI, LICENSE | `make lint test` passes locally; CI green |
| 1 | Storage: MinIO + bucket init, Postgres + 3 DBs, Hive Metastore | bucket visible in console; metastore answers on 9083 |
| 2 | Spark + Iceberg: master, worker, thrift | Beeline: create Iceberg table, insert, select; files visible in MinIO |
| 3 | Data + ingestion: `make data`, Kafka, producer | `fraud_all.csv` = 1,852,394 rows; JSON events visible with `kafka-console-consumer` |
| 4 | Streaming: Bronze + Silver | `count(*)` grows every 30 s; after container restart, no duplicates in Silver |
| 5 | dbt + Airflow: Gold | both DAGs green; Gold tables filled; `dbt test` passes |
| 6 | Metabase | 3 dashboards update on their own |
| 7 | Monitoring + alerts | stopping the producer sends an email alert within 6 min |
| 8 | Data science notebooks | notebook reads `gold.ml_features`, trains a model, prints PR-AUC, precision, recall on the test split |
| 9 | Hardening: healthchecks, restart policies, `make demo`, final README | `docker compose down && up`: everything recovers, no data loss |

## 11. Decisions

- Iceberg over plain Parquet: atomic commits on S3, `MERGE` for dedup, compaction, time travel.
- Hive Metastore as catalog: shared by streaming, thrift and Jupyter; Metabase sees tables in the default catalog.
- Spark Standalone instead of YARN; Spark Thrift Server instead of HiveServer2.
- dbt for Gold: SQL models, ordering via `ref()`, tests; runs inside the Airflow image, no extra container.
- No MLflow, no Schema Registry, no Kafka Connect, no Alertmanager, no Celery: not needed at this scale.

## 12. Out of scope

Multi-node deployment, per-user access control (would be Apache Ranger), real-time model scoring in the stream,
cloud deployment.