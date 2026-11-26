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
| hive-metastore | `apache/hive` 3.1.3 (metastore service) | Iceberg catalog, port 9083; pinned to 3.1.3, the newest metastore Spark 3.5's Hive client accepts; the pinned Postgres JDBC driver replaces the image's old one; S3A uses the image's own `hadoop-aws` |
| spark-master / spark-worker | built from `spark/` (on `apache/spark` 3.5, Python 3.10) | Spark Standalone; worker 6 cores, `SPARK_WORKER_MEMORY=1920m` |
| spark-streaming | same Spark image | long-running job: Kafka → Bronze → Silver in one `foreachBatch` |
| spark-thrift | same Spark image | JDBC SQL endpoint on port 10000, FAIR scheduler |
| airflow | built from `apache/airflow` 3, `airflow standalone`, LocalExecutor | DAGs `gold_refresh`, `lakehouse_maintenance`; dbt-spark lives in its own venv (`/opt/dbt-venv`) and is called with `BashOperator`, so its dependencies never conflict with Airflow's |
| metabase | `metabase/metabase` (LTS) | dashboards over Silver and Gold (Spark SQL driver → spark-thrift); app data in Postgres `metabase`; dashboards created by `make metabase-setup` |
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
enrich (`age` in full years at `trans_ts`, `distance_km` haversine, `hour_of_day`, `day_of_week` 1 = Sunday … 7) →
deduplicate on `trans_num` within the batch (`MERGE` fails if two source rows match one target row) → `MERGE` on `trans_num`.
`HASH_SALT` comes from `.env`. Table property `write.merge.mode=merge-on-read`; nightly compaction rewrites the delete files.

Columns: `trans_num, trans_ts, event_ts, card_hash, merchant, category, amt, gender, city, state, zip, lat, long,
merch_lat, merch_long, city_pop, job, age, distance_km, hour_of_day, day_of_week, is_fraud, ingest_ts`.

Validation rules, checked in this order; the first failure is the `reason` in
`silver.transactions_rejected(raw_value, reason, ingest_ts)`:
JSON parses (`malformed_json`) · `trans_num` not null (`trans_num_null`) · `trans_ts` valid (`trans_ts_invalid`) ·
`amt > 0` (`amt_not_positive`) · `category` in the 14 known values (`category_unknown`) ·
lat and merch_lat in [-90, 90], long and merch_long in [-180, 180] (`coordinates_out_of_range`) ·
`is_fraud` in (0, 1) (`is_fraud_invalid`).

### gold.* — built by dbt from `stg_transactions` (view over Silver)

Fraud metrics, shared by the aggregates (macro `fraud_metrics`): `tx_count`, `fraud_count`,
`fraud_rate` (a fraction 0–1; Metabase shows it as %), `fraud_amt` (sum of `amt` over frauds).

| Model | Grain | Columns besides the fraud metrics |
|---|---|---|
| `fraud_by_category` | category | |
| `fraud_by_hour_of_day` | `hour_of_day` 0–23 | |
| `fraud_by_state` | state | `lat`, `long` (means, for the map) |
| `fraud_by_amount_band` | `amount_band` <10, 10–50, 50–100, 100–500, 500–1000, >1000 $ | `band_order` 1–6 for sorting |
| `fraud_by_age_band` | `age_band` <18, 18–25, 26–40, 41–60, >60 (the data has cardholders from 13) | `band_order` 1–5 |
| `merchant_risk` | merchant, min 100 transactions | `risk_rank` (by `fraud_rate` desc, then `fraud_count` desc) |
| `customer_risk_profile` | card_hash | `total_amt`, `first_ts`, `last_ts` |
| `ml_features` | transaction | see below; tag `ml` |

`ml_features` columns: `trans_num, trans_ts, card_hash, category, gender, age, amt, log_amt, hour_of_day, day_of_week,
is_night` (22:00–05:59) `, distance_km, log_city_pop, card_tx_count_1h, card_tx_count_24h, card_amt_ratio`
(`amt` / card's mean `amt` over the previous 24 h, null without history) `, secs_since_last_tx` (null for a card's
first transaction) `, is_fraud, split` (`train` if `trans_ts < TEST_SPLIT_TS`, else `test`; the dbt var
`test_split_ts` must equal `TEST_SPLIT_TS`, checked by a unit test). Card-level features use windows over the past
only, ordered by `unix_timestamp(trans_ts)` (`range between N preceding and 1 preceding`). It is excluded from
`gold_refresh` and built on demand with `make ml-features`.

Every Gold model has `not_null` / `unique` tests in `schema.yml`. `dbt source freshness` warns (does not fail)
if Silver has no new row for 5 min.

Data science reads `silver` and `gold` and writes only to a `sandbox` schema, created the first time a notebook
needs to save something (the step 8 notebook only prints its results, so it does not exist yet).

## 5. Orchestration

| DAG | Schedule | Tasks |
|---|---|---|
| `gold_refresh` | every 10 min | `dbt source freshness` (warn only) → `dbt run --exclude tag:ml` → `dbt test --exclude tag:ml` |
| `lakehouse_maintenance` | daily 03:00 | per table: `rewrite_data_files` → `rewrite_manifests` → `expire_snapshots` (> 3 days) → `remove_orphan_files` |

Both DAGs only send SQL to spark-thrift; Airflow does not install Spark. Failures email `ALERT_EMAIL`.
Compaction can conflict with a concurrent streaming `MERGE` commit: the streaming batch fails, the service restarts
and replays it from its checkpoint, with no loss and no duplicate. This is accepted. The conflict can also fail the
maintenance side (seen in step 5: `rewrite_manifests` on Bronze lost to a streaming append), so maintenance tasks
retry twice after 1 min; the Iceberg procedures are safe to rerun.

## 6. Serving

Metabase, 3 dashboards:
1. **Live** (refresh 1 min, on Silver, last hour by `event_ts`): transactions, frauds, fraud rate, fraud amount;
   per-minute line; recent frauds table.
2. **Fraud patterns** (refresh 10 min, on Gold): rate by category, hour, state (map), amount band, age band.
3. **Risk** (refresh 10 min, on Gold): top merchants, top cards, number of cards hit.

The dashboards are code: `src/fraud_lakehouse/dashboards.py` declares them (SQL, chart type, grid position) and
`make metabase-setup` creates or updates them through the Metabase REST API, found by name, so it is safe to rerun
(Metabase open source cannot export dashboards to files). `make metabase-check` runs every card once.
Auto-refresh is not stored by Metabase: it is the `#refresh=` part of the URL, so Live is opened as
`/dashboard/<id>#refresh=60`, Fraud patterns and Risk with `#refresh=600` (the setup prints the links).

Two Metabase connections to spark-thrift, `Lakehouse silver` (Live) and `Lakehouse gold` (the others): the Spark SQL
driver syncs only the tables of its connection's database, and has no schema filter. Neither connection ever scans
column values (`is_full_sync` false), so Metabase sends no `SELECT DISTINCT` over Silver.

Live reads only the last hour of `event_ts`. Silver is written ordered by `event_ts` (`WRITE ORDERED BY`, hash
distribution, sort compaction), so Iceberg skips older files by their min/max: measured in step 6, the Live query
reads 3 of 32 data files and runs in about 0.2 s.

Alert on the last-hour fraud rate: not set. Measured in step 6 over the full dataset cut into 72,000-transaction
windows (one hour of replay at `RATE=20`): min 0.29 %, median 0.50 %, p95 0.84 %, max 0.96 %. One replay hour covers
about a day of data, so the night clustering averages out and no threshold separates a real anomaly from noise.

After the producer reaches the end of the CSV, Live stays flat: a replay (`make reset-producer`) sends `trans_num`
values Silver already holds, and the `MERGE` ignores them.

## 7. Monitoring

Prometheus scrapes every 15 s (`monitoring/prometheus.yml`, profile `monitoring`, http://localhost:9090):
- cAdvisor: memory and CPU per container. Queries filter on the Compose project label, so other projects on the
  same Docker host are ignored.
- spark-streaming: Spark's native endpoint `/metrics/prometheus` on the driver UI, with
  `spark.sql.streaming.metricsEnabled` and `spark.metrics.namespace=${spark.app.name}`, so metric names
  (`metrics_spark_streaming_driver_spark_streaming_transactions_inputRate_total_Value`, `…processingRate…`,
  `…latency…`) do not change when the app restarts.
- producer: `producer_rows_sent_total` and `producer_finished` (1 once the whole CSV is sent), port 8000.
- MinIO: `/minio/v2/metrics/cluster`, without a token (`MINIO_PROMETHEUS_AUTH_TYPE=public`).

Spark does not commit offsets to Kafka, so Kafka lag is not measured: lag = Spark processed rate vs input rate,
plus table freshness.

Grafana (http://localhost:3001, user `admin`) provisions everything from `monitoring/grafana/`: the Prometheus
data source, the Pipeline dashboard and six email alerts (rules evaluated every 30 s, first email 10 s after a rule
fires, repeated every 4 h while it lasts):

| Alert | Fires when | For |
|---|---|---|
| Target down | a scrape target does not answer | 1 min |
| Streaming falls behind | processed rows/s < input rows/s | 5 min |
| Streaming batch over 30 s | batch duration > the 30 s trigger | 5 min |
| No streaming input | input rate = 0 **and** the producer has not reported `producer_finished = 1` | 4 min |
| Container memory over 90 % of its limit | working set / limit (not usage, which counts page cache) | 5 min |
| MinIO free disk under 10 % | usable free / usable total | 5 min |

"No streaming input" waits 4 min: with Spark's next empty batch (≤ 30 s), the scrape and the evaluation, the email
leaves about 5 min after the input stops. It stays silent once the producer has replayed the whole CSV, and fires
if the producer is gone. Measured in step 7 with `docker compose stop producer`: Target down sent 1 min 28 s after
the stop, No streaming input 5 min 25 s after.
Spark's input rate is what it reads, capped by `MAX_OFFSETS_PER_TRIGGER` per 30 s (about 1,666 rows/s): a producer
faster than that would grow Kafka lag without firing "Streaming falls behind". At `RATE` 20 or 500 it cannot happen.

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
| Producer metrics (127.0.0.1 only) | 8000 |
| Control center (`make control-center`, a host process on 127.0.0.1) | 8099 |

## 9. Resources (memory limits)

core profile ≈ 8.9 GB: spark-worker 3.5 G · spark-thrift 1.25 G · spark-streaming 1.25 G · kafka 768 M ·
hive-metastore 640 M · minio 512 M · spark-master 512 M · postgres 384 M · producer 128 M.
Other profiles: `orchestration` (Airflow 2 G) · `bi` (Metabase 1.5 G) · `notebook` (Jupyter 1.5 G) ·
`monitoring` (1.1 G). All profiles ≈ 15 GB of limits; `make up-all` (all but `notebook`) ≈ 13.5 GB.

spark-worker: 3 executors × (640 MB + 384 MB overhead) ≈ 3 GB, plus the worker daemon, fits in 3.5 G.
Measured in step 2 (`docker stats`, idle right after a Beeline create/insert/select on a 2-row Iceberg table):
spark-worker 674 MiB · spark-thrift 555 MiB · spark-master 225 MiB · hive-metastore 341 MiB · postgres 96 MiB ·
minio 95 MiB. The worker figure holds only the thrift executor.

Measured in step 4, all core services running (`docker stats`):
- spark-streaming first had 768 M: its 512m-heap driver reached 742 MB of JVM RSS and the container hit its limit
  257 times at RATE=20. About 266 MB were glibc malloc arenas outside the JVM's own memory (Native Memory
  Tracking); `MALLOC_ARENA_MAX=2` brought that to 53 MB, and the limit went to 1 G.
- RATE=20, after 7 batches of 600 events: spark-worker 2.0 GiB · spark-streaming 685 MiB · spark-thrift 623 MiB ·
  kafka 539 MiB · hive-metastore 321 MiB · spark-master 295 MiB · minio 288 MiB · postgres 97 MiB · producer 18 MiB.
- RATE=500 for 10 min (batches of 15,000 events every 30 s), peak of 28 samples: spark-worker 1759 MiB ·
  spark-streaming 768 MiB · spark-thrift 550 MiB · kafka 527 MiB · hive-metastore 338 MiB · minio 293 MiB ·
  spark-master 269 MiB · postgres 106 MiB · producer 20 MiB. spark-streaming never reached its limit.

Measured in step 5, core + orchestration at RATE=20, 10 min covering `lakehouse_maintenance`, `make ml-features`
and two `gold_refresh` runs (peak of 27 samples): spark-worker 2665 MiB · airflow 1715 MiB · spark-thrift 995 MiB ·
spark-streaming 818 MiB · minio 420 MiB · spark-master 268 MiB · kafka 285 MiB · hive-metastore 253 MiB ·
postgres 152 MiB · producer 14 MiB; sum of peaks 7.6 GiB. spark-thrift first filled its 1 G limit under dbt load
(1018 MB JVM RSS for a 640m heap); `MALLOC_ARENA_MAX=2`, as for spark-streaming, was not enough (664 limit hits
in 23 min, 1044 MB RSS), so its limit went to 1.25 G in step 6.

`gold_refresh` duration: 26–28 s alone (freshness 6 s, run 12 s, test 8 s), 60 s while maintenance and
`ml_features` run, 72 s for the first run after Airflow starts (full dbt parse); the 8-min task timeout leaves margin.
`lakehouse_maintenance`: 67 s on a quiet stack; 4 min 12 s under load, including one retry of `maintain_gold`
(it read `gold.ml_features` while dbt was recreating it).

Measured in step 6, core + bi + orchestration, every dashboard card run once a minute for 10 min (as an open Live
dashboard would), producer idle at the end of the CSV: spark-worker 1458 MiB · airflow 879 MiB · spark-thrift
978 MiB (limit 1.25 G, never reached) · spark-streaming 491 MiB · minio 297 MiB · postgres 249 MiB · kafka 236 MiB.
Metabase with a 512m heap needs about 1.05 GB of process memory (metaspace and compiled code of a large Clojure
app): at 1 G it hit its limit 5707 times in 10 min; `MALLOC_ARENA_MAX=2` changed nothing (6097), so it was not
kept; at 1.5 G it settles at 1054 MiB, and its limit hits (927, near startup) stop under steady load.

Measured in step 7, monitoring profile (limits 864 MB): prometheus 117 MiB of 256m, cadvisor 34 MiB of 96m,
grafana at 512m: working set at most 61 % (310 MiB, 276 MiB anon). Grafana first had 256m (7831 limit hits), then
384m, where its working set (process plus active page cache) stayed above 90 % for 5 min after a restart and fired
its own memory alert.
spark-thrift working set before the monitoring profile started: 1065 MiB of 1.25 G (83 %).

Measured in step 8, `make notebook-run` (about 5 min of training on 1.04 M rows, streaming running at RATE=20):
jupyter (driver 512m, kernel, JupyterLab) hit its 1 G limit 101 times; with `MALLOC_ARENA_MAX=2` no hits but a 95 %
working set; at 1.5 G it peaks at 74 % (1205 MiB). spark-worker 2648 MiB of 3.5 G; streaming kept its 30 s batches.

Measured in step 9, every profile running for 8 h at RATE=20 (Prometheus, working set / limit):
- spark-streaming at 1 G: RSS 878 MiB after 1 h, then 960–998 MiB from the second hour on (512m heap fully
  committed, 179 MB metaspace, 356 threads, `MALLOC_ARENA_MAX=2` already set), above 90 % for 415 min, 137 limit
  hits; flat, not a leak, so the limit went to 1.25 G (61 % 10 min after a restart).
- grafana at 512m, after a stop and start from the control center: 243 MiB of process plus 200 MiB of its own files in
  active page cache, 89–92 % for 5 min; after a recreate 71–81 %. At 768m, after a stop and start: 65–75 % for
  6 min, then 53 %.
- `make recovery-check`: every service healthy 136 s after `down`. From the control center, Stop all took 29 s and
  Start all (`make up-all`, image builds from cache and Metabase setup included) 395 s.

`make up-all` runs every profile at once. Daily development runs `core` + one profile.

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
| 9 | Hardening: check healthchecks and restart policies, `make control-center`, final README | `docker compose down && up`: everything recovers, no data loss |

## 11. Decisions

- Iceberg over plain Parquet: atomic commits on S3, `MERGE` for dedup, compaction, time travel.
- Hive Metastore as catalog: shared by streaming, thrift and Jupyter; Metabase sees tables in the default catalog.
  Spike passed on Hive 3.1.3; JDBC catalog fallback not needed.
- Spark Standalone instead of YARN; Spark Thrift Server instead of HiveServer2.
- Python 3.10 everywhere: one version, the one of the Spark image, so driver and workers always match.
- Streaming: one `foreachBatch` for Bronze and Silver instead of two queries; simpler, Bronze may hold replay duplicates.
- dbt for Gold: SQL models, ordering via `ref()`, tests; runs in its own venv inside the Airflow image, no extra container.
- `airflow standalone` in one container: enough for one laptop, not for production.
- No MLflow, no Schema Registry, no Kafka Connect, no Alertmanager, no Celery, no kafka-exporter: not needed at this scale.

## 12. Out of scope

Multi-node deployment, per-user access control (would be Apache Ranger), real-time model scoring in the stream,
cloud deployment.
