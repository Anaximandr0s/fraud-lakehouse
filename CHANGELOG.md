# Changelog

Notable changes to this project. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and versions follow [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [1.0.0] - 2026-11-27

First public release: the whole pipeline runs on one laptop with `make up-all`.

### Pipeline

- CSV replay producer that resumes from its saved position after a restart, at least once, keyed by card.
- Kafka 4.3 in KRaft mode, topic `transactions` with 3 partitions.
- Spark 3.5 Structured Streaming into Iceberg: Bronze (raw), Silver (typed, validated, deduplicated with `MERGE`
  on `trans_num`), and a rejected table with a reason for every invalid row.
- dbt models for Gold: fraud by category, hour, state, amount and age band; merchant and customer risk; ML
  features with past-only card windows. Data tests on every model.
- Airflow 3.3: Gold refresh every 10 min and nightly Iceberg maintenance, with failure emails.
- Iceberg catalog in Hive Metastore 3.1.3 on PostgreSQL; data on MinIO.

### Serving and monitoring

- Three Metabase dashboards (Live, Fraud patterns, Risk), created by code through the REST API.
- SQL through the Spark Thrift Server and Beeline.
- A fraud model notebook trained on the cluster with Spark MLlib on a time-based split: gradient-boosted trees
  reach a PR-AUC of 0.88 on the test period, against 0.19 for logistic regression and 0.004 for a random model.
- Prometheus, Grafana and cAdvisor: a pipeline dashboard and six email alerts.
- A control center (`make control-center`) that draws the pipeline with live state and starts, stops and pauses
  services.

### Reliability

- Healthchecks, restart policies and measured memory limits on every long-running service.
- `make recovery-check`: stop and start the whole stack, then prove no Silver row is lost, no Kafka offset is
  skipped and no duplicate appears.
- The Thrift Server stops cleanly on SIGTERM instead of being killed after 10 s.

### Quality

- Unit tests on a local SparkSession with branch coverage of `src/fraud_lakehouse`, enforced at 95 % or more.
- CI: ruff, pytest with coverage, `dbt parse`, `docker compose config`, promtool, hadolint, actionlint and a
  check on PR titles. A release workflow publishes a GitHub release from each `vX.Y.Z` tag.
- Dependabot updates for GitHub Actions and Python dependencies, kept on Spark 3.5.

[Unreleased]: https://github.com/Anaximandr0s/fraud-lakehouse/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/Anaximandr0s/fraud-lakehouse/releases/tag/v1.0.0
