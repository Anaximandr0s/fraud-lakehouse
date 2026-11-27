# Contributing

Thanks for taking the time to contribute. Bug reports, fixes, documentation and new features are all welcome.

## Before you start

- **Bugs**: open an [issue](https://github.com/Anaximandr0s/fraud-lakehouse/issues/new/choose) with the command
  you ran, what you expected and the output (`make logs s=<service>` helps).
- **Features and larger changes**: open an issue first to agree on the approach. The stack runs on one 16 GB
  laptop, so any new service has to fit the [memory budget](docs/architecture.md#memory-budget).
- **Security issues**: do not open a public issue; see [SECURITY.md](SECURITY.md).

## Development setup

Requirements: Docker with Compose 2.24+, [uv](https://docs.astral.sh/uv/), GNU Make, Java 17 (for the PySpark
unit tests), and about 14 GB of free RAM to run every service.

```bash
git clone https://github.com/Anaximandr0s/fraud-lakehouse.git && cd fraud-lakehouse
uv sync                      # Python 3.10 with the dev and dbt dependencies
uv run pre-commit install    # ruff on every commit
make test                    # unit tests: no Docker, no network
```

To run the stack: `cp .env.example .env`, then `make up-core` for the pipeline alone or `make up-all` for
everything. `make help` lists every target.

## Where code goes

| Change | Location | Tested by |
|---|---|---|
| Transformation or business logic | `src/fraud_lakehouse/`, as a pure function | a unit test in `tests/` |
| Spark job, producer, script | `spark/jobs/`, `producer/`, `scripts/`: thin entrypoints that call the package | the unit tests of what they call |
| Gold model | `dbt/models/gold/` | `not_null` / `unique` tests in `schema.yml` |
| Dashboard card | `src/fraud_lakehouse/dashboards.py`, then `make metabase-setup` | `tests/test_dashboards.py` |
| Service | `compose.yaml`, with its image version in `.env.example` | `docker compose config`, `make recovery-check` |
| Command | a Makefile target with a `##` description | – |

The conventions behind this layout:

- Business logic stays out of I/O, so it can be tested on a local SparkSession in seconds.
- Every image and jar version is pinned in `.env.example`; never `latest`.
- Every long-running service has a healthcheck, `restart: unless-stopped` and a measured memory limit.
- Every Spark application sets `spark.cores.max=2`, or it starves the others on the shared worker.
- Every job is safe to restart: no lost event, no duplicate in Silver.
- Configuration comes from `.env`; secrets never land in the repository.

## Checks

```bash
make lint                    # ruff lint and format check
make test                    # pytest with branch coverage; fails under 95 %
make dbt args="test"         # dbt data tests, with the stack running
make recovery-check          # if you changed the stack: stop, start, and nothing is lost
```

CI runs lint, tests, `dbt parse`, `docker compose config`, promtool, hadolint and actionlint on every pull request.

## Pull requests

1. Fork the repository and branch from `main`.
2. Keep the pull request focused on one change; split unrelated work.
3. Fill in the template: what changed, why, and the commands you ran with their results.
4. Update the README or `docs/architecture.md` when behaviour, commands or ports change.
5. CI must be green. The maintainer reviews every pull request; changes are squash-merged.

## License

By contributing, you agree that your contributions are licensed under the [MIT License](LICENSE).
