# Contributing

Bug reports, fixes and improvements are welcome. Open an issue first for anything larger than a small fix, so
we can agree on the approach before you spend time on it.

## Set up

You need Docker with Compose v2, [uv](https://docs.astral.sh/uv/), Java 17 (for the PySpark unit tests) and
about 12 GB of free RAM to run the full stack.

```bash
uv sync                    # Python 3.10 environment with the dev and dbt tools
uv run pre-commit install  # ruff on every commit
make test                  # unit tests, no Docker needed
```

## Where things go

- Logic goes in `src/fraud_lakehouse/` as pure functions (DataFrame in, DataFrame out; text in, data out),
  with unit tests in `tests/`. Spark jobs, the producer and scripts stay thin entrypoints around them.
- Every command is a Makefile target. Every image and jar version is pinned in `.env.example`.
- Every long-running service needs a healthcheck, `restart: unless-stopped` and a memory limit that keeps the
  whole stack inside the memory budget in [docs/architecture.md](docs/architecture.md).
- Jobs must be safe to restart: no lost events, no duplicates in Silver.

## Before you open a pull request

```bash
make lint test             # ruff, then the pytest unit tests
make recovery-check        # only if you changed the stack: down, up, and Silver loses nothing
```

## Commits and pull requests

- Branch from `main`: `feat/short-description`, `fix/short-description`.
- [Conventional Commits](https://www.conventionalcommits.org/): `type(scope): subject`, imperative and
  lowercase, for example `fix(spark): stop the thrift server cleanly on sigterm`.
  Types: `feat` `fix` `refactor` `test` `docs` `ci` `build` `chore` `perf`.
  Scopes: `producer` `spark` `dbt` `airflow` `compose` `monitoring` `ci` `docs`.
- One logical change per commit. The PR title follows the same format; CI checks it.
- Never commit `.env` or anything under `data/`.

## Releases

Versions follow [SemVer](https://semver.org/). To release, move the `Unreleased` notes in
[CHANGELOG.md](CHANGELOG.md) under a new version heading, merge, then push a `vX.Y.Z` tag: the release workflow
reruns CI and publishes a GitHub release with those notes.
