# 0009 — Demo page: a host process on 127.0.0.1, not a container

**Context.** The demo page starts, pauses and stops services, so something must run `docker compose`. In a container
that means mounting `/var/run/docker.sock`, which gives that container root on the laptop. The page must also not
let a stray click stop a service the pipeline depends on, nor run two actions at once.

**Decision.** `scripts/demo_server.py` (standard library) runs on the host with the user's `docker` and `make`, on
127.0.0.1:8099. Each button maps to a fixed command in `fraud_lakehouse.demo` (tested); anything else is refused.
Only services ingestion never waits on can be stopped: producer, Airflow (Gold pauses), Metabase, JupyterLab,
Prometheus, Grafana, cAdvisor. One action at a time. POSTs need an `X-Demo` header (another site cannot send it without a CORS preflight,
never answered) and a `localhost` Host header (blocks DNS rebinding). Live numbers come from Spark's and the
producer's own metrics endpoints, so the page works with monitoring stopped.

**Consequences.** One command, `make demo`, before the page exists; everything else is a button. No login: whoever
has a shell on the laptop can already run `docker`. Reset deletes every volume and asks to type `reset`.
