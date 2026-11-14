# 0006 — Metabase dashboards as code, through the REST API

**Context.** The three dashboards of §6 must be reproducible from the repository. Metabase open source has no
export to files (serialization is a paid feature), and dashboards built by hand in the UI live only in its
Postgres database.

**Decision.** `src/fraud_lakehouse/dashboards.py` declares the dashboards as data: native SQL, chart type and
grid position per card. Pure functions turn them into API payloads (unit-tested: no overlap, the right schema per
dashboard). `scripts/metabase_setup.py` creates the admin from `.env`, one connection per schema (the Spark SQL
driver syncs only its connection's database; no column-value scans), then creates or updates every card and
dashboard, found by name. Rejected: building in the UI and dumping Metabase's Postgres database, which is opaque
and tied to one Metabase version.

**Consequences.** `make metabase-setup` rebuilds the dashboards on a fresh volume and is safe to rerun. Changes
made in the UI are overwritten on the next run: edit `dashboards.py` instead. Auto-refresh is a URL fragment
(`#refresh=60`), not a stored setting. Metabase logs `Method not supported` when it saves a native card: the Hive
JDBC driver cannot describe a query before running it; the cards run normally.
