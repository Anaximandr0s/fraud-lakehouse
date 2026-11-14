"""Creates or updates the Metabase admin, the Lakehouse connections and the dashboards.

Idempotent: every object is found by name and updated in place, so it is safe to rerun.
With the argument `check`, it changes nothing: it runs every dashboard card and prints its result.
Settings come from the environment: METABASE_URL, METABASE_ADMIN_EMAIL, METABASE_ADMIN_PASSWORD,
SPARK_THRIFT_HOST, SPARK_THRIFT_PORT.
"""

import json
import logging
import os
import sys
import time
import urllib.error
import urllib.request
from typing import Any

from fraud_lakehouse.dashboards import (
    DASHBOARDS,
    REFRESH_SECONDS,
    card_payload,
    dashcards_payload,
    database_payload,
)

log = logging.getLogger("metabase_setup")

COLLECTION = "fraud-lakehouse"
HEALTH_TIMEOUT_SECONDS = 180
DATABASE_ATTEMPTS = 6


class MetabaseError(Exception):
    pass


class Metabase:
    def __init__(self, url: str) -> None:
        self.url = url.rstrip("/")
        self.session: str | None = None

    def call(self, method: str, path: str, body: Any = None) -> Any:
        request = urllib.request.Request(
            f"{self.url}/api/{path}",
            method=method,
            data=None if body is None else json.dumps(body).encode(),
            headers={"Content-Type": "application/json"},
        )
        if self.session:
            request.add_header("X-Metabase-Session", self.session)
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                raw = response.read()
        except urllib.error.HTTPError as e:
            detail = e.read().decode()[:500]
            raise MetabaseError(f"{method} /api/{path} failed with {e.code}: {detail}") from e
        return json.loads(raw) if raw else None

    def upsert(self, kind: str, existing: dict[str, int], payload: dict[str, Any]) -> int:
        name = payload["name"]
        if name in existing:
            self.call("PUT", f"{kind}/{existing[name]}", payload)
            return existing[name]
        return self.call("POST", kind, payload)["id"]


def wait_until_healthy(mb: Metabase) -> None:
    deadline = time.monotonic() + HEALTH_TIMEOUT_SECONDS
    while True:
        try:
            if mb.call("GET", "health").get("status") == "ok":
                return
        except (OSError, MetabaseError):
            pass
        if time.monotonic() > deadline:
            raise MetabaseError(f"Metabase not reachable at {mb.url}, run make up-bi")
        time.sleep(5)


def log_in(mb: Metabase, email: str, password: str) -> None:
    properties = mb.call("GET", "session/properties")
    if not properties["has-user-setup"]:
        mb.call(
            "POST",
            "setup",
            {
                "token": properties["setup-token"],
                "user": {
                    "email": email,
                    "password": password,
                    "first_name": "Admin",
                    "last_name": "Lakehouse",
                    "site_name": COLLECTION,
                },
                "prefs": {"site_name": COLLECTION, "site_locale": "en", "allow_tracking": False},
            },
        )
        log.info("created the admin account")
    mb.session = mb.call("POST", "session", {"username": email, "password": password})["id"]


def by_name(items: list[dict[str, Any]]) -> dict[str, int]:
    return {item["name"]: item["id"] for item in items if not item.get("archived")}


def upsert_database(mb: Metabase, databases: dict[str, int], payload: dict[str, Any]) -> int:
    # Metabase tests the connection with a 10 s timeout; a spark-thrift just restarted by a code
    # change can need longer for its first session.
    for attempt in range(1, DATABASE_ATTEMPTS + 1):
        try:
            return mb.upsert("database", databases, payload)
        except MetabaseError as e:
            if "Timed out" not in str(e) or attempt == DATABASE_ATTEMPTS:
                raise
            log.warning("spark-thrift not ready (attempt %d), retrying in 15 s", attempt)
            time.sleep(15)
    raise AssertionError("unreachable")


def setup(mb: Metabase) -> None:
    databases = by_name(mb.call("GET", "database")["data"])
    database_ids = {}
    for schema in sorted({d.schema for d in DASHBOARDS}):
        payload = database_payload(
            schema, os.environ["SPARK_THRIFT_HOST"], int(os.environ["SPARK_THRIFT_PORT"])
        )
        database_ids[schema] = upsert_database(mb, databases, payload)
        # Metadata only (tables and columns); column values are never scanned (is_full_sync false).
        mb.call("POST", f"database/{database_ids[schema]}/sync_schema")

    collections = by_name(mb.call("GET", "collection"))
    collection_id = mb.upsert("collection", collections, {"name": COLLECTION})
    items = mb.call("GET", f"collection/{collection_id}/items")["data"]
    cards = by_name([i for i in items if i["model"] == "card"])
    dashboards = by_name([i for i in items if i["model"] == "dashboard"])

    for dashboard in DASHBOARDS:
        database_id = database_ids[dashboard.schema]
        card_ids = {
            card.name: mb.upsert("card", cards, card_payload(card, database_id, collection_id))
            for card in dashboard.cards
        }
        dashboard_id = mb.upsert(
            "dashboard",
            dashboards,
            {
                "name": dashboard.name,
                "description": dashboard.description,
                "collection_id": collection_id,
            },
        )
        mb.call(
            "PUT",
            f"dashboard/{dashboard_id}",
            {"dashcards": dashcards_payload(dashboard, card_ids)},
        )
        refresh = REFRESH_SECONDS.get(dashboard.name)
        suffix = f"#refresh={refresh}" if refresh else ""
        log.info("%s: %s/dashboard/%d%s", dashboard.name, mb.url, dashboard_id, suffix)


def check(mb: Metabase) -> None:
    for database in mb.call("GET", "database")["data"]:
        flags = {k: database[k] for k in ("is_full_sync", "is_on_demand")}
        schemas = mb.call("GET", f"database/{database['id']}/schemas")
        log.info("database %s %s, synced schemas %s", database["name"], flags, schemas)
    collection_id = by_name(mb.call("GET", "collection"))[COLLECTION]
    items = mb.call("GET", f"collection/{collection_id}/items")["data"]
    cards = by_name([i for i in items if i["model"] == "card"])
    declared = [card.name for dashboard in DASHBOARDS for card in dashboard.cards]
    if undeclared := sorted(set(cards) - set(declared)):
        log.warning("cards not in dashboards.py (renamed or removed): %s", undeclared)
    failed = []
    for name in declared:
        if name not in cards:
            failed.append(name)
            log.error("%-45s missing, run make metabase-setup", name)
            continue
        result = mb.call("POST", f"card/{cards[name]}/query", {})
        rows = result.get("data", {}).get("rows") or [None]
        log.info("%-45s %s rows=%s first=%s", name, result["status"], result["row_count"], rows[0])
        if result["status"] != "completed":
            failed.append(name)
    if failed:
        raise MetabaseError(f"{len(failed)} card(s) failed: {failed}")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    mb = Metabase(os.environ["METABASE_URL"])
    try:
        wait_until_healthy(mb)
        log_in(mb, os.environ["METABASE_ADMIN_EMAIL"], os.environ["METABASE_ADMIN_PASSWORD"])
        check(mb) if sys.argv[1:] == ["check"] else setup(mb)
    except MetabaseError as e:
        sys.exit(str(e))
    except KeyError as e:
        sys.exit(f"{e.args[0]} is not set: add it to .env (see .env.example)")


if __name__ == "__main__":
    main()
