"""Serves the demo page on 127.0.0.1; its buttons run fixed docker compose and make commands."""

import io
import json
import logging
import os
import subprocess
import tarfile
import threading
import time
import urllib.request
from collections import deque
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from fraud_lakehouse import demo

ROOT = Path(__file__).resolve().parent.parent
PROJECT = "fraud-lakehouse"
PORT = int(os.environ.get("DEMO_PORT", "8099"))
STREAMING_METRICS = "http://localhost:4040/metrics/prometheus/"
PRODUCER_METRICS = "http://127.0.0.1:8000/metrics"

log = logging.getLogger("demo")


def run(args: list[str], timeout: float = 30) -> str:
    result = subprocess.run(args, cwd=ROOT, capture_output=True, text=True, timeout=timeout)
    return result.stdout if result.returncode == 0 else ""


def fetch(url: str) -> str:
    try:
        with urllib.request.urlopen(url, timeout=1.5) as response:
            return response.read().decode()
    except OSError:
        return ""


def count_rows(path: Path) -> int:
    # Before make up-all has built the CSV, the page still works and shows no total.
    if not path.exists():
        return 0
    with path.open("rb") as f:
        return sum(1 for _ in f) - 1


class Live:
    """Last known state of the stack, refreshed by background threads."""

    def __init__(self, total_rows: int) -> None:
        self.lock = threading.Lock()
        self.total_rows = total_rows
        self.ps: dict = {}
        self.stats: dict = {}
        self.rates: dict = {}
        self.position: int | None = None
        self.finished = False
        self.producer_rate = 0.0
        self.last_sent: tuple[float, float] | None = None
        self.tables: list[dict] = []
        self.databases: list[dict] = []
        self.dashboards: list[dict] = []
        self.job: dict = {"running": False, "log": []}
        self.job_log: deque[str] = deque(maxlen=200)

    def snapshot(self) -> dict:
        with self.lock:
            producer_state = self.ps.get("producer", {}).get("state", "absent")
            known = self.position is not None and self.total_rows > 0
            progress = (
                demo.progress(self.position, self.total_rows, self.producer_rate) if known else {}
            )
            return {
                "services": self.ps,
                "stats": self.stats,
                "streaming": self.rates,
                "producer": {
                    "state": producer_state,
                    "finished": self.finished,
                    "rate": round(self.producer_rate, 1),
                    "known": known,
                    "silver_complete": demo.silver_complete(self.tables, self.total_rows),
                    **progress,
                },
                "tables": self.tables,
                "databases": self.databases,
                "dashboards": self.dashboards,
                "job": {**self.job, "log": list(self.job_log)[-12:]},
            }

    def poll_fast(self) -> None:
        while True:
            try:
                self.refresh_fast()
            except (OSError, ValueError, subprocess.SubprocessError, tarfile.TarError) as error:
                log.warning("refresh failed, retrying: %s", error)
            time.sleep(2)

    def refresh_fast(self) -> None:
        ps = demo.parse_ps(
            run(["docker", "compose", "--profile", "*", "ps", "-a", "--format", "json"])
        )
        rates = demo.streaming_rates(demo.parse_metrics(fetch(STREAMING_METRICS)))
        producer = demo.parse_metrics(fetch(PRODUCER_METRICS))
        saved = None
        if "producer_position" not in producer and "producer" in ps and not self.job["running"]:
            saved = read_saved_position()
        with self.lock:
            self.ps = ps
            self.rates = rates
            if "producer" not in ps:
                # No container: after Reset or before the first start, its position is unknown here.
                self.position = None
            self.read_producer(producer, saved)

    def read_producer(self, metrics: dict[str, float], saved: int | None) -> None:
        now = time.monotonic()
        if "producer_position" in metrics:
            self.position = int(metrics["producer_position"])
            self.finished = metrics.get("producer_finished") == 1
            sent = metrics.get("producer_rows_sent_total", 0.0)
            if self.last_sent and sent >= self.last_sent[1] and now > self.last_sent[0]:
                self.producer_rate = (sent - self.last_sent[1]) / (now - self.last_sent[0])
            self.last_sent = (now, sent)
            return
        self.producer_rate = 0.0
        self.last_sent = None
        if saved is not None:
            self.position = saved
            self.finished = saved >= self.total_rows

    def poll_stats(self) -> None:
        while True:
            try:
                out = run(["docker", "stats", "--no-stream", "--format", "{{json .}}"])
                stats = demo.parse_stats(out, PROJECT)
                with self.lock:
                    self.stats = stats
            except (OSError, ValueError, subprocess.SubprocessError) as error:
                log.warning("stats refresh failed, retrying: %s", error)
            time.sleep(4)

    def poll_postgres(self) -> None:
        while True:
            try:
                tables = demo.parse_tables(psql("metastore", demo.TABLES_SQL))
                databases = demo.parse_databases(psql("postgres", demo.DATABASES_SQL))
                dashboards = demo.dashboard_links(psql("metabase", demo.DASHBOARDS_SQL))
                with self.lock:
                    self.tables, self.databases, self.dashboards = tables, databases, dashboards
            except (OSError, ValueError, subprocess.SubprocessError) as error:
                log.warning("postgres refresh failed, retrying: %s", error)
            time.sleep(15)

    def start_job(self, action: str, key: str | None) -> bool:
        commands = demo.commands(action, key)
        with self.lock:
            if self.job["running"]:
                return False
            self.job = {
                "running": True,
                "action": action,
                "key": key,
                "ok": None,
                "started": time.time(),
            }
            self.job_log.clear()
        threading.Thread(target=self.run_job, args=(commands,), daemon=True).start()
        return True

    def run_job(self, commands: list[list[str]]) -> None:
        ok = False
        try:
            ok = all(self.run_command(args) for args in commands)
        except (OSError, ValueError) as error:
            self.job_log.append(f"failed to run: {error}")
        finally:
            # Always released: a stuck job would lock the page until the server restarts.
            log.info("%s finished, ok=%s", self.job.get("action"), ok)
            with self.lock:
                self.job = {**self.job, "running": False, "ok": ok, "finished": time.time()}

    def run_command(self, args: list[str]) -> bool:
        self.job_log.append("$ " + " ".join(args))
        # Own session: Ctrl+C on make demo closes the page without killing a half-done action.
        process = subprocess.Popen(
            args,
            cwd=ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            errors="replace",
            start_new_session=True,
        )
        for line in process.stdout:
            self.job_log.append(line.rstrip())
        if process.wait() != 0:
            self.job_log.append(f"exit code {process.returncode}")
            return False
        return True


def psql(database: str, query: str) -> str:
    """Read-only query through the container's socket: no password leaves .env."""
    args = ["docker", "compose", "exec", "-T", "postgres", "psql", "-U", "postgres"]
    return run([*args, "-d", database, "-At", "-F|", "-c", query])


def read_saved_position() -> int | None:
    """Reads the producer's position file from its container, even stopped."""
    container = f"{PROJECT}-producer-1"
    result = subprocess.run(
        ["docker", "cp", f"{container}:/state/position", "-"], capture_output=True, timeout=10
    )
    if result.returncode != 0:
        return None
    with tarfile.open(fileobj=io.BytesIO(result.stdout)) as tar:
        member = tar.extractfile("position")
        return int(member.read()) if member else None


class Handler(SimpleHTTPRequestHandler):
    live: Live

    def log_message(self, fmt: str, *args) -> None:
        pass

    def end_headers(self) -> None:
        # Without it, browsers keep an old app.js after the page changes.
        if not self.path.startswith("/api/"):
            self.send_header("Cache-Control", "no-cache")
        super().end_headers()

    def allowed_host(self) -> bool:
        # Blocks DNS rebinding: a remote page resolving its own name to 127.0.0.1.
        return self.headers.get("Host") in (f"localhost:{PORT}", f"127.0.0.1:{PORT}")

    def send_json(self, status: int, body: object) -> None:
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:
        if not self.allowed_host():
            self.send_json(403, {"error": "forbidden host"})
        elif self.path == "/api/catalog":
            self.send_json(200, demo.catalog())
        elif self.path == "/api/state":
            self.send_json(200, self.live.snapshot())
        else:
            super().do_GET()

    def do_POST(self) -> None:
        # A custom header forces a CORS preflight, never answered here: other sites cannot post.
        if not self.allowed_host() or self.headers.get("X-Demo") != "1":
            self.send_json(403, {"error": "forbidden"})
            return
        if self.path != "/api/action":
            self.send_json(404, {"error": "not found"})
            return
        try:
            body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", "0"))))
            started = self.live.start_job(str(body["action"]), body.get("key"))
        except (ValueError, KeyError, TypeError, AttributeError) as error:
            self.send_json(400, {"error": str(error)})
            return
        if started:
            self.send_json(202, {"started": True})
        else:
            self.send_json(409, {"error": "another action is running"})


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    live = Live(count_rows(ROOT / "data" / "fraud_all.csv"))
    threading.Thread(target=live.poll_fast, daemon=True).start()
    threading.Thread(target=live.poll_stats, daemon=True).start()
    threading.Thread(target=live.poll_postgres, daemon=True).start()
    Handler.live = live
    handler = partial(Handler, directory=str(ROOT / "demo"))
    server = ThreadingHTTPServer(("127.0.0.1", PORT), handler)
    log.info("demo page on http://localhost:%d (Ctrl+C to stop the page, not the stack)", PORT)
    server.serve_forever()


if __name__ == "__main__":
    main()
