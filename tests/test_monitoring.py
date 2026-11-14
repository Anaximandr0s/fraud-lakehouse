import json
from pathlib import Path

import yaml

MONITORING = Path(__file__).parents[1] / "monitoring"
PROVISIONING = MONITORING / "grafana" / "provisioning"


def load(path: Path):
    return yaml.safe_load(path.read_text())


def rules() -> dict[str, dict]:
    groups = load(PROVISIONING / "alerting" / "rules.yml")["groups"]
    return {rule["title"]: rule for group in groups for rule in group["rules"]}


def test_prometheus_scrapes_the_five_sources():
    jobs = {job["job_name"] for job in load(MONITORING / "prometheus.yml")["scrape_configs"]}

    assert jobs == {"prometheus", "cadvisor", "spark-streaming", "producer", "minio"}


def test_the_six_alerts_of_section_7_wait_their_agreed_duration():
    assert {title: rule["for"] for title, rule in rules().items()} == {
        "Target down": "1m",
        "Streaming falls behind": "5m",
        "Streaming batch over 30 s": "5m",
        "No streaming input": "4m",
        "Container memory over 90 % of its limit": "5m",
        "MinIO free disk under 10 %": "5m",
    }


def test_every_rule_queries_prometheus_and_has_a_unique_uid():
    all_rules = rules().values()
    assert len({rule["uid"] for rule in all_rules}) == len(all_rules)
    for rule in all_rules:
        queries = [d for d in rule["data"] if d["datasourceUid"] != "__expr__"]
        assert queries and all(d["datasourceUid"] == "prometheus" for d in queries)
        assert rule["condition"] in {d["refId"] for d in rule["data"]}


def test_no_input_alert_stays_silent_once_the_producer_has_finished():
    expr = rules()["No streaming input"]["data"][0]["model"]["expr"]

    assert "inputRate_total" in expr
    assert "unless" in expr and "producer_finished == 1" in expr


def test_memory_alert_uses_the_working_set_not_usage():
    expr = rules()["Container memory over 90 % of its limit"]["data"][0]["model"]["expr"]

    assert "container_memory_working_set_bytes" in expr
    assert "container_memory_usage_bytes" not in expr


def test_alerts_are_mailed_to_the_alert_address():
    contact = load(PROVISIONING / "alerting" / "contact-points.yml")["contactPoints"][0]
    receiver = contact["receivers"][0]
    policy = load(PROVISIONING / "alerting" / "policies.yml")["policies"][0]

    assert receiver["type"] == "email"
    assert receiver["settings"]["addresses"] == "${ALERT_EMAIL}"
    assert policy["receiver"] == contact["name"]


def test_dashboard_panels_query_the_provisioned_datasource():
    datasource = load(PROVISIONING / "datasources" / "prometheus.yml")["datasources"][0]
    dashboard = json.loads((MONITORING / "grafana" / "dashboards" / "pipeline.json").read_text())

    assert datasource["uid"] == "prometheus"
    assert dashboard["panels"]
    for panel in dashboard["panels"]:
        assert panel["datasource"]["uid"] == "prometheus"
        assert all(target["expr"] for target in panel["targets"])
