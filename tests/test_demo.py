import json

import pytest

from fraud_lakehouse import demo


def test_only_services_without_dependents_can_be_stopped():
    stoppable = {s.key for s in demo.NODES if s.stoppable}

    assert stoppable == {
        "producer",
        "airflow",
        "metabase",
        "jupyter",
        "prometheus",
        "grafana",
        "cadvisor",
    }


def test_stopping_a_core_service_is_refused():
    with pytest.raises(ValueError, match="spark-streaming"):
        demo.commands("stop", "spark-streaming")


def test_unknown_action_is_refused():
    with pytest.raises(ValueError, match="unknown action"):
        demo.commands("rm -rf", None)


def test_a_profile_service_is_stopped_and_started_with_its_profile():
    assert demo.commands("stop", "grafana") == [
        ["docker", "compose", "--profile", "monitoring", "stop", "grafana"]
    ]
    assert demo.commands("start", "grafana") == [
        [
            "docker",
            "compose",
            "--profile",
            "monitoring",
            "up",
            "-d",
            "--no-deps",
            "--wait",
            "grafana",
        ]
    ]


def test_global_actions_keep_or_delete_volumes():
    assert demo.commands("stop-all", None) == [["make", "down"]]
    assert demo.commands("reset", None) == [["docker", "compose", "--profile", "*", "down", "-v"]]


def test_parse_ps_reads_state_and_health_per_service():
    lines = "\n".join(
        json.dumps(row)
        for row in [
            {"Service": "kafka", "State": "running", "Health": "healthy", "Image": "k:4"},
            {"Service": "kafka-init", "State": "exited", "Health": "", "Image": "k:4"},
        ]
    )

    assert demo.parse_ps(lines) == {
        "kafka": {"state": "running", "health": "healthy", "image": "k:4"},
        "kafka-init": {"state": "exited", "health": "", "image": "k:4"},
    }


def test_parse_stats_maps_container_names_to_services():
    line = json.dumps(
        {
            "Name": "fraud-lakehouse-spark-worker-1",
            "MemUsage": "2.1GiB / 3.5GiB",
            "MemPerc": "60.12%",
            "CPUPerc": "12.50%",
        }
    )
    other_project = json.dumps(
        {"Name": "other-db-1", "MemUsage": "1MiB / 1GiB", "MemPerc": "0.1%", "CPUPerc": "0%"}
    )

    assert demo.parse_stats(f"{line}\n{other_project}", "fraud-lakehouse") == {
        "spark-worker": {"memory": "2.1GiB / 3.5GiB", "memory_percent": 60.12, "cpu_percent": 12.5}
    }


def test_parse_metrics_ignores_comments_and_labels():
    text = (
        "# HELP producer_position rows\n"
        "producer_position 630000.0\n"
        'metrics_x_inputRate_total_Value{type="gauges"} 20.0\n'
    )

    assert demo.parse_metrics(text) == {
        "producer_position": 630000.0,
        "metrics_x_inputRate_total_Value": 20.0,
    }


def test_streaming_rates_pick_the_query_gauges():
    prefix = "metrics_spark_streaming_driver_spark_streaming_transactions_"
    metrics = {
        f"{prefix}inputRate_total_Value": 20.0,
        f"{prefix}processingRate_total_Value": 105.2,
        f"{prefix}latency_Value": 5703.0,
    }

    assert demo.streaming_rates(metrics) == {
        "input_per_second": 20.0,
        "processed_per_second": 105.2,
        "batch_ms": 5703.0,
    }


def test_progress_estimates_the_remaining_time_from_the_rate():
    assert demo.progress(position=600, total=1000, rate=20.0) == {
        "position": 600,
        "total": 1000,
        "percent": 60.0,
        "remaining": 400,
        "eta_seconds": 20,
    }


def test_progress_has_no_estimate_when_nothing_is_sent():
    assert demo.progress(position=1000, total=1000, rate=0.0)["eta_seconds"] is None


def test_parse_metrics_skips_the_text_spark_serves_while_starting():
    assert demo.parse_metrics("Spark is starting up. Please wait a while until it's ready.") == {}


def test_starting_jupyter_builds_it_without_restarting_paused_services():
    assert demo.commands("start", "jupyter") == [
        [
            "docker",
            "compose",
            "--profile",
            "notebook",
            "up",
            "-d",
            "--build",
            "--no-deps",
            "--wait",
            "--wait-timeout",
            "180",
            "jupyter",
        ]
    ]
