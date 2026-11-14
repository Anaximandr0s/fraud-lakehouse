"""Failure email shared by the DAGs: what failed, when, with which error, and where to look next."""

import os

from airflow.providers.smtp.notifications.smtp import SmtpNotifier

SUBJECT = "[fraud-lakehouse] FAILED {{ ti.dag_id }} / {{ ti.task_id }} ({{ ti.run_id }})"

ROW = (
    '<tr><td style="padding:6px 16px 6px 0;color:#59636e;white-space:nowrap;vertical-align:top">'
    '{label}</td><td style="padding:6px 0">{value}</td></tr>'
)
ROWS = [
    ("Pipeline", "fraud-lakehouse"),
    ("DAG", "<b>{{ ti.dag_id }}</b> &mdash; {{ dag.description }}"),
    ("Task", "<b>{{ ti.task_id }}</b>"),
    ("Run", "{{ ti.run_id }}"),
    ("Attempt", "{{ ti.try_number }} of {{ ti.max_tries + 1 }} (no retry left)"),
    ("Started", "{{ ti.start_date.strftime('%Y-%m-%d %H:%M:%S') if ti.start_date else '?' }} UTC"),
    ("Command", '<code style="font-size:12px">{{ task.bash_command }}</code>'),
    (
        "Error",
        '<code style="font-size:12px;color:#d1242f">'
        "{{ exception | default('not available, see the task log') }}</code>",
    ),
]
DAG_URL = os.environ["AIRFLOW__API__BASE_URL"] + "/dags/{{ ti.dag_id }}"
BUTTON = (
    '<a href="{href}" style="display:inline-block;padding:8px 14px;margin-right:8px;'
    'background:#0969da;color:#ffffff;text-decoration:none;border-radius:6px">{text}</a>'
)

HTML = f"""
<div style="font-family:Arial,Helvetica,sans-serif;font-size:14px;color:#1f2328;max-width:720px">
  <div style="background:#d1242f;color:#ffffff;padding:12px 16px;font-size:16px;font-weight:bold">
    Task failed: {{{{ ti.dag_id }}}} / {{{{ ti.task_id }}}}
  </div>
  <table style="border-collapse:collapse;margin:16px 0">
    {"".join(ROW.format(label=label, value=value) for label, value in ROWS)}
  </table>
  <p>
    {BUTTON.format(href="{{ ti.log_url }}", text="Open the task log")}
    {BUTTON.format(href=DAG_URL, text="Open the DAG")}
  </p>
  <p style="color:#59636e;font-size:13px">
    Next steps: read the end of the task log (dbt prints the failing model or SQL there),
    check that spark-thrift is healthy (<code>docker compose ps</code>), then clear the task
    in the Airflow UI to rerun it.
  </p>
</div>
"""


def failure_email() -> SmtpNotifier:
    return SmtpNotifier(
        to=os.environ["ALERT_EMAIL"],
        smtp_conn_id="smtp_default",
        subject=SUBJECT,
        html_content=HTML,
    )
