from pathlib import Path

import yaml

from fraud_lakehouse.schemas import TEST_SPLIT_TS

DBT_PROJECT = Path(__file__).parents[1] / "dbt" / "dbt_project.yml"


def test_dbt_split_matches_the_python_split():
    project = yaml.safe_load(DBT_PROJECT.read_text())

    assert project["vars"]["test_split_ts"] == TEST_SPLIT_TS
