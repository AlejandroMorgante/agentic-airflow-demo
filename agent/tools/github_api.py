from __future__ import annotations

import logging

from tools.strands_compat import tool

log = logging.getLogger(__name__)


@tool
def fetch_dag_source(filename: str) -> str:
    """Return mocked DAG source for local agent development."""
    log.info("Mock fetch_dag_source filename=%s", filename)
    return '''from airflow.sdk import DAG, task

@task
def extract() -> dict:
    return {"rows": 100, "source": "demo"}

@task
def transform(data: dict) -> dict:
    return {"transformed": data["rowz"]}

@task
def load(data: dict) -> str:
    return f"loaded {data['transformed']}"
'''
