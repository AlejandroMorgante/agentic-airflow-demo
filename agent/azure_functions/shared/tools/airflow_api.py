from __future__ import annotations

import logging
import os
from typing import Any
from urllib.parse import quote

import requests
from shared.tools.strands_compat import tool

log = logging.getLogger(__name__)

LOG_TAIL_CHARS = 8000


def _use_mocks() -> bool:
    return os.environ.get("AGENT_USE_MOCKS", "false").lower() in {"1", "true", "yes"}


def _mock_failed_tasks(dag_id: str, run_id: str) -> list[dict[str, Any]]:
    log.info("Mock fetch_failed_tasks dag_id=%s run_id=%s", dag_id, run_id)
    return [
        {
            "task_id": "transform",
            "try_number": 1,
            "state": "failed",
            "start_date": "2026-06-05T12:00:00+00:00",
            "end_date": "2026-06-05T12:00:05+00:00",
            "dag_file": f"{dag_id}.py",
        }
    ]


def _mock_task_logs(dag_id: str, run_id: str, task_id: str, try_number: int) -> str:
    log.info(
        "Mock fetch_task_logs dag_id=%s run_id=%s task_id=%s try_number=%s",
        dag_id,
        run_id,
        task_id,
        try_number,
    )
    return "\n".join(
        [
            "[2026-06-05T12:00:01+0000] {taskinstance.py:3312} ERROR - Task failed",
            "Traceback (most recent call last):",
            '  File "/opt/airflow/dags/demo_failing_etl.py", line 23, in transform',
            '    return {"transformed": data["rowz"]}',
            "KeyError: 'rowz'",
        ]
    )


def _encoded(value: str) -> str:
    return quote(value, safe="")


def _airflow_auth() -> tuple[str, str]:
    return os.environ["AIRFLOW_USERNAME"], os.environ["AIRFLOW_PASSWORD"]


def _airflow_base_url() -> str:
    return os.environ["AIRFLOW_BASE_URL"].rstrip("/")


def _airflow_get_json(path: str) -> dict[str, Any]:
    url = f"{_airflow_base_url()}{path}"
    log.info("Airflow GET %s", url)
    resp = requests.get(url, auth=_airflow_auth(), timeout=15)
    resp.raise_for_status()
    return resp.json()


@tool
def fetch_failed_tasks(dag_id: str, run_id: str) -> list[dict[str, Any]]:
    """List failed task instances for an Airflow DAG run."""
    if _use_mocks():
        return _mock_failed_tasks(dag_id, run_id)

    try:
        path = (
            f"/api/v2/dags/{_encoded(dag_id)}/dagRuns/{_encoded(run_id)}"
            "/taskInstances?state=failed"
        )
        data = _airflow_get_json(path)
        return [
            {
                "task_id": ti["task_id"],
                "try_number": ti.get("try_number") or ti.get("try_number_int") or 1,
                "state": ti.get("state"),
                "start_date": ti.get("start_date"),
                "end_date": ti.get("end_date"),
                "dag_file": ti.get("dag_file") or ti.get("fileloc"),
            }
            for ti in data.get("task_instances", [])
        ]
    except Exception as exc:
        log.exception("Failed to fetch failed tasks")
        return [{"error": str(exc), "dag_id": dag_id, "run_id": run_id}]


@tool
def fetch_task_logs(dag_id: str, run_id: str, task_id: str, try_number: int = 1) -> str:
    """Fetch logs for a specific Airflow task instance attempt."""
    if _use_mocks():
        return _mock_task_logs(dag_id, run_id, task_id, try_number)

    try:
        path = (
            f"/api/v2/dags/{_encoded(dag_id)}/dagRuns/{_encoded(run_id)}"
            f"/taskInstances/{_encoded(task_id)}/logs/{try_number}"
        )
        url = f"{_airflow_base_url()}{path}"
        log.info("Airflow GET %s", url)
        resp = requests.get(url, auth=_airflow_auth(), timeout=30)
        resp.raise_for_status()
        text = resp.text
        if len(text) > LOG_TAIL_CHARS:
            text = "...[truncated]...\n" + text[-LOG_TAIL_CHARS:]
        return text
    except Exception as exc:
        log.exception("Failed to fetch task logs")
        return f"ERROR fetching logs for {dag_id}.{task_id} run {run_id}: {exc}"
