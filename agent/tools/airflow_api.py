from __future__ import annotations

import logging
from typing import Any

from tools.strands_compat import tool

log = logging.getLogger(__name__)


@tool
def fetch_failed_tasks(dag_id: str, run_id: str) -> list[dict[str, Any]]:
    """Return mocked failed task instances for local agent development."""
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


@tool
def fetch_task_logs(dag_id: str, run_id: str, task_id: str, try_number: int = 1) -> str:
    """Return mocked Airflow logs for local agent development."""
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
