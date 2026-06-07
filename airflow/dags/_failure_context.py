from __future__ import annotations

import json
from pathlib import Path
from typing import Any


LOG_TAIL_CHARS = 8000


def _task_log_dir(dag_id: str, run_id: str, task_id: str) -> Path:
    return (
        Path("/opt/airflow/logs")
        / f"dag_id={dag_id}"
        / f"run_id={run_id}"
        / f"task_id={task_id}"
    )


def _latest_try_number(dag_id: str, run_id: str, task_id: str) -> int:
    attempts = sorted(_task_log_dir(dag_id, run_id, task_id).glob("attempt=*.log"))
    if not attempts:
        return 1
    return int(attempts[-1].stem.split("=", 1)[1])


def _read_task_log(dag_id: str, run_id: str, task_id: str, try_number: int) -> str:
    log_dir = _task_log_dir(dag_id, run_id, task_id)
    attempt_path = log_dir / f"attempt={try_number}.log"
    if not attempt_path.exists():
        attempts = sorted(log_dir.glob("attempt=*.log"))
        attempt_path = attempts[-1] if attempts else attempt_path

    try:
        log_text = attempt_path.read_text(encoding="utf-8", errors="replace")
    except FileNotFoundError:
        return f"Log file not found for {dag_id}.{task_id} run {run_id} attempt {try_number}"

    if len(log_text) > LOG_TAIL_CHARS:
        return "...[truncated]...\n" + log_text[-LOG_TAIL_CHARS:]
    return log_text


def collect_failure_context_payload(dag_file: str, failed_task_id: str, **context: Any) -> str:
    dag_run = context["dag_run"]
    dag_id = context["dag"].dag_id
    run_id = dag_run.run_id
    try_number = _latest_try_number(dag_id, run_id, failed_task_id)

    payload = {
        "dag_id": dag_id,
        "run_id": run_id,
        "dag_file": dag_file,
        "failed_task": {
            "task_id": failed_task_id,
            "state": "failed",
            "try_number": try_number,
        },
        "log_excerpt": _read_task_log(dag_id, run_id, failed_task_id, try_number),
    }
    return json.dumps(payload)
