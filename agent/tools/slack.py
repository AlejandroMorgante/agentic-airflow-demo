from __future__ import annotations

import logging
from typing import Any

from tools.strands_compat import tool

log = logging.getLogger(__name__)


@tool
def post_to_slack(
    dag_id: str,
    task_id: str,
    run_id: str,
    what_happened: str,
    likely_cause: str,
    suggested_fix: str,
    airflow_url: str | None = None,
) -> dict[str, Any]:
    """Mock Slack posting for local agent development."""
    message = {
        "dag_id": dag_id,
        "task_id": task_id,
        "run_id": run_id,
        "what_happened": what_happened,
        "likely_cause": likely_cause,
        "suggested_fix": suggested_fix,
        "airflow_url": airflow_url,
    }
    log.info("Mock post_to_slack payload=%s", message)
    return {"ok": True, "mock": True, "message": message}
