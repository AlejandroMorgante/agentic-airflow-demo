from __future__ import annotations

import logging
import os
from typing import Any

import requests
from tools.strands_compat import tool

log = logging.getLogger(__name__)


def _use_mocks() -> bool:
    return os.environ.get("AGENT_USE_MOCKS", "false").lower() in {"1", "true", "yes"}


def _mock_slack_message(
    dag_id: str,
    task_id: str,
    run_id: str,
    what_happened: str,
    likely_cause: str,
    suggested_fix: str,
    airflow_url: str | None,
) -> dict[str, Any]:
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
    """Post a structured troubleshooting message to Slack."""
    if _use_mocks():
        return _mock_slack_message(
            dag_id,
            task_id,
            run_id,
            what_happened,
            likely_cause,
            suggested_fix,
            airflow_url,
        )

    try:
        webhook_url = os.environ["SLACK_WEBHOOK_URL"]
        airflow_url = airflow_url or os.environ.get("AIRFLOW_BASE_URL")
        footer = f"run_id: {run_id}"
        if airflow_url:
            footer = f"{footer} | Airflow: {airflow_url.rstrip('/')}/dags/{dag_id}"

        payload = {
            "text": f"Airflow task failed: {dag_id}.{task_id}",
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": f"Airflow task failed: {dag_id}.{task_id}",
                    },
                },
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": f"*What happened*\n{what_happened}"},
                },
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": f"*Likely cause*\n{likely_cause}"},
                },
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": f"*Suggested fix*\n{suggested_fix}"},
                },
                {"type": "context", "elements": [{"type": "mrkdwn", "text": footer}]},
            ],
        }
        log.info("Posting Slack message for %s.%s", dag_id, task_id)
        resp = requests.post(webhook_url, json=payload, timeout=15)
        resp.raise_for_status()
        return {"ok": True, "status_code": resp.status_code}
    except Exception as exc:
        log.exception("Failed to post Slack message")
        return {"ok": False, "error": str(exc)}
