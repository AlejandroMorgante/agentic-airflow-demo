from __future__ import annotations

import hashlib
import json
import re
from typing import Any


def normalize_query_payload(message: str | dict[str, Any]) -> dict[str, Any]:
    """Normalize the Airflow failure context sent as the ADK user message."""
    payload: Any = message
    if isinstance(payload, str):
        payload = json.loads(payload)
    if not isinstance(payload, dict):
        raise ValueError("ADK query message must resolve to a JSON object")
    return payload


def session_id_for(payload: dict[str, Any]) -> str:
    """Create a valid, stable Agent Engine session ID for one Airflow Dag run."""
    dag_id = str(payload.get("dag_id", "unknown-dag"))
    run_id = str(payload.get("run_id", "unknown-run"))
    readable = re.sub(r"[^a-z0-9-]+", "-", f"{dag_id}-{run_id}".lower()).strip("-")
    digest = hashlib.sha256(f"{dag_id}:{run_id}".encode()).hexdigest()[:12]
    return f"a-{readable[:48].strip('-')}-{digest}"


def final_response(events: list[dict[str, Any]]) -> str:
    """Extract the final textual response while retaining every ADK event."""
    for event in reversed(events):
        content = event.get("content") or {}
        texts = [
            part["text"]
            for part in content.get("parts", [])
            if isinstance(part, dict) and part.get("text")
        ]
        if texts:
            return "\n".join(texts)
    return "ADK completed without a textual final response."
