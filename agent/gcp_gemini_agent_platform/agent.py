from __future__ import annotations

import logging
import os
from typing import Any

from google.adk.agents import Agent
from vertexai.agent_engines import AdkApp

from agent.gcp_gemini_agent_platform.query import (
    final_response,
    normalize_query_payload,
    session_id_for,
)
from agent.shared.troubleshooting import SYSTEM_PROMPT, _payload_prompt
from agent.shared.tools.github_api import create_github_pr, fetch_dag_source
from agent.shared.tools.slack import post_to_slack

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

MODEL_ID = os.environ.get("GEMINI_MODEL_ID", "gemini-2.5-pro")
APP_NAME = "airflow-self-healing"


root_agent = Agent(
    model=MODEL_ID,
    name="airflow_self_healing_agent",
    description=(
        "Diagnoses failed Airflow tasks, proposes a source fix, opens a draft "
        "GitHub pull request, and notifies Slack."
    ),
    instruction=(
        f"{SYSTEM_PROMPT}\n\n"
        "You must use fetch_dag_source to inspect the DAG, create_github_pr to "
        "open the draft fix, and post_to_slack to notify the team. Never claim a "
        "PR or Slack message exists unless the corresponding tool returned it."
    ),
    tools=[fetch_dag_source, create_github_pr, post_to_slack],
)


class AirflowAdkRuntime:
    """Source-deployed runtime that delegates every invocation to Google ADK."""

    def register_operations(self) -> dict[str, list[str]]:
        return {"async": ["query"]}

    async def query(
        self,
        message: str | dict[str, Any],
        user_id: str = "airflow",
        session_id: str | None = None,
    ) -> dict[str, Any]:
        payload = normalize_query_payload(message)
        resolved_session_id = session_id or session_id_for(payload)
        prompt = _payload_prompt(payload)

        adk_app = AdkApp(agent=root_agent, app_name=APP_NAME)
        try:
            session = await adk_app.async_get_session(
                user_id=user_id,
                session_id=resolved_session_id,
            )
        except Exception as exc:
            if "Session not found" not in str(exc):
                raise
            session = None
        if not session:
            await adk_app.async_create_session(
                user_id=user_id,
                session_id=resolved_session_id,
            )

        log.info(
            "Running ADK troubleshooting user_id=%s session_id=%s dag_id=%s run_id=%s",
            user_id,
            resolved_session_id,
            payload.get("dag_id"),
            payload.get("run_id"),
        )
        events = [
            event
            async for event in adk_app.async_stream_query(
                user_id=user_id,
                session_id=resolved_session_id,
                message=prompt,
            )
        ]
        return {
            "summary": final_response(events),
            "user_id": user_id,
            "session_id": resolved_session_id,
            "events": events,
        }


app = AirflowAdkRuntime()
