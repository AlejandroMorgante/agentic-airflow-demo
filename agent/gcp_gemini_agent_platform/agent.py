from __future__ import annotations

import logging
import os
from typing import Any, Callable

from shared.local_runtime import LocalInvocationApp
from shared.troubleshooting import SYSTEM_PROMPT, troubleshoot as run_troubleshooting
from shared.tools.github_api import create_github_pr, fetch_dag_source
from shared.tools.slack import post_to_slack

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

MODEL_ID = os.environ.get("GEMINI_MODEL_ID", "gemini-2.5-pro")
PROJECT_ID = (
    os.environ.get("GCP_PROJECT_ID")
    or os.environ.get("GCP_PROJECT")
    or os.environ.get("GOOGLE_CLOUD_PROJECT")
)
LOCATION = os.environ.get("GCP_REGION") or os.environ.get(
    "GOOGLE_CLOUD_LOCATION", "us-central1"
)
USE_MODEL = os.environ.get("AGENT_USE_MODEL", "false").lower() in {"1", "true", "yes"}
MAX_TOOL_ROUNDS = int(os.environ.get("GEMINI_MAX_TOOL_ROUNDS", "8"))

app = LocalInvocationApp()


def _tool_declarations() -> list[Any]:
    from google.genai import types

    return [
        types.Tool(
            function_declarations=[
                types.FunctionDeclaration(
                    name="fetch_dag_source",
                    description="Fetch Airflow Dag source code from the configured GitHub repository.",
                    parameters_json_schema={
                        "type": "object",
                        "properties": {"filename": {"type": "string"}},
                        "required": ["filename"],
                    },
                ),
                types.FunctionDeclaration(
                    name="create_github_pr",
                    description="Create a draft GitHub PR with the corrected Airflow Dag file.",
                    parameters_json_schema={
                        "type": "object",
                        "properties": {
                            "filename": {"type": "string"},
                            "fixed_content": {"type": "string"},
                            "pr_title": {"type": "string"},
                            "pr_body": {"type": "string"},
                        },
                        "required": [
                            "filename",
                            "fixed_content",
                            "pr_title",
                            "pr_body",
                        ],
                    },
                ),
                types.FunctionDeclaration(
                    name="post_to_slack",
                    description="Post a short structured Airflow incident summary to Slack.",
                    parameters_json_schema={
                        "type": "object",
                        "properties": {
                            "dag_id": {"type": "string"},
                            "task_id": {"type": "string"},
                            "run_id": {"type": "string"},
                            "what_happened": {"type": "string"},
                            "likely_cause": {"type": "string"},
                            "suggested_fix": {"type": "string"},
                            "pr_url": {"type": "string"},
                            "airflow_url": {"type": "string"},
                        },
                        "required": [
                            "dag_id",
                            "task_id",
                            "run_id",
                            "what_happened",
                            "likely_cause",
                            "suggested_fix",
                        ],
                    },
                ),
            ]
        )
    ]


def _tool_functions() -> dict[str, Callable[..., Any]]:
    return {
        "fetch_dag_source": fetch_dag_source,
        "create_github_pr": create_github_pr,
        "post_to_slack": post_to_slack,
    }


def _execute_tool(name: str | None, args: dict[str, Any] | None) -> dict[str, Any]:
    if not name:
        return {"error": "Function call is missing a name"}

    tool_func = _tool_functions().get(name)
    if tool_func is None:
        return {"error": f"Unknown tool: {name}"}

    try:
        log.info("Executing Gemini tool call: %s", name)
        return {"output": tool_func(**(args or {}))}
    except Exception as exc:
        log.exception("Gemini tool call failed: %s", name)
        return {"error": str(exc)}


def _run_gemini(prompt: str) -> str:
    if not PROJECT_ID:
        raise RuntimeError(
            "AGENT_USE_MODEL=true requires GCP_PROJECT_ID, GCP_PROJECT, or GOOGLE_CLOUD_PROJECT"
        )

    from google import genai
    from google.genai import types

    client = genai.Client(vertexai=True, project=PROJECT_ID, location=LOCATION)
    contents: list[types.Content] = [
        types.Content(role="user", parts=[types.Part.from_text(text=prompt)])
    ]
    config = types.GenerateContentConfig(
        system_instruction=(
            f"{SYSTEM_PROMPT}\n\n"
            "You must execute the provided tools for source retrieval, PR creation, "
            "and Slack notification. Do not claim a PR or Slack message exists unless "
            "the corresponding tool returned it."
        ),
        tools=_tool_declarations(),
        temperature=0.2,
    )

    for _ in range(MAX_TOOL_ROUNDS):
        response = client.models.generate_content(
            model=MODEL_ID,
            contents=contents,
            config=config,
        )
        function_calls = response.function_calls or []
        if not function_calls:
            return response.text or str(response)

        if response.candidates and response.candidates[0].content:
            contents.append(response.candidates[0].content)

        tool_response_parts = []
        for function_call in function_calls:
            tool_result = _execute_tool(
                function_call.name, dict(function_call.args or {})
            )
            tool_response_parts.append(
                types.Part.from_function_response(
                    name=function_call.name or "unknown_tool",
                    response=tool_result,
                )
            )
        contents.append(types.Content(role="user", parts=tool_response_parts))

    raise RuntimeError(
        f"Gemini did not produce a final response after {MAX_TOOL_ROUNDS} tool rounds"
    )


@app.entrypoint
def troubleshoot(payload: dict[str, Any]) -> dict[str, Any]:
    return run_troubleshooting(payload, model_runner=_run_gemini if USE_MODEL else None)


if __name__ == "__main__":
    app.run()
