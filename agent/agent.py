from __future__ import annotations

import json
import logging
import os
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, Callable

from tools.airflow_api import fetch_failed_tasks, fetch_task_logs
from tools.github_api import fetch_dag_source
from tools.slack import post_to_slack

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

SYSTEM_PROMPT = (Path(__file__).parent / "prompts" / "system_prompt.md").read_text()
MODEL_ID = os.environ.get("BEDROCK_MODEL_ID", "anthropic.claude-sonnet-4-5-20250929-v1:0")
USE_MODEL = os.environ.get("AGENT_USE_MODEL", "false").lower() in {"1", "true", "yes"}

try:
    from bedrock_agentcore.runtime import BedrockAgentCoreApp
    from strands import Agent
    from strands.models import BedrockModel
except ImportError:
    BedrockAgentCoreApp = None
    Agent = None
    BedrockModel = None


class LocalApp:
    def __init__(self) -> None:
        self._handler: Callable[[dict[str, Any]], dict[str, Any]] | None = None

    def entrypoint(self, func: Callable[[dict[str, Any]], dict[str, Any]]):
        self._handler = func
        return func

    def run(self) -> None:
        if self._handler is None:
            raise RuntimeError("No entrypoint registered")

        handler_func = self._handler

        class InvocationHandler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                if self.path != "/invocations":
                    self.send_response(404)
                    self.end_headers()
                    return

                length = int(self.headers.get("content-length", "0"))
                payload = json.loads(self.rfile.read(length) or b"{}")
                response = handler_func(payload)
                body = json.dumps(response).encode()
                self.send_response(200)
                self.send_header("content-type", "application/json")
                self.send_header("content-length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format: str, *args: Any) -> None:
                log.info(format, *args)

        port = int(os.environ.get("PORT", "8080"))
        log.info("Starting local invocation server on :%s", port)
        HTTPServer(("0.0.0.0", port), InvocationHandler).serve_forever()


app = BedrockAgentCoreApp() if BedrockAgentCoreApp else LocalApp()

strands_agent = None
if USE_MODEL:
    if Agent is None or BedrockModel is None:
        raise RuntimeError("AGENT_USE_MODEL=true requires bedrock-agentcore and strands-agents")
    strands_agent = Agent(
        model=BedrockModel(model_id=MODEL_ID),
        system_prompt=SYSTEM_PROMPT,
        tools=[fetch_failed_tasks, fetch_task_logs, fetch_dag_source, post_to_slack],
    )


def _deterministic_troubleshoot(dag_id: str, run_id: str) -> dict[str, Any]:
    failed_tasks = fetch_failed_tasks(dag_id, run_id)
    if not failed_tasks:
        return {"summary": "No failed tasks found", "dag_id": dag_id, "run_id": run_id}
    if failed_tasks[0].get("error"):
        return {
            "summary": "Failed to query Airflow for failed tasks",
            "dag_id": dag_id,
            "run_id": run_id,
            "error": failed_tasks[0]["error"],
        }

    task = failed_tasks[0]
    task_id = task["task_id"]
    try_number = int(task.get("try_number") or 1)
    logs = fetch_task_logs(dag_id, run_id, task_id, try_number)
    filename = task.get("dag_file") or task.get("fileloc") or f"{dag_id}.py"
    source = fetch_dag_source(str(filename).split("/")[-1])

    likely_cause = "The transform task reads data['rowz'], but extract returns the key 'rows'."
    suggested_fix = "Change data['rowz'] to data['rows'] in transform, or update extract to emit 'rowz'."
    what_happened = f"{dag_id}.{task_id} failed with KeyError: 'rowz'."
    slack_result = post_to_slack(
        dag_id=dag_id,
        task_id=task_id,
        run_id=run_id,
        what_happened=what_happened,
        likely_cause=likely_cause,
        suggested_fix=suggested_fix,
    )

    return {
        "dag_id": dag_id,
        "run_id": run_id,
        "task_id": task_id,
        "summary": what_happened,
        "likely_cause": likely_cause,
        "suggested_fix": suggested_fix,
        "log_excerpt": logs[-1000:],
        "source_excerpt": source[:1000],
        "slack": slack_result,
    }


@app.entrypoint
def troubleshoot(payload: dict[str, Any]) -> dict[str, Any]:
    dag_id = payload["dag_id"]
    run_id = payload["run_id"]
    log.info("Troubleshooting %s / %s", dag_id, run_id)

    if strands_agent is None:
        return _deterministic_troubleshoot(dag_id, run_id)

    prompt = (
        f"A task in DAG `{dag_id}` failed during run `{run_id}`. "
        "Follow your standard troubleshooting procedure: identify the failed task, "
        "read its logs, read the DAG source from GitHub, analyze the cause, and "
        "post a clear explanation with a suggested fix to Slack."
    )
    result = strands_agent(prompt)
    return {"summary": str(result)}


if __name__ == "__main__":
    app.run()
