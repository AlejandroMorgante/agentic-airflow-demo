from __future__ import annotations

import asyncio
import json
import logging
import os
import traceback
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

from shared.troubleshooting import troubleshoot as run_troubleshooting

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

USE_MODEL = (
    os.environ.get("AIRFLOW_AGENT_USE_MODEL") or os.environ.get("AGENT_USE_MODEL", "false")
).lower() in {"1", "true", "yes"}


def _extract_payload(message: str) -> dict[str, Any]:
    start = message.find("{")
    end = message.rfind("}")
    if start == -1 or end == -1 or end < start:
        raise ValueError("Invocation message must include a JSON Airflow failure context payload.")
    return json.loads(message[start : end + 1])


def _deterministic_or_tooling_response(message: str) -> dict[str, Any]:
    payload = _extract_payload(message)
    return run_troubleshooting(payload)


async def _model_response(message: str) -> dict[str, Any]:
    try:
        from agent_framework import Agent
        from agent_framework.foundry import FoundryChatClient
        from azure.identity import DefaultAzureCredential, ManagedIdentityCredential

        from shared.troubleshooting import SYSTEM_PROMPT
        from shared.tools.github_api import create_github_pr, fetch_dag_source
        from shared.tools.slack import post_to_slack
    except ImportError as exc:
        raise RuntimeError("AGENT_USE_MODEL=true requires the Azure hosted agent dependencies.") from exc

    project_endpoint = os.environ.get("FOUNDRY_PROJECT_ENDPOINT") or os.environ.get(
        "AIRFLOW_AGENT_FOUNDRY_PROJECT_ENDPOINT"
    )
    model = os.environ.get("AZURE_AI_MODEL_DEPLOYMENT_NAME") or os.environ.get("MODEL_DEPLOYMENT_NAME")
    if not project_endpoint or not model:
        raise RuntimeError(
            "AGENT_USE_MODEL=true requires AIRFLOW_AGENT_FOUNDRY_PROJECT_ENDPOINT and AZURE_AI_MODEL_DEPLOYMENT_NAME."
        )

    instance_client_id = os.environ.get("FOUNDRY_AGENT_INSTANCE_CLIENT_ID")
    credential = (
        ManagedIdentityCredential(client_id=instance_client_id)
        if instance_client_id
        else DefaultAzureCredential()
    )
    client = FoundryChatClient(
        project_endpoint=project_endpoint,
        model=model,
        credential=credential,
    )
    agent = Agent(
        client=client,
        instructions=(
            f"{SYSTEM_PROMPT}\n\n"
            "Use the available local tools for source retrieval, PR creation, and Slack notification. "
            "Do not claim a PR or Slack message exists unless the corresponding tool returned it."
        ),
        tools=[fetch_dag_source, create_github_pr, post_to_slack],
        default_options={"store": False},
    )
    response = await agent.run(message)
    return {"summary": response.text}


async def _handle_message(message: str) -> dict[str, Any]:
    if USE_MODEL:
        return await _model_response(message)
    return _deterministic_or_tooling_response(message)


def _run_local_fallback() -> None:
    class InvocationHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if self.path not in {"/", "/health", "/readiness"}:
                self.send_response(404)
                self.end_headers()
                return
            self._send_json({"ok": True})

        def do_POST(self) -> None:
            if self.path != "/invocations":
                self.send_response(404)
                self.end_headers()
                return

            try:
                length = int(self.headers.get("content-length", "0"))
                body = json.loads(self.rfile.read(length) or b"{}")
                message = body.get("message")
                if not isinstance(message, str):
                    raise ValueError("Missing string field 'message'.")
                response = {"response": asyncio.run(_handle_message(message))}
                self._send_json(response)
            except Exception as exc:
                log.exception("Hosted agent invocation failed")
                self._send_json({"error": str(exc)}, status=500)

        def _send_json(self, payload: dict[str, Any], status: int = 200) -> None:
            body = json.dumps(payload, default=str).encode()
            self.send_response(status)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: Any) -> None:
            log.info(format, *args)

    port = int(os.environ.get("PORT", "8088"))
    log.info("Starting local Azure hosted-agent fallback server on :%s", port)
    HTTPServer(("0.0.0.0", port), InvocationHandler).serve_forever()


def main() -> None:
    try:
        from azure.ai.agentserver.invocations import InvocationAgentServerHost
        from starlette.requests import Request
        from starlette.responses import JSONResponse, Response
    except ImportError:
        _run_local_fallback()
        return

    app = InvocationAgentServerHost()

    @app.invoke_handler
    async def handle_invoke(request: Request):
        data = await request.json()
        message = data.get("message")
        if not isinstance(message, str):
            return Response(content="Missing string field 'message'.", status_code=400)
        try:
            return JSONResponse({"response": await _handle_message(message)})
        except Exception as exc:
            log.exception("Hosted agent invocation failed")
            return JSONResponse(
                {
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                    "traceback": traceback.format_exc(limit=8),
                },
                status_code=500,
            )

    app.run()


if __name__ == "__main__":
    main()
