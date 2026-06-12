from __future__ import annotations

import logging
import os
from typing import Any

from shared.local_runtime import LocalInvocationApp
from shared.troubleshooting import SYSTEM_PROMPT, troubleshoot as run_troubleshooting
from shared.tools.github_api import create_github_pr, fetch_dag_source
from shared.tools.slack import post_to_slack

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

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

app = BedrockAgentCoreApp() if BedrockAgentCoreApp else LocalInvocationApp()

strands_agent = None
if USE_MODEL:
    if Agent is None or BedrockModel is None:
        raise RuntimeError("AGENT_USE_MODEL=true requires bedrock-agentcore and strands-agents")
    strands_agent = Agent(
        model=BedrockModel(model_id=MODEL_ID),
        system_prompt=SYSTEM_PROMPT,
        tools=[fetch_dag_source, create_github_pr, post_to_slack],
    )


@app.entrypoint
def troubleshoot(payload: dict[str, Any]) -> dict[str, Any]:
    return run_troubleshooting(payload, model_runner=strands_agent)


if __name__ == "__main__":
    app.run()
