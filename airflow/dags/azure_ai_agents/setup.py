from __future__ import annotations

import json

import pendulum
from airflow.providers.microsoft.azure.operators.ai_agents import (
    CreateAzureAIAgentOperator,
    DeleteAzureAIAgentOperator,
    RunAzureAIAgentOperator,
    UpdateAzureAIAgentOperator,
)
from airflow.sdk import DAG

try:
    from airflow.sdk import TriggerRule
except ImportError:
    from airflow.utils.trigger_rule import TriggerRule


DEFAULT_ARGS = {"owner": "airflow"}

AZURE_AI_AGENTS_CONN_ID = "{{ var.value.get('AZURE_AI_AGENTS_CONN_ID', 'azure_ai_agents_default') }}"
ENDPOINT = "{{ var.value.get('AZURE_AI_AGENTS_ENDPOINT', '') }}"
MODEL = "{{ var.value.get('AZURE_AI_AGENTS_MODEL', 'gpt-4o') }}"
AGENT_NAME = "{{ var.value.get('AZURE_AI_AGENT_NAME', 'airflow-troubleshooting-agent') }}"
AGENT_ID = "{{ var.value.AZURE_AI_AGENT_ID }}"
CREATED_AGENT_ID = "{{ ti.xcom_pull(task_ids='create_agent')['id'] }}"

AGENT_INSTRUCTIONS = (
    "You troubleshoot failed Airflow DAG tasks from JSON failure context. "
    "Analyze the logs and task metadata, then return JSON with summary, root_cause, "
    "and suggested_fix. Do not claim PR or Slack actions."
)

# Each tool maps to an Azure Function with Storage Queue input/output bindings.
# The agent writes tool call arguments to the input queue and reads results from
# the output queue — Azure handles the execution loop without requires_action.
def _az_function_tool(
    name: str,
    description: str,
    parameters: dict,
    storage_endpoint: str,
) -> dict:
    queue_prefix = name.replace("_", "-")
    return {
        "type": "azure_function",
        "azure_function": {
            "function": {
                "name": name,
                "description": description,
                "parameters": parameters,
            },
            "input_binding": {
                "type": "storage_queue",
                "storage_queue": {
                    "queue_name": f"{queue_prefix}-input",
                    "queue_service_endpoint": storage_endpoint,
                },
            },
            "output_binding": {
                "type": "storage_queue",
                "storage_queue": {
                    "queue_name": f"{queue_prefix}-output",
                    "queue_service_endpoint": storage_endpoint,
                },
            },
        },
    }


def _build_tools(storage_endpoint: str) -> list:
    return [
        _az_function_tool(
            name="fetch_dag_source",
            description="Fetch Airflow DAG source code from the configured GitHub repository.",
            parameters={
                "type": "object",
                "properties": {"filename": {"type": "string"}},
                "required": ["filename"],
            },
            storage_endpoint=storage_endpoint,
        ),
        _az_function_tool(
            name="create_github_pr",
            description="Create a draft GitHub PR with the agent-suggested fix for a failing DAG.",
            parameters={
                "type": "object",
                "properties": {
                    "filename": {"type": "string"},
                    "fixed_content": {"type": "string"},
                    "pr_title": {"type": "string"},
                    "pr_body": {"type": "string"},
                },
                "required": ["filename", "fixed_content", "pr_title", "pr_body"],
            },
            storage_endpoint=storage_endpoint,
        ),
        _az_function_tool(
            name="post_to_slack",
            description="Post a short structured Airflow incident summary to Slack.",
            parameters={
                "type": "object",
                "properties": {
                    "dag_id": {"type": "string"},
                    "task_id": {"type": "string"},
                    "run_id": {"type": "string"},
                    "what_happened": {"type": "string"},
                    "likely_cause": {"type": "string"},
                    "suggested_fix": {"type": "string"},
                    "pr_url": {"type": "string"},
                },
                "required": ["dag_id", "task_id", "run_id", "what_happened", "likely_cause", "suggested_fix"],
            },
            storage_endpoint=storage_endpoint,
        ),
    ]


# Resolved at DAG parse time from the Airflow Variable.
import os as _os
_STORAGE_ENDPOINT = _os.environ.get(
    "AIRFLOW_VAR_AZURE_STORAGE_QUEUE_ENDPOINT", ""
)

CREATE_AGENT_CONFIG = {
    "name": AGENT_NAME,
    "instructions": AGENT_INSTRUCTIONS,
    # Azure Function tools require Enterprise Standard tier.
    # "tools": _build_tools(_STORAGE_ENDPOINT),
}

UPDATE_AGENT_CONFIG = {
    "instructions": AGENT_INSTRUCTIONS,
    # "tools": _build_tools(_STORAGE_ENDPOINT),
}

SMOKE_FAILURE_CONTEXT = json.dumps(
    {
        "dag_id": "azure_ai_agents_demo_failing_etl",
        "run_id": "manual__azure_ai_agents_smoke",
        "dag_file": "azure_ai_agents/demo_failing_etl.py",
        "failed_task": {
            "task_id": "transform",
            "state": "failed",
            "try_number": 1,
        },
        "log_excerpt": "KeyError: 'rowz'",
    }
)

RUN_AGENT_CONFIG = {
    "thread": {
        "messages": [
            {
                "role": "user",
                "content": (
                    "Analyze this Airflow failure context and return the requested JSON.\n\n"
                    f"{SMOKE_FAILURE_CONTEXT}"
                ),
            }
        ],
    },
}


with DAG(
    dag_id="azure_ai_agents_create_agent",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["agentic-airflow", "azure", "ai-agents", "setup"],
) as create_dag:
    create_agent = CreateAzureAIAgentOperator(
        task_id="create_agent",
        model=MODEL,
        config=CREATE_AGENT_CONFIG,
        azure_ai_agents_conn_id=AZURE_AI_AGENTS_CONN_ID,
        endpoint=ENDPOINT,
    )


with DAG(
    dag_id="azure_ai_agents_update_agent",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["agentic-airflow", "azure", "ai-agents", "setup"],
) as update_dag:
    update_agent = UpdateAzureAIAgentOperator(
        task_id="update_agent",
        agent_id=AGENT_ID,
        config=UPDATE_AGENT_CONFIG,
        azure_ai_agents_conn_id=AZURE_AI_AGENTS_CONN_ID,
        endpoint=ENDPOINT,
    )


with DAG(
    dag_id="azure_ai_agents_run_agent",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["agentic-airflow", "azure", "ai-agents", "setup"],
) as run_dag:
    run_agent = RunAzureAIAgentOperator(
        task_id="run_agent",
        agent_id=AGENT_ID,
        config=RUN_AGENT_CONFIG,
        wait_for_completion=True,
        poll_interval=10,
        timeout=900,
        deferrable=True,
        azure_ai_agents_conn_id=AZURE_AI_AGENTS_CONN_ID,
        endpoint=ENDPOINT,
    )


with DAG(
    dag_id="azure_ai_agents_delete_agent",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["agentic-airflow", "azure", "ai-agents", "setup"],
) as delete_dag:
    delete_agent = DeleteAzureAIAgentOperator(
        task_id="delete_agent",
        agent_id=AGENT_ID,
        poll_interval=10,
        timeout=900,
        deferrable=True,
        trigger_rule=TriggerRule.ALL_DONE,
        azure_ai_agents_conn_id=AZURE_AI_AGENTS_CONN_ID,
        endpoint=ENDPOINT,
    )


with DAG(
    dag_id="azure_ai_agents_full_lifecycle",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["agentic-airflow", "azure", "ai-agents", "setup"],
) as lifecycle_dag:
    create_agent = CreateAzureAIAgentOperator(
        task_id="create_agent",
        model=MODEL,
        config=CREATE_AGENT_CONFIG,
        azure_ai_agents_conn_id=AZURE_AI_AGENTS_CONN_ID,
        endpoint=ENDPOINT,
    )
    update_agent = UpdateAzureAIAgentOperator(
        task_id="update_agent",
        agent_id=CREATED_AGENT_ID,
        config=UPDATE_AGENT_CONFIG,
        azure_ai_agents_conn_id=AZURE_AI_AGENTS_CONN_ID,
        endpoint=ENDPOINT,
    )
    run_agent = RunAzureAIAgentOperator(
        task_id="run_agent",
        agent_id=CREATED_AGENT_ID,
        config=RUN_AGENT_CONFIG,
        wait_for_completion=True,
        poll_interval=10,
        timeout=900,
        deferrable=True,
        azure_ai_agents_conn_id=AZURE_AI_AGENTS_CONN_ID,
        endpoint=ENDPOINT,
    )
    delete_agent = DeleteAzureAIAgentOperator(
        task_id="delete_agent",
        agent_id=CREATED_AGENT_ID,
        poll_interval=10,
        timeout=900,
        deferrable=True,
        trigger_rule=TriggerRule.ALL_DONE,
        azure_ai_agents_conn_id=AZURE_AI_AGENTS_CONN_ID,
        endpoint=ENDPOINT,
    )

    create_agent >> update_agent >> run_agent >> delete_agent
