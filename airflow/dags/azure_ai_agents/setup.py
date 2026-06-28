from __future__ import annotations

import json
from typing import Any

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
MODEL_DEPLOYMENT_NAME = "{{ var.value.get('AZURE_AI_AGENTS_MODEL_DEPLOYMENT_NAME', 'gpt-4o') }}"
CONTAINER_IMAGE = "{{ var.value.AZURE_AI_AGENTS_CONTAINER_IMAGE }}"
AGENT_NAME = "{{ var.value.get('AZURE_AI_AGENT_NAME', 'airflow-troubleshooting-agent') }}"

HOSTED_AGENT_DEFINITION: dict[str, Any] = {
    "kind": "hosted",
    "container_configuration": {
        "image": CONTAINER_IMAGE,
    },
    "cpu": "{{ var.value.get('AZURE_AI_AGENTS_CPU', '1') }}",
    "memory": "{{ var.value.get('AZURE_AI_AGENTS_MEMORY', '2Gi') }}",
    "protocol_versions": [
        {"protocol": "invocations", "version": "1.0.0"},
    ],
    "environment_variables": {
        "AZURE_AI_MODEL_DEPLOYMENT_NAME": MODEL_DEPLOYMENT_NAME,
        "AIRFLOW_AGENT_FOUNDRY_PROJECT_ENDPOINT": "{{ var.value.get('AZURE_AI_AGENTS_ENDPOINT', '') }}",
        "AIRFLOW_AGENT_USE_MODEL": "{{ var.value.get('AZURE_AI_AGENTS_USE_MODEL', 'true') }}",
        "AIRFLOW_AGENT_USE_MOCKS": "{{ var.value.get('AGENT_USE_MOCKS', 'false') }}",
        "GITHUB_REPO": "{{ var.value.get('GITHUB_REPO', '') }}",
        "GITHUB_REF": "{{ var.value.get('GITHUB_REF', 'main') }}",
        "GITHUB_DAG_PATH": "{{ var.value.get('GITHUB_DAG_PATH', 'airflow/dags') }}",
        "GITHUB_TOKEN": "{{ var.value.get('GITHUB_TOKEN', '') }}",
        "SLACK_WEBHOOK_URL": "{{ var.value.get('SLACK_WEBHOOK_URL', '') }}",
    },
}

UPDATED_AGENT_DEFINITION: dict[str, Any] = {
    **HOSTED_AGENT_DEFINITION,
    "environment_variables": {
        **HOSTED_AGENT_DEFINITION["environment_variables"],
        "AIRFLOW_AGENT_BEHAVIOR": "troubleshoot-airflow-task-failures",
    },
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

RUN_AGENT_INPUT = {
    "message": (
        "Analyze this Airflow failure context and return JSON with summary, "
        "root_cause, and suggested_fix. Do not claim PR or Slack actions unless "
        "the hosted agent tools actually performed them.\n\n"
        f"{SMOKE_FAILURE_CONTEXT}"
    ),
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
        agent_name=AGENT_NAME,
        definition=HOSTED_AGENT_DEFINITION,
        poll_interval=10,
        timeout=900,
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
        agent_name=AGENT_NAME,
        definition=UPDATED_AGENT_DEFINITION,
        poll_interval=10,
        timeout=900,
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
        agent_name=AGENT_NAME,
        protocol="invocations",
        input_data=RUN_AGENT_INPUT,
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
        agent_name=AGENT_NAME,
        poll_interval=10,
        timeout=900,
        force=True,
        deferrable=False,
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
        agent_name=AGENT_NAME,
        definition=HOSTED_AGENT_DEFINITION,
        poll_interval=10,
        timeout=900,
        azure_ai_agents_conn_id=AZURE_AI_AGENTS_CONN_ID,
        endpoint=ENDPOINT,
    )
    update_agent = UpdateAzureAIAgentOperator(
        task_id="update_agent",
        agent_name=AGENT_NAME,
        definition=UPDATED_AGENT_DEFINITION,
        poll_interval=10,
        timeout=900,
        azure_ai_agents_conn_id=AZURE_AI_AGENTS_CONN_ID,
        endpoint=ENDPOINT,
    )
    run_agent = RunAzureAIAgentOperator(
        task_id="run_agent",
        agent_name=AGENT_NAME,
        protocol="invocations",
        input_data=RUN_AGENT_INPUT,
        azure_ai_agents_conn_id=AZURE_AI_AGENTS_CONN_ID,
        endpoint=ENDPOINT,
    )
    delete_agent = DeleteAzureAIAgentOperator(
        task_id="delete_agent",
        agent_name=AGENT_NAME,
        poll_interval=10,
        timeout=900,
        force=True,
        deferrable=False,
        trigger_rule=TriggerRule.ALL_DONE,
        azure_ai_agents_conn_id=AZURE_AI_AGENTS_CONN_ID,
        endpoint=ENDPOINT,
    )

    create_agent >> update_agent >> run_agent >> delete_agent
