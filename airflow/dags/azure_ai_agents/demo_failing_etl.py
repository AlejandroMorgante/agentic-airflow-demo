from __future__ import annotations

from typing import Any

import pendulum
from azure_ai_agents._failure_context import collect_failure_context_payload
from airflow.providers.microsoft.azure.operators.ai_agents import RunAzureAIAgentOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

try:
    from airflow.sdk import TriggerRule
except ImportError:
    from airflow.utils.trigger_rule import TriggerRule


def fail_transform() -> dict:
    data = {"rows": 100, "source": "demo"}
    return {"transformed": data["rowz"]}


def collect_failure_context(**context: Any) -> str:
    return collect_failure_context_payload(
        "azure_ai_agents/demo_failing_etl.py", "transform", **context
    )


with DAG(
    dag_id="azure_ai_agents_demo_failing_etl",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["agentic-airflow", "azure", "ai-agents", "demo"],
):
    extract = EmptyOperator(task_id="extract")
    transform = PythonOperator(task_id="transform", python_callable=fail_transform)
    load = EmptyOperator(task_id="load")

    failure_context = PythonOperator(
        task_id="collect_failure_context",
        python_callable=collect_failure_context,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    troubleshoot = RunAzureAIAgentOperator(
        task_id="troubleshoot_with_agent",
        agent_name="{{ var.value.get('AZURE_AI_AGENT_NAME', 'airflow-troubleshooting-agent') }}",
        protocol="invocations",
        input_data={
            "message": (
                "Analyze this Airflow failure context and return JSON with "
                "summary, root_cause, and suggested_fix. Do not claim PR or Slack actions unless "
                "the hosted agent tools actually performed them.\n\n"
                "{{ ti.xcom_pull(task_ids='collect_failure_context') }}"
            ),
        },
        azure_ai_agents_conn_id="{{ var.value.get('AZURE_AI_AGENTS_CONN_ID', 'azure_ai_agents_default') }}",
        endpoint="{{ var.value.get('AZURE_AI_AGENTS_ENDPOINT', '') }}",
    )

    extract >> transform >> load
    [extract, transform, load] >> failure_context >> troubleshoot
