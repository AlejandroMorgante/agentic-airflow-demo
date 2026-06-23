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


def extract_orders() -> dict[str, Any]:
    return {
        "batch_id": "orders-2026-06-06",
        "records": [
            {"order_id": "A-100", "amount": "19.95"},
            {"order_id": "A-101", "amount": "24.50"},
        ],
    }


def normalize_orders(**context: Any) -> dict[str, Any]:
    payload = context["ti"].xcom_pull(task_ids="extract_orders")
    rows = payload["rows"]
    return {"order_count": len(rows)}


def collect_failure_context(**context: Any) -> str:
    return collect_failure_context_payload(
        "azure_ai_agents/demo_schema_contract_etl.py",
        "normalize_orders",
        **context,
    )


with DAG(
    dag_id="azure_ai_agents_demo_schema_contract_etl",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["agentic-airflow", "azure", "ai-agents", "demo", "schema"],
):
    extract = PythonOperator(task_id="extract_orders", python_callable=extract_orders)
    normalize = PythonOperator(
        task_id="normalize_orders", python_callable=normalize_orders
    )
    publish = EmptyOperator(task_id="publish_orders")

    failure_context = PythonOperator(
        task_id="collect_failure_context",
        python_callable=collect_failure_context,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    troubleshoot = RunAzureAIAgentOperator(
        task_id="troubleshoot_with_agent",
        agent_id="{{ var.value.AZURE_AI_AGENT_ID }}",
        config={
            "thread": {
                "messages": [
                    {
                        "role": "user",
                        "content": (
                            "Analyze this Airflow failure context and return JSON with "
                            "summary, root_cause, and suggested_fix. Do not claim PR or Slack actions.\n\n"
                            "{{ ti.xcom_pull(task_ids='collect_failure_context') }}"
                        ),
                    }
                ],
            },
        },
        wait_for_completion=True,
        poll_interval=10,
        timeout=900,
        deferrable=True,
        azure_ai_agents_conn_id="{{ var.value.get('AZURE_AI_AGENTS_CONN_ID', 'azure_ai_agents_default') }}",
        endpoint="{{ var.value.get('AZURE_AI_AGENTS_ENDPOINT', '') }}",
    )

    extract >> normalize >> publish
    [extract, normalize, publish] >> failure_context >> troubleshoot
