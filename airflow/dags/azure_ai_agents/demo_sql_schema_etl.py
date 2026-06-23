from __future__ import annotations

import sqlite3
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


def build_customer_report() -> list[tuple[Any, ...]]:
    connection = sqlite3.connect(":memory:")
    connection.execute(
        "create table orders (order_id text, customer_id text, amount real)"
    )
    connection.executemany(
        "insert into orders values (?, ?, ?)",
        [
            ("A-100", "C-001", 19.95),
            ("A-101", "C-002", 24.50),
        ],
    )
    return connection.execute(
        """
        select customer_id, customer_tier, sum(amount) as total_amount
        from orders
        group by customer_id, customer_tier
        """
    ).fetchall()


def collect_failure_context(**context: Any) -> str:
    return collect_failure_context_payload(
        "azure_ai_agents/demo_sql_schema_etl.py",
        "build_customer_report",
        **context,
    )


with DAG(
    dag_id="azure_ai_agents_demo_sql_schema_etl",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["agentic-airflow", "azure", "ai-agents", "demo", "sql"],
):
    stage_orders = EmptyOperator(task_id="stage_orders")
    report = PythonOperator(
        task_id="build_customer_report", python_callable=build_customer_report
    )
    publish = EmptyOperator(task_id="publish_report")

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

    stage_orders >> report >> publish
    [stage_orders, report, publish] >> failure_context >> troubleshoot
