from __future__ import annotations

from typing import Any

import pendulum
from _failure_context import collect_failure_context_payload
from airflow.providers.amazon.aws.operators.bedrock import BedrockInvokeAgentRuntimeOperator
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
    rows = payload["records"]
    return {"order_count": len(rows)}


def collect_failure_context(**context: Any) -> str:
    return collect_failure_context_payload("demo_schema_contract_etl.py", "normalize_orders", **context)


with DAG(
    dag_id="demo_schema_contract_etl",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["agentic-airflow", "demo", "schema"],
):
    extract = PythonOperator(task_id="extract_orders", python_callable=extract_orders)
    normalize = PythonOperator(task_id="normalize_orders", python_callable=normalize_orders)
    publish = EmptyOperator(task_id="publish_orders")

    failure_context = PythonOperator(
        task_id="collect_failure_context",
        python_callable=collect_failure_context,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    troubleshoot = BedrockInvokeAgentRuntimeOperator(
        task_id="troubleshoot_with_agent",
        agent_runtime_arn="{{ var.value.AGENT_RUNTIME_ARN }}",
        payload="{{ ti.xcom_pull(task_ids='collect_failure_context') }}",
        botocore_config={"read_timeout": 300},
    )

    extract >> normalize >> publish
    [extract, normalize, publish] >> failure_context >> troubleshoot
