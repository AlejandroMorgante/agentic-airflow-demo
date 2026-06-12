from __future__ import annotations

import os
from typing import Any

import pendulum
from aws_agentcore._failure_context import collect_failure_context_payload
from airflow.providers.amazon.aws.operators.bedrock import (
    BedrockInvokeAgentRuntimeOperator,
)
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

try:
    from airflow.sdk import TriggerRule
except ImportError:
    from airflow.utils.trigger_rule import TriggerRule


def resolve_target_table() -> dict[str, str]:
    schema = os.environ["WAREHOUSE_SCHEMA"]
    return {"target_table": f"{schema}.daily_order_summary"}


def collect_failure_context(**context: Any) -> str:
    return collect_failure_context_payload(
        "aws_agentcore/demo_missing_config_etl.py", "resolve_target_table", **context
    )


with DAG(
    dag_id="aws_agentcore_demo_missing_config_etl",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["agentic-airflow", "aws", "agentcore", "demo", "config"],
):
    start = EmptyOperator(task_id="start")
    resolve_config = PythonOperator(
        task_id="resolve_target_table", python_callable=resolve_target_table
    )
    load_summary = EmptyOperator(task_id="load_summary")

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

    start >> resolve_config >> load_summary
    [start, resolve_config, load_summary] >> failure_context >> troubleshoot
