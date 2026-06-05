from __future__ import annotations

import json

import pendulum
from airflow.providers.amazon.aws.operators.bedrock import BedrockInvokeAgentRuntimeOperator
from airflow.sdk import DAG, task

try:
    from airflow.sdk import TriggerRule
except ImportError:
    from airflow.utils.trigger_rule import TriggerRule


with DAG(
    dag_id="demo_failing_etl",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["agentic-airflow", "demo"],
):

    @task
    def extract() -> dict:
        return {"rows": 100, "source": "demo"}

    @task
    def transform(data: dict) -> dict:
        # Intentional demo failure: extract emits "rows", not "rowz".
        return {"transformed": data["rowz"]}

    @task
    def load(data: dict) -> str:
        return f"loaded {data['transformed']}"

    troubleshoot = BedrockInvokeAgentRuntimeOperator(
        task_id="troubleshoot_with_agent",
        agent_runtime_arn="{{ var.value.AGENT_RUNTIME_ARN }}",
        payload=json.dumps(
            {
                "dag_id": "{{ dag.dag_id }}",
                "run_id": "{{ run_id }}",
            }
        ),
        botocore_config={"read_timeout": 300},
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    raw = extract()
    transformed = transform(raw)
    loaded = load(transformed)

    [raw, transformed, loaded] >> troubleshoot
