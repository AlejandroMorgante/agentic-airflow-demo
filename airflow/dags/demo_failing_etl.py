from __future__ import annotations

import json

import pendulum
from airflow.providers.amazon.aws.operators.bedrock import BedrockInvokeAgentRuntimeOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

try:
    from airflow.sdk import TriggerRule
except ImportError:
    from airflow.utils.trigger_rule import TriggerRule


def fail_transform() -> dict:
    data = {"rows": 100, "source": "demo"}
    # Intentional demo failure: extract emits "rows", not "rowz".
    return {"transformed": data["rowz"]}


with DAG(
    dag_id="demo_failing_etl",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["agentic-airflow", "demo"],
):
    extract = EmptyOperator(task_id="extract")
    transform = PythonOperator(task_id="transform", python_callable=fail_transform)
    load = EmptyOperator(task_id="load")

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

    extract >> transform >> load
    [extract, transform, load] >> troubleshoot
