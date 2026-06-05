from __future__ import annotations

from airflow.providers.amazon.aws.operators.bedrock import (
    BedrockCreateAgentRuntimeOperator,
    BedrockDeleteAgentRuntimeOperator,
    BedrockInvokeAgentRuntimeOperator,
)
from airflow.sdk import DAG
from pendulum import datetime


DEFAULT_ARGS = {
    "owner": "airflow",
}


with DAG(
    dag_id="agentcore_create_runtime",
    start_date=datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["agentic-airflow", "agentcore", "setup"],
) as create_dag:
    create_agent_runtime = BedrockCreateAgentRuntimeOperator(
        task_id="create_agent_runtime",
        agent_runtime_name="{{ var.value.get('AGENTCORE_RUNTIME_NAME', 'airflow-troubleshooter') }}",
        agent_runtime_artifact={
            "containerConfiguration": {
                "containerUri": "{{ var.value.AGENT_CONTAINER_URI }}",
            },
        },
        role_arn="{{ var.value.AGENTCORE_EXECUTION_ROLE_ARN }}",
        network_configuration={"networkMode": "PUBLIC"},
        create_agent_runtime_kwargs={
            "environmentVariables": {
                "AIRFLOW_BASE_URL": "{{ var.value.AIRFLOW_BASE_URL }}",
                "AIRFLOW_USERNAME": "{{ var.value.get('AIRFLOW_USERNAME', 'admin') }}",
                "AIRFLOW_PASSWORD": "{{ var.value.get('AIRFLOW_PASSWORD', 'admin') }}",
                "BEDROCK_MODEL_ID": "{{ var.value.BEDROCK_MODEL_ID }}",
                "GITHUB_REPO": "{{ var.value.GITHUB_REPO }}",
                "GITHUB_REF": "{{ var.value.get('GITHUB_REF', 'main') }}",
                "GITHUB_DAG_PATH": "{{ var.value.get('GITHUB_DAG_PATH', 'airflow/dags') }}",
                "GITHUB_TOKEN": "{{ var.value.GITHUB_TOKEN }}",
                "SLACK_WEBHOOK_URL": "{{ var.value.SLACK_WEBHOOK_URL }}",
                "AGENT_USE_MODEL": "true",
                "AGENT_USE_MOCKS": "false",
            },
        },
        wait_for_completion=True,
        waiter_delay=15,
        waiter_max_attempts=40,
        botocore_config={"read_timeout": 300},
    )


with DAG(
    dag_id="agentcore_invoke_smoke",
    start_date=datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["agentic-airflow", "agentcore", "setup"],
) as invoke_dag:
    invoke_agent_runtime = BedrockInvokeAgentRuntimeOperator(
        task_id="invoke_agent_runtime",
        agent_runtime_arn="{{ var.value.AGENT_RUNTIME_ARN }}",
        payload={
            "dag_id": "demo_failing_etl",
            "run_id": "manual__agentcore_smoke",
        },
        botocore_config={"read_timeout": 300},
    )


with DAG(
    dag_id="agentcore_delete_runtime",
    start_date=datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["agentic-airflow", "agentcore", "setup"],
) as delete_dag:
    delete_agent_runtime = BedrockDeleteAgentRuntimeOperator(
        task_id="delete_agent_runtime",
        agent_runtime_id="{{ var.value.AGENT_RUNTIME_ID }}",
    )
