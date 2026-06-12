from __future__ import annotations

from airflow.providers.amazon.aws.operators.bedrock import (
    BedrockCreateAgentRuntimeOperator,
    BedrockDeleteAgentRuntimeOperator,
    BedrockInvokeAgentRuntimeOperator,
)
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from pendulum import datetime

try:
    from airflow.sdk import TriggerRule
except ImportError:
    from airflow.utils.trigger_rule import TriggerRule


DEFAULT_ARGS = {
    "owner": "airflow",
}


def extract_agentcore_runtime_id(agent_runtime_arn: str) -> str:
    return agent_runtime_arn.rsplit("/", maxsplit=1)[-1]


AGENTCORE_CREATE_KWARGS = {
    "agent_runtime_name": "{{ var.value.get('AGENTCORE_RUNTIME_NAME', 'airflow-troubleshooter') }}",
    "agent_runtime_artifact": {
        "containerConfiguration": {
            "containerUri": "{{ var.value.AGENT_CONTAINER_URI }}",
        },
    },
    "role_arn": "{{ var.value.AGENTCORE_EXECUTION_ROLE_ARN }}",
    "network_configuration": {"networkMode": "PUBLIC"},
    "create_agent_runtime_kwargs": {
        "environmentVariables": {
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
    "wait_for_completion": True,
    "waiter_delay": 15,
    "waiter_max_attempts": 40,
    "botocore_config": {"read_timeout": 300},
}

AGENTCORE_INVOKE_PAYLOAD = {
    "dag_id": "aws_agentcore_demo_failing_etl",
    "run_id": "manual__agentcore_smoke",
    "dag_file": "aws_agentcore/demo_failing_etl.py",
    "failed_task": {
        "task_id": "transform",
        "state": "failed",
        "try_number": 1,
    },
    "log_excerpt": "KeyError: 'rowz'",
}


with DAG(
    dag_id="aws_agentcore_create_runtime",
    start_date=datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["agentic-airflow", "agentcore", "setup"],
) as create_dag:
    create_agent_runtime = BedrockCreateAgentRuntimeOperator(
        task_id="create_agent_runtime",
        **AGENTCORE_CREATE_KWARGS,
    )


with DAG(
    dag_id="aws_agentcore_invoke_runtime",
    start_date=datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["agentic-airflow", "agentcore", "setup"],
) as invoke_dag:
    invoke_agent_runtime = BedrockInvokeAgentRuntimeOperator(
        task_id="invoke_agent_runtime",
        agent_runtime_arn="{{ var.value.AGENT_RUNTIME_ARN }}",
        payload=AGENTCORE_INVOKE_PAYLOAD,
        botocore_config={"read_timeout": 300},
    )


with DAG(
    dag_id="aws_agentcore_delete_runtime",
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


with DAG(
    dag_id="aws_agentcore_full_lifecycle",
    start_date=datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["agentic-airflow", "aws", "agentcore", "setup"],
) as lifecycle_dag:
    create_agent_runtime = BedrockCreateAgentRuntimeOperator(
        task_id="create_agent_runtime",
        **AGENTCORE_CREATE_KWARGS,
    )
    invoke_agent_runtime = BedrockInvokeAgentRuntimeOperator(
        task_id="invoke_agent_runtime",
        agent_runtime_arn="{{ ti.xcom_pull(task_ids='create_agent_runtime') }}",
        payload=AGENTCORE_INVOKE_PAYLOAD,
        botocore_config={"read_timeout": 300},
    )
    extract_runtime_id = PythonOperator(
        task_id="extract_runtime_id",
        python_callable=extract_agentcore_runtime_id,
        op_args=["{{ ti.xcom_pull(task_ids='create_agent_runtime') }}"],
    )
    delete_agent_runtime = BedrockDeleteAgentRuntimeOperator(
        task_id="delete_agent_runtime",
        agent_runtime_id="{{ ti.xcom_pull(task_ids='extract_runtime_id') }}",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_agent_runtime >> [invoke_agent_runtime, extract_runtime_id]
    [invoke_agent_runtime, extract_runtime_id] >> delete_agent_runtime
