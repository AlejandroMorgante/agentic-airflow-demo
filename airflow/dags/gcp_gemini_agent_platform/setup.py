#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import pendulum
from airflow.providers.google.cloud.operators.vertex_ai.agent_engine import (
    CreateAgentEngineOperator,
    DeleteAgentEngineOperator,
    GetAgentEngineOperator,
    QueryAgentEngineOperator,
    UpdateAgentEngineOperator,
)
from airflow.sdk import DAG

try:
    from airflow.sdk import TriggerRule
except ImportError:
    from airflow.utils.trigger_rule import TriggerRule


DEFAULT_ARGS = {"owner": "airflow"}

PROJECT_ID = "{{ var.value.GCP_PROJECT_ID }}"
LOCATION = "{{ var.value.get('GCP_REGION', 'us-central1') }}"
DISPLAY_NAME = (
    "{{ var.value.get('GCP_AGENT_ENGINE_DISPLAY_NAME', 'airflow-agent-engine') }}"
)
CONTAINER_URI = "{{ var.value.GCP_AGENT_ENGINE_CONTAINER_URI }}"

CREATE_AGENT_ENGINE_CONFIG = {
    "display_name": DISPLAY_NAME,
    "description": "Airflow system test Agent Engine",
    "agent_framework": "custom",
    "min_instances": 0,
    "max_instances": 1,
    "resource_limits": {"cpu": "1", "memory": "1Gi"},
    "container_spec": {
        "image_uri": CONTAINER_URI,
    },
    "env_vars": {
        "GCP_PROJECT": PROJECT_ID,
        "GCP_REGION": LOCATION,
        "GEMINI_MODEL_ID": "{{ var.value.get('GEMINI_MODEL_ID', 'gemini-2.5-pro') }}",
        "GITHUB_REPO": "{{ var.value.GITHUB_REPO }}",
        "GITHUB_REF": "{{ var.value.get('GITHUB_REF', 'main') }}",
        "GITHUB_DAG_PATH": "{{ var.value.get('GITHUB_DAG_PATH', 'airflow/dags') }}",
        "GITHUB_TOKEN": "{{ var.value.GITHUB_TOKEN }}",
        "SLACK_WEBHOOK_URL": "{{ var.value.SLACK_WEBHOOK_URL }}",
        "AGENT_USE_MODEL": "true",
        "AGENT_USE_MOCKS": "false",
    },
    "class_methods": [
        {
            "name": "query",
            "api_mode": "",
        },
    ],
}

UPDATE_AGENT_ENGINE_CONFIG = {
    "display_name": f"{DISPLAY_NAME}-updated",
    "description": "Updated Airflow system test Agent Engine",
}

QUERY_CONFIG = {
    "input": {
        "dag_id": "gcp_agentengine_demo_failing_etl",
        "run_id": "manual__agentengine_smoke",
        "dag_file": "gcp_gemini_agent_platform/demo_failing_etl.py",
        "failed_task": {
            "task_id": "transform",
            "state": "failed",
            "try_number": 1,
        },
        "log_excerpt": "KeyError: 'rowz'",
    },
}


with DAG(
    dag_id="gcp_agentengine_create_runtime",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["agentic-airflow", "gcp", "setup"],
) as create_dag:
    create_agent_engine = CreateAgentEngineOperator(
        task_id="create_agent_engine",
        project_id=PROJECT_ID,
        location=LOCATION,
        config=CREATE_AGENT_ENGINE_CONFIG,
    )


with DAG(
    dag_id="gcp_agentengine_get_runtime",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["agentic-airflow", "gcp", "setup"],
) as get_dag:
    get_agent_engine = GetAgentEngineOperator(
        task_id="get_agent_engine",
        project_id=PROJECT_ID,
        location=LOCATION,
        name="{{ var.value.GCP_AGENT_ENGINE_NAME }}",
    )


with DAG(
    dag_id="gcp_agentengine_query_runtime",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["agentic-airflow", "gcp", "setup"],
) as query_dag:
    query_agent_engine = QueryAgentEngineOperator(
        task_id="query_agent_engine",
        project_id=PROJECT_ID,
        location=LOCATION,
        name="{{ var.value.GCP_AGENT_ENGINE_NAME }}",
        config=QUERY_CONFIG,
    )


with DAG(
    dag_id="gcp_agentengine_update_runtime",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["agentic-airflow", "gcp", "setup"],
) as update_dag:
    update_agent_engine = UpdateAgentEngineOperator(
        task_id="update_agent_engine",
        project_id=PROJECT_ID,
        location=LOCATION,
        name="{{ var.value.GCP_AGENT_ENGINE_NAME }}",
        config=UPDATE_AGENT_ENGINE_CONFIG,
    )


with DAG(
    dag_id="gcp_agentengine_delete_runtime",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["agentic-airflow", "gcp", "setup"],
) as delete_dag:
    delete_agent_engine = DeleteAgentEngineOperator(
        task_id="delete_agent_engine",
        project_id=PROJECT_ID,
        location=LOCATION,
        name="{{ var.value.GCP_AGENT_ENGINE_NAME }}",
        force=True,
        deferrable=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )


with DAG(
    dag_id="gcp_agentengine_full_lifecycle",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["agentic-airflow", "gcp", "setup"],
) as lifecycle_dag:
    create_agent_engine = CreateAgentEngineOperator(
        task_id="create_agent_engine",
        project_id=PROJECT_ID,
        location=LOCATION,
        config=CREATE_AGENT_ENGINE_CONFIG,
    )
    get_agent_engine = GetAgentEngineOperator(
        task_id="get_agent_engine",
        project_id=PROJECT_ID,
        location=LOCATION,
        name="{{ ti.xcom_pull(task_ids='create_agent_engine', key='agent_engine_name') }}",
    )
    query_agent_engine = QueryAgentEngineOperator(
        task_id="query_agent_engine",
        project_id=PROJECT_ID,
        location=LOCATION,
        name="{{ ti.xcom_pull(task_ids='create_agent_engine', key='agent_engine_name') }}",
        config=QUERY_CONFIG,
    )
    update_agent_engine = UpdateAgentEngineOperator(
        task_id="update_agent_engine",
        project_id=PROJECT_ID,
        location=LOCATION,
        name="{{ ti.xcom_pull(task_ids='create_agent_engine', key='agent_engine_name') }}",
        config=UPDATE_AGENT_ENGINE_CONFIG,
    )
    delete_agent_engine = DeleteAgentEngineOperator(
        task_id="delete_agent_engine",
        project_id=PROJECT_ID,
        location=LOCATION,
        name="{{ ti.xcom_pull(task_ids='create_agent_engine', key='agent_engine_name') }}",
        force=True,
        deferrable=True,
    )

    (
        create_agent_engine
        >> get_agent_engine
        >> query_agent_engine
        >> update_agent_engine
        >> delete_agent_engine
    )
