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
"""
Example Airflow DAG for Google Vertex AI Agent Engine operations.
"""

from __future__ import annotations

import json
from datetime import datetime

from airflow.providers.google.cloud.operators.vertex_ai.agent_engine import (
    CheckQueryAgentEngineOperator,
    CreateAgentEngineOperator,
    DeleteAgentEngineOperator,
    GetAgentEngineOperator,
    QueryAgentEngineOperator,
    UpdateAgentEngineOperator,
)

try:
    from airflow.sdk import DAG, TriggerRule
except ImportError:
    from airflow.models.dag import DAG  # type: ignore[attr-defined,no-redef,assignment]
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

DAG_ID = "vertex_ai_agent_engine_operations"
PROJECT_ID = "{{ var.value.GCP_PROJECT_ID }}"
LOCATION = "{{ var.value.get('GCP_REGION', 'us-central1') }}"
DISPLAY_NAME = "{{ var.value.get('GCP_AGENT_ENGINE_DISPLAY_NAME', 'airflow-agent-engine') }}"
CONTAINER_URI = "{{ var.value.GCP_AGENT_ENGINE_CONTAINER_URI }}"
QUERY_OUTPUT_GCS_URI = (
    "{{ var.value.get('GCP_AGENT_ENGINE_QUERY_OUTPUT_GCS_URI', "
    "'gs://' ~ var.value.GCP_PROJECT_ID ~ '-agent-engine-query-output/query-output/') }}"
)

AGENT_ENGINE_ID = (
    "{{ task_instance.xcom_pull(task_ids='create_agent_engine')['name'].split('/')[-1] }}"
)

QUERY_CONFIG = {
    "query": json.dumps(
        {
        "dag_id": "gcp_agentengine_demo_failing_etl",
        "run_id": "manual__agentengine_smoke",
        "dag_file": "gcp_gemini_agent_platform/demo_failing_etl.py",
        "failed_task": {
            "task_id": "transform",
            "state": "failed",
            "try_number": 1,
        },
        "log_excerpt": "KeyError: 'rowz'",
        }
    ),
    "output_gcs_uri": QUERY_OUTPUT_GCS_URI,
}
CHECK_QUERY_CONFIG = {"retrieve_result": True}

with DAG(
    DAG_ID,
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["example", "vertex_ai", "agent_engine", "gcp"],
) as dag:
    # [START how_to_cloud_vertex_ai_create_agent_engine_operator]
    create_agent_engine = CreateAgentEngineOperator(
        task_id="create_agent_engine",
        project_id=PROJECT_ID,
        location=LOCATION,
        config={
            "display_name": DISPLAY_NAME,
            "description": "Airflow system test Agent Engine",
            "agent_framework": "custom",
            "min_instances": 0,
            "max_instances": 1,
            "resource_limits": {"cpu": "1", "memory": "1Gi"},
            "container_spec": {"image_uri": CONTAINER_URI},
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
        },
    )
    # [END how_to_cloud_vertex_ai_create_agent_engine_operator]

    # [START how_to_cloud_vertex_ai_get_agent_engine_operator]
    get_agent_engine = GetAgentEngineOperator(
        task_id="get_agent_engine",
        project_id=PROJECT_ID,
        location=LOCATION,
        agent_engine_id=AGENT_ENGINE_ID,
    )
    # [END how_to_cloud_vertex_ai_get_agent_engine_operator]

    # [START how_to_cloud_vertex_ai_query_agent_engine_operator]
    query_agent_engine = QueryAgentEngineOperator(
        task_id="query_agent_engine",
        project_id=PROJECT_ID,
        location=LOCATION,
        agent_engine_id=AGENT_ENGINE_ID,
        config=QUERY_CONFIG,
    )
    # [END how_to_cloud_vertex_ai_query_agent_engine_operator]

    check_query_agent_engine = CheckQueryAgentEngineOperator(
        task_id="check_query_agent_engine",
        project_id=PROJECT_ID,
        location=LOCATION,
        operation_name="{{ task_instance.xcom_pull(task_ids='query_agent_engine')['job_name'] }}",
        config=CHECK_QUERY_CONFIG,
        poll_interval=10,
        timeout=900,
        deferrable=True,
    )

    # [START how_to_cloud_vertex_ai_update_agent_engine_operator]
    update_agent_engine = UpdateAgentEngineOperator(
        task_id="update_agent_engine",
        project_id=PROJECT_ID,
        location=LOCATION,
        agent_engine_id=AGENT_ENGINE_ID,
        config={
            "display_name": f"{DISPLAY_NAME}-updated",
            "description": "Updated Airflow system test Agent Engine",
        },
    )
    # [END how_to_cloud_vertex_ai_update_agent_engine_operator]

    # [START how_to_cloud_vertex_ai_delete_agent_engine_operator]
    delete_agent_engine = DeleteAgentEngineOperator(
        task_id="delete_agent_engine",
        project_id=PROJECT_ID,
        location=LOCATION,
        agent_engine_id=AGENT_ENGINE_ID,
        force=True,
        deferrable=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END how_to_cloud_vertex_ai_delete_agent_engine_operator]

    (
        create_agent_engine
        >> get_agent_engine
        >> query_agent_engine
        >> check_query_agent_engine
        >> update_agent_engine
        >> delete_agent_engine
    )
