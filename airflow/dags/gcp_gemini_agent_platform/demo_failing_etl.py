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

from typing import Any

import pendulum
from gcp_gemini_agent_platform._failure_context import collect_failure_context_payload
from airflow.providers.google.cloud.operators.vertex_ai.agent_engine import (
    QueryAgentEngineOperator,
)
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

try:
    from airflow.sdk import TriggerRule
except ImportError:
    from airflow.utils.trigger_rule import TriggerRule


def fail_transform() -> dict:
    data = {"rows": 100, "source": "demo"}
    return {"transformed": data["rows"]}


def collect_failure_context(**context: Any) -> str:
    return collect_failure_context_payload(
        "gcp_gemini_agent_platform/demo_failing_etl.py", "transform", **context
    )


with DAG(
    dag_id="gcp_agentengine_demo_failing_etl",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["agentic-airflow", "gcp", "agentengine", "demo"],
):
    extract = EmptyOperator(task_id="extract")
    transform = PythonOperator(task_id="transform", python_callable=fail_transform)
    load = EmptyOperator(task_id="load")

    failure_context = PythonOperator(
        task_id="collect_failure_context",
        python_callable=collect_failure_context,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    troubleshoot = QueryAgentEngineOperator(
        task_id="troubleshoot_with_agent",
        project_id="{{ var.value.GCP_PROJECT_ID }}",
        location="{{ var.value.get('GCP_REGION', 'us-central1') }}",
        name="{{ var.value.GCP_AGENT_ENGINE_NAME }}",
        config={"input": "{{ ti.xcom_pull(task_ids='collect_failure_context') }}"},
    )

    extract >> transform >> load
    [extract, transform, load] >> failure_context >> troubleshoot
