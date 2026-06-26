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

import sqlite3
from typing import Any

import pendulum
from gcp_gemini_agent_platform._failure_context import collect_failure_context_payload
from airflow.providers.google.cloud.operators.vertex_ai.agent_engine import (
    RunQueryJobOperator,
)
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
        "gcp_gemini_agent_platform/demo_sql_schema_etl.py",
        "build_customer_report",
        **context,
    )


with DAG(
    dag_id="gcp_agentengine_demo_sql_schema_etl",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["agentic-airflow", "gcp", "agentengine", "demo", "sql"],
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

    troubleshoot = RunQueryJobOperator(
        task_id="troubleshoot_with_agent",
        project_id="{{ var.value.GCP_PROJECT_ID }}",
        location="{{ var.value.get('GCP_REGION', 'us-central1') }}",
        agent_engine_id="{{ var.value.GCP_AGENT_ENGINE_NAME.split('/')[-1] }}",
        config={
            "query": "{{ ti.xcom_pull(task_ids='collect_failure_context') }}",
            "output_gcs_uri": "{{ var.value.get('GCP_AGENT_ENGINE_QUERY_OUTPUT_GCS_URI', 'gs://' ~ var.value.GCP_PROJECT_ID ~ '-agent-engine-query-output/query-output/') }}",
        },
        check_config={"retrieve_result": True},
        poll_interval=10,
        timeout=900,
        deferrable=True,
    )

    stage_orders >> report >> publish
    [stage_orders, report, publish] >> failure_context >> troubleshoot
