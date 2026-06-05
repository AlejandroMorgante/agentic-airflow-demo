from __future__ import annotations

import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator


with DAG(
    dag_id="smoke_dummy",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["smoke"],
):
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    start >> end
