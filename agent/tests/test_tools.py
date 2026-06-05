from tools.airflow_api import fetch_failed_tasks, fetch_task_logs
from tools.github_api import fetch_dag_source
from tools.slack import post_to_slack


def test_mock_tools_return_expected_failure_shape():
    failed = fetch_failed_tasks("demo_failing_etl", "manual__test")

    assert failed[0]["task_id"] == "transform"
    assert failed[0]["try_number"] == 1

    logs = fetch_task_logs("demo_failing_etl", "manual__test", "transform", 1)
    assert "KeyError: 'rowz'" in logs

    source = fetch_dag_source("demo_failing_etl.py")
    assert 'data["rowz"]' in source


def test_mock_slack_returns_payload():
    result = post_to_slack(
        dag_id="demo_failing_etl",
        task_id="transform",
        run_id="manual__test",
        what_happened="failed",
        likely_cause="bad key",
        suggested_fix="fix key",
    )

    assert result["ok"] is True
    assert result["mock"] is True
    assert result["message"]["task_id"] == "transform"
