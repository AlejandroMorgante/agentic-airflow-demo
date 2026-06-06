from tools.airflow_api import fetch_failed_tasks, fetch_task_logs
from tools.github_api import create_github_pr, fetch_dag_source
from tools.slack import post_to_slack


class _FakeJsonResponse:
    text = "log body"
    status_code = 200

    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


def test_mock_tools_return_expected_failure_shape(monkeypatch):
    monkeypatch.setenv("AGENT_USE_MOCKS", "true")
    failed = fetch_failed_tasks("demo_failing_etl", "manual__test")

    assert failed[0]["task_id"] == "transform"
    assert failed[0]["try_number"] == 1

    logs = fetch_task_logs("demo_failing_etl", "manual__test", "transform", 1)
    assert "KeyError: 'rowz'" in logs

    source = fetch_dag_source("demo_failing_etl.py")
    assert 'data["rowz"]' in source


def test_mock_slack_returns_payload(monkeypatch):
    monkeypatch.setenv("AGENT_USE_MOCKS", "true")
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


def test_airflow_api_url_encodes_path_parts(monkeypatch):
    calls = []

    def fake_get(url, **kwargs):
        calls.append(url)
        return _FakeJsonResponse({"task_instances": []})

    monkeypatch.delenv("AGENT_USE_MOCKS", raising=False)
    monkeypatch.setenv("AIRFLOW_BASE_URL", "https://airflow.example")
    monkeypatch.setenv("AIRFLOW_USERNAME", "admin")
    monkeypatch.setenv("AIRFLOW_PASSWORD", "admin")
    monkeypatch.setattr("tools.airflow_api.requests.get", fake_get)

    fetch_failed_tasks("demo/failing", "manual__2026-06-05T12:00:00+00:00")

    assert "demo%2Ffailing" in calls[0]
    assert "manual__2026-06-05T12%3A00%3A00%2B00%3A00" in calls[0]


def test_mock_create_github_pr_returns_pr_url(monkeypatch):
    monkeypatch.setenv("AGENT_USE_MOCKS", "true")
    result = create_github_pr(
        filename="demo_failing_etl.py",
        fixed_content='return {"transformed": data["rows"]}',
        pr_title="fix(demo_failing_etl): correct key name",
        pr_body="Root cause: wrong key.",
    )

    assert result["mock"] is True
    assert "pr_url" in result
    assert result["pr_number"] == 1


def test_mock_slack_includes_pr_url(monkeypatch):
    monkeypatch.setenv("AGENT_USE_MOCKS", "true")
    result = post_to_slack(
        dag_id="demo_failing_etl",
        task_id="transform",
        run_id="manual__test",
        what_happened="failed",
        likely_cause="bad key",
        suggested_fix="fix key",
        pr_url="https://github.com/owner/repo/pull/42",
    )

    assert result["ok"] is True
    assert result["message"]["pr_url"] == "https://github.com/owner/repo/pull/42"


def test_github_source_uses_prefix_and_ref(monkeypatch):
    calls = []

    def fake_get(url, **kwargs):
        calls.append((url, kwargs))
        return _FakeJsonResponse({"content": "cHJpbnQoJ29rJykK"})

    monkeypatch.delenv("AGENT_USE_MOCKS", raising=False)
    monkeypatch.setenv("GITHUB_REPO", "owner/repo")
    monkeypatch.setenv("GITHUB_TOKEN", "token")
    monkeypatch.setenv("GITHUB_REF", "feature/demo")
    monkeypatch.setenv("GITHUB_DAG_PATH", "airflow/dags")
    monkeypatch.setattr("tools.github_api.requests.get", fake_get)

    source = fetch_dag_source("demo_failing_etl.py")

    assert source == "print('ok')\n"
    assert calls[0][0].endswith("/repos/owner/repo/contents/airflow/dags/demo_failing_etl.py")
    assert calls[0][1]["params"] == {"ref": "feature/demo"}
