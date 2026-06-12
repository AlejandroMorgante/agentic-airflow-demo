from shared.tools.airflow_api import fetch_failed_tasks, fetch_task_logs
from shared.tools.github_api import create_github_pr, fetch_dag_source
from shared.tools.slack import post_to_slack


class _FakeJsonResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code
        self.text = "log body"

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")
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
    monkeypatch.setattr("shared.tools.airflow_api.requests.get", fake_get)

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
    monkeypatch.setattr("shared.tools.github_api.requests.get", fake_get)

    source = fetch_dag_source("demo_failing_etl.py")

    assert source == "print('ok')\n"
    assert calls[0][0].endswith("/repos/owner/repo/contents/airflow/dags/demo_failing_etl.py")
    assert calls[0][1]["params"] == {"ref": "feature/demo"}


def test_github_source_rejects_paths_outside_dag_prefix(monkeypatch):
    monkeypatch.delenv("AGENT_USE_MOCKS", raising=False)
    monkeypatch.setenv("GITHUB_REPO", "owner/repo")
    monkeypatch.setenv("GITHUB_DAG_PATH", "airflow/dags")

    source = fetch_dag_source(".github/workflows/deploy.yml")

    assert source.startswith("ERROR fetching DAG source")
    assert "filename must be under airflow/dags" in source


def test_create_github_pr_uses_draft_pr_and_deterministic_branch(monkeypatch):
    get_calls = []
    post_calls = []
    put_calls = []
    fixed_content = "print('fixed')\n"

    def fake_get(url, **kwargs):
        get_calls.append((url, kwargs))
        if url.endswith("/pulls"):
            return _FakeJsonResponse([])
        if url.endswith("/git/ref/heads/main"):
            return _FakeJsonResponse({"object": {"sha": "base-sha"}})
        if "/contents/airflow/dags/demo_failing_etl.py" in url:
            return _FakeJsonResponse({"sha": "file-sha"})
        raise AssertionError(f"unexpected GET {url}")

    def fake_post(url, **kwargs):
        post_calls.append((url, kwargs))
        if url.endswith("/git/refs"):
            return _FakeJsonResponse({"ref": kwargs["json"]["ref"]})
        if url.endswith("/pulls"):
            return _FakeJsonResponse({"html_url": "https://github.com/owner/repo/pull/42", "number": 42})
        raise AssertionError(f"unexpected POST {url}")

    def fake_put(url, **kwargs):
        put_calls.append((url, kwargs))
        return _FakeJsonResponse({"content": {"sha": "new-sha"}})

    monkeypatch.delenv("AGENT_USE_MOCKS", raising=False)
    monkeypatch.setenv("GITHUB_REPO", "owner/repo")
    monkeypatch.setenv("GITHUB_REF", "main")
    monkeypatch.setenv("GITHUB_DAG_PATH", "airflow/dags")
    monkeypatch.setattr("shared.tools.github_api.requests.get", fake_get)
    monkeypatch.setattr("shared.tools.github_api.requests.post", fake_post)
    monkeypatch.setattr("shared.tools.github_api.requests.put", fake_put)

    result = create_github_pr(
        filename="demo_failing_etl.py",
        fixed_content=fixed_content,
        pr_title="fix(demo_failing_etl): correct key name",
        pr_body="Root cause: wrong key.",
    )

    branch = result["branch"]
    assert result["pr_url"] == "https://github.com/owner/repo/pull/42"
    assert branch.startswith("agent/fix-demo_failing_etl-")
    assert get_calls[0][1]["params"] == {"state": "open", "head": f"owner:{branch}", "base": "main"}
    assert post_calls[0][1]["json"] == {"ref": f"refs/heads/{branch}", "sha": "base-sha"}
    assert put_calls[0][1]["json"]["branch"] == branch
    assert post_calls[1][1]["json"]["draft"] is True


def test_create_github_pr_returns_existing_pr_without_new_branch(monkeypatch):
    post_calls = []

    def fake_get(url, **kwargs):
        if url.endswith("/pulls"):
            return _FakeJsonResponse([{"html_url": "https://github.com/owner/repo/pull/42", "number": 42}])
        raise AssertionError(f"unexpected GET {url}")

    def fake_post(url, **kwargs):
        post_calls.append((url, kwargs))
        return _FakeJsonResponse({})

    monkeypatch.delenv("AGENT_USE_MOCKS", raising=False)
    monkeypatch.setenv("GITHUB_REPO", "owner/repo")
    monkeypatch.setenv("GITHUB_REF", "main")
    monkeypatch.setenv("GITHUB_DAG_PATH", "airflow/dags")
    monkeypatch.setattr("shared.tools.github_api.requests.get", fake_get)
    monkeypatch.setattr("shared.tools.github_api.requests.post", fake_post)

    result = create_github_pr(
        filename="demo_failing_etl.py",
        fixed_content="print('fixed')\n",
        pr_title="fix(demo_failing_etl): correct key name",
        pr_body="Root cause: wrong key.",
    )

    assert result["existing"] is True
    assert result["pr_number"] == 42
    assert post_calls == []


def test_create_github_pr_cleans_created_branch_on_pr_failure(monkeypatch):
    deleted = []

    def fake_get(url, **kwargs):
        if url.endswith("/pulls"):
            return _FakeJsonResponse([])
        if url.endswith("/git/ref/heads/main"):
            return _FakeJsonResponse({"object": {"sha": "base-sha"}})
        if "/contents/airflow/dags/demo_failing_etl.py" in url:
            return _FakeJsonResponse({"sha": "file-sha"})
        raise AssertionError(f"unexpected GET {url}")

    def fake_post(url, **kwargs):
        if url.endswith("/git/refs"):
            return _FakeJsonResponse({"ref": kwargs["json"]["ref"]})
        if url.endswith("/pulls"):
            return _FakeJsonResponse({"message": "validation failed"}, status_code=422)
        raise AssertionError(f"unexpected POST {url}")

    def fake_put(url, **kwargs):
        return _FakeJsonResponse({"content": {"sha": "new-sha"}})

    def fake_delete(url, **kwargs):
        deleted.append(url)
        return _FakeJsonResponse({}, status_code=204)

    monkeypatch.delenv("AGENT_USE_MOCKS", raising=False)
    monkeypatch.setenv("GITHUB_REPO", "owner/repo")
    monkeypatch.setenv("GITHUB_REF", "main")
    monkeypatch.setenv("GITHUB_DAG_PATH", "airflow/dags")
    monkeypatch.setattr("shared.tools.github_api.requests.get", fake_get)
    monkeypatch.setattr("shared.tools.github_api.requests.post", fake_post)
    monkeypatch.setattr("shared.tools.github_api.requests.put", fake_put)
    monkeypatch.setattr("shared.tools.github_api.requests.delete", fake_delete)

    result = create_github_pr(
        filename="demo_failing_etl.py",
        fixed_content="print('fixed')\n",
        pr_title="fix(demo_failing_etl): correct key name",
        pr_body="Root cause: wrong key.",
    )

    assert "HTTP 422" in result["error"]
    assert deleted[0].startswith("https://api.github.com/repos/owner/repo/git/refs/heads/agent/fix-demo_failing_etl-")
