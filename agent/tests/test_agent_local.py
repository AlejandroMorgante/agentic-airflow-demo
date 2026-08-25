import pytest

from gcp_gemini_agent_platform.query import normalize_query_payload, session_id_for
from shared.troubleshooting import troubleshoot


def test_deterministic_troubleshoot_returns_summary(monkeypatch):
    monkeypatch.setenv("AGENT_USE_MOCKS", "true")
    result = troubleshoot({"dag_id": "demo_failing_etl", "run_id": "manual__test"})

    assert result["task_id"] == "transform"
    assert "KeyError" in result["summary"]
    assert "data['rowz']" in result["likely_cause"]
    assert result["slack"]["ok"] is True
    assert "pr_url" in result
    assert result["pr_url"] is not None
    assert result["slack"]["message"]["pr_url"] == result["pr_url"]


def test_deterministic_troubleshoot_rejects_real_data_without_model(monkeypatch):
    monkeypatch.delenv("AGENT_USE_MOCKS", raising=False)

    with pytest.raises(RuntimeError, match="AGENT_USE_MOCKS=true"):
        troubleshoot({"dag_id": "demo_failing_etl", "run_id": "manual__test"})


@pytest.mark.parametrize(
    "payload",
    [
        {"dag_id": "demo_failing_etl", "run_id": "manual__test"},
        '{"dag_id": "demo_failing_etl", "run_id": "manual__test"}',
    ],
)
def test_gcp_query_payload_normalization(payload):
    assert normalize_query_payload(payload) == {
        "dag_id": "demo_failing_etl",
        "run_id": "manual__test",
    }


def test_gcp_session_id_is_stable_and_valid():
    payload = {"dag_id": "Demo_DAG", "run_id": "manual__2026-08-25T10:00:00+00:00"}

    session_id = session_id_for(payload)

    assert session_id == session_id_for(payload)
    assert len(session_id) <= 63
    assert session_id[0].isalpha()
    assert all(char.islower() or char.isdigit() or char == "-" for char in session_id)
