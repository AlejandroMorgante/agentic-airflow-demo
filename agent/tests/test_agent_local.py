import pytest

from gcp_gemini_agent_platform.agent import _normalize_query_payload
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
        {
            "input": {
                "dag_id": "demo_failing_etl",
                "run_id": "manual__test",
            }
        },
        {
            "input": (
                '{"dag_id": "demo_failing_etl", '
                '"run_id": "manual__test"}'
            )
        },
    ],
)
def test_gcp_query_payload_normalization(payload):
    assert _normalize_query_payload(payload) == {
        "dag_id": "demo_failing_etl",
        "run_id": "manual__test",
    }
