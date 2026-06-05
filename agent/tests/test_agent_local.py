from agent import troubleshoot


def test_deterministic_troubleshoot_returns_summary():
    result = troubleshoot({"dag_id": "demo_failing_etl", "run_id": "manual__test"})

    assert result["task_id"] == "transform"
    assert "KeyError" in result["summary"]
    assert "data['rowz']" in result["likely_cause"]
    assert result["slack"]["ok"] is True
