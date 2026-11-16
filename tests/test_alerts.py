import json
from types import SimpleNamespace

from mmp.orchestration import alerts


def test_callbacks_write_structured_alerts(tmp_path, monkeypatch):
    monkeypatch.setenv("MMP_DATA_DIR", str(tmp_path))
    ti = SimpleNamespace(dag_id="hmda_mortgage_metrics", task_id="ingest", map_index=1, try_number=3)
    alerts.task_retrying({"ti": ti, "run_id": "r1", "exception": RuntimeError("HTTP 503")})
    alerts.task_failed({"ti": ti, "run_id": "r1", "exception": RuntimeError("gave up")})
    alerts.task_failed({"ti": ti, "run_id": "r1"})  # killed by a signal: no exception object
    alerts.dag_failed(
        {"dag": SimpleNamespace(dag_id="hmda_mortgage_metrics"), "run_id": "r1", "reason": "task_failure"}
    )
    alerts.deadline_missed(
        budget_minutes=90, context={"dag_run": {"dag_id": "hmda_mortgage_metrics", "run_id": "r1"}}
    )

    lines = [json.loads(line) for line in (tmp_path / "logs" / "alerts.jsonl").read_text().splitlines()]
    assert [a["kind"] for a in lines] == [
        "task_retry",
        "task_failed",
        "task_failed",
        "dag_run_failed",
        "deadline_missed",
    ]
    assert lines[0]["error"] == "HTTP 503" and lines[1]["task_id"] == "ingest" and lines[1]["map_index"] == 1
    assert lines[2]["error"] is None
    assert lines[3]["reason"] == "task_failure"
    assert lines[4]["budget_minutes"] == 90 and lines[4]["run_id"] == "r1"
