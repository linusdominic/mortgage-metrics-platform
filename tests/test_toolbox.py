import json

import pytest

from mmp.semantic.layer import SemanticLayer
from mmp.semantic.toolbox import AuditLog, MetricsToolbox, ToolRejected


@pytest.fixture
def toolbox(settings, tmp_path):
    return MetricsToolbox(SemanticLayer(settings), AuditLog(tmp_path / "audit.jsonl"), client="pytest")


def read_log(toolbox):
    return [json.loads(line) for line in toolbox.audit.path.read_text().splitlines()]


def test_every_call_is_logged_including_rejections(toolbox):
    toolbox.list_metrics()
    toolbox.describe_metric("denial_rate")
    toolbox.query_metric(
        "denial_rate", ["state_code"], [{"dimension": "activity_year", "op": "=", "value": 2024}]
    )
    with pytest.raises(ToolRejected, match="not allowed"):
        toolbox.query_metric("denial_rate", ["password"])
    log = read_log(toolbox)
    assert [e["tool"] for e in log] == ["list_metrics", "describe_metric", "query_metric", "query_metric"]
    assert [e["status"] for e in log] == ["ok", "ok", "ok", "rejected"]
    ok = log[2]
    assert ok["row_count"] == 2 and len(ok["sql_sha256"]) == 64 and ok["client"] == "pytest"
    assert ok["arguments"]["filters"] == [{"dimension": "activity_year", "op": "=", "value": 2024}]
    assert "password" in log[3]["error"] and "row_count" not in log[3]
    assert all("request_id" in e and "elapsed_ms" in e and "ts" in e for e in log)


def test_describe_metric_tells_an_agent_what_it_may_ask(toolbox):
    d = toolbox.describe_metric("approval_rate")
    assert d["formula"] == "approval_count / decisioned_application_count"
    assert d["allowed_grains"] == ["total", "year"]
    assert d["limits"]["max_rows"] == 1000
    names = {x["name"] for x in d["dimensions"]}
    assert {"state_code", "lender_name", "activity_year"} <= names
    assert next(x for x in d["dimensions"] if x["name"] == "derived_sex")["demographic"] is True


def test_list_dimension_values_returns_exact_labels(toolbox):
    values = toolbox.list_dimension_values("approval_rate", "loan_type")
    assert {"Conventional", "FHA", "VA"} <= {v["value"] for v in values["values"]}
    with pytest.raises(ToolRejected):
        toolbox.list_dimension_values("approval_rate", "nope")


def test_dispatch_rejects_unknown_tools_and_arguments(toolbox):
    with pytest.raises(ToolRejected, match="unknown tool"):
        toolbox.dispatch("run_sql", {"sql": "select 1"})
    with pytest.raises(ToolRejected, match="bad arguments"):
        toolbox.dispatch("query_metric", {"metric": "denial_rate", "sql": "select 1"})
    assert toolbox.dispatch("get_lineage", {"metric": "denial_rate"})["metric"] == "denial_rate"
