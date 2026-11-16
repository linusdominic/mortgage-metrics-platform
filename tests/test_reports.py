import json

from mmp import telemetry, warehouse
from mmp.publish import catalog_markdown, publish
from mmp.report.build import build_report
from mmp.report.cost import cost_report, load_run_metrics
from mmp.semantic.layer import SemanticLayer


def test_telemetry_step_records_success_and_failure(tmp_path):
    with telemetry.step(tmp_path, "unit.ok", "thing", activity_year=2024, state_code="DE") as rec:
        rec.rows_out = 5
    try:
        with telemetry.step(tmp_path, "unit.bad", "thing"):
            raise RuntimeError("boom")
    except RuntimeError:
        pass
    records = sorted((json.loads(p.read_text()) for p in tmp_path.rglob("*.json")), key=lambda r: r["step"])
    assert [(r["step"], r["status"]) for r in records] == [("unit.bad", "failed"), ("unit.ok", "success")]
    assert records[1]["rows_out"] == 5 and records[1]["state_code"] == "DE" and records[1]["duration_s"] >= 0
    assert "boom" in records[0]["detail"]["error"]


def test_run_metrics_cover_ingestion_models_and_partitions(scratch):
    counts = load_run_metrics(scratch)
    assert counts["run_metrics"] > 50
    with warehouse.connect(scratch.warehouse_path) as con:
        steps = dict(con.execute("select step, count(*) from ops.run_metrics group by 1").fetchall())
        fact = con.execute(
            "select rows_out, bytes_scanned, bytes_written, duration_s from ops.run_metrics "
            "where step = 'dbt.model' and target = 'fct_loan_applications'"
        ).fetchone()
        partitions = con.execute(
            "select count(*), sum(rows_out) from ops.run_metrics where step = 'dbt.partition' and target = 'fct_loan_applications'"
        ).fetchone()
        total = con.execute("select count(*) from marts.fct_loan_applications").fetchone()[0]
    assert {
        "ingest.download",
        "ingest.convert",
        "dbt.invocation",
        "dbt.model",
        "dbt.test",
        "dbt.partition",
    } <= set(steps)
    assert fact[0] == total and fact[1] > 0 and fact[2] > 0 and fact[3] > 0
    assert partitions == (4, total), "each loaded partition is attributed, and the rows add up"


def test_cost_report_renders_all_sections(scratch):
    load_run_metrics(scratch)
    text = cost_report(scratch)
    for heading in ("## Runs", "## Steps, run", "## dbt models by time", "## Per partition"):
        assert heading in text
    assert "fct_loan_applications" in text and "| 2024 | DE" in text


def test_publish_writes_catalogue_and_snapshot_from_the_semantic_layer(scratch):
    paths = publish(scratch)
    catalog = json.loads(paths["catalog"].read_text())["metrics"]
    layer = SemanticLayer(scratch)
    assert {m["name"] for m in catalog} == set(layer.catalog())
    lines = paths["snapshot"].read_text().splitlines()
    assert lines[0] == "metric,unit,activity_year,state_code,value"
    assert len(lines) - 1 == len(catalog) * 4  # 2 years x 2 states
    assert paths["semantic_manifest"].exists()
    assert "| `approval_rate` | ratio |" in catalog_markdown(layer)


def test_report_builds_five_charts_and_a_page(scratch, tmp_path):
    index = build_report(scratch, tmp_path / "report")
    pngs = sorted(p.name for p in index.parent.glob("*.png"))
    assert pngs == [
        "approval_rate_by_state.png",
        "denial_reasons.png",
        "rate_over_pmms.png",
        "rate_spread_distribution.png",
        "top_lenders.png",
    ]
    assert all((index.parent / p).stat().st_size > 5000 for p in pngs)
    html = index.read_text()
    assert html.count("<img ") == 5 and html.count("<table>") == 5 and "not loan performance" in html
