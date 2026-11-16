"""Deterministic evals. No model, no API key, runs in CI.

For every golden question: execute the expected tool call through the same
toolbox the MCP server uses, run the independent reference SQL against the
marts, and require the two to agree. This is the check that the semantic layer
computes what the metric definitions say, on real data, for every metric.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from mmp.evals.golden import compare_rows, load_golden, reference_rows
from mmp.semantic.layer import SemanticLayer
from mmp.semantic.toolbox import MetricsToolbox
from mmp.settings import Settings

ADDITIVE_UNITS = {"count", "usd"}


def run_deterministic(settings: Settings, golden_path: Path | None = None) -> dict[str, Any]:
    layer = SemanticLayer(settings)
    toolbox = MetricsToolbox(layer, audit=None, client="deterministic-eval")
    questions = load_golden(golden_path)
    catalog = layer.catalog()
    results = []
    for q in questions:
        entry: dict[str, Any] = {"id": q.id, "question": q.question, "metric": q.metric}
        try:
            tool_result = toolbox.query_metric(**q.expected_call)
            expected = reference_rows(settings, q)
            unit = catalog[q.metric].unit
            ok, why = compare_rows(
                tool_result["rows"], expected, q.metric, q.compare, additive=unit in ADDITIVE_UNITS
            )
            entry.update(passed=ok, detail=why, rows=tool_result["rows"], reference_rows=expected, unit=unit)
        except Exception as exc:
            entry.update(passed=False, detail=f"{type(exc).__name__}: {exc}")
        results.append(entry)
    passed = sum(r["passed"] for r in results)
    covered = sorted({r["metric"] for r in results})
    return {
        "total": len(results),
        "passed": passed,
        "failed": len(results) - passed,
        "metrics_covered": covered,
        "metrics_not_covered": sorted(set(catalog) - set(covered)),
        "results": results,
    }


def format_report(report: dict[str, Any]) -> str:
    lines = []
    for r in report["results"]:
        lines.append(f"  {'PASS' if r['passed'] else 'FAIL'}  {r['id']}  {r['metric']:<30} {r['detail']}")
    lines.append(
        f"deterministic evals: {report['passed']}/{report['total']} passed; "
        f"{len(report['metrics_covered'])} metrics covered"
        + (f"; not covered: {report['metrics_not_covered']}" if report["metrics_not_covered"] else "")
    )
    return "\n".join(lines)
