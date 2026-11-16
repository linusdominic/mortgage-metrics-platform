from datetime import UTC, datetime, timedelta

import pytest

from mmp import warehouse
from mmp.quality.gate import QualityGateFailed, run_checks, run_gate

EXPECTED = [(y, s) for y in (2023, 2024) for s in ("DE", "RI")]


def by_name(results):
    return {r.check: r for r in results}


def test_gate_passes_on_a_clean_build(settings):
    results = by_name(run_gate(settings, EXPECTED))
    assert set(results) == {
        "partition_completeness",
        "reconciliation",
        "quarantine_rate",
        "volume_anomaly",
        "freshness",
        "dbt_results",
    }
    assert all(r.status in ("pass", "warn") for r in results.values())
    assert results["reconciliation"].status == "pass" and results["partition_completeness"].status == "pass"
    written = list((settings.telemetry_dir / "dq").rglob("gate-*.json"))
    assert written, "gate results must be persisted"


def test_gate_fails_when_an_expected_partition_is_missing(settings):
    with pytest.raises(QualityGateFailed, match="missing partitions"):
        run_gate(settings, [*EXPECTED, (2024, "WY")])


def test_gate_fails_on_stale_data(settings):
    later = datetime.now(UTC) + timedelta(days=settings.freshness_max_age_days + 5)
    results = by_name(run_checks(settings, EXPECTED, now=later))
    assert results["freshness"].status == "fail"


def test_gate_fails_on_unbalanced_reconciliation_and_volume_collapse(scratch):
    with warehouse.connect(scratch.warehouse_path, read_only=False) as con:
        con.execute(
            "update ops.ops_reconciliation set is_staging_to_fact_balanced = false where state_code = 'DE' and activity_year = 2024"
        )
        con.execute(
            "update ops.ops_volume_anomalies set status = 'fail', pct_change = -93.0 where state_code = 'RI' and activity_year = 2024"
        )
    results = by_name(run_checks(scratch, EXPECTED))
    assert results["reconciliation"].status == "fail" and "(2024, 'DE')" in results["reconciliation"].detail
    assert results["volume_anomaly"].status == "fail"
    with pytest.raises(QualityGateFailed):
        run_gate(scratch, EXPECTED)


def test_gate_requires_dbt_results_unless_the_orchestrator_enforces_them(scratch):
    (scratch.dbt_target_dir / "run_results.json").unlink()
    assert by_name(run_checks(scratch, EXPECTED))["dbt_results"].status == "fail"
    orchestrated = by_name(run_checks(scratch, EXPECTED, check_dbt_results=False))["dbt_results"]
    assert orchestrated.status == "pass" and "task dependencies" in orchestrated.detail
