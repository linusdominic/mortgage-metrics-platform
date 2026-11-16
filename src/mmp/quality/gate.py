"""Data quality gate.

Runs after dbt and before anything is published. dbt has already enforced
contracts and tests inside the build; the gate is the orchestration-level check
that the build that just ran is fit to publish: every expected partition
arrived, every hop reconciles, volumes and freshness are within policy, and the
last dbt invocation had no errors. Any `fail` exits non-zero, which is what
stops the Airflow DAG before the publish task.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

from mmp import telemetry, warehouse
from mmp.settings import Settings

QUARANTINE_FAIL_PCT = 1.0


@dataclass
class CheckResult:
    check: str
    status: str  # pass | warn | fail
    detail: str
    observed: float | None = None
    threshold: float | None = None


class QualityGateFailed(RuntimeError):
    def __init__(self, results: list[CheckResult]):
        self.results = results
        failed = [r for r in results if r.status == "fail"]
        super().__init__("; ".join(f"{r.check}: {r.detail}" for r in failed))


def run_checks(
    settings: Settings,
    expected_partitions: list[tuple[int, str]],
    now: datetime | None = None,
    check_dbt_results: bool = True,
) -> list[CheckResult]:
    now = now or datetime.now(UTC)
    results: list[CheckResult] = []
    with warehouse.connect(settings.warehouse_path, read_only=True) as con:
        # 1. completeness: every expected partition is in the fact
        present = set(
            con.execute(
                "select distinct activity_year, state_code from marts.fct_loan_applications"
            ).fetchall()
        )
        missing = sorted(set(expected_partitions) - present)
        results.append(
            CheckResult(
                "partition_completeness",
                "fail" if missing else "pass",
                f"missing partitions: {missing}"
                if missing
                else f"{len(expected_partitions)} expected partitions present",
                observed=len(missing),
                threshold=0,
            )
        )

        # 2. reconciliation across raw -> bronze -> staging -> fact
        recon = con.execute(
            "select activity_year, state_code, is_raw_to_bronze_balanced, is_bronze_to_staging_balanced, "
            "is_staging_to_fact_balanced from ops.ops_reconciliation"
        ).fetchall()
        unbalanced = [(y, s) for y, s, a, b, c in recon if not (a and b and c)]
        unreconciled = sorted(set(expected_partitions) - {(y, s) for y, s, *_ in recon})
        bad = unbalanced + unreconciled
        results.append(
            CheckResult(
                "reconciliation",
                "fail" if bad else "pass",
                f"unbalanced or unreconciled partitions: {bad}"
                if bad
                else f"{len(recon)} partitions balanced across all hops",
                observed=len(bad),
                threshold=0,
            )
        )

        # 3. quarantine rate
        q_rows, b_rows = con.execute(
            "select coalesce(sum(quarantined_rows), 0), coalesce(sum(bronze_rows), 0) from ops.ops_reconciliation"
        ).fetchone()
        pct = 100.0 * q_rows / b_rows if b_rows else 0.0
        status = "fail" if pct > QUARANTINE_FAIL_PCT else ("warn" if q_rows else "pass")
        results.append(
            CheckResult(
                "quarantine_rate",
                status,
                f"{q_rows} of {b_rows} rows quarantined ({pct:.3f}%)",
                observed=round(pct, 4),
                threshold=QUARANTINE_FAIL_PCT,
            )
        )

        # 4. volume anomalies
        anomalies = con.execute(
            "select activity_year, state_code, pct_change, status from ops.ops_volume_anomalies "
            "where status in ('warn', 'fail') order by 1, 2"
        ).fetchall()
        worst = "fail" if any(a[3] == "fail" for a in anomalies) else ("warn" if anomalies else "pass")
        results.append(
            CheckResult(
                "volume_anomaly",
                worst,
                "; ".join(f"{y}/{s} {p:+.1f}% ({st})" for y, s, p, st in anomalies)
                or f"no year-over-year change beyond {settings.volume_warn_pct:g}%",
                observed=max((abs(a[2]) for a in anomalies), default=0.0),
                threshold=settings.volume_fail_pct,
            )
        )

        # 5. freshness of the landed LAR files
        oldest, newest = con.execute(
            "select min(fetched_at), max(fetched_at) from staging.stg_ops__ingest_manifest where dataset = 'hmda_lar'"
        ).fetchone()
        if newest is None:
            results.append(CheckResult("freshness", "fail", "no LAR files in the manifest"))
        else:
            age_days = (now - newest).total_seconds() / 86400
            results.append(
                CheckResult(
                    "freshness",
                    "fail" if age_days > settings.freshness_max_age_days else "pass",
                    f"newest LAR file fetched {age_days:.2f} days ago (oldest {oldest:%Y-%m-%d})",
                    observed=round(age_days, 3),
                    threshold=settings.freshness_max_age_days,
                )
            )

    # 6. the dbt invocation that produced this state
    run_results = settings.dbt_target_dir / "run_results.json"
    if not check_dbt_results:
        # Under Airflow each dbt node is its own task and the gate is downstream of all of
        # them, so it only runs if they succeeded; there is no single run_results.json to read.
        results.append(CheckResult("dbt_results", "pass", "enforced by task dependencies (orchestrated run)"))
    elif run_results.exists():
        statuses = [r["status"] for r in json.loads(run_results.read_text())["results"]]
        errors = sum(s in ("error", "fail", "runtime error") for s in statuses)
        warns = sum(s == "warn" for s in statuses)
        results.append(
            CheckResult(
                "dbt_results",
                "fail" if errors else ("warn" if warns else "pass"),
                f"{len(statuses)} nodes: {errors} failed, {warns} warned",
                observed=errors,
                threshold=0,
            )
        )
    else:
        results.append(CheckResult("dbt_results", "fail", "no run_results.json; dbt has not run"))
    return results


def write_results(settings: Settings, results: list[CheckResult]) -> Path:
    out_dir = settings.telemetry_dir / "dq" / f"run_id={telemetry.current_run_id()}"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"gate-{datetime.now(UTC):%Y%m%dT%H%M%S%f}.json"
    checked_at = datetime.now(UTC).isoformat()
    path.write_text(
        json.dumps(
            [{**asdict(r), "run_id": telemetry.current_run_id(), "checked_at": checked_at} for r in results]
        )
    )
    return path


def run_gate(
    settings: Settings, expected_partitions: list[tuple[int, str]], check_dbt_results: bool = True
) -> list[CheckResult]:
    with telemetry.step(settings.telemetry_dir, "quality.gate", "quality gate") as rec:
        results = run_checks(settings, expected_partitions, check_dbt_results=check_dbt_results)
        write_results(settings, results)
        rec.detail = {r.check: r.status for r in results}
        if any(r.status == "fail" for r in results):
            raise QualityGateFailed(results)
    return results


def format_results(results: list[CheckResult]) -> str:
    width = max(len(r.check) for r in results)
    return "\n".join(f"  {r.status.upper():4s}  {r.check:<{width}}  {r.detail}" for r in results)
