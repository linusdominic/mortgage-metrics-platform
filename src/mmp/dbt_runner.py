"""Run dbt as a subprocess and turn its artifacts into telemetry.

dbt is invoked through its CLI, not dbtRunner, so a dbt failure cannot corrupt
the calling process and the same code path works from the Makefile, pytest and
an Airflow task.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from mmp import telemetry, warehouse
from mmp.ingest.lar import partition_dir
from mmp.settings import Settings, dbt_env

log = logging.getLogger(__name__)

INCREMENTAL_SCHEMAS = {
    "fct_loan_applications": "marts",
    "fct_application_denial_reasons": "marts",
    "rejected_lar_rows": "quarantine",
    "ops_reconciliation": "ops",
}


class DbtError(RuntimeError):
    pass


@dataclass
class DbtResult:
    returncode: int
    elapsed_s: float
    counts: dict[str, int]
    invocation_id: str | None
    stdout_tail: str


def dbt_executable() -> str:
    candidate = Path(sys.executable).parent / "dbt"
    return str(candidate) if candidate.exists() else "dbt"


def run_dbt(settings: Settings, args: list[str], check: bool = True, quiet: bool = False) -> DbtResult:
    cmd = [
        dbt_executable(),
        *args,
        "--project-dir",
        str(settings.dbt_project_dir),
        "--target",
        settings.dbt_target,
    ]
    log.info("running: %s", " ".join(cmd))
    settings.warehouse_path.parent.mkdir(parents=True, exist_ok=True)
    t0 = time.monotonic()
    proc = subprocess.run(
        cmd, env=dbt_env(settings), capture_output=True, text=True, cwd=settings.dbt_project_dir
    )
    elapsed = time.monotonic() - t0
    if not quiet and os.environ.get("MMP_DBT_QUIET") != "1":
        sys.stdout.write(proc.stdout[-6000:] if len(proc.stdout) > 6000 else proc.stdout)
    counts, invocation_id = _read_run_results(settings)
    if check and proc.returncode != 0:
        tail = (proc.stdout + proc.stderr)[-3000:]
        raise DbtError(f"dbt {' '.join(args)} failed with exit code {proc.returncode}\n{tail}")
    return DbtResult(proc.returncode, elapsed, counts, invocation_id, proc.stdout[-3000:])


def _run_results_path(settings: Settings) -> Path:
    return settings.dbt_target_dir / "run_results.json"


def _read_run_results(settings: Settings) -> tuple[dict[str, int], str | None]:
    path = _run_results_path(settings)
    if not path.exists():
        return {}, None
    payload = json.loads(path.read_text())
    counts: dict[str, int] = {}
    for r in payload.get("results", []):
        counts[r["status"]] = counts.get(r["status"], 0) + 1
    return counts, payload.get("metadata", {}).get("invocation_id")


def build(
    settings: Settings,
    full_refresh: bool = False,
    reprocess_years: list[int] | None = None,
    select: str | None = None,
    check: bool = True,
) -> DbtResult:
    """dbt build + per-node telemetry + saved state for slim CI."""
    args = ["build"]
    if full_refresh:
        args.append("--full-refresh")
    if select:
        args += ["--select", select]
    dbt_vars = {
        "reprocess_years": sorted(reprocess_years or []),
        "volume_warn_pct": settings.volume_warn_pct,
        "volume_fail_pct": settings.volume_fail_pct,
    }
    args += ["--vars", json.dumps(dbt_vars)]
    before = settings.warehouse_path.stat().st_size if settings.warehouse_path.exists() else 0
    with telemetry.step(settings.telemetry_dir, "dbt.invocation", "dbt build") as rec:
        result = run_dbt(settings, args, check=False)
        after = settings.warehouse_path.stat().st_size if settings.warehouse_path.exists() else 0
        rec.bytes_written = max(after - before, 0)
        rec.detail = {
            "args": args[1:],
            "counts": result.counts,
            "invocation_id": result.invocation_id,
            "warehouse_bytes_before": before,
            "warehouse_bytes_after": after,
        }
        if result.returncode != 0:
            rec.status = "failed"
    record_node_telemetry(settings, result.invocation_id)
    if result.returncode != 0:
        if check:
            raise DbtError(f"dbt build failed (exit {result.returncode}); results: {result.counts}")
        return result
    save_state(settings)
    return result


def save_state(settings: Settings) -> None:
    """Keep the manifest of the last good build; slim CI defers to it."""
    src = settings.dbt_target_dir / "manifest.json"
    if src.exists():
        settings.state_dir.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src, settings.state_dir / "manifest.json")


def record_node_telemetry(settings: Settings, invocation_id: str | None) -> int:
    """One telemetry record per dbt node, plus one per partition for incremental models."""
    path = _run_results_path(settings)
    manifest_path = settings.dbt_target_dir / "manifest.json"
    if not path.exists() or not manifest_path.exists():
        return 0
    results = json.loads(path.read_text())["results"]
    nodes = json.loads(manifest_path.read_text())["nodes"]
    run_id, kind = telemetry.current_run_id(), telemetry.run_kind()
    written = 0
    if not settings.warehouse_path.exists():
        return 0
    with warehouse.connect(settings.warehouse_path, read_only=True) as con:
        loaded: dict[str, list[tuple[int, str]]] = {}
        if invocation_id and warehouse.table_exists(con, "ops.partition_load_log"):
            for model, year, state in con.execute(
                "select model_name, activity_year, state_code from ops.partition_load_log where invocation_id = ?",
                [invocation_id],
            ).fetchall():
                loaded.setdefault(model, []).append((year, state))
        for r in results:
            node = nodes.get(r["unique_id"])
            if node is None:
                continue
            resource = node["resource_type"]
            name = node["name"]
            materialized = node.get("config", {}).get("materialized")
            started = (
                next((t.get("started_at") for t in r.get("timing", []) if t.get("name") == "execute"), None)
                or datetime.now(UTC).isoformat()
            )
            rec = telemetry.StepRecord(
                run_id=run_id,
                run_kind=kind,
                step=f"dbt.{resource}",
                target=name,
                status=r["status"],
                started_at=started,
                duration_s=round(r.get("execution_time") or 0.0, 4),
                detail={"materialized": materialized, "invocation_id": invocation_id},
            )
            qualified = f"{node['schema']}.{node.get('alias') or name}"
            is_table = resource in ("model", "seed", "snapshot") and materialized not in ("view", "ephemeral")
            if is_table and warehouse.table_exists(con, qualified):
                total_rows = con.execute(f"select count(*) from {qualified}").fetchone()[0]
                size = warehouse.table_bytes(con, qualified)
                rec.detail["table_rows"] = total_rows
                rec.detail["table_bytes"] = size
                rec.rows_out = total_rows
                rec.bytes_written = size
                if name in INCREMENTAL_SCHEMAS:
                    partitions = loaded.get(name, [])
                    part_rows, scanned = 0, 0
                    per_partition = []
                    for year, state in partitions:
                        n = con.execute(
                            f"select count(*) from {qualified} where activity_year = ? and state_code = ?",
                            [year, state],
                        ).fetchone()[0]
                        parquet = partition_dir(settings.bronze_dir, "hmda_lar", year, state) / "lar.parquet"
                        b = parquet.stat().st_size if parquet.exists() else 0
                        part_rows += n
                        scanned += b
                        per_partition.append((year, state, n, b))
                    rec.rows_out = part_rows
                    rec.bytes_scanned = scanned
                    # Bytes written this run: the table's on-disk size, pro-rated by rows written.
                    rec.bytes_written = int(size * part_rows / total_rows) if size and total_rows else 0
                    rec.detail["partitions_loaded"] = len(partitions)
                    rec.detail["bytes_written_is_estimate"] = True
                    for year, state, n, b in per_partition:
                        share = n / part_rows if part_rows else 0
                        telemetry.write_record(
                            settings.telemetry_dir,
                            telemetry.StepRecord(
                                run_id=run_id,
                                run_kind=kind,
                                step="dbt.partition",
                                target=name,
                                status=r["status"],
                                activity_year=year,
                                state_code=state,
                                started_at=started,
                                # model time allocated to the partition by its share of rows
                                duration_s=round((r.get("execution_time") or 0.0) * share, 4),
                                rows_out=n,
                                bytes_scanned=b,
                                bytes_written=int(rec.bytes_written * share),
                                detail={"allocation": "by_rows"},
                            ),
                        )
                        written += 1
            telemetry.write_record(settings.telemetry_dir, rec)
            written += 1
    return written
