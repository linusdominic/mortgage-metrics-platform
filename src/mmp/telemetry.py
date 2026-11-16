"""Run telemetry.

Every pipeline step writes one JSON record: what ran, for which partition, how
long it took, rows in/out and bytes scanned/written. Records are files, one per
step, because ingestion steps run in parallel under Airflow and DuckDB allows a
single writer; `load_run_metrics` folds them into `ops.run_metrics` at the end
of a run.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path


def new_run_id() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]


def current_run_id() -> str:
    """One id per pipeline run. Airflow passes its own run id via MMP_RUN_ID."""
    run_id = os.environ.get("MMP_RUN_ID")
    if not run_id:
        run_id = new_run_id()
        os.environ["MMP_RUN_ID"] = run_id
    return run_id


@dataclass
class StepRecord:
    run_id: str
    run_kind: str  # full_refresh | incremental | backfill | adhoc
    step: str  # e.g. ingest.download, ingest.convert, dbt.model, quality.gate
    target: str  # model name, source name or step name
    status: str = "success"
    activity_year: int | None = None
    state_code: str | None = None
    started_at: str = ""
    duration_s: float = 0.0
    rows_in: int | None = None
    rows_out: int | None = None
    bytes_scanned: int | None = None
    bytes_written: int | None = None
    detail: dict = field(default_factory=dict)


def run_kind() -> str:
    return os.environ.get("MMP_RUN_KIND", "adhoc")


def write_record(telemetry_dir: Path, record: StepRecord) -> Path:
    out_dir = telemetry_dir / f"run_id={record.run_id}"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{record.step}-{uuid.uuid4().hex[:10]}.json"
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(asdict(record), default=str))
    tmp.replace(path)
    return path


@contextmanager
def step(telemetry_dir: Path, step_name: str, target: str, **fields):
    """Time a step and persist its record, including on failure."""
    record = StepRecord(
        run_id=current_run_id(),
        run_kind=run_kind(),
        step=step_name,
        target=target,
        started_at=datetime.now(UTC).isoformat(),
        **fields,
    )
    t0 = time.monotonic()
    try:
        yield record
    except BaseException as exc:
        record.status = "failed"
        record.detail = {**record.detail, "error": f"{type(exc).__name__}: {exc}"[:500]}
        raise
    finally:
        record.duration_s = round(time.monotonic() - t0, 4)
        write_record(telemetry_dir, record)
