"""Airflow callbacks.

In a real deployment these would page or post to a channel. Here they write a
structured line to the task log and to <data>/logs/alerts.jsonl, which is
enough to show that the hooks fire and what they carry.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

log = logging.getLogger("mmp.alerts")


def _alerts_path() -> Path:
    data_dir = Path(os.environ.get("MMP_DATA_DIR", "data"))
    path = data_dir / "logs" / "alerts.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def emit(kind: str, **fields: Any) -> dict[str, Any]:
    alert = {"ts": datetime.now(UTC).isoformat(), "kind": kind, **fields}
    log.error("ALERT %s", json.dumps(alert, default=str, sort_keys=True))
    with _alerts_path().open("a") as fh:
        fh.write(json.dumps(alert, default=str, sort_keys=True) + "\n")
    return alert


def _get(obj: Any, name: str) -> Any:
    return getattr(obj, name, None) if obj is not None else None


def _error(context: dict[str, Any]) -> str | None:
    """The exception if Airflow passed one. A task killed by a signal (OOM) has none."""
    exc = context.get("exception")
    return str(exc)[:500] if exc else None


def task_failed(context: dict[str, Any]) -> None:
    """on_failure_callback for every task: fires once retries are exhausted."""
    ti = context.get("ti") or context.get("task_instance")
    emit(
        "task_failed",
        dag_id=_get(ti, "dag_id"),
        task_id=_get(ti, "task_id"),
        map_index=_get(ti, "map_index"),
        run_id=context.get("run_id"),
        try_number=_get(ti, "try_number"),
        error=_error(context),
    )


def task_retrying(context: dict[str, Any]) -> None:
    """on_retry_callback: recorded so flaky upstreams are visible before they fail a run."""
    ti = context.get("ti") or context.get("task_instance")
    emit(
        "task_retry",
        dag_id=_get(ti, "dag_id"),
        task_id=_get(ti, "task_id"),
        map_index=_get(ti, "map_index"),
        run_id=context.get("run_id"),
        try_number=_get(ti, "try_number"),
        error=_error(context),
    )


def dag_failed(context: dict[str, Any]) -> None:
    """DAG-level on_failure_callback."""
    emit(
        "dag_run_failed",
        dag_id=_get(context.get("dag"), "dag_id"),
        run_id=context.get("run_id"),
        reason=context.get("reason"),
    )


def deadline_missed(**kwargs: Any) -> None:
    """DeadlineAlert callback: the run did not finish within the allowed time after it was queued."""
    context = kwargs.get("context") or {}
    dag_run = context.get("dag_run") if isinstance(context, dict) else None
    emit(
        "deadline_missed",
        dag_id=(dag_run or {}).get("dag_id") if isinstance(dag_run, dict) else _get(dag_run, "dag_id"),
        run_id=(dag_run or {}).get("run_id") if isinstance(dag_run, dict) else _get(dag_run, "run_id"),
        budget_minutes=kwargs.get("budget_minutes"),
    )
