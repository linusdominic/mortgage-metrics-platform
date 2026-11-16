"""DAG integrity tests. Need the airflow dependency group:

uv run --group airflow pytest tests/dags -m airflow
"""

from __future__ import annotations

import os
from datetime import timedelta
from pathlib import Path

import pytest

pytest.importorskip("airflow", reason="install with: uv sync --group airflow")
pytest.importorskip("cosmos", reason="install with: uv sync --group airflow")

pytestmark = pytest.mark.airflow

REPO = Path(__file__).resolve().parents[2]
DAG_ID = "hmda_mortgage_metrics"


@pytest.fixture(scope="module")
def dag(tmp_path_factory):
    home = tmp_path_factory.mktemp("airflow_home")
    os.environ.setdefault("AIRFLOW_HOME", str(home))
    os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
    os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")
    # Cosmos renders the dbt task group from a manifest; make sure one exists.
    manifest = REPO / "dbt" / "target" / "manifest.json"
    if not manifest.exists():
        import subprocess
        import sys

        subprocess.run(
            [str(Path(sys.executable).parent / "dbt"), "parse", "--project-dir", str(REPO / "dbt")],
            check=True,
            env={**os.environ, "DBT_PROFILES_DIR": str(REPO / "dbt"), "MMP_DATA_DIR": str(home)},
            cwd=REPO / "dbt",
        )
    from airflow.dag_processing.dagbag import DagBag

    bag = DagBag(dag_folder=str(REPO / "dags"))
    assert not bag.import_errors, bag.import_errors
    assert DAG_ID in bag.dags
    return bag.dags[DAG_ID]


def test_dag_imports_without_errors_and_has_no_cycles(dag):
    # topological_sort raises on a cycle
    order = [t.task_id for t in dag.topological_sort()]
    assert len(order) == len(dag.tasks) > 30


def test_schedule_and_run_policy(dag):
    from airflow.timetables.interval import CronDataIntervalTimetable

    assert isinstance(dag.timetable, CronDataIntervalTimetable)
    assert dag.timetable.summary == "0 6 1 7 *"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.dagrun_timeout == timedelta(hours=3)
    assert dag.deadline, "the DAG must carry a deadline alert"
    assert dag.has_on_failure_callback or dag.on_failure_callback


def test_default_args_give_every_task_retries_backoff_and_alerting(dag):
    assert dag.default_args["retries"] >= 2
    assert dag.default_args["retry_exponential_backoff"]
    assert dag.default_args["execution_timeout"] <= timedelta(hours=1)
    from mmp.orchestration import alerts

    assert dag.default_args["on_failure_callback"] is alerts.task_failed
    for t in dag.tasks:
        assert t.owner == "data-platform", t.task_id
        assert t.on_failure_callback, f"{t.task_id} has no failure callback"
        assert t.execution_timeout is not None, t.task_id


def test_pools_serialise_warehouse_access_and_limit_api_concurrency(dag):
    by_id = {t.task_id: t for t in dag.tasks}
    assert by_id["ingest"].pool == "hmda_api" and by_id["ingest_reference"].pool == "hmda_api"
    assert by_id["ingest"].max_active_tis_per_dagrun == 2
    warehouse_tasks = [t for t in dag.tasks if t.task_id.startswith("dbt.")] + [
        by_id[n]
        for n in ("source_freshness", "quality_gate", "publish_metrics", "record_checksums", "load_telemetry")
    ]
    assert all(t.pool == "duckdb_writer" for t in warehouse_tasks)
    assert {t.pool for t in dag.tasks} <= {"hmda_api", "duckdb_writer", "default_pool"}


def test_ingestion_is_dynamically_mapped_over_partitions(dag):
    from airflow.sdk.definitions.mappedoperator import MappedOperator

    ingest = dag.get_task("ingest")
    assert isinstance(ingest, MappedOperator)
    assert ingest.upstream_task_ids == {"partitions"}


def test_dbt_group_is_rendered_per_model_and_passes_the_run_year(dag):
    ids = {t.task_id for t in dag.tasks}
    assert any(i.startswith("dbt.fct_loan_applications") and i.endswith("run") for i in ids), sorted(ids)
    assert any("dim_lender" in i for i in ids) and any("snap_lender_registry" in i for i in ids)
    run = next(
        t
        for t in dag.tasks
        if t.task_id.startswith("dbt.fct_loan_applications") and t.task_id.endswith("run")
    )
    assert run.vars == {"reprocess_years": "[{{ data_interval_end.year - 1 }}]"}


def test_gate_blocks_publish_and_telemetry_always_runs(dag):
    gate, publish = dag.get_task("quality_gate"), dag.get_task("publish_metrics")
    assert "quality_gate" in publish.upstream_task_ids
    assert any(i.startswith("dbt.") for i in gate.upstream_task_ids)
    assert gate.retries == 0, "a failed quality gate is not retried into passing"
    assert dag.get_task("record_checksums").upstream_task_ids == {"publish_metrics"}
    assert str(dag.get_task("load_telemetry").trigger_rule) in ("all_done", "TriggerRule.ALL_DONE")


def test_run_for_an_interval_loads_the_preceding_activity_year(dag):
    import pendulum
    from airflow.timetables.base import DataInterval

    # the interval that ends 2023-07-01 is the run that loads activity year 2022
    interval = DataInterval(pendulum.datetime(2022, 7, 1, 6), pendulum.datetime(2023, 7, 1, 6))
    assert interval.end.year - 1 == 2022
    nxt = dag.timetable.next_dagrun_info(last_automated_data_interval=interval, restriction=_no_restriction())
    assert (nxt.data_interval.start.year, nxt.data_interval.end.year) == (2023, 2024)


def _no_restriction():
    from airflow.timetables.base import TimeRestriction

    return TimeRestriction(earliest=None, latest=None, catchup=True)
