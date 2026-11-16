"""HMDA mortgage metrics: annual batch pipeline.

One run loads one HMDA activity year. HMDA is published once a year, so the DAG
has a yearly data-interval schedule: the run whose interval ends on 1 July of
year N+1 loads activity year N (the national snapshot for year N is published
in the first half of N+1).

    partitions -> ingest (mapped, one task per state)  --+
                  ingest_reference -----------------------+-> index_manifest
      -> source_freshness -> dbt (Cosmos, one task per model) -> quality_gate
      -> publish_metrics -> record_checksums ; load_telemetry (always)

Idempotency. A run for year N always rewrites exactly the year-N partitions
(dbt var reprocess_years=[N], partition overwrite) and nothing else, so running
it once, twice, or as part of a backfill produces the same tables. That is what
makes `airflow backfill create ... --reprocess-behavior completed` safe.

Concurrency. DuckDB allows one writer. Every task that opens the warehouse runs
in the single-slot pool `duckdb_writer`; downloads run in `hmda_api` (2 slots)
so the public API is never hit with more than two concurrent requests.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param, get_current_context, task
from airflow.sdk.definitions.callback import SyncCallback
from airflow.sdk.definitions.deadline import DeadlineAlert, DeadlineReference
from airflow.timetables.interval import CronDataIntervalTimetable
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import ExecutionMode, LoadMode, TestBehavior

from mmp.orchestration import alerts
from mmp.settings import REPO_ROOT, load_scopes

DAG_ID = "hmda_mortgage_metrics"
DBT_DIR = Path(os.environ.get("MMP_DBT_DIR", REPO_ROOT / "dbt"))
SCOPE = load_scopes()[os.environ.get("MMP_SCOPE", "demo")]
RETRY_DELAY = timedelta(seconds=int(os.environ.get("MMP_RETRY_DELAY_SECONDS", "120")))
RUN_BUDGET = timedelta(minutes=int(os.environ.get("MMP_RUN_BUDGET_MINUTES", "90")))

POOL_API = "hmda_api"
POOL_WAREHOUSE = "duckdb_writer"

# The activity year a run is responsible for.
DATA_YEAR = "{{ data_interval_end.year - 1 }}"

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": RETRY_DELAY,
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": alerts.task_failed,
    "on_retry_callback": alerts.task_retrying,
}


def _data_year(context) -> int:
    return context["data_interval_end"].year - 1


def _bind_run(context) -> None:
    """Tag telemetry written by this task with the Airflow run."""
    dag_run = context["dag_run"]
    os.environ["MMP_RUN_ID"] = f"airflow__{dag_run.run_id}"
    os.environ["MMP_RUN_KIND"] = "backfill" if "backfill" in str(dag_run.run_type).lower() else "incremental"


with DAG(
    dag_id=DAG_ID,
    description="Load one HMDA activity year, build the star schema, gate on data quality, publish metrics.",
    schedule=CronDataIntervalTimetable("0 6 1 7 *", timezone="UTC"),
    start_date=datetime(2019, 7, 1),  # first interval ends 2020-07-01 -> activity year 2019
    catchup=False,  # history is loaded deliberately, with `airflow backfill create`
    max_active_runs=1,  # one writer; runs for different years must not interleave
    max_active_tasks=4,
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=3),
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_QUEUED_AT,
        interval=RUN_BUDGET,
        callback=SyncCallback(
            "mmp.orchestration.alerts.deadline_missed",
            kwargs={"budget_minutes": int(RUN_BUDGET.total_seconds() // 60)},
        ),
    ),
    on_failure_callback=alerts.dag_failed,
    params={
        "states": Param(list(SCOPE.states), type="array", description="States to load (two-letter codes)."),
        "refresh": Param(
            False, type="boolean", description="Re-download raw files even if the cache is intact."
        ),
    },
    tags=["hmda", "dbt", "duckdb"],
    doc_md=__doc__,
) as dag:

    @task
    def partitions() -> list[dict]:
        """The (activity_year, state) partitions this run owns."""
        context = get_current_context()
        year = _data_year(context)
        return [{"year": year, "state": state} for state in context["params"]["states"]]

    @task(
        pool=POOL_API,
        max_active_tis_per_dagrun=2,
        map_index_template="{{ task.op_kwargs['partition']['state'] }}",
    )
    def ingest(partition: dict) -> dict:
        """Land one LAR partition and its filer list, then convert both to bronze Parquet."""
        from mmp.ingest.lar import convert_partition, land_partition
        from mmp.ingest.sources import make_source
        from mmp.settings import load_settings

        context = get_current_context()
        _bind_run(context)
        settings = load_settings()
        landed = land_partition(
            settings,
            make_source(settings),
            partition["year"],
            partition["state"],
            refresh=bool(context["params"]["refresh"]),
        )
        rows = convert_partition(settings, partition["year"], partition["state"])
        manifest = landed["hmda_lar"].manifest
        return {
            **partition,
            "status": landed["hmda_lar"].status,
            "bytes": manifest.bytes,
            "sha256": manifest.sha256,
            "rows": rows["hmda_lar"],
        }

    @task(pool=POOL_API)
    def ingest_reference() -> dict:
        """PMMS weekly rates and Census county names."""
        from mmp.ingest.reference import ingest_reference as run
        from mmp.ingest.sources import make_source
        from mmp.settings import load_settings

        _bind_run(get_current_context())
        settings = load_settings()
        return run(settings, make_source(settings))

    @task
    def index_manifest(landed: list[dict]) -> int:
        """Fold the per-partition manifests into the one relation dbt reads."""
        from mmp.ingest.manifest import build_manifest_index
        from mmp.settings import load_settings

        settings = load_settings()
        print(f"landed partitions: {landed}")
        return build_manifest_index(settings.raw_dir, settings.bronze_dir)

    source_freshness = BashOperator(
        task_id="source_freshness",
        bash_command="mmp freshness",
        pool=POOL_WAREHOUSE,
        env={
            "MMP_RUN_ID": "airflow__{{ run_id }}",
            "MMP_DBT_TARGET_PATH": "/tmp/dbt_target_{{ ti.task_id }}",
        },
        append_env=True,
    )

    dbt = DbtTaskGroup(
        group_id="dbt",
        project_config=ProjectConfig(
            dbt_project_path=DBT_DIR,
            manifest_path=DBT_DIR / "target" / "manifest.json",
            project_name="mortgage_metrics",
            install_dbt_deps=False,  # the project has no packages
        ),
        profile_config=ProfileConfig(
            profile_name="mortgage_metrics",
            target_name=os.environ.get("DBT_TARGET", "dev"),
            profiles_yml_filepath=DBT_DIR / "profiles.yml",
        ),
        execution_config=ExecutionConfig(execution_mode=ExecutionMode.LOCAL),
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST,
            test_behavior=TestBehavior.AFTER_EACH,
            # A test with several parents (relationships, reconciliation) runs once, after all of them.
            should_detach_multiple_parents_tests=True,
        ),
        operator_args={
            "pool": POOL_WAREHOUSE,
            # Rewrite exactly this run's activity year; see macros/partition_overwrite.sql.
            "vars": {"reprocess_years": f"[{DATA_YEAR}]"},
            "retries": 1,
        },
    )

    @task(pool=POOL_WAREHOUSE, retries=0)
    def quality_gate() -> list[dict]:
        """Fail closed: nothing is published unless every check passes."""
        from dataclasses import asdict

        from mmp.cli import _loaded_years
        from mmp.quality.gate import format_results, run_gate
        from mmp.settings import load_settings

        context = get_current_context()
        _bind_run(context)
        settings = load_settings()
        states = context["params"]["states"]
        years = sorted(_loaded_years(settings) | {_data_year(context)})
        results = run_gate(settings, [(y, s) for y in years for s in states], check_dbt_results=False)
        print(format_results(results))
        return [asdict(r) for r in results]

    @task(pool=POOL_WAREHOUSE)
    def publish_metrics() -> dict:
        """Metric catalogue, snapshot and the static report, all through the semantic layer."""
        from mmp.publish import publish
        from mmp.report.build import build_report
        from mmp.settings import load_settings

        _bind_run(get_current_context())
        settings = load_settings()
        paths = {name: str(path) for name, path in publish(settings).items()}
        paths["report"] = str(build_report(settings, settings.data_dir / "published" / "report"))
        return paths

    @task(pool=POOL_WAREHOUSE)
    def record_checksums() -> str:
        """Row counts and content checksums of this run's partitions, kept per run for idempotency checks."""
        from mmp import warehouse
        from mmp.settings import load_settings

        context = get_current_context()
        settings = load_settings()
        year = _data_year(context)
        with warehouse.connect(settings.warehouse_path, read_only=True) as con:
            rows = [r for r in warehouse.checksums(con) if r["partition"].startswith(f"{year}/")]
        text = "".join(
            f"{r['table']:<38} {r['partition']:<8} {r['rows']:>9,} {r['checksum']}\n" for r in rows
        )
        out = (
            settings.data_dir
            / "published"
            / "checksums"
            / f"{year}__{context['run_id'].replace(':', '-')}.txt"
        )
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(text)
        print(text)
        return str(out)

    @task(pool=POOL_WAREHOUSE, trigger_rule="all_done", retries=0)
    def load_telemetry() -> dict:
        """Runs even when the gate fails, so a failed run still leaves its metrics behind."""
        from mmp.report.cost import load_run_metrics
        from mmp.settings import load_settings

        return load_run_metrics(load_settings())

    landed = ingest.expand(partition=partitions())
    indexed = index_manifest(landed)
    ingest_reference() >> indexed
    gate = quality_gate()
    published = publish_metrics()
    indexed >> source_freshness >> dbt >> gate >> published >> record_checksums()
    [gate, published] >> load_telemetry()
