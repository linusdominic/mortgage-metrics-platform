# ADR 0006: Airflow 3 with Cosmos, and a plain-Python path that runs the same steps

Status: accepted

## Context

The pipeline needs scheduling, retries, backfills and visibility per step. It
also needs to be runnable by someone who does not want to start Docker.

## Decision

**One implementation of every step, two ways to run it.** Each step is a
function in the `mmp` package with a CLI command. `make pipeline` calls them in
order. The Airflow DAG calls the same functions from tasks. There is no logic
in the DAG file beyond wiring.

**Airflow 3.4** (the current stable line when this was built), task SDK imports
(`airflow.sdk`), LocalExecutor, Postgres metadata database.

**One run loads one activity year.** HMDA is annual, so the DAG uses a yearly
data-interval timetable (`0 6 1 7 *`): the run whose interval ends on 1 July of
year N+1 loads year N. `catchup=False`; history is loaded on purpose with
`airflow backfill create`.

**dbt through astronomer-cosmos 1.16**, `LoadMode.DBT_MANIFEST`,
`ExecutionMode.LOCAL`, tests after each model, multi-parent tests detached to
the end. Cosmos and dbt resolve in the same environment as Airflow 3.4 with the
versions in `uv.lock`, so no separate dbt virtualenv is needed. The manifest is
baked into the image at build time, so DAG parsing does not run dbt.

**Dynamic task mapping for ingestion.** `partitions` returns the states for the
run's year; `ingest` is expanded over them, so a failed state retries alone.

**Pools instead of hope.** `hmda_api` (2 slots) caps concurrent downloads from
the public API. `duckdb_writer` (1 slot) holds every task that opens the
warehouse, because DuckDB allows one writer (ADR 0001).

**Failure handling.** Two retries with exponential backoff on every task, an
execution timeout, `on_retry_callback` and `on_failure_callback` on every task,
a DAG-level failure callback, a three hour `dagrun_timeout`, and a
`DeadlineAlert` (the Airflow 3 replacement for SLAs) that fires a callback if a
run has not finished 90 minutes after it was queued. The quality gate has no
retries: a failed gate is a finding, not a flake. `load_telemetry` runs with
`trigger_rule=all_done` so a failed run still records its metrics.

## Consequences

- Model-level visibility in the Airflow grid: 60-odd dbt tasks per run. The
  price is one dbt start-up per task. With a single-slot pool that is serial,
  so a run whose `dbt build` takes about six seconds from the shell took 144
  to 172 seconds under Airflow in testing. On a warehouse with real concurrency the
  pool goes away and that trade improves; on DuckDB it is the cost of the view.
- **Runs must not overlap.** dbt tests compare whole tables (fact against
  manifest, fact against dimensions), so two runs for different years
  interleaving inside the pool can fail each other's tests. `max_active_runs=1`
  covers scheduled runs. In testing on Airflow 3.4, backfill runs were not held
  back by it: a scheduled run and two backfill runs started together. Backfills
  are therefore created with `--max-active-runs 1` and not while a scheduled
  run is active. This is an operating rule, written in the runbook, not
  something the DAG enforces.
- Per-model timings for Airflow runs live in Airflow's task instances. The
  `run_metrics` table gets the ingestion, gate and publish steps from Airflow
  runs but not per-model dbt records (those come from `mmp dbt-build`, which
  reads dbt's `run_results.json`; Cosmos runs each node in a temporary
  directory and discards it).
- The compose file is sized for a laptop: four containers, 1992 MiB of memory
  limits in total, two task slots. Three concurrent task processes were enough
  to get the scheduler container OOM-killed at 880 MiB during testing, which is
  why parallelism is 2 and the scheduler now has 1000 MiB. dbt runs with its
  own profile target in the container (one thread, DuckDB capped at 400 MB).
- The quality gate cannot read dbt's `run_results.json` under Cosmos, so in
  the DAG it relies on being downstream of every dbt task instead.
- The deadline callback is configured and covered by the DAG tests; no run in
  this build was slow enough to trigger it, so it has not been seen firing.

## Alternatives considered

- **One BashOperator running `dbt build`.** Faster on DuckDB and what
  `make pipeline` effectively does. It loses per-model retries and visibility.
  Kept as the no-Docker path rather than as the DAG.
- **Cosmos with `LoadMode.DBT_LS`.** Runs dbt at parse time in the DAG
  processor; slower parsing and a dependency on the warehouse path at parse.
- **A separate virtualenv for dbt.** The usual advice when Airflow and dbt
  pins conflict. They do not conflict here, and one environment is one fewer
  thing to keep in sync.
- **Airflow 2.10.** No reason to start a new project on the previous major.
