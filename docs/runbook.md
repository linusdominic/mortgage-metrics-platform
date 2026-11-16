# Runbook

Operational notes for running and repairing the pipeline. Commands assume the
repository root; `uv run mmp --help` lists every step.

## Routine operations

| Task | Command |
|---|---|
| Full run, demo scope | `make pipeline` |
| Re-run (no-op if nothing changed) | `make pipeline` |
| Load or reload one year | `uv run mmp backfill --years 2022` |
| Reload a year from freshly downloaded files | `uv run mmp backfill --years 2023 --refresh` |
| Rebuild all incremental models | `make full-refresh` |
| Another scope | `MMP_SCOPE=full make pipeline` (see config/pipeline.yml; not run in this build) |
| Check a metric from the shell | `uv run mmp query approval_rate -d state_code --grain year` |
| See the SQL behind a metric request | add `--explain` |
| Row counts and checksums | `uv run mmp checksum` |
| Time and bytes per run | `make cost-report` |

Everything under `data/` can be deleted and rebuilt. `data/raw` is a download
cache; deleting it means downloading again.

## How a run decides what to load

1. Ingestion skips a partition when its raw file exists and still hashes to
   the manifest's sha256. `--refresh` forces a download.
2. Conversion skips a partition when the Parquet footer already carries that
   sha256.
3. Each incremental dbt model loads the partitions whose manifest hash is not
   in `ops.partition_load_log` for that model, plus any year in
   `reprocess_years`. To see what is logged:

   ```sql
   select * from ops.partition_load_log order by model_name, activity_year, state_code;
   ```

To force one partition to reload without re-downloading, delete its rows from
`ops.partition_load_log` or run `uv run mmp dbt-build --reprocess-years <year>`.

## Failure modes

### Download fails

`DownloadError: HTTP 4xx`: not retried, because it will not fix itself. Check
the state code and year (`HTTP 400` with `invalid-states` for a bad state).
`gave up ... after N attempts`: 429/5xx or transport errors survived the
backoff. Re-run later; landed partitions are kept and skipped.

`HTTP 403` from `ffiec.cfpb.gov` on every request: see "User-Agent" below.

### `SchemaContractError` during conversion

The source file does not match `src/mmp/ingest/schema.py`: a renamed, added or
removed column, or a value of the wrong type in a typed column. Nothing was
written for that partition; other partitions are unaffected.

1. Compare the file header with `LAR_CSV_HEADER`.
2. If the FFIEC changed the layout, update the schema, the bronze source
   documentation and staging together, then re-run. Do not loosen a type to
   make one file pass without understanding why it differs.

### dbt contract error on a mart

A model's columns no longer match its YAML contract. Fix the model or change
the contract deliberately; a contract change on a mart is a breaking change for
the semantic layer and should be reviewed as one.

### `assert_reconciliation_balanced` or the gate's reconciliation check fails

```sql
select * from ops.ops_reconciliation where not is_balanced;
```

| Unbalanced flag | Usual cause |
|---|---|
| `is_raw_to_bronze_balanced` | Parquet built from a different file version than the manifest. Re-run `uv run mmp ingest --years <y>`. |
| `is_bronze_to_staging_balanced` | Quarantine and staging disagree: a model was run on its own. Run the full `dbt build`. |
| `is_staging_to_fact_balanced` | Fact partition is stale, duplicated or partly loaded. `uv run mmp dbt-build --reprocess-years <y>`. |

### Quality gate fails

`uv run mmp quality-gate` prints every check. Results are kept in
`ops.dq_results` after `mmp telemetry-load`.

- `partition_completeness`: an expected (year, state) is missing from the fact.
  Usually an ingest task failed; fix it and re-run.
- `quarantine_rate`: more than 1% of rows rejected. Look at
  `select reject_reason, count(*) from quarantine.rejected_lar_rows group by 1`.
  A new code or category published by the FFIEC shows up here as
  `unknown_product_code` / `unknown_domain_value`; extend the seed.
- `volume_anomaly`: a state moved more than `volume_fail_pct` year over year.
  Check whether the file is partial before raising the threshold in
  `config/pipeline.yml`.
- `freshness`: the newest LAR file was fetched more than
  `freshness_max_age_days` ago. Re-run ingestion with `--refresh`.

Nothing is published while the gate fails. The previous published catalogue
and snapshot stay in place.

### `could not open the warehouse read-only`

DuckDB allows one writer. The MCP server and `mmp query` open the file
read-only and cannot do so while a build holds the write lock. Wait for the
build, or look for a stale process holding the file. Under Airflow the
`duckdb_writer` pool prevents overlap between tasks.

### `Out of Memory Error` from DuckDB

dbt runs with `memory_limit` = `MMP_DUCKDB_MEMORY_LIMIT` (1GB locally, 400MB in
the Airflow container). Raise it, or lower `threads` in `dbt/profiles.yml`.

## User-Agent

`config/pipeline.yml` leaves `http.user_agent` unset, so requests carry the
HTTP library's stock identifier (`python-httpx/<version>`).

This is deliberate. While building this repo, requests sent with a custom
descriptive User-Agent string were answered `403 Access Denied` by the CDN in
front of `ffiec.cfpb.gov`, while the same request with a stock client
identifier returned `200`. Which strings the CDN rejects was not investigated
further. If you set `MMP_USER_AGENT`, keep it honest (name your client; do not
imitate a browser), keep personal contact details out of it, and expect to
have to test it.

The client is otherwise conservative: one request at a time per process, a
one second pause between requests, exponential backoff with jitter on 429 and
5xx, `Retry-After` honoured, and a local cache so no file is fetched twice.
Under Airflow the `hmda_api` pool caps concurrent downloads at two.

## Airflow

```bash
make airflow-up        # build the image, start Postgres, API server, DAG processor, scheduler
open http://localhost:8793
make airflow-down      # stop and delete volumes
```

The DAG is created paused. One run loads one activity year: the run whose data
interval ends on 1 July of year N+1 loads year N. Unpausing it creates one
scheduled run for the latest complete interval (`catchup=False`).

Backfill runs only start while the DAG is unpaused.

```bash
A="docker compose -f docker/airflow/docker-compose.yml exec airflow-scheduler"

# load activity years 2022, 2023 and 2024, one run at a time
$A airflow backfill create --dag-id hmda_mortgage_metrics \
    --from-date 2022-07-01 --to-date 2024-07-02 --max-active-runs 1

# run 2022 again; its partitions are rewritten and the results must not change
$A airflow backfill create --dag-id hmda_mortgage_metrics \
    --from-date 2022-07-01 --to-date 2022-07-02 --max-active-runs 1 \
    --reprocess-behavior completed

# each run records the checksums of the partitions it owns
$A bash -c 'cd /opt/airflow/data/published/checksums && ls && cat 2022__*'
```

`--from-date` / `--to-date` select runs by logical date, which for this
timetable is the start of the data interval: `2022-07-01` is the run that ends
on 2023-07-01 and loads 2022.

**Run one at a time.** Always pass `--max-active-runs 1`, and do not start a
backfill while a scheduled run is active. The dbt tests compare whole tables,
so two runs interleaving can fail each other. `max_active_runs=1` on the DAG
did not stop backfill runs overlapping a scheduled run in testing on 3.4.

**Resuming a failed run.** Fix the cause, then clear the failed task and what
follows it; completed tasks are not repeated:

```bash
$A airflow tasks clear hmda_mortgage_metrics -t quality_gate -d -y
```

**Memory.** The compose file gives the scheduler container 1000 MiB and runs
two task processes at most. dbt uses the `airflow` profile target (one dbt
thread, DuckDB capped at 400 MB with a spill directory). With the default
target's four threads, concurrent tests hit the cap and failed with DuckDB
out-of-memory errors; with three task processes the container was OOM-killed.

Pools are created by the `airflow-init` service: `hmda_api` (2 slots) and
`duckdb_writer` (1 slot). If tasks sit in `scheduled`, check that the pools
exist (`airflow pools list`).

Alerts (task failure, retry, DAG failure, missed deadline) are written to the
task log and to `/opt/airflow/data/logs/alerts.jsonl`. Wire `mmp.orchestration.alerts.emit`
to a pager or chat webhook for real use.

## Releasing a metric change

1. Change the definition in `dbt/models/semantic/_metrics.yml` (and the model,
   if a new column is needed, with its contract and tests).
2. Add or update a golden question with independent reference SQL.
3. `make lint test evals`.
4. The published catalogue and the MCP server pick the change up on the next
   pipeline run; there is nothing else to update.
