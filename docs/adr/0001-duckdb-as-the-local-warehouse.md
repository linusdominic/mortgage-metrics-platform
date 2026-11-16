# ADR 0001: DuckDB as the local warehouse engine

Status: accepted

## Context

The repository has to be cloned and run by a reviewer in a couple of minutes,
with no account, no credentials and no bill. It also has to show the practices
that matter on a cloud warehouse: medallion layers, incremental loads, tests,
contracts, a semantic layer.

## Decision

Use DuckDB (one file, `data/warehouse/mortgage.duckdb`) through `dbt-duckdb`.
Bronze stays outside the database as Hive-partitioned Parquet and is read in
place with `read_parquet`, the way an external table or stage would be.

## Consequences

Good:

- Zero setup. `make pipeline` builds a real star schema from real data.
- Columnar and vectorised, so the demo scope (about 213k rows, 99 source
  columns) builds in seconds and the full national file would still fit on a laptop.
- dbt, MetricFlow and sqlfluff all support it, so the project structure is the
  same one that would run on Snowflake or BigQuery.

Costs, and how the design deals with them:

- **One writer per file.** Two processes cannot write at once. Ingestion
  therefore never touches the database (it writes files), dbt runs with
  threads inside one process, and under Airflow every task that opens the
  warehouse sits in a single-slot pool. The serving path opens the file
  read-only. A serving process cannot open the file while a build holds the
  write lock; it reports that instead of hanging.
- **No bytes-billed counter.** Cost telemetry uses what can be measured:
  seconds per node, Parquet bytes of the partitions a run scanned, and the
  on-disk block size of each table. See `src/mmp/report/cost.py`.
- **No access control.** Governance in this repo is at the serving layer (the
  MCP tools cannot express anything but a governed metric). On a real warehouse
  that would be backed by grants on the marts schema as well.

## What changes on Snowflake, Redshift or BigQuery

| Concern | Here | On a cloud warehouse |
|---|---|---|
| Bronze | Parquet on local disk, `read_parquet` in `sources.yml` | Same files in object storage; external tables, or `COPY INTO` a raw schema. Only `_sources.yml` changes. |
| Profile | `dbt/profiles.yml`, a file path | Adapter credentials from the environment or a secrets manager |
| Incremental strategy | pre-hook `delete` + `append` per (year, state) | Same macro works. BigQuery: `insert_overwrite` on a partitioned table is cheaper. Snowflake/Redshift: `delete+insert`. |
| `safe_cast_double` | `try_cast` | Already dispatched: `safe_cast` on BigQuery, a regex guard on Redshift |
| DuckDB-only SQL | `select * exclude`, `count(*) filter (where ...)`, `qualify`, `make_date`, `bit_xor(hash(...))`, `median` | `exclude`/`qualify` exist on Snowflake and BigQuery (`except`); `filter` becomes `count_if`/`sum(case ...)`; `median` becomes `approx_percentile` or `percentile_cont`; the checksum uses `hash_agg` (Snowflake) or `bit_xor(farm_fingerprint(...))` (BigQuery). About a dozen expressions, all in staging, intermediate and `warehouse.py`. |
| Time spine | `generate_series` | `generator` / `generate_date_array` |
| Concurrency | single-slot pool | Remove the pool; raise dbt threads; size the warehouse instead |
| Semantic layer | MetricFlow renders DuckDB SQL | MetricFlow renders the target dialect; `_CompileOnlyClient` takes a different renderer |
| Cost telemetry | seconds, Parquet bytes, block sizes | Join `run_metrics` to query history on dbt's `invocation_id` query tag for credits or bytes billed |
| Clustering | none needed | Partition/cluster the fact on `activity_year`, `state_code` |

None of that has been run in this repository. It is the porting list, not a claim.
