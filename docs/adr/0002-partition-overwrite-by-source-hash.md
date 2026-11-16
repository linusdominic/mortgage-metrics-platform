# ADR 0002: Incremental loads by partition overwrite, driven by source file hashes

Status: accepted

## Context

HMDA arrives as one file per (activity year, state). The public file has no
record identifier (the ULI is stripped) and no row-level timestamp, so neither a
merge on a business key nor a "newer than the last load" watermark is possible.
A published year can also be revised: the FFIEC replaces the file.

The loads have to be safe to re-run and safe to backfill out of order: load
2023 and 2024 first, then 2022, then 2022 again, and end in the same state as
loading all three once.

## Decision

The unit of loading is the partition. A partition is replaced whole or left
alone; rows are never merged.

- Every raw file has a manifest with its sha256. The hash is carried onto every
  bronze and fact row (`_source_sha256`).
- A control table, `ops.partition_load_log`, records which hash each incremental
  model last loaded for each partition.
- For each incremental model, dbt computes the pending partitions at run time:
  those whose manifest hash differs from the logged hash, plus any year listed
  in the `reprocess_years` variable, plus everything on first build or
  `--full-refresh`.
- The model deletes the pending partitions (pre-hook), selects only those
  partitions (a literal predicate, so the Parquet scan prunes to those files)
  and logs what it loaded (post-hook).

Row identity is `md5(year, state, row position in the file)`. It is stable for
a given file version, which is all a whole-partition replace needs.

## Consequences

- A run with no new or changed files selects zero rows from bronze. Measured in
  the README cost table.
- A scheduled or backfill run passes its own year in `reprocess_years` and so
  rewrites exactly that year, every time. Running it twice gives the same rows;
  `make backfill-proof` and `tests/test_pipeline.py` check that with content
  checksums, and check that other partitions are untouched.
- A revised source file is picked up without anyone asking: the hash differs,
  the partition is replaced. `test_changed_source_file_replaces_only_its_partition`.
- A fact partition depends only on same-year inputs (lender names for that
  year, the PMMS average for that year). That is a constraint on modelling, and
  it is why `dim_lender` keys are built the way ADR 0004 describes.
- `application_id` is not stable across file revisions. Nothing downstream may
  store it as a durable key. This is a property of the public data, not of the design.
- The delete and the insert are separate statements. dbt-duckdb runs the
  pre-hook and the model in one transaction by default; that has not been
  verified here with fault injection. If the insert failed after the delete had
  committed, the reconciliation test and the quality gate would fail the run
  (fact rows would not match staging), and the partition would be reloaded on
  the next run because its hash was never logged.

## Alternatives considered

- `delete+insert` with `unique_key = [activity_year, state_code]`. Close, but a
  partition whose new batch is empty would keep its old rows, which is exactly
  the quarantine table's normal case.
- `merge` on `application_id`. Would leave orphans when a revised file has
  fewer rows, and costs a join against the whole target.
- Full refresh every run. Correct and simple at 213k rows; not at 11 million
  rows a year.
