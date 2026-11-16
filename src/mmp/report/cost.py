"""Run telemetry: load step records into the warehouse and report on them.

There is no cloud bill for a local DuckDB file, so "cost" here is the three
things a warehouse bill is made of: time, bytes scanned and bytes written, per
run, per model and per partition. The same table on Snowflake or BigQuery would
be joined to query history for credits or bytes billed.
"""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa

from mmp import warehouse
from mmp.settings import Settings

RUN_METRICS_SCHEMA = pa.schema(
    [
        ("run_id", pa.string()),
        ("run_kind", pa.string()),
        ("step", pa.string()),
        ("target", pa.string()),
        ("status", pa.string()),
        ("activity_year", pa.int32()),
        ("state_code", pa.string()),
        ("started_at", pa.string()),
        ("duration_s", pa.float64()),
        ("rows_in", pa.int64()),
        ("rows_out", pa.int64()),
        ("bytes_scanned", pa.int64()),
        ("bytes_written", pa.int64()),
        ("detail", pa.string()),
    ]
)
DQ_SCHEMA = pa.schema(
    [
        ("run_id", pa.string()),
        ("checked_at", pa.string()),
        ("check_name", pa.string()),
        ("status", pa.string()),
        ("detail", pa.string()),
        ("observed", pa.float64()),
        ("threshold", pa.float64()),
    ]
)


def load_run_metrics(settings: Settings) -> dict[str, int]:
    """Rebuild ops.run_metrics and ops.dq_results from the telemetry files."""
    records, dq = [], []
    for path in sorted(settings.telemetry_dir.glob("run_id=*/*.json")):
        row = json.loads(path.read_text())
        row["detail"] = json.dumps(row.get("detail") or {}, sort_keys=True)
        records.append({name: row.get(name) for name in RUN_METRICS_SCHEMA.names})
    for path in sorted(settings.telemetry_dir.glob("dq/run_id=*/*.json")):
        for row in json.loads(path.read_text()):
            dq.append({name: row.get("check" if name == "check_name" else name) for name in DQ_SCHEMA.names})
    run_metrics = pa.Table.from_pylist(records, schema=RUN_METRICS_SCHEMA)
    dq_results = pa.Table.from_pylist(dq, schema=DQ_SCHEMA)
    with warehouse.connect(settings.warehouse_path, read_only=False) as con:
        con.execute("create schema if not exists ops")
        con.register("run_metrics_src", run_metrics)
        con.register("dq_results_src", dq_results)
        con.execute(
            "create or replace table ops.run_metrics as "
            "select * replace (cast(started_at as timestamptz) as started_at) from run_metrics_src"
        )
        con.execute(
            "create or replace table ops.dq_results as "
            "select * replace (cast(checked_at as timestamptz) as checked_at) from dq_results_src"
        )
    return {"run_metrics": run_metrics.num_rows, "dq_results": dq_results.num_rows}


def _table(headers: list[str], rows: list[tuple]) -> str:
    def fmt(v) -> str:
        if v is None:
            return "-"
        if isinstance(v, float):
            return f"{v:,.2f}"
        if isinstance(v, int):
            return f"{v:,}"
        return str(v)

    cells = [[fmt(v) for v in row] for row in rows]
    widths = [max(len(h), *(len(r[i]) for r in cells)) if cells else len(h) for i, h in enumerate(headers)]
    numeric = [
        bool(rows) and all(isinstance(r[i], int | float) or r[i] is None for r in rows)
        for i in range(len(headers))
    ]
    line = "| " + " | ".join(h.ljust(w) for h, w in zip(headers, widths, strict=True)) + " |"
    sep = (
        "|"
        + "|".join(
            ("-" * (w + 1) + ":") if n else ("-" * (w + 2)) for w, n in zip(widths, numeric, strict=True)
        )
        + "|"
    )
    body = [
        "| "
        + " | ".join(c.rjust(w) if n else c.ljust(w) for c, w, n in zip(r, widths, numeric, strict=True))
        + " |"
        for r in cells
    ]
    return "\n".join([line, sep, *body])


RUNS_SQL = """
with per_run as (
    select
        run_id,
        any_value(run_kind) as run_kind,
        min(started_at) as started_at,
        sum(duration_s) filter (where step = 'ingest.download') as download_s,
        sum(bytes_written) filter (where step = 'ingest.download') as downloaded_bytes,
        sum(duration_s) filter (where step = 'ingest.convert') as convert_s,
        sum(duration_s) filter (where step = 'dbt.invocation') as dbt_wall_s,
        sum(duration_s) filter (where step = 'dbt.model') as dbt_model_s,
        sum(duration_s) filter (where step = 'dbt.test' or step = 'dbt.unit_test') as dbt_test_s,
        sum(duration_s) filter (where step = 'dbt.model' and target = 'fct_loan_applications') as fact_model_s,
        sum(rows_out) filter (where step = 'dbt.model' and target = 'fct_loan_applications') as fact_rows_written,
        sum(bytes_scanned) filter (where step = 'dbt.model' and target = 'fct_loan_applications') as fact_bytes_scanned,
        sum(bytes_written) filter (where step = 'dbt.model' and target = 'fct_loan_applications') as fact_bytes_written
    from ops.run_metrics
    group by run_id
)
select
    run_id, run_kind, round(coalesce(download_s, 0), 2), coalesce(downloaded_bytes, 0),
    round(coalesce(convert_s, 0), 2), round(coalesce(dbt_wall_s, 0), 2), round(coalesce(dbt_model_s, 0), 2),
    round(coalesce(dbt_test_s, 0), 2), coalesce(fact_rows_written, 0), coalesce(fact_bytes_scanned, 0),
    coalesce(fact_bytes_written, 0), round(coalesce(fact_model_s, 0), 3)
from per_run
where dbt_wall_s is not null
order by started_at
"""


def cost_report(settings: Settings, run_id: str | None = None) -> str:
    out: list[str] = []
    with warehouse.connect(settings.warehouse_path, read_only=True) as con:
        if not warehouse.table_exists(con, "ops.run_metrics"):
            return "ops.run_metrics does not exist yet; run the pipeline first."
        runs = con.execute(RUNS_SQL).fetchall()
        out.append("## Runs\n")
        out.append(
            _table(
                [
                    "run_id",
                    "kind",
                    "download_s",
                    "downloaded_bytes",
                    "convert_s",
                    "dbt_wall_s",
                    "dbt_model_s",
                    "dbt_test_s",
                    "fact_rows_written",
                    "fact_bytes_scanned",
                    "fact_bytes_written_est",
                ],
                [r[:11] for r in runs],
            )
        )

        # incremental vs full refresh: most recent run of each kind
        latest: dict[str, tuple] = {}
        for row in runs:
            latest[row[1]] = row
        if "full_refresh" in latest:
            full = latest["full_refresh"]
            rows = []
            for kind in ("full_refresh", "incremental", "backfill"):
                if kind not in latest:
                    continue
                r = latest[kind]
                rows.append(
                    (
                        kind,
                        r[0],
                        r[8],
                        r[9],
                        r[11],
                        r[6],
                        r[5],
                        round(100.0 * r[9] / full[9], 1) if full[9] else None,
                        round(100.0 * r[11] / full[11], 1) if full[11] else None,
                    )
                )
            out.append("\n## Incremental vs full refresh (latest run of each kind)\n")
            out.append(
                _table(
                    [
                        "kind",
                        "run_id",
                        "fact_rows_written",
                        "fact_bytes_scanned",
                        "fact_model_s",
                        "all_models_s",
                        "dbt_wall_s",
                        "bytes_scanned_pct_of_full",
                        "fact_model_s_pct_of_full",
                    ],
                    rows,
                )
            )

        target_run = run_id or (runs[-1][0] if runs else None)
        if target_run:
            out.append(f"\n## Steps, run {target_run}\n")
            out.append(
                _table(
                    ["step", "records", "seconds", "rows_out", "bytes_scanned", "bytes_written"],
                    con.execute(
                        "select step, count(*), round(sum(duration_s), 2), sum(rows_out), sum(bytes_scanned), "
                        "sum(bytes_written) from ops.run_metrics where run_id = ? and step <> 'dbt.partition' "
                        "group by 1 order by min(started_at), 1",
                        [target_run],
                    ).fetchall(),
                )
            )
            out.append(f"\n## dbt models by time, run {target_run}\n")
            out.append(
                _table(
                    [
                        "model",
                        "materialized",
                        "seconds",
                        "rows_written",
                        "bytes_scanned",
                        "bytes_written_est",
                    ],
                    con.execute(
                        "select target, json_extract_string(detail, '$.materialized'), round(duration_s, 3), "
                        "rows_out, bytes_scanned, bytes_written from ops.run_metrics "
                        "where run_id = ? and step in ('dbt.model', 'dbt.seed', 'dbt.snapshot') "
                        "order by duration_s desc limit 12",
                        [target_run],
                    ).fetchall(),
                )
            )

        out.append("\n## Per partition, all runs (fct_loan_applications)\n")
        out.append(
            _table(
                [
                    "year",
                    "state",
                    "raw_bytes",
                    "download_s",
                    "convert_s",
                    "parquet_bytes",
                    "times_loaded",
                    "fact_rows",
                    "alloc_model_s",
                ],
                con.execute(
                    """
                    select
                        cast(activity_year as varchar), state_code,
                        max(bytes_written) filter (where step = 'ingest.download' and target = 'hmda_lar'),
                        round(sum(duration_s) filter (where step = 'ingest.download' and target = 'hmda_lar'
                              and bytes_written > 0), 2),
                        round(sum(duration_s) filter (where step = 'ingest.convert' and target = 'hmda_lar'
                              and bytes_written > 0), 2),
                        max(bytes_scanned) filter (where step = 'dbt.partition' and target = 'fct_loan_applications'),
                        count(*) filter (where step = 'dbt.partition' and target = 'fct_loan_applications'),
                        max(rows_out) filter (where step = 'dbt.partition' and target = 'fct_loan_applications'),
                        round(sum(duration_s) filter (where step = 'dbt.partition'
                              and target = 'fct_loan_applications'), 3)
                    from ops.run_metrics
                    where activity_year is not null
                    group by 1, 2 order by 1, 2
                    """
                ).fetchall(),
            )
        )
    return "\n".join(out) + "\n"


def write_cost_report(settings: Settings, dest: Path | None = None) -> str:
    text = cost_report(settings)
    dest = dest or settings.data_dir / "published" / "cost_report.md"
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(text)
    return text
