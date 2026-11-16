"""DuckDB warehouse helpers: connections, table sizes, checksums."""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path

import duckdb

# Columns that record when/by which run a row was written. They change on every
# rewrite by design and are excluded from idempotency checksums.
AUDIT_COLUMNS = {"_loaded_at", "_dbt_invocation_id", "quarantined_at", "reconciled_at"}

CHECKSUM_TABLES = (
    "marts.fct_loan_applications",
    "marts.fct_application_denial_reasons",
    "marts.dim_lender",
    "marts.dim_geography",
    "marts.dim_loan_product",
    "marts.dim_applicant_demographics",
    "marts.dim_date",
    "quarantine.rejected_lar_rows",
)
PARTITIONED_TABLES = {
    "marts.fct_loan_applications",
    "marts.fct_application_denial_reasons",
    "quarantine.rejected_lar_rows",
}


@contextmanager
def connect(path: Path, read_only: bool = True):
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path), read_only=read_only)
    try:
        yield con
    finally:
        con.close()


def table_exists(con: duckdb.DuckDBPyConnection, qualified: str) -> bool:
    schema, table = qualified.split(".")
    return (
        con.execute(
            "select count(*) from information_schema.tables where table_schema = ? and table_name = ?",
            [schema, table],
        ).fetchone()[0]
        > 0
    )


def table_bytes(con: duckdb.DuckDBPyConnection, qualified: str) -> int | None:
    """On-disk size of a table: distinct persistent blocks x block size.

    DuckDB has no per-table byte counter; this is the closest real measurement.
    Returns None for views.
    """
    try:
        block_size = con.execute("select block_size from pragma_database_size()").fetchone()[0]
        blocks = con.execute(
            f"select count(distinct block_id) from pragma_storage_info('{qualified}') where persistent"
        ).fetchone()[0]
    except duckdb.Error:
        return None
    return int(blocks) * int(block_size)


def business_columns(con: duckdb.DuckDBPyConnection, qualified: str) -> list[str]:
    schema, table = qualified.split(".")
    rows = con.execute(
        "select column_name from information_schema.columns "
        "where table_schema = ? and table_name = ? order by ordinal_position",
        [schema, table],
    ).fetchall()
    return [r[0] for r in rows if r[0] not in AUDIT_COLUMNS]


def checksums(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Row count and an order-independent content hash per table (per partition for facts).

    The hash is bit_xor over hash(<every business column>), so it does not
    depend on physical row order and changes if any value in any row changes.
    """
    out: list[dict] = []
    for qualified in CHECKSUM_TABLES:
        if not table_exists(con, qualified):
            continue
        cols = ", ".join(f'"{c}"' for c in business_columns(con, qualified))
        if qualified in PARTITIONED_TABLES:
            rows = con.execute(
                f"select activity_year, state_code, count(*), bit_xor(hash({cols})) "
                f"from {qualified} group by 1, 2 order by 1, 2"
            ).fetchall()
            for year, state, n, h in rows:
                out.append(
                    {"table": qualified, "partition": f"{year}/{state}", "rows": n, "checksum": f"{h:016x}"}
                )
        else:
            n, h = con.execute(f"select count(*), bit_xor(hash({cols})) from {qualified}").fetchone()
            out.append({"table": qualified, "partition": "-", "rows": n, "checksum": f"{(h or 0):016x}"})
    return out
