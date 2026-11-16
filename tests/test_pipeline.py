"""End-to-end behaviour of the dbt layer on the fixture sample: reconciliation,
incremental no-ops, partition overwrite, backfill idempotency, quarantine."""

import json
import shutil

from helpers import FIXTURE_SOURCE, ingest_all, make_settings
from mmp import dbt_runner, warehouse
from mmp.ingest.schema import LAR_CSV_HEADER

FIXTURE_ROWS = {
    (year, state): len((FIXTURE_SOURCE / "hmda_lar" / f"{year}_{state}.csv").read_text().splitlines()) - 1
    for year in (2022, 2023, 2024)
    for state in ("DE", "RI")
}


def q(settings, sql, params=None):
    with warehouse.connect(settings.warehouse_path, read_only=True) as con:
        return con.execute(sql, params or []).fetchall()


def snapshot(settings):
    with warehouse.connect(settings.warehouse_path, read_only=True) as con:
        return warehouse.checksums(con)


def test_build_has_no_failures(settings):
    results = json.loads((settings.dbt_target_dir / "run_results.json").read_text())["results"]
    statuses = {r["status"] for r in results}
    assert not statuses & {"error", "fail"}, statuses
    kinds = {r["unique_id"].split(".")[0] for r in results}
    assert {"model", "seed", "snapshot", "test", "unit_test"} <= kinds


def test_fact_row_counts_equal_the_source_files(settings):
    rows = {
        (y, s): n
        for y, s, n in q(
            settings,
            "select activity_year, state_code, count(*) from marts.fct_loan_applications group by 1, 2",
        )
    }
    assert rows == {k: v for k, v in FIXTURE_ROWS.items() if k[0] in (2023, 2024)}


def test_every_partition_reconciles(settings):
    rows = q(
        settings,
        "select raw_row_count, bronze_rows, staged_rows, quarantined_rows, fact_rows, is_balanced from ops.ops_reconciliation",
    )
    assert len(rows) == 4
    for raw, bronze, staged, quarantined, fact, balanced in rows:
        assert balanced and raw == bronze == staged + quarantined and fact == staged


def test_star_schema_has_no_orphans(settings):
    for dim, key in (
        ("dim_lender", "lender_key"),
        ("dim_geography", "geography_key"),
        ("dim_loan_product", "loan_product_key"),
        ("dim_applicant_demographics", "applicant_demographics_key"),
        ("dim_date", "date_key"),
    ):
        orphans = q(
            settings,
            f"select count(*) from marts.fct_loan_applications f anti join marts.{dim} d using ({key})",
        )
        assert orphans[0][0] == 0, dim


def test_rerun_without_new_data_loads_nothing(scratch):
    before = snapshot(scratch)
    loaded_before = q(scratch, "select count(*), count(distinct invocation_id) from ops.partition_load_log")
    result = dbt_runner.build(scratch)
    assert result.returncode == 0
    assert snapshot(scratch) == before
    # no partition was re-logged, and no fact row was rewritten
    assert (
        q(scratch, "select count(*), count(distinct invocation_id) from ops.partition_load_log")
        == loaded_before
    )
    assert q(scratch, "select count(distinct _dbt_invocation_id) from marts.fct_loan_applications")[0][0] == 1


def test_backfill_adds_a_year_and_is_idempotent(scratch):
    before = {(r["table"], r["partition"]): r for r in snapshot(scratch)}
    ingest_all(scratch, years=(2022,))
    dbt_runner.build(scratch, reprocess_years=[2022])
    first = snapshot(scratch)
    dbt_runner.build(scratch, reprocess_years=[2022])
    second = snapshot(scratch)
    assert first == second, "re-running the backfill must not change a single row"

    after = {(r["table"], r["partition"]): r for r in first}
    for state in ("DE", "RI"):
        assert after[("marts.fct_loan_applications", f"2022/{state}")]["rows"] == FIXTURE_ROWS[(2022, state)]
    # the partitions that were already loaded are byte-for-byte what they were
    for key, row in before.items():
        if key[0] in warehouse.PARTITIONED_TABLES:
            assert after[key] == row, key
    # the reprocessed year was rewritten by the second run; the other years were not touched
    invocations = dict(
        q(
            scratch,
            "select activity_year, count(distinct _dbt_invocation_id) from marts.fct_loan_applications group by 1",
        )
    )
    assert invocations == {2022: 1, 2023: 1, 2024: 1}
    distinct = q(scratch, "select count(distinct _dbt_invocation_id) from marts.fct_loan_applications")[0][0]
    assert distinct == 2
    assert q(scratch, "select count(*) from ops.ops_reconciliation where is_balanced")[0][0] == 6


def test_full_refresh_reproduces_the_incremental_result(scratch):
    before = snapshot(scratch)
    dbt_runner.build(scratch, full_refresh=True)
    assert snapshot(scratch) == before


def _mutated_source(tmp_path, mutate):
    root = tmp_path / "source"
    shutil.copytree(FIXTURE_SOURCE, root)
    target = root / "hmda_lar" / "2024_DE.csv"
    lines = target.read_text().splitlines()
    target.write_text("\n".join(mutate(lines)) + "\n")
    return root


def test_changed_source_file_replaces_only_its_partition(scratch, tmp_path):
    root = _mutated_source(tmp_path, lambda lines: lines[:-25])
    changed = make_settings(scratch.data_dir, fixture_dir=root)

    def other_partitions():
        # dims are rebuilt from all loaded data and may legitimately change; fact partitions may not
        return [
            r
            for r in snapshot(changed)
            if r["table"] in warehouse.PARTITIONED_TABLES and r["partition"] != "2024/DE"
        ]

    untouched_before = other_partitions()
    ingest_all(changed, refresh=True)
    dbt_runner.build(changed)
    rows = {
        (y, s): n
        for y, s, n in q(
            changed,
            "select activity_year, state_code, count(*) from marts.fct_loan_applications group by 1, 2",
        )
    }
    assert rows[(2024, "DE")] == FIXTURE_ROWS[(2024, "DE")] - 25
    assert other_partitions() == untouched_before
    assert q(changed, "select count(*) from ops.ops_reconciliation where not is_balanced")[0][0] == 0
    # only that partition carries the new invocation id
    assert q(
        changed,
        "select activity_year, state_code from marts.fct_loan_applications group by 1, 2 having count(distinct _dbt_invocation_id) = 1 and max(_loaded_at) = (select max(_loaded_at) from marts.fct_loan_applications)",
    ) == [(2024, "DE")]


def test_bad_rows_are_quarantined_with_reasons_and_still_reconcile(tmp_path):
    col = LAR_CSV_HEADER.index

    def mutate(lines):
        def edit(line, column, value):
            cells = line.split(",")
            cells[col(column)] = value
            return ",".join(cells)

        lines[1] = edit(lines[1], "lei", "TOO-SHORT")
        lines[2] = edit(lines[2], "loan_amount", "0")
        lines[3] = edit(lines[3], "state_code", "RI")
        lines[4] = edit(lines[4], "action_taken", "9")
        lines[5] = edit(lines[5], "derived_race", "Unpublished category")
        return lines

    settings = make_settings(tmp_path / "data", fixture_dir=_mutated_source(tmp_path, mutate))
    ingest_all(settings)
    result = dbt_runner.build(settings)
    assert result.returncode == 0
    rejected = q(
        settings, "select source_row_number, reject_reason from quarantine.rejected_lar_rows order by 1"
    )
    assert rejected == [
        (1, "invalid_lei"),
        (2, "invalid_loan_amount"),
        (3, "partition_mismatch"),
        (4, "invalid_action_taken"),
        (5, "unknown_domain_value"),
    ]
    recon = q(
        settings,
        "select bronze_rows, staged_rows, quarantined_rows, fact_rows, is_balanced from ops.ops_reconciliation where activity_year = 2024 and state_code = 'DE'",
    )
    assert recon == [
        (FIXTURE_ROWS[(2024, "DE")], FIXTURE_ROWS[(2024, "DE")] - 5, 5, FIXTURE_ROWS[(2024, "DE")] - 5, True)
    ]
    # quarantined rows are not in the fact
    assert (
        q(
            settings,
            "select count(*) from marts.fct_loan_applications f semi join quarantine.rejected_lar_rows r using (application_id)",
        )[0][0]
        == 0
    )


def test_a_broken_reconciliation_fails_the_build(scratch):
    """Fail closed: if the fact loses rows behind dbt's back, the next build's tests catch it."""
    with warehouse.connect(scratch.warehouse_path, read_only=False) as con:
        con.execute(
            "delete from marts.fct_loan_applications where activity_year = 2024 and state_code = 'DE' and source_row_number <= 3"
        )
        con.execute(
            "update ops.ops_reconciliation set fact_rows = fact_rows - 3, is_staging_to_fact_balanced = false, is_balanced = false where activity_year = 2024 and state_code = 'DE'"
        )
    result = dbt_runner.build(scratch, check=False)
    assert result.returncode != 0
    failed = [
        r["unique_id"]
        for r in json.loads((scratch.dbt_target_dir / "run_results.json").read_text())["results"]
        if r["status"] == "fail"
    ]
    assert any("assert_reconciliation_balanced" in f for f in failed)


def test_lender_snapshot_tracks_one_current_row_per_lei(settings):
    rows = q(
        settings,
        "select count(*), count(distinct lei) from snapshots.snap_lender_registry where dbt_valid_to is null",
    )
    assert rows[0][0] == rows[0][1] > 0
