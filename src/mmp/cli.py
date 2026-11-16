"""Command line entry point: `mmp <command>`.

Each command is one pipeline step. The Makefile, the Airflow DAG and CI all
call these same commands, so there is one implementation of every step.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path

from mmp import telemetry, warehouse
from mmp.settings import REPO_ROOT, Settings, load_settings

log = logging.getLogger("mmp")


def _years(settings: Settings, args) -> tuple[int, ...]:
    return tuple(args.years) if getattr(args, "years", None) else settings.scope.years


def _partitions(settings: Settings, args) -> list[tuple[int, str]]:
    states = tuple(args.states) if getattr(args, "states", None) else settings.scope.states
    return [(y, s) for y in _years(settings, args) for s in states]


# --------------------------------------------------------------------- steps


def cmd_ingest(settings: Settings, args) -> int:
    from mmp.ingest.lar import convert_partition, land_partition
    from mmp.ingest.manifest import build_manifest_index
    from mmp.ingest.reference import ingest_reference
    from mmp.ingest.sources import make_source

    source = make_source(settings)
    partitions = _partitions(settings, args)
    # dbt (and sqlfluff's dbt templater) open the warehouse file; its directory has to exist.
    settings.warehouse_path.parent.mkdir(parents=True, exist_ok=True)
    for year, state in partitions:
        landed = land_partition(settings, source, year, state, refresh=args.refresh)
        rows = convert_partition(settings, year, state)
        lar = landed["hmda_lar"]
        print(
            f"  {year}/{state}  {lar.status:<10}  {lar.manifest.bytes:>12,} bytes  "
            f"{rows['hmda_lar']:>8,} rows  sha256 {lar.manifest.sha256[:12]}"
        )
    if not args.skip_reference:
        ref = ingest_reference(settings, source, refresh=args.refresh)
        print(f"  reference: pmms {ref['pmms']:,} rows, census_counties {ref['census_counties']:,} rows")
    n = build_manifest_index(settings.raw_dir, settings.bronze_dir)
    print(f"  manifest index: {n} files")
    return 0


def cmd_ingest_partition(settings: Settings, args) -> int:
    """One partition, no manifest index. For parallel (mapped) orchestration."""
    from mmp.ingest.lar import convert_partition, land_partition
    from mmp.ingest.sources import make_source

    landed = land_partition(settings, make_source(settings), args.year, args.state, refresh=args.refresh)
    rows = convert_partition(settings, args.year, args.state)
    lar = landed["hmda_lar"].manifest
    print(
        json.dumps(
            {
                "activity_year": args.year,
                "state_code": args.state,
                "status": landed["hmda_lar"].status,
                "bytes": lar.bytes,
                "sha256": lar.sha256,
                "rows": rows["hmda_lar"],
            }
        )
    )
    return 0


def cmd_ingest_reference(settings: Settings, args) -> int:
    from mmp.ingest.reference import ingest_reference
    from mmp.ingest.sources import make_source

    print(json.dumps(ingest_reference(settings, make_source(settings), refresh=args.refresh)))
    return 0


def cmd_index_manifest(settings: Settings, args) -> int:
    from mmp.ingest.manifest import build_manifest_index

    print(f"manifest index: {build_manifest_index(settings.raw_dir, settings.bronze_dir)} files")
    return 0


def cmd_freshness(settings: Settings, args) -> int:
    from mmp import dbt_runner

    with telemetry.step(settings.telemetry_dir, "dbt.source_freshness", "dbt source freshness"):
        dbt_runner.run_dbt(settings, ["source", "freshness"])
    return 0


def cmd_dbt_build(settings: Settings, args) -> int:
    from mmp import dbt_runner

    result = dbt_runner.build(
        settings,
        full_refresh=args.full_refresh,
        reprocess_years=args.reprocess_years,
        select=args.select,
    )
    print(f"dbt build: {result.counts} in {result.elapsed_s:.1f}s")
    return 0


def cmd_quality_gate(settings: Settings, args) -> int:
    from mmp.quality.gate import QualityGateFailed, format_results, run_gate

    expected = _partitions(settings, args)
    try:
        results = run_gate(settings, expected)
    except QualityGateFailed as exc:
        print("quality gate: FAILED")
        print(format_results(exc.results))
        return 1
    print("quality gate: passed")
    print(format_results(results))
    return 0


def cmd_publish(settings: Settings, args) -> int:
    from mmp.publish import publish

    for name, path in publish(settings).items():
        print(f"  published {name}: {path}")
    return 0


def cmd_report(settings: Settings, args) -> int:
    from mmp.report.build import build_report

    out = Path(args.out) if args.out else settings.data_dir / "published" / "report"
    print(f"report: {build_report(settings, out)}")
    return 0


def cmd_telemetry_load(settings: Settings, args) -> int:
    from mmp.report.cost import load_run_metrics

    print(f"telemetry loaded: {load_run_metrics(settings)}")
    return 0


def cmd_cost_report(settings: Settings, args) -> int:
    from mmp.report.cost import load_run_metrics, write_cost_report

    load_run_metrics(settings)
    print(write_cost_report(settings))
    return 0


def cmd_checksum(settings: Settings, args) -> int:
    with warehouse.connect(settings.warehouse_path, read_only=True) as con:
        rows = warehouse.checksums(con)
    if args.years:
        keep = {str(y) for y in args.years}
        rows = [r for r in rows if r["partition"] == "-" or r["partition"].split("/")[0] in keep]
    lines = [f"{r['table']:<38} {r['partition']:<8} {r['rows']:>9,} {r['checksum']}" for r in rows]
    text = "\n".join(lines) + "\n"
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(text)
    sys.stdout.write(text)
    return 0


def cmd_catalog(settings: Settings, args) -> int:
    from mmp.publish import catalog_markdown, catalog_payload
    from mmp.semantic.layer import SemanticLayer

    layer = SemanticLayer(settings)
    print(catalog_markdown(layer) if args.markdown else json.dumps(catalog_payload(layer), indent=2))
    return 0


def cmd_query(settings: Settings, args) -> int:
    """Query a governed metric from the shell, through the same path the MCP server uses."""
    from mmp.semantic.layer import MetricRequest, SemanticLayer

    layer = SemanticLayer(settings)
    request = MetricRequest.from_args(
        args.metric,
        dimensions=args.dimensions,
        filters=[json.loads(f) for f in args.filter or []],
        grain=args.grain,
        limit=args.limit,
        order_by=args.order_by,
    )
    if args.explain:
        print(layer.compile(request).sql)
        return 0
    print(json.dumps(layer.query(request).to_dict(include_sql=False), indent=2, default=str))
    return 0


def cmd_eval(settings: Settings, args) -> int:
    from mmp.evals.deterministic import format_report, run_deterministic

    report = run_deterministic(settings, Path(args.golden) if args.golden else None)
    print(format_report(report))
    if args.out:
        Path(args.out).write_text(json.dumps(report, indent=2, default=str))
    return 0 if report["failed"] == 0 else 1


def cmd_eval_llm(settings: Settings, args) -> int:
    from mmp.evals.llm_runner import main as llm_main

    return llm_main(settings, args)


# ------------------------------------------------------------------ pipelines


def _banner(text: str) -> None:
    print(f"\n== {text}")


def cmd_pipeline(settings: Settings, args) -> int:
    """All steps in order. The no-Docker equivalent of the Airflow DAG."""
    first_build = not _fact_exists(settings)
    kind = "full_refresh" if (args.full_refresh or first_build) else "incremental"
    os.environ.setdefault("MMP_RUN_KIND", kind)
    run_id = telemetry.current_run_id()
    t0 = time.monotonic()
    print(
        f"run {run_id}  kind={os.environ['MMP_RUN_KIND']}  scope={settings.scope.name}  "
        f"years={list(_years(settings, args))}  states={list(settings.scope.states)}  source={settings.source_mode}"
    )

    _banner("ingest: land raw files, write manifests, convert to bronze Parquet")
    args.refresh = getattr(args, "refresh", False)
    args.skip_reference = False
    cmd_ingest(settings, args)

    _banner("dbt source freshness")
    cmd_freshness(settings, args)

    _banner("dbt build (seeds, snapshot, models, data tests, unit tests)")
    args.select = None
    cmd_dbt_build(settings, args)

    _banner("data quality gate")
    # The gate checks every partition the warehouse is supposed to hold, not just this run's.
    gate_args = argparse.Namespace(
        years=sorted(_loaded_years(settings) | set(_years(settings, args))), states=None
    )
    if cmd_quality_gate(settings, gate_args) != 0:
        cmd_telemetry_load(settings, args)
        return 1

    _banner("publish metrics")
    cmd_publish(settings, args)
    if not args.no_report:
        args.out = None
        cmd_report(settings, args)

    _banner("load run telemetry")
    cmd_telemetry_load(settings, args)
    print(f"\npipeline finished in {time.monotonic() - t0:.1f}s (run {run_id})")
    return 0


def _fact_exists(settings: Settings) -> bool:
    if not settings.warehouse_path.exists():
        return False
    with warehouse.connect(settings.warehouse_path, read_only=True) as con:
        return warehouse.table_exists(con, "marts.fct_loan_applications")


def _loaded_years(settings: Settings) -> set[int]:
    """Years that have landed LAR files for the scope's states."""
    from mmp.ingest.lar import partition_dir
    from mmp.ingest.manifest import Manifest

    years = set()
    for path in (settings.raw_dir / "hmda_lar").glob("year=*"):
        year = int(path.name.split("=")[1])
        if all(
            Manifest.read(partition_dir(settings.raw_dir, "hmda_lar", year, s)) for s in settings.scope.states
        ):
            years.add(year)
    return years


def cmd_backfill(settings: Settings, args) -> int:
    """Load (or reload) specific activity years and rewrite exactly those partitions."""
    os.environ.setdefault("MMP_RUN_KIND", "backfill")
    years = list(args.years or settings.scope.backfill_years)
    if not years:
        print("no years given and the scope has no backfill_years")
        return 2
    args.years = years
    args.reprocess_years = years
    args.full_refresh = False
    return cmd_pipeline(settings, args)


# --------------------------------------------------------------------- parser


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mmp", description=__doc__.splitlines()[0])
    parser.add_argument("--scope", help="scope from config/pipeline.yml (default: MMP_SCOPE or demo)")
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    def add(name: str, fn, help_text: str) -> argparse.ArgumentParser:
        p = sub.add_parser(name, help=help_text)
        p.set_defaults(fn=fn)
        return p

    def years_states(p: argparse.ArgumentParser) -> None:
        p.add_argument("--years", type=int, nargs="+", help="activity years (default: the scope's years)")
        p.add_argument("--states", nargs="+", help="states (default: the scope's states)")

    p = add("ingest", cmd_ingest, "land and convert every partition in scope, plus reference data")
    years_states(p)
    p.add_argument("--refresh", action="store_true", help="re-download even if the cached file is intact")
    p.add_argument("--skip-reference", action="store_true")

    p = add("ingest-partition", cmd_ingest_partition, "land and convert one (year, state) partition")
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--state", required=True)
    p.add_argument("--refresh", action="store_true")

    p = add("ingest-reference", cmd_ingest_reference, "land and convert PMMS and Census county files")
    p.add_argument("--refresh", action="store_true")

    add("index-manifest", cmd_index_manifest, "fold partition manifests into bronze/ingest_manifest")
    add("freshness", cmd_freshness, "dbt source freshness")

    p = add("dbt-build", cmd_dbt_build, "dbt build with telemetry")
    p.add_argument("--full-refresh", action="store_true")
    p.add_argument("--reprocess-years", type=int, nargs="+", default=[])
    p.add_argument("--select")

    p = add("quality-gate", cmd_quality_gate, "reconciliation, volume, freshness and completeness checks")
    years_states(p)

    add("publish", cmd_publish, "write the metric catalogue and snapshot")
    p = add("report", cmd_report, "build the static HTML report")
    p.add_argument("--out", help="output directory (default: <data>/published/report)")
    add("telemetry-load", cmd_telemetry_load, "load step telemetry into ops.run_metrics")
    add("cost-report", cmd_cost_report, "time and bytes per run, model and partition")

    p = add("checksum", cmd_checksum, "row counts and content checksums of the marts")
    p.add_argument("--years", type=int, nargs="+")
    p.add_argument("--out")

    p = add("catalog", cmd_catalog, "print the governed metric catalogue")
    p.add_argument("--markdown", action="store_true")

    p = add("query", cmd_query, "query a governed metric")
    p.add_argument("metric")
    p.add_argument("-d", "--dimensions", nargs="*", default=[])
    p.add_argument(
        "-f",
        "--filter",
        action="append",
        help='JSON, e.g. \'{"dimension":"state_code","op":"=","value":"DE"}\'',
    )
    p.add_argument("--grain")
    p.add_argument("--limit", type=int)
    p.add_argument("--order-by", nargs="*", default=[])
    p.add_argument("--explain", action="store_true", help="print the compiled SQL instead of running it")

    p = add("eval", cmd_eval, "deterministic evals: golden tool calls vs independent SQL")
    p.add_argument("--golden")
    p.add_argument("--out")

    p = add("eval-llm", cmd_eval_llm, "optional LLM eval through the MCP server (needs ANTHROPIC_API_KEY)")
    p.add_argument("--golden")
    p.add_argument("--model", default=None)
    p.add_argument("--limit", type=int, help="only the first N questions")
    p.add_argument("--out", default=None)

    p = add("pipeline", cmd_pipeline, "run every step end to end")
    p.add_argument("--years", type=int, nargs="+")
    p.add_argument("--full-refresh", action="store_true")
    p.add_argument("--reprocess-years", type=int, nargs="+", default=[])
    p.add_argument("--refresh", action="store_true", help="re-download raw files")
    p.add_argument("--no-report", action="store_true")

    p = add("backfill", cmd_backfill, "load specific years and rewrite exactly those partitions")
    p.add_argument("--years", type=int, nargs="+")
    p.add_argument("--refresh", action="store_true")
    p.add_argument("--no-report", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    settings = load_settings(args.scope)
    os.environ.setdefault("MMP_REPO_ROOT", str(REPO_ROOT))
    try:
        return int(args.fn(settings, args))
    except Exception as exc:
        if args.verbose:
            raise
        print(f"error: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
