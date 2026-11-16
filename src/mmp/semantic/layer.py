"""The semantic layer: one compile path from a metric request to SQL to rows.

Metrics, dimensions and joins are defined once, in dbt (models/semantic/*.yml).
MetricFlow compiles a request into SQL. This module is the only code that talks
to MetricFlow, and everything that reads a metric goes through it: the MCP
server, the static report, the published snapshots and the evals.

    MetricRequest -> validate (guardrails) -> MetricFlow explain -> SQL
                  -> read-only DuckDB with a timeout -> QueryResult

MetricFlow is used as a compiler only. It is given a client that can render the
DuckDB dialect but cannot execute, so the dbt adapter never opens the warehouse
file and the only connection made at query time is the read-only one below.
"""

from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb

from mmp.semantic.guardrails import (
    ACTIVITY_YEAR,
    Filter,
    GuardrailError,
    Limits,
    MetricRequest,
    render_where,
    validate_request,
)
from mmp.settings import Settings

METRIC_TIME_YEAR = "metric_time__year"
DEMOGRAPHIC_SEMANTIC_MODEL = "applicant_demographics"


class SemanticLayerError(RuntimeError):
    pass


class QueryTimeout(SemanticLayerError):
    pass


class WarehouseUnavailable(SemanticLayerError):
    pass


@dataclass(frozen=True)
class DimensionInfo:
    name: str  # name exposed to consumers
    qualified_name: str  # MetricFlow group-by name
    description: str
    kind: str  # categorical | time
    is_demographic: bool = False


@dataclass(frozen=True)
class MetricInfo:
    name: str
    label: str
    description: str
    type: str  # simple | ratio
    unit: str
    dimensions: dict[str, DimensionInfo]
    grains: tuple[str, ...]
    numerator: str | None = None
    denominator: str | None = None
    measures: tuple[str, ...] = ()
    semantic_models: tuple[str, ...] = ()


@dataclass(frozen=True)
class CompiledQuery:
    sql: str
    output_columns: tuple[str, ...]  # consumer-facing names, in SQL column order
    metric: str
    support_column: str | None  # column used for small-cell suppression, if any
    limit: int


@dataclass
class QueryResult:
    metric: str
    unit: str
    columns: list[str]
    rows: list[dict[str, Any]]
    row_count: int
    truncated: bool
    suppressed_rows: int
    elapsed_ms: float
    sql: str
    notes: list[str] = field(default_factory=list)

    def to_dict(self, include_sql: bool = True) -> dict[str, Any]:
        out = {
            "metric": self.metric,
            "unit": self.unit,
            "columns": self.columns,
            "rows": self.rows,
            "row_count": self.row_count,
            "truncated": self.truncated,
            "suppressed_rows": self.suppressed_rows,
            "elapsed_ms": round(self.elapsed_ms, 1),
            "notes": self.notes,
        }
        if include_sql:
            out["sql"] = self.sql
        return out


class _CompileOnlyClient:
    """Satisfies MetricFlow's SqlClient protocol for rendering; refuses to execute."""

    def __init__(self) -> None:
        from metricflow.protocols.sql_client import SqlEngine
        from metricflow.sql.render.duckdb_renderer import DuckDbSqlPlanRenderer

        self._engine = SqlEngine.DUCKDB
        self._renderer = DuckDbSqlPlanRenderer()

    @property
    def sql_engine_type(self):
        return self._engine

    @property
    def sql_plan_renderer(self):
        return self._renderer

    def _refuse(self, *_args, **_kwargs):
        raise SemanticLayerError("the MetricFlow client in this process is compile-only")

    query = execute = dry_run = _refuse

    def close(self) -> None:
        return None

    def render_bind_parameter_key(self, bind_parameter_key: str) -> str:
        return f"${bind_parameter_key}"


class SemanticLayer:
    def __init__(
        self,
        settings: Settings,
        limits: Limits | None = None,
        semantic_manifest_path: Path | None = None,
        dbt_manifest_path: Path | None = None,
    ) -> None:
        self.settings = settings
        self.limits = limits or Limits.from_env()
        target = settings.dbt_target_dir
        self._semantic_manifest_path = semantic_manifest_path or target / "semantic_manifest.json"
        self._dbt_manifest_path = dbt_manifest_path or target / "manifest.json"
        self._engine = None
        self._catalog: dict[str, MetricInfo] | None = None
        self._raw_semantic_manifest: dict | None = None
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ setup

    def _load_engine(self):
        if self._engine is None:
            if not self._semantic_manifest_path.exists():
                raise SemanticLayerError(
                    f"{self._semantic_manifest_path} not found; run `mmp dbt-build` (or `dbt parse`) first"
                )
            from metricflow.engine.metricflow_engine import MetricFlowEngine
            from metricflow_semantics.model.dbt_manifest_parser import (
                parse_manifest_from_dbt_generated_manifest,
            )
            from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup

            raw = self._semantic_manifest_path.read_text()
            self._raw_semantic_manifest = json.loads(raw)
            manifest = parse_manifest_from_dbt_generated_manifest(manifest_json_string=raw)
            self._engine = MetricFlowEngine(
                semantic_manifest_lookup=SemanticManifestLookup(manifest),
                sql_client=_CompileOnlyClient(),
            )
        return self._engine

    def catalog(self) -> dict[str, MetricInfo]:
        """Every governed metric with its allowed dimensions and grains."""
        with self._lock:
            if self._catalog is None:
                self._catalog = self._build_catalog()
            return self._catalog

    def _build_catalog(self) -> dict[str, MetricInfo]:
        engine = self._load_engine()
        raw_metrics = {m["name"]: m for m in self._raw_semantic_manifest["metrics"]}
        catalog: dict[str, MetricInfo] = {}
        for metric in sorted(engine.list_metrics(), key=lambda m: m.name):
            dims: dict[str, DimensionInfo] = {}
            has_time = False
            candidates = []
            for dim in metric.dimensions:
                if str(dim.type.value).lower() == "time" or dim.name == "metric_time":
                    has_time = True
                    continue
                candidates.append(dim)
            bare_names = [d.name for d in candidates]
            for dim in candidates:
                exposed = dim.name if bare_names.count(dim.name) == 1 else dim.dunder_name
                model = (
                    dim.semantic_model_reference.semantic_model_name if dim.semantic_model_reference else ""
                )
                dims[exposed] = DimensionInfo(
                    name=exposed,
                    qualified_name=dim.dunder_name,
                    description=(dim.description or "").strip(),
                    kind="categorical",
                    is_demographic=model == DEMOGRAPHIC_SEMANTIC_MODEL,
                )
            if has_time:
                dims[ACTIVITY_YEAR] = DimensionInfo(
                    name=ACTIVITY_YEAR,
                    qualified_name=METRIC_TIME_YEAR,
                    description="HMDA activity year (integer). The only time grain in the public LAR.",
                    kind="time",
                )
            raw = raw_metrics.get(metric.name, {})
            params = raw.get("type_params", {}) or {}
            meta = ((raw.get("config") or {}).get("meta")) or {}
            catalog[metric.name] = MetricInfo(
                name=metric.name,
                label=metric.label or metric.name,
                description=" ".join((metric.description or "").split()),
                type=str(metric.type.value).lower(),
                unit=str(meta.get("unit", "number")),
                dimensions=dict(sorted(dims.items())),
                grains=("year",) if has_time else (),
                numerator=(params.get("numerator") or {}).get("name"),
                denominator=(params.get("denominator") or {}).get("name"),
                measures=tuple(m["name"] for m in params.get("input_measures", []) or []),
                semantic_models=tuple(sorted(ref.semantic_model_name for ref in metric.semantic_models)),
            )
        return catalog

    def describe(self, metric: str) -> MetricInfo:
        catalog = self.catalog()
        if metric not in catalog:
            raise GuardrailError(f"unknown metric {metric!r}; known metrics: {sorted(catalog)}")
        return catalog[metric]

    # ---------------------------------------------------------------- compile

    def _support_metric(self, info: MetricInfo) -> str:
        """Record-count metric used to decide whether a demographic cell is too small to show."""
        return info.name if info.semantic_models == ("denial_reasons",) else "lar_record_count"

    def compile(self, request: MetricRequest) -> CompiledQuery:
        """Validate a request and compile it to SQL. The only compile path."""
        from metricflow.engine.metricflow_engine import MetricFlowQueryRequest

        catalog = self.catalog()
        request = validate_request(request, catalog, self.limits)
        info = catalog[request.metric]
        engine = self._load_engine()

        dims = [info.dimensions[name] for name in request.dimensions]
        group_by = [d.qualified_name for d in dims]
        metric_names = [info.name]
        support_column = None
        if any(d.is_demographic for d in dims) and self.limits.min_cell_size > 1:
            support = self._support_metric(info)
            support_column = support
            if support != info.name:
                metric_names.append(support)

        order_by = []
        for item in request.order_by:
            descending = item.startswith("-")
            name = item.lstrip("-")
            qualified = info.dimensions[name].qualified_name if name in info.dimensions else name
            order_by.append(("-" if descending else "") + qualified)
        if not order_by:
            order_by = list(group_by)

        where = render_where(request.filters, info)
        mf_request = MetricFlowQueryRequest.create(
            metric_names=metric_names,
            group_by_names=group_by or None,
            where_constraints=where or None,
            order_by_names=order_by or None,
            # one extra row tells us whether the result was truncated
            limit=request.limit + 1,
        )
        try:
            explained = engine.explain(mf_request)
        except Exception as exc:  # MetricFlow raises many unrelated exception types
            raise SemanticLayerError(f"MetricFlow could not compile the request: {exc}") from exc
        sql = explained.sql_statement.sql

        return CompiledQuery(
            sql=sql,
            output_columns=tuple([d.name for d in dims] + metric_names),
            metric=info.name,
            support_column=support_column,
            limit=request.limit,
        )

    # ---------------------------------------------------------------- execute

    def _connect(self) -> duckdb.DuckDBPyConnection:
        path = self.settings.warehouse_path
        if not path.exists():
            raise WarehouseUnavailable(f"warehouse file {path} does not exist; run the pipeline first")
        try:
            return duckdb.connect(str(path), read_only=True)
        except duckdb.Error as exc:
            raise WarehouseUnavailable(
                f"could not open the warehouse read-only (a pipeline run may hold the write lock): {exc}"
            ) from exc

    def execute_sql(self, sql: str, timeout_s: float | None = None) -> tuple[list[str], list[tuple]]:
        """Run compiled SQL on a read-only connection with a hard timeout."""
        timeout_s = timeout_s or self.limits.query_timeout_s
        con = self._connect()
        timed_out = threading.Event()

        def _interrupt() -> None:
            timed_out.set()
            con.interrupt()

        timer = threading.Timer(timeout_s, _interrupt)
        timer.start()
        try:
            cursor = con.execute(sql)
            columns = [d[0] for d in cursor.description]
            rows = cursor.fetchall()
            return columns, rows
        except duckdb.Error as exc:
            if timed_out.is_set():
                raise QueryTimeout(f"query exceeded the {timeout_s:g}s timeout and was cancelled") from exc
            raise SemanticLayerError(f"query failed: {exc}") from exc
        finally:
            timer.cancel()
            con.close()

    def query(self, request: MetricRequest) -> QueryResult:
        """request -> SQL -> rows, with guardrails applied on the way in and out."""
        compiled = self.compile(request)
        info = self.catalog()[compiled.metric]
        t0 = time.monotonic()
        sql_columns, raw_rows = self.execute_sql(compiled.sql)
        elapsed_ms = (time.monotonic() - t0) * 1000

        truncated = len(raw_rows) > compiled.limit
        raw_rows = raw_rows[: compiled.limit]

        # MetricFlow names output columns by qualified name; map back to exposed names.
        qualified_to_exposed = {d.qualified_name: d.name for d in info.dimensions.values()}
        exposed = [qualified_to_exposed.get(c, c) for c in sql_columns]

        rows: list[dict[str, Any]] = []
        suppressed = 0
        for raw in raw_rows:
            row = {name: _plain(value, name) for name, value in zip(exposed, raw, strict=True)}
            if compiled.support_column is not None:
                support = row.get(compiled.support_column)
                if support is not None and support < self.limits.min_cell_size:
                    row[compiled.metric] = None
                    suppressed += 1
                if compiled.support_column != compiled.metric:
                    row.pop(compiled.support_column, None)
            rows.append(row)

        # Requested dimension order first, then the metric.
        columns = [
            c for c in compiled.output_columns if not (c == compiled.support_column and c != compiled.metric)
        ]
        rows = [{c: row.get(c) for c in columns} for row in rows]
        notes: list[str] = []
        if truncated:
            notes.append(
                f"Result truncated to the first {compiled.limit} rows; narrow the filters or raise limit."
            )
        if suppressed:
            notes.append(
                f"{suppressed} row(s) have fewer than {self.limits.min_cell_size} records and their "
                "metric value is withheld (small-cell suppression on demographic breakdowns)."
            )
        if any(info.dimensions[d].is_demographic for d in columns if d in info.dimensions):
            notes.append(
                "Demographic breakdowns of HMDA outcomes are descriptive. Differences are not evidence of "
                "discrimination without controlling for credit factors that public HMDA does not contain."
            )
        return QueryResult(
            metric=compiled.metric,
            unit=info.unit,
            columns=columns,
            rows=rows,
            row_count=len(rows),
            truncated=truncated,
            suppressed_rows=suppressed,
            elapsed_ms=elapsed_ms,
            sql=compiled.sql,
            notes=notes,
        )

    # ---------------------------------------------------------------- lineage

    def lineage(self, metric: str) -> dict[str, Any]:
        """Metric -> measures -> semantic models -> dbt models -> sources."""
        info = self.describe(metric)
        self._load_engine()
        semantic = self._raw_semantic_manifest
        metrics_by_name = {m["name"]: m for m in semantic["metrics"]}

        def input_measures(name: str) -> list[str]:
            params = metrics_by_name[name].get("type_params", {}) or {}
            return [m["name"] for m in params.get("input_measures", []) or []]

        measures = input_measures(metric)
        models: dict[str, dict[str, Any]] = {}
        for sm in semantic["semantic_models"]:
            used = [m["name"] for m in sm.get("measures", []) if m["name"] in measures]
            if used:
                relation = sm["node_relation"]
                models[sm["name"]] = {
                    "semantic_model": sm["name"],
                    "dbt_model": relation["alias"],
                    "relation": relation["relation_name"],
                    "measures": {
                        m["name"]: {"agg": m["agg"], "expr": m.get("expr")}
                        for m in sm["measures"]
                        if m["name"] in used
                    },
                }
        upstream: dict[str, list[str]] = {}
        if self._dbt_manifest_path.exists():
            manifest = json.loads(self._dbt_manifest_path.read_text())
            parent_map = manifest.get("parent_map", {})
            nodes = {**manifest.get("nodes", {}), **manifest.get("sources", {})}
            by_name = {
                n["name"]: uid
                for uid, n in manifest.get("nodes", {}).items()
                if n["resource_type"] == "model"
            }
            for entry in models.values():
                start = by_name.get(entry["dbt_model"])
                seen: list[str] = []
                stack = list(parent_map.get(start, [])) if start else []
                while stack:
                    uid = stack.pop()
                    node = nodes.get(uid)
                    if node is None or uid in seen:
                        continue
                    if node["resource_type"] in ("model", "seed", "source"):
                        seen.append(uid)
                        stack.extend(parent_map.get(uid, []))
                upstream[entry["dbt_model"]] = sorted(
                    f"{nodes[u]['resource_type']}:{nodes[u].get('source_name', '') + '.' if nodes[u]['resource_type'] == 'source' else ''}{nodes[u]['name']}"
                    for u in seen
                )
        return {
            "metric": metric,
            "type": info.type,
            "numerator": info.numerator,
            "denominator": info.denominator,
            "defined_in": "dbt/models/semantic/_metrics.yml",
            "measures": measures,
            "semantic_models": list(models.values()),
            "upstream_dbt_nodes": upstream,
        }


def _plain(value: Any, column: str) -> Any:
    """Convert DuckDB values to JSON-friendly ones; metric_time year becomes an int."""
    if column == ACTIVITY_YEAR and isinstance(value, datetime | date):
        return value.year
    if isinstance(value, datetime | date):
        return value.isoformat()
    if hasattr(value, "is_finite") and not isinstance(value, int | float):  # Decimal
        return float(value)
    return value


__all__ = [
    "CompiledQuery",
    "DimensionInfo",
    "Filter",
    "GuardrailError",
    "MetricInfo",
    "MetricRequest",
    "QueryResult",
    "QueryTimeout",
    "SemanticLayer",
    "SemanticLayerError",
    "WarehouseUnavailable",
]
