"""Request validation for the semantic layer.

A request can name a governed metric, dimensions from that metric's allow-list,
structured filters on those dimensions, a grain and a row limit. It cannot carry
SQL. Filter values are checked for type and length and are rendered as escaped
literals inside MetricFlow's own filter templates, so nothing a caller sends is
ever concatenated into a query as code.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field, replace
from datetime import date
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from mmp.semantic.layer import MetricInfo

ACTIVITY_YEAR = "activity_year"
FIRST_ACTIVITY_YEAR = 2018

SCALAR_OPS = ("=", "!=", ">", ">=", "<", "<=")
LIST_OPS = ("in", "not_in")
ALLOWED_OPS = SCALAR_OPS + LIST_OPS
# Ordering comparisons only make sense on the year; categorical values are labels.
CATEGORICAL_OPS = ("=", "!=", "in", "not_in")

_CONTROL_CHARS = re.compile(r"[\x00-\x1f\x7f]")


class GuardrailError(ValueError):
    """The request is not allowed. The message is safe to show to the caller."""


@dataclass(frozen=True)
class Limits:
    default_rows: int = 100
    max_rows: int = 1000
    max_dimensions: int = 4
    max_filters: int = 8
    max_in_values: int = 50
    max_value_length: int = 120
    query_timeout_s: float = 15.0
    min_cell_size: int = 10

    @staticmethod
    def from_env() -> Limits:
        env = os.environ
        return Limits(
            default_rows=int(env.get("MMP_DEFAULT_ROWS", 100)),
            max_rows=int(env.get("MMP_MAX_ROWS", 1000)),
            query_timeout_s=float(env.get("MMP_QUERY_TIMEOUT_S", 15)),
            min_cell_size=int(env.get("MMP_MIN_CELL_SIZE", 10)),
        )


@dataclass(frozen=True)
class Filter:
    dimension: str
    op: str
    value: Any

    @staticmethod
    def from_dict(raw: Any) -> Filter:
        if not isinstance(raw, dict):
            raise GuardrailError("each filter must be an object with keys dimension, op, value")
        unknown = set(raw) - {"dimension", "op", "value"}
        if unknown:
            raise GuardrailError(f"unknown filter keys {sorted(unknown)}; allowed: dimension, op, value")
        missing = {"dimension", "op", "value"} - set(raw)
        if missing:
            raise GuardrailError(f"filter is missing {sorted(missing)}")
        return Filter(str(raw["dimension"]), str(raw["op"]).lower().strip(), raw["value"])


@dataclass(frozen=True)
class MetricRequest:
    metric: str
    dimensions: tuple[str, ...] = ()
    filters: tuple[Filter, ...] = ()
    grain: str | None = None
    limit: int | None = None
    order_by: tuple[str, ...] = field(default=())

    @staticmethod
    def from_args(
        metric: str,
        dimensions: list[str] | None = None,
        filters: list[dict] | None = None,
        grain: str | None = None,
        limit: int | None = None,
        order_by: list[str] | None = None,
    ) -> MetricRequest:
        if not isinstance(metric, str):
            raise GuardrailError("metric must be a string")
        for name, value in (("dimensions", dimensions), ("filters", filters), ("order_by", order_by)):
            if value is not None and not isinstance(value, list | tuple):
                raise GuardrailError(f"{name} must be a list")
        return MetricRequest(
            metric=metric,
            dimensions=tuple(str(d) for d in dimensions or ()),
            filters=tuple(Filter.from_dict(f) for f in filters or ()),
            grain=grain,
            limit=limit,
            order_by=tuple(str(o) for o in order_by or ()),
        )


def _check_scalar(value: Any, dimension: str, limits: Limits) -> None:
    if isinstance(value, bool) or not isinstance(value, str | int | float):
        raise GuardrailError(f"filter value for {dimension!r} must be a string or a number")
    if isinstance(value, str):
        if len(value) > limits.max_value_length:
            raise GuardrailError(
                f"filter value for {dimension!r} is longer than {limits.max_value_length} characters"
            )
        if _CONTROL_CHARS.search(value):
            raise GuardrailError(f"filter value for {dimension!r} contains control characters")


def _check_year(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int | str):
        raise GuardrailError("activity_year filter values must be integers such as 2024")
    try:
        year = int(value)
    except ValueError as exc:
        raise GuardrailError("activity_year filter values must be integers such as 2024") from exc
    if not FIRST_ACTIVITY_YEAR <= year <= date.today().year:
        raise GuardrailError(f"activity_year must be between {FIRST_ACTIVITY_YEAR} and {date.today().year}")
    return year


def validate_request(request: MetricRequest, catalog: dict[str, MetricInfo], limits: Limits) -> MetricRequest:
    """Return a normalised copy of the request, or raise GuardrailError."""
    if request.metric not in catalog:
        raise GuardrailError(f"unknown metric {request.metric!r}; call list_metrics for the catalogue")
    info = catalog[request.metric]
    allowed = info.dimensions

    dimensions = list(dict.fromkeys(request.dimensions))
    grain = (request.grain or "").strip().lower() or None
    if grain in ("total", "none", "all"):
        grain = None
    if grain is not None:
        if grain not in info.grains:
            raise GuardrailError(
                f"grain {grain!r} is not available for {info.name}; allowed grains: {list(info.grains) or ['total']}"
            )
        if ACTIVITY_YEAR not in dimensions:
            dimensions.insert(0, ACTIVITY_YEAR)

    unknown = [d for d in dimensions if d not in allowed]
    if unknown:
        raise GuardrailError(
            f"dimension(s) {unknown} are not allowed for metric {info.name}; allowed: {sorted(allowed)}"
        )
    if len(dimensions) > limits.max_dimensions:
        raise GuardrailError(f"at most {limits.max_dimensions} dimensions per query")

    if len(request.filters) > limits.max_filters:
        raise GuardrailError(f"at most {limits.max_filters} filters per query")
    filters: list[Filter] = []
    for f in request.filters:
        if f.dimension not in allowed:
            raise GuardrailError(
                f"cannot filter on {f.dimension!r} for metric {info.name}; allowed: {sorted(allowed)}"
            )
        if f.op not in ALLOWED_OPS:
            raise GuardrailError(f"filter operator {f.op!r} is not allowed; allowed: {list(ALLOWED_OPS)}")
        is_year = f.dimension == ACTIVITY_YEAR
        if not is_year and f.op not in CATEGORICAL_OPS:
            raise GuardrailError(
                f"operator {f.op!r} is only allowed on activity_year; use one of {list(CATEGORICAL_OPS)}"
            )
        if f.op in LIST_OPS:
            if not isinstance(f.value, list | tuple) or not f.value:
                raise GuardrailError(f"operator {f.op!r} needs a non-empty list of values")
            if len(f.value) > limits.max_in_values:
                raise GuardrailError(f"at most {limits.max_in_values} values in an {f.op!r} filter")
            values = tuple(_check_year(v) for v in f.value) if is_year else tuple(f.value)
            if not is_year:
                for v in values:
                    _check_scalar(v, f.dimension, limits)
            filters.append(Filter(f.dimension, f.op, values))
        else:
            if isinstance(f.value, list | tuple | dict):
                raise GuardrailError(f"operator {f.op!r} takes a single value, not a list")
            if is_year:
                filters.append(Filter(f.dimension, f.op, _check_year(f.value)))
            else:
                _check_scalar(f.value, f.dimension, limits)
                filters.append(Filter(f.dimension, f.op, f.value))

    limit = request.limit if request.limit is not None else limits.default_rows
    if isinstance(limit, bool) or not isinstance(limit, int):
        raise GuardrailError("limit must be an integer")
    if limit < 1:
        raise GuardrailError("limit must be at least 1")
    if limit > limits.max_rows:
        raise GuardrailError(f"limit {limit} exceeds the maximum of {limits.max_rows} rows")

    order_by: list[str] = []
    for item in request.order_by:
        name = item.lstrip("-")
        if item.count("-") > 1 and item.startswith("--"):
            raise GuardrailError(f"invalid order_by entry {item!r}")
        if name != info.name and name not in dimensions:
            raise GuardrailError(
                f"order_by {item!r} must be the metric or one of the requested dimensions {dimensions}"
            )
        order_by.append(item)

    return replace(
        request,
        dimensions=tuple(dimensions),
        filters=tuple(filters),
        grain=grain,
        limit=limit,
        order_by=tuple(order_by),
    )


def sql_literal(value: Any) -> str:
    """Render a validated scalar as a SQL literal."""
    if isinstance(value, bool):
        raise GuardrailError("boolean literals are not supported")
    if isinstance(value, int | float):
        return repr(value)
    return "'" + str(value).replace("'", "''") + "'"


def _dimension_template(qualified_name: str) -> str:
    """MetricFlow filter reference for a (possibly multi-hop) dimension."""
    parts = qualified_name.split("__")
    if len(parts) <= 2:
        return "{{ Dimension('" + qualified_name + "') }}"
    entity_path = ", ".join(f"'{p}'" for p in parts[:-2])
    return "{{ Dimension('" + "__".join(parts[-2:]) + "', entity_path=[" + entity_path + "]) }}"


def render_where(filters: tuple[Filter, ...], info: MetricInfo) -> list[str]:
    """Turn validated filters into MetricFlow where-clause strings."""
    clauses: list[str] = []
    for f in filters:
        if f.dimension == ACTIVITY_YEAR:
            ref = "{{ TimeDimension('metric_time', 'year') }}"

            def lit(year: int) -> str:
                return f"'{year:04d}-01-01'"
        else:
            ref = _dimension_template(info.dimensions[f.dimension].qualified_name)
            lit = sql_literal
        if f.op in LIST_OPS:
            keyword = "in" if f.op == "in" else "not in"
            clauses.append(f"{ref} {keyword} ({', '.join(lit(v) for v in f.value)})")
        else:
            op = "<>" if f.op == "!=" else f.op
            clauses.append(f"{ref} {op} {lit(f.value)}")
    return clauses
