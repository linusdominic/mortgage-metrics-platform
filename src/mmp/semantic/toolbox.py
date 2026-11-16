"""The agent-facing tool implementations, independent of any transport.

The MCP server registers these functions as tools. The deterministic evals and
the unit tests call them directly. Either way a call is validated, executed
through SemanticLayer and written to the audit log.
"""

from __future__ import annotations

import hashlib
import json
import threading
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from mmp.semantic.guardrails import ALLOWED_OPS, GuardrailError, MetricRequest
from mmp.semantic.layer import SemanticLayer, SemanticLayerError

MAX_DIMENSION_VALUES = 200


class ToolRejected(Exception):
    """Raised to the transport for a request the guardrails refused."""


class ToolFailed(Exception):
    """Raised to the transport for a request that was allowed but could not be served."""


class AuditLog:
    """Append-only JSONL log of every tool call, including rejected ones."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.Lock()
        path.parent.mkdir(parents=True, exist_ok=True)

    def write(self, entry: dict[str, Any]) -> None:
        line = json.dumps(entry, default=str, sort_keys=True)
        with self._lock, self.path.open("a") as fh:
            fh.write(line + "\n")


class MetricsToolbox:
    def __init__(self, layer: SemanticLayer, audit: AuditLog | None = None, client: str = "unknown") -> None:
        self.layer = layer
        self.audit = audit
        self.client = client

    # ---------------------------------------------------------------- plumbing

    def _call(self, tool: str, arguments: dict[str, Any], fn) -> Any:
        request_id = uuid.uuid4().hex[:12]
        entry: dict[str, Any] = {
            "ts": datetime.now(UTC).isoformat(),
            "request_id": request_id,
            "client": self.client,
            "tool": tool,
            "arguments": arguments,
        }
        t0 = time.monotonic()
        try:
            result, extra = fn()
            entry.update(status="ok", **extra)
            return result
        except GuardrailError as exc:
            entry.update(status="rejected", error=str(exc))
            raise ToolRejected(f"Request rejected: {exc}") from exc
        except SemanticLayerError as exc:
            entry.update(status="error", error=f"{type(exc).__name__}: {exc}")
            raise ToolFailed(str(exc)) from exc
        except Exception as exc:
            entry.update(status="error", error=f"{type(exc).__name__}: {exc}")
            raise ToolFailed(
                f"internal error ({type(exc).__name__}); see the audit log entry {request_id}"
            ) from exc
        finally:
            entry["elapsed_ms"] = round((time.monotonic() - t0) * 1000, 1)
            if self.audit:
                self.audit.write(entry)

    # ------------------------------------------------------------------- tools

    def list_metrics(self) -> list[dict[str, Any]]:
        def run():
            metrics = [
                {
                    "name": m.name,
                    "label": m.label,
                    "type": m.type,
                    "unit": m.unit,
                    "description": m.description,
                }
                for m in self.layer.catalog().values()
            ]
            return metrics, {"row_count": len(metrics)}

        return self._call("list_metrics", {}, run)

    def describe_metric(self, metric: str) -> dict[str, Any]:
        def run():
            info = self.layer.describe(metric)
            limits = self.layer.limits
            payload = {
                "name": info.name,
                "label": info.label,
                "type": info.type,
                "unit": info.unit,
                "description": info.description,
                "formula": f"{info.numerator} / {info.denominator}" if info.type == "ratio" else None,
                "allowed_grains": ["total", *info.grains],
                "dimensions": [
                    {
                        "name": d.name,
                        "kind": d.kind,
                        "description": d.description,
                        "demographic": d.is_demographic,
                    }
                    for d in info.dimensions.values()
                ],
                "filter_operators": list(ALLOWED_OPS),
                "limits": {
                    "default_rows": limits.default_rows,
                    "max_rows": limits.max_rows,
                    "max_dimensions": limits.max_dimensions,
                    "max_filters": limits.max_filters,
                    "query_timeout_seconds": limits.query_timeout_s,
                    "min_cell_size_for_demographic_breakdowns": limits.min_cell_size,
                },
            }
            return payload, {}

        return self._call("describe_metric", {"metric": metric}, run)

    def query_metric(
        self,
        metric: str,
        dimensions: list[str] | None = None,
        filters: list[dict[str, Any]] | None = None,
        grain: str | None = None,
        limit: int | None = None,
        order_by: list[str] | None = None,
    ) -> dict[str, Any]:
        arguments = {
            "metric": metric,
            "dimensions": dimensions or [],
            "filters": filters or [],
            "grain": grain,
            "limit": limit,
            "order_by": order_by or [],
        }

        def run():
            request = MetricRequest.from_args(metric, dimensions, filters, grain, limit, order_by)
            result = self.layer.query(request)
            return result.to_dict(), {
                "row_count": result.row_count,
                "truncated": result.truncated,
                "suppressed_rows": result.suppressed_rows,
                "query_ms": round(result.elapsed_ms, 1),
                "sql_sha256": hashlib.sha256(result.sql.encode()).hexdigest(),
            }

        return self._call("query_metric", arguments, run)

    def list_dimension_values(self, metric: str, dimension: str) -> dict[str, Any]:
        """Distinct values of a dimension, with record counts, so filters can use exact labels."""

        def run():
            info = self.layer.describe(metric)
            if dimension not in info.dimensions:
                raise GuardrailError(
                    f"dimension {dimension!r} is not allowed for metric {metric}; allowed: {sorted(info.dimensions)}"
                )
            counter = self.layer._support_metric(info)
            result = self.layer.query(
                MetricRequest.from_args(
                    counter, [dimension], limit=MAX_DIMENSION_VALUES, order_by=[f"-{counter}"]
                )
            )
            values = [{"value": r[dimension], "records": r[counter]} for r in result.rows]
            return {
                "metric": metric,
                "dimension": dimension,
                "values": values,
                "truncated": result.truncated,
            }, {"row_count": len(values)}

        return self._call("list_dimension_values", {"metric": metric, "dimension": dimension}, run)

    def get_lineage(self, metric: str) -> dict[str, Any]:
        def run():
            return self.layer.lineage(metric), {}

        return self._call("get_lineage", {"metric": metric}, run)

    def dispatch(self, tool: str, arguments: dict[str, Any]) -> Any:
        """Call a tool by name. Used by the evals; unknown tools and arguments are rejected."""
        tools = {
            "list_metrics": self.list_metrics,
            "describe_metric": self.describe_metric,
            "query_metric": self.query_metric,
            "list_dimension_values": self.list_dimension_values,
            "get_lineage": self.get_lineage,
        }
        if tool not in tools:
            raise ToolRejected(f"Request rejected: unknown tool {tool!r}")
        try:
            return tools[tool](**arguments)
        except TypeError as exc:
            raise ToolRejected(f"Request rejected: bad arguments for {tool}: {exc}") from exc
