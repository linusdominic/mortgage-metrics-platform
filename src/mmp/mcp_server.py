"""MCP server (stdio) exposing the governed metrics to AI agents.

Tools: list_metrics, describe_metric, list_dimension_values, query_metric,
get_lineage. There is deliberately no tool that accepts SQL. Everything an agent
can ask for is a metric from the catalogue, sliced by dimensions on that
metric's allow-list, and is compiled by MetricFlow.

Run:  uv run mmp-mcp            (or: uv run python -m mmp.mcp_server)
"""

from __future__ import annotations

import logging
import sys
from typing import Annotated, Any

from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from pydantic import Field

from mmp.semantic.layer import SemanticLayer
from mmp.semantic.toolbox import AuditLog, MetricsToolbox, ToolFailed, ToolRejected
from mmp.settings import load_settings

INSTRUCTIONS = """\
Governed mortgage metrics over US HMDA Loan Application Register data (applications and
originations by lender, state, county, loan product and year; not loan performance).

Workflow: call list_metrics to find the metric, describe_metric to see its allowed
dimensions and grains, list_dimension_values when you need the exact label to filter on,
then query_metric. You cannot run SQL. Report numbers exactly as returned and say which
metric and filters produced them. activity_year is the only time grain."""


def _anticipated(call):
    """Run a toolbox call; hand refusals and known failures to the model as readable tool errors."""
    try:
        return call()
    except (ToolRejected, ToolFailed) as exc:
        raise ToolError(str(exc)) from exc


def build_server(toolbox: MetricsToolbox | None = None) -> MCPServer:
    if toolbox is None:
        settings = load_settings()
        toolbox = MetricsToolbox(
            SemanticLayer(settings), AuditLog(settings.audit_log_path), client="mcp-stdio"
        )
    server = MCPServer(
        name="mortgage-metrics", instructions=INSTRUCTIONS, version="0.1.0", log_level="WARNING"
    )

    @server.tool(description="List every governed metric with its label, unit and definition.")
    def list_metrics() -> list[dict[str, Any]]:
        return _anticipated(lambda: toolbox.list_metrics())

    @server.tool(
        description=(
            "Describe one metric: definition, formula for ratios, allowed dimensions, allowed grains, "
            "filter operators and query limits."
        )
    )
    def describe_metric(
        metric: Annotated[str, Field(description="Metric name from list_metrics, e.g. approval_rate")],
    ) -> dict[str, Any]:
        return _anticipated(lambda: toolbox.describe_metric(metric))

    @server.tool(
        description=(
            "List the distinct values of a dimension (with record counts) so a filter can use the exact "
            "label, e.g. loan_purpose -> 'Home purchase'."
        )
    )
    def list_dimension_values(
        metric: Annotated[str, Field(description="Metric the dimension will be used with")],
        dimension: Annotated[str, Field(description="Dimension name from describe_metric")],
    ) -> dict[str, Any]:
        return _anticipated(lambda: toolbox.list_dimension_values(metric, dimension))

    @server.tool(
        description=(
            "Query one governed metric. dimensions: names from describe_metric to group by. "
            "filters: list of {dimension, op, value}; op is one of =, !=, in, not_in, and additionally "
            ">, >=, <, <= on activity_year; value is a string or number, or a list for in / not_in. "
            "grain: 'year' to break out by activity_year, or 'total'. order_by: the metric name or a "
            "requested dimension, prefix with '-' for descending. limit: max rows (default 100, max 1000). "
            "Returns rows, the unit, notes and the compiled SQL for provenance."
        )
    )
    def query_metric(
        metric: Annotated[str, Field(description="Metric name, e.g. denial_rate")],
        dimensions: Annotated[list[str] | None, Field(description="Dimensions to group by")] = None,
        filters: Annotated[
            list[dict[str, Any]] | None,
            Field(description='e.g. [{"dimension": "state_code", "op": "=", "value": "DE"}]'),
        ] = None,
        grain: Annotated[str | None, Field(description="'year' or 'total' (default total)")] = None,
        limit: Annotated[int | None, Field(description="Maximum rows to return")] = None,
        order_by: Annotated[list[str] | None, Field(description="e.g. ['-origination_count']")] = None,
    ) -> dict[str, Any]:
        return _anticipated(lambda: toolbox.query_metric(metric, dimensions, filters, grain, limit, order_by))

    @server.tool(
        description=(
            "Lineage of a metric: its measures and their SQL expressions, the semantic model and dbt "
            "model they come from, and every upstream dbt model, seed and source."
        )
    )
    def get_lineage(
        metric: Annotated[str, Field(description="Metric name")],
    ) -> dict[str, Any]:
        return _anticipated(lambda: toolbox.get_lineage(metric))

    return server


def main() -> None:
    # stdout carries the protocol; anything else must go to stderr.
    logging.basicConfig(level=logging.WARNING, stream=sys.stderr)
    build_server().run("stdio")


if __name__ == "__main__":
    main()
