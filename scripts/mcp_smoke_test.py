"""Smoke test: start the MCP server as a subprocess and call every tool over stdio.

    uv run python scripts/mcp_smoke_test.py

Exits non-zero if any call misbehaves. This is a real client session against
the real server and the warehouse built by `make pipeline`.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

from mcp import ClientSession, StdioServerParameters, stdio_client

REPO = Path(__file__).resolve().parents[1]


def payload(result):
    """Tool result as Python data: structured content if present, else the JSON text block."""
    structured = getattr(result, "structured_content", None) or getattr(result, "structuredContent", None)
    if structured is not None:
        return structured.get("result", structured) if isinstance(structured, dict) else structured
    text = "".join(block.text for block in result.content if getattr(block, "type", "") == "text")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def is_error(result) -> bool:
    return bool(getattr(result, "is_error", None) or getattr(result, "isError", None))


async def run() -> int:
    params = StdioServerParameters(
        command=sys.executable,
        args=["-m", "mmp.mcp_server"],
        env={**os.environ, "MMP_REPO_ROOT": str(REPO)},
        cwd=str(REPO),
    )
    failures: list[str] = []

    def check(name: str, ok: bool, detail: str) -> None:
        print(f"  {'ok  ' if ok else 'FAIL'}  {name}: {detail}")
        if not ok:
            failures.append(name)

    async with stdio_client(params) as (read, write), ClientSession(read, write) as session:
        init = await session.initialize()
        info = getattr(init, "server_info", None) or getattr(init, "serverInfo", None)
        print(f"connected to {info.name} {info.version}")

        tools = (await session.list_tools()).tools
        names = sorted(t.name for t in tools)
        expected = ["describe_metric", "get_lineage", "list_dimension_values", "list_metrics", "query_metric"]
        check("tools/list", names == expected, str(names))
        check("no SQL tool", not any("sql" in n.lower() for n in names), "no tool name mentions sql")

        metrics = payload(await session.call_tool("list_metrics", {}))
        check("list_metrics", len(metrics) >= 15, f"{len(metrics)} metrics")

        described = payload(await session.call_tool("describe_metric", {"metric": "approval_rate"}))
        check(
            "describe_metric",
            described["formula"] == "approval_count / decisioned_application_count",
            f"formula={described['formula']}, {len(described['dimensions'])} dimensions, grains={described['allowed_grains']}",
        )

        values = payload(
            await session.call_tool(
                "list_dimension_values", {"metric": "approval_rate", "dimension": "loan_purpose"}
            )
        )
        check(
            "list_dimension_values", len(values["values"]) >= 4, str([v["value"] for v in values["values"]])
        )

        q = payload(
            await session.call_tool(
                "query_metric",
                {
                    "metric": "approval_rate",
                    "dimensions": ["state_code"],
                    "filters": [{"dimension": "loan_purpose", "op": "=", "value": "Home purchase"}],
                    "grain": "year",
                },
            )
        )
        check(
            "query_metric",
            q["row_count"] > 0 and all(0 < r["approval_rate"] < 1 for r in q["rows"]),
            f"{q['row_count']} rows, first={q['rows'][0]}",
        )

        lineage = payload(await session.call_tool("get_lineage", {"metric": "approval_rate"}))
        check(
            "get_lineage",
            lineage["semantic_models"][0]["dbt_model"] == "fct_loan_applications",
            f"measures={lineage['measures']}, upstream nodes={len(lineage['upstream_dbt_nodes']['fct_loan_applications'])}",
        )

        # guardrails, exercised through the protocol
        bad_dim = await session.call_tool("query_metric", {"metric": "approval_rate", "dimensions": ["ssn"]})
        check("reject unknown dimension", is_error(bad_dim), str(payload(bad_dim))[:110])
        bad_limit = await session.call_tool("query_metric", {"metric": "approval_rate", "limit": 100000})
        check("reject oversized limit", is_error(bad_limit), str(payload(bad_limit))[:110])
        injection = await session.call_tool(
            "query_metric",
            {
                "metric": "application_count",
                "filters": [{"dimension": "state_code", "op": "=", "value": "DE' OR '1'='1"}],
            },
        )
        inj = payload(injection)
        check(
            "filter value is data, not SQL",
            not is_error(injection) and inj["rows"][0]["application_count"] in (None, 0),
            f"rows={inj['rows']}",
        )
        bad_op = await session.call_tool(
            "query_metric",
            {
                "metric": "application_count",
                "filters": [{"dimension": "state_code", "op": "like", "value": "%"}],
            },
        )
        check("reject unknown operator", is_error(bad_op), str(payload(bad_op))[:110])
        unknown = await session.call_tool(
            "query_metric", {"metric": "select * from marts.fct_loan_applications"}
        )
        check("reject unknown metric", is_error(unknown), str(payload(unknown))[:110])

    print("MCP smoke test:", "FAILED " + str(failures) if failures else "all checks passed")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(run()))
