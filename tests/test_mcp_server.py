"""The MCP server, exercised through a real client session over stdio."""

import asyncio
import json
import os
import subprocess
import sys
from types import SimpleNamespace

from mmp.evals import llm_runner
from mmp.evals.golden import load_golden
from mmp.settings import REPO_ROOT


def test_smoke_script_calls_every_tool_over_stdio(settings):
    proc = subprocess.run(
        [sys.executable, str(REPO_ROOT / "scripts" / "mcp_smoke_test.py")],
        env=dict(os.environ),
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "all checks passed" in proc.stdout
    assert "reject unknown dimension" in proc.stdout and "FAIL" not in proc.stdout
    log = [json.loads(line) for line in settings.audit_log_path.read_text().splitlines()]
    assert {"ok", "rejected"} <= {e["status"] for e in log}
    assert all(e["client"] == "mcp-stdio" for e in log)


def test_llm_runner_works_against_the_real_server(settings):
    """Fake model, real MCP transport: tool schemas come from the server and calls go over stdio."""
    q = next(x for x in load_golden() if x.id == "q04")

    def script(messages):
        turn = sum(1 for m in messages if m["role"] == "assistant")
        if turn == 0:
            block = SimpleNamespace(type="tool_use", id="toolu_a", name="query_metric", input=q.expected_call)
        elif turn == 1:
            payload = json.loads(messages[-1]["content"][0]["content"])
            block = SimpleNamespace(
                type="tool_use",
                id="toolu_b",
                name="submit_answer",
                input={
                    "value": payload["rows"][0]["origination_count"],
                    "label": None,
                    "summary": "originations",
                },
            )
        else:
            return SimpleNamespace(content=[], stop_reason="end_turn", usage=None)
        return SimpleNamespace(content=[block], stop_reason="tool_use", usage=None)

    class Client:
        def __init__(self):
            self.messages = self
            self.tools = None

        async def create(self, **kwargs):
            self.tools = kwargs["tools"]
            return script(kwargs["messages"])

    async def run():
        client = Client()
        async with llm_runner.McpBackend() as backend:
            summary = await llm_runner.run_llm_eval(
                settings, client, backend, [q], units={"origination_count": "count"}
            )
        return client, summary

    client, summary = asyncio.run(run())
    r = summary["results"][0]
    assert (r["tool_selection_correct"], r["result_correct"], r["answer_correct"]) == (True, True, True)
    query_tool = next(t for t in client.tools if t["name"] == "query_metric")
    assert set(query_tool["input_schema"]["properties"]) == {
        "metric",
        "dimensions",
        "filters",
        "grain",
        "limit",
        "order_by",
    }
    assert query_tool["input_schema"]["required"] == ["metric"]
