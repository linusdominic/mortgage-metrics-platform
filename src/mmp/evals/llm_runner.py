"""Optional LLM eval: a model answers the golden questions through the MCP server.

    export ANTHROPIC_API_KEY=...
    uv sync --extra llm
    uv run mmp eval-llm --limit 5

For each question the model gets only the MCP tools (plus a local submit_answer
tool to hand in a structured final answer) and is scored on

  tool_selection   did a query_metric call match the expected metric, dimensions,
                   filters and grain?
  result           did any query_metric call return the reference rows? (An
                   equivalent call that differs from the expected one still counts.)
  answer           is the submitted number / label right? (scalar and label questions)

The model talks to the real server over stdio: the same process an MCP client
such as Claude Desktop would start. Tests replace the Anthropic client with a
scripted fake; nothing in this module fabricates a model response.

Server-side refusal fallbacks are deliberately not enabled: a fallback would
answer with a different model and the score would no longer describe the model
under test. A refusal is recorded as a failed question.
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import sys
from contextlib import AsyncExitStack
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Protocol

from mmp.evals.golden import GoldenQuestion, compare_rows, load_golden, reference_rows
from mmp.settings import REPO_ROOT, Settings

DEFAULT_MODEL = "claude-sonnet-5-5"
MAX_TURNS = 12
MAX_TOKENS = 16000
ANSWER_REL_TOL = 5e-3  # a model may round; 0.5% relative
ADDITIVE_UNITS = {"count", "usd"}

SYSTEM_PROMPT = """\
You answer questions about US mortgage applications using the governed metrics tools.

Use the tools to find the right metric, check its dimensions with describe_metric, and \
use list_dimension_values when you need the exact label for a filter. Query the metric \
with the filters and dimensions the question calls for; do not fetch a broader result \
and compute the answer yourself. You cannot run SQL.

When you have the answer, call submit_answer exactly once and then stop:
- value: the single number that answers the question, exactly as the tool returned it \
(rates as fractions, e.g. 0.7431), or null if the answer is a table or a label;
- label: the name that answers a "which ..." question, or null;
- summary: one sentence stating the answer and the metric and filters used."""

SUBMIT_ANSWER_TOOL = {
    "name": "submit_answer",
    "description": "Hand in the final answer. Call exactly once, after the metric queries.",
    "input_schema": {
        "type": "object",
        "properties": {
            "value": {"type": ["number", "null"], "description": "The numeric answer, or null."},
            "label": {"type": ["string", "null"], "description": "The label answer, or null."},
            "summary": {
                "type": "string",
                "description": "One sentence with the answer and how it was obtained.",
            },
        },
        "required": ["summary"],
    },
}


# ----------------------------------------------------------------- tool backends


class ToolBackend(Protocol):
    async def list_tools(self) -> list[dict[str, Any]]: ...

    async def call(self, name: str, arguments: dict[str, Any]) -> tuple[Any, bool]: ...


class McpBackend:
    """A real MCP client session to the server, over stdio."""

    def __init__(self, env: dict[str, str] | None = None) -> None:
        self._env = env
        self._stack = AsyncExitStack()
        self._session = None

    async def __aenter__(self) -> McpBackend:
        from mcp import ClientSession, StdioServerParameters, stdio_client

        params = StdioServerParameters(
            command=sys.executable,
            args=["-m", "mmp.mcp_server"],
            env={**os.environ, "MMP_REPO_ROOT": str(REPO_ROOT), **(self._env or {})},
            cwd=str(REPO_ROOT),
        )
        read, write = await self._stack.enter_async_context(stdio_client(params))
        self._session = await self._stack.enter_async_context(ClientSession(read, write))
        await self._session.initialize()
        return self

    async def __aexit__(self, *exc) -> None:
        await self._stack.aclose()

    async def list_tools(self) -> list[dict[str, Any]]:
        tools = (await self._session.list_tools()).tools
        return [
            {
                "name": t.name,
                "description": t.description or "",
                "input_schema": getattr(t, "input_schema", None) or getattr(t, "inputSchema", None),
            }
            for t in tools
        ]

    async def call(self, name: str, arguments: dict[str, Any]) -> tuple[Any, bool]:
        result = await self._session.call_tool(name, arguments)
        is_error = bool(getattr(result, "is_error", None) or getattr(result, "isError", None))
        structured = getattr(result, "structured_content", None) or getattr(result, "structuredContent", None)
        if structured is not None and not is_error:
            return (
                structured.get("result", structured) if isinstance(structured, dict) else structured
            ), False
        text = "".join(b.text for b in result.content if getattr(b, "type", "") == "text")
        try:
            return json.loads(text), is_error
        except json.JSONDecodeError:
            return text, is_error


class InProcessBackend:
    """The same tools without a subprocess. For unit tests."""

    TOOL_SCHEMAS = (
        ("list_metrics", "List governed metrics.", {}),
        ("describe_metric", "Describe one metric.", {"metric": {"type": "string"}}),
        (
            "list_dimension_values",
            "List values of a dimension.",
            {"metric": {"type": "string"}, "dimension": {"type": "string"}},
        ),
        (
            "query_metric",
            "Query one governed metric.",
            {
                "metric": {"type": "string"},
                "dimensions": {"type": "array", "items": {"type": "string"}},
                "filters": {"type": "array", "items": {"type": "object"}},
                "grain": {"type": "string"},
                "limit": {"type": "integer"},
                "order_by": {"type": "array", "items": {"type": "string"}},
            },
        ),
        ("get_lineage", "Lineage of a metric.", {"metric": {"type": "string"}}),
    )

    def __init__(self, toolbox) -> None:
        self.toolbox = toolbox

    async def list_tools(self) -> list[dict[str, Any]]:
        return [
            {"name": n, "description": d, "input_schema": {"type": "object", "properties": p}}
            for n, d, p in self.TOOL_SCHEMAS
        ]

    async def call(self, name: str, arguments: dict[str, Any]) -> tuple[Any, bool]:
        from mmp.semantic.toolbox import ToolFailed, ToolRejected

        try:
            return self.toolbox.dispatch(name, arguments), False
        except (ToolRejected, ToolFailed) as exc:
            return str(exc), True


# ----------------------------------------------------------------------- scoring


def _normalise_filter(f: dict[str, Any]) -> tuple:
    value = f.get("value")
    op = str(f.get("op", "")).lower()
    if isinstance(value, list | tuple):
        values = tuple(sorted(str(v) for v in value))
        if op == "in" and len(values) == 1:
            op, values = "=", values
    else:
        values = (str(value),)
    return (str(f.get("dimension")), op, values)


def normalise_call(call: dict[str, Any]) -> dict[str, Any]:
    """Canonical form of a query_metric call, so equivalent spellings compare equal."""
    dims = [str(d) for d in call.get("dimensions") or []]
    grain = (call.get("grain") or "").lower()
    if grain == "year" and "activity_year" not in dims:
        dims.append("activity_year")
    return {
        "metric": call.get("metric"),
        "dimensions": frozenset(dims),
        "filters": frozenset(_normalise_filter(f) for f in call.get("filters") or [] if isinstance(f, dict)),
    }


def call_matches(actual: dict[str, Any], expected: dict[str, Any]) -> bool:
    return normalise_call(actual) == normalise_call(expected)


def answer_matches(
    question: GoldenQuestion, expected_rows: list[dict[str, Any]], submitted: dict | None
) -> bool | None:
    """None when the question type has no single answer to grade."""
    if question.answer_type == "table":
        return None
    if not submitted:
        return False
    if question.answer_type == "scalar":
        expected = expected_rows[0][question.metric] if expected_rows else None
        value = submitted.get("value")
        if expected is None or value is None:
            return expected is None and value is None
        return math.isclose(float(value), float(expected), rel_tol=ANSWER_REL_TOL, abs_tol=1e-9)
    expected_label = str(expected_rows[0][question.label_column]) if expected_rows else ""
    label = str(submitted.get("label") or "")
    return bool(label) and label.strip().lower() == expected_label.strip().lower()


@dataclass
class QuestionResult:
    id: str
    question: str
    status: str  # completed | max_turns | refusal | api_error
    tool_selection_correct: bool = False
    result_correct: bool = False
    answer_correct: bool | None = None
    tool_calls: list[dict[str, Any]] = field(default_factory=list)
    submitted: dict[str, Any] | None = None
    turns: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    error: str | None = None


# ------------------------------------------------------------------------- loop


async def answer_question(
    client: Any,
    backend: ToolBackend,
    tools: list[dict[str, Any]],
    question: GoldenQuestion,
    expected_rows: list[dict[str, Any]],
    model: str,
    unit: str = "number",
) -> QuestionResult:
    """Run one question to completion with a manual tool loop."""
    result = QuestionResult(id=question.id, question=question.question, status="max_turns")
    messages: list[dict[str, Any]] = [{"role": "user", "content": question.question}]
    for _ in range(MAX_TURNS):
        response = await client.messages.create(
            model=model,
            max_tokens=MAX_TOKENS,
            system=SYSTEM_PROMPT,
            tools=tools,
            thinking={"type": "adaptive"},
            output_config={"effort": "medium"},
            messages=messages,
        )
        result.turns += 1
        usage = getattr(response, "usage", None)
        result.input_tokens += getattr(usage, "input_tokens", 0) or 0
        result.output_tokens += getattr(usage, "output_tokens", 0) or 0

        if response.stop_reason == "refusal":
            result.status = "refusal"
            break
        # Thinking blocks must go back unchanged, so the whole content list is appended.
        messages.append({"role": "assistant", "content": response.content})
        tool_uses = [b for b in response.content if b.type == "tool_use"]
        if response.stop_reason != "tool_use" or not tool_uses:
            result.status = "completed"
            break

        tool_results = []
        for block in tool_uses:
            arguments = dict(block.input or {})
            if block.name == "submit_answer":
                result.submitted = arguments
                payload, is_error = {"received": True}, False
            else:
                payload, is_error = await backend.call(block.name, arguments)
                result.tool_calls.append({"name": block.name, "arguments": arguments, "is_error": is_error})
                if block.name == "query_metric" and not is_error and isinstance(payload, dict):
                    if call_matches(arguments, question.expected_call):
                        result.tool_selection_correct = True
                    ok, _ = compare_rows(
                        payload.get("rows", []),
                        expected_rows,
                        question.metric,
                        question.compare,
                        additive=unit in ADDITIVE_UNITS,
                    )
                    if ok and payload.get("metric") == question.metric:
                        result.result_correct = True
            tool_results.append(
                {
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": json.dumps(payload, default=str),
                    "is_error": is_error,
                }
            )
        messages.append({"role": "user", "content": tool_results})
        if result.submitted is not None:
            result.status = "completed"
            break
    result.answer_correct = answer_matches(question, expected_rows, result.submitted)
    return result


def summarise(results: list[QuestionResult], model: str) -> dict[str, Any]:
    n = len(results)
    graded = [r for r in results if r.answer_correct is not None]

    def rate(count: int, total: int) -> float | None:
        return round(count / total, 4) if total else None

    return {
        "model": model,
        "questions": n,
        "tool_selection_accuracy": rate(sum(r.tool_selection_correct for r in results), n),
        "result_accuracy": rate(sum(r.result_correct for r in results), n),
        "answer_accuracy": rate(sum(bool(r.answer_correct) for r in graded), len(graded)),
        "answer_graded_questions": len(graded),
        "status_counts": {
            s: sum(r.status == s for r in results) for s in sorted({r.status for r in results})
        },
        "input_tokens": sum(r.input_tokens for r in results),
        "output_tokens": sum(r.output_tokens for r in results),
        "results": [asdict(r) for r in results],
    }


async def run_llm_eval(
    settings: Settings,
    client: Any,
    backend: ToolBackend,
    questions: list[GoldenQuestion],
    model: str = DEFAULT_MODEL,
    units: dict[str, str] | None = None,
) -> dict[str, Any]:
    mcp_tools = await backend.list_tools()
    tools = [*mcp_tools, SUBMIT_ANSWER_TOOL]
    results: list[QuestionResult] = []
    for q in questions:
        expected = reference_rows(settings, q)
        try:
            results.append(
                await answer_question(
                    client, backend, tools, q, expected, model, (units or {}).get(q.metric, "number")
                )
            )
        except Exception as exc:
            if not _is_api_error(exc):
                raise
            results.append(
                QuestionResult(
                    id=q.id, question=q.question, status="api_error", error=f"{type(exc).__name__}: {exc}"
                )
            )
    return summarise(results, model)


def _is_api_error(exc: Exception) -> bool:
    try:
        import anthropic
    except ImportError:
        return False
    # RateLimitError and the other status errors are APIStatusError subclasses; the
    # SDK has already retried 429/5xx/connection errors before any of these surface.
    return isinstance(exc, anthropic.APIStatusError | anthropic.APIConnectionError)


def format_summary(summary: dict[str, Any]) -> str:
    lines = [
        f"model: {summary['model']}   questions: {summary['questions']}",
        f"tool selection accuracy: {summary['tool_selection_accuracy']}",
        f"result accuracy:         {summary['result_accuracy']}",
        f"answer accuracy:         {summary['answer_accuracy']} (over {summary['answer_graded_questions']} scalar/label questions)",
        f"status: {summary['status_counts']}   tokens in/out: {summary['input_tokens']}/{summary['output_tokens']}",
    ]
    for r in summary["results"]:
        flags = "".join(
            "Y" if x else ("-" if x is None else "n")
            for x in (r["tool_selection_correct"], r["result_correct"], r["answer_correct"])
        )
        lines.append(f"  {r['id']}  [{flags}]  {r['status']:<10} {r['question'][:70]}")
    return "\n".join(lines)


def main(settings: Settings, args) -> int:
    if not os.environ.get("ANTHROPIC_API_KEY"):
        print(
            "ANTHROPIC_API_KEY is not set; the LLM eval needs it. The deterministic evals (`mmp eval`) do not."
        )
        return 2
    try:
        import anthropic
    except ImportError:
        print("the anthropic package is not installed; run `uv sync --extra llm`")
        return 2
    from mmp.semantic.layer import SemanticLayer

    questions = load_golden(Path(args.golden) if args.golden else None)
    if args.limit:
        questions = questions[: args.limit]
    model = args.model or os.environ.get("MMP_EVAL_MODEL", DEFAULT_MODEL)
    units = {name: info.unit for name, info in SemanticLayer(settings).catalog().items()}

    async def run() -> dict[str, Any]:
        client = anthropic.AsyncAnthropic()
        async with McpBackend() as backend:
            return await run_llm_eval(settings, client, backend, questions, model, units)

    summary = asyncio.run(run())
    print(format_summary(summary))
    out = Path(args.out) if args.out else settings.data_dir / "published" / "llm_eval.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(summary, indent=2, default=str))
    print(f"written: {out}")
    return 0
