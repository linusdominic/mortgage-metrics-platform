"""Golden set, deterministic evals, and the LLM eval runner driven by a scripted fake model."""

import asyncio
import itertools
import json
from types import SimpleNamespace

import pytest

from mmp.evals import llm_runner
from mmp.evals.deterministic import run_deterministic
from mmp.evals.golden import GoldenQuestion, compare_rows, load_golden, reference_rows, values_equal
from mmp.semantic.layer import SemanticLayer
from mmp.semantic.toolbox import MetricsToolbox


def test_golden_set_is_well_formed(settings):
    questions = load_golden()
    catalog = SemanticLayer(settings).catalog()
    assert len(questions) >= 30
    assert len({q.id for q in questions}) == len(questions)
    for q in questions:
        info = catalog[q.metric]
        used = set(q.expected_call.get("dimensions", [])) | {
            f["dimension"] for f in q.expected_call.get("filters", [])
        }
        assert used <= set(info.dimensions), (q.id, used - set(info.dimensions))
        assert "fct_" in q.reference_sql and "metricflow" not in q.reference_sql.lower()
        if q.answer_type == "label":
            assert q.label_column
    assert {"scalar", "label", "table"} == {q.answer_type for q in questions}


def test_deterministic_evals_all_pass_against_independent_sql(settings):
    report = run_deterministic(settings)
    failures = [(r["id"], r["detail"]) for r in report["results"] if not r["passed"]]
    assert not failures, failures
    assert report["total"] >= 30 and len(report["metrics_covered"]) >= 15


def test_deterministic_eval_detects_a_wrong_definition(settings, tmp_path):
    """The eval must fail when the semantic layer and the reference disagree."""
    golden = tmp_path / "golden.yml"
    golden.write_text(
        """
questions:
  - id: bad
    question: approval rate, but the reference counts withdrawn applications in the denominator
    expected_call: {metric: approval_rate}
    reference_sql: |
      select count(*) filter (where action_taken_code in (1, 2)) * 1.0
        / count(*) filter (where action_taken_code in (1, 2, 3, 4)) as approval_rate
      from marts.fct_loan_applications
    answer: {type: scalar}
"""
    )
    report = run_deterministic(settings, golden)
    assert report["failed"] == 1 and "!=" in report["results"][0]["detail"]


def test_compare_rows_semantics():
    assert compare_rows([{"s": "DE", "m": 0.5}], [{"s": "DE", "m": 0.5 + 1e-13}], "m")[0]
    assert not compare_rows([{"s": "DE", "m": 0.5}], [{"s": "DE", "m": 0.51}], "m")[0]
    assert not compare_rows([{"s": "DE", "m": 1}], [{"s": "RI", "m": 1}], "m")[0]
    assert not compare_rows([{"s": "DE", "m": 1}], [{"s": "DE", "m": 1}, {"s": "RI", "m": 2}], "m")[0]
    assert compare_rows(
        [{"x": "a", "m": 3}, {"x": "b", "m": 3}],
        [{"x": "b", "m": 3}, {"x": "c", "m": 3}],
        "m",
        "metric_values",
    )[0]
    # additive metrics: "no row" and 0 are the same answer; ratios are not
    assert compare_rows([{"m": None}], [{"m": 0}], "m", additive=True)[0]
    assert not compare_rows([{"m": None}], [{"m": 0}], "m", additive=False)[0]
    assert values_equal(None, None) and not values_equal(None, 1)


def test_call_normalisation_treats_equivalent_spellings_alike():
    expected = {
        "metric": "approval_rate",
        "dimensions": ["state_code"],
        "grain": "year",
        "filters": [
            {"dimension": "state_code", "op": "in", "value": ["DE"]},
            {"dimension": "activity_year", "op": "=", "value": 2024},
        ],
    }
    same = {
        "metric": "approval_rate",
        "dimensions": ["activity_year", "state_code"],
        "filters": [
            {"dimension": "activity_year", "op": "=", "value": "2024"},
            {"dimension": "state_code", "op": "=", "value": "DE"},
        ],
        "limit": 50,
    }
    assert llm_runner.call_matches(same, expected)
    assert not llm_runner.call_matches({**same, "metric": "denial_rate"}, expected)
    assert not llm_runner.call_matches({**same, "filters": same["filters"][:1]}, expected)
    assert not llm_runner.call_matches({**same, "dimensions": ["state_code"]}, expected)


# --------------------------------------------------------------------- fake model


_TOOL_IDS = itertools.count()


def tool_use(name, arguments):
    return SimpleNamespace(type="tool_use", id=f"toolu_{next(_TOOL_IDS):04d}", name=name, input=arguments)


def response(*blocks, stop_reason="tool_use"):
    return SimpleNamespace(
        content=list(blocks),
        stop_reason=stop_reason,
        usage=SimpleNamespace(input_tokens=100, output_tokens=20),
    )


class FakeAnthropic:
    """Stands in for anthropic.AsyncAnthropic. `script` maps the conversation so far to the next response."""

    def __init__(self, script):
        self.script = script
        self.requests = []
        self.messages = self

    async def create(self, **kwargs):
        self.requests.append({**kwargs, "messages": list(kwargs["messages"])})
        return self.script(kwargs["messages"])


def last_tool_payload(messages):
    for message in reversed(messages):
        if message["role"] == "user" and isinstance(message["content"], list):
            return json.loads(message["content"][-1]["content"])
    return None


def scripted_agent(calls, answer_from):
    """A model that makes `calls` in order, then submits an answer derived from the last tool result."""

    def script(messages):
        turn = sum(1 for m in messages if m["role"] == "assistant")
        if turn < len(calls):
            return response(SimpleNamespace(type="thinking", thinking=""), tool_use(*calls[turn]))
        if turn == len(calls):
            return response(tool_use("submit_answer", answer_from(last_tool_payload(messages))))
        return response(SimpleNamespace(type="text", text="done"), stop_reason="end_turn")

    return script


@pytest.fixture
def backend(settings):
    return llm_runner.InProcessBackend(
        MetricsToolbox(SemanticLayer(settings), audit=None, client="llm-eval-test")
    )


def question(qid):
    return next(q for q in load_golden() if q.id == qid)


def run(coro):
    return asyncio.run(coro)


def test_llm_runner_scores_a_correct_agent(settings, backend):
    q = question("q01")
    client = FakeAnthropic(
        scripted_agent(
            [
                ("list_metrics", {}),
                ("describe_metric", {"metric": "approval_rate"}),
                (
                    "query_metric",
                    {
                        "metric": "approval_rate",
                        "filters": [
                            {"dimension": "activity_year", "op": "=", "value": 2024},
                            {"dimension": "state_code", "op": "=", "value": "DE"},
                        ],
                    },
                ),
            ],
            lambda payload: {
                "value": round(payload["rows"][0]["approval_rate"], 4),
                "label": None,
                "summary": "approval rate",
            },
        )
    )
    summary = run(llm_runner.run_llm_eval(settings, client, backend, [q], units={"approval_rate": "ratio"}))
    r = summary["results"][0]
    assert (r["status"], r["tool_selection_correct"], r["result_correct"], r["answer_correct"]) == (
        "completed",
        True,
        True,
        True,
    )
    assert (
        summary["tool_selection_accuracy"] == summary["result_accuracy"] == summary["answer_accuracy"] == 1.0
    )
    assert [c["name"] for c in r["tool_calls"]] == ["list_metrics", "describe_metric", "query_metric"]
    assert summary["input_tokens"] == 400 and r["turns"] == 4

    first = client.requests[0]
    assert first["model"] == "claude-sonnet-5-5" and first["thinking"] == {"type": "adaptive"}
    assert "temperature" not in first and "tool_choice" not in first
    names = [t["name"] for t in first["tools"]]
    assert names[-1] == "submit_answer" and "query_metric" in names and not any("sql" in n for n in names)
    # the assistant turn goes back unchanged (thinking block included), followed by the tool result
    second = client.requests[1]["messages"]
    assert second[1]["role"] == "assistant" and second[1]["content"][0].type == "thinking"
    assert second[2]["content"][0]["type"] == "tool_result" and second[2]["content"][0][
        "tool_use_id"
    ].startswith("toolu_")


def test_llm_runner_scores_a_wrong_metric_as_wrong(settings, backend):
    q = question("q01")
    client = FakeAnthropic(
        scripted_agent(
            [("query_metric", {"metric": "origination_rate", "filters": q.expected_call["filters"]})],
            lambda payload: {
                "value": payload["rows"][0]["origination_rate"],
                "label": None,
                "summary": "wrong metric",
            },
        )
    )
    r = run(llm_runner.run_llm_eval(settings, client, backend, [q]))["results"][0]
    assert (r["tool_selection_correct"], r["result_correct"], r["answer_correct"]) == (False, False, False)


def test_equivalent_call_counts_for_result_but_not_for_tool_selection(settings, backend):
    """Fetching by year and state and reading one row gets the right result with a different call."""
    q = question("q01")
    expected = reference_rows(settings, q)[0]["approval_rate"]
    client = FakeAnthropic(
        scripted_agent(
            [("query_metric", {"metric": "approval_rate", "dimensions": ["state_code"], "grain": "year"})],
            lambda payload: {"value": expected, "label": None, "summary": "read from a broader table"},
        )
    )
    r = run(llm_runner.run_llm_eval(settings, client, backend, [q]))["results"][0]
    assert (r["tool_selection_correct"], r["result_correct"], r["answer_correct"]) == (False, False, True)


def test_label_questions_and_guardrail_errors_are_fed_back(settings, backend):
    q = question("q21")
    client = FakeAnthropic(
        scripted_agent(
            [
                (
                    "query_metric",
                    {"metric": "origination_count", "dimensions": ["county"]},
                ),  # rejected: not a dimension
                ("query_metric", q.expected_call),
            ],
            lambda payload: {
                "value": None,
                "label": payload["rows"][0]["county_name"],
                "summary": "top county",
            },
        )
    )
    summary = run(
        llm_runner.run_llm_eval(settings, client, backend, [q], units={"origination_count": "count"})
    )
    r = summary["results"][0]
    assert [c["is_error"] for c in r["tool_calls"]] == [True, False]
    rejected = client.requests[1]["messages"][-1]["content"][0]
    assert rejected["is_error"] is True and "not allowed" in rejected["content"]
    assert (r["tool_selection_correct"], r["result_correct"], r["answer_correct"]) == (True, True, True)


def test_refusal_and_runaway_loops_are_recorded_not_hidden(settings, backend):
    q = question("q03")
    refusing = FakeAnthropic(lambda messages: response(stop_reason="refusal"))
    r = run(llm_runner.run_llm_eval(settings, refusing, backend, [q]))["results"][0]
    assert r["status"] == "refusal" and r["answer_correct"] is False

    looping = FakeAnthropic(lambda messages: response(tool_use("list_metrics", {})))
    summary = run(llm_runner.run_llm_eval(settings, looping, backend, [q]))
    r = summary["results"][0]
    assert r["status"] == "max_turns" and r["turns"] == llm_runner.MAX_TURNS
    assert summary["status_counts"] == {"max_turns": 1} and summary["answer_accuracy"] == 0.0


def test_table_questions_are_scored_on_results_only(settings, backend):
    q = question("q05")
    client = FakeAnthropic(
        scripted_agent([("query_metric", q.expected_call)], lambda p: {"summary": "table"})
    )
    summary = run(llm_runner.run_llm_eval(settings, client, backend, [q]))
    assert summary["results"][0]["answer_correct"] is None
    assert summary["answer_accuracy"] is None and summary["answer_graded_questions"] == 0
    assert summary["result_accuracy"] == 1.0


def test_answer_matching_tolerates_rounding_only():
    q = GoldenQuestion("x", "?", {"metric": "m"}, "select 1", "scalar")
    rows = [{"m": 0.74312}]
    assert llm_runner.answer_matches(q, rows, {"value": 0.7431})
    assert not llm_runner.answer_matches(q, rows, {"value": 0.75})
    assert not llm_runner.answer_matches(q, rows, {"value": None})
    assert not llm_runner.answer_matches(q, rows, None)


def test_llm_eval_refuses_to_run_without_a_key(settings, monkeypatch, capsys):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    args = SimpleNamespace(golden=None, limit=1, model=None, out=None)
    assert llm_runner.main(settings, args) == 2
    assert "ANTHROPIC_API_KEY is not set" in capsys.readouterr().out


def test_api_errors_are_classified_with_the_sdk_types():
    anthropic = pytest.importorskip("anthropic")
    import httpx

    request = httpx.Request("POST", "https://api.anthropic.com/v1/messages")
    rate_limited = anthropic.RateLimitError(
        "slow down", response=httpx.Response(429, request=request), body=None
    )
    assert llm_runner._is_api_error(rate_limited)
    assert llm_runner._is_api_error(anthropic.APIConnectionError(request=request))
    assert not llm_runner._is_api_error(ValueError("a bug, not an API error"))
