# ADR 0005: Agents query governed metrics through MCP and never write SQL

Status: accepted

## Context

An agent with a SQL tool and a schema will produce a number for any question.
Whether it used the same definition of "approval rate" as the finance report is
left to chance, and the query it wrote is the only record of what it meant.

## Decision

Expose the semantic layer over the Model Context Protocol (official `mcp`
Python SDK, stdio transport) with five tools:

| Tool | Purpose |
|---|---|
| `list_metrics` | the catalogue |
| `describe_metric` | definition, formula, allowed dimensions and grains, limits |
| `list_dimension_values` | exact labels to filter on, with record counts |
| `query_metric` | one metric, grouped and filtered |
| `get_lineage` | measures, expressions, dbt models and sources behind a metric |

There is no tool that accepts SQL, a table name or an expression.

Guardrails, all enforced before anything is compiled (`semantic/guardrails.py`):

- the metric must be in the catalogue;
- dimensions and filter columns must be on that metric's allow-list, which
  comes from MetricFlow's join graph;
- filters are `{dimension, op, value}` with a fixed operator set; ordering
  operators only on `activity_year`; values are type- and length-checked and
  rendered as escaped literals inside MetricFlow's filter templates;
- at most 4 dimensions and 8 filters; row limit default 100, maximum 1000,
  with truncation reported;
- queries run on a read-only connection and are cancelled after 15 seconds;
- breakdowns by a demographic dimension withhold the value of any cell with
  fewer than 10 records and attach a note about interpretation;
- every call, including refused ones, is appended to `logs/mcp_audit.jsonl`
  with arguments, status, row count, timing and a hash of the SQL.

Refusals are returned to the model as readable tool errors that say what is
allowed, so it can correct itself.

## Consequences

- An agent's answer is reproducible from the logged tool call; the compiled SQL
  is returned with every result.
- Questions outside the catalogue cannot be answered. That is the point, and it
  is also the cost: the catalogue has to grow with demand.
- The guardrails are application-level. On a shared warehouse the service
  account behind the server would also be limited to `select` on the marts.
- stdio means one server process per client. A multi-user deployment would use
  the streamable HTTP transport with authentication; not built here.
- Small-cell suppression is a display rule, not a privacy guarantee. HMDA is
  already public at loan level; the rule exists to stop tiny cells being read
  as rates.

## Evaluation

`evals/golden_questions.yml` holds 32 questions with the expected tool call and
independent reference SQL. The deterministic layer runs in CI. An LLM layer
(`mmp eval-llm`) scores a model's tool selection and answers through the real
server; see the README for what was and was not run.
