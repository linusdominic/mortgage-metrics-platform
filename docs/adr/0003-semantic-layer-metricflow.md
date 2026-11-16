# ADR 0003: dbt semantic models with MetricFlow as the compiler

Status: accepted

## Context

Metrics must be defined once and give the same number to the report, to an
analyst at a shell and to an AI agent. The brief allowed two routes: dbt
semantic models with MetricFlow if it genuinely works on DuckDB, or a small
home-grown YAML registry and SQL compiler.

## What was tested

In this build, with `dbt-core 1.12.7`, `dbt-duckdb 1.11.1`, `duckdb 2.0.0`,
`dbt-metricflow 0.15.0` and `metricflow 0.213.0` (the versions in `uv.lock`):

- `mf validate-configs` passes, including the warehouse validation of every
  semantic model, dimension, entity, measure and metric.
- Simple metrics, ratio metrics and median aggregations compile and run.
- Dimensions resolve through one join (fact to dimension) and two joins
  (denial-reason bridge to fact to dimension).
- Compiled results match hand-written SQL for all 32 golden questions.

So MetricFlow is used. No home-grown compiler.

## Decision

- Metrics, measures, dimensions and joins live in `dbt/models/semantic/`.
- `mmp.semantic.layer.SemanticLayer` is the only code that calls MetricFlow.
  It uses the engine as a compiler: `explain()` returns SQL, and the layer runs
  that SQL itself on a read-only DuckDB connection with a timeout.
- The engine is given a client that can render the DuckDB dialect and refuses
  to execute. The dbt adapter is never instantiated in the serving process, so
  serving cannot take the write lock.
- The catalogue (which dimensions and grains each metric allows) is read from
  MetricFlow, not maintained by hand, so the guardrail allow-list cannot drift
  from the definitions.

## Consequences

- One compile path: `MetricRequest -> validate -> MetricFlow -> SQL -> rows`.
  The MCP server, `mmp query`, the static report, the published snapshot and
  the evals all use it.
- `MetricFlowEngine` is not a stable public API (its own docstring says so).
  The dependency is pinned and wrapped in one module; an upgrade is a change to
  `layer.py` and a run of the evals.
- Measured on the demo data: about 1.5 seconds to import MetricFlow and load
  the semantic manifest on first use, about 10 ms to compile a request, and
  20 to 230 ms to run one (the slowest being the first query on a cold file).
- MetricFlow cannot express "share of total across a dimension" (a ratio whose
  denominator ignores the group-by). The denial-reason mix is therefore
  published as counts per reason, and the report divides by the total. That is
  noted in the metric description and in the README limitations.
- The only time grain is the year, because that is all the public LAR has.
  `activity_year` is exposed to consumers as a normal dimension and mapped to
  `metric_time__year` internally.

## Alternatives considered

- **`mf query` as a subprocess.** 5.3 seconds for one call in a test here
  (interpreter start and manifest load), and it executes through the dbt
  adapter with a read-write connection. Not usable behind an interactive tool.
- **dbt Cloud Semantic Layer.** The managed version of the same definitions,
  with its own JDBC and GraphQL APIs and MCP server. Needs an account; not
  reproducible from a clone.
- **Home-grown registry and compiler.** More control, but it would re-implement
  join resolution and ratio/median handling, and the definitions would no
  longer be portable to any other MetricFlow consumer.
