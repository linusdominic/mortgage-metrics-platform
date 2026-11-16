# Every target is a thin wrapper around `uv run mmp ...`; see `uv run mmp --help`.
.DEFAULT_GOAL := help
SHELL := /bin/bash
COMPOSE := docker compose -f docker/airflow/docker-compose.yml

.PHONY: help setup pipeline backfill backfill-proof full-refresh evals eval-llm mcp-smoke cost-report \
        report docs catalog test test-dags lint format ci-local slim-ci-demo fixture-pipeline \
        airflow-requirements airflow-up airflow-down clean

help:  ## List targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-22s %s\n", $$1, $$2}'

setup:  ## Create the virtualenv from uv.lock (Python 3.12)
	uv sync

pipeline:  ## Download the demo scope and run every step (ingest, dbt, gate, publish, report)
	uv run mmp pipeline

backfill:  ## Load the scope's backfill year(s) and rewrite exactly those partitions
	uv run mmp backfill

backfill-proof:  ## Run the backfill twice and diff row counts and checksums
	./scripts/backfill_proof.sh

full-refresh:  ## Rebuild every incremental model from scratch
	uv run mmp pipeline --full-refresh

evals:  ## Deterministic evals: golden tool calls vs independent SQL
	uv run mmp eval

eval-llm:  ## Optional LLM eval through the MCP server (needs ANTHROPIC_API_KEY)
	uv run --extra llm mmp eval-llm

mcp-smoke:  ## Start the MCP server and call every tool over stdio
	uv run python scripts/mcp_smoke_test.py

cost-report:  ## Time and bytes per run, model and partition
	uv run mmp cost-report

report:  ## Rebuild the static report under docs/report
	uv run mmp report --out docs/report

catalog:  ## Print the metric catalogue as a markdown table
	uv run mmp catalog --markdown

docs:  ## Generate the dbt docs site into dbt/target
	cd dbt && MMP_DATA_DIR=$(CURDIR)/data DBT_PROFILES_DIR=$(CURDIR)/dbt uv run dbt docs generate

test:  ## Python tests (builds a warehouse from the committed fixture sample)
	uv run pytest

test-dags:  ## Airflow DAG integrity tests (installs the airflow dependency group)
	uv run --group airflow pytest tests/dags -m airflow

lint:  ## ruff, ruff format --check, sqlfluff
	uv run ruff check .
	uv run ruff format --check .
	MMP_DATA_DIR=$(CURDIR)/data uv run sqlfluff lint dbt/models dbt/tests dbt/snapshots

format:  ## Apply ruff fixes and formatting
	uv run ruff check . --fix
	uv run ruff format .

fixture-pipeline:  ## What CI runs: the whole pipeline offline on the committed fixture sample
	MMP_SCOPE=fixture MMP_DATA_DIR=$(CURDIR)/.ci-data MMP_DBT_TARGET_PATH=$(CURDIR)/.ci-data/dbt_target uv run mmp pipeline
	MMP_SCOPE=fixture MMP_DATA_DIR=$(CURDIR)/.ci-data MMP_DBT_TARGET_PATH=$(CURDIR)/.ci-data/dbt_target uv run mmp eval

slim-ci-demo:  ## Show state:modified+ with deferral against the last local build
	./scripts/slim_ci_demo.sh

ci-local: lint test test-dags fixture-pipeline  ## Everything the CI workflow runs

airflow-requirements:  ## Re-export the image's pinned requirements from uv.lock
	uv export --no-dev --group airflow --no-hashes --no-emit-project -q -o docker/airflow/requirements.txt

airflow-up:  ## Build and start Airflow 3 (UI on http://localhost:8793)
	$(COMPOSE) up -d --build

airflow-down:  ## Stop Airflow and delete its volumes
	$(COMPOSE) down -v

clean:  ## Delete local data, dbt artifacts and caches (raw downloads included)
	rm -rf data .ci-data dbt/target dbt/logs .pytest_cache .ruff_cache
