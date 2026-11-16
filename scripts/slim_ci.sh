#!/usr/bin/env bash
# Slim CI: build only what changed, defer everything else to the last good build.
#
#   scripts/slim_ci.sh <state-dir> [dbt-project-dir]
#
# <state-dir> holds manifest.json from the build being compared against (the
# base branch in CI, the last local build otherwise). Modified models and
# everything downstream are built into ci_-prefixed schemas in the same DuckDB
# file; refs to unmodified parents resolve to the existing relations (--defer).
set -euo pipefail
state_dir="$1"
project_dir="${2:-dbt}"
repo="$(cd "$(dirname "$0")/.." && pwd)"

export MMP_DATA_DIR="${MMP_DATA_DIR:-$repo/data}"
export DBT_PROFILES_DIR="$repo/dbt"
export DO_NOT_TRACK=1

if [ ! -f "$state_dir/manifest.json" ]; then
  echo "no manifest.json in $state_dir; build the comparison state first" >&2
  exit 2
fi

echo "== modified nodes (state:modified)"
uv run dbt ls --project-dir "$project_dir" --target ci --select state:modified --state "$state_dir" \
  --resource-type model --resource-type seed --resource-type snapshot --quiet || true

echo "== dbt build --select state:modified+ --defer"
uv run dbt build --project-dir "$project_dir" --target ci \
  --select state:modified+ --defer --state "$state_dir" \
  --target-path "${SLIM_CI_TARGET_PATH:-$MMP_DATA_DIR/dbt_target_ci}"
