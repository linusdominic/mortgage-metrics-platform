#!/usr/bin/env bash
# Local demonstration of slim CI without touching the working tree: copy the dbt
# project, change one model in the copy, and run scripts/slim_ci.sh against the
# state saved by the last `mmp dbt-build` / `make pipeline`.
set -euo pipefail
repo="$(cd "$(dirname "$0")/.." && pwd)"
export MMP_DATA_DIR="${MMP_DATA_DIR:-$repo/data}"
work="$(mktemp -d)"
cleanup() {
  rm -rf "$work"
  # drop the ci_ schemas the demo created
  (cd "$repo" && uv run python - <<'PY'
import os, duckdb
con = duckdb.connect(os.path.join(os.environ["MMP_DATA_DIR"], "warehouse", "mortgage.duckdb"))
for (schema,) in con.execute("select schema_name from information_schema.schemata where schema_name like 'ci\\_%' escape '\\'").fetchall():
    con.execute(f'drop schema "{schema}" cascade')
PY
  )
}
trap cleanup EXIT

cp -R "$repo/dbt" "$work/dbt"
rm -rf "$work/dbt/target" "$work/dbt/logs"
# The "pull request": add a column to dim_date.
python3 - "$work/dbt/models/marts/core" <<'PY'
import pathlib, sys
core = pathlib.Path(sys.argv[1])
sql = core / "dim_date.sql"
sql.write_text(sql.read_text().replace(
    "    cast(pmms.pmms_15y_avg as double) as pmms_15y_avg\n",
    "    cast(pmms.pmms_15y_avg as double) as pmms_15y_avg,\n"
    "    cast(pmms.pmms_30y_max - pmms.pmms_30y_min as double) as pmms_30y_range\n"))
yml = core / "_core.yml"
yml.write_text(yml.read_text().replace(
    "      - name: pmms_15y_avg\n        description: Mean weekly 15-year fixed PMMS rate, percent.\n        data_type: double\n",
    "      - name: pmms_15y_avg\n        description: Mean weekly 15-year fixed PMMS rate, percent.\n        data_type: double\n"
    "      - name: pmms_30y_range\n        description: Spread between the highest and lowest weekly 30-year rate.\n        data_type: double\n"))
PY
echo "changed in the copy: models/marts/core/dim_date.sql (+ its contract)"
cd "$repo"
SLIM_CI_TARGET_PATH="$work/target" ./scripts/slim_ci.sh "$MMP_DATA_DIR/dbt_state" "$work/dbt"
