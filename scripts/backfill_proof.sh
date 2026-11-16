#!/usr/bin/env bash
# Idempotency proof for backfills.
#
# Loads the scope's backfill year(s) twice and compares row counts and content
# checksums of every mart table after each run. The second run rewrites the
# same partitions from the same source files, so the two snapshots must be
# byte-identical. Exits non-zero if they are not.
set -euo pipefail
cd "$(dirname "$0")/.."

out="${MMP_DATA_DIR:-data}/published/backfill_proof"
mkdir -p "$out"

echo "== checksums before the backfill"
uv run mmp checksum --out "$out/0_before.txt" > /dev/null
grep fct_loan_applications "$out/0_before.txt"

echo; echo "== backfill, run 1"
uv run mmp backfill --no-report "$@" | grep -E "^run |cached|downloaded|dbt build:|quality gate|pipeline finished"
uv run mmp checksum --out "$out/1_after_first_run.txt" > /dev/null

echo; echo "== backfill, run 2 (same years again)"
uv run mmp backfill --no-report "$@" | grep -E "^run |cached|downloaded|dbt build:|quality gate|pipeline finished"
uv run mmp checksum --out "$out/2_after_second_run.txt" > /dev/null

echo; echo "== fact partitions after run 1 and run 2 (table, partition, rows, checksum)"
paste -d'|' <(grep fct_loan_applications "$out/1_after_first_run.txt") <(grep fct_loan_applications "$out/2_after_second_run.txt" | awk '{print " "$3" "$4}')

echo; echo "== diff of all mart checksums between run 1 and run 2"
if diff "$out/1_after_first_run.txt" "$out/2_after_second_run.txt"; then
  echo "identical: $(wc -l < "$out/1_after_first_run.txt" | tr -d ' ') table/partition checksums match"
else
  echo "MISMATCH: the backfill is not idempotent" >&2
  exit 1
fi

echo; echo "== partitions that existed before the backfill are unchanged"
if diff <(grep -E "^(marts\.fct|quarantine)" "$out/0_before.txt") \
        <(grep -E "^(marts\.fct|quarantine)" "$out/2_after_second_run.txt" | grep -F -f <(awk '{print $2}' "$out/0_before.txt" | sort -u | grep / | sed 's/^/ /;s/$/ /')); then
  echo "unchanged: $(grep -cE "^(marts\.fct|quarantine)" "$out/0_before.txt") pre-existing fact partition checksums"
else
  echo "MISMATCH: the backfill changed a partition it does not own" >&2
  exit 1
fi
