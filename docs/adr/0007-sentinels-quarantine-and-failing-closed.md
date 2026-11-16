# ADR 0007: Sentinel values, quarantine, and where the pipeline fails closed

Status: accepted

## Context

HMDA numeric fields carry `NA` (not applicable or not reported) and `Exempt`
(the filer is exempt from reporting the field) next to real numbers, and a few
fields mix numbers with text bands (`debt_to_income_ratio` is `36`, `37`, ...
or `30%-<36%`). Integer code fields use `1111` for exempt. A reader that infers
types either fails on these or silently turns them into nulls, after which
"not applicable" and "missing" cannot be told apart.

## Decision

Three layers with one job each.

**Bronze: faithful and typed.** The Parquet schema is declared column by column
(`ingest/schema.py`). Any column that can carry a sentinel or a band is a
string and keeps `NA` / `Exempt` as data. Pure code columns are `int32`, always
numeric columns are `double`. The header must match the declared 99 columns
exactly.

**Staging: one interpretation.** `hmda_numeric(col)` yields a number or null;
`hmda_value_status(col)` yields `reported`, `not_applicable`, `exempt`,
`missing` or `unparseable`. The pair is kept for every measure a metric uses, so
"average of reported rate spreads" is expressible and tests can tell legitimate
absence from damage. Both macros are unit tested.

**Row rules: quarantine, do not drop.** Each bronze row gets at most one
`reject_reason` (partition mismatch, invalid LEI, invalid action code,
non-positive loan amount, unknown product code, unknown demographic value).
Rejected rows go to `quarantine.rejected_lar_rows` with the reason; accepted
rows go on. Accepted plus quarantined must equal bronze, per partition.

## Where it fails closed

| Condition | Effect |
|---|---|
| Source header differs from the contract | conversion raises; no Parquet written; run stops |
| A code or amount column has a value of the wrong type | same |
| Converter row count differs from the raw line count | same |
| Mart column missing, extra or of the wrong type | dbt contract error at build |
| Raw, bronze, staging and fact do not reconcile for a partition | `assert_reconciliation_balanced` fails the build |
| Fact partitions and manifest disagree on file hashes | `assert_fact_partitions_match_manifest` fails the build |
| More than 1% of rows quarantined, a state's volume moves 80% year over year, an expected partition is missing, or the newest file is over 30 days old | the quality gate exits non-zero; nothing is published |

Warn-only: values outside a plausible range (interest rate above 25%, LTV above
1000%), a 40% volume move, lender version overlap. Real HMDA has reporting
errors in these fields; they are surfaced and left in, and the catalogue points
to medians where means are sensitive to them.

## Consequences

- On the real demo data nothing is quarantined: 0 of 213,473 rows. The rules
  are exercised by dbt unit tests and by a pytest that corrupts five rows of a
  real file and checks the reasons and the reconciliation.
- Out-of-domain values are quarantined rather than given an "unknown" dimension
  member. A new code published by the FFIEC would show up as a quarantine spike
  and a gate failure, which is the prompt to extend the seed.
- Only the first failed rule is recorded per row.
