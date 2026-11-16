# ADR 0004: A Type 2 lender dimension derived from year-stamped filings, plus a dbt snapshot

Status: accepted

## Context

Lender names change between filing years: rebrands, mergers, a switch to
upper case. In the data loaded for this build (three states, 2022 to 2024),
112 of the 1,256 LEIs appear under more than one name. Reports should show the
name the lender filed under in the year being reported.

dbt's tool for Type 2 history is the snapshot. A snapshot records a change when
a run first sees it. Its history is therefore a function of when runs happened,
which conflicts with ADR 0002: backfilling 2022 after 2024 must give the same
dimension as loading the years in order, and a snapshot cannot be replayed.

## Decision

Two objects, with different jobs.

`dim_lender` is derived, on every build, from the year-stamped filer lists:

- one row per (LEI, name as filed);
- `lender_key = md5(lei, lender_name)`;
- `valid_from_year` / `valid_to_year` are the first and last loaded years the
  LEI filed under that name; `is_current` marks the name in its latest year;
- the fact joins through `int_lender__names_by_year`, so a fact row's key
  depends only on its own year's filer list.

Because the key is a function of the version's content rather than of its
position in a sequence, it does not move when an earlier or later year is
loaded. Fact partitions never need rewriting when the dimension changes.

`snap_lender_registry` is a real dbt snapshot (check strategy on the name) of
each lender's current name. It answers a different question: what did this
pipeline believe on a given date, including revisions to an already published
year. It is built and tested but nothing in the marts reads it.

## Consequences

- Backfill-safe and fully rebuildable. Covered by the backfill idempotency test.
- A lender that returns to an earlier name (A, then B, then A) has two versions
  whose year ranges overlap. Fact keys are still right, because they follow the
  name filed each year, but the range columns are then a summary rather than an
  exclusive interval. `warn_lender_versions_overlap` reports these; 8 LEIs in
  the three-year demo data.
- Capitalisation-only changes create a new version. That is faithful to the
  filings; `lender_name_normalized` is there for grouping.
- An LEI present in the LAR but missing from that year's filer list (3 cases in
  the demo data) gets an explicit placeholder version, so the fact never has a
  null lender key.
- Richer slowly-changing attributes (regulator, asset size, parent) are in the
  HMDA transmittal sheet and panel files. Their static download URLs returned
  403 when this was built, so only the name from the `filers` endpoint is used.
