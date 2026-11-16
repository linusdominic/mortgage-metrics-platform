# Test fixtures

`source/` is a small sample of real public data, laid out the way
`mmp.ingest.sources.FixtureSource` expects. Tests and CI run the real ingestion,
conversion and dbt code against it with `MMP_SCOPE=fixture`, so CI never calls
the HMDA API.

| Path | Contents |
|---|---|
| `source/hmda_lar/<year>_<state>.csv` | Header plus every k-th data row of the real HMDA LAR file for that state and year (about 700 rows each; DE and RI; 2022, 2023, 2024). Rows are unmodified. |
| `source/hmda_filers/<year>_<state>.json` | The real filers response, cut down to the LEIs present in the sample. Names and counts are unmodified; counts are the lender's full state total, not the sample's. |
| `source/pmms/PMMS_history.csv` | Freddie Mac PMMS history, unmodified. |
| `source/census/national_county2020.txt` | Census 2020 county reference, unmodified. |
| `PROVENANCE.json` | For every sampled file: source URL, sha256 and row count of the raw file it was cut from, download time, and the sampling step. |

Regenerate after a demo run with `uv run python scripts/make_fixtures.py`.

The sample is systematic (every k-th row), so it keeps the mix of action codes,
loan types and sentinel values of the full files, but its rates and medians
are not the population values. Numbers in the README come from the full demo
scope, not from this sample.

HMDA LAR data is a US government work in the public domain. PMMS is published
by Freddie Mac; Census reference files are public domain.
