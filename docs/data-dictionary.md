# Data dictionary

Column-level documentation for every model lives in the dbt YAML files and is
persisted to the warehouse as column comments (`make docs` builds the dbt docs
site). This page covers what the YAML cannot: where the data comes from, what
it does and does not mean, and the conventions used.

## Sources

All endpoints were checked on 2026-11-17 from the machine that built this repo.

### HMDA Loan Application Register (LAR)

| | |
|---|---|
| Publisher | CFPB, on behalf of the FFIEC |
| Licence | US government work, public domain |
| Endpoint | `https://ffiec.cfpb.gov/v2/data-browser-api/view/csv?states=<ST>&years=<YYYY>` |
| Behaviour | Responds `301` to a pre-built file on `https://files.ffiec.cfpb.gov/data-browser/datasets/<YYYY>/filtered-queries/...csv`; the client follows the redirect and records the final URL in the manifest |
| API documentation | https://ffiec.cfpb.gov/documentation/api/data-browser/ |
| Field definitions | https://ffiec.cfpb.gov/documentation/publications/loan-level-datasets/lar-data-fields |
| Grain | One row per reported record: an application, a preapproval request, or a purchased loan |
| Format | CSV, 99 columns, 2018-and-later LAR layout |
| Years | 2018 onward; this repo was run for 2022, 2023 and 2024 |

What it is: the public, privacy-modified loan-level file that covered lenders
file under the Home Mortgage Disclosure Act (Regulation C).

What it is not:

- **Not performance or servicing data.** Nothing about payments, delinquency,
  prepayment or loss. It ends at the credit decision.
- **Not identifiable.** No universal loan identifier, no dates (only the
  activity year), no address below census tract. Loan amount and property value
  are published as the midpoint of a $10,000 band; age is banded; DTI is partly
  banded.
- **Not every lender or loan.** Small-volume lenders are exempt from filing,
  and partially exempt filers report `Exempt` for many pricing fields.
- **Filtered by property state.** Records with no state are not returned by a
  state query, so state partitions do not sum exactly to the national file.

### HMDA filers

| | |
|---|---|
| Endpoint | `https://ffiec.cfpb.gov/v2/data-browser-api/view/filers?states=<ST>&years=<YYYY>` |
| Format | JSON: `{"institutions": [{"lei", "name", "count", "period"}]}` |
| Use | Lender name as filed for the year (`dim_lender`) |

The richer institution files (transmittal sheet and panel: regulator, assets,
parent) are published as static downloads. The static URLs tried during this
build returned 403, so they are not used.

### Freddie Mac Primary Mortgage Market Survey (PMMS)

| | |
|---|---|
| Endpoint | `https://www.freddiemac.com/pmms/docs/PMMS_history.csv` |
| Format | CSV, weekly, 1971 onward: `date, pmms30, pmms30p, pmms15, ...` |
| Use | Annual average 30-year and 15-year rates in `dim_date`; benchmark for `avg_rate_over_pmms_30y` |
| Terms | Published by Freddie Mac for public use with attribution; see https://www.freddiemac.com/pmms |

Joined at year grain only, because the LAR has no dates.

### Census county reference

| | |
|---|---|
| Endpoint | `https://www2.census.gov/geo/docs/reference/codes2020/national_county2020.txt` |
| Format | Pipe-delimited: `STATE|STATEFP|COUNTYFP|COUNTYNS|COUNTYNAME|CLASSFP|FUNCSTAT` |
| Use | County names in `dim_geography` |

## Layers

| Layer | Where | Contents |
|---|---|---|
| raw | `data/raw/<dataset>/year=YYYY/state=ST/` | Bytes exactly as downloaded, plus `_manifest.json` (URL, bytes, sha256, row count, fetched_at) |
| bronze | `data/bronze/...` Parquet | Same rows, explicit types, sentinels preserved as strings, lineage columns added |
| staging | schema `staging` (views) | Renamed, sentinels parsed to number + status, reject reason assigned |
| intermediate | schema `intermediate` (views) | Business definitions: outcome flags, derived measures, dimension keys |
| marts | schema `marts` (tables) | Star schema with enforced contracts |
| quarantine | schema `quarantine` | Rejected rows with reasons |
| ops | schema `ops` | Reconciliation, volume anomalies, partition load log, run metrics, DQ results |
| reference | schema `reference` | Seeds: HMDA code labels and value domains |
| snapshots | schema `snapshots` | `snap_lender_registry` |

## Sentinel values

| In the source | Meaning | Bronze | Staging and marts |
|---|---|---|---|
| a number | reported value | string (sentinel-capable columns) or typed | number, `*_status = reported` |
| `NA` | not applicable / not reported | `"NA"` | null, `*_status = not_applicable` |
| `Exempt` | filer exempt from the field | `"Exempt"` | null, `*_status = exempt` |
| empty | absent | null | null, `*_status = missing` |
| anything else | damaged | as is | null, `*_status = unparseable` |
| `1111` in a code column | exempt | `1111` | kept as a dimension member (`Exempt`), or null for denial reasons |
| `8888` / `9999` in age | not applicable / not available | as is | `Not applicable` / `Not available` |

## Action taken, and the definitions built on it

| Code | Meaning | application | originated | approved | denied | decisioned |
|---|---|:-:|:-:|:-:|:-:|:-:|
| 1 | Loan originated | yes | yes | yes | | yes |
| 2 | Approved but not accepted | yes | | yes | | yes |
| 3 | Application denied | yes | | | yes | yes |
| 4 | Withdrawn by applicant | yes | | | | |
| 5 | File closed for incompleteness | yes | | | | |
| 6 | Purchased loan | | | | | |
| 7 | Preapproval request denied | yes | | | | |
| 8 | Preapproval approved but not accepted | yes | | | | |

- approval rate = approved / decisioned
- denial rate = denied / decisioned
- origination rate = originated / applications

These are conventions chosen for this project and pinned by a dbt unit test
(`outcome_flags_follow_action_taken`). Other published analyses include
preapproval requests or exclude approved-not-accepted; compare definitions
before comparing numbers.

## Star schema

| Table | Grain | Key | Load |
|---|---|---|---|
| `fct_loan_applications` | one LAR record | `application_id` | incremental, partition overwrite by (year, state) |
| `fct_application_denial_reasons` | one stated reason per denied application | `denial_reason_id` | incremental, same |
| `dim_lender` | LEI x name as filed (Type 2) | `lender_key` | rebuilt each run |
| `dim_geography` | county, plus "county not reported" per state | `geography_key` | rebuilt each run |
| `dim_loan_product` | combination of nine loan attributes (junk) | `loan_product_key` | rebuilt from seeds |
| `dim_applicant_demographics` | combination of derived ethnicity, race, sex, age band (junk) | `applicant_demographics_key` | rebuilt from seeds |
| `dim_date` | activity year | `date_key` | rebuilt each run |

## Demographic fields

`derived_ethnicity`, `derived_race`, `derived_sex` and `applicant_age` are
collected under Regulation C so that regulators and the public can monitor
fair lending. They are in the model because that is the purpose of the dataset.

Use them with care:

- Public HMDA lacks credit score, full DTI and LTV detail, and other
  underwriting inputs. A difference in approval or pricing between groups in
  this data is a prompt for analysis, not a finding of discrimination.
- A large share of records carry "not available" values, and reporting of these
  fields differs by channel (in person, phone, online).
- The serving layer withholds metric values for demographic cells with fewer
  than 10 records and attaches a note to every demographic breakdown.
- Do not join this data to anything that could re-identify applicants, and do
  not use it to make decisions about individuals.
