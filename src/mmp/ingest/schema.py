"""Explicit schema for the HMDA public LAR CSV (2018+ format, 99 columns).

The column order below is the order the FFIEC data browser API returns. It is a
contract: `convert` refuses a file whose header differs (fail closed) instead of
guessing.

Typing policy
-------------
HMDA mixes three kinds of values in what look like numeric columns:

* real numbers                         e.g. interest_rate = "6.875"
* "NA"      not applicable / not reported for this record
* "Exempt"  the institution is exempt from reporting the field (partial
            exemption under EGRRCPA, small filers)

plus bucketed text in a few columns (debt_to_income_ratio "30%-<36%",
total_units "5-24", applicant_age "35-44").

Bronze does not interpret any of that. Columns that can carry a sentinel or a
bucket stay STRING so the Parquet file is a faithful, typed copy of the CSV.
Columns that are pure integer codes are INT32 and columns that are always
numeric are DOUBLE; a value that does not parse in one of those is a contract
break and fails the partition. The numeric interpretation, with a companion
`*_status` column that keeps the reported / not_applicable / exempt
distinction, happens once, in dbt staging (macro `hmda_numeric`).
"""

from __future__ import annotations

import pyarrow as pa

STR, INT, DBL = pa.string(), pa.int32(), pa.float64()

# (csv_name, arrow_type). Bronze column names are the csv names with "-" -> "_".
LAR_COLUMNS: list[tuple[str, pa.DataType]] = [
    ("activity_year", INT),
    ("lei", STR),
    ("derived_msa-md", STR),
    ("state_code", STR),
    ("county_code", STR),  # 5-digit FIPS or "NA"
    ("census_tract", STR),  # 11-digit FIPS or "NA"
    ("conforming_loan_limit", STR),
    ("derived_loan_product_type", STR),
    ("derived_dwelling_category", STR),
    ("derived_ethnicity", STR),
    ("derived_race", STR),
    ("derived_sex", STR),
    ("action_taken", INT),
    ("purchaser_type", INT),
    ("preapproval", INT),
    ("loan_type", INT),
    ("loan_purpose", INT),
    ("lien_status", INT),
    ("reverse_mortgage", INT),
    ("open-end_line_of_credit", INT),
    ("business_or_commercial_purpose", INT),
    ("loan_amount", DBL),
    ("loan_to_value_ratio", STR),  # number | NA | Exempt
    ("interest_rate", STR),  # number | NA | Exempt
    ("rate_spread", STR),  # number | NA | Exempt
    ("hoepa_status", INT),
    ("total_loan_costs", STR),  # number | NA | Exempt
    ("total_points_and_fees", STR),  # number | NA | Exempt
    ("origination_charges", STR),  # number | NA | Exempt
    ("discount_points", STR),  # number | NA | Exempt | empty
    ("lender_credits", STR),  # number | NA | Exempt | empty
    ("loan_term", STR),  # number | NA | Exempt
    ("prepayment_penalty_term", STR),  # number | NA | Exempt
    ("intro_rate_period", STR),  # number | NA | Exempt
    ("negative_amortization", INT),
    ("interest_only_payment", INT),
    ("balloon_payment", INT),
    ("other_nonamortizing_features", INT),
    ("property_value", STR),  # number | NA | Exempt
    ("construction_method", INT),
    ("occupancy_type", INT),
    ("manufactured_home_secured_property_type", INT),
    ("manufactured_home_land_property_interest", INT),
    ("total_units", STR),  # 1-4 or a bucket such as "5-24"
    ("multifamily_affordable_units", STR),  # number | NA | Exempt
    ("income", STR),  # thousands of dollars | NA
    ("debt_to_income_ratio", STR),  # number, bucket, NA or Exempt
    ("applicant_credit_score_type", INT),
    ("co-applicant_credit_score_type", INT),
    ("applicant_ethnicity-1", INT),
    ("applicant_ethnicity-2", INT),
    ("applicant_ethnicity-3", INT),
    ("applicant_ethnicity-4", INT),
    ("applicant_ethnicity-5", INT),
    ("co-applicant_ethnicity-1", INT),
    ("co-applicant_ethnicity-2", INT),
    ("co-applicant_ethnicity-3", INT),
    ("co-applicant_ethnicity-4", INT),
    ("co-applicant_ethnicity-5", INT),
    ("applicant_ethnicity_observed", INT),
    ("co-applicant_ethnicity_observed", INT),
    ("applicant_race-1", INT),
    ("applicant_race-2", INT),
    ("applicant_race-3", INT),
    ("applicant_race-4", INT),
    ("applicant_race-5", INT),
    ("co-applicant_race-1", INT),
    ("co-applicant_race-2", INT),
    ("co-applicant_race-3", INT),
    ("co-applicant_race-4", INT),
    ("co-applicant_race-5", INT),
    ("applicant_race_observed", INT),
    ("co-applicant_race_observed", INT),
    ("applicant_sex", INT),
    ("co-applicant_sex", INT),
    ("applicant_sex_observed", INT),
    ("co-applicant_sex_observed", INT),
    ("applicant_age", STR),  # bucket, or 8888 / 9999
    ("co-applicant_age", STR),
    ("applicant_age_above_62", STR),  # Yes | No | NA
    ("co-applicant_age_above_62", STR),
    ("submission_of_application", INT),
    ("initially_payable_to_institution", INT),
    ("aus-1", INT),
    ("aus-2", INT),
    ("aus-3", INT),
    ("aus-4", INT),
    ("aus-5", INT),
    ("denial_reason-1", INT),
    ("denial_reason-2", INT),
    ("denial_reason-3", INT),
    ("denial_reason-4", INT),
    ("tract_population", INT),
    ("tract_minority_population_percent", DBL),
    ("ffiec_msa_md_median_family_income", INT),
    ("tract_to_msa_income_percentage", DBL),
    ("tract_owner_occupied_units", INT),
    ("tract_one_to_four_family_homes", INT),
    ("tract_median_age_of_housing_units", INT),
]

LAR_CSV_HEADER: list[str] = [name for name, _ in LAR_COLUMNS]


def bronze_name(csv_name: str) -> str:
    return csv_name.replace("-", "_")


# Lineage columns appended by the converter.
LINEAGE_FIELDS: list[pa.Field] = [
    pa.field("source_row_number", pa.int64(), nullable=False),
    pa.field("_source_sha256", pa.string(), nullable=False),
    pa.field("_source_url", pa.string(), nullable=False),
    pa.field("_ingested_at", pa.timestamp("us", tz="UTC"), nullable=False),
]

LAR_BRONZE_SCHEMA: pa.Schema = pa.schema(
    [pa.field(bronze_name(name), dtype) for name, dtype in LAR_COLUMNS] + LINEAGE_FIELDS
)


class SchemaContractError(RuntimeError):
    """The source file does not match the declared contract."""


def check_header(header: list[str]) -> None:
    if header == LAR_CSV_HEADER:
        return
    missing = [c for c in LAR_CSV_HEADER if c not in header]
    extra = [c for c in header if c not in LAR_CSV_HEADER]
    detail = (
        f"missing={missing} unexpected={extra}" if (missing or extra) else "same columns, different order"
    )
    raise SchemaContractError(f"LAR header does not match the declared contract: {detail}")
