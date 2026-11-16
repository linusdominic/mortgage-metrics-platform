import dataclasses

import pytest

from mmp import warehouse
from mmp.semantic.guardrails import GuardrailError, Limits, MetricRequest, render_where, sql_literal
from mmp.semantic.layer import QueryTimeout, SemanticLayer, SemanticLayerError

EXPECTED_METRICS = {
    "application_count", "origination_count", "approval_rate", "denial_rate", "origination_rate",
    "median_loan_amount", "avg_loan_amount", "avg_rate_spread", "median_rate_spread",
    "median_loan_to_income", "denial_reason_citations", "originated_loan_volume", "avg_interest_rate",
    "avg_rate_over_pmms_30y", "lar_record_count", "denial_count",
}  # fmt: skip


@pytest.fixture
def layer(settings):
    return SemanticLayer(settings)


def req(metric="approval_rate", **kwargs):
    return MetricRequest.from_args(metric, **kwargs)


def test_catalog_comes_from_the_dbt_semantic_manifest(layer):
    catalog = layer.catalog()
    assert set(catalog) >= EXPECTED_METRICS
    rate = catalog["approval_rate"]
    assert (rate.type, rate.numerator, rate.denominator) == (
        "ratio",
        "approval_count",
        "decisioned_application_count",
    )
    assert rate.grains == ("year",) and rate.unit == "ratio"
    assert {
        "state_code",
        "lender_name",
        "county_name",
        "loan_purpose",
        "derived_race",
        "activity_year",
    } <= set(rate.dimensions)
    assert rate.dimensions["derived_race"].is_demographic and not rate.dimensions["state_code"].is_demographic
    assert all(m.description for m in catalog.values()), "every metric needs a description"


def test_single_compile_path_produces_sql_over_the_marts(layer):
    compiled = layer.compile(
        req(dimensions=["state_code"], filters=[{"dimension": "activity_year", "op": "=", "value": 2024}])
    )
    sql = compiled.sql.lower()
    assert "fct_loan_applications" in sql and "is_approved" in sql and "is_decisioned" in sql
    assert compiled.output_columns == ("state_code", "approval_rate")


def test_query_matches_hand_written_sql(layer, settings):
    result = layer.query(req(dimensions=["state_code"], grain="year"))
    with warehouse.connect(settings.warehouse_path) as con:
        expected = con.execute(
            "select activity_year, state_code, count(*) filter (where action_taken_code in (1,2)) * 1.0 "
            "/ count(*) filter (where action_taken_code in (1,2,3)) from marts.fct_loan_applications group by 1, 2"
        ).fetchall()
    assert result.columns == ["activity_year", "state_code", "approval_rate"]
    got = {(r["activity_year"], r["state_code"]): r["approval_rate"] for r in result.rows}
    assert got == pytest.approx({(y, s): v for y, s, v in expected})


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"metric": "revenue"}, "unknown metric"),
        ({"metric": "select 1"}, "unknown metric"),
        ({"dimensions": ["ssn"]}, "not allowed"),
        ({"dimensions": ["state_code; drop table x"]}, "not allowed"),
        (
            {"dimensions": ["state_code", "lender_name", "county_name", "loan_type", "loan_purpose"]},
            "at most 4 dimensions",
        ),
        ({"grain": "month"}, "grain 'month' is not available"),
        ({"limit": 0}, "at least 1"),
        ({"limit": 5000}, "exceeds the maximum"),
        ({"limit": "10"}, "must be an integer"),
        ({"filters": [{"dimension": "ssn", "op": "=", "value": "1"}]}, "cannot filter"),
        (
            {"filters": [{"dimension": "state_code", "op": "like", "value": "D%"}]},
            "operator 'like' is not allowed",
        ),
        (
            {"filters": [{"dimension": "state_code", "op": ">", "value": "A"}]},
            "only allowed on activity_year",
        ),
        ({"filters": [{"dimension": "state_code", "op": "=", "value": ["DE"]}]}, "single value"),
        ({"filters": [{"dimension": "state_code", "op": "in", "value": "DE"}]}, "non-empty list"),
        ({"filters": [{"dimension": "state_code", "op": "=", "value": {"a": 1}}]}, "single value"),
        ({"filters": [{"dimension": "state_code", "op": "=", "value": None}]}, "string or a number"),
        ({"filters": [{"dimension": "state_code", "op": "=", "value": "x" * 500}]}, "longer than"),
        ({"filters": [{"dimension": "state_code", "op": "=", "value": "DE\x00"}]}, "control characters"),
        ({"filters": [{"dimension": "activity_year", "op": "=", "value": 1999}]}, "between 2018"),
        ({"filters": [{"dimension": "activity_year", "op": "=", "value": "2024; drop"}]}, "integers"),
        (
            {"filters": [{"dimension": "state_code", "op": "=", "value": "DE", "sql": "1=1"}]},
            "unknown filter keys",
        ),
        ({"filters": ["state_code = 'DE'"]}, "must be an object"),
        ({"filters": [{"dimension": "state_code", "op": "=", "value": "DE"}] * 9}, "at most 8 filters"),
        ({"order_by": ["lender_name"]}, "order_by"),
    ],
)
def test_guardrails_reject(layer, kwargs, message):
    kwargs = {"metric": "approval_rate", **kwargs}
    with pytest.raises(GuardrailError, match=message):
        layer.compile(MetricRequest.from_args(**kwargs))


def test_filter_values_are_escaped_literals(layer):
    assert sql_literal("O'Brien") == "'O''Brien'"
    hostile = "DE' OR '1'='1"
    compiled = layer.compile(
        req("application_count", filters=[{"dimension": "state_code", "op": "=", "value": hostile}])
    )
    assert "'DE'' OR ''1''=''1'" in compiled.sql
    result = layer.query(
        req("application_count", filters=[{"dimension": "state_code", "op": "=", "value": hostile}])
    )
    assert result.rows[0]["application_count"] in (None, 0), "the hostile value must match no rows"


def test_multi_hop_dimension_filters_use_entity_paths(layer):
    info = layer.catalog()["denial_reason_citations"]
    request = req(
        "denial_reason_citations",
        filters=[{"dimension": "loan_purpose", "op": "=", "value": "Home purchase"}],
    )
    clauses = render_where(request.filters, info)
    assert clauses == [
        "{{ Dimension('loan_product__loan_purpose', entity_path=['application']) }} = 'Home purchase'"
    ]
    assert layer.query(request).row_count == 1


def test_row_limit_is_enforced_and_reported(layer):
    result = layer.query(
        req("origination_count", dimensions=["lender_name"], limit=5, order_by=["-origination_count"])
    )
    assert result.row_count == 5 and result.truncated
    assert any("truncated" in n for n in result.notes)
    values = [r["origination_count"] for r in result.rows]
    assert values == sorted(values, reverse=True)


def test_year_grain_and_year_filters(layer):
    result = layer.query(
        req(
            "lar_record_count",
            grain="year",
            filters=[{"dimension": "activity_year", "op": ">=", "value": 2024}],
        )
    )
    assert [r["activity_year"] for r in result.rows] == [2024]
    both = layer.query(req("lar_record_count", dimensions=["activity_year"]))
    assert [r["activity_year"] for r in both.rows] == [2023, 2024]


def test_small_demographic_cells_are_suppressed(settings):
    strict = SemanticLayer(settings, limits=dataclasses.replace(Limits(), min_cell_size=40))
    result = strict.query(req("denial_rate", dimensions=["derived_race", "state_code"]))
    assert result.suppressed_rows > 0
    assert sum(r["denial_rate"] is None for r in result.rows) >= result.suppressed_rows
    assert "lar_record_count" not in result.columns, "the support count is internal"
    assert any("withheld" in n for n in result.notes) and any(
        "not evidence of discrimination" in n for n in result.notes
    )
    # non-demographic breakdowns are never suppressed
    plain = strict.query(req("denial_rate", dimensions=["state_code"]))
    assert plain.suppressed_rows == 0 and not plain.notes


def test_query_timeout_cancels_the_query(layer):
    with pytest.raises(QueryTimeout, match="timeout"):
        layer.execute_sql(
            "select count(*) from range(100000000000) t(i) where hash(i) % 7 = 3", timeout_s=0.2
        )


def test_serving_connection_is_read_only(layer):
    with pytest.raises(SemanticLayerError):
        layer.execute_sql("delete from marts.fct_loan_applications")
    with pytest.raises(SemanticLayerError):
        layer.execute_sql("create table marts.x as select 1")


def test_lineage_reaches_the_sources(layer):
    lineage = layer.lineage("approval_rate")
    assert lineage["measures"] == ["approvals", "decisioned_applications"]
    assert lineage["semantic_models"][0]["dbt_model"] == "fct_loan_applications"
    upstream = lineage["upstream_dbt_nodes"]["fct_loan_applications"]
    assert "source:bronze.hmda_lar" in upstream and "model:stg_hmda__lar_typed" in upstream
    with pytest.raises(GuardrailError):
        layer.lineage("nope")
