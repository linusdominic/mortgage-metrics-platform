"""Static report: five charts, each one a metric query through the semantic layer.

No chart reads the marts directly. If a number here disagrees with what an agent
gets from the MCP server, that is a bug in exactly one place.

Output: <out_dir>/index.html plus one PNG per chart. Every chart has its data
table underneath, so nothing depends on colour alone.
"""

from __future__ import annotations

import html
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from mmp import telemetry
from mmp.semantic.layer import MetricRequest, QueryResult, SemanticLayer
from mmp.settings import Settings

SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_SECONDARY = "#52514e"
MUTED = "#898781"
GRID = "#e1e0d9"
AXIS = "#c3c2b7"
# Fixed categorical order; a state keeps its colour in every chart.
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]
SINGLE = "#2a78d6"

RATE_SPREAD_ORDER = [
    "Below 0",
    "0 to <0.5",
    "0.5 to <1.0",
    "1.0 to <1.5",
    "1.5 to <3.0",
    "3.0 or more",
    "Not reported",
]


@dataclass
class Chart:
    key: str
    title: str
    subtitle: str
    request: dict
    result: QueryResult
    image: str


def _style(ax, grid_axis: str = "y") -> None:
    ax.set_facecolor(SURFACE)
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(AXIS)
    ax.tick_params(colors=MUTED, length=0, labelsize=9)
    ax.grid(axis=grid_axis, color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)


def _figure(title: str, subtitle: str, height: float = 3.8):
    fig, ax = plt.subplots(figsize=(8.4, height), dpi=160)
    fig.patch.set_facecolor(SURFACE)
    # Header positions are fixed in inches so tall and short charts look the same.
    fig.suptitle(
        title, x=0.02, y=1 - 0.08 / height, ha="left", va="top", fontsize=12, fontweight="bold", color=INK
    )
    fig.text(0.02, 1 - 0.40 / height, subtitle, ha="left", va="top", fontsize=8.5, color=INK_SECONDARY)
    return fig, ax


def _save(fig, path: Path) -> None:
    height = fig.get_size_inches()[1]
    fig.tight_layout(rect=(0, 0, 1, 1 - 0.62 / height))
    fig.savefig(path, facecolor=SURFACE)
    plt.close(fig)


def _lines_by_state(
    result: QueryResult, metric: str, path: Path, title: str, subtitle: str, states: list[str]
) -> None:
    fig, ax = _figure(title, subtitle)
    _style(ax)
    years = sorted({r["activity_year"] for r in result.rows})
    ends = []
    for i, state in enumerate(states):
        points = {r["activity_year"]: r[metric] for r in result.rows if r["state_code"] == state}
        ys = [points.get(y) for y in years]
        colour = SERIES[i % len(SERIES)]
        ax.plot(
            years,
            ys,
            color=colour,
            linewidth=2,
            marker="o",
            markersize=6,
            markeredgecolor=SURFACE,
            markeredgewidth=1.5,
            label=state,
        )
        ends.append((ys[-1], state))
    # Direct end labels, pushed apart when two series finish close together.
    low, high = ax.get_ylim()
    min_gap = (high - low) * 0.08
    placed = None
    for value, state in sorted(ends):
        y_text = value if placed is None else max(value, placed + min_gap)
        placed = y_text
        ax.annotate(
            f"{state}  {value:.1%}",
            (years[-1], value),
            xytext=(years[-1] + 0.06, y_text),
            textcoords="data",
            va="center",
            fontsize=9,
            color=INK,
        )
    ax.set_xticks(years)
    ax.set_xlim(years[0] - 0.15, years[-1] + 0.55)
    ax.yaxis.set_major_formatter(lambda v, _: f"{v:.0%}")
    ax.legend(
        frameon=False,
        fontsize=9,
        loc="lower left",
        bbox_to_anchor=(0, 1.0),
        ncol=len(states),
        labelcolor=INK_SECONDARY,
    )
    _save(fig, path)


def _hbars(labels: list[str], values: list[float], path: Path, title: str, subtitle: str, fmt) -> None:
    fig, ax = _figure(title, subtitle, height=0.36 * len(labels) + 1.5)
    _style(ax, grid_axis="x")
    positions = range(len(labels))
    ax.barh(list(positions), values, color=SINGLE, height=0.62)
    ax.set_yticks(list(positions))
    ax.set_yticklabels(labels, color=INK, fontsize=9)
    ax.invert_yaxis()
    ax.spines["bottom"].set_visible(False)
    ax.tick_params(axis="x", labelbottom=False)
    low, high = min(0, min(values)), max(0, max(values))
    span = high - low
    for pos, value in zip(positions, values, strict=True):
        # the label sits just beyond the end of its bar, on the side the bar points to
        if value >= 0:
            ax.text(value + span * 0.01, pos, fmt(value), va="center", ha="left", fontsize=9, color=INK)
        else:
            ax.text(value - span * 0.01, pos, fmt(value), va="center", ha="right", fontsize=9, color=INK)
    ax.set_xlim(low - (span * 0.14 if low < 0 else 0), high + span * 0.14)
    if low < 0:
        ax.axvline(0, color=AXIS, linewidth=1)
    _save(fig, path)


def _vbars(labels: list[str], values: list[float], path: Path, title: str, subtitle: str, fmt) -> None:
    fig, ax = _figure(title, subtitle)
    _style(ax)
    ax.bar(labels, values, color=SINGLE, width=0.66)
    for i, value in enumerate(values):
        ax.text(i, value, fmt(value), ha="center", va="bottom", fontsize=9, color=INK)
    ax.tick_params(axis="x", labelcolor=INK, labelsize=9)
    ax.yaxis.set_major_formatter(lambda v, _: f"{v:,.0f}")
    _save(fig, path)


def _table_html(result: QueryResult, metric: str) -> str:
    def cell(column: str, value) -> str:
        if value is None:
            return "<td class='num'>-</td>"
        if column == metric:
            if result.unit == "ratio":
                return f"<td class='num'>{value:.4f}</td>"
            if isinstance(value, float):
                return f"<td class='num'>{value:,.3f}</td>"
            return f"<td class='num'>{value:,}</td>"
        return f"<td>{html.escape(str(value))}</td>"

    head = "".join(f"<th>{html.escape(c)}</th>" for c in result.columns)
    body = "".join("<tr>" + "".join(cell(c, row[c]) for c in result.columns) + "</tr>" for row in result.rows)
    return f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"


def build_report(settings: Settings, out_dir: Path, layer: SemanticLayer | None = None) -> Path:
    layer = layer or SemanticLayer(settings)
    out_dir.mkdir(parents=True, exist_ok=True)
    charts: list[Chart] = []

    def run(**kwargs) -> QueryResult:
        return layer.query(MetricRequest.from_args(**kwargs))

    with telemetry.step(settings.telemetry_dir, "publish.report", "static report") as rec:
        years_result = run(metric="lar_record_count", dimensions=["activity_year"])
        years = sorted(r["activity_year"] for r in years_result.rows)
        latest = years[-1]
        states_result = run(metric="lar_record_count", dimensions=["state_code"])
        states = sorted(r["state_code"] for r in states_result.rows)
        total_records = sum(r["lar_record_count"] for r in years_result.rows)
        scope = f"{', '.join(states)}; activity years {years[0]}-{latest}"

        # 1. approval rate by state and year
        req = {"metric": "approval_rate", "dimensions": ["activity_year", "state_code"]}
        res = run(**req)
        _lines_by_state(
            res,
            "approval_rate",
            out_dir / "approval_rate_by_state.png",
            "Approval rate by state and year",
            "Approved (originated or approved-not-accepted) / decisioned applications. " + scope,
            states,
        )
        charts.append(
            Chart(
                "approval_rate_by_state",
                "Approval rate by state and year",
                "approval_rate by activity_year, state_code",
                req,
                res,
                "approval_rate_by_state.png",
            )
        )

        # 2. denial reason mix, latest year
        req = {
            "metric": "denial_reason_citations",
            "dimensions": ["denial_reason"],
            "filters": [{"dimension": "activity_year", "op": "=", "value": latest}],
            "order_by": ["-denial_reason_citations"],
        }
        res = run(**req)
        total = sum(r["denial_reason_citations"] for r in res.rows)
        _hbars(
            [r["denial_reason"] for r in res.rows],
            [r["denial_reason_citations"] / total for r in res.rows],
            out_dir / "denial_reasons.png",
            f"Stated denial reasons, {latest}",
            f"Share of {total:,} reason citations on denied applications (a denial can cite up to four).",
            lambda v: f"{v:.1%}",
        )
        charts.append(
            Chart(
                "denial_reasons",
                f"Stated denial reasons, {latest}",
                "denial_reason_citations by denial_reason",
                req,
                res,
                "denial_reasons.png",
            )
        )

        # 3. rate spread distribution, latest year
        req = {
            "metric": "origination_count",
            "dimensions": ["rate_spread_band"],
            "filters": [
                {"dimension": "activity_year", "op": "=", "value": latest},
                {"dimension": "rate_spread_band", "op": "!=", "value": "Not originated"},
            ],
        }
        res = run(**req)
        by_band = {r["rate_spread_band"]: r["origination_count"] for r in res.rows}
        bands = [b for b in RATE_SPREAD_ORDER if b in by_band]
        res.rows.sort(key=lambda r: RATE_SPREAD_ORDER.index(r["rate_spread_band"]))
        _vbars(
            bands,
            [by_band[b] for b in bands],
            out_dir / "rate_spread_distribution.png",
            f"Rate spread distribution of originated loans, {latest}",
            "Originations by rate spread band (APR minus average prime offer rate, percentage points).",
            lambda v: f"{v:,.0f}",
        )
        charts.append(
            Chart(
                "rate_spread_distribution",
                f"Rate spread distribution, {latest}",
                "origination_count by rate_spread_band",
                req,
                res,
                "rate_spread_distribution.png",
            )
        )

        # 4. top lenders by originations, latest year
        req = {
            "metric": "origination_count",
            "dimensions": ["lender_name"],
            "filters": [{"dimension": "activity_year", "op": "=", "value": latest}],
            "order_by": ["-origination_count"],
            "limit": 10,
        }
        res = run(**req)
        _hbars(
            [r["lender_name"][:44] for r in res.rows],
            [r["origination_count"] for r in res.rows],
            out_dir / "top_lenders.png",
            f"Top 10 lenders by originations, {latest}",
            f"Loans originated in {', '.join(states)}.",
            lambda v: f"{v:,.0f}",
        )
        charts.append(
            Chart(
                "top_lenders",
                f"Top 10 lenders by originations, {latest}",
                "origination_count by lender_name, top 10",
                req,
                res,
                "top_lenders.png",
            )
        )

        # 5. note rate over the PMMS benchmark by loan type, latest year
        req = {
            "metric": "avg_rate_over_pmms_30y",
            "dimensions": ["loan_type"],
            "filters": [{"dimension": "activity_year", "op": "=", "value": latest}],
        }
        res = run(**req)
        res.rows = [r for r in res.rows if r["avg_rate_over_pmms_30y"] is not None]
        res.rows.sort(key=lambda r: -r["avg_rate_over_pmms_30y"])
        _hbars(
            [r["loan_type"] for r in res.rows],
            [r["avg_rate_over_pmms_30y"] for r in res.rows],
            out_dir / "rate_over_pmms.png",
            f"Average note rate over the 30-year PMMS average, {latest}",
            "Originated first-lien 30-year closed-end loans; percentage points over the Freddie Mac annual average.",
            lambda v: f"{v:+.2f} pp",
        )
        charts.append(
            Chart(
                "rate_over_pmms",
                f"Note rate over PMMS by loan type, {latest}",
                "avg_rate_over_pmms_30y by loan_type",
                req,
                res,
                "rate_over_pmms.png",
            )
        )
        rec.rows_out = len(charts)

    sections = []
    for c in charts:
        sections.append(
            f"<section><h2>{html.escape(c.title)}</h2>"
            f"<img src='{c.image}' alt='{html.escape(c.title)}'>"
            f"<details><summary>Data and metric request ({html.escape(c.subtitle)})</summary>"
            f"<pre>{html.escape(str(c.request))}</pre>{_table_html(c.result, c.result.metric)}</details></section>"
        )
    page = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Mortgage metrics report</title>
<style>
body {{ font-family: -apple-system, "Segoe UI", Helvetica, Arial, sans-serif; background: #f9f9f7; color: {INK}; margin: 0; }}
main {{ max-width: 880px; margin: 0 auto; padding: 24px 16px 48px; }}
h1 {{ font-size: 22px; margin-bottom: 4px; }} h2 {{ font-size: 16px; margin: 28px 0 8px; }}
p.meta {{ color: {INK_SECONDARY}; font-size: 13px; line-height: 1.5; }}
img {{ width: 100%; border: 1px solid {GRID}; border-radius: 6px; }}
details {{ margin-top: 6px; font-size: 13px; }} summary {{ cursor: pointer; color: {INK_SECONDARY}; }}
pre {{ background: #fff; border: 1px solid {GRID}; padding: 8px; overflow-x: auto; font-size: 12px; }}
table {{ border-collapse: collapse; font-size: 12px; margin-top: 6px; }}
th, td {{ border-bottom: 1px solid {GRID}; padding: 3px 10px; text-align: left; }} td.num {{ text-align: right; font-variant-numeric: tabular-nums; }}
</style></head><body><main>
<h1>Mortgage metrics report</h1>
<p class="meta">HMDA Loan Application Register (CFPB/FFIEC public data). Scope: {html.escape(scope)}; {total_records:,} LAR records.
Generated {datetime.now(UTC):%Y-%m-%d %H:%M} UTC by <code>mmp report</code>. Every chart is a governed metric
queried through the semantic layer. HMDA describes applications and originations, not loan performance.
Differences between demographic groups in this data are not, on their own, evidence of discrimination.</p>
{"".join(sections)}
</main></body></html>
"""
    index = out_dir / "index.html"
    index.write_text(page)
    return index
