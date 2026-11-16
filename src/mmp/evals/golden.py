"""Golden question set: loading, reference answers and comparison helpers."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from mmp import warehouse
from mmp.settings import REPO_ROOT, Settings

DEFAULT_GOLDEN = REPO_ROOT / "evals" / "golden_questions.yml"
REL_TOL = 1e-9
ABS_TOL = 1e-9


@dataclass(frozen=True)
class GoldenQuestion:
    id: str
    question: str
    expected_call: dict[str, Any]
    reference_sql: str
    answer_type: str  # scalar | label | table
    label_column: str | None = None
    compare: str = "rows"  # rows | metric_values
    extra: dict[str, Any] = field(default_factory=dict)

    @property
    def metric(self) -> str:
        return self.expected_call["metric"]


def load_golden(path: Path | None = None) -> list[GoldenQuestion]:
    raw = yaml.safe_load((path or DEFAULT_GOLDEN).read_text())
    questions = []
    seen: set[str] = set()
    for item in raw["questions"]:
        if item["id"] in seen:
            raise ValueError(f"duplicate golden question id {item['id']}")
        seen.add(item["id"])
        answer = item.get("answer", {"type": "table"})
        if answer["type"] not in ("scalar", "label", "table"):
            raise ValueError(f"{item['id']}: unknown answer type {answer['type']!r}")
        questions.append(
            GoldenQuestion(
                id=item["id"],
                question=item["question"].strip(),
                expected_call=item["expected_call"],
                reference_sql=item["reference_sql"].strip(),
                answer_type=answer["type"],
                label_column=answer.get("label_column"),
                compare=item.get("compare", "rows"),
            )
        )
    return questions


def reference_rows(settings: Settings, question: GoldenQuestion) -> list[dict[str, Any]]:
    """Run the independent reference SQL straight against the marts."""
    with warehouse.connect(settings.warehouse_path, read_only=True) as con:
        cursor = con.execute(question.reference_sql)
        columns = [d[0] for d in cursor.description]
        return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]


def _num(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def values_equal(a: Any, b: Any, *, zero_is_null: bool = False) -> bool:
    """Numbers within tolerance; None only equals None (or 0 for additive counts)."""
    if a is None or b is None:
        if a is None and b is None:
            return True
        other = a if b is None else b
        return zero_is_null and isinstance(other, int | float) and other == 0
    if (
        (isinstance(a, int | float) and isinstance(b, int | float))
        or hasattr(a, "is_finite")
        or hasattr(b, "is_finite")
    ):
        return math.isclose(_num(a), _num(b), rel_tol=REL_TOL, abs_tol=ABS_TOL)
    return str(a) == str(b)


def compare_rows(
    actual: list[dict[str, Any]],
    expected: list[dict[str, Any]],
    metric: str,
    mode: str = "rows",
    additive: bool = False,
) -> tuple[bool, str]:
    """Compare tool rows with reference rows. Returns (match, explanation)."""
    if mode == "metric_values":
        a = sorted((_num(r.get(metric)) or 0.0) for r in actual)
        e = sorted((_num(r.get(metric)) or 0.0) for r in expected)
        if len(a) != len(e):
            return False, f"row count {len(a)} != {len(e)}"
        for x, y in zip(a, e, strict=True):
            if not math.isclose(x, y, rel_tol=REL_TOL, abs_tol=ABS_TOL):
                return False, f"metric values differ: {a} != {e}"
        return True, f"{len(a)} metric values match"

    def drop_empty(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        # An additive metric over no rows is "no row" in one engine and 0 in another.
        return (
            [r for r in rows if not (additive and (r.get(metric) is None or r.get(metric) == 0))]
            if additive
            else rows
        )

    actual, expected = drop_empty(actual), drop_empty(expected)
    if len(actual) != len(expected):
        return False, f"row count {len(actual)} != {len(expected)}"
    if not expected:
        return True, "both empty"
    dims = [c for c in expected[0] if c != metric]
    missing = [c for c in [*dims, metric] if actual and c not in actual[0]]
    if missing:
        return False, f"tool result lacks column(s) {missing}"

    def key(row: dict[str, Any]) -> tuple:
        return tuple(str(row[d]) for d in dims)

    expected_by_key = {key(r): r for r in expected}
    if len(expected_by_key) != len(expected):
        return False, "reference rows are not unique on their dimensions"
    for row in actual:
        ref = expected_by_key.get(key(row))
        if ref is None:
            return False, f"unexpected row {key(row)}"
        if not values_equal(row[metric], ref[metric], zero_is_null=additive):
            return False, f"{key(row)}: {row[metric]!r} != {ref[metric]!r}"
    return True, f"{len(actual)} rows match"
