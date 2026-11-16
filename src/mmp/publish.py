"""Metric publish step.

Runs only after the quality gate passes. It produces the artifacts consumers
read without touching the warehouse:

  published/metric_catalog.json     the governed catalogue (metrics, dimensions, grains)
  published/metric_snapshot.csv     every metric by activity_year x state_code
  published/semantic_manifest.json  the compiled semantic manifest the catalogue came from

All three come from the semantic layer, so they cannot disagree with what the
MCP server answers.
"""

from __future__ import annotations

import csv
import json
import shutil
from datetime import UTC, datetime
from pathlib import Path

from mmp import telemetry
from mmp.semantic.layer import MetricRequest, SemanticLayer
from mmp.settings import Settings


def catalog_payload(layer: SemanticLayer) -> list[dict]:
    out = []
    for info in layer.catalog().values():
        out.append(
            {
                "name": info.name,
                "label": info.label,
                "description": info.description,
                "type": info.type,
                "unit": info.unit,
                "numerator": info.numerator,
                "denominator": info.denominator,
                "grains": list(info.grains),
                "dimensions": [
                    {"name": d.name, "description": d.description, "demographic": d.is_demographic}
                    for d in info.dimensions.values()
                ],
            }
        )
    return out


def catalog_markdown(layer: SemanticLayer) -> str:
    lines = ["| Metric | Type | Unit | Definition |", "|---|---|---|---|"]
    for info in layer.catalog().values():
        formula = f" `{info.numerator} / {info.denominator}`." if info.type == "ratio" else ""
        lines.append(f"| `{info.name}` | {info.type} | {info.unit} | {info.description}{formula} |")
    return "\n".join(lines)


def publish(settings: Settings, layer: SemanticLayer | None = None) -> dict[str, Path]:
    layer = layer or SemanticLayer(settings)
    out_dir = settings.data_dir / "published"
    out_dir.mkdir(parents=True, exist_ok=True)
    with telemetry.step(settings.telemetry_dir, "publish.metrics", "metric catalogue and snapshot") as rec:
        catalog = catalog_payload(layer)
        catalog_path = out_dir / "metric_catalog.json"
        catalog_path.write_text(
            json.dumps({"generated_at": datetime.now(UTC).isoformat(), "metrics": catalog}, indent=2)
        )

        snapshot_path = out_dir / "metric_snapshot.csv"
        rows_written = 0
        with snapshot_path.open("w", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(["metric", "unit", "activity_year", "state_code", "value"])
            for info in layer.catalog().values():
                result = layer.query(
                    MetricRequest.from_args(
                        info.name, ["activity_year", "state_code"], limit=layer.limits.max_rows
                    )
                )
                for row in result.rows:
                    writer.writerow(
                        [info.name, info.unit, row["activity_year"], row["state_code"], row[info.name]]
                    )
                    rows_written += 1

        manifest_src = settings.dbt_target_dir / "semantic_manifest.json"
        manifest_path = out_dir / "semantic_manifest.json"
        shutil.copyfile(manifest_src, manifest_path)
        rec.rows_out = rows_written
        rec.bytes_written = sum(p.stat().st_size for p in (catalog_path, snapshot_path, manifest_path))
        rec.detail = {"metrics": len(catalog)}
    return {"catalog": catalog_path, "snapshot": snapshot_path, "semantic_manifest": manifest_path}
