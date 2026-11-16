"""Reference data: Freddie Mac PMMS weekly rates and Census county names.

Both are small single files. They are landed as-is with a manifest and then
written to bronze Parquet with explicit schemas, same as the LAR partitions.
"""

from __future__ import annotations

import csv
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa

from mmp import telemetry
from mmp.ingest.lar import _already_converted, _land, _write_atomic
from mmp.ingest.manifest import Manifest, count_data_rows
from mmp.ingest.schema import SchemaContractError
from mmp.ingest.sources import Source
from mmp.settings import Settings

PMMS_SCHEMA = pa.schema(
    [
        pa.field("week_date", pa.date32(), nullable=False),
        pa.field("rate_30y_fixed", pa.float64()),
        pa.field("rate_15y_fixed", pa.float64()),
        pa.field("_source_sha256", pa.string(), nullable=False),
        pa.field("_ingested_at", pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)

COUNTY_SCHEMA = pa.schema(
    [
        pa.field("state_code", pa.string(), nullable=False),
        pa.field("state_fips", pa.string(), nullable=False),
        pa.field("county_fips", pa.string(), nullable=False),
        pa.field("county_name", pa.string(), nullable=False),
        pa.field("_source_sha256", pa.string(), nullable=False),
        pa.field("_ingested_at", pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)

PMMS_REQUIRED = {"date", "pmms30", "pmms15"}
COUNTY_REQUIRED = {"STATE", "STATEFP", "COUNTYFP", "COUNTYNAME"}


def _float_or_none(value: str | None) -> float | None:
    value = (value or "").strip()
    return float(value) if value else None


def parse_pmms(raw_path: Path) -> list[dict]:
    with raw_path.open(newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        if not PMMS_REQUIRED.issubset(reader.fieldnames or []):
            raise SchemaContractError(f"PMMS header {reader.fieldnames} lacks {sorted(PMMS_REQUIRED)}")
        return [
            {
                "week_date": datetime.strptime(row["date"].strip(), "%m/%d/%Y").date(),
                "rate_30y_fixed": _float_or_none(row["pmms30"]),
                "rate_15y_fixed": _float_or_none(row["pmms15"]),
            }
            for row in reader
            if row["date"].strip()
        ]


def parse_counties(raw_path: Path) -> list[dict]:
    with raw_path.open(newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh, delimiter="|")
        if not COUNTY_REQUIRED.issubset(reader.fieldnames or []):
            raise SchemaContractError(f"county header {reader.fieldnames} lacks {sorted(COUNTY_REQUIRED)}")
        return [
            {
                "state_code": row["STATE"],
                "state_fips": row["STATEFP"],
                "county_fips": row["STATEFP"] + row["COUNTYFP"],
                "county_name": row["COUNTYNAME"],
            }
            for row in reader
        ]


def _count_pmms_rows(path: Path) -> int:
    return len(parse_pmms(path))


def ingest_reference(settings: Settings, source: Source, refresh: bool = False) -> dict[str, int]:
    """Land and convert both reference files. Returns bronze row counts."""
    out: dict[str, int] = {}
    specs = (
        (
            "pmms",
            "PMMS_history.csv",
            "pmms_weekly.parquet",
            source.fetch_pmms,
            parse_pmms,
            PMMS_SCHEMA,
            _count_pmms_rows,
        ),
        (
            "census_counties",
            "national_county2020.txt",
            "counties.parquet",
            source.fetch_counties,
            parse_counties,
            COUNTY_SCHEMA,
            count_data_rows,
        ),
    )
    for dataset, raw_name, parquet_name, fetch, parse, schema, counter in specs:
        raw_dir = settings.raw_dir / dataset
        _land(settings, dataset, raw_name, raw_dir, fetch, counter, None, None, refresh)
        manifest = Manifest.read(raw_dir)
        assert manifest is not None
        dest = settings.bronze_dir / dataset / parquet_name
        with telemetry.step(settings.telemetry_dir, "ingest.convert", dataset) as rec:
            rec.rows_in = manifest.row_count
            rec.bytes_scanned = manifest.bytes
            if _already_converted(dest, manifest.sha256):
                rec.detail = {"cache": "hit"}
                rec.bytes_written = 0
                out[dataset] = manifest.row_count
                rec.rows_out = manifest.row_count
                continue
            rows = parse(raw_dir / raw_name)
            now = datetime.now(UTC)
            for row in rows:
                row["_source_sha256"] = manifest.sha256
                row["_ingested_at"] = now
            table = pa.Table.from_pylist(rows, schema=schema)
            written = _write_atomic([table], schema, dest, manifest.sha256)
            if written != manifest.row_count:
                dest.unlink(missing_ok=True)
                raise SchemaContractError(
                    f"{dataset}: converter produced {written} rows, manifest says {manifest.row_count}"
                )
            rec.rows_out = written
            rec.bytes_written = dest.stat().st_size
            out[dataset] = written
    return out
