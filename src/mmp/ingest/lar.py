"""HMDA LAR and filer ingestion, one (activity_year, state) partition at a time.

land_partition      source -> data/raw/...        (as-is bytes + manifest)
convert_partition   raw CSV -> data/bronze/...    (typed Parquet, explicit schema)

Both are idempotent and resumable:

* landing skips a partition whose raw file is present and still hashes to the
  manifest's sha256; a partially written download never has the final name;
* conversion skips a partition whose Parquet footer already carries the raw
  file's sha256, and otherwise rewrites the whole partition file atomically.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from mmp import telemetry
from mmp.http import sha256_file
from mmp.ingest.manifest import Manifest, count_data_rows, utc_now_iso
from mmp.ingest.schema import (
    LAR_BRONZE_SCHEMA,
    LAR_COLUMNS,
    SchemaContractError,
    check_header,
)
from mmp.ingest.sources import Source
from mmp.settings import Settings

log = logging.getLogger(__name__)

SHA_KEY = b"mmp.source_sha256"


def partition_dir(root: Path, dataset: str, year: int, state: str) -> Path:
    return root / dataset / f"year={year}" / f"state={state}"


@dataclass(frozen=True)
class LandResult:
    manifest: Manifest
    status: str  # downloaded | cached


def _land(
    settings: Settings,
    dataset: str,
    file_name: str,
    directory: Path,
    fetch,
    row_counter,
    year: int | None,
    state: str | None,
    refresh: bool,
) -> LandResult:
    raw_path = directory / file_name
    existing = Manifest.read(directory)
    with telemetry.step(
        settings.telemetry_dir, "ingest.download", dataset, activity_year=year, state_code=state
    ) as rec:
        if not refresh and existing and raw_path.exists() and sha256_file(raw_path) == existing.sha256:
            rec.detail = {"cache": "hit", "sha256": existing.sha256}
            rec.rows_out = existing.row_count
            rec.bytes_written = 0
            return LandResult(existing, "cached")
        result = fetch(raw_path)
        manifest = Manifest(
            dataset=dataset,
            activity_year=year,
            state_code=state,
            file_name=file_name,
            source_url=result.url,
            bytes=result.bytes,
            sha256=result.sha256,
            row_count=row_counter(raw_path),
            fetched_at=utc_now_iso(),
            http_status=result.http_status,
            attempts=result.attempts,
            download_seconds=round(result.duration_s, 3),
        )
        manifest.write(directory)
        rec.rows_out = manifest.row_count
        rec.bytes_written = manifest.bytes
        rec.detail = {"cache": "miss", "sha256": manifest.sha256, "attempts": result.attempts}
        return LandResult(manifest, "downloaded")


def _count_filers(path: Path) -> int:
    return len(json.loads(path.read_text()).get("institutions", []))


def land_partition(
    settings: Settings, source: Source, year: int, state: str, refresh: bool = False
) -> dict[str, LandResult]:
    """Land the LAR file and the filer list for one partition."""
    lar = _land(
        settings,
        "hmda_lar",
        "lar.csv",
        partition_dir(settings.raw_dir, "hmda_lar", year, state),
        lambda dest: source.fetch_lar(year, state, dest),
        count_data_rows,
        year,
        state,
        refresh,
    )
    filers = _land(
        settings,
        "hmda_filers",
        "filers.json",
        partition_dir(settings.raw_dir, "hmda_filers", year, state),
        lambda dest: source.fetch_filers(year, state, dest),
        _count_filers,
        year,
        state,
        refresh,
    )
    return {"hmda_lar": lar, "hmda_filers": filers}


def _already_converted(parquet_path: Path, sha256: str) -> bool:
    if not parquet_path.exists():
        return False
    meta = pq.read_metadata(parquet_path).metadata or {}
    return meta.get(SHA_KEY, b"").decode() == sha256


def _write_atomic(table_iter, schema: pa.Schema, dest: Path, sha256: str) -> int:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".part")
    rows = 0
    schema = schema.with_metadata({SHA_KEY: sha256.encode()})
    try:
        with pq.ParquetWriter(tmp, schema, compression="zstd") as writer:
            for table in table_iter:
                writer.write_table(table)
                rows += table.num_rows
        tmp.replace(dest)
    finally:
        tmp.unlink(missing_ok=True)
    return rows


def _lar_batches(raw_path: Path, manifest: Manifest, ingested_at: datetime):
    column_types = dict(LAR_COLUMNS)
    with raw_path.open("rb") as fh:
        header = fh.readline().decode("utf-8").rstrip("\r\n").split(",")
    check_header(header)
    offset = 0
    try:
        reader = pacsv.open_csv(
            raw_path,
            read_options=pacsv.ReadOptions(block_size=8 << 20),
            convert_options=pacsv.ConvertOptions(
                column_types=column_types,
                # Only the empty string is null. "NA" and "Exempt" are data.
                null_values=[""],
                strings_can_be_null=True,
            ),
        )
        for batch in reader:
            n = batch.num_rows
            arrays = [batch.column(name) for name, _ in LAR_COLUMNS]
            arrays += [
                pa.array(range(offset + 1, offset + n + 1), pa.int64()),
                pa.array([manifest.sha256] * n, pa.string()),
                pa.array([manifest.source_url] * n, pa.string()),
                pa.array([ingested_at] * n, pa.timestamp("us", tz="UTC")),
            ]
            offset += n
            yield pa.Table.from_arrays(arrays, schema=LAR_BRONZE_SCHEMA)
    except pa.ArrowInvalid as exc:
        # A value that does not fit the declared type: contract break, fail the partition.
        raise SchemaContractError(f"{raw_path}: {exc}") from exc


FILERS_SCHEMA = pa.schema(
    [
        pa.field("lei", pa.string(), nullable=False),
        pa.field("institution_name", pa.string()),
        pa.field("lar_count", pa.int64()),
        pa.field("activity_year", pa.int32(), nullable=False),
        pa.field("_source_sha256", pa.string(), nullable=False),
        pa.field("_ingested_at", pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)


def _filers_table(raw_path: Path, manifest: Manifest, ingested_at: datetime) -> pa.Table:
    payload = json.loads(raw_path.read_text())
    if "institutions" not in payload:
        raise SchemaContractError(f"{raw_path}: filers response has no 'institutions' key")
    rows = payload["institutions"]
    try:
        return pa.Table.from_pydict(
            {
                "lei": [r["lei"] for r in rows],
                "institution_name": [r.get("name") for r in rows],
                "lar_count": [r.get("count") for r in rows],
                "activity_year": [int(r["period"]) for r in rows],
                "_source_sha256": [manifest.sha256] * len(rows),
                "_ingested_at": [ingested_at] * len(rows),
            },
            schema=FILERS_SCHEMA,
        )
    except (KeyError, ValueError, pa.ArrowInvalid) as exc:
        raise SchemaContractError(f"{raw_path}: {exc!r}") from exc


def convert_partition(settings: Settings, year: int, state: str) -> dict[str, int]:
    """Convert one landed partition to bronze Parquet. Returns rows written per dataset."""
    out: dict[str, int] = {}
    ingested_at = datetime.now(UTC)
    for dataset, raw_name, parquet_name in (
        ("hmda_lar", "lar.csv", "lar.parquet"),
        ("hmda_filers", "filers.json", "filers.parquet"),
    ):
        raw_dir = partition_dir(settings.raw_dir, dataset, year, state)
        manifest = Manifest.read(raw_dir)
        if manifest is None:
            raise FileNotFoundError(f"no manifest in {raw_dir}; run ingest for {year}/{state} first")
        raw_path = raw_dir / raw_name
        dest = partition_dir(settings.bronze_dir, dataset, year, state) / parquet_name
        with telemetry.step(
            settings.telemetry_dir, "ingest.convert", dataset, activity_year=year, state_code=state
        ) as rec:
            rec.rows_in = manifest.row_count
            rec.bytes_scanned = manifest.bytes
            if _already_converted(dest, manifest.sha256):
                rec.detail = {"cache": "hit"}
                rec.rows_out = pq.read_metadata(dest).num_rows
                rec.bytes_written = 0
                out[dataset] = rec.rows_out
                continue
            if dataset == "hmda_lar":
                rows = _write_atomic(
                    _lar_batches(raw_path, manifest, ingested_at), LAR_BRONZE_SCHEMA, dest, manifest.sha256
                )
            else:
                table = _filers_table(raw_path, manifest, ingested_at)
                rows = _write_atomic([table], FILERS_SCHEMA, dest, manifest.sha256)
            if rows != manifest.row_count:
                dest.unlink(missing_ok=True)
                raise SchemaContractError(
                    f"{dataset} {year}/{state}: converter produced {rows} rows, "
                    f"manifest says {manifest.row_count}"
                )
            rec.rows_out = rows
            rec.bytes_written = dest.stat().st_size
            rec.detail = {"cache": "miss"}
            out[dataset] = rows
    return out
