"""Per-partition ingestion manifest.

One `_manifest.json` sits next to every raw file. It records what was fetched
and is the left-hand side of every reconciliation downstream. dbt reads the
manifests directly as a source (`ops.ingest_manifest`).
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

MANIFEST_NAME = "_manifest.json"
HISTORY_NAME = "_manifest_history.jsonl"


@dataclass(frozen=True)
class Manifest:
    dataset: str
    activity_year: int | None
    state_code: str | None
    file_name: str
    source_url: str
    bytes: int
    sha256: str
    row_count: int
    fetched_at: str
    http_status: int
    attempts: int
    download_seconds: float

    def write(self, directory: Path) -> None:
        directory.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(asdict(self), sort_keys=True)
        tmp = directory / (MANIFEST_NAME + ".tmp")
        tmp.write_text(payload)
        tmp.replace(directory / MANIFEST_NAME)
        with (directory / HISTORY_NAME).open("a") as fh:
            fh.write(payload + "\n")

    @staticmethod
    def read(directory: Path) -> Manifest | None:
        path = directory / MANIFEST_NAME
        if not path.exists():
            return None
        return Manifest(**json.loads(path.read_text()))


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def count_data_rows(path: Path) -> int:
    """Data rows in a delimited text file: newline-terminated lines minus the header.

    Deliberately not a CSV parse. This count is compared against the row count
    the Parquet converter produces, so it has to come from different code.
    """
    newlines = 0
    last = b""
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            newlines += chunk.count(b"\n")
            last = chunk[-1:]
    lines = newlines + (0 if last in (b"\n", b"") else 1)
    return max(lines - 1, 0)


def build_manifest_index(raw_dir: Path, bronze_dir: Path) -> int:
    """Fold every per-partition manifest into one typed Parquet file.

    Partition manifests are separate files so parallel ingestion tasks never
    write the same file. dbt wants one relation, so this runs once after
    ingestion and before dbt. Rewritten in full each time; it is a few KB.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    schema = pa.schema(
        [
            pa.field("dataset", pa.string(), nullable=False),
            pa.field("activity_year", pa.int32()),
            pa.field("state_code", pa.string()),
            pa.field("file_name", pa.string()),
            pa.field("source_url", pa.string()),
            pa.field("bytes", pa.int64()),
            pa.field("sha256", pa.string(), nullable=False),
            pa.field("row_count", pa.int64()),
            pa.field("fetched_at", pa.timestamp("us", tz="UTC")),
            pa.field("http_status", pa.int32()),
            pa.field("attempts", pa.int32()),
            pa.field("download_seconds", pa.float64()),
        ]
    )
    rows = []
    for path in sorted(raw_dir.rglob(MANIFEST_NAME)):
        row = json.loads(path.read_text())
        row["fetched_at"] = datetime.fromisoformat(row["fetched_at"])
        rows.append(row)
    dest = bronze_dir / "ingest_manifest" / "manifest.parquet"
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".part")
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), tmp)
    tmp.replace(dest)
    return len(rows)
