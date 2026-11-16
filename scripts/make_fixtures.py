"""Build the committed test fixture from real downloaded data.

    uv run python scripts/make_fixtures.py

Reads the raw files a demo run landed under data/raw and writes a small sample
to tests/fixtures/source, laid out the way FixtureSource expects:

  hmda_lar/<year>_<state>.csv      header + every k-th data row (about 700 rows)
  hmda_filers/<year>_<state>.json  the real filer list, cut to the LEIs in the sample
  pmms/PMMS_history.csv            unchanged
  census/national_county2020.txt   unchanged

Rows are real HMDA records, byte for byte; only the selection is ours. The
sample is systematic (every k-th row), not random, so it is reproducible from
the same raw file. PROVENANCE.json records the source URL and sha256 of every
raw file the sample was cut from.
"""

from __future__ import annotations

import json
import math
import shutil
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
RAW = REPO / "data" / "raw"
OUT = REPO / "tests" / "fixtures" / "source"
YEARS = (2022, 2023, 2024)
STATES = ("DE", "RI")
TARGET_ROWS = 700


def main() -> None:
    provenance = []
    for sub in ("hmda_lar", "hmda_filers", "pmms", "census"):
        (OUT / sub).mkdir(parents=True, exist_ok=True)
    for year in YEARS:
        for state in STATES:
            lar_dir = RAW / "hmda_lar" / f"year={year}" / f"state={state}"
            manifest = json.loads((lar_dir / "_manifest.json").read_text())
            lines = (lar_dir / "lar.csv").read_text().splitlines(keepends=True)
            header, rows = lines[0], lines[1:]
            step = math.ceil(len(rows) / TARGET_ROWS)
            sample = rows[::step]
            (OUT / "hmda_lar" / f"{year}_{state}.csv").write_text(header + "".join(sample))
            leis = {line.split(",", 2)[1] for line in sample}

            filers_dir = RAW / "hmda_filers" / f"year={year}" / f"state={state}"
            filers_manifest = json.loads((filers_dir / "_manifest.json").read_text())
            filers = json.loads((filers_dir / "filers.json").read_text())
            kept = [i for i in filers["institutions"] if i["lei"] in leis]
            (OUT / "hmda_filers" / f"{year}_{state}.json").write_text(
                json.dumps({"institutions": kept}, separators=(",", ":"))
            )
            provenance.append(
                {
                    "activity_year": year,
                    "state_code": state,
                    "lar_source_url": manifest["source_url"],
                    "lar_source_sha256": manifest["sha256"],
                    "lar_source_rows": manifest["row_count"],
                    "lar_fetched_at": manifest["fetched_at"],
                    "sample_every_kth_row": step,
                    "sample_rows": len(sample),
                    "filers_source_url": filers_manifest["source_url"],
                    "filers_source_sha256": filers_manifest["sha256"],
                    "filers_kept": len(kept),
                    "filers_in_source": len(filers["institutions"]),
                }
            )
    reference = []
    for dataset, name, dest in (
        ("pmms", "PMMS_history.csv", "pmms/PMMS_history.csv"),
        ("census_counties", "national_county2020.txt", "census/national_county2020.txt"),
    ):
        manifest = json.loads((RAW / dataset / "_manifest.json").read_text())
        shutil.copyfile(RAW / dataset / name, OUT / dest)
        reference.append(
            {k: manifest[k] for k in ("dataset", "source_url", "sha256", "row_count", "fetched_at")}
        )
    (OUT.parent / "PROVENANCE.json").write_text(
        json.dumps({"lar_partitions": provenance, "reference_files": reference}, indent=2)
    )
    total = sum(p["sample_rows"] for p in provenance)
    print(f"wrote {total} LAR rows across {len(provenance)} partitions to {OUT}")


if __name__ == "__main__":
    main()
