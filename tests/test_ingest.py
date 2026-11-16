import json
import shutil

import pyarrow.parquet as pq
import pytest

from helpers import FIXTURE_SOURCE, make_settings
from mmp.ingest.lar import convert_partition, land_partition, partition_dir
from mmp.ingest.manifest import Manifest, build_manifest_index, count_data_rows
from mmp.ingest.reference import ingest_reference, parse_pmms
from mmp.ingest.schema import LAR_BRONZE_SCHEMA, LAR_CSV_HEADER, SchemaContractError
from mmp.ingest.sources import FixtureSource


class CountingSource(FixtureSource):
    def __init__(self, root):
        super().__init__(root)
        self.fetches = 0

    def fetch_lar(self, year, state, dest):
        self.fetches += 1
        return super().fetch_lar(year, state, dest)


@pytest.fixture
def fresh(tmp_path):
    return make_settings(tmp_path / "data")


def test_header_matches_the_declared_contract():
    header = (FIXTURE_SOURCE / "hmda_lar" / "2024_DE.csv").read_text().splitlines()[0].split(",")
    assert header == LAR_CSV_HEADER and len(header) == 99


def test_landing_is_idempotent_and_writes_a_manifest(fresh):
    source = CountingSource(FIXTURE_SOURCE)
    first = land_partition(fresh, source, 2024, "DE")["hmda_lar"]
    second = land_partition(fresh, source, 2024, "DE")["hmda_lar"]
    assert (first.status, second.status) == ("downloaded", "cached")
    assert source.fetches == 1, "an intact cached file must not be fetched again"
    manifest = first.manifest
    raw = partition_dir(fresh.raw_dir, "hmda_lar", 2024, "DE") / "lar.csv"
    assert manifest.bytes == raw.stat().st_size
    assert manifest.row_count == len(raw.read_text().splitlines()) - 1
    assert manifest.source_url == "fixture://hmda_lar/2024_DE.csv" and manifest.fetched_at
    assert second.manifest == manifest


def test_landing_resumes_when_the_cached_file_is_damaged(fresh):
    source = CountingSource(FIXTURE_SOURCE)
    land_partition(fresh, source, 2024, "DE")
    raw = partition_dir(fresh.raw_dir, "hmda_lar", 2024, "DE") / "lar.csv"
    raw.write_bytes(raw.read_bytes()[:5000])  # simulate a truncated file
    again = land_partition(fresh, source, 2024, "DE")["hmda_lar"]
    assert again.status == "downloaded" and source.fetches == 2
    assert Manifest.read(raw.parent).sha256 == again.manifest.sha256


def test_conversion_uses_the_explicit_schema_and_keeps_sentinels(fresh):
    land_partition(fresh, FixtureSource(FIXTURE_SOURCE), 2024, "DE")
    rows = convert_partition(fresh, 2024, "DE")
    path = partition_dir(fresh.bronze_dir, "hmda_lar", 2024, "DE") / "lar.parquet"
    table = pq.read_table(path)
    assert table.schema.equals(LAR_BRONZE_SCHEMA, check_metadata=False)
    assert table.num_rows == rows["hmda_lar"]
    rates = set(table.column("interest_rate").to_pylist())
    assert "NA" in rates, "NA must survive as data, not become null"
    assert str(table.schema.field("interest_rate").type) == "string"
    assert str(table.schema.field("action_taken").type) == "int32"
    assert table.column("source_row_number").to_pylist() == list(range(1, table.num_rows + 1))
    manifest = Manifest.read(partition_dir(fresh.raw_dir, "hmda_lar", 2024, "DE"))
    assert set(table.column("_source_sha256").to_pylist()) == {manifest.sha256}


def test_conversion_is_idempotent(fresh):
    land_partition(fresh, FixtureSource(FIXTURE_SOURCE), 2024, "DE")
    convert_partition(fresh, 2024, "DE")
    path = partition_dir(fresh.bronze_dir, "hmda_lar", 2024, "DE") / "lar.parquet"
    before = (path.stat().st_mtime_ns, path.read_bytes())
    convert_partition(fresh, 2024, "DE")
    assert (path.stat().st_mtime_ns, path.read_bytes()) == before


def _broken_source(tmp_path, mutate):
    root = tmp_path / "source"
    shutil.copytree(FIXTURE_SOURCE, root)
    target = root / "hmda_lar" / "2024_DE.csv"
    lines = target.read_text().splitlines()
    target.write_text("\n".join(mutate(lines)) + "\n")
    return root


def test_header_drift_fails_closed(fresh, tmp_path):
    root = _broken_source(tmp_path, lambda lines: [lines[0].replace("loan_amount", "loan_amt"), *lines[1:]])
    land_partition(fresh, FixtureSource(root), 2024, "DE")
    with pytest.raises(SchemaContractError, match="loan_amount"):
        convert_partition(fresh, 2024, "DE")
    assert not (partition_dir(fresh.bronze_dir, "hmda_lar", 2024, "DE") / "lar.parquet").exists()


def test_unparseable_code_column_fails_closed(fresh, tmp_path):
    def mutate(lines):
        cells = lines[1].split(",")
        cells[LAR_CSV_HEADER.index("action_taken")] = "originated"
        return [lines[0], ",".join(cells), *lines[2:]]

    land_partition(fresh, FixtureSource(_broken_source(tmp_path, mutate)), 2024, "DE")
    with pytest.raises(SchemaContractError):
        convert_partition(fresh, 2024, "DE")
    assert not list(partition_dir(fresh.bronze_dir, "hmda_lar", 2024, "DE").glob("*"))


def test_count_data_rows_with_and_without_trailing_newline(tmp_path):
    a, b, c = tmp_path / "a", tmp_path / "b", tmp_path / "c"
    a.write_text("h\n1\n2\n")
    b.write_text("h\n1\n2")
    c.write_text("h\n")
    assert (count_data_rows(a), count_data_rows(b), count_data_rows(c)) == (2, 2, 0)


def test_reference_files_and_manifest_index(fresh):
    source = FixtureSource(FIXTURE_SOURCE)
    counts = ingest_reference(fresh, source)
    assert counts["pmms"] > 2800 and counts["census_counties"] > 3000
    land_partition(fresh, source, 2024, "DE")
    assert build_manifest_index(fresh.raw_dir, fresh.bronze_dir) == 4
    index = pq.read_table(fresh.bronze_dir / "ingest_manifest" / "manifest.parquet").to_pylist()
    assert {r["dataset"] for r in index} == {"hmda_lar", "hmda_filers", "pmms", "census_counties"}
    assert all(len(r["sha256"]) == 64 for r in index)


def test_pmms_blank_cells_become_null(tmp_path):
    path = tmp_path / "pmms.csv"
    path.write_text("date,pmms30,pmms30p,pmms15\n4/2/1971,7.33, ,\n1/4/2024,6.62,,5.89\n")
    rows = parse_pmms(path)
    assert rows[0]["rate_15y_fixed"] is None and rows[1]["rate_15y_fixed"] == 5.89
    assert str(rows[1]["week_date"]) == "2024-01-04"


def test_filers_payload_without_institutions_is_a_contract_break(fresh, tmp_path):
    root = tmp_path / "source"
    shutil.copytree(FIXTURE_SOURCE, root)
    (root / "hmda_filers" / "2024_DE.json").write_text(json.dumps({"institutions": [{"name": "no lei"}]}))
    land_partition(fresh, FixtureSource(root), 2024, "DE")
    with pytest.raises(SchemaContractError):
        convert_partition(fresh, 2024, "DE")
