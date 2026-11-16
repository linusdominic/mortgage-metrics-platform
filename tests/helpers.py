"""Helpers shared by the test modules."""

from __future__ import annotations

import os
from pathlib import Path

from mmp.ingest.lar import convert_partition, land_partition
from mmp.ingest.manifest import build_manifest_index
from mmp.ingest.reference import ingest_reference
from mmp.ingest.sources import FixtureSource
from mmp.settings import REPO_ROOT, Settings, load_settings

FIXTURE_SOURCE = REPO_ROOT / "tests" / "fixtures" / "source"


def make_settings(data_dir: Path, fixture_dir: Path = FIXTURE_SOURCE) -> Settings:
    """Settings for the fixture scope rooted at data_dir. Sets the env dbt will read."""
    os.environ["MMP_DATA_DIR"] = str(data_dir)
    os.environ["MMP_DBT_TARGET_PATH"] = str(data_dir / "dbt_target")
    os.environ["MMP_FIXTURE_DIR"] = str(fixture_dir)
    os.environ["MMP_SOURCE_MODE"] = "fixture"
    os.environ["MMP_RUN_ID"] = f"test-{data_dir.name}"
    os.environ["MMP_RUN_KIND"] = "adhoc"
    os.environ["MMP_DBT_QUIET"] = "1"
    return load_settings("fixture")


def ingest_all(settings: Settings, years=None, refresh: bool = False) -> None:
    source = FixtureSource(settings.fixture_dir)
    for year, state in settings.scope.partitions(years):
        land_partition(settings, source, year, state, refresh=refresh)
        convert_partition(settings, year, state)
    ingest_reference(settings, source)
    build_manifest_index(settings.raw_dir, settings.bronze_dir)
