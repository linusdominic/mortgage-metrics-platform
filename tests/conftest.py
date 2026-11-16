"""Shared fixtures.

`built` runs the real pipeline steps (ingest -> convert -> dbt build) once per
session against the committed fixture sample, in a temporary data directory.
Everything downstream of ingestion is tested against that warehouse.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path

import pytest

from helpers import ingest_all, make_settings
from mmp import dbt_runner
from mmp.settings import Settings


@pytest.fixture(autouse=True)
def _restore_env():
    saved = dict(os.environ)
    yield
    os.environ.clear()
    os.environ.update(saved)


@pytest.fixture(scope="session")
def built(tmp_path_factory) -> Path:
    """Data directory holding a warehouse built from the fixture sample (2023 and 2024)."""
    saved = dict(os.environ)
    data_dir = tmp_path_factory.mktemp("mmp_data")
    settings = make_settings(data_dir)
    ingest_all(settings)
    dbt_runner.build(settings)
    os.environ.clear()
    os.environ.update(saved)
    return data_dir


@pytest.fixture
def settings(built) -> Settings:
    return make_settings(built)


@pytest.fixture
def scratch(built, tmp_path) -> Settings:
    """A private copy of the built data directory, for tests that mutate the warehouse."""
    data_dir = tmp_path / "data"
    shutil.copytree(built, data_dir)
    return make_settings(data_dir)
