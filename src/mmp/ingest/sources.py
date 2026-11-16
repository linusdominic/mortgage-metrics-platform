"""Where raw bytes come from.

`HttpSource` talks to the real endpoints. `FixtureSource` serves the committed
sample under tests/fixtures/source through the same interface, so CI exercises
the identical landing, manifest and conversion code without the network.
"""

from __future__ import annotations

import shutil
import time
from pathlib import Path
from typing import Protocol

from mmp import settings as cfg
from mmp.http import DownloadResult, download, sha256_file


class Source(Protocol):
    def fetch_lar(self, year: int, state: str, dest: Path) -> DownloadResult: ...

    def fetch_filers(self, year: int, state: str, dest: Path) -> DownloadResult: ...

    def fetch_pmms(self, dest: Path) -> DownloadResult: ...

    def fetch_counties(self, dest: Path) -> DownloadResult: ...


class HttpSource:
    def __init__(self, user_agent: str | None, max_retries: int, pause_seconds: float):
        self.user_agent = user_agent
        self.max_retries = max_retries
        self.pause_seconds = pause_seconds

    def _get(self, url: str, dest: Path, params: dict[str, str] | None = None) -> DownloadResult:
        result = download(url, dest, params=params, user_agent=self.user_agent, max_retries=self.max_retries)
        time.sleep(self.pause_seconds)
        return result

    def fetch_lar(self, year: int, state: str, dest: Path) -> DownloadResult:
        return self._get(cfg.HMDA_LAR_CSV_URL, dest, {"states": state, "years": str(year)})

    def fetch_filers(self, year: int, state: str, dest: Path) -> DownloadResult:
        return self._get(cfg.HMDA_FILERS_URL, dest, {"states": state, "years": str(year)})

    def fetch_pmms(self, dest: Path) -> DownloadResult:
        return self._get(cfg.PMMS_HISTORY_URL, dest)

    def fetch_counties(self, dest: Path) -> DownloadResult:
        return self._get(cfg.CENSUS_COUNTY_URL, dest)


class FixtureSource:
    """Serves files from a local directory laid out like tests/fixtures/source."""

    def __init__(self, root: Path):
        self.root = root

    def _copy(self, relative: str, dest: Path) -> DownloadResult:
        src = self.root / relative
        if not src.exists():
            raise FileNotFoundError(f"fixture source has no {relative} (looked in {self.root})")
        t0 = time.monotonic()
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_name(dest.name + ".part")
        shutil.copyfile(src, tmp)
        tmp.replace(dest)
        return DownloadResult(
            url=f"fixture://{relative}",
            path=dest,
            bytes=dest.stat().st_size,
            sha256=sha256_file(dest),
            attempts=1,
            duration_s=time.monotonic() - t0,
            http_status=200,
        )

    def fetch_lar(self, year: int, state: str, dest: Path) -> DownloadResult:
        return self._copy(f"hmda_lar/{year}_{state}.csv", dest)

    def fetch_filers(self, year: int, state: str, dest: Path) -> DownloadResult:
        return self._copy(f"hmda_filers/{year}_{state}.json", dest)

    def fetch_pmms(self, dest: Path) -> DownloadResult:
        return self._copy("pmms/PMMS_history.csv", dest)

    def fetch_counties(self, dest: Path) -> DownloadResult:
        return self._copy("census/national_county2020.txt", dest)


def make_source(settings: cfg.Settings) -> Source:
    if settings.source_mode == "fixture":
        return FixtureSource(settings.fixture_dir)
    if settings.source_mode == "http":
        return HttpSource(settings.user_agent, settings.max_retries, settings.request_pause_seconds)
    raise ValueError(f"unknown MMP_SOURCE_MODE {settings.source_mode!r}")
