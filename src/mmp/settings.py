"""Runtime settings: paths, scope, and source endpoints.

Everything that varies between a laptop run, CI, and the Airflow container is an
environment variable read here. Nothing else in the codebase reads os.environ.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml

REPO_ROOT = Path(os.environ.get("MMP_REPO_ROOT", Path(__file__).resolve().parents[2]))

# Source endpoints. Verified reachable on 2026-11-17; see docs/data-dictionary.md.
HMDA_LAR_CSV_URL = "https://ffiec.cfpb.gov/v2/data-browser-api/view/csv"
HMDA_FILERS_URL = "https://ffiec.cfpb.gov/v2/data-browser-api/view/filers"
PMMS_HISTORY_URL = "https://www.freddiemac.com/pmms/docs/PMMS_history.csv"
CENSUS_COUNTY_URL = "https://www2.census.gov/geo/docs/reference/codes2020/national_county2020.txt"


@dataclass(frozen=True)
class Scope:
    """Which (year, state) partitions a run covers."""

    name: str
    years: tuple[int, ...]
    states: tuple[str, ...]
    backfill_years: tuple[int, ...] = ()

    def partitions(self, years: tuple[int, ...] | None = None) -> list[tuple[int, str]]:
        return [(y, s) for y in (years or self.years) for s in self.states]


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    dbt_project_dir: Path
    dbt_target: str
    scope: Scope
    source_mode: str  # "http" or "fixture"
    fixture_dir: Path
    user_agent: str | None
    request_pause_seconds: float
    max_retries: int
    volume_warn_pct: float
    volume_fail_pct: float
    freshness_max_age_days: int
    extra: dict = field(default_factory=dict)

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def bronze_dir(self) -> Path:
        return self.data_dir / "bronze"

    @property
    def telemetry_dir(self) -> Path:
        return self.data_dir / "telemetry"

    @property
    def warehouse_path(self) -> Path:
        return self.data_dir / "warehouse" / "mortgage.duckdb"

    @property
    def audit_log_path(self) -> Path:
        return self.data_dir / "logs" / "mcp_audit.jsonl"

    @property
    def dbt_target_dir(self) -> Path:
        """Where dbt writes artifacts. Overridable so tests and CI do not share a target dir."""
        return Path(os.environ.get("MMP_DBT_TARGET_PATH", self.dbt_project_dir / "target"))

    @property
    def state_dir(self) -> Path:
        """Saved dbt artifacts from the last good build (used for slim CI deferral)."""
        return self.data_dir / "dbt_state"


def load_scopes(path: Path | None = None) -> dict[str, Scope]:
    cfg = yaml.safe_load((path or REPO_ROOT / "config" / "pipeline.yml").read_text())
    return {
        name: Scope(
            name=name,
            years=tuple(body["years"]),
            states=tuple(body["states"]),
            backfill_years=tuple(body.get("backfill_years", ())),
        )
        for name, body in cfg["scopes"].items()
    }


def load_settings(scope: str | None = None) -> Settings:
    cfg = yaml.safe_load((REPO_ROOT / "config" / "pipeline.yml").read_text())
    scope_name = scope or os.environ.get("MMP_SCOPE", cfg.get("default_scope", "demo"))
    scopes = load_scopes()
    if scope_name not in scopes:
        raise ValueError(f"Unknown scope {scope_name!r}; known scopes: {sorted(scopes)}")
    data_dir = Path(os.environ.get("MMP_DATA_DIR", REPO_ROOT / "data")).resolve()
    source_mode = os.environ.get("MMP_SOURCE_MODE", "fixture" if scope_name == "fixture" else "http")
    http_cfg = cfg.get("http", {})
    dq_cfg = cfg.get("quality", {})
    return Settings(
        data_dir=data_dir,
        dbt_project_dir=REPO_ROOT / "dbt",
        dbt_target=os.environ.get("DBT_TARGET", "dev"),
        scope=scopes[scope_name],
        source_mode=source_mode,
        fixture_dir=Path(os.environ.get("MMP_FIXTURE_DIR", REPO_ROOT / "tests" / "fixtures" / "source")),
        user_agent=os.environ.get("MMP_USER_AGENT") or http_cfg.get("user_agent"),
        request_pause_seconds=float(http_cfg.get("pause_seconds", 1.0)),
        max_retries=int(http_cfg.get("max_retries", 4)),
        volume_warn_pct=float(dq_cfg.get("volume_warn_pct", 40)),
        volume_fail_pct=float(dq_cfg.get("volume_fail_pct", 80)),
        freshness_max_age_days=int(dq_cfg.get("freshness_max_age_days", 30)),
    )


def dbt_env(settings: Settings) -> dict[str, str]:
    """Environment handed to every dbt / MetricFlow invocation."""
    return {
        **os.environ,
        "MMP_DATA_DIR": str(settings.data_dir),
        "DBT_PROFILES_DIR": str(settings.dbt_project_dir),
        "DBT_PROJECT_DIR": str(settings.dbt_project_dir),
        "DBT_TARGET_PATH": str(settings.dbt_target_dir),
        "DBT_LOG_PATH": str(settings.data_dir / "logs" / "dbt"),
        "DBT_TARGET": settings.dbt_target,
        "DO_NOT_TRACK": "1",
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
    }
