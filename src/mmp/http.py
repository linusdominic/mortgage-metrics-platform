"""A small, polite HTTP downloader.

- streams to a temp file and renames atomically, so a killed download never
  leaves a half-written file under the final name;
- retries on 429/5xx and transport errors with exponential backoff and jitter,
  honouring Retry-After;
- hashes while streaming, so the sha256 in the manifest is of the exact bytes
  that landed on disk;
- sleeps between requests (configurable) because the HMDA data browser is a
  shared public service, not a bulk-export endpoint.
"""

from __future__ import annotations

import hashlib
import logging
import random
import time
from dataclasses import dataclass
from pathlib import Path

import httpx

log = logging.getLogger(__name__)

RETRYABLE_STATUS = {408, 425, 429, 500, 502, 503, 504}


class DownloadError(RuntimeError):
    pass


@dataclass(frozen=True)
class DownloadResult:
    url: str
    path: Path
    bytes: int
    sha256: str
    attempts: int
    duration_s: float
    http_status: int


def backoff_seconds(attempt: int, base: float = 2.0, cap: float = 60.0, jitter: float = 0.25) -> float:
    """Exponential backoff with +/- jitter. attempt is 1-based."""
    raw = min(cap, base * (2 ** (attempt - 1)))
    return raw * (1 + random.uniform(-jitter, jitter))


def _retry_after(response: httpx.Response) -> float | None:
    value = response.headers.get("Retry-After")
    if value and value.isdigit():
        return float(value)
    return None


def download(
    url: str,
    dest: Path,
    *,
    params: dict[str, str] | None = None,
    user_agent: str | None = None,
    max_retries: int = 4,
    timeout_s: float = 300.0,
    client: httpx.Client | None = None,
    sleep=time.sleep,
) -> DownloadResult:
    """Download url to dest atomically. Raises DownloadError after max_retries."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".part")
    headers = {"Accept-Encoding": "gzip"}
    if user_agent:
        headers["User-Agent"] = user_agent
    owns_client = client is None
    client = client or httpx.Client(timeout=httpx.Timeout(timeout_s, connect=30.0), follow_redirects=True)
    started = time.monotonic()
    last_error: str = ""
    try:
        for attempt in range(1, max_retries + 1):
            try:
                with client.stream("GET", url, params=params, headers=headers) as response:
                    if response.status_code in RETRYABLE_STATUS:
                        last_error = f"HTTP {response.status_code}"
                        wait = _retry_after(response) or backoff_seconds(attempt)
                        log.warning("retryable %s for %s; sleeping %.1fs", last_error, url, wait)
                        sleep(wait)
                        continue
                    if response.status_code != 200:
                        # 4xx other than 408/425/429 will not fix itself: fail now.
                        raise DownloadError(f"HTTP {response.status_code} for {response.url}")
                    digest = hashlib.sha256()
                    size = 0
                    with tmp.open("wb") as fh:
                        for chunk in response.iter_bytes(chunk_size=1 << 20):
                            fh.write(chunk)
                            digest.update(chunk)
                            size += len(chunk)
                    tmp.replace(dest)
                    return DownloadResult(
                        url=str(response.url),
                        path=dest,
                        bytes=size,
                        sha256=digest.hexdigest(),
                        attempts=attempt,
                        duration_s=time.monotonic() - started,
                        http_status=response.status_code,
                    )
            except httpx.TransportError as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                wait = backoff_seconds(attempt)
                log.warning("transport error for %s (%s); sleeping %.1fs", url, last_error, wait)
                sleep(wait)
        raise DownloadError(f"gave up on {url} after {max_retries} attempts; last error: {last_error}")
    finally:
        tmp.unlink(missing_ok=True)
        if owns_client:
            client.close()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()
