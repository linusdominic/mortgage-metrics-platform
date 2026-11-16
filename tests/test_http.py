import hashlib

import httpx
import pytest

from mmp.http import DownloadError, backoff_seconds, download


def client_with(responses):
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        status, body, headers = responses[min(calls["n"], len(responses)) - 1]
        return httpx.Response(status, content=body, headers=headers)

    return httpx.Client(transport=httpx.MockTransport(handler)), calls


def test_backoff_grows_and_is_capped():
    first = [backoff_seconds(1) for _ in range(50)]
    assert all(1.5 <= v <= 2.5 for v in first)
    assert all(backoff_seconds(10) <= 60 * 1.25 for _ in range(50))
    assert min(backoff_seconds(4) for _ in range(20)) > max(first)


def test_download_retries_then_succeeds_and_hashes_what_landed(tmp_path):
    body = b"a,b\n1,2\n"
    client, calls = client_with([(503, b"", {}), (429, b"", {"Retry-After": "7"}), (200, body, {})])
    sleeps = []
    result = download("https://example.test/x", tmp_path / "x.csv", client=client, sleep=sleeps.append)
    assert calls["n"] == 3 and result.attempts == 3
    assert sleeps[1] == 7.0, "Retry-After must be honoured"
    assert (tmp_path / "x.csv").read_bytes() == body
    assert result.sha256 == hashlib.sha256(body).hexdigest() and result.bytes == len(body)
    assert not list(tmp_path.glob("*.part"))


def test_download_does_not_retry_a_client_error(tmp_path):
    client, calls = client_with([(403, b"denied", {})])
    with pytest.raises(DownloadError, match="HTTP 403"):
        download("https://example.test/x", tmp_path / "x.csv", client=client, sleep=lambda s: None)
    assert calls["n"] == 1
    assert not (tmp_path / "x.csv").exists() and not list(tmp_path.glob("*.part"))


def test_download_gives_up_after_max_retries(tmp_path):
    client, calls = client_with([(500, b"", {})])
    with pytest.raises(DownloadError, match="gave up"):
        download(
            "https://example.test/x", tmp_path / "x.csv", client=client, max_retries=3, sleep=lambda s: None
        )
    assert calls["n"] == 3


def test_user_agent_is_only_overridden_when_configured(tmp_path):
    seen = []

    def handler(request):
        seen.append(request.headers["user-agent"])
        return httpx.Response(200, content=b"x")

    client = httpx.Client(transport=httpx.MockTransport(handler))
    download("https://example.test/x", tmp_path / "a", client=client)
    download("https://example.test/x", tmp_path / "b", client=client, user_agent="my-team-etl/1.0")
    assert seen[0].startswith("python-httpx/") and seen[1] == "my-team-etl/1.0"
