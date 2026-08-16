from datetime import UTC, datetime

import httpx
import pytest
import respx

from ffh.ingest.http import (
    Fetched,
    NotFound,
    NotModified,
    RetryableStatus,
    get_bytes,
    make_client,
)

URL = "https://example.invalid/asset.parquet"


def test_make_client_follows_redirects_and_sets_user_agent():
    with make_client() as client:
        assert client.follow_redirects is True
        assert client.headers["user-agent"].startswith("ffh/")


@respx.mock
def test_get_bytes_returns_fetched_with_etag_and_mtime():
    respx.get(URL).mock(
        return_value=httpx.Response(
            200,
            content=b"PAR1",
            headers={
                "ETag": '"v1"',
                "Last-Modified": "Sun, 16 Aug 2026 08:23:16 GMT",
            },
        )
    )
    with make_client() as client:
        result = get_bytes(client, URL)
    assert isinstance(result, Fetched)
    assert result.content == b"PAR1"
    assert result.etag == '"v1"'
    assert result.mtime == datetime(2026, 8, 16, 8, 23, 16, tzinfo=UTC)


@respx.mock
def test_get_bytes_sends_if_none_match_and_maps_304():
    route = respx.get(URL).mock(return_value=httpx.Response(304))
    with make_client() as client:
        result = get_bytes(client, URL, etag='"v1"')
    assert isinstance(result, NotModified)
    assert result.etag == '"v1"'
    assert route.calls.last.request.headers["if-none-match"] == '"v1"'


@respx.mock
def test_get_bytes_does_not_send_if_none_match_without_an_etag():
    route = respx.get(URL).mock(return_value=httpx.Response(200, content=b"x"))
    with make_client() as client:
        get_bytes(client, URL)
    assert "if-none-match" not in route.calls.last.request.headers


@respx.mock
def test_get_bytes_maps_404_to_notfound_without_retrying():
    route = respx.get(URL).mock(return_value=httpx.Response(404))
    with make_client() as client:
        result = get_bytes(client, URL)
    assert isinstance(result, NotFound)
    assert result.url == URL
    assert route.call_count == 1


@respx.mock
def test_get_bytes_retries_5xx_then_succeeds(monkeypatch):
    monkeypatch.setattr("ffh.ingest.http._RETRY_WAIT_CAP", 0.0)
    route = respx.get(URL).mock(
        side_effect=[
            httpx.Response(503),
            httpx.Response(502),
            httpx.Response(200, content=b"ok", headers={"ETag": '"v2"'}),
        ]
    )
    with make_client() as client:
        result = get_bytes(client, URL)
    assert isinstance(result, Fetched)
    assert result.content == b"ok"
    assert route.call_count == 3


@respx.mock
def test_get_bytes_gives_up_after_five_attempts(monkeypatch):
    monkeypatch.setattr("ffh.ingest.http._RETRY_WAIT_CAP", 0.0)
    route = respx.get(URL).mock(return_value=httpx.Response(429))
    with make_client() as client:
        with pytest.raises(RetryableStatus) as excinfo:
            get_bytes(client, URL)
    assert excinfo.value.status_code == 429
    assert route.call_count == 5


@respx.mock
def test_get_bytes_raises_on_unexpected_4xx():
    respx.get(URL).mock(return_value=httpx.Response(403))
    with make_client() as client:
        with pytest.raises(httpx.HTTPStatusError):
            get_bytes(client, URL)


@respx.mock
def test_retry_after_header_is_captured(monkeypatch):
    monkeypatch.setattr("ffh.ingest.http._RETRY_WAIT_CAP", 0.0)
    respx.get(URL).mock(
        side_effect=[
            httpx.Response(429, headers={"Retry-After": "2"}),
            httpx.Response(200, content=b"ok"),
        ]
    )
    with make_client() as client:
        assert isinstance(get_bytes(client, URL), Fetched)


@respx.mock
def test_malformed_last_modified_yields_mtime_none():
    respx.get(URL).mock(
        return_value=httpx.Response(200, content=b"ok", headers={"Last-Modified": "not a date"})
    )
    with make_client() as client:
        result = get_bytes(client, URL)
    assert isinstance(result, Fetched)
    assert result.mtime is None
