"""The one HTTP client every ingest job uses. Conditional GET + tenacity backoff.

The ETag a server returns depends on the negotiated Content-Encoding, so a stored ETag is
only valid for a request made with the same client configuration. That is why there is
exactly one client factory and every job goes through it.
"""

from dataclasses import dataclass
from datetime import datetime
from email.utils import parsedate_to_datetime

import httpx
import structlog
from tenacity import RetryCallState, retry, retry_if_exception_type, stop_after_attempt
from tenacity.wait import wait_exponential

from ffh import __version__

log = structlog.get_logger(__name__)

RETRYABLE_STATUSES = frozenset({408, 425, 429, 500, 502, 503, 504})
MAX_ATTEMPTS = 5

# Multiplied into every computed wait; tests set it to 0.0 to keep the suite fast.
_RETRY_WAIT_CAP = 30.0


@dataclass(frozen=True, slots=True)
class Fetched:
    """The asset was downloaded."""

    content: bytes
    etag: str | None
    mtime: datetime | None


@dataclass(frozen=True, slots=True)
class NotModified:
    """The server answered 304 — the stored ETag is still current."""

    etag: str | None


@dataclass(frozen=True, slots=True)
class NotFound:
    """The asset does not exist yet (nflverse seasonal assets 404 before Week 1)."""

    url: str


type FetchResult = Fetched | NotModified | NotFound


class RetryableStatus(Exception):
    """A transient HTTP status worth retrying."""

    def __init__(self, status_code: int, url: str, retry_after: float | None = None) -> None:
        super().__init__(f"{status_code} from {url}")
        self.status_code = status_code
        self.url = url
        self.retry_after = retry_after


def make_client(timeout: float = 60.0) -> httpx.Client:
    """The shared client. GitHub Releases redirect to objects.githubusercontent.com."""
    return httpx.Client(
        follow_redirects=True,
        timeout=httpx.Timeout(timeout, connect=15.0),
        headers={"User-Agent": f"ffh/{__version__} (+https://github.com/nflverse)"},
    )


def _retry_after_seconds(response: httpx.Response) -> float | None:
    raw = response.headers.get("retry-after")
    if raw is None:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _wait(state: RetryCallState) -> float:
    exc = state.outcome.exception() if state.outcome is not None else None
    if isinstance(exc, RetryableStatus) and exc.retry_after is not None:
        return min(exc.retry_after, _RETRY_WAIT_CAP)
    base = wait_exponential(multiplier=1.0, min=1.0, max=30.0)(state)
    return min(base, _RETRY_WAIT_CAP)


@retry(
    retry=retry_if_exception_type((RetryableStatus, httpx.TransportError)),
    wait=_wait,
    stop=stop_after_attempt(MAX_ATTEMPTS),
    reraise=True,
)
def _request(client: httpx.Client, url: str, headers: dict[str, str]) -> httpx.Response:
    response = client.get(url, headers=headers)
    if response.status_code in RETRYABLE_STATUSES:
        raise RetryableStatus(response.status_code, url, _retry_after_seconds(response))
    return response


def get_bytes(client: httpx.Client, url: str, etag: str | None = None) -> FetchResult:
    """Conditional GET. 304 -> NotModified, 404 -> NotFound, 2xx -> Fetched.

    Any other 4xx/5xx raises. Retryable statuses are retried up to MAX_ATTEMPTS with
    exponential backoff, honouring a numeric ``Retry-After`` when the server sends one.
    """
    headers = {"If-None-Match": etag} if etag else {}
    response = _request(client, url, headers)

    if response.status_code == 304:
        log.info("ingest.http.not_modified", url=url)
        return NotModified(etag=response.headers.get("etag") or etag)
    if response.status_code == 404:
        log.info("ingest.http.not_found", url=url)
        return NotFound(url=url)
    response.raise_for_status()

    last_modified = response.headers.get("last-modified")
    mtime = parsedate_to_datetime(last_modified) if last_modified else None
    log.info("ingest.http.fetched", url=url, bytes=len(response.content))
    return Fetched(content=response.content, etag=response.headers.get("etag"), mtime=mtime)
