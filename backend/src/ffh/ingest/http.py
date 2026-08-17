"""The one HTTP client every ingest job uses. Conditional GET + tenacity backoff.

The ETag a server returns depends on the negotiated Content-Encoding, so a stored ETag is
only valid for a request made with the same client configuration. That is why there is
exactly one client factory and every job goes through it.
"""

import math
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime

import httpx
import structlog
from tenacity import RetryCallState, retry, retry_if_exception, stop_after_attempt
from tenacity.wait import wait_exponential

from ffh import __version__

log = structlog.get_logger(__name__)

RETRYABLE_STATUSES = frozenset({408, 425, 429, 500, 502, 503, 504})
MAX_ATTEMPTS = 5

# Upper bound (``min()``) on every *backoff* wait; tests set it to 0.0 to keep the suite fast.
_RETRY_WAIT_CAP = 30.0
# A server-directed ``Retry-After`` is honoured exactly up to this many seconds. Beyond it we
# stop retrying (and raise) rather than silently re-request early against the directive.
_RETRY_AFTER_MAX = 120.0
# Indirection so tests can observe/neutralize real sleeps.
_sleep = time.sleep


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
    """Parse ``Retry-After`` (RFC 9110 §10.2.3): delay-seconds or an HTTP-date.

    Returns a non-negative finite number of seconds, or ``None`` when the header is absent
    or malformed (negative, NaN/inf, unparseable) — ``None`` means "fall back to backoff".
    """
    raw = response.headers.get("retry-after")
    if raw is None:
        return None
    raw = raw.strip()
    try:
        seconds = float(raw)
    except ValueError:
        seconds = _http_date_delay(raw)
        if seconds is None:
            log.warning("ingest.http.bad_retry_after", value=raw)
            return None
    if not math.isfinite(seconds) or seconds < 0:
        log.warning("ingest.http.bad_retry_after", value=raw)
        return None
    return seconds


def _http_date_delay(raw: str) -> float | None:
    """Seconds from now until an HTTP-date; 0.0 if it is already past; None if unparseable."""
    try:
        when = parsedate_to_datetime(raw)
    except (TypeError, ValueError, IndexError):
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=UTC)
    return max(0.0, (when - datetime.now(UTC)).total_seconds())


def _last_modified(response: httpx.Response) -> datetime | None:
    raw = response.headers.get("last-modified")
    if not raw:
        return None
    try:
        return parsedate_to_datetime(raw)
    except (TypeError, ValueError):
        log.warning("ingest.http.bad_last_modified", value=raw)
        return None


def _wait(state: RetryCallState) -> float:
    exc = state.outcome.exception() if state.outcome is not None else None
    if isinstance(exc, RetryableStatus) and exc.retry_after is not None:
        # honoured exactly (a server directive is not a backoff heuristic); the stop
        # predicate below has already refused anything above _RETRY_AFTER_MAX.
        return exc.retry_after
    base = wait_exponential(multiplier=1.0, min=1.0, max=30.0)(state)
    return min(base, _RETRY_WAIT_CAP)


def _should_retry(exc: BaseException) -> bool:
    if isinstance(exc, httpx.TransportError):
        return True
    if isinstance(exc, RetryableStatus):
        if exc.retry_after is not None and exc.retry_after > _RETRY_AFTER_MAX:
            log.warning(
                "ingest.http.retry_after_too_long",
                url=exc.url,
                retry_after=exc.retry_after,
                max=_RETRY_AFTER_MAX,
            )
            return False
        return True
    return False


@retry(
    retry=retry_if_exception(_should_retry),
    wait=_wait,
    stop=stop_after_attempt(MAX_ATTEMPTS),
    sleep=lambda seconds: _sleep(seconds),
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
    exponential backoff. A numeric ``Retry-After`` is honoured exactly up to
    ``_RETRY_AFTER_MAX`` seconds; a longer directive ends the retry loop and raises
    ``RetryableStatus`` instead of re-requesting early.
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

    mtime = _last_modified(response)
    log.info("ingest.http.fetched", url=url, bytes=len(response.content))
    return Fetched(content=response.content, etag=response.headers.get("etag"), mtime=mtime)
