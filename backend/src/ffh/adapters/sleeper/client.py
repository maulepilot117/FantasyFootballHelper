"""Thin in-house async Sleeper client.

No auth, no key — and therefore an IP-BASED 1000 req/min ceiling. We hold 300 req/min.
Sleeper is READ-ONLY and non-commercial-use-only (docs/DATA_SOURCES.md §3).

GET /players/nfl is deliberately NOT exposed here: it is 14.6 MB and belongs to the
`sleeper_players` IngestJob, at most once a day.
"""

from __future__ import annotations

import math
from collections.abc import Awaitable, Callable
from typing import Any

import httpx
from tenacity import (
    AsyncRetrying,
    RetryCallState,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from ffh.adapters.base import PlatformAuthError, PlatformError, PlatformNotFound
from ffh.adapters.ratelimit import TokenBucket
from ffh.adapters.sleeper.models import (
    RawDraft,
    RawDraftPick,
    RawLeague,
    RawMatchup,
    RawRoster,
    RawState,
    RawTransaction,
    RawUser,
)
from ffh.config import get_settings

MAX_ATTEMPTS = 5
# Never trust a server to park us for longer than this, whatever Retry-After says.
MAX_RETRY_AFTER_SECONDS = 60.0


class _Retryable(PlatformError):
    """429 or 5xx — worth another attempt. Never escapes get_json()."""

    def __init__(self, message: str, *, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


def _parse_retry_after(resp: httpx.Response) -> float | None:
    """Numeric `Retry-After` seconds, capped; None if absent, HTTP-date form, negative or
    non-finite ("nan"/"inf" parse as floats but are not a wait)."""
    raw = resp.headers.get("Retry-After")
    if raw is None:
        return None
    try:
        seconds = float(raw.strip())
    except ValueError:
        return None
    if not math.isfinite(seconds) or seconds < 0:
        return None
    return min(seconds, MAX_RETRY_AFTER_SECONDS)


class SleeperClient:
    def __init__(
        self,
        base_url: str | None = None,
        http: httpx.AsyncClient | None = None,
        rate: TokenBucket | None = None,
        *,
        timeout: float = 10.0,
        retry_sleep: Callable[[float], Awaitable[None]] | None = None,
    ) -> None:
        self.base_url = (base_url or get_settings().sleeper_base_url).rstrip("/")
        self._owns_http = http is None
        self._http = http or httpx.AsyncClient(base_url=self.base_url, timeout=timeout)
        self.rate = rate or TokenBucket(rate_per_min=300, burst=30)
        self._retry_sleep = retry_sleep
        self._backoff = wait_exponential_jitter(initial=0.5, max=8.0)

    async def __aenter__(self) -> SleeperClient:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._owns_http:
            await self._http.aclose()

    async def _get_once(self, path: str) -> Any:
        await self.rate.acquire()
        resp = await self._http.get(path)
        code = resp.status_code
        if code == 404:
            raise PlatformNotFound(f"sleeper 404 for {path}")
        if code in (401, 403):
            raise PlatformAuthError(f"sleeper {code} for {path}")
        if code == 429 or code >= 500:
            raise _Retryable(f"sleeper {code} for {path}", retry_after=_parse_retry_after(resp))
        if code >= 400:
            raise PlatformError(f"sleeper {code} for {path}")
        try:
            return resp.json()
        except ValueError as exc:
            # CDN/maintenance HTML, or a 3xx body (httpx does not follow redirects).
            raise PlatformError(f"sleeper: non-JSON body for {path}") from exc

    def _wait(self, retry_state: RetryCallState) -> float:
        """Honour a numeric Retry-After when the server sent one; else exponential jitter."""
        outcome = retry_state.outcome
        exc = outcome.exception() if outcome is not None and outcome.failed else None
        if isinstance(exc, _Retryable) and exc.retry_after is not None:
            return exc.retry_after
        return self._backoff(retry_state)

    async def get_json(self, path: str) -> Any:
        retry_kwargs: dict[str, Any] = {
            "retry": retry_if_exception_type((_Retryable, httpx.TransportError)),
            "stop": stop_after_attempt(MAX_ATTEMPTS),
            "wait": self._wait,
            "reraise": True,
        }
        if self._retry_sleep is not None:
            retry_kwargs["sleep"] = self._retry_sleep
        retryer = AsyncRetrying(**retry_kwargs)
        try:
            return await retryer(self._get_once, path)
        except _Retryable as exc:
            raise PlatformError(
                f"sleeper unavailable after {MAX_ATTEMPTS} attempts: {path}"
            ) from exc
        except httpx.TransportError as exc:
            raise PlatformError(
                f"sleeper transport failure after {MAX_ATTEMPTS} attempts: {path}"
            ) from exc

    # --- endpoints -------------------------------------------------------------
    async def get_state(self) -> RawState:
        return RawState.model_validate(await self.get_json("/state/nfl"))

    async def get_user(self, username_or_id: str) -> RawUser:
        # Sleeper answers an unknown user with HTTP 200 and a literal `null` body.
        payload = await self.get_json(f"/user/{username_or_id}")
        if payload is None:
            raise PlatformNotFound(f"sleeper user not found: {username_or_id}")
        return RawUser.model_validate(payload)

    async def get_user_leagues(self, user_id: str, season: int) -> list[RawLeague]:
        payload = await self.get_json(f"/user/{user_id}/leagues/nfl/{season}")
        return [RawLeague.model_validate(x) for x in payload]

    async def get_league(self, league_id: str) -> RawLeague:
        return RawLeague.model_validate(await self.get_json(f"/league/{league_id}"))

    async def get_rosters(self, league_id: str) -> list[RawRoster]:
        payload = await self.get_json(f"/league/{league_id}/rosters")
        return [RawRoster.model_validate(x) for x in payload]

    async def get_users(self, league_id: str) -> list[RawUser]:
        payload = await self.get_json(f"/league/{league_id}/users")
        return [RawUser.model_validate(x) for x in payload]

    async def get_matchups(self, league_id: str, week: int) -> list[RawMatchup]:
        payload = await self.get_json(f"/league/{league_id}/matchups/{week}")
        return [RawMatchup.model_validate(x) for x in payload]

    async def get_transactions(self, league_id: str, week: int) -> list[RawTransaction]:
        payload = await self.get_json(f"/league/{league_id}/transactions/{week}")
        return [RawTransaction.model_validate(x) for x in payload]

    async def get_league_drafts(self, league_id: str) -> list[RawDraft]:
        payload = await self.get_json(f"/league/{league_id}/drafts")
        return [RawDraft.model_validate(x) for x in payload]

    async def get_draft(self, draft_id: str) -> RawDraft:
        return RawDraft.model_validate(await self.get_json(f"/draft/{draft_id}"))

    async def get_draft_picks(self, draft_id: str) -> list[RawDraftPick]:
        payload = await self.get_json(f"/draft/{draft_id}/picks")
        return [RawDraftPick.model_validate(x) for x in payload]
