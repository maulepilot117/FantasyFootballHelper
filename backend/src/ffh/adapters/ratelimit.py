"""Client-side rate limiting for read-only platform APIs.

Sleeper's ceiling is 1000 req/min and IP-BASED — no key identifies us, so a block hits
everyone behind the same address. We hold 300 req/min (30% of ceiling) with a burst of 30,
still ~150x the 1-2s draft-poll budget (docs/ARCHITECTURE.md § Latency budgets).
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable


class TokenBucket:
    """Async token bucket. Clock and sleep are injected so tests are deterministic."""

    def __init__(
        self,
        rate_per_min: int = 300,
        burst: int = 30,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        if rate_per_min <= 0:
            raise ValueError("rate_per_min must be positive")
        if burst <= 0:
            raise ValueError("burst must be positive")
        self._rate_per_sec = rate_per_min / 60.0
        self._capacity = float(burst)
        self._clock = clock
        self._sleep = sleep
        self._tokens = float(burst)
        self._updated = clock()
        self._lock = asyncio.Lock()

    def _refill(self) -> None:
        now = self._clock()
        elapsed = max(0.0, now - self._updated)
        self._updated = now
        self._tokens = min(self._capacity, self._tokens + elapsed * self._rate_per_sec)

    @property
    def tokens(self) -> float:
        """Current allowance, refilled to `now`. Capped at `burst`."""
        self._refill()
        return self._tokens

    async def acquire(self, n: int = 1) -> None:
        """Block until `n` tokens are available, then consume them."""
        if n <= 0:
            raise ValueError("n must be positive")
        if n > self._capacity:
            raise ValueError(f"cannot acquire {n} tokens from a bucket of {self._capacity}")
        async with self._lock:
            self._refill()
            deficit = n - self._tokens
            if deficit > 0:
                await self._sleep(deficit / self._rate_per_sec)
                self._refill()
            self._tokens -= n
