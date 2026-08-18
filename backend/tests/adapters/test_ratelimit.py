import asyncio

import pytest

from ffh.adapters.ratelimit import TokenBucket


class FakeClock:
    """Monotonic clock that only advances when the fake sleep is awaited."""

    def __init__(self) -> None:
        self.now = 1000.0
        self.slept: list[float] = []

    def __call__(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        assert seconds >= 0
        self.slept.append(seconds)
        self.now += seconds


def _bucket(clock: FakeClock, rate: int = 300, burst: int = 30) -> TokenBucket:
    return TokenBucket(rate_per_min=rate, burst=burst, clock=clock, sleep=clock.sleep)


async def test_burst_is_served_without_sleeping():
    clock = FakeClock()
    bucket = _bucket(clock)
    for _ in range(30):
        await bucket.acquire()
    assert clock.slept == []
    assert bucket.tokens == pytest.approx(0.0)


async def test_the_31st_call_in_a_burst_waits_for_one_refill():
    clock = FakeClock()
    bucket = _bucket(clock)
    for _ in range(31):
        await bucket.acquire()
    # 300/min == 5 tokens/s == 0.2s per token
    assert clock.slept == [pytest.approx(0.2)]


async def test_never_exceeds_budget_over_a_sustained_burst():
    """100 calls at 300/min with burst 30 must span exactly (100-30)/5 seconds."""
    clock = FakeClock()
    bucket = _bucket(clock)
    start = clock.now
    for _ in range(100):
        await bucket.acquire()
    elapsed = clock.now - start
    assert elapsed == pytest.approx((100 - 30) / 5.0)


async def test_tokens_refill_while_idle_and_cap_at_burst():
    clock = FakeClock()
    bucket = _bucket(clock)
    for _ in range(30):
        await bucket.acquire()
    clock.now += 3600.0  # an hour idle
    assert bucket.tokens == pytest.approx(30.0)  # capped, not 18000
    for _ in range(30):
        await bucket.acquire()
    assert clock.slept == []


async def test_acquire_more_than_one_token():
    clock = FakeClock()
    bucket = _bucket(clock)
    await bucket.acquire(30)
    assert bucket.tokens == pytest.approx(0.0)
    await bucket.acquire(5)
    assert clock.slept == [pytest.approx(1.0)]


async def test_rejects_a_request_larger_than_the_bucket():
    clock = FakeClock()
    bucket = _bucket(clock)
    with pytest.raises(ValueError):
        await bucket.acquire(31)


def test_rejects_nonsense_construction():
    with pytest.raises(ValueError):
        TokenBucket(rate_per_min=300, burst=0)
    with pytest.raises(ValueError):
        TokenBucket(rate_per_min=0, burst=30)


class VirtualClock:
    """Shared virtual timeline for real-concurrency tests (multiple live asyncio tasks).

    `FakeClock` above only advances inside a single synchronous caller and never lets
    tasks interleave, so it cannot exercise `TokenBucket`'s internal lock. `VirtualClock`
    instead lets many tasks call `sleep` "at once": each call registers a deadline on a
    shared timeline and cooperatively yields with a bare `await asyncio.sleep(0)` so any
    sibling task gets a turn to run (and register its own deadline, or resume past its
    own). Once a round passes where the set of pending deadlines did not grow — i.e.
    every live task is now blocked on a deadline rather than doing more synchronous work
    — the clock jumps `now` forward to the earliest pending deadline, waking whichever
    sleeper(s) that satisfies. No real wall time ever elapses.
    """

    def __init__(self) -> None:
        self._now = 1000.0
        self._deadlines: list[float] = []

    def now(self) -> float:
        return self._now

    async def sleep(self, seconds: float) -> None:
        assert seconds >= 0
        deadline = self._now + seconds
        self._deadlines.append(deadline)
        stable_rounds = 0
        while self._now < deadline:
            before = len(self._deadlines)
            await asyncio.sleep(0)
            if self._now >= deadline:
                break
            if len(self._deadlines) == before:
                stable_rounds += 1
            else:
                stable_rounds = 0
            if stable_rounds >= 1:
                self._now = max(self._now, min(self._deadlines))
        self._deadlines.remove(deadline)


async def test_lock_serialises_concurrent_acquires():
    """90 concurrent acquire() calls must never let two tasks double-spend the budget.

    Without the internal lock, concurrent tasks all observe the same stale `tokens` and
    each independently compute a 0.2s wait — so the mutant races through in ~0.2s total.
    With the lock, the 60 calls beyond the initial burst of 30 are strictly serialised,
    each waiting for its own token to refill at 5/s: 60 tokens / 5 per s = 12.0s.
    """
    vc = VirtualClock()
    bucket = TokenBucket(rate_per_min=300, burst=30, clock=vc.now, sleep=vc.sleep)
    start = vc.now()
    await asyncio.gather(*(bucket.acquire() for _ in range(90)))
    elapsed = vc.now() - start
    assert elapsed == pytest.approx(12.0)
