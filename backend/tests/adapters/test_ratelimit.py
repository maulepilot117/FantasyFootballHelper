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
