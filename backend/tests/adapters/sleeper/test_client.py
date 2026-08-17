import time

import httpx
import pytest
import respx

from ffh.adapters.base import PlatformAuthError, PlatformError, PlatformNotFound
from ffh.adapters.ratelimit import TokenBucket
from ffh.adapters.sleeper.client import SleeperClient
from ffh.config import get_settings

BASE = get_settings().sleeper_base_url.rstrip("/")


class SleepRecorder:
    """Injected in place of asyncio.sleep so retry waits are observable and instant."""

    def __init__(self) -> None:
        self.calls: list[float] = []

    async def __call__(self, seconds: float) -> None:
        self.calls.append(seconds)


def _client(**kw) -> SleeperClient:
    kw.setdefault("base_url", BASE)
    kw.setdefault("retry_sleep", SleepRecorder())
    return SleeperClient(**kw)


async def test_get_state_parses(sleeper_client):
    state = await sleeper_client.get_state()
    assert state.season == "2026" and state.season_type == "regular" and state.week == 1


async def test_league_rosters_users_drafts_picks(sleeper_client, sleeper_fixture):
    league = await sleeper_client.get_league("1000000000000000001")
    rosters = await sleeper_client.get_rosters("1000000000000000001")
    users = await sleeper_client.get_users("1000000000000000001")
    drafts = await sleeper_client.get_league_drafts("1000000000000000001")
    draft = await sleeper_client.get_draft("2000000000000000001")
    picks = await sleeper_client.get_draft_picks("2000000000000000001")
    assert league.settings.num_teams == 2
    assert league.scoring_settings == sleeper_fixture("league")["scoring_settings"]
    assert [r.roster_id for r in rosters] == [1, 2]
    assert {u.user_id for u in users} == {"USER_ME", "USER_OPP"}
    assert len(drafts) == 1 and drafts[0].slot_to_roster_id == {}
    assert draft.slot_to_roster_id == {"1": 1, "2": 2}
    assert draft.last_picked == 1756083970192
    assert [p.pick_no for p in picks] == [1, 2, 3, 4]


async def test_matchups_and_transactions(sleeper_client):
    matchups = await sleeper_client.get_matchups("1000000000000000001", 1)
    txns = await sleeper_client.get_transactions("1000000000000000001", 1)
    assert [m.roster_id for m in matchups] == [1, 2]
    assert {t.transaction_id for t in txns} == {"TXN1", "TXN2"}


@respx.mock
async def test_404_raises_platform_not_found():
    respx.get(f"{BASE}/league/nope").mock(return_value=httpx.Response(404))
    async with _client() as client:
        with pytest.raises(PlatformNotFound):
            await client.get_json("/league/nope")


@respx.mock
async def test_401_raises_platform_auth_error():
    respx.get(f"{BASE}/league/x").mock(return_value=httpx.Response(401))
    async with _client() as client:
        with pytest.raises(PlatformAuthError):
            await client.get_json("/league/x")


@respx.mock
async def test_retries_a_500_then_succeeds():
    route = respx.get(f"{BASE}/state/nfl").mock(
        side_effect=[httpx.Response(500), httpx.Response(200, json={"ok": True})]
    )
    sleeper = SleepRecorder()
    async with _client(retry_sleep=sleeper) as client:
        assert await client.get_json("/state/nfl") == {"ok": True}
    assert route.call_count == 2
    # No Retry-After header -> exponential jitter (initial 0.5s, jitter up to 1s).
    assert len(sleeper.calls) == 1
    assert 0.5 <= sleeper.calls[0] <= 1.5


@respx.mock
async def test_retries_a_429_then_succeeds_honouring_retry_after():
    route = respx.get(f"{BASE}/state/nfl").mock(
        side_effect=[
            httpx.Response(429, headers={"Retry-After": "1"}),
            httpx.Response(200, json={"ok": True}),
        ]
    )
    sleeper = SleepRecorder()
    async with _client(retry_sleep=sleeper) as client:
        assert await client.get_json("/state/nfl") == {"ok": True}
    assert route.call_count == 2
    assert sleeper.calls == [1.0]


@respx.mock
async def test_retry_after_is_capped_at_sixty_seconds():
    respx.get(f"{BASE}/state/nfl").mock(
        side_effect=[
            httpx.Response(503, headers={"Retry-After": "3600"}),
            httpx.Response(200, json={"ok": True}),
        ]
    )
    sleeper = SleepRecorder()
    async with _client(retry_sleep=sleeper) as client:
        assert await client.get_json("/state/nfl") == {"ok": True}
    assert sleeper.calls == [60.0]


@respx.mock
async def test_non_numeric_retry_after_falls_back_to_backoff():
    respx.get(f"{BASE}/state/nfl").mock(
        side_effect=[
            httpx.Response(429, headers={"Retry-After": "Wed, 21 Oct 2026 07:28:00 GMT"}),
            httpx.Response(200, json={"ok": True}),
        ]
    )
    sleeper = SleepRecorder()
    async with _client(retry_sleep=sleeper) as client:
        assert await client.get_json("/state/nfl") == {"ok": True}
    assert len(sleeper.calls) == 1
    assert 0.5 <= sleeper.calls[0] <= 1.5


@pytest.mark.parametrize("header", ["nan", "inf", "-inf"])
@respx.mock
async def test_non_finite_retry_after_falls_back_to_backoff(header):
    # float("nan") parses; it must not become the sleep duration.
    respx.get(f"{BASE}/state/nfl").mock(
        side_effect=[
            httpx.Response(429, headers={"Retry-After": header}),
            httpx.Response(200, json={"ok": True}),
        ]
    )
    sleeper = SleepRecorder()
    async with _client(retry_sleep=sleeper) as client:
        assert await client.get_json("/state/nfl") == {"ok": True}
    assert len(sleeper.calls) == 1
    assert 0.5 <= sleeper.calls[0] <= 1.5


@respx.mock
async def test_gives_up_after_five_attempts_and_raises_platform_error():
    route = respx.get(f"{BASE}/state/nfl").mock(return_value=httpx.Response(503))
    async with _client() as client:
        with pytest.raises(PlatformError) as exc_info:
            await client.get_json("/state/nfl")
    assert route.call_count == 5
    # The internal retryable marker must never escape: the final error is the plain
    # PlatformError, and it says how hard we tried.
    assert exc_info.type is PlatformError
    assert "after 5 attempts" in str(exc_info.value)


@respx.mock
async def test_transport_error_is_retried_then_translated():
    route = respx.get(f"{BASE}/state/nfl").mock(side_effect=httpx.ConnectError("boom"))
    async with _client() as client:
        with pytest.raises(PlatformError) as exc_info:
            await client.get_json("/state/nfl")
    assert route.call_count == 5
    assert exc_info.type is PlatformError
    assert isinstance(exc_info.value.__cause__, httpx.TransportError)


@respx.mock
async def test_other_4xx_raises_platform_error_without_retry():
    route = respx.get(f"{BASE}/league/bad").mock(return_value=httpx.Response(400))
    async with _client() as client:
        with pytest.raises(PlatformError) as exc_info:
            await client.get_json("/league/bad")
    assert route.call_count == 1
    assert exc_info.type is PlatformError


@respx.mock
async def test_non_json_200_body_raises_platform_error():
    route = respx.get(f"{BASE}/state/nfl").mock(
        return_value=httpx.Response(200, text="<html>maintenance</html>")
    )
    async with _client() as client:
        with pytest.raises(PlatformError) as exc_info:
            await client.get_json("/state/nfl")
    assert route.call_count == 1
    assert exc_info.type is PlatformError
    assert "non-JSON" in str(exc_info.value)


@respx.mock
async def test_get_user_null_body_raises_platform_not_found():
    # Sleeper returns HTTP 200 with body `null` for an unknown username.
    respx.get(f"{BASE}/user/nobody").mock(
        return_value=httpx.Response(200, text="null", headers={"content-type": "application/json"})
    )
    async with _client() as client:
        with pytest.raises(PlatformNotFound):
            await client.get_user("nobody")


@respx.mock
async def test_typed_getter_translates_a_bad_shape_into_platform_error():
    # A 200 whose body does not fit the Raw model is Sleeper misbehaving, not our bug:
    # pydantic's ValidationError must never escape the client.
    respx.get(f"{BASE}/league/5").mock(return_value=httpx.Response(200, json={"league_id": 5}))
    async with _client() as client:
        with pytest.raises(PlatformError) as exc_info:
            await client.get_league("5")
    assert exc_info.type is PlatformError
    assert "unexpected shape for /league/5" in str(exc_info.value)


@respx.mock
async def test_typed_list_getter_translates_a_null_body_into_platform_error():
    # `null` where a list was expected used to surface as TypeError from iterating None.
    respx.get(f"{BASE}/league/5/rosters").mock(
        return_value=httpx.Response(200, text="null", headers={"content-type": "application/json"})
    )
    async with _client() as client:
        with pytest.raises(PlatformError) as exc_info:
            await client.get_rosters("5")
    assert exc_info.type is PlatformError
    assert "unexpected shape for /league/5/rosters" in str(exc_info.value)


@respx.mock
async def test_typed_object_getter_translates_a_null_body_into_platform_error():
    respx.get(f"{BASE}/draft/9").mock(
        return_value=httpx.Response(200, text="null", headers={"content-type": "application/json"})
    )
    async with _client() as client:
        with pytest.raises(PlatformError) as exc_info:
            await client.get_draft("9")
    assert exc_info.type is PlatformError
    assert "null body" in str(exc_info.value)


@respx.mock
async def test_typed_list_getter_translates_a_bad_element_into_platform_error():
    respx.get(f"{BASE}/league/5/users").mock(
        return_value=httpx.Response(200, json=[{"user_id": "u1"}, {"display_name": "no id"}])
    )
    async with _client() as client:
        with pytest.raises(PlatformError, match="unexpected shape for /league/5/users"):
            await client.get_users("5")


@respx.mock
async def test_every_request_spends_a_rate_limit_token():
    respx.get(f"{BASE}/state/nfl").mock(return_value=httpx.Response(200, json={}))
    bucket = TokenBucket(rate_per_min=300, burst=30)
    async with _client(rate=bucket) as client:
        before = bucket.tokens
        await client.get_json("/state/nfl")
        assert bucket.tokens < before


async def test_default_rate_is_300_per_minute_burst_30():
    async with _client() as client:
        bucket = client.rate
        assert bucket.tokens == pytest.approx(30.0)
        # Drain the burst; nothing sleeps until the bucket is empty.
        for _ in range(30):
            await bucket.acquire()
        assert bucket.tokens < 1.0
        # 300/min == 5 tokens/s: the 31st acquire must wait for the remaining deficit,
        # i.e. (1 - tokens) / 5 seconds (~0.2s), whatever the drain loop cost.
        t0 = time.monotonic()
        await bucket.acquire()
        elapsed = time.monotonic() - t0
    # Loose bounds: it must actually wait (not fall through) and must not wait for a
    # whole burst; timer resolution and CI load make a tight approx flaky.
    assert 0.15 <= elapsed <= 1.0


async def test_context_manager_closes_owned_http():
    client = _client()
    http = client._http
    async with client:
        pass
    assert http.is_closed


async def test_injected_http_client_is_not_closed():
    async with httpx.AsyncClient(base_url=BASE) as http:
        async with _client(http=http):
            pass
        assert not http.is_closed
