"""Record Sleeper fixtures from a real league.

  FFH_SLEEPER_MOCK_LEAGUE_ID=<id> uv run python scripts/record_sleeper_fixtures.py

Rewrites backend/tests/fixtures/sleeper/. This is the **only** code in this repo that
talks to the live Sleeper API — every test drives `respx` instead, and this script is
never imported except by `tests/adapters/sleeper/test_record_fixtures.py`, which mocks
it with the `sleeper_mock` fixture. It has no pytest-level network test of its own: unlike
the `network`-marked tests elsewhere in this suite (`pyproject.toml`'s `network` marker,
excluded from CI via `-m 'not network'`), this script *is* the manual invocation those
tests would otherwise describe — run it directly by hand, never via `pytest -m network`.

/players/nfl is 14.6 MB; only the rostered players plus a few free agents are kept, so the
committed fixture stays small.

Sleeper data is licensed for **non-commercial use only** — these fixtures exist solely to
test this self-hosted personal project.
"""

from __future__ import annotations

import asyncio
import json
import sys
from datetime import date
from pathlib import Path

from ffh.adapters.sleeper.client import SleeperClient
from ffh.config import get_settings

FIXTURES = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "sleeper"
EXTRA_FREE_AGENTS = 5

#: Single source of truth for both EXPECTED_FILES (what `record()` must write) and the
#: generated README table (ruling: never hand-transcribe the table a second time).
EXPECTED_FILES: tuple[tuple[str, str], ...] = (
    ("state_nfl", "`GET /state/nfl`"),
    ("league", "`GET /league/{id}`"),
    ("rosters", "`GET /league/{id}/rosters`"),
    ("users", "`GET /league/{id}/users`"),
    ("league_drafts", "`GET /league/{id}/drafts`"),
    ("draft", "`GET /draft/{id}`"),
    ("draft_picks", "`GET /draft/{id}/picks`"),
    ("matchups_week1", "`GET /league/{id}/matchups/1`"),
    ("transactions_week1", "`GET /league/{id}/transactions/1`"),
    (
        "players_slice",
        f"`GET /players/nfl`, restricted to rostered players plus {EXTRA_FREE_AGENTS} free agents",
    ),
)

README = """# Sleeper fixtures

Recorded responses from `https://api.sleeper.app/v1`. **CI never touches the network** —
every test drives these through `respx` mounted on `settings.sleeper_base_url`.

Source league: `{league_id}` (draft `{draft_id}`), recorded {recorded_on}.
Re-record from `backend/`:

    FFH_SLEEPER_MOCK_LEAGUE_ID=<id> uv run python scripts/record_sleeper_fixtures.py

Sleeper data is licensed for **non-commercial use only**. These fixtures exist solely to
test this self-hosted personal project.

| File | Endpoint |
|---|---|
{table_rows}
"""


def _render_table() -> str:
    return "\n".join(f"| `{stem}.json` | {desc} |" for stem, desc in EXPECTED_FILES)


def _dump(out_dir: Path, stem: str, payload: object) -> int:
    path = out_dir / f"{stem}.json"
    text = json.dumps(payload, indent=1, sort_keys=True) + "\n"
    path.write_text(text, encoding="utf-8")
    return len(text)


async def record(league_id: str, out_dir: Path, client: SleeperClient) -> dict[str, int]:
    out_dir.mkdir(parents=True, exist_ok=True)
    written: dict[str, int] = {}

    written["state_nfl"] = _dump(out_dir, "state_nfl", await client.get_json("/state/nfl"))
    league = await client.get_json(f"/league/{league_id}")
    written["league"] = _dump(out_dir, "league", league)
    rosters = await client.get_json(f"/league/{league_id}/rosters")
    written["rosters"] = _dump(out_dir, "rosters", rosters)
    written["users"] = _dump(out_dir, "users", await client.get_json(f"/league/{league_id}/users"))
    drafts = await client.get_json(f"/league/{league_id}/drafts")
    written["league_drafts"] = _dump(out_dir, "league_drafts", drafts)

    draft_id = league.get("draft_id") or (drafts[0]["draft_id"] if drafts else None)
    if draft_id is None:
        raise SystemExit(f"league {league_id} has no draft")
    written["draft"] = _dump(out_dir, "draft", await client.get_json(f"/draft/{draft_id}"))
    written["draft_picks"] = _dump(
        out_dir, "draft_picks", await client.get_json(f"/draft/{draft_id}/picks")
    )
    written["matchups_week1"] = _dump(
        out_dir, "matchups_week1", await client.get_json(f"/league/{league_id}/matchups/1")
    )
    written["transactions_week1"] = _dump(
        out_dir,
        "transactions_week1",
        await client.get_json(f"/league/{league_id}/transactions/1"),
    )

    rostered = {
        pid
        for r in rosters
        for pid in (
            *(r.get("players") or []),
            *(r.get("starters") or []),
            *(r.get("reserve") or []),
            *(r.get("taxi") or []),
        )
        if pid and pid != "0"
    }
    blob = await client.get_json("/players/nfl")
    sliced = {pid: blob[pid] for pid in sorted(rostered) if pid in blob}
    extras = [
        pid
        for pid in sorted(blob, key=lambda p: (blob[p].get("search_rank") or 10**9, p))
        if pid not in rostered
    ][:EXTRA_FREE_AGENTS]
    for pid in extras:
        sliced[pid] = blob[pid]
    missing = rostered - set(sliced)
    if missing:
        raise SystemExit(f"/players/nfl is missing rostered ids {sorted(missing)}")
    written["players_slice"] = _dump(out_dir, "players_slice", sliced)

    (out_dir / "README.md").write_text(
        README.format(
            league_id=league_id,
            draft_id=draft_id,
            recorded_on=date.today().isoformat(),
            table_rows=_render_table(),
        ),
        encoding="utf-8",
    )
    return written


async def _main() -> int:
    settings = get_settings()
    league_id = settings.sleeper_mock_league_id
    if not league_id:
        print("set FFH_SLEEPER_MOCK_LEAGUE_ID in backend/.env first", file=sys.stderr)
        return 2
    async with SleeperClient() as client:
        written = await record(league_id, FIXTURES, client)
    for stem, size in sorted(written.items()):
        print(f"{stem}.json  {size:>9,} bytes")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
