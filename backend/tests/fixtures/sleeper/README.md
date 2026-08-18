# Sleeper fixtures

Recorded responses from `https://api.sleeper.app/v1`. **CI never touches the network** —
every test drives these through `respx` mounted on `settings.sleeper_base_url`.

Source league: hand-written SYNTHETIC corpus (`league_id=1000000000000000001`,
`draft_id=2000000000000000001`), authored 2026-08-16 to match live-verified shapes.
The unit tests are bound to this corpus (ids, counts, names) — do not record over it and
do not re-point the tests elsewhere. A live recording is a separate directory,
`tests/fixtures/sleeper_live/`, written from `backend/` by (or set the var in
`backend/.env`, which works on every host):

    FFH_SLEEPER_MOCK_LEAGUE_ID=<id> uv run python scripts/record_sleeper_fixtures.py   # POSIX
    $env:FFH_SLEEPER_MOCK_LEAGUE_ID="<id>"; uv run python scripts/record_sleeper_fixtures.py  # pwsh

No live recording has been made yet — `tests/fixtures/sleeper_live/` does not exist in the
tree, and nothing in the suite reads it.

Sleeper data is licensed for **non-commercial use only**. These fixtures exist solely to
test this self-hosted personal project.

| File | Endpoint |
|---|---|
| `state_nfl.json` | `GET /state/nfl` |
| `league.json` | `GET /league/{id}` |
| `rosters.json` | `GET /league/{id}/rosters` |
| `users.json` | `GET /league/{id}/users` |
| `league_drafts.json` | `GET /league/{id}/drafts` |
| `draft.json` | `GET /draft/{id}` |
| `draft_picks.json` | `GET /draft/{id}/picks` |
| `matchups_week1.json` | `GET /league/{id}/matchups/1` |
| `transactions_week1.json` | `GET /league/{id}/transactions/1` |
| `players_slice.json` | `GET /players/nfl`, restricted to rostered players plus two free agents |
