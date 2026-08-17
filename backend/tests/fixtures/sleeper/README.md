# Sleeper fixtures

Recorded responses from `https://api.sleeper.app/v1`. **CI never touches the network** —
every test drives these through `respx` mounted on `settings.sleeper_base_url`.

Source league: hand-written placeholder (`league_id=1000000000000000001`,
`draft_id=2000000000000000001`), authored 2026-08-16 to match live-verified shapes.
Replace with a real recording by running, from `backend/`:

    FFH_SLEEPER_MOCK_LEAGUE_ID=<id> uv run python scripts/record_sleeper_fixtures.py

That script rewrites every file here and updates the "Source league" line above.

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
