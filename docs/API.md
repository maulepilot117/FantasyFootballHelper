# API Contract

FastAPI backend ↔ Bun/React frontend. **Change this doc in the same PR as the code.**

Base path `/api/v1`. JSON everywhere. Timestamps RFC 3339 UTC. Money in whole dollars,
points as floats.

---

## Conventions

- **Errors** — RFC 7807 problem details:
  ```jsonc
  {"type": "https://ffh/errors/upstream-unavailable", "title": "Sleeper unreachable",
   "status": 503, "detail": "...", "instance": "/api/v1/draft/abc/state"}
  ```
- **Staleness** — any response derived from a cached upstream carries
  `"_meta": {"stale": true, "as_of": "...", "source": "sleeper"}`. The UI must show this.
  Undocumented endpoints break; silently serving old data is worse than saying so.
- **Pagination** — `?limit=&cursor=`, response `{"items": [...], "next_cursor": null}`.
- **Auth** — single-user, self-hosted. Bearer token from Vault, checked by middleware.
  Not multi-tenant; do not build user accounts.

---

## Health and meta

```
GET  /healthz                    liveness — no dependency checks
GET  /api/v1/healthz             same payload as /healthz, reachable through the /api Gateway route (used by the UI)
GET  /readyz                     readiness — Postgres + Redis reachable
GET  /api/v1/meta/state          current NFL season, week, season_type
GET  /api/v1/meta/ingest         last successful run per source/asset + staleness flags
```

`/api/v1/meta/ingest` powers a status strip in the UI. If nflverse hasn't refreshed since
Sunday night, Chris should be able to see that without reading logs.

---

## Leagues

```
GET  /api/v1/leagues                              all configured leagues
POST /api/v1/leagues                              {platform, external_id, season}
GET  /api/v1/leagues/{id}                         incl. scoring + roster settings
POST /api/v1/leagues/{id}/sync                    force refresh from platform
GET  /api/v1/leagues/{id}/teams
GET  /api/v1/leagues/{id}/rosters?week=
GET  /api/v1/leagues/{id}/matchups?week=
GET  /api/v1/leagues/{id}/standings                incl. all-play record + schedule luck
GET  /api/v1/leagues/{id}/playoff-odds             Monte Carlo output
```

`POST /leagues` triggers adapter detection, scoring/roster normalization, crosswalk
resolution for every rostered player, and returns any unmatched players as a warning —
**do not fail silently on crosswalk gaps.**

---

## Draft

The latency-critical surface. See [`ARCHITECTURE.md`](ARCHITECTURE.md) for budgets.

```
GET  /api/v1/drafts?league_id=
GET  /api/v1/drafts/{id}                          metadata, my slot, status
GET  /api/v1/drafts/{id}/board                    full board: available, taken, tiers
GET  /api/v1/drafts/{id}/picks                    pick log
POST /api/v1/drafts/{id}/picks                    manual entry (degraded mode fallback)
GET  /api/v1/drafts/{id}/recommendations          ← the money endpoint
GET  /api/v1/drafts/{id}/export/cheatsheet        static PDF/HTML — REQUIRED deliverable
WS   /api/v1/drafts/{id}/live
```

### `GET /drafts/{id}/recommendations`

Returns in **< 2s**, engine-only. The debate arrives separately over the WebSocket.

```jsonc
{
  "pick_no": 14, "round": 2, "picks_until_my_turn": 21,
  "recommendations": [
    {
      "rank": 1,
      "player": {"id": "uuid", "name": "Saquon Barkley", "pos": "RB",
                 "team": "PHI", "bye": 7},
      "weighted_score": 87.3,
      "components": {
        "vona": 68.4, "vorp": 141.2, "tier": 2,
        "tier_players_remaining": 3, "roster_need_multiplier": 1.15,
        "adp_value": 2.6
      },
      "projection": {"mean": 274.6, "p10": 198.1, "p90": 351.2},
      "rationale": "Engine-generated one-liner",
      "flags": ["tier_cliff_imminent"]
    }
    // max 3
  ],
  "debate": null,          // populated later via WS; null means not yet
  "computed_in_ms": 840
}
```

⚠️ **Max 3 recommendations, per spec.** `components` is always present — showing the
weighting is the point, not a debug affordance.

### WebSocket `/drafts/{id}/live`

Server→client events:

```jsonc
{"type": "pick",            "data": {"pick_no": 15, "player": {...}, "team": {...}}}
{"type": "on_the_clock",    "data": {"team": {...}, "is_me": true, "seconds": 90}}
{"type": "recommendations", "data": { /* same shape as REST */ }}
{"type": "debate_started",  "data": {"debate_id": "uuid"}}
{"type": "debate_progress", "data": {"round": 2, "status": "refutation"}}
{"type": "debate_complete", "data": { /* verdict, see below */ }}
{"type": "stale",           "data": {"source": "sleeper", "since": "..."}}
{"type": "error",           "data": {"recoverable": true, "message": "..."}}
```

Client→server: `{"type": "ping"}` only. Draft state is read-only from the platform.

**Reconnect must be seamless.** On connect the server replays current state (board, picks,
latest recommendations) before streaming deltas. A dropped socket at pick 1.03 cannot
require a page reload.

---

## Lineup

```
GET  /api/v1/leagues/{id}/lineup?week=            current + recommended
POST /api/v1/leagues/{id}/lineup/analyze?week=    force recompute
GET  /api/v1/leagues/{id}/lineup/history
```

```jsonc
{
  "week": 3,
  "opponent": {"name": "...", "projected": 118.0},
  "current":     {"projected": 112.0, "win_prob": 0.393, "sigma": 22.0},
  "recommended": {"projected": 111.6, "win_prob": 0.421, "sigma": 30.0},
  "posture": "underdog",          // underdog | favorite | tossup
  "changes": [
    {
      "action": "start", "player_in": {...}, "player_out": {...},
      "delta_points": -0.4, "delta_win_prob": 0.028,
      "reason": "You're a 6-point underdog. His higher variance raises your win
                 probability by 2.8% despite costing 0.4 expected points.",
      "confidence": "high"
    }
  ],
  "locks_at": "2026-09-24T20:15:00Z",
  "debate": { /* ... */ }
}
```

**`delta_points` and `delta_win_prob` are both always shown.** A recommendation that
lowers expected points and raises win probability is correct and counterintuitive — the UI
has to make that legible or Chris won't trust it.

---

## Waiver

```
GET  /api/v1/leagues/{id}/waivers?week=           ranked available players
GET  /api/v1/leagues/{id}/waivers/{player_id}     detail + FAAB curve
```

```jsonc
{
  "week": 3, "faab_remaining": 73,
  "candidates": [
    {
      "player": {...},
      "delta_vorp": 18.0,
      "delta_playoff_odds": 0.031,
      "suggested_drop": {...},
      "faab": {
        "recommended_bid": 12,
        "bid_base": 16.43,
        "expected_bidders": 4,
        "win_curve": [{"bid": 8, "p_win": 0.31}, {"bid": 12, "p_win": 0.55},
                      {"bid": 19, "p_win": 0.80}]
      },
      "contested": {"trending_adds_24h": 14203, "rostered_pct": 22.4}
    }
  ]
}
```

`win_curve` drives a small chart — the decision is the curve, not the point estimate.

---

## Trade

```
GET  /api/v1/leagues/{id}/trade/targets           buy-low / sell-high from arbitrage
GET  /api/v1/leagues/{id}/trade/partners          rival needs analysis
POST /api/v1/leagues/{id}/trade/evaluate          evaluate a specific package
GET  /api/v1/leagues/{id}/trade/proposals         engine-generated ranked packages
```

`POST /trade/evaluate`:

```jsonc
// request
{"send": ["player_uuid"], "receive": ["player_uuid"], "partner_team_id": "uuid"}

// response
{
  "my_delta_value": 12.4,
  "their_delta_value": 3.1,           // must be ≥0 or they won't accept
  "my_delta_playoff_odds": 0.047,
  "verdict": "favorable",
  "arbitrage": [
    {"player": "...", "model_pctile": 0.81, "market_pctile": 0.62, "gap": 0.19,
     "signal": "buy_low"}
  ],
  "their_need_addressed": "Thin at RB after the Week 2 injury; bye collision in Week 9",
  "pitch": "Drafted message framing the trade from their perspective",
  "debate": { /* ... */ }
}
```

`pitch` is LLM-generated prose — the one place a model's output goes straight to the user
verbatim, because it's language, not a number.

---

## Debate results

```
GET  /api/v1/debates/{id}
GET  /api/v1/debates?module=&since=               for the backtest view
```

```jsonc
{
  "debate_id": "uuid",
  "consensus_score": 0.62,
  "disagreement_axis": "Whether the Week 7 bye outweighs the tier cliff",
  "recommendation_summary": "2-3 sentences",
  "analysts": [
    {"label": "Analyst A", "ranked": [...], "key_tension": "..."},
    {"label": "Analyst B", "ranked": [...], "key_tension": "..."}
  ],
  "refutations": [
    {"by": "Analyst A", "target_claim": "...", "verdict": "refute", "argument": "..."}
  ],
  "unresolved": ["..."],
  "_meta": {"latency_ms": 18400, "cost_usd": 0.071, "degraded": false}
}
```

⚠️ **Provider identity is not exposed to the frontend at all.** "Analyst A/B" only. This
keeps the UI honest — Chris shouldn't be anchoring on a vendor name — and matches the
blinding used in the protocol. Provider identity lives in `ai_debates` for analysis.

---

## Backtest / analysis

```
GET  /api/v1/backtest/recommendations?module=&season=
GET  /api/v1/backtest/debate-value
```

`debate-value` answers the question from [`AI_INTERACTIONS.md`](AI_INTERACTIONS.md) §9:
did following post-debate recommendations beat the raw engine? Returns hit rates by
module, by consensus bucket, and by provider, with total cost. **Build this before Week 8**
so there's a real checkpoint rather than a season-end postmortem.

---

## Frontend notes

- **TanStack Query** for all REST. The draft WebSocket writes directly into the query
  cache so REST and WS never disagree.
- **Never block a render on `debate`.** Render the recommendation card immediately; the
  debate section has its own loading state inside the card.
- Show `_meta.stale` prominently. A stale board during a live draft is a correctness
  problem, not a cosmetic one.
- The draft board is the one screen used under a 90-second clock. Design it for glanceable
  reading: three cards, the weighted score large, `components` available but not shouting,
  the consensus flag impossible to miss.
