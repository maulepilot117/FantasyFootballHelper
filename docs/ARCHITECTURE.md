# Architecture

## Thesis

> **Deterministic math produces the numbers. The LLMs argue the judgment.**

An LLM asked to project a player's Week 6 points produces a plausible number that is worse
than a regression anchored on the Vegas team total — and that cannot be backtested. An LLM
asked *"the engine says these three picks are within 4 points; here's the injury news, the
bye structure, and what the manager two picks ahead needs — which do you take and why"* is
doing work no regression can do.

Every existing open-source LLM fantasy tool (~15 repos surveyed) is a thin wrapper that
dumps league JSON into a prompt. The quant-engine-plus-adversarial-LLM combination is the
reason this project exists. Do not erode it.

### The four features that justify building rather than buying

| Feature | Why it's differentiated |
|---|---|
| **VONA by ADP Monte Carlo** | VORP and VOLS are precomputable and therefore generic. VONA is the only draft metric correct for *this pick slot in this draft right now.* |
| **`P(win) = Φ(μ/σ)` start/sit** | Underdogs should *maximize* variance. Consumer tools optimize expected points and get this backwards roughly half the time. |
| **FAAB as budget allocation** | Bid against the opportunity cost of future claims, not standalone value. Correctly more aggressive in Week 12 than Week 2. |
| **Model-vs-market trade arbitrage** | Reporting market value is a lookup table. `model_value − market_value` is where buy-low targets live. |

Math for all four is specified in [`ENGINE.md`](ENGINE.md).

---

## Layers

```
┌─ L0  INGEST ──────────────────────────────────────────────────────┐
│  nflverse Parquet · platform adapters · ESPN odds/news            │
│  Open-Meteo · DynastyProcess · FantasyCalc                        │
│  → writes raw Parquet to the NFS lake, watermarks to Postgres     │
└───────────────────────────────────────────────────────────────────┘
                              ↓
┌─ L1  NORMALIZE ───────────────────────────────────────────────────┐
│  Postgres — leagues, rosters, players, transactions, drafts       │
│  ★ PLAYER ID CROSSWALK ★  (highest-risk component in the system)  │
└───────────────────────────────────────────────────────────────────┘
                              ↓
┌─ L2  FEATURES ────────────────────────────────────────────────────┐
│  DuckDB, read-only over Parquet on NFS                            │
│  usage shares · snap % · red zone · defensive EPA allowed · trends │
└───────────────────────────────────────────────────────────────────┘
                              ↓
┌─ L3  PROJECTIONS ─────────────────────────────────────────────────┐
│  Vegas-anchored → usage-distributed → context-adjusted            │
│  OUTPUT: a Gamma distribution per player per week, plus a         │
│          correlation matrix. Never a bare point estimate.         │
└───────────────────────────────────────────────────────────────────┘
                              ↓
┌─ L4  DECIDE ──────────────────────────────────────────────────────┐
│  draft (VORP/VONA/tiers) · lineup (win prob) · waiver (ΔVORP+FAAB)│
│  trade (model vs market) · season Monte Carlo                     │
│  OUTPUT: a ranked candidate set with numbers and a rationale trace │
└───────────────────────────────────────────────────────────────────┘
                              ↓
┌─ L5  DEBATE ──────────────────────────────────────────────────────┐
│  Claude ⚔ GPT → forced refutation → blind judge → consensus score │
│  ASYNC. Never blocks L4 output. See AI_INTERACTIONS.md            │
└───────────────────────────────────────────────────────────────────┘
                              ↓
┌─ L6  SERVE ───────────────────────────────────────────────────────┐
│  FastAPI + WebSocket → Bun/React                                  │
└───────────────────────────────────────────────────────────────────┘
```

**The critical property: L4 is complete and correct without L5.** If both providers are
down, the app degrades to a very good quantitative tool. If L5 ever becomes load-bearing
for correctness, the design has broken.

---

## Module boundaries

```
backend/src/ffh/
  adapters/      Platform clients behind ONE interface. Nothing outside this
                 package may know which platform is in use.
  ingest/        Fetch → validate → land as Parquet. Idempotent, watermarked.
                 No business logic.
  crosswalk/     Player identity resolution. See DATABASE.md §3.
  features/      DuckDB SQL over Parquet → feature tables. Pure functions of
                 the lake; safe to recompute from scratch at any time.
  projections/   Gamma params per player-week + correlation matrix.
  engine/        Pure math. NO I/O, NO network, NO LLM calls. Takes features
                 and league config in, returns scored candidates out.
                 Must be unit-testable with hand-computed fixtures.
  ai/            The debate layer. The ONLY package permitted to call an LLM.
  api/           FastAPI routes, WebSocket, auth. Thin — orchestration only.
  db/            SQLAlchemy models + Alembic migrations.
```

**`engine/` purity is enforced by test.** A test asserts the package imports no network or
LLM module. This is what keeps the system backtestable.

---

## The platform adapter interface

Chris is not locked into a league yet, so nothing above `adapters/` may assume a platform.
Build the Sleeper adapter first — it is the only one with no external approval dependency.

```python
class FantasyPlatformAdapter(Protocol):
    platform: Literal["sleeper", "espn", "yahoo"]

    async def get_league(self, league_id: str) -> League: ...
    async def get_scoring_settings(self, league_id: str) -> ScoringSettings: ...
    async def get_roster_settings(self, league_id: str) -> RosterSettings: ...
    async def get_teams(self, league_id: str) -> list[LeagueTeam]: ...
    async def get_rosters(self, league_id: str, week: int) -> list[Roster]: ...
    async def get_matchups(self, league_id: str, week: int) -> list[Matchup]: ...
    async def get_transactions(self, league_id: str, week: int) -> list[Transaction]: ...
    async def get_free_agents(self, league_id: str) -> list[PlayerRef]: ...

    # Draft
    async def get_draft(self, draft_id: str) -> Draft: ...
    async def get_draft_picks(self, draft_id: str) -> list[DraftPick]: ...
    async def draft_changed_since(self, draft_id: str, cursor: str | None) -> tuple[bool, str]:
        """Cheap change detector. Sleeper: last_picked epoch ms.
        ESPN: inProgress + count of picks with playerId != -1.
        Returns (changed, new_cursor). Called at 1-2s; must be cheap."""
```

**All three platforms are read-only.** Sleeper's API cannot write at all; Yahoo removed
write access in 2026. The app recommends; Chris executes in the platform UI. Do not design
around automated lineup submission.

**Scoring and roster settings are always fetched, never hardcoded.** PPR vs half vs
standard, superflex, TE premium, and starter counts all move the VORP baselines materially.
A hardcoded default is a bug even when it happens to be right.

---

## Latency budgets

These are hard requirements, not targets. The draft pick clock is 90 seconds.

| Path | Budget | Notes |
|---|---|---|
| Draft pick detected → recommendation on screen | **< 2s** | Engine only. Precompute everything possible between picks. |
| Draft change detection poll | 1–2s interval | Sleeper ceiling is 1000 req/min; this is ~0.1% of budget |
| LLM debate → streamed to UI | < 25s, async | Renders *into* an already-visible recommendation card |
| Weekly lineup batch | minutes, offline | Runs Tue/Wed, no interactive budget |
| Season Monte Carlo (10k sims) | < 30s | Chunked. See the OOM note below. |

**Precompute between picks.** After each pick lands there is typically 60–90s of dead time
while other managers deliberate. Use it: re-run VONA for the next several plausible board
states so the recommendation is already warm when it's Chris's turn.

---

## Storage decisions and their rationale

| Store | Role | Why |
|---|---|---|
| **Postgres 17** | System of record — leagues, rosters, drafts, transactions, recommendations, AI debate logs | The **only** one of the three candidates officially sanctioned on NFS. Requires `hard` mount client-side and `sync` export server-side. |
| **DuckDB** | Analytics, **read-only over Parquet** | Reading Parquet is plain file I/O with no lock contention, so many reader pods are safe. A `.duckdb` *database file* on NFS is not — never create one. |
| **Parquet on NFS** | The raw + feature lake | Cheap, columnar, recomputable from source at any time |
| **Redis** | Cache + draft pub/sub | Draft state fan-out to WebSocket clients |
| **SQLite** | ❌ Not used | SQLite's own corruption docs name NFS locking bugs explicitly |

TimescaleDB is deliberately **not** used. Volume is single-digit GB; plain Postgres with a
BRIN index on time columns handles it with zero added operational surface and no ARM
support question.

---

## Failure design

The system must degrade gracefully, in this order:

```
Full            engine + live platform data + LLM debate
  ↓ LLM provider down
Degraded-AI     engine + live data, debate panel shows "unavailable"
  ↓ platform endpoint breaks mid-draft
Degraded-Data   engine on last-known-good roster state, manual pick entry
  ↓ everything down
Floor           STATIC CHEAT SHEET EXPORT — a pre-generated ranked board
                with tiers and bye weeks, as PDF/HTML, no runtime required
```

**The static cheat sheet export is a required deliverable before draft day, not a
nice-to-have.** If the app dies at pick 1.03 there must still be something usable.

Every undocumented third-party endpoint (all Sleeper `api.sleeper.com/*` research
endpoints, all ESPN v3) is wrapped with cached last-known-good and a staleness indicator
surfaced in the UI.

---

## Known operational hazards

- **Player ID crosswalk gaps fail as *missing players*, not exceptions.** Explicit
  reconciliation tests with alerting are mandatory. See [`DATABASE.md`](DATABASE.md) §3.
- **Vectorized Monte Carlo is the realistic OOM cause** on 8–16GB Pi nodes — an
  `(n_sims × n_players)` array gets large fast. Sims must chunk over simulation batches.
- **k8s memory limits are decorative on Pi** unless `cgroup_memory=1 cgroup_enable=memory`
  is set in `/boot/firmware/cmdline.txt`. See [`DEPLOYMENT.md`](DEPLOYMENT.md).
- **Polars joins silently drop non-matching rows.** Every ingest/feature join asserts row
  counts or uses `validate=`.

---

## Related docs

[`DATA_SOURCES.md`](DATA_SOURCES.md) · [`DATABASE.md`](DATABASE.md) ·
[`ENGINE.md`](ENGINE.md) · [`AI_INTERACTIONS.md`](AI_INTERACTIONS.md) ·
[`API.md`](API.md) · [`DEPLOYMENT.md`](DEPLOYMENT.md) · [`WORKFLOW.md`](WORKFLOW.md) ·
[`ROADMAP.md`](ROADMAP.md)
