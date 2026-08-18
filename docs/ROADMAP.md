# Roadmap

**Read this before starting any work.** It defines the current phase and what is in scope.

```
Today          2026-08-15
Peak drafts    2026-09-04 → 09-07   ← the real deadline
Season opens   2026-09-09 (Wed, NE @ SEA)
Week 1 lock    2026-09-13 13:00 ET
```

**Current phase: Phase 0 — Foundation.**

---

## Scope rule

Everything before the draft is **draft-critical only**. Lineup, waiver, and trade have a
16-week runway; the draft has one shot. When scope and the date conflict, **cut scope.**

An ugly correct draft board beats a beautiful late one.

---

## Phase 0 — Foundation · Aug 15–22

Nothing works without this. No shortcuts here.

- [x] Repo scaffold — `backend/` (uv, FastAPI, ruff, pytest), `frontend/` (Bun, Vite,
      React 19, Tailwind v4), `deploy/` (kustomize base + overlays)
- [x] Dockerfiles — `python:3.13-slim-bookworm`, **not Alpine**
- [x] CI on `ubuntu-24.04-arm`: lint → test → build → push GHCR. **No QEMU.**
- [x] Postgres schema + initial Alembic migration ([`DATABASE.md`](DATABASE.md))
- [x] **★ Player ID crosswalk ★** — DynastyProcess ingest, resolution ladder, unmatched
      table, `ffh crosswalk report|seed|verify|resolve-unmatched`.
      **Highest-risk component; do it first.** Two of the four mandatory coverage tests
      ship with it; `test_crosswalk_covers_all_rostered_players` lands with the Sleeper
      adapter and `test_crosswalk_covers_top_300_adp` with ADP ingest (see
      [`DATABASE.md`](DATABASE.md) §3)
- [x] nflverse ingest → Parquet lake (players, stats_player_week, snap counts, depth
      charts, injuries, pbp). Release Parquet URLs read directly with httpx + Polars —
      **no `nflreadpy`, no `nfl_data_py`** (archived)
- [x] `nfldata/games.csv` ingest → schedule + Vegas lines + roof state
- [ ] Platform adapter interface + **Sleeper implementation** (no auth, no approval
      dependency — the one that can't block us)
- [ ] ADP + ECR ingest with `adp_stdev` ⚠️ required for VONA
- [x] Engine purity test (no network/LLM imports in `ffh.engine`)

**Exit criteria:** a league loads from Sleeper, every rostered player resolves through the
crosswalk with zero unmatched, and nflverse data queries from DuckDB.

---

## Phase 1 — Draft engine · Aug 23–30

- [ ] Season projections v0 — Vegas-anchored, usage-distributed, Gamma-fitted
      ([`ENGINE.md`](ENGINE.md) §4). Good enough for draft ranking; refined in Phase 4.
- [ ] VORP with configurable replacement baselines
- [ ] Tier GMM (BIC-selected k) + **tier-cliff distance**
- [ ] **★ VONA by ADP Monte Carlo ★** — the differentiating metric. Vectorized, <2s,
      warmed between picks.
- [ ] Roster-need weighting, bye collisions, handcuff logic
- [ ] Weighted scoring → **max 3 recommendations** with visible components
- [ ] Live draft poller — `last_picked` change detection, 1–2s, Redis pub/sub
- [ ] WebSocket with **state replay on reconnect**
- [ ] Draft board UI — the one screen that must be genuinely good
- [ ] **Static cheat sheet export** (PDF/HTML). Required, not optional.
- [ ] Unit tests against every worked example in `ENGINE.md`

**Exit criteria:** a full Sleeper mock draft runs end to end with live recommendations
inside the pick clock.

---

## Phase 2 — Debate layer · Aug 28–Sep 3

Overlaps Phase 1 deliberately — it's additive and never blocking.

- [ ] Provider clients (Anthropic + OpenAI) with strict structured outputs
- [ ] Evidence packet builder — byte-identical for both models (assert it)
- [ ] Round 1 independent → Round 2 refutation → Round 3 blind judge
- [ ] Bias controls: randomized order, anonymized labels, alternating judge
- [ ] Consensus score → UI flagging
- [ ] Async streaming into the recommendation card. **Never blocks.**
- [ ] Degradation: one provider down, both down, timeout, schema refusal
- [ ] `ai_debates` logging with cost and latency
- [ ] Skip-debate rule when the engine's margin is wide

**Exit criteria:** debate streams into a live mock draft without ever delaying a
recommendation, and killing both providers changes nothing about correctness.

---

## Phase 3 — Harden and deploy · Aug 31–Sep 7

- [ ] Deploy to k3s — ArgoCD app-of-apps, Vault/ESO secrets, Cilium HTTPRoute
- [ ] ⚠️ **WebSocket timeout on the Gateway listener** — drafts run 1–3 hours
- [ ] Cached last-known-good for every undocumented endpoint + staleness in the UI
- [ ] **Run 3+ full mock drafts on the deployed stack.** Tune weights against real boards.
- [ ] Manual pick entry (degraded mode)
- [ ] Verify the cheat sheet export works from a cold start
- [ ] ⚠️ **Freeze ArgoCD auto-sync on draft day**
- [ ] ESPN adapter *if* the league lands there

**Exit criteria:** Chris could draft with this tomorrow, and could still draft if the
cluster died.

---

## 🏈 DRAFT — Sept 4–7

---

## Phase 4 — In-season core · Sep 8–20

Week 1 locks **Sunday Sept 13, 1:00pm ET**. Lineup must work before then.

- [ ] Projection engine v1 — game script, opponent EPA, weather + crosswind, injury
      `p_active` × effectiveness
- [ ] **Walk-forward backtest against 2025.** This is the evidence for the props decision.
- [ ] Correlation matrix + Gaussian copula
- [ ] **Lineup module** — `P(win) = Φ(μ/σ)`, MILP over top-N candidate lineups
- [ ] Tue/Wed CronJobs on the **Batch API** (50% off)
- [ ] Lineup UI showing `delta_points` **and** `delta_win_prob` together
- [ ] **Waiver module** — ΔVORP on Chris's roster, FAAB allocation + first-price shading,
      win curve
- [ ] Contested-claim analysis from trending adds + ownership

**Exit criteria:** lineup recommendations by Thu Sept 10; waiver by Tue Sept 15.

---

## Phase 5 — Trade and simulation · Sep 21–Oct 15

- [ ] Season Monte Carlo — chunked, playoff odds, seed distribution
- [ ] All-play record + schedule luck decomposition
- [ ] Model value curve vs. FantasyCalc market → **arbitrage scoring**
- [ ] Rival roster need analysis (computed, not guessed)
- [ ] Trade package generation ranked by **Δ P(playoffs)**
- [ ] LLM-drafted pitch messages — the one place model prose ships verbatim
- [ ] Trade UI

---

## Phase 6 — Evaluate and refine · Oct 15 → end of season

- [ ] **`/backtest/debate-value` — build before Week 8.** Did the debate beat the engine?
      A real checkpoint, not a season-end postmortem.
- [ ] Decide the **$99/mo player props** question with backtest evidence, not vibes
- [ ] Tune weights against realized outcomes
- [ ] Yahoo adapter if ever needed (submit the API application early — human review, no SLA)
- [ ] Observability: crosswalk alerting, LLM spend tracking, ingest freshness

---

## Deferred — explicitly not doing these

| | Why |
|---|---|
| Multi-user / hosted version | Sleeper and Open-Meteo free tiers are **non-commercial**. Revisit `DATA_SOURCES.md` licensing first. |
| Automated lineup submission | All three platforms are read-only. Sleeper's API cannot write; Yahoo removed writes in 2026. |
| Local LLM on the Pi cluster | ~54 min per call for an 8B model on a 20k-token prompt. Not viable. Prompt *processing* is the killer, not generation. |
| DVOA / PFF | Paywalled. EPA metrics from pbp are competitive and transparent. |
| NFL.com adapter | NFL exited season-long fantasy in July 2026. |
| TimescaleDB | Single-digit GB. Plain Postgres + BRIN. |
| DFS | Different problem. Not the goal. |

---

## Progress log

Append a line per completed phase — date, what shipped, what changed in the plan.

<!-- 2026-08-15 · Phase 0 started. Docs and specs written. -->
