# FantasyFootballHelper

Self-hosted fantasy football decision engine for the 2026 NFL season. Four modules:
draft assistant, weekly lineup, waiver wire, trades. Deploys to a Raspberry Pi 5 k3s
homelab via ArgoCD.

**Core thesis: deterministic math produces the numbers, LLMs argue the judgment.**

---

## Hard rules — never violate these

1. **No LLM ever emits a projection, score, or ranking number.** LLMs rank pre-computed
   candidates, argue, critique, and explain. All numbers come from `ffh.engine` and
   `ffh.projections`. Violating this makes the system unbacktestable.
2. **Projections are distributions, not point estimates.** Every projection carries Gamma
   params. Downstream code that needs only the mean still receives the distribution.
3. **Never `import pandas` in new code.** This project is Polars-native. `nfl_data_py` is
   archived and `nflreadpy` is deliberately **not** a dependency: nflverse assets are read
   straight from their release Parquet/CSV URLs with httpx + Polars (`ffh.ingest.nflverse`,
   `DATA_SOURCES.md` §1). Both imports are banned by ruff.
4. **Never use Alpine base images for Python services.** No musl aarch64 wheels for
   scikit-learn / xgboost / lightgbm / duckdb. Use `python:3.13-slim-bookworm`.
5. **Never put a SQLite or `.duckdb` file on NFS.** Postgres is the system of record;
   DuckDB is read-only over Parquet.
6. **All secrets come from Vault via ESO.** Never a literal key in code, manifests, or
   `.env` committed to git.
7. **The engine result must render before the LLM debate returns.** Debate streams in
   async and never blocks a recommendation. Draft pick clocks are 90 seconds.
8. **Every recommendation is logged with its inputs and outcome** (`recommendations`,
   `ai_debates` tables). Backtestability is a feature, not an afterthought.

---

## Stack

**Backend** Python 3.13 · `uv` · FastAPI · Polars · DuckDB · SQLAlchemy 2 + Alembic ·
PuLP · scikit-learn · Pydantic v2 · pytest · ruff
**Frontend** Bun · React 19 · Vite · TypeScript · Tailwind v4 · TanStack Query · shadcn/ui
**Data** Postgres 17 (OLTP) · DuckDB over Parquet on NFS (analytics) · Redis (cache + draft pub/sub)
**Deploy** k3s on Pi 5 (arm64 only) · Cilium Gateway-API · nfs-synology CSI · Vault + ESO · ArgoCD

---

## Repo map

```
backend/src/ffh/
  adapters/     platform clients (sleeper, espn, yahoo) behind one interface
  ingest/       nflverse, odds, weather, adp, market values
  crosswalk/    player ID mapping — the highest-risk component
  features/     DuckDB feature builds over Parquet
  projections/  the projection engine (Gamma + copula)
  engine/       vorp, vona, tiers, lineup, waiver, trade, season sim
  ai/           debate layer (providers, prompts, schemas, judge)
  api/          FastAPI routes + WebSocket
  db/           SQLAlchemy models, Alembic migrations
frontend/       Bun + React
deploy/         kustomize base + overlays + ArgoCD Applications
docs/           see index below
```

---

## Docs — read the relevant one BEFORE writing code

| Read this | Before doing this |
|---|---|
| `docs/ROADMAP.md` | **Anything.** Tells you the current phase and what's in scope. |
| `docs/ARCHITECTURE.md` | Any structural change or new module |
| `docs/DATA_SOURCES.md` | Any ingest work. Contains verified URLs and gotchas that **contradict your training data.** |
| `docs/DATABASE.md` | Any schema change, migration, or query |
| `docs/ENGINE.md` | Any math — VORP, VONA, tiers, projections, win prob, FAAB, trade value |
| `docs/AI_INTERACTIONS.md` | Any LLM call, prompt, or schema change |
| `docs/API.md` | Any endpoint or frontend/backend contract change |
| `docs/DEPLOYMENT.md` | Any manifest, Dockerfile, or CI change |
| `docs/WORKFLOW.md` | Committing, opening a PR, or requesting Codex review |

---

## Workflow

**Claude Code is the primary builder. Codex writes code and performs adversarial review.**

- Branch per unit of work. Never commit to `main` directly.
- **No git worktrees.** Work on feature branches directly in the repo root
  (`git switch -c feat/...`). Never use `EnterWorktree` / `git worktree add` — the
  worktree-isolation guard blocks post-merge cleanup and buys nothing for a solo repo.
  After a PR merges: `git switch main && git pull --ff-only && git branch -d <branch>`.
- Every PR needs a Codex adversarial review pass before merge — see `docs/WORKFLOW.md`.
- Push to GitHub. ArgoCD reconciles `deploy/` to the homelab; never `kubectl apply` by hand.
- Definition of done: tests pass, `ruff check` clean, docs updated, Codex review addressed.

---

## Deadline

2026 NFL season opens **Wed Sept 9**. Peak fantasy draft weekend is **Sept 4–7**.
The draft module ships first. Everything else is phase 2. When trading off scope against
the draft date, **cut scope**.
