# FantasyFootballHelper

A self-hosted fantasy football decision engine for the 2026 NFL season.

Most fantasy tools give you a ranking. This one computes the math that actually applies to
*your* roster in *your* league at *this* moment, then makes two frontier models argue about
the call and shows you where they disagree.

## What it does

| Module | What it answers |
|---|---|
| **Draft** | Live draft board with the top 3 picks, weighted. Uses VONA (Value Over Next Available) computed by Monte Carlo over ADP — the only draft metric that's correct for your specific pick slot. |
| **Lineup** | Weekly start/sit that maximizes **P(win)**, not expected points. When you're the underdog, higher variance is strictly better; most tools get this backwards. |
| **Waiver** | ΔVORP against *your* roster, contested-claim analysis, and FAAB bids sized as marginal-value budget allocation across the remaining season. |
| **Trade** | Ranked packages scored on the gap between a self-computed value curve and the market consensus — that difference is where buy-low targets live. |

## The AI layer

Claude and GPT each receive an identical evidence packet, argue independently, then are
forced to refute each other's reasoning. A blind judge (provider alternating) reconciles
and reports a consensus score. **High disagreement is surfaced, not hidden** — it means the
call is genuinely close and worth thirty seconds of your own attention.

Neither model ever produces a number. They rank, argue, and explain; the engine does math.

## Stack

Python 3.13 · FastAPI · Polars · DuckDB · Postgres 17 · Bun · React 19 · Tailwind

Deploys to a Raspberry Pi 5 k3s cluster (arm64) via ArgoCD, with Cilium Gateway-API for
ingress, nfs-synology for storage, and Vault + External Secrets Operator for credentials.

## Local development

```bash
docker compose up -d --wait            # postgres 17 (ffh + ffh_test) and redis 7
cd backend && uv sync && uv run pytest # backend tests (db-marked tests need compose up)
uv run uvicorn ffh.api.app:app --reload   # separate terminal
cd ../frontend && bun install && bun run dev   # http://localhost:3000, proxies /api → :8000
```

Copy `backend/.env.example` to `backend/.env` for local overrides (gitignored).
The Postgres init script (`docker/postgres/init/`) creates `ffh_test` only on a fresh volume — run `docker compose down -v` to re-initialize.
Frontend lint: `cd frontend && bun run lint` (oxlint).

## Docs

Design and specs live in [`docs/`](docs/). Start with
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

## Status

Pre-alpha, built against a hard deadline: the 2026 season opens September 9.

## License

Personal project. Third-party data is used under the terms of its respective sources —
see [`docs/DATA_SOURCES.md`](docs/DATA_SOURCES.md) for per-source licensing, including
non-commercial restrictions that apply.
