# PR ① `chore/scaffold` — Repo Scaffold, CI, Dockerfiles Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up `backend/`, `frontend/`, `deploy/`, Docker Compose, Dockerfiles, and CI so every later PR has a lint → test → build pipeline and the engine-purity guard from day one.

**Architecture:** A uv-managed Python 3.13 package `ffh` under `backend/src/ffh/` with all module packages present (most empty), a FastAPI app exposing only `GET /health`, a typer CLI stub, and the engine-purity test. A Bun/Vite/React 19/Tailwind v4 frontend with one health page and `bun test` wired. Kustomize/ArgoCD skeleton under `deploy/`. Compose for Postgres 17 + Redis 7. GitHub Actions on `ubuntu-24.04-arm` running lint → pytest (Postgres service) → bun test → arm64 image build+push to GHCR on `main`.

**Tech Stack:** uv 0.11.x · Python 3.13 · FastAPI 0.141.1 · pydantic-settings 2.15.0 · typer 0.27.1 · pytest 9.1.1 · ruff 0.16.2 · Bun 1.3 · Vite 8.2.1 · React 19 · Tailwind 4.3.3 · Docker · GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-08-15-phase0-foundation-design.md` (§1, §7, cross-cutting rules)

## Global Constraints

- Python base image `python:3.13.14-slim-bookworm` (Docker Hub `last_updated` 2026-08-05 — verified 2026-08-15). Never Alpine.
- Frontend build stage `oven/bun:1-debian`; static serve `nginx:1.28.0-bookworm` (2025-12-09).
- Compose: `postgres:17.10-bookworm` (2026-08-05), `redis:7.4.10-bookworm` (2026-08-05).
- Every PyPI/npm/Action pin below was checked against the 7-day cooldown on 2026-08-15. If you install anything not listed here, check its publish date first (`https://pypi.org/pypi/<pkg>/json`, `npm view <pkg> time`).
- Actions pinned to commit SHAs (resolved 2026-08-15 with `gh api repos/<r>/commits/<tag>`).
- Never `import pandas`, `nfl_data_py`, or `nflreadpy` — ruff bans them.
- No SQLite / `.duckdb` file anywhere.
- No secrets in the repo; `backend/.env` is gitignored.
- Branch: `chore/scaffold`. Conventional commits.
- Windows dev box: run shell steps in Git Bash or PowerShell as noted; paths use forward slashes.

---

## Pre-flight (once, by Chris or the executor)

- [ ] Start Docker Desktop and confirm `docker info` prints a server version.
- [ ] `uv python install 3.13` — confirm `uv python find 3.13` prints a path.
- [ ] `git checkout -b chore/scaffold` from `main` (after the docs branch is merged, or from `main` directly — the docs branch only adds files under `docs/superpowers/`).

---

### Task 1: Backend package skeleton, config, ruff, pytest

**Files:**
- Create: `backend/pyproject.toml`
- Create: `backend/.python-version`
- Create: `backend/src/ffh/__init__.py`
- Create: `backend/src/ffh/{adapters,ingest,crosswalk,features,projections,engine,ai,api,db}/__init__.py`
- Create: `backend/src/ffh/config.py`
- Create: `backend/tests/__init__.py`
- Create: `backend/tests/conftest.py`
- Create: `backend/tests/test_config.py`
- Create: `backend/.env.example`
- Modify: `.gitignore` (add `backend/.env`, `backend/data/`)

**Interfaces:**
- Produces: `ffh.config.Settings` (pydantic-settings) with fields `database_url: str`, `test_database_url: str`, `redis_url: str`, `lake_root: Path`, `sleeper_base_url: str`, `log_level: str`, `season: int`; and `ffh.config.get_settings() -> Settings` (lru_cached). Env prefix `FFH_`.

- [ ] **Step 1: Write `backend/pyproject.toml`**

```toml
[project]
name = "ffh"
version = "0.1.0"
description = "FantasyFootballHelper backend — deterministic engine + LLM debate"
requires-python = ">=3.13,<3.14"
dependencies = [
    "fastapi==0.141.1",
    "uvicorn[standard]==0.52.1",
    "polars==1.43.2",
    "duckdb==1.5.5",
    "sqlalchemy==2.0.51",
    "alembic==1.19.0",
    "psycopg[binary]==3.3.4",
    "pydantic==2.13.4",
    "pydantic-settings==2.15.0",
    "httpx==0.28.1",
    "tenacity==9.1.4",
    "typer==0.27.1",
    "redis==8.1.0",
    "rapidfuzz==3.14.5",
    "structlog==26.1.0",
]

[project.scripts]
ffh = "ffh.cli:app"

[dependency-groups]
dev = [
    "pytest==9.1.1",
    "pytest-asyncio==1.4.0",
    "respx==0.23.1",
    "ruff==0.16.2",
]

[build-system]
requires = ["hatchling==1.31.0"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/ffh"]

[tool.ruff]
line-length = 100
target-version = "py313"
src = ["src", "tests"]

[tool.ruff.lint]
select = ["E", "F", "I", "B", "UP", "TID", "RUF"]
ignore = ["E501"]

[tool.ruff.lint.flake8-tidy-imports.banned-api]
"pandas".msg = "Polars-native project. Never import pandas (CLAUDE.md rule 3)."
"nfl_data_py".msg = "Archived. Read nflverse Parquet URLs directly (DATA_SOURCES.md §1)."
"nflreadpy".msg = "Not a dependency by decision — read nflverse Parquet URLs directly."

[tool.ruff.lint.isort]
known-first-party = ["ffh"]

[tool.pytest.ini_options]
testpaths = ["tests"]
asyncio_mode = "auto"
markers = [
    "db: requires a reachable Postgres at FFH_TEST_DATABASE_URL",
    "network: hits a live third-party API; never run in CI",
]
addopts = "-m 'not network' --strict-markers"
```

- [ ] **Step 2: Write `backend/.python-version`**

```
3.13
```

- [ ] **Step 3: Create the package tree**

Git Bash:
```bash
mkdir -p backend/src/ffh/{adapters,ingest,crosswalk,features,projections,engine,ai,api,db} backend/tests
for d in "" adapters ingest crosswalk features projections engine ai api db; do : > "backend/src/ffh/${d:+$d/}__init__.py"; done
: > backend/tests/__init__.py
```

Then write `backend/src/ffh/__init__.py`:
```python
"""FantasyFootballHelper backend."""

__version__ = "0.1.0"
```

And `backend/src/ffh/engine/__init__.py`:
```python
"""Pure math. NO I/O, NO network, NO LLM calls. Enforced by tests/test_engine_purity.py."""
```

- [ ] **Step 4: Write the failing config test `backend/tests/test_config.py`**

```python
from pathlib import Path

from ffh.config import Settings, get_settings


def test_settings_read_env_with_prefix(monkeypatch):
    monkeypatch.setenv("FFH_DATABASE_URL", "postgresql+psycopg://u:p@h:5432/db")
    monkeypatch.setenv("FFH_LAKE_ROOT", "/tmp/lake")
    monkeypatch.setenv("FFH_SEASON", "2026")
    s = Settings()
    assert s.database_url == "postgresql+psycopg://u:p@h:5432/db"
    assert s.lake_root == Path("/tmp/lake")
    assert s.season == 2026


def test_settings_defaults_are_local_dev():
    s = Settings(_env_file=None)
    assert s.database_url.startswith("postgresql+psycopg://ffh:ffh@localhost:5432/ffh")
    assert s.test_database_url.endswith("/ffh_test")
    assert s.redis_url == "redis://localhost:6379/0"
    assert s.sleeper_base_url == "https://api.sleeper.app/v1"
    assert s.season == 2026


def test_get_settings_is_cached():
    get_settings.cache_clear()
    assert get_settings() is get_settings()
```

- [ ] **Step 5: Run it to verify it fails**

Run (from `backend/`): `uv sync && uv run pytest tests/test_config.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ffh.config'`.

- [ ] **Step 6: Write `backend/src/ffh/config.py`**

```python
from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration. Every value comes from the environment (prefix FFH_).

    Secrets are injected by ESO in-cluster; locally they come from backend/.env (gitignored).
    """

    model_config = SettingsConfigDict(env_prefix="FFH_", env_file=".env", extra="ignore")

    database_url: str = "postgresql+psycopg://ffh:ffh@localhost:5432/ffh"
    test_database_url: str = "postgresql+psycopg://ffh:ffh@localhost:5432/ffh_test"
    redis_url: str = "redis://localhost:6379/0"
    lake_root: Path = Path("data/lake")
    sleeper_base_url: str = "https://api.sleeper.app/v1"
    log_level: str = "INFO"
    season: int = 2026


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
```

- [ ] **Step 7: Write `backend/tests/conftest.py`**

```python
import pytest


@pytest.fixture(autouse=True)
def _clear_settings_cache():
    from ffh.config import get_settings

    get_settings.cache_clear()
    yield
    get_settings.cache_clear()
```

- [ ] **Step 8: Write `backend/.env.example` and update `.gitignore`**

`backend/.env.example`:
```
# Copy to backend/.env (gitignored). Local dev only — cluster values come from Vault via ESO.
FFH_DATABASE_URL=postgresql+psycopg://ffh:ffh@localhost:5432/ffh
FFH_TEST_DATABASE_URL=postgresql+psycopg://ffh:ffh@localhost:5432/ffh_test
FFH_REDIS_URL=redis://localhost:6379/0
FFH_LAKE_ROOT=data/lake
FFH_SEASON=2026
# Filled in for PR ⑤:
# FFH_SLEEPER_MOCK_LEAGUE_ID=
```

Append to `.gitignore` under `# Env & secrets`:
```
backend/.env
```
and under `# Data lake / local artifacts`:
```
backend/data/
```

- [ ] **Step 9: Run tests + lint**

Run: `uv run pytest -v && uv run ruff check . && uv run ruff format --check .`
Expected: 3 passed; ruff clean (run `uv run ruff format .` first if format complains).

- [ ] **Step 10: Commit**

```bash
git add backend/pyproject.toml backend/.python-version backend/uv.lock backend/src backend/tests backend/.env.example .gitignore
git commit -m "chore(backend): uv project skeleton, settings, ruff + pytest config"
```

---

### Task 2: Engine purity test

**Files:**
- Create: `backend/tests/test_engine_purity.py`

**Interfaces:**
- Produces: the guard every later engine PR must pass. `FORBIDDEN_MODULES` is the authoritative list.

- [ ] **Step 1: Write the test**

```python
"""ffh.engine must be pure math: no I/O, network, DB, or LLM imports (ARCHITECTURE.md)."""

import ast
import importlib
import pkgutil
import sys
from pathlib import Path

import ffh.engine

FORBIDDEN_MODULES = {
    "httpx", "requests", "aiohttp", "urllib3", "urllib.request",
    "anthropic", "openai",
    "sqlalchemy", "psycopg", "asyncpg", "alembic",
    "redis", "duckdb",
    "ffh.ai", "ffh.adapters", "ffh.db", "ffh.ingest", "ffh.api",
}

ENGINE_DIR = Path(ffh.engine.__file__).parent


def _imported_names(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
    return names


def _is_forbidden(name: str) -> bool:
    return any(name == f or name.startswith(f + ".") for f in FORBIDDEN_MODULES)


def test_engine_sources_import_nothing_impure():
    offenders = {}
    for py in ENGINE_DIR.rglob("*.py"):
        bad = {n for n in _imported_names(py) if _is_forbidden(n)}
        if bad:
            offenders[str(py.relative_to(ENGINE_DIR))] = sorted(bad)
    assert not offenders, f"Impure imports in ffh.engine: {offenders}"


def test_importing_engine_loads_no_forbidden_module():
    before = set(sys.modules)
    for mod in pkgutil.walk_packages(ffh.engine.__path__, prefix="ffh.engine."):
        importlib.import_module(mod.name)
    newly = set(sys.modules) - before
    leaked = sorted(n for n in newly if _is_forbidden(n))
    assert not leaked, f"Importing ffh.engine loaded forbidden modules: {leaked}"
```

- [ ] **Step 2: Run it — it must pass on the empty package**

Run: `uv run pytest tests/test_engine_purity.py -v`
Expected: 2 passed.

- [ ] **Step 3: Prove it bites — temporarily add `import httpx` to `backend/src/ffh/engine/__init__.py`, rerun, expect FAIL on both tests, then revert the line.**

- [ ] **Step 4: Commit**

```bash
git add backend/tests/test_engine_purity.py
git commit -m "test(engine): purity guard — no I/O, network, DB, or LLM imports"
```

---

### Task 3: FastAPI app with `/health`

**Files:**
- Create: `backend/src/ffh/api/app.py`
- Create: `backend/tests/api/__init__.py`
- Create: `backend/tests/api/test_health.py`

**Interfaces:**
- Produces: `ffh.api.app.app: FastAPI`; `GET /health -> {"status": "ok", "version": str, "season": int}`.

- [ ] **Step 1: Write the failing test**

```python
from fastapi.testclient import TestClient

from ffh import __version__
from ffh.api.app import app


def test_health_reports_ok_version_and_season(monkeypatch):
    monkeypatch.setenv("FFH_SEASON", "2026")
    with TestClient(app) as client:
        r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok", "version": __version__, "season": 2026}
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/api/test_health.py -v`
Expected: FAIL `ModuleNotFoundError: No module named 'ffh.api.app'`.

- [ ] **Step 3: Write `backend/src/ffh/api/app.py`**

```python
from fastapi import FastAPI

from ffh import __version__
from ffh.config import get_settings

app = FastAPI(title="FantasyFootballHelper", version=__version__)


@app.get("/health")
def health() -> dict[str, object]:
    return {"status": "ok", "version": __version__, "season": get_settings().season}
```

- [ ] **Step 4: Run to verify it passes**

Run: `uv run pytest tests/api -v`  Expected: 1 passed.

- [ ] **Step 5: Smoke the server once**

Run: `uv run uvicorn ffh.api.app:app --port 8000` then in another shell `curl -s localhost:8000/health`. Expected JSON as above. Stop the server.

- [ ] **Step 6: Commit**

```bash
git add backend/src/ffh/api/app.py backend/tests/api
git commit -m "feat(api): FastAPI app with GET /health"
```

---

### Task 4: `ffh` CLI stub

**Files:**
- Create: `backend/src/ffh/cli.py`
- Create: `backend/tests/test_cli.py`

**Interfaces:**
- Produces: `ffh.cli.app: typer.Typer` with sub-apps `ingest_app`, `league_app`, `crosswalk_app` (empty; later PRs register commands on them) and command `ffh version`.

- [ ] **Step 1: Write the failing test**

```python
from typer.testing import CliRunner

from ffh import __version__
from ffh.cli import app

runner = CliRunner()


def test_version_prints_package_version():
    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0
    assert __version__ in result.stdout


def test_subcommand_groups_exist():
    for group in ("ingest", "league", "crosswalk"):
        result = runner.invoke(app, [group, "--help"])
        assert result.exit_code == 0, result.stdout
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/test_cli.py -v`  Expected: FAIL `No module named 'ffh.cli'`.

- [ ] **Step 3: Write `backend/src/ffh/cli.py`**

```python
import typer

from ffh import __version__

app = typer.Typer(no_args_is_help=True, help="FantasyFootballHelper CLI.")
ingest_app = typer.Typer(no_args_is_help=True, help="Run ingest jobs.")
league_app = typer.Typer(no_args_is_help=True, help="Load leagues from platforms.")
crosswalk_app = typer.Typer(no_args_is_help=True, help="Player ID crosswalk tools.")

app.add_typer(ingest_app, name="ingest")
app.add_typer(league_app, name="league")
app.add_typer(crosswalk_app, name="crosswalk")


@app.command()
def version() -> None:
    """Print the ffh version."""
    typer.echo(__version__)


# Placeholder commands so `--help` works on empty groups; later PRs replace these.
@ingest_app.command("list")
def ingest_list() -> None:
    """List registered ingest jobs (none yet)."""
    typer.echo("no ingest jobs registered")


@league_app.command("platforms")
def league_platforms() -> None:
    """List supported platforms."""
    typer.echo("sleeper")


@crosswalk_app.command("report")
def crosswalk_report() -> None:
    """Crosswalk coverage report (implemented in PR ④)."""
    typer.echo("crosswalk not yet implemented")
```

- [ ] **Step 4: Run to verify it passes** — `uv run pytest tests/test_cli.py -v` → 2 passed. Also `uv run ffh version` prints `0.1.0`.

- [ ] **Step 5: Commit**

```bash
git add backend/src/ffh/cli.py backend/tests/test_cli.py
git commit -m "feat(cli): typer entrypoint with ingest/league/crosswalk groups"
```

---

### Task 5: Docker Compose for Postgres + Redis, DB smoke test

**Files:**
- Create: `docker-compose.yml`
- Create: `docker/postgres/init/01-test-db.sql`
- Create: `backend/tests/db/__init__.py`
- Create: `backend/tests/db/test_connection.py`

**Interfaces:**
- Produces: local Postgres at `localhost:5432` with DBs `ffh` and `ffh_test` (user/pass `ffh`/`ffh`), Redis at `localhost:6379`. Marker `db` for tests needing Postgres.

- [ ] **Step 1: Write `docker-compose.yml`**

```yaml
services:
  postgres:
    image: postgres:17.10-bookworm
    environment:
      POSTGRES_USER: ffh
      POSTGRES_PASSWORD: ffh
      POSTGRES_DB: ffh
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data
      - ./docker/postgres/init:/docker-entrypoint-initdb.d:ro
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ffh -d ffh"]
      interval: 5s
      timeout: 3s
      retries: 10

  redis:
    image: redis:7.4.10-bookworm
    ports:
      - "6379:6379"
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 10

volumes:
  pgdata:
```

- [ ] **Step 2: Write `docker/postgres/init/01-test-db.sql`**

```sql
CREATE DATABASE ffh_test OWNER ffh;
```

- [ ] **Step 3: Start it** — `docker compose up -d --wait` from repo root. Expected: both services healthy.

- [ ] **Step 4: Write the failing DB smoke test `backend/tests/db/test_connection.py`**

```python
import pytest
from sqlalchemy import create_engine, text

from ffh.config import get_settings

pytestmark = pytest.mark.db


def test_test_database_is_reachable_and_is_postgres_17():
    engine = create_engine(get_settings().test_database_url)
    with engine.connect() as conn:
        version = conn.execute(text("SHOW server_version_num")).scalar_one()
    assert int(version) >= 170000
```

- [ ] **Step 5: Run it** — `uv run pytest -m db -v` → 1 passed (against compose). If Docker is down it must FAIL, not skip: DB tests are load-bearing.

- [ ] **Step 6: Commit**

```bash
git add docker-compose.yml docker/postgres/init/01-test-db.sql backend/tests/db
git commit -m "chore: docker compose for postgres 17 + redis 7 with ffh_test db"
```

---

### Task 6: Backend Dockerfile

**Files:**
- Create: `backend/Dockerfile`
- Create: `backend/.dockerignore`

- [ ] **Step 1: Write `backend/.dockerignore`**

```
.venv
.env
data
.pytest_cache
.ruff_cache
__pycache__
tests
```

- [ ] **Step 2: Write `backend/Dockerfile`**

```dockerfile
# syntax=docker/dockerfile:1
# python:3.13.14-slim-bookworm — NEVER Alpine (no musl aarch64 wheels for sklearn/duckdb).
FROM python:3.13.14-slim-bookworm AS builder
COPY --from=ghcr.io/astral-sh/uv:0.11.7 /uv /uvx /bin/
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

FROM python:3.13.14-slim-bookworm
RUN useradd --create-home --uid 10001 ffh
WORKDIR /app
COPY --from=builder --chown=ffh:ffh /app /app
ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1
USER ffh
EXPOSE 8000
CMD ["uvicorn", "ffh.api.app:app", "--host", "0.0.0.0", "--port", "8000"]
```

- [ ] **Step 3: Build locally (x86 is fine for a smoke; CI builds arm64)**

Run from `backend/`: `docker build -t ffh-api:dev .` then `docker run --rm -p 8001:8000 ffh-api:dev` and `curl -s localhost:8001/health`. Expected: health JSON.

- [ ] **Step 4: Commit**

```bash
git add backend/Dockerfile backend/.dockerignore
git commit -m "chore(backend): multi-stage Dockerfile on python:3.13-slim-bookworm with uv"
```

---

### Task 7: Frontend scaffold (Bun + Vite + React 19 + Tailwind v4) with health page and `bun test`

**Files:**
- Create: `frontend/` via `bun create vite`
- Modify: `frontend/package.json`, `frontend/vite.config.ts`, `frontend/src/index.css`, `frontend/src/App.tsx`, `frontend/src/main.tsx`
- Create: `frontend/src/api/health.ts`, `frontend/src/api/health.test.ts`
- Delete: Vite template boilerplate (`App.css`, `assets/react.svg`, `public/vite.svg`)

**Interfaces:**
- Produces: `fetchHealth(fetchImpl?): Promise<Health>` where `Health = {status: string; version: string; season: number}`; dev proxy `/api` → `http://localhost:8000`.

- [ ] **Step 1: Scaffold**

From repo root: `bun create vite frontend --template react-ts` (non-interactive when the template flag is given; if it prompts, choose React → TypeScript). Then `cd frontend && bun install`.

- [ ] **Step 2: Verify the pinned versions the template wrote (7-day cooldown)**

Run: `node -e 'const p=require("./package.json");console.log({...p.dependencies,...p.devDependencies})'` and for each package `npm view <pkg>@<ver> time --json | tail -3`. Known-good on 2026-08-15: `vite 8.2.1` (2026-08-06), `react`/`react-dom 19.0.8` (2026-07-21), `typescript 7.0.2` (2026-07-08), `@vitejs/plugin-react 6.0.5` (2026-07-30), `@types/react 19.2.18`, `@types/react-dom 19.2.4` (2026-07-30). Downgrade any pin published < 7 days ago to the newest compliant version with `bun add <pkg>@<ver>`.

- [ ] **Step 3: Add Tailwind v4** — `bun add tailwindcss@4.3.3 @tailwindcss/vite@4.3.3` (both 2026-07-16).

- [ ] **Step 4: Write `frontend/vite.config.ts`**

```ts
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    port: 3000,
    proxy: { "/api": { target: "http://localhost:8000", rewrite: (p) => p.replace(/^\/api/, "") } },
  },
});
```

- [ ] **Step 5: Replace `frontend/src/index.css`**

```css
@import "tailwindcss";
```

- [ ] **Step 6: Write the failing test `frontend/src/api/health.test.ts`**

```ts
import { expect, test } from "bun:test";
import { fetchHealth } from "./health";

test("fetchHealth parses the backend health payload", async () => {
  const fake = (async () =>
    new Response(JSON.stringify({ status: "ok", version: "0.1.0", season: 2026 }), {
      status: 200,
      headers: { "content-type": "application/json" },
    })) as unknown as typeof fetch;
  const h = await fetchHealth(fake);
  expect(h).toEqual({ status: "ok", version: "0.1.0", season: 2026 });
});

test("fetchHealth throws on non-2xx", async () => {
  const fake = (async () => new Response("nope", { status: 503 })) as unknown as typeof fetch;
  await expect(fetchHealth(fake)).rejects.toThrow("health 503");
});
```

- [ ] **Step 7: Run to verify it fails** — `bun test` → fails: cannot resolve `./health`.

- [ ] **Step 8: Write `frontend/src/api/health.ts`**

```ts
export type Health = { status: string; version: string; season: number };

export async function fetchHealth(fetchImpl: typeof fetch = fetch): Promise<Health> {
  const res = await fetchImpl("/api/health");
  if (!res.ok) throw new Error(`health ${res.status}`);
  return (await res.json()) as Health;
}
```

- [ ] **Step 9: Run to verify it passes** — `bun test` → 2 pass.

- [ ] **Step 10: Replace `frontend/src/App.tsx` with the health page**

```tsx
import { useEffect, useState } from "react";
import { fetchHealth, type Health } from "./api/health";

export default function App() {
  const [health, setHealth] = useState<Health | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetchHealth().then(setHealth).catch((e: Error) => setError(e.message));
  }, []);

  return (
    <main className="min-h-screen bg-neutral-950 text-neutral-100 p-8 font-sans">
      <h1 className="text-2xl font-semibold">FantasyFootballHelper</h1>
      <p className="mt-2 text-neutral-400">Phase 0 — foundation</p>
      <section className="mt-6 rounded-lg border border-neutral-800 p-4">
        <h2 className="text-sm uppercase tracking-wide text-neutral-500">API health</h2>
        {health && (
          <p className="mt-2">
            {health.status} · v{health.version} · season {health.season}
          </p>
        )}
        {error && <p className="mt-2 text-red-400">unreachable: {error}</p>}
        {!health && !error && <p className="mt-2 text-neutral-500">checking…</p>}
      </section>
    </main>
  );
}
```

Ensure `frontend/src/main.tsx` imports `./index.css` and renders `<App />` (template default does; remove the `App.css` import and delete `App.css`, `assets/react.svg`, `public/vite.svg`).

- [ ] **Step 11: Type-check and build** — `bun run build` → succeeds. Optionally `bun run dev` with the API up and confirm the health line renders at `http://localhost:3000`.

- [ ] **Step 12: Commit**

```bash
git add frontend
git commit -m "chore(frontend): bun + vite + react 19 + tailwind v4 scaffold with health page"
```

---

### Task 8: Frontend Dockerfile

**Files:**
- Create: `frontend/Dockerfile`, `frontend/nginx.conf`, `frontend/.dockerignore`

- [ ] **Step 1: Write `frontend/.dockerignore`**

```
node_modules
dist
```

- [ ] **Step 2: Write `frontend/nginx.conf`**

```nginx
server {
    listen 3000;
    root /usr/share/nginx/html;
    index index.html;
    location / { try_files $uri $uri/ /index.html; }
}
```

- [ ] **Step 3: Write `frontend/Dockerfile`**

```dockerfile
# syntax=docker/dockerfile:1
FROM oven/bun:1-debian AS build
WORKDIR /app
COPY package.json bun.lock ./
# Lockfiles generated on x86 can poison arm installs (DEPLOYMENT.md §6). If --frozen-lockfile
# fails on the arm runner, regenerate the lockfile there and commit it.
RUN bun install --frozen-lockfile
COPY . .
RUN bun run build

FROM nginx:1.28.0-bookworm
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 3000
```

(Build context is `frontend/` both locally and in CI.)

- [ ] **Step 4: Build and smoke** — from `frontend/`: `docker build -t ffh-frontend:dev .` then `docker run --rm -p 3001:3000 ffh-frontend:dev` and `curl -s localhost:3001 | head -5` shows the HTML.

- [ ] **Step 5: Commit**

```bash
git add frontend/Dockerfile frontend/nginx.conf frontend/.dockerignore
git commit -m "chore(frontend): Dockerfile — bun build stage, nginx bookworm serve"
```

---

### Task 9: `deploy/` skeleton

**Files:**
- Create: `deploy/base/kustomization.yaml`, `deploy/base/namespace.yaml`
- Create: `deploy/overlays/homelab/kustomization.yaml`
- Create: `deploy/argocd/ffh.yaml`
- Create: `deploy/README.md`

- [ ] **Step 1: Write the files**

`deploy/base/namespace.yaml`:
```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: ffh
```

`deploy/base/kustomization.yaml`:
```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: ffh
resources:
  - namespace.yaml
# Phase 3 adds: api deployment/service, frontend deployment/service, PVCs, ExternalSecrets,
# HTTPRoute, CronJobs. See docs/DEPLOYMENT.md.
```

`deploy/overlays/homelab/kustomization.yaml`:
```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ../../base
# Phase 3 adds: arm64 nodeSelectors, resource limits, hostnames.
```

`deploy/argocd/ffh.yaml`:
```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: ffh
  namespace: argocd
spec:
  project: default
  source:
    repoURL: https://github.com/maulepilot117/FantasyFootballHelper
    targetRevision: main
    path: deploy/overlays/homelab
  destination:
    server: https://kubernetes.default.svc
    namespace: ffh
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
    syncOptions:
      - CreateNamespace=true
```

`deploy/README.md`:
```markdown
# deploy/

Kustomize base + homelab overlay + ArgoCD Application. **Do not `kubectl apply` by hand** —
ArgoCD owns rollouts (docs/DEPLOYMENT.md). Phase 0 ships only the namespace; workloads,
PVCs, ExternalSecrets, HTTPRoute, and CronJobs land in Phase 3. The ArgoCD Application is
NOT applied to the cluster until Phase 3.
```

- [ ] **Step 2: Validate** — `kubectl kustomize deploy/overlays/homelab` (if kubectl is installed) renders one Namespace. If not installed, skip; CI does not validate manifests in Phase 0.

- [ ] **Step 3: Commit**

```bash
git add deploy
git commit -m "chore(deploy): kustomize base/overlay skeleton and ArgoCD Application"
```

---

### Task 10: GitHub Actions CI

**Files:**
- Create: `.github/workflows/ci.yml`

**Interfaces:**
- Produces: images `ghcr.io/maulepilot117/ffh-api` and `ghcr.io/maulepilot117/ffh-frontend`, tags `sha-<short>` and `main`, `linux/arm64` only, pushed on `main` only.

- [ ] **Step 1: Write `.github/workflows/ci.yml`**

```yaml
name: ci

on:
  push:
    branches: [main]
  pull_request:

permissions:
  contents: read
  packages: write

concurrency:
  group: ci-${{ github.ref }}
  cancel-in-progress: true

jobs:
  backend:
    runs-on: ubuntu-24.04-arm
    services:
      postgres:
        image: postgres:17.10-bookworm
        env:
          POSTGRES_USER: ffh
          POSTGRES_PASSWORD: ffh
          POSTGRES_DB: ffh_test
        ports: ["5432:5432"]
        options: >-
          --health-cmd "pg_isready -U ffh -d ffh_test"
          --health-interval 5s --health-timeout 3s --health-retries 10
    env:
      FFH_TEST_DATABASE_URL: postgresql+psycopg://ffh:ffh@localhost:5432/ffh_test
      FFH_DATABASE_URL: postgresql+psycopg://ffh:ffh@localhost:5432/ffh_test
    defaults:
      run:
        working-directory: backend
    steps:
      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1
      - uses: astral-sh/setup-uv@c771a70e6277c0a99b617c7a806ffedaca235ff9 # v9.0.0
        with:
          enable-cache: true
          cache-dependency-glob: backend/uv.lock
      - run: uv python install
      - run: uv sync --frozen
      - run: uv run ruff check .
      - run: uv run ruff format --check .
      - run: uv run pytest -v

  frontend:
    runs-on: ubuntu-24.04-arm
    defaults:
      run:
        working-directory: frontend
    steps:
      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1
      - uses: oven-sh/setup-bun@0c5077e51419868618aeaa5fe8019c62421857d6 # v2.2.0
        with:
          bun-version: 1.3.12
      - run: bun install --frozen-lockfile
      - run: bun test
      - run: bun run build

  images:
    if: github.event_name == 'push' && github.ref == 'refs/heads/main'
    needs: [backend, frontend]
    runs-on: ubuntu-24.04-arm
    strategy:
      matrix:
        include:
          - name: ffh-api
            context: backend
          - name: ffh-frontend
            context: frontend
    steps:
      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1
      - uses: docker/setup-buildx-action@bb05f3f5519dd87d3ba754cc423b652a5edd6d2c # v4.2.0
      - uses: docker/login-action@dbcb813823bdd20940b903addbd779551569679f # v4.6.0
        with:
          registry: ghcr.io
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}
      - id: meta
        uses: docker/metadata-action@dc802804100637a589fabce1cb79ff13a1411302 # v6.2.0
        with:
          images: ghcr.io/${{ github.repository_owner }}/${{ matrix.name }}
          tags: |
            type=sha,prefix=sha-
            type=raw,value=main
      - uses: docker/build-push-action@53b7df96c91f9c12dcc8a07bcb9ccacbed38856a # v7.3.0
        with:
          context: ${{ matrix.context }}
          platforms: linux/arm64
          push: true
          tags: ${{ steps.meta.outputs.tags }}
          labels: ${{ steps.meta.outputs.labels }}
          cache-from: type=gha,scope=${{ matrix.name }}
          cache-to: type=gha,scope=${{ matrix.name }},mode=max
```

Notes for the executor: no QEMU step on purpose (DEPLOYMENT.md CI section). CI does not update `deploy/` image tags in Phase 0 — that wiring is Phase 3. `bun-version` must match the local `bun --version` (1.3.12 on 2026-08-15) so lockfiles agree.

- [ ] **Step 2: Lint the workflow locally if `actionlint` is available; otherwise rely on the PR run.**

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: lint, test (postgres service), bun test, arm64 image build+push on main"
```

---

### Task 11: Docs, README dev section, ROADMAP ticks, open PR

**Files:**
- Modify: `README.md` (add "Local development" section)
- Modify: `docs/ROADMAP.md` (tick: repo scaffold, Dockerfiles, CI, engine purity test)
- Modify: `docs/DEPLOYMENT.md` CI section (one line: image names + tag scheme)

- [ ] **Step 1: Add to `README.md` before `## Docs`**

```markdown
## Local development

```bash
docker compose up -d --wait            # postgres 17 (ffh + ffh_test) and redis 7
cd backend && uv sync && uv run pytest # backend tests (db-marked tests need compose up)
uv run uvicorn ffh.api.app:app --reload
cd ../frontend && bun install && bun run dev   # http://localhost:3000, proxies /api → :8000
```

Copy `backend/.env.example` to `backend/.env` for local overrides (gitignored).
```

- [ ] **Step 2: Tick ROADMAP.md Phase 0 items** — change `- [ ]` to `- [x]` for: Repo scaffold; Dockerfiles; CI on `ubuntu-24.04-arm`; Engine purity test.

- [ ] **Step 3: Add to `docs/DEPLOYMENT.md` under CI** — after the pipeline paragraph:

```markdown
Images: `ghcr.io/maulepilot117/ffh-api` and `ghcr.io/maulepilot117/ffh-frontend`, tagged
`sha-<short>` and `main`, built only on push to `main`. Actions are pinned to commit SHAs.
```

- [ ] **Step 4: Full local verification**

From `backend/`: `uv run ruff check . && uv run ruff format --check . && uv run pytest -v` → all green (including `db` tests with compose up).
From `frontend/`: `bun test && bun run build` → green.

- [ ] **Step 5: Commit and push, open PR**

```bash
git add README.md docs/ROADMAP.md docs/DEPLOYMENT.md
git commit -m "docs: local dev instructions, tick Phase 0 scaffold items"
git push -u origin chore/scaffold
gh pr create --title "chore: repo scaffold, CI, Dockerfiles (Phase 0 ①)" --body-file - <<'EOF'
## Summary
- backend: uv project, settings, FastAPI /health, typer CLI, engine-purity test, Dockerfile
- frontend: Bun + Vite + React 19 + Tailwind v4 scaffold, health page, bun test, Dockerfile
- deploy: kustomize + ArgoCD skeleton (not applied until Phase 3)
- compose: postgres 17 + redis 7 (ffh, ffh_test)
- CI: ubuntu-24.04-arm, lint → pytest → bun test → arm64 images to GHCR on main

Spec: docs/superpowers/specs/2026-08-15-phase0-foundation-design.md §1
Plan: docs/superpowers/plans/2026-08-15-phase0-01-scaffold.md

## Review checklist (AGENTS.md)
- [ ] No pandas / nfl_data_py / nflreadpy
- [ ] No Alpine
- [ ] No secrets
- [ ] Engine purity test present and bites

🤖 Generated with [Claude Code](https://claude.com/claude-code)

https://claude.ai/code/session_018YN1sk8qzRpGdzgXYHBPgP
EOF
```

- [ ] **Step 6: Watch CI to green** — `gh pr checks --watch`. Fix any arm-runner surprises (most likely: `bun install --frozen-lockfile` — if it fails, run `bun install` on the runner via a temporary step, commit the regenerated `bun.lock`, and remove the step).

- [ ] **Step 7: Hand off for Codex adversarial review** (Chris runs it). Resolve BLOCKING findings, then merge with squash.
