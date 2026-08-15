"""ffh.engine must be pure math: no I/O, network, DB, or LLM imports (ARCHITECTURE.md)."""

import ast
import importlib
import pkgutil
import sys
from pathlib import Path

import ffh.engine

FORBIDDEN_MODULES = {
    "httpx",
    "requests",
    "aiohttp",
    "urllib3",
    "urllib.request",
    "anthropic",
    "openai",
    "sqlalchemy",
    "psycopg",
    "asyncpg",
    "alembic",
    "redis",
    "duckdb",
    "ffh.ai",
    "ffh.adapters",
    "ffh.db",
    "ffh.ingest",
    "ffh.api",
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
