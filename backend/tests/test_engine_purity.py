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


def _imported_names_from_source(source: str) -> set[str]:
    tree = ast.parse(source)
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.level > 0 or not node.module:
                continue  # relative imports stay inside ffh.engine
            names.add(node.module)
            # `from ffh import db` must be seen as `ffh.db`, not just `ffh`
            names.update(f"{node.module}.{alias.name}" for alias in node.names)
    return names


def _imported_names(path: Path) -> set[str]:
    return _imported_names_from_source(path.read_text(encoding="utf-8"))


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


def test_purity_guard_catches_from_imports_of_forbidden_packages():
    src = "\n".join(
        [
            "from ffh import db",
            "import ffh.ai as a",
            "from . import vorp",
            "from ffh.engine import tiers",
            "import polars as pl",
        ]
    )
    names = _imported_names_from_source(src)
    assert sorted(n for n in names if _is_forbidden(n)) == ["ffh.ai", "ffh.db"]
    assert "vorp" not in names  # relative imports are skipped
