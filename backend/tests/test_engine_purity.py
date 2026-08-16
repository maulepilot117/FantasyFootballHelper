"""ffh.engine must be pure math: no I/O, network, DB, or LLM imports (ARCHITECTURE.md)."""

import ast
import subprocess
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
BACKEND_DIR = ENGINE_DIR.parents[2]  # .../backend/src/ffh/engine -> .../backend
SRC_DIR = BACKEND_DIR / "src"


def _module_name(path: Path) -> str:
    """Dotted module name of a source file relative to backend/src (``__init__`` -> package)."""
    parts = list(path.relative_to(SRC_DIR).with_suffix("").parts)
    if parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts)


def _imported_names_from_source(source: str, module: str, *, is_package: bool = False) -> set[str]:
    """Absolute names imported by ``source``, resolving relative imports against ``module``."""
    package = module if is_package else module.rpartition(".")[0]
    tree = ast.parse(source)
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.level > 0:
                # `from .. import x` inside a.b.c -> base package a
                base_parts = package.split(".")[: len(package.split(".")) - (node.level - 1)]
                base = ".".join(base_parts)
                if node.module:
                    base = f"{base}.{node.module}" if base else node.module
            else:
                base = node.module or ""
            if base:
                names.add(base)
            # `from ffh import db` must be seen as `ffh.db`, not just `ffh`
            names.update(f"{base}.{alias.name}" if base else alias.name for alias in node.names)
    return names


def _imported_names(path: Path) -> set[str]:
    return _imported_names_from_source(
        path.read_text(encoding="utf-8"),
        _module_name(path),
        is_package=path.name == "__init__.py",
    )


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
    """Import every ffh.engine submodule in a clean interpreter and inspect sys.modules.

    Runs in a subprocess so modules preloaded by other tests (sqlalchemy, httpx, ...)
    cannot mask a leak.
    """
    code = (
        "import sys, importlib, pkgutil, ffh.engine; "
        "[importlib.import_module(m.name) "
        "for m in pkgutil.walk_packages(ffh.engine.__path__, 'ffh.engine.')]; "
        "print(chr(10).join(sorted(sys.modules)))"
    )
    proc = subprocess.run(
        [sys.executable, "-c", code],
        cwd=BACKEND_DIR,
        capture_output=True,
        text=True,
        check=True,
    )
    loaded = proc.stdout.split()
    leaked = sorted(n for n in loaded if _is_forbidden(n))
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
    names = _imported_names_from_source(src, "ffh.engine.lineup")
    assert sorted(n for n in names if _is_forbidden(n)) == ["ffh.ai", "ffh.db"]
    assert "ffh.engine.vorp" in names  # relative imports resolve inside ffh.engine


def test_purity_guard_resolves_relative_imports():
    src = "\n".join(
        [
            "from .. import db",  # ffh.db — forbidden
            "from ..db import models",  # ffh.db.models — forbidden
            "from . import tiers",  # ffh.engine.tiers — fine
            "from .vorp import compute",  # ffh.engine.vorp.compute — fine
        ]
    )
    names = _imported_names_from_source(src, "ffh.engine.lineup")
    assert sorted(n for n in names if _is_forbidden(n)) == ["ffh.db", "ffh.db.models"]
    assert {"ffh.engine.tiers", "ffh.engine.vorp", "ffh.engine.vorp.compute"} <= names

    # From the package __init__ itself, `.` is ffh.engine and `..` is ffh.
    pkg_names = _imported_names_from_source(
        "from .. import api\nfrom . import tiers", "ffh.engine", is_package=True
    )
    assert sorted(n for n in pkg_names if _is_forbidden(n)) == ["ffh.api"]
    assert "ffh.engine.tiers" in pkg_names


def test_module_name_derivation():
    assert _module_name(ENGINE_DIR / "__init__.py") == "ffh.engine"
    assert _module_name(ENGINE_DIR / "vorp.py") == "ffh.engine.vorp"
    assert _module_name(ENGINE_DIR / "sim" / "season.py") == "ffh.engine.sim.season"
