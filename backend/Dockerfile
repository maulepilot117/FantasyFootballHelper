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
