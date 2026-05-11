FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

WORKDIR /app

COPY pyproject.toml uv.lock ./
COPY mtbl_prefect ./mtbl_prefect

RUN uv sync --frozen --no-dev

ENTRYPOINT ["uv", "run", "python", "-m", "mtbl_prefect"]
