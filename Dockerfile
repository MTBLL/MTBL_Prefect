FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

# postgresql-client-18 provides pg_dump and psql at the version that matches
# the fantasy-pg server (postgres:18.3). The default Debian bookworm packages
# only go up to PG 15, and pg_dump refuses to dump from a newer server than
# its own version, so we add the PGDG apt repository for current clients.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates gnupg \
    && install -d /usr/share/postgresql-common/pgdg \
    && curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
        -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc \
    && echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt bookworm-pgdg main" \
        > /etc/apt/sources.list.d/pgdg.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends postgresql-client-18 \
    && apt-get purge -y curl gnupg \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml uv.lock README.md ./
COPY mtbl_prefect ./mtbl_prefect

RUN uv sync --frozen --no-dev

ENTRYPOINT ["uv", "run", "python", "-m", "mtbl_prefect"]
