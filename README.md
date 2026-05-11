# MTBL Prefect Orchestration

Self-hosted Prefect server + ephemeral runner for the MTBL fantasy baseball ETL pipeline.

Design rationale: [TDD in Notion](https://www.notion.so/35d8d8a04528815fb440ecc2daf39472)
Execution tracking: `mtbl-player-etl` project in Linear (trp workspace)

## Quickstart

1. Copy `.env.example` to `.env` and fill in real values.
2. Start the persistent stack (Prefect server + Postgres):

   ```
   docker compose up -d
   ```

3. Open the UI at http://localhost:4200.
4. From Phase 1 onward, trigger a flow on demand:

   ```
   docker compose --profile runner run --rm runner full-pipeline --year 2026
   ```

## Layout

- `mtbl_prefect/` — package source. Flows, tasks, and hooks are added in Phase 1.
- `Dockerfile` — runner image, built on `ghcr.io/astral-sh/uv:python3.13-bookworm-slim`.
- `docker-compose.yml` — `prefect-server` + `postgres` always-on; `runner` under the `runner` profile so it stays dormant during a default `up`.
- `.env.example` — environment variables required at runtime.
- `tests/` — pytest suite (populated in Phase 1).

## Phase status

- **Phase 0** (scaffold + UI reachable): in progress on this branch.
- **Phase 1+** tracked in the `mtbl-player-etl` Linear project.
