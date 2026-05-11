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

## Invocation matrix

Everything is triggered from your host shell. The runner container is ephemeral — spawned per flow run, exits when the flow completes. Only `prefect-server` and `postgres` run continuously.

| Need | Command | Where it runs |
|---|---|---|
| Start the always-on stack | `docker compose up -d` | Host triggers, server + Postgres run in Docker |
| Stop the stack | `docker compose down` | Host triggers |
| Trigger a flow (production-faithful) | `docker compose --profile runner run --rm runner full-pipeline --year 2026` | Host triggers, flow runs in ephemeral container |
| Trigger a flow (dev, fast iteration) | `uv run python -m mtbl_prefect full-pipeline --year 2026` | Host directly, bypassing Docker |
| Inspect Prefect state | `prefect flow-run ls` (with `PREFECT_API_URL` set) | Host shell hits the in-Docker server over HTTP |
| Tail server logs | `docker compose logs -f prefect-server` | Host triggers |
| Open a debug shell in the runner image | `docker compose --profile runner run --rm --entrypoint bash runner` | Escape hatch, not a normal workflow |

The Phase 3 LaunchAgent is just the "production-faithful" row, fired by `launchd` at 00:16 America/Denver instead of by your shell — see [`launchd/README.md`](launchd/README.md) for install / verify / uninstall.

## Layout

- `mtbl_prefect/` — package source: flows, tasks, hooks.
- `Dockerfile` — runner image, built on `ghcr.io/astral-sh/uv:python3.13-bookworm-slim` + `postgresql-client-18`.
- `docker-compose.yml` — `prefect-server` + `postgres` + `fantasy-pg` always-on; `runner` under the `runner` profile so it stays dormant during a default `up`.
- `.env.example` — environment variables required at runtime.
- `launchd/` — macOS LaunchAgent + install scripts for nightly scheduling (Phase 3).
- `tests/` — pytest suite.

## Phase status

- **Phase 0** (scaffold + UI reachable) — ✓ merged.
- **Phase 1** (full_pipeline flow + parallel extractors) — ✓ merged.
- **Phase 2** (alerting hooks + container path fix) — ✓ merged.
- **Phase 3** (LaunchAgent nightly scheduling) — in progress on this branch.
- **Phase 4+** tracked in the `mtbl-player-etl` Linear project.
