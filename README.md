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

## Host-direct setup

The "dev, fast iteration" row above runs the orchestrator on your host shell, bypassing Docker. The flow still needs to reach the `fantasy-pg` Postgres container — but that container's hostname (`fantasy-pg`) is a docker-compose service name and resolves only from inside the compose network, not from host. You'd see:

```
❌ Connection failed: could not translate host name "fantasy-pg" to address: nodename nor servname provided, or not known
```

Fix: `fantasy-pg` publishes its Postgres on host port **5433** (chosen over 5432 to avoid colliding with a host-native Postgres). Override `LOCAL_DATABASE_URL` for your host shell by creating `.envrc.local` (gitignored) with:

```bash
export LOCAL_DATABASE_URL=postgresql://fantasy:fantasy@localhost:5433/fantasy_baseball
```

`.envrc` loads `.env` first (in-container default), then sources `.envrc.local` if present — so host-direct runs see the localhost URL while the runner container keeps using `fantasy-pg`. After creating the file:

```
direnv allow              # tell direnv the new .envrc.local is trusted
psql "$LOCAL_DATABASE_URL" -c '\dt'   # confirm the host reaches fantasy-pg on 5433
```

You'll need this once per machine. The override is a no-op for the docker-runner path — that container reads `.env` directly via `env_file:`, not via direnv.

## Flag mapping

The orchestrator exposes a minimal CLI surface. Flags are translated and routed to each sub-project's own CLI by the flow code in `mtbl_prefect/flows/`. This section documents exactly which flags reach which sub-project, and where each value comes from.

### Top-level orchestrator surface

| Flag | Type | Default | Purpose |
|---|---|---|---|
| `--year` | int | current calendar year | League year for extraction, transformation, and load |
| `--preseason` | boolean | `false` | Preseason mode: shifts Savant's `--season` to year-1 for prior recorded stats |

No mode flags (e.g. no `--ros`, no `--winter-meetings`, no `--predraft`) — the sub-projects own their own season-context defaults. No per-pipe override flags — to customize a specific sub-project's behavior, edit its invocation in `mtbl_prefect/flows/extract.py` (or transform / load).

### How `--year` and `--preseason` route to sub-projects

```
Top-level CLI (`python -m mtbl_prefect full-pipeline ...`)
   │
   ├── --year YYYY ──┬── ESPN: espn players-extract --year YYYY
   │                 ├── ESPN: espn league-extract --year YYYY
   │                 ├── Fangraphs: fangraphs-api-extractor --year YYYY
   │                 ├── universe-trx --year YYYY
   │                 ├── player-universe-load load-and-sync --year YYYY
   │                 │
   │                 └── (season derivation) ─┐
   │                                          ▼
   └── --preseason  ──────► season = year - 1 if preseason else year
                                     │
                                     └── savant-extract --season SSSS
```

### Per-sub-project invocation table

| Sub-project | CLI invoked | Flags from orchestrator | Hardcoded by orchestrator |
|---|---|---|---|
| `_extract/ESPN_API_Extractor` | `espn players-extract` | `--year YYYY` | `--output-dir`, `--force-full-extraction` |
| `_extract/ESPN_API_Extractor` | `espn league-extract` | `--year YYYY` | `--output-dir` |
| `_extract/Fangraphs_API_Extractor` | `fangraphs-api-extractor` | `--year YYYY` | `--output-dir` |
| `_extract/Savant_API_Extractor` | `savant-extract` | `--season SSSS` (year-1 if preseason, else year) | `--output-dir` |
| `_transform/Player_Universe_Trx` | `universe-trx` | `--year YYYY` | (none) |
| `_transform/MTBL_Valuations` | `mtbl-valuations hydrate` | (none) | `hydrate` subcommand |
| `_load/Player_Universe_Load` | `player-universe-load load-and-sync` | `--year YYYY` | `load-and-sync` subcommand |

Notes on the irregular cases:

- **Savant uses `--season`, not `--year`** — flag name rename happens at the orchestrator boundary. The preseason-vs-in-season derivation (`year - 1 if preseason else year`) lives in `flows/full_pipeline.py` and only affects this one downstream argument.
- **ESPN players-extract always gets `--force-full-extraction`** — orchestrator hardcode; ensures a full pull rather than ESPN's GraphQL-optimized partial. Override by editing `flows/extract.py:espn_api_extractor`.
- **Fangraphs takes no mode flag** — the CLI pulls all three projection slots (`projections`, `projs_updated`, `ros`) on every run regardless of season context. Source mix customization (`--batter-sources`, `--pitcher-sources`, `--weights`, `--winter-meetings`) is NOT exposed at the orchestrator level; defaults apply. Edit `flows/extract.py:fangraphs_extractor` if you need to override.
- **MTBL_Valuations receives no flags** — the CLI reads files from the transform output directory directly. `hydrate` is the subcommand; everything else is implicit from filesystem state.
### Where flag VALUES come from

| Value | Source | Notes |
|---|---|---|
| `YYYY` (year) | `--year` CLI arg, or `datetime.now().year` default | Used everywhere except Savant in preseason mode |
| `SSSS` (Savant season) | Derived in `full_pipeline`: `year - 1 if preseason else year` | Only diverges from year when `--preseason` is set |
| Output directories | `mtbl_prefect/config.py` constants (`EXTRACT_OUTPUT_DIR`, `TRANSFORM_OUTPUT_DIR`) | Anchored to `/Users/Shared/BaseballHQ/resources/{extract,transform}` |
| Database URLs | `.env` (`LOCAL_DATABASE_URL`, plus sub-project's `NEON_DATABASE_URL` for sync) | Routed via env var, not CLI args |
| Webhook / healthcheck URLs | `.env` (`SLACK_WEBHOOK_URL`, `HEALTHCHECKS_PING_URL`) | Read by `tasks/hooks.py`, not passed to sub-projects |

### Adding a new top-level flag

If you need to route a new flag from the orchestrator into one or more sub-projects:

1. **Argparse declaration** — add the argument in `mtbl_prefect/__main__.py:run_full_pipeline`.
2. **Plumb through full_pipeline** — add the parameter to `full_pipeline()` signature in `flows/full_pipeline.py` and pass it into the relevant subflow.
3. **Plumb through subflow** — `extract()` / `transform()` / `load()` in the respective flow modules.
4. **Reach the task** — pass into the relevant `cli_task` invocation (templated via `{kwarg}` substitution) OR into a hand-rolled `@task` function.
5. **Lock with a test** — add an assertion in `tests/test_flow_smoke.py` that the flag reaches the correct sub-project's CLI invocation with the expected value.

The shell-task pattern means each new flag is ~5 lines of code spread across these 5 files. No CLI rewrites in any sub-project required.

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
