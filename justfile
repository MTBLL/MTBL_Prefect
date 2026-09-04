# Task runner for the MTBL Prefect stack. `just` lists recipes.
# Mirrors README §Invocation matrix — keep the two in sync.

year := "2026"

default:
    @just --list

# Start the always-on stack (prefect-server, postgres, flaresolverr)
up:
    docker compose up -d

# Stop the stack
down:
    docker compose down

# Trigger a flow in the ephemeral runner container (production-faithful)
run flow="full-pipeline" *args="":
    docker compose --profile runner run --rm runner {{flow}} --year {{year}} {{args}}

# Trigger a flow on the host directly (dev, fast iteration; needs .envrc.local)
dev flow="full-pipeline" *args="":
    uv run python -m mtbl_prefect {{flow}} --year {{year}} {{args}}

# Tail server logs
logs:
    docker compose logs -f prefect-server

# Debug shell in the runner image
shell:
    docker compose --profile runner run --rm --entrypoint bash runner

test:
    uv run pytest
