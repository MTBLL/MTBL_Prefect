## Linear ticket

<!-- e.g. MTBL-149. Auto-links in Linear when included. -->

## Summary

<!-- 1-3 sentences: what changed and why. -->

## Test plan

- [ ] `uv run pytest` passes locally
- [ ] (flow changes) Triggered a run and verified DAG / concurrency in the Prefect UI at http://localhost:4200
- [ ] (compose or Dockerfile changes) `docker compose config --quiet` validates

## Operator notes

<!-- New env vars in .env.example? New CLI subcommands? New schedule? Anything an operator running this in production needs to know. Delete this section if there's nothing. -->
