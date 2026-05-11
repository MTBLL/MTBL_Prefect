"""Load subflow: sync transformed data to Neon Postgres."""

from __future__ import annotations

from prefect import flow

from mtbl_prefect.tasks.shell import cli_task


player_universe_load = cli_task(
    "player-universe-load",
    project_dir="_load/Player_Universe_Load",
    command=["player-universe-load", "load-and-sync", "--year", "{year}"],
    retries=2,
    retry_delay_seconds=[30, 120],
)


@flow(name="load", log_prints=True)
def load(year: int) -> None:
    player_universe_load(year=year)
