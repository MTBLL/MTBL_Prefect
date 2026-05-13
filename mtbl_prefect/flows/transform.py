"""Transform subflow: universe-trx then mtbl-valuations, sequential."""

from __future__ import annotations

from prefect import flow

from mtbl_prefect.tasks.shell import cli_task


player_universe_trx = cli_task(
    "player-universe-trx",
    project_dir="_transform/Player_Universe_Trx",
    command=["universe-trx", "--year", "{year}"],
    retries=1,
    retry_delay_seconds=30,
)


mtbl_valuations = cli_task(
    "mtbl-valuations",
    project_dir="_transform/MTBL_Valuations",
    command=["mtbl-valuations", "hydrate"],
    retries=1,
    retry_delay_seconds=30,
)


@flow(name="transform", log_prints=True)
def transform(year: int) -> None:
    player_universe_trx(year=year)
    mtbl_valuations()
