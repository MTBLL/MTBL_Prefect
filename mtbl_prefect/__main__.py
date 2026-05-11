"""CLI dispatcher for `python -m mtbl_prefect <command>`."""

from __future__ import annotations

from datetime import datetime

import click

from mtbl_prefect.flows.full_pipeline import full_pipeline


@click.group()
def cli() -> None:
    """MTBL Prefect orchestrator."""


@cli.command("full-pipeline")
@click.option("--year", type=int, default=None, help="League year (defaults to current)")
@click.option(
    "--preseason",
    is_flag=True,
    default=False,
    help="Preseason mode: Savant uses year-1 while ESPN/Fangraphs use --year",
)
def run_full_pipeline(year: int | None, preseason: bool) -> None:
    """Run the full ETL pipeline: extract -> transform -> load."""
    if year is None:
        year = datetime.now().year
    full_pipeline(year=year, preseason=preseason)


def main() -> None:
    cli()


if __name__ == "__main__":
    main()
