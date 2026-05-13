"""Top-level orchestration: extract -> transform -> load."""

from __future__ import annotations

from prefect import flow

from mtbl_prefect.flows.extract import extract
from mtbl_prefect.flows.load import load
from mtbl_prefect.flows.transform import transform
from mtbl_prefect.tasks.hooks import notify_failure, notify_success


@flow(
    name="full-pipeline",
    log_prints=True,
    on_failure=[notify_failure],
    on_crashed=[notify_failure],
    on_completion=[notify_success],
)
def full_pipeline(year: int, preseason: bool = False) -> None:
    season = year - 1 if preseason else year
    print(f"full_pipeline: year={year} season={season} preseason={preseason}")
    extract(year=year, season=season)
    transform(year=year)
    load(year=year)
