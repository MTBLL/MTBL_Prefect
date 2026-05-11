"""Flow state hooks: failure -> Slack + healthcheck fail-ping; completion -> healthcheck success-ping.

All hooks are env-tolerant. If `SLACK_WEBHOOK_URL` or `HEALTHCHECKS_PING_URL`
is unset, the corresponding side-effect is skipped rather than crashing the flow.
"""

from __future__ import annotations

import logging
import os

import httpx

logger = logging.getLogger(__name__)


def notify_failure(flow, flow_run, state) -> None:
    """on_failure / on_crashed hook: post to Slack + ping healthcheck fail endpoint."""
    _post_slack(flow_run, state)
    _ping_healthcheck(success=False)


def notify_success(flow, flow_run, state) -> None:
    """on_completion hook: ping healthcheck success endpoint. No Slack noise on success."""
    _ping_healthcheck(success=True)


def _post_slack(flow_run, state) -> None:
    webhook = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook:
        logger.warning("SLACK_WEBHOOK_URL unset; skipping Slack notification")
        return
    try:
        httpx.post(
            webhook,
            json={"text": f":x: MTBL pipeline `{flow_run.name}` failed: {state.message}"},
            timeout=10,
        )
    except httpx.HTTPError as e:
        logger.error("Slack post failed: %s", e)


def _ping_healthcheck(*, success: bool) -> None:
    base = os.environ.get("HEALTHCHECKS_PING_URL")
    if not base:
        logger.warning("HEALTHCHECKS_PING_URL unset; skipping healthcheck ping")
        return
    url = base.rstrip("/") if success else f"{base.rstrip('/')}/fail"
    try:
        httpx.get(url, timeout=10)
    except httpx.HTTPError as e:
        logger.error("Healthcheck ping failed: %s", e)
