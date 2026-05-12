"""Flow state hooks: failure -> Slack + healthcheck fail-ping; completion -> Slack + healthcheck success-ping.

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
    # On failure paths, missing Slack config IS a real concern — alerting
    # might be invisible. Keep the WARN-level "unset" log.
    _post_slack(flow_run, state, success=False, warn_if_missing=True)
    _ping_healthcheck(success=False)


def notify_success(flow, flow_run, state) -> None:
    """on_completion hook: post to Slack + ping healthcheck success endpoint.

    Solo-ops workflow treats the Slack channel as the dashboard — positive
    confirmation each morning is more useful than silent success.
    """
    # On success paths, Slack is an OPT-IN confirmation. Missing config is
    # expected for users who rely on healthchecks-only signalling, so
    # demote the "unset" log from WARN to DEBUG to avoid noise on healthy runs.
    _post_slack(flow_run, state, success=True, warn_if_missing=False)
    _ping_healthcheck(success=True)


def _post_slack(flow_run, state, *, success: bool, warn_if_missing: bool = True) -> None:
    webhook = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook:
        if warn_if_missing:
            logger.warning("SLACK_WEBHOOK_URL unset; skipping Slack notification")
        else:
            logger.debug("SLACK_WEBHOOK_URL unset; skipping Slack notification")
        return
    if success:
        text = f":white_check_mark: MTBL pipeline `{flow_run.name}` completed successfully"
    else:
        msg = state.message if state and state.message else "no state message"
        text = f":x: MTBL pipeline `{flow_run.name}` failed: {msg}"
    try:
        response = httpx.post(webhook, json={"text": text}, timeout=10)
        response.raise_for_status()
    except httpx.HTTPError as e:
        logger.error("Slack post failed: %s", e)


def _ping_healthcheck(*, success: bool) -> None:
    base = os.environ.get("HEALTHCHECKS_PING_URL")
    if not base:
        logger.warning("HEALTHCHECKS_PING_URL unset; skipping healthcheck ping")
        return
    url = base.rstrip("/") if success else f"{base.rstrip('/')}/fail"
    try:
        response = httpx.get(url, timeout=10)
        response.raise_for_status()
    except httpx.HTTPError as e:
        logger.error("Healthcheck ping failed: %s", e)
