"""Unit tests for flow state hooks (Slack notify + healthcheck pings)."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import httpx

from mtbl_prefect.tasks import hooks


def _flow_run(name: str = "flow-run-x"):
    return SimpleNamespace(name=name)


def _state(message: str = "boom"):
    return SimpleNamespace(message=message)


def _bad_response(status_code: int) -> MagicMock:
    """Build a mock response whose raise_for_status raises HTTPStatusError."""
    resp = MagicMock()
    resp.raise_for_status.side_effect = httpx.HTTPStatusError(
        f"{status_code}",
        request=MagicMock(),
        response=MagicMock(status_code=status_code),
    )
    return resp


# ---------------------------------------------------------------------------
# notify_failure
# ---------------------------------------------------------------------------


def test_notify_failure_posts_to_slack(monkeypatch):
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/T/B/X")
    monkeypatch.delenv("HEALTHCHECKS_PING_URL", raising=False)
    with patch.object(hooks.httpx, "post") as mock_post:
        hooks.notify_failure(None, _flow_run("nightly-2026"), _state("Fangraphs 502"))
    mock_post.assert_called_once()
    body = mock_post.call_args.kwargs["json"]
    assert "nightly-2026" in body["text"]
    assert "Fangraphs 502" in body["text"]
    assert ":x:" in body["text"]


def test_notify_failure_pings_healthcheck_fail_endpoint(monkeypatch):
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    monkeypatch.setenv("HEALTHCHECKS_PING_URL", "https://hc-ping.com/abc")
    with patch.object(hooks.httpx, "get") as mock_get:
        hooks.notify_failure(None, _flow_run(), _state())
    mock_get.assert_called_once_with("https://hc-ping.com/abc/fail", timeout=10)


def test_notify_failure_does_not_propagate_http_errors(monkeypatch):
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/T/B/X")
    monkeypatch.setenv("HEALTHCHECKS_PING_URL", "https://hc-ping.com/abc")

    def boom(*a, **kw):
        raise httpx.HTTPError("network down")

    monkeypatch.setattr(hooks.httpx, "post", boom)
    monkeypatch.setattr(hooks.httpx, "get", boom)
    # Hook itself must not raise even if both endpoints are down.
    hooks.notify_failure(None, _flow_run(), _state())


def test_notify_failure_catches_slack_4xx_response(monkeypatch):
    """A revoked webhook returning 410 Gone is caught + logged, not propagated."""
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/T/B/X")
    monkeypatch.delenv("HEALTHCHECKS_PING_URL", raising=False)
    with patch.object(hooks.httpx, "post", return_value=_bad_response(410)):
        hooks.notify_failure(None, _flow_run(), _state())


def test_notify_failure_catches_slack_5xx_response(monkeypatch):
    """A Slack server-side error is caught + logged, not propagated."""
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/T/B/X")
    monkeypatch.delenv("HEALTHCHECKS_PING_URL", raising=False)
    with patch.object(hooks.httpx, "post", return_value=_bad_response(503)):
        hooks.notify_failure(None, _flow_run(), _state())


def test_notify_failure_catches_healthcheck_4xx_during_fail_ping(monkeypatch):
    """When Slack succeeds but healthcheck fail-ping returns 4xx, hook still succeeds."""
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/T/B/X")
    monkeypatch.setenv("HEALTHCHECKS_PING_URL", "https://hc-ping.com/abc")
    with patch.object(hooks.httpx, "post"), \
         patch.object(hooks.httpx, "get", return_value=_bad_response(404)):
        hooks.notify_failure(None, _flow_run(), _state())


# ---------------------------------------------------------------------------
# notify_success
# ---------------------------------------------------------------------------


def test_notify_success_posts_to_slack(monkeypatch):
    """Success hook now also posts to Slack — solo-ops dashboard pattern."""
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/T/B/X")
    monkeypatch.delenv("HEALTHCHECKS_PING_URL", raising=False)
    with patch.object(hooks.httpx, "post") as mock_post:
        hooks.notify_success(None, _flow_run("nightly-2026"), _state())
    mock_post.assert_called_once()
    body = mock_post.call_args.kwargs["json"]
    assert "nightly-2026" in body["text"]
    assert ":white_check_mark:" in body["text"]
    assert "completed successfully" in body["text"]


def test_notify_success_pings_healthcheck_base_url(monkeypatch):
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    monkeypatch.setenv("HEALTHCHECKS_PING_URL", "https://hc-ping.com/abc")
    with patch.object(hooks.httpx, "get") as mock_get:
        hooks.notify_success(None, _flow_run(), _state())
    mock_get.assert_called_once_with("https://hc-ping.com/abc", timeout=10)


def test_notify_success_strips_trailing_slash(monkeypatch):
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    monkeypatch.setenv("HEALTHCHECKS_PING_URL", "https://hc-ping.com/abc/")
    with patch.object(hooks.httpx, "get") as mock_get:
        hooks.notify_success(None, _flow_run(), _state())
    mock_get.assert_called_once_with("https://hc-ping.com/abc", timeout=10)


def test_notify_success_catches_slack_4xx_response(monkeypatch):
    """A revoked webhook returning 4xx on the success path is caught + logged."""
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/T/B/X")
    monkeypatch.delenv("HEALTHCHECKS_PING_URL", raising=False)
    with patch.object(hooks.httpx, "post", return_value=_bad_response(410)):
        hooks.notify_success(None, _flow_run(), _state())


def test_notify_success_catches_healthcheck_4xx_response(monkeypatch):
    """A bad healthcheck URL returning 404 is caught + logged, not propagated."""
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    monkeypatch.setenv("HEALTHCHECKS_PING_URL", "https://hc-ping.com/abc")
    with patch.object(hooks.httpx, "get", return_value=_bad_response(404)):
        hooks.notify_success(None, _flow_run(), _state())


# ---------------------------------------------------------------------------
# Shared env-tolerance
# ---------------------------------------------------------------------------


def test_hooks_no_crash_when_env_unset(monkeypatch):
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    monkeypatch.delenv("HEALTHCHECKS_PING_URL", raising=False)
    # Neither call should raise.
    hooks.notify_failure(None, _flow_run(), _state())
    hooks.notify_success(None, _flow_run(), _state())
