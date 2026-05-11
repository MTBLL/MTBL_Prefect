"""Unit tests for flow state hooks (Slack notify + healthcheck pings)."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

from mtbl_prefect.tasks import hooks


def _flow_run(name: str = "flow-run-x"):
    return SimpleNamespace(name=name)


def _state(message: str = "boom"):
    return SimpleNamespace(message=message)


def test_notify_failure_posts_to_slack(monkeypatch):
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/T/B/X")
    monkeypatch.delenv("HEALTHCHECKS_PING_URL", raising=False)
    with patch.object(hooks.httpx, "post") as mock_post:
        hooks.notify_failure(None, _flow_run("nightly-2026"), _state("Fangraphs 502"))
    mock_post.assert_called_once()
    body = mock_post.call_args.kwargs["json"]
    assert "nightly-2026" in body["text"]
    assert "Fangraphs 502" in body["text"]


def test_notify_failure_pings_healthcheck_fail_endpoint(monkeypatch):
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    monkeypatch.setenv("HEALTHCHECKS_PING_URL", "https://hc-ping.com/abc")
    with patch.object(hooks.httpx, "get") as mock_get:
        hooks.notify_failure(None, _flow_run(), _state())
    mock_get.assert_called_once_with("https://hc-ping.com/abc/fail", timeout=10)


def test_notify_success_pings_healthcheck_base_url(monkeypatch):
    monkeypatch.setenv("HEALTHCHECKS_PING_URL", "https://hc-ping.com/abc")
    with patch.object(hooks.httpx, "get") as mock_get:
        hooks.notify_success(None, _flow_run(), _state())
    mock_get.assert_called_once_with("https://hc-ping.com/abc", timeout=10)


def test_notify_success_strips_trailing_slash(monkeypatch):
    monkeypatch.setenv("HEALTHCHECKS_PING_URL", "https://hc-ping.com/abc/")
    with patch.object(hooks.httpx, "get") as mock_get:
        hooks.notify_success(None, _flow_run(), _state())
    mock_get.assert_called_once_with("https://hc-ping.com/abc", timeout=10)


def test_hooks_no_crash_when_env_unset(monkeypatch):
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    monkeypatch.delenv("HEALTHCHECKS_PING_URL", raising=False)
    # Neither call should raise.
    hooks.notify_failure(None, _flow_run(), _state())
    hooks.notify_success(None, _flow_run(), _state())


def test_notify_failure_does_not_propagate_http_errors(monkeypatch):
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/T/B/X")
    monkeypatch.setenv("HEALTHCHECKS_PING_URL", "https://hc-ping.com/abc")
    import httpx as real_httpx

    def boom(*a, **kw):
        raise real_httpx.HTTPError("network down")

    monkeypatch.setattr(hooks.httpx, "post", boom)
    monkeypatch.setattr(hooks.httpx, "get", boom)
    # Hook itself must not raise even if both endpoints are down.
    hooks.notify_failure(None, _flow_run(), _state())
