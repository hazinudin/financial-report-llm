"""Shared test setup.

The shipped config enables ``chat.trace_audit`` for both modes. Most unit tests
exercise the single-pass orchestration, so they opt out by default; the tests
that cover the trace-audited path set ``enabled: true`` explicitly.
"""
from __future__ import annotations

import pytest

from chat_app import agent


@pytest.fixture(autouse=True)
def _trace_audit_off_by_default(monkeypatch):
    real = agent._load_chat_config

    def load() -> dict:
        cfg = dict(real())
        trace = dict(cfg.get("trace_audit") or {})
        trace["enabled"] = False
        cfg["trace_audit"] = trace
        return cfg

    monkeypatch.setattr(agent, "_load_chat_config", load)
