"""
Thin client for the OpenCode Go subscription API (OpenAI-compatible surface).

- Base URL + key read from config.yaml (analysis.opencode.base_url) and the
  OPENCODE_API_KEY env var (loaded from .env). Never logs headers/keys.
- One code path: POST /chat/completions (OpenAI-compatible). Models that are
  only Anthropic-native are avoided per the implementation plan (§4).
- GET /models lists available IDs (used by --list-models).
- Exponential backoff on 429/5xx.

No SDK dependency; httpx only.
"""

from __future__ import annotations

import os
import time
import uuid
from pathlib import Path
from typing import Any

import httpx
import yaml

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_ENV_PATH = _PROJECT_ROOT / ".env"
_CONFIG_PATH = _PROJECT_ROOT / "config.yaml"
_SESSION_ID = os.environ.get("OPENCODE_SESSION_ID", str(uuid.uuid4()))
_USER_AGENT = "financial-report-llm-analysis-pipeline/1.0"


def _load_env_file(path: Path = _ENV_PATH) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        key, val = key.strip(), val.strip().strip("'\"")
        if key and key not in os.environ:
            os.environ[key] = val


def load_analysis_config() -> dict:
    if _CONFIG_PATH.exists():
        with open(_CONFIG_PATH, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
        return cfg.get("analysis", {}) or {}
    return {}


def get_api_key() -> str:
    _load_env_file()
    key = os.environ.get("OPENCODE_API_KEY", "").strip()
    if not key:
        raise RuntimeError(
            "OPENCODE_API_KEY not set. Add it to .env (the file is gitignored)."
        )
    return key


def get_session_id() -> str:
    """Return the stable session ID used for this pipeline process."""
    return _SESSION_ID


def _request_headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {get_api_key()}",
        "Content-Type": "application/json",
        "User-Agent": _USER_AGENT,
        "x-opencode-session": get_session_id(),
    }


def _base_url() -> str:
    cfg = load_analysis_config()
    return (cfg.get("opencode", {}) or {}).get("base_url", "https://openrouter.ai/api/v1/")


def _timeout() -> float:
    cfg = load_analysis_config()
    return float((cfg.get("opencode", {}) or {}).get("timeout_sec", 300))


class APIError(RuntimeError):
    pass


def list_models() -> list[str]:
    r = httpx.get(
        f"{_base_url()}/models",
        headers=_request_headers(),
        timeout=_timeout(),
    )
    if r.status_code != 200:
        raise APIError(f"GET /models -> {r.status_code}: {r.text[:500]}")
    data = r.json().get("data", [])
    return [m.get("id") for m in data if m.get("id")]


def chat(
    model: str,
    system: str,
    user: str,
    temperature: float = 0.0,
    max_tokens: int = 4096,
    retries: int = 4,
    reasoning_effort: str | None = None,
    timeout_sec: float | None = None,
) -> dict[str, Any]:
    """OpenAI-compatible chat completion. Returns content, usage and finish_reason.

    `reasoning_effort` ("low"|"medium"|"high") is honored by reasoning models
    (e.g. deepseek-v4-flash): reasoning tokens count against max_tokens, so a
    low effort both speeds up copy-style tasks and avoids finish_reason=length
    with empty content.
    """
    headers = _request_headers()
    payload = {
        "model": model,
        "temperature": temperature,
        "max_tokens": max_tokens,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
    }
    if reasoning_effort:
        payload["reasoning_effort"] = reasoning_effort
    delay = 2.0
    last_err: str | None = None
    for attempt in range(retries):
        try:
            r = httpx.post(
                f"{_base_url()}/chat/completions",
                headers=headers,
                json=payload,
                timeout=timeout_sec or _timeout(),
            )
            if r.status_code in (429, 500, 502, 503, 504):
                last_err = f"HTTP {r.status_code}: {r.text[:300]}"
                time.sleep(delay)
                delay *= 2
                continue
            if r.status_code != 200:
                raise APIError(f"HTTP {r.status_code}: {r.text[:500]}")
            data = r.json()
            content = data["choices"][0]["message"]["content"]
            usage = data.get("usage", {}) or {}
            return {
                "content": content,
                "usage": {
                    "prompt_tokens": usage.get("prompt_tokens", 0),
                    "completion_tokens": usage.get("completion_tokens", 0),
                },
                "finish_reason": data.get("choices", [{}])[0].get("finish_reason"),
            }
        except httpx.HTTPError as e:
            last_err = str(e)
            time.sleep(delay)
            delay *= 2
            continue
    raise APIError(f"chat failed after {retries} retries: {last_err}")


def parse_model_settings(role: str) -> dict[str, Any]:
    """Return model-call settings for a configured role."""
    cfg = load_analysis_config()
    models = cfg.get("models", {}) or {}
    temps = cfg.get("temperature", {}) or {}
    tokens = cfg.get("max_tokens", {}) or {}
    efforts = cfg.get("reasoning_effort", {}) or {}
    timeouts = cfg.get("timeouts", {}) or {}
    return {
        "model": models.get(role),
        "temperature": float(temps.get(role, 0.0)),
        "max_tokens": int(tokens.get(role, 4096)),
        "reasoning_effort": efforts.get(role),
        "timeout_sec": float(timeouts[role]) if role in timeouts else None,
    }
