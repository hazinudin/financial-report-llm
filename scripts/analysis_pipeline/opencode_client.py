"""
Thin client for the OpenAI-compatible stages (extract, red-flags).

- Base URL, key env var name and timeout read from config.yaml
  (``analysis.llm``; the legacy ``analysis.opencode`` block still works).
  The key itself is loaded from ``.env``. Never logs headers/keys.
- One code path: POST /chat/completions (OpenAI-compatible), so the endpoint
  must serve that wire. Under Command Code that means non-Claude models:
  Claude IDs answer on /messages only and would 404 here.
- GET /models lists available IDs (used by --list-models).
- Exponential backoff on 429/5xx.

No SDK dependency; httpx only.
"""

from __future__ import annotations

import json
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

# Local-dev convenience only: reuse an existing `pi` / Command Code login so the
# key does not have to be duplicated into .env. Servers should set the env var.
_AUTH_FILES = (
    Path.home() / ".pi" / "agent" / "auth.json",
    Path.home() / ".commandcode" / "auth.json",
)


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


def _llm_cfg() -> dict:
    """The provider block for the HTTP stages (``analysis.llm``).

    ``analysis.opencode`` is the pre-Command-Code name and is still read so old
    checkouts/branches keep working; ``analysis.llm`` wins when both exist.
    """
    cfg = load_analysis_config()
    merged = dict(cfg.get("opencode", {}) or {})
    merged.update(cfg.get("llm", {}) or {})
    return merged


def _auth_names() -> tuple[str, ...]:
    """Credential keys to look for in the auth files, derived from the base URL."""
    base_url = str(_llm_cfg().get("base_url") or "")
    if "commandcode.ai" in base_url:
        return ("commandcode", "command-code")
    if "opencode.ai" in base_url:
        return ("opencode-go", "opencode")
    if "openrouter.ai" in base_url:
        return ("openrouter",)
    return ()


def _key_from_auth_files(names: tuple[str, ...]) -> str:
    for path in _AUTH_FILES:
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if not isinstance(data, dict):
            continue
        for name in names:
            entry = data.get(name)
            if isinstance(entry, str) and entry.strip():
                return entry.strip()
            if isinstance(entry, dict):
                for field in ("key", "apiKey", "access", "refresh"):
                    value = entry.get(field)
                    if isinstance(value, str) and value.strip():
                        return value.strip()
    return ""


def get_api_key() -> str:
    _load_env_file()
    env_name = str(_llm_cfg().get("api_key_env") or "OPENCODE_API_KEY")
    key = os.environ.get(env_name, "").strip()
    if key:
        return key
    key = _key_from_auth_files(_auth_names())
    if key:
        return key
    raise RuntimeError(
        f"{env_name} not set and no matching local login found. Add it to .env "
        "(the file is gitignored), or point analysis.llm.api_key_env at the env "
        "var holding the key."
    )


def get_session_id() -> str:
    """Return the stable session ID used for this pipeline process."""
    return _SESSION_ID


def _request_headers() -> dict[str, str]:
    headers = {
        "Authorization": f"Bearer {get_api_key()}",
        "Content-Type": "application/json",
        "User-Agent": _USER_AGENT,
    }
    # Session hint only meaningful to the OpenCode gateway; harmless elsewhere.
    if "opencode.ai" in _base_url():
        headers["x-opencode-session"] = get_session_id()
    return headers


def _base_url() -> str:
    return str(_llm_cfg().get("base_url") or "https://api.commandcode.ai/provider/v1").rstrip("/")


def _timeout() -> float:
    return float(_llm_cfg().get("timeout_sec", 300))


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
