"""
Thin client for the OpenCode Go subscription API (OpenAI-compatible surface).

- Base URL + key read from config.yaml (analysis.opencode.base_url) and the
  OPENCODE_API_KEY env var (loaded from .env). Never logs headers/keys.
- One code path: POST /chat/completions (OpenAI-compatible). Models that are
  only Anthropic-native are avoided per the implementation plan (§4).
- GET /models lists available IDs (used by --list-models).
- Exponential backoff on 429/5xx.
- chat_with_tools() adds an OpenAI-compatible tool-calling loop for the
  external-research stage.

No SDK dependency; httpx only.
"""

from __future__ import annotations

import concurrent.futures
import json
import os
import time
import uuid
from pathlib import Path
from typing import Any, Callable

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
    # return (cfg.get("opencode", {}) or {}).get("base_url", "https://ollama.com/api/chat")
    # return (cfg.get("opencode", {}) or {}).get("base_url", "https://opencode.ai/zen/go/v1")


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


def _model_call(
    model: str,
    messages: list[dict[str, Any]],
    tools: list[dict[str, Any]] | None = None,
    temperature: float = 0.0,
    max_tokens: int = 4096,
    reasoning_effort: str | None = None,
    retries: int = 4,
    timeout_sec: float | None = None,
) -> dict[str, Any]:
    """Low-level POST to /chat/completions. Returns parsed JSON response dict."""
    headers = _request_headers()
    payload: dict[str, Any] = {
        "model": model,
        "temperature": temperature,
        "max_tokens": max_tokens,
        "messages": messages,
    }
    if tools:
        payload["tools"] = tools
    if reasoning_effort:
        payload["reasoning_effort"] = reasoning_effort

    delay = 2.0
    last_err: str | None = None
    for _attempt in range(retries):
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
            return r.json()
        except httpx.HTTPError as e:
            last_err = str(e)
            time.sleep(delay)
            delay *= 2
            continue
    raise APIError(f"model call failed after {retries} retries: {last_err}")


def chat_with_tools(
    model: str,
    messages: list[dict[str, Any]],
    tools: list[dict[str, Any]],
    tool_executor: Callable[[str, dict[str, Any]], dict[str, Any]],
    *,
    max_tool_rounds: int = 6,
    max_parallel_calls: int = 8,
    temperature: float = 0.0,
    max_tokens: int = 4096,
    reasoning_effort: str | None = None,
    timeout_sec: float | None = None,
) -> dict[str, Any]:
    """OpenAI-compatible tool-calling loop.

    Sends messages + tools to the model. If the model returns tool_calls,
    executes them via `tool_executor`, appends results, and loops up to
    `max_tool_rounds` times. Tool calls within a round execute in parallel
    (bounded by `max_parallel_calls`).

    Returns:
        {
            "content": str,              # final assistant text
            "tool_rounds": int,          # rounds where tools were called
            "tool_trace": list[dict],    # full trace of calls and results
            "usage": {"prompt_tokens": int, "completion_tokens": int},
            "mode": "llm_tools" | "deterministic_fallback",
            "error": str | None,
        }
    """
    working_messages = list(messages)
    total_prompt = 0
    total_completion = 0
    tool_trace: list[dict] = []
    round_count = 0
    mode: str = "llm_tools"
    last_content: str = ""
    last_finish_reason: str | None = None

    while round_count < max_tool_rounds:
        print(
            f"  [RESEARCH LLM] request round={round_count + 1}/{max_tool_rounds} "
            f"model={model} messages={len(working_messages)}",
            flush=True,
        )
        try:
            data = _model_call(
                model=model,
                messages=working_messages,
                tools=tools,
                temperature=temperature,
                max_tokens=max_tokens,
                reasoning_effort=reasoning_effort,
                timeout_sec=timeout_sec,
            )
        except APIError as exc:
            return {
                "content": last_content,
                "tool_rounds": round_count,
                "tool_trace": tool_trace,
                "usage": {"prompt_tokens": total_prompt, "completion_tokens": total_completion},
                "mode": "deterministic_fallback" if round_count == 0 else mode,
                "error": str(exc),
            }

        usage = data.get("usage", {}) or {}
        total_prompt += usage.get("prompt_tokens", 0)
        total_completion += usage.get("completion_tokens", 0)

        choice = data["choices"][0] if data.get("choices") else {}
        message = choice.get("message", {}) or {}
        last_finish_reason = choice.get("finish_reason")

        tool_calls = message.get("tool_calls") or []
        content = message.get("content") or ""

        print(
            f"  [RESEARCH LLM] response round={round_count + 1} "
            f"tool_calls={len(tool_calls)} content_chars={len(content)}",
            flush=True,
        )

        if content:
            last_content = content

        if not tool_calls:
            working_messages.append(message)
            return {
                "content": content or last_content,
                "tool_rounds": round_count,
                "tool_trace": tool_trace,
                "usage": {"prompt_tokens": total_prompt, "completion_tokens": total_completion},
                "mode": mode,
                "error": None,
                "finish_reason": last_finish_reason,
            }

        round_count += 1
        working_messages.append(message)

        tool_results: list[dict[str, Any]] = []
        bounded_calls = tool_calls[:max_parallel_calls]

        def _execute_one(tc: dict) -> dict:
            fn_name = tc.get("function", {}).get("name", "")
            try:
                fn_args = json.loads(tc.get("function", {}).get("arguments", "{}"))
            except json.JSONDecodeError:
                fn_args = {}
            result = tool_executor(fn_name, fn_args)
            return {
                "tool_call_id": tc.get("id", ""),
                "tool_name": fn_name,
                "arguments": fn_args,
                "result": result,
            }

        with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(bounded_calls), max_parallel_calls)) as pool:
            futures = [pool.submit(_execute_one, tc) for tc in bounded_calls]
            for future in concurrent.futures.as_completed(futures):
                tool_results.append(future.result())

        for tr in tool_results:
            tool_trace.append(tr)
            result_str = json.dumps(tr["result"], ensure_ascii=False)
            working_messages.append({
                "role": "tool",
                "tool_call_id": tr["tool_call_id"],
                "content": result_str,
            })

    # Max tool rounds exhausted. Synthesize with tools disabled so a model that
    # keeps requesting searches cannot leave us with an unprocessed tool call.
    print(
        f"  [RESEARCH LLM] final synthesis request after {round_count} tool rounds "
        f"model={model}",
        flush=True,
    )
    working_messages.append({
        "role": "user",
        "content": (
            "The tool-call budget is exhausted. Do not call tools. "
            "Synthesize the available evidence now and output only the required YAML."
        ),
    })
    try:
        data = _model_call(
            model=model,
            messages=working_messages,
            tools=None,
            temperature=temperature,
            max_tokens=max_tokens,
            reasoning_effort=reasoning_effort,
            timeout_sec=timeout_sec,
        )
        final_content = ""
        final_finish_reason: str | None = None
        if data.get("choices"):
            final_choice = data["choices"][0]
            final_content = final_choice.get("message", {}).get("content", "") or ""
            final_finish_reason = final_choice.get("finish_reason")
        usage = data.get("usage", {}) or {}
        total_prompt += usage.get("prompt_tokens", 0)
        total_completion += usage.get("completion_tokens", 0)
        content_out = final_content or last_content
        final_error = None
        if not content_out:
            final_error = "final synthesis returned no content"
        elif final_finish_reason in {"length", "max_tokens"}:
            final_error = "final synthesis truncated at max_tokens"
        return {
            "content": content_out,
            "tool_rounds": round_count,
            "tool_trace": tool_trace,
            "usage": {"prompt_tokens": total_prompt, "completion_tokens": total_completion},
            "mode": mode,
            "error": final_error,
            "finish_reason": final_finish_reason or last_finish_reason,
        }
    except APIError as exc:
        return {
            "content": last_content,
            "tool_rounds": round_count,
            "tool_trace": tool_trace,
            "usage": {"prompt_tokens": total_prompt, "completion_tokens": total_completion},
            "mode": mode,
            "error": str(exc),
            "finish_reason": last_finish_reason,
        }
