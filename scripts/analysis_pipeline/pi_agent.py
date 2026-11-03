"""
Shared headless `pi` agent runner.

Single responsibility: spawn a headless ``pi -p --mode json`` run and translate
its NDJSON event stream into the result contract shared by Stage 3b (research,
``research.py``) and Stage 4 (write, ``write.py``) — final text, correlated tool
trace, usage/cost, stop reason.

Design notes (see PI_RESEARCH_AGENT_PLAN.md §3.1/§4.1 and
PI_WRITER_AGENT_PLAN.md §4.1):

- The JSONL stream is split on LF only. ``str.splitlines()`` also splits on
  U+2028/U+2029/``\\x85``, which can appear inside JSON strings and break
  records.
- ``tool_execution_end`` has no ``args`` field — arguments are paired from the
  matching ``tool_execution_start`` event via ``toolCallId``.
- The subprocess cwd must be the project root so pi discovers ``.pi`` project
  config; ``XDG_CONFIG_HOME`` is redirected to ``<project>/.pi/xdg``.
- Credentials come from ``.env`` (reuse ``opencode_client._load_env_file``)
  inherited through the subprocess environment.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import signal
import subprocess
import time
from pathlib import Path
from typing import Any

import opencode_client

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

_DEFAULT_PROVIDER = "commandcode"
_DEFAULT_MODEL = "deepseek/deepseek-v4.1-flash"
_DEFAULT_THINKING = "off"
_DEFAULT_TIMEOUT_SEC = 900
_DEFAULT_TOOLS = ["exa_search", "fetch_content"]

_URL_RE = re.compile(r"https?://[^\s\"'<>\)\]]+")


def _as_int(value: Any) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    return 0


def _as_float(value: Any) -> float:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    return 0.0


def _normalise_thinking(cfg: dict[str, Any]) -> str:
    """YAML parses an unquoted ``off`` as boolean ``False``; map it back."""
    value = cfg.get("thinking", _DEFAULT_THINKING)
    if value is False:
        return "off"
    if value is True:
        return "high"
    if value is None or value == "":
        return _DEFAULT_THINKING
    return str(value)


def _clean_url(url: str) -> str:
    """Trim trailing punctuation that the URL regex may have swallowed."""
    return url.rstrip(",;.)]")


def _empty_result(cfg: dict[str, Any]) -> dict[str, Any]:
    return {
        "content": "",
        "tool_rounds": 0,
        "tool_trace": [],
        "usage": {"prompt_tokens": 0, "completion_tokens": 0, "reasoning_tokens": 0, "cost_usd": 0.0},
        "finish_reason": None,
        "mode": "pi_agent",
        "error": None,
        "pi_meta": {
            "provider": cfg.get("provider", _DEFAULT_PROVIDER),
            "model": cfg.get("model", _DEFAULT_MODEL),
            "session_id": "",
            "exit_code": None,
            "duration_s": 0.0,
            "thinking": _normalise_thinking(cfg),
            "retries": 0,
            "tool_calls": {},
            "tool_errors": {},
            "exa_cost_usd": 0.0,
            "settled": False,
        },
        "raw_jsonl": "",
    }


def _build_argv(
    cfg: dict[str, Any], user_path: Path, system_path: Path,
) -> list[str]:
    """Build the pi argv exactly per the §3.1 invocation contract."""
    provider = cfg.get("provider", _DEFAULT_PROVIDER)
    model = cfg.get("model", _DEFAULT_MODEL)
    thinking = _normalise_thinking(cfg)
    tools = cfg.get("tools", _DEFAULT_TOOLS) or []

    argv = ["pi", "-p", "@" + str(user_path), "--mode", "json"]
    if not bool(cfg.get("save_session", False)):
        argv.append("--no-session")
    if not bool(cfg.get("load_skills", False)):
        argv.append("--no-skills")
    if not bool(cfg.get("load_context_files", False)):
        argv.append("--no-context-files")
    argv += ["--no-mcp", "--no-prompt-templates"]
    if bool(cfg.get("approve", True)):
        argv.append("--approve")
    argv += [
        "--provider", str(provider),
        "--model", str(model),
        "--thinking", str(thinking),
    ]
    if tools:
        argv += ["-t", ",".join(str(t) for t in tools)]
    else:
        argv.append("--no-tools")
    argv += ["--system-prompt", str(system_path)]
    return argv


def _split_records(stdout: str) -> list[str]:
    """Split the NDJSON stream on LF only (never ``str.splitlines``)."""
    records: list[str] = []
    for raw in stdout.split("\n"):
        line = raw.rstrip("\r")
        if line.strip():
            records.append(line)
    return records


def _parse_jsonl(stdout: str) -> list[dict[str, Any]]:
    """Parse the NDJSON stream, skipping unparseable lines."""
    events: list[dict[str, Any]] = []
    for line in _split_records(stdout):
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            events.append(obj)
    return events


def _spawn_pi(
    argv: list[str], env: dict[str, str], cwd: Path, timeout_s: float,
) -> tuple[int | None, str, str, bool]:
    """Run pi. Returns (exit_code, stdout, stderr, timed_out).

    On timeout the whole process group is killed (``start_new_session=True``)
    and stdout/stderr are drained so the recorded stream is complete.
    """
    proc = subprocess.Popen(
        argv,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
        cwd=str(cwd),
        text=True,
        start_new_session=True,
    )
    try:
        out, err = proc.communicate(timeout=timeout_s)
        return proc.returncode, out or "", err or "", False
    except subprocess.TimeoutExpired:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except (OSError, ProcessLookupError, PermissionError):
            try:
                proc.kill()
            except OSError:
                pass
        try:
            out, err = proc.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            out, err = "", ""
        return proc.returncode, out or "", err or "", True


def _build_result(
    events: list[dict[str, Any]],
    cfg: dict[str, Any],
    *,
    exit_code: int | None,
    duration_s: float,
    stderr: str,
    timed_out: bool,
    timeout_s: float,
) -> dict[str, Any]:
    result = _empty_result(cfg)
    meta = result["pi_meta"]
    meta["exit_code"] = exit_code
    meta["duration_s"] = round(duration_s, 3)

    starts: dict[str, dict[str, Any]] = {}
    for event in events:
        etype = event.get("type")
        if etype == "tool_execution_start":
            call_id = event.get("toolCallId")
            if call_id is not None:
                starts[str(call_id)] = event
        elif etype == "session":
            session_id = event.get("id")
            if session_id:
                meta["session_id"] = str(session_id)
        elif etype == "auto_retry_start":
            meta["retries"] = int(meta["retries"]) + 1
        elif etype == "agent_settled":
            meta["settled"] = True

    trace: list[dict[str, Any]] = []
    tool_calls: dict[str, int] = {}
    tool_errors: dict[str, int] = {}
    exa_cost = 0.0
    start_count = 0
    for event in events:
        if event.get("type") != "tool_execution_end":
            continue
        call_id = str(event.get("toolCallId", ""))
        name = str(event.get("toolName", ""))
        start_event = starts.get(call_id)
        if start_event is not None:
            start_count += 1
        trace.append({
            "tool_call_id": call_id,
            "tool_name": name,
            "arguments": (start_event or {}).get("args", {}) or {},
            "result": event.get("result") or {},
        })
        tool_calls[name] = tool_calls.get(name, 0) + 1
        res = event.get("result") or {}
        if isinstance(res, dict) and res.get("isError"):
            tool_errors[name] = tool_errors.get(name, 0) + 1
        if name == "exa_search":
            for query in (res.get("details") or {}).get("queries") or []:
                if isinstance(query, dict):
                    exa_cost += _as_float(query.get("costDollars"))

    result["tool_trace"] = trace
    meta["tool_calls"] = tool_calls
    meta["tool_errors"] = tool_errors
    meta["exa_cost_usd"] = exa_cost

    prompt_tokens = 0
    completion_tokens = 0
    reasoning_tokens = 0
    cost_usd = 0.0
    last_text = ""
    last_finish: str | None = None
    last_provider: str | None = None
    last_model: str | None = None
    toolcall_blocks = 0
    for event in events:
        if event.get("type") != "message_end":
            continue
        message = event.get("message")
        if not isinstance(message, dict) or message.get("role") != "assistant":
            continue
        usage = message.get("usage") or {}
        prompt_tokens += _as_int(usage.get("input"))
        completion_tokens += _as_int(usage.get("output"))
        reasoning_tokens += _as_int(usage.get("reasoning"))
        cost_usd += _as_float((usage.get("cost") or {}).get("total"))
        if message.get("provider"):
            last_provider = str(message["provider"])
        if message.get("model"):
            last_model = str(message["model"])
        blocks = message.get("content") or []
        texts = [
            block["text"] for block in blocks
            if isinstance(block, dict)
            and block.get("type") == "text"
            and isinstance(block.get("text"), str)
            and block["text"].strip()
        ]
        if texts:
            last_text = "\n".join(texts)
            last_finish = message.get("stopReason")
        if any(
            isinstance(block, dict) and block.get("type") in ("toolCall", "tool_call")
            for block in blocks
        ):
            toolcall_blocks += 1

    if last_provider:
        meta["provider"] = last_provider
    if last_model:
        meta["model"] = last_model

    result["content"] = last_text
    result["finish_reason"] = last_finish
    result["usage"] = {
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "reasoning_tokens": reasoning_tokens,
        "cost_usd": cost_usd,
    }
    # Fixture streams carry explicit toolCall content blocks; fall back to
    # counting tool_execution_start events when they are absent.
    result["tool_rounds"] = toolcall_blocks if toolcall_blocks else start_count

    if timed_out:
        result["error"] = f"pi timed out after {timeout_s:g}s"
    elif exit_code not in (0, None):
        tail = (stderr or "")[-1000:]
        result["error"] = f"pi exited with code {exit_code}: {tail}".rstrip()
    elif not last_text:
        result["error"] = "pi produced no assistant text"

    return result


def run_agent(
    system_prompt: str,
    user_prompt: str,
    *,
    cfg: dict[str, Any],
    run_dir: Path,
    project_root: Path | None = None,
) -> dict[str, Any]:
    """Run the pi agent once and return the translated result contract.

    Writes ``run_dir/system.md`` and ``run_dir/user.md`` before spawning and
    ``run_dir/events.jsonl`` (raw stdout) after. Always returns the full result
    dict shape; failures are surfaced through ``error``/``pi_meta``.
    """
    cfg = dict(cfg or {})
    run_dir = Path(run_dir)
    run_dir.mkdir(parents=True, exist_ok=True)
    project_root = Path(project_root) if project_root is not None else _PROJECT_ROOT

    system_path = (run_dir / "system.md").resolve()
    user_path = (run_dir / "user.md").resolve()
    system_path.write_text(system_prompt, encoding="utf-8")
    user_path.write_text(user_prompt, encoding="utf-8")

    opencode_client._load_env_file()
    env = dict(os.environ)
    env["XDG_CONFIG_HOME"] = str(project_root / ".pi" / "xdg")

    if shutil.which("pi") is None:
        result = _empty_result(cfg)
        result["error"] = "pi binary not found in PATH"
        (run_dir / "events.jsonl").write_text("", encoding="utf-8")
        return result

    argv = _build_argv(cfg, user_path, system_path)
    timeout_s = float(cfg.get("timeout_sec", _DEFAULT_TIMEOUT_SEC))
    start = time.perf_counter()
    try:
        exit_code, stdout, stderr, timed_out = _spawn_pi(
            argv, env, project_root, timeout_s,
        )
    except FileNotFoundError:
        result = _empty_result(cfg)
        result["error"] = "pi binary not found in PATH"
        result["pi_meta"]["duration_s"] = round(time.perf_counter() - start, 3)
        (run_dir / "events.jsonl").write_text("", encoding="utf-8")
        return result
    duration_s = time.perf_counter() - start

    stdout = stdout or ""
    (run_dir / "events.jsonl").write_text(stdout, encoding="utf-8")

    events = _parse_jsonl(stdout)
    result = _build_result(
        events,
        cfg,
        exit_code=exit_code,
        duration_s=duration_s,
        stderr=stderr,
        timed_out=timed_out,
        timeout_s=timeout_s,
    )
    result["raw_jsonl"] = stdout
    return result


def extract_tool_urls(tool_trace: list[dict[str, Any]]) -> set[str]:
    """Return raw, deduped URLs found in a pi tool trace (no normalization).

    ``exa_search`` URLs come from the structured ``details.queries[].results[]``
    payload with a text-regex fallback; ``fetch_content`` URLs come from the
    call arguments plus the ``details``/``structuredContent`` result shapes and
    the same regex fallback.
    """

    def _add_from_text(container: Any) -> None:
        for block in container or []:
            if isinstance(block, dict) and isinstance(block.get("text"), str):
                for match in _URL_RE.findall(block["text"]):
                    urls.add(_clean_url(match))

    urls: set[str] = set()
    for entry in tool_trace:
        if not isinstance(entry, dict):
            continue
        name = entry.get("tool_name")
        arguments = entry.get("arguments") or {}
        result = entry.get("result") or {}
        if not isinstance(arguments, dict):
            arguments = {}
        if not isinstance(result, dict):
            result = {}

        if name == "exa_search":
            details = result.get("details") or {}
            for query in details.get("queries") or []:
                if not isinstance(query, dict):
                    continue
                for item in query.get("results") or []:
                    if isinstance(item, dict) and item.get("url"):
                        urls.add(str(item["url"]))
            _add_from_text(result.get("content"))

        elif name == "fetch_content":
            if arguments.get("url"):
                urls.add(str(arguments["url"]))
            for url in arguments.get("urls") or []:
                if url:
                    urls.add(str(url))
            for url in (result.get("details") or {}).get("urls") or []:
                if isinstance(url, str) and url:
                    urls.add(url)
            for item in (result.get("structuredContent") or {}).get("urls") or []:
                if isinstance(item, dict) and item.get("url"):
                    urls.add(str(item["url"]))
            _add_from_text(result.get("content"))

    return urls
