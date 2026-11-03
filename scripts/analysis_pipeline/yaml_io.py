"""
Fail-loud YAML loading for pipeline artifacts.

Model output and on-disk artifacts are parsed in several stages (research
brief, chunk sheets, red-flag brief/trace, research plan). A silent
``except: {}`` there turns a corrupt artifact into an empty one, which the
pipeline then reports as a successful stage. This module keeps one
conservative repair pass (fences, trailing commas, unbalanced flow
collections) and otherwise raises ``YamlLoadError`` — it never returns ``{}``
on failure.

Usage:
    from yaml_io import YamlLoadError, load_file_with_repair, loads_with_repair

    brief = loads_with_repair(raw_model_output, context="research brief PTBA Q1 2026")
    sheet = load_file_with_repair(path)
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

_TRAILING_COMMA_RE = re.compile(r",(\s*[}\]])")
# A model sometimes leaves a comma after a complete quoted/flow value in a
# block mapping (``explanation: "text",``). That comma is not part of the
# value and YAML rejects it. Only touch values that are unambiguously closed.
_TRAILING_VALUE_COMMA_RE = re.compile(
    r"^(?P<prefix>\s*(?:-\s+)?[A-Za-z_][\w-]*:\s*)"
    r"(?P<value>\"(?:[^\"\\]|\\.)*\"|'(?:[^']|'')*'|\[.*\]|\{.*\})"
    r",\s*$",
    re.MULTILINE,
)
_FLOW_PAIRS = {"]": "[", "}": "{"}


class YamlLoadError(ValueError):
    """A YAML document could not be parsed, even after the repair pass."""

    def __init__(self, context: str, parser_message: str):
        self.context = context
        self.parser_message = parser_message
        super().__init__(f"Could not parse {context} as YAML: {parser_message}")


def strip_markdown_fences(text: str) -> str:
    """Drop a single surrounding ``` fence (with an optional language tag)."""
    stripped = text.strip()
    if not stripped.startswith("```"):
        return text
    lines = stripped.splitlines()
    if lines and lines[0].lstrip().startswith("```"):
        lines = lines[1:]
    if lines and lines[-1].strip().startswith("```"):
        lines = lines[:-1]
    return "\n".join(lines)


def _remove_trailing_commas(text: str) -> str:
    text = _TRAILING_COMMA_RE.sub(r"\1", text)
    return _TRAILING_VALUE_COMMA_RE.sub(r"\g<prefix>\g<value>", text)


def _balance_flow_collections(text: str) -> str:
    """Append missing closers for unbalanced ``[ ]`` / ``{ }`` flow collections."""
    stack: list[str] = []
    in_single = False
    in_double = False
    i = 0
    while i < len(text):
        ch = text[i]
        if ch == "'" and not in_double:
            # YAML escapes a single quote inside a single-quoted scalar by doubling it.
            if in_single and i + 1 < len(text) and text[i + 1] == "'":
                i += 2
                continue
            in_single = not in_single
        elif ch == '"' and not in_single:
            # Ignore escaped quotes inside a double-quoted scalar.
            if not (i > 0 and text[i - 1] == "\\"):
                in_double = not in_double
        elif not in_single and not in_double:
            if ch in "[{":
                stack.append(ch)
            elif ch in "]}":
                if stack and stack[-1] == _FLOW_PAIRS[ch]:
                    stack.pop()
        i += 1
    if not stack:
        return text
    closers = "".join("]" if opener == "[" else "}" for opener in reversed(stack))
    return text + closers


def loads_with_repair(text: str, *, context: str = "yaml") -> Any:
    """Parse YAML, with one conservative repair pass before failing.

    Raises ``YamlLoadError`` when the document is still invalid after the
    repair pass. An empty document loads as ``None`` (a valid YAML value); it
    is never silently replaced with ``{}``.
    """
    candidate = strip_markdown_fences(text)
    try:
        return yaml.safe_load(candidate)
    except yaml.YAMLError as first_error:
        repaired = _balance_flow_collections(_remove_trailing_commas(candidate))
        try:
            return yaml.safe_load(repaired)
        except yaml.YAMLError:
            raise YamlLoadError(context, str(first_error)) from first_error


def load_file_with_repair(path: str | Path, *, context: str | None = None) -> Any:
    """Read ``path`` as UTF-8 and parse it with :func:`loads_with_repair`."""
    path = Path(path)
    return loads_with_repair(
        path.read_text(encoding="utf-8"),
        context=context or str(path),
    )


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description="Parse a YAML file, repairing once or failing loudly")
    p.add_argument("path")
    a = p.parse_args()
    try:
        print(yaml.safe_dump(load_file_with_repair(a.path), sort_keys=False))
    except YamlLoadError as e:
        raise SystemExit(str(e))
