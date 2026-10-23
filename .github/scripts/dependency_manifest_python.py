"""requirements, pyproject, setup.py, setup.cfg and Pipfile grammar for a Dependabot diff."""

from __future__ import annotations

import re

_REQ_RE = re.compile(
    r"^([A-Za-z0-9][A-Za-z0-9._-]*)\s*(?:\[[^\]]*\])?\s*((?:===|==|~=|!=|>=|<=|>|<)\s*[^\s#;,]+)",
    re.M,
)
# A TOML table header, and a `name = "spec"` or `name = { version = "spec" }` dependency line.
_TOML_TABLE_RE = re.compile(r"^\s*\[+\s*([^\]]+?)\s*\]+\s*$")
_TOML_DEP_RE = re.compile(
    r'^\s*"?([A-Za-z0-9][A-Za-z0-9._-]*)"?\s*=\s*(?:\{[^}]*?\bversion\s*=\s*)?["\']([^"\']+)["\']'
)
_QUOTED_RE = re.compile(r'["\']([^"\']+)["\']')
_PYTHON_MANIFESTS = frozenset({"pyproject.toml", "setup.py", "setup.cfg", "Pipfile"})
# PEP 503: lowercase, and collapse each run of `-`, `_`, or `.` to one `-`.
_PEP503_SEPARATORS = re.compile(r"[-_.]+")


def _requirement_name(name: str) -> str:
    """PEP 503 name. Requirements parsing and Dependabot names both use this."""
    return _PEP503_SEPARATORS.sub("-", name).lower()


def _requirement_specs(line: str, table: str) -> list[tuple[str, str]]:
    """(name, version) pairs on one requirements, setup or TOML line.

    A pinned PEP 508 line, or each quoted PEP 508 string, is an entry. Inside a
    Poetry or Pipfile dependency table a `name = "spec"` line is one as well.
    """
    if table.endswith("dependencies") or table in {"packages", "dev-packages"}:
        match = _TOML_DEP_RE.match(line)
        if match and match.group(1).lower() != "python":
            return [(match.group(1), match.group(2))]
    whole = _REQ_RE.match(line.strip())
    candidates = [whole] if whole else [_REQ_RE.match(text.strip()) for text in _QUOTED_RE.findall(line)]
    return [(match.group(1), match.group(2)) for match in candidates if match]


def _req_parts(text: str) -> tuple[dict[str, list[tuple[str, str]]], str]:
    entries: dict[str, list[tuple[str, str]]] = {}
    residue: list[str] = []
    table = ""
    for line in text.splitlines(keepends=True):
        header = _TOML_TABLE_RE.match(line)
        table = header.group(1) if header else table
        found = _requirement_specs(line, table)
        for name, version in found:
            entries.setdefault(_requirement_name(name), []).append((version, line.strip()))
        if not found:
            residue.append(line)
    return entries, "".join(residue)
