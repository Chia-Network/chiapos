"""go.mod and go.sum grammar for a Dependabot diff."""

from __future__ import annotations

import re

from dependency_manifest_versions import _exact_body, _version_body

_GO_MOD_RE = re.compile(
    r"^\s*(?:require\s+)?([A-Za-z0-9_.\-]+(?:/[A-Za-z0-9_.\-]+)+)\s+(v\S+)",
    re.M,
)
_GO_SUM_RE = re.compile(r"^(\S+)\s+(v\S+)\s+", re.M)
_GO_BLOCK_STARTS = frozenset({"require(", "replace(", "exclude(", "retract("})


def _go_mod_lines(text: str) -> list[tuple[str, str]]:
    """Classify go.mod lines. Only require lines are entries."""
    classified: list[tuple[str, str]] = []
    block = ""
    for line in text.splitlines(keepends=True):
        compact = "".join(line.split("//", 1)[0].split())
        if block:
            classified.append(("entry" if block == "require" else "residue", line))
            if compact == ")":
                block = ""
            continue
        if compact in _GO_BLOCK_STARTS:
            block = compact[:-1]
            classified.append(("residue", line))
            continue
        if compact.startswith(("replace", "exclude", "retract")):
            classified.append(("residue", line))
            continue
        classified.append(("entry", line))
    return classified


def _go_mod_parts(text: str) -> tuple[dict[str, list[tuple[str, str]]], str]:
    entries: dict[str, list[tuple[str, str]]] = {}
    residue: list[str] = []
    for kind, line in _go_mod_lines(text):
        code = line.split("//", 1)[0]
        match = _GO_MOD_RE.match(code) if kind == "entry" else None
        if match:
            entries.setdefault(match.group(1), []).append((match.group(2), code.strip()))
            continue
        residue.append(line)
    return entries, "".join(residue)


def _go_sum_version(token: str) -> str:
    """Module version, without the `/go.mod` checksum suffix."""
    return token.split("/go.mod", 1)[0]


def _go_sum_parts(text: str) -> tuple[dict[str, list[tuple[str, str]]], str]:
    """One block per module version. Versions of a module stay a set."""
    grouped: dict[str, dict[str, list[str]]] = {}
    residue: list[str] = []
    for line in text.splitlines(keepends=True):
        body = line.strip()
        match = _GO_SUM_RE.match(body)
        if not body or not match:
            residue.append(line)
            continue
        module, version = match.group(1), _go_sum_version(match.group(2))
        grouped.setdefault(module, {}).setdefault(version, []).append(body)
    entries = {
        module: [(version, "\n".join(sorted(rows))) for version, rows in sorted(by_version.items())]
        for module, by_version in grouped.items()
    }
    return entries, "".join(residue)


def _version_map(blocks: list[tuple[str, str]]) -> dict[str, str] | None:
    found: dict[str, str] = {}
    for version, payload in blocks:
        if version in found:
            return None
        found[version] = payload
    return found


def _version_named(versions: dict[str, str], listed: str) -> str | None:
    body = _exact_body(listed)
    if not body:
        return None
    found = [version for version in versions if _version_body(version) == body]
    if len(found) != 1:
        return None
    return found[0]


def _sum_update_covers(old: dict[str, str], new: dict[str, str], frm: str, to: str) -> bool:
    """True when `to` is in the new set and `from` was in the old set.

    Removing `from` matches. Keeping those lines and adding `to` matches.
    A checksum rewrite of any version that stays does not.
    """
    to_key = _version_named(new, to)
    from_key = _version_named(old, frm)
    if to_key is None or from_key is None or _version_body(from_key) == _version_body(to_key):
        return False
    removed = set(old) - set(new)
    added = set(new) - set(old)
    if removed - {from_key} or added - {to_key}:
        return False
    stayed = set(old) & set(new)
    if any(old[key] != new[key] for key in stayed):
        return False
    return bool(removed or added)


def _go_sum_explained(
    name: str,
    old_blocks: list[tuple[str, str]],
    new_blocks: list[tuple[str, str]],
    listed: dict[str, set[tuple[str, str]]],
) -> bool:
    old = _version_map(old_blocks)
    new = _version_map(new_blocks)
    if old is None or new is None:
        return False
    if old == new:
        return True
    return any(_sum_update_covers(old, new, frm, to) for frm, to in listed.get(name, ()))
