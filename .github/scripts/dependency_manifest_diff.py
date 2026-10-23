"""Manifest and lockfile changes checked against a Dependabot update list.

Each ecosystem's grammar lives in its own module (dependency_manifest_npm,
dependency_manifest_go, dependency_manifest_python). Those entries are the only
source for the changed-package labels and for whether the diff matches the
listed update.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

from dependency_manifest_go import _go_mod_parts, _go_sum_explained, _go_sum_parts
from dependency_manifest_npm import (
    _fold_residue,
    _json_parts,
    _json_witnesses,
    _payloads_match,
    _registry_block,
    _yarn_parts,
    _yarn_witnesses,
)
from dependency_manifest_python import _PYTHON_MANIFESTS, _req_parts, _requirement_name
from dependency_manifest_versions import _reached_listed_version

# npm and Yarn locks whose unlisted names may be registry tarball residue once every listed bump is explained.
TRANSITIVE_LOCK_NAMES = frozenset({"package-lock.json", "npm-shrinkwrap.json", "yarn.lock"})
# Versions a lock resolves a dependency to, keyed by package name for npm and
# by `name@range` descriptor for Yarn. The forms differ because the locks do:
# a lockfile v1 records no root spec, so npm can only witness the top-level
# copy of a name, while Yarn records one stanza per descriptor. They never
# collide: an npm name has no `@` past its scope, a descriptor always does.
Witnesses = dict[str, frozenset[str]]
_OPAQUE_LOCKS = {
    "pnpm-lock.yaml",
    "Cargo.lock",
    "poetry.lock",
    "Pipfile.lock",
    "Gemfile.lock",
    "composer.lock",
}


def _manifest_kind(path: str) -> str:
    name = Path(path).name
    posix = "/" + path.replace("\\", "/")
    if name in {"package.json", "package-lock.json", "npm-shrinkwrap.json"}:
        return "json"
    if name == "go.mod":
        return "go.mod"
    if name == "go.sum":
        return "go.sum"
    if name == "yarn.lock":
        return "yarn"
    if name in _OPAQUE_LOCKS:
        return "opaque"
    if name in _PYTHON_MANIFESTS:
        return "requirements"
    if (name.startswith("requirements") or "/requirements/" in posix) and name.endswith(".txt"):
        return "requirements"
    return ""


# Manifests and locks Dependabot bumps that this grammar has no parser for:
# Cargo, Docker, Swift (Package.swift, Package.resolved), Terraform, NuGet and
# composite actions (action.yml, action.yaml).
_UNPARSED_MANIFEST_NAMES = frozenset(
    {
        "Cargo.toml",
        "Dockerfile",
        "Package.swift",
        "Package.resolved",
        ".terraform.lock.hcl",
        "packages.config",
        "Directory.Packages.props",
        "action.yml",
        "action.yaml",
    }
)
_UNPARSED_MANIFEST_SUFFIXES = (".tf", ".csproj", ".fsproj", ".vbproj")


def _unparsed_manifest(path: str) -> bool:
    """True when `path` is a manifest Dependabot bumps but this grammar has no parser for.

    A workflow, a composite action, a Dockerfile, Cargo.toml, a Swift package
    or its Package.resolved, *.tf or .terraform.lock.hcl, and a NuGet project,
    packages.config or Directory.Packages.props are real manifests; anything
    else this grammar does not parse is not a manifest.
    """
    name = Path(path).name
    if name in _UNPARSED_MANIFEST_NAMES or name.startswith("Dockerfile.") or name.endswith(_UNPARSED_MANIFEST_SUFFIXES):
        return True
    posix = "/" + path.replace("\\", "/")
    return "/.github/workflows/" in posix and name.endswith((".yml", ".yaml"))


def _canonical_package_name(kind: str, name: str) -> str:
    if kind == "requirements":
        return _requirement_name(name)
    return name


def _names_listed(kind: str, names: set[str], listed: set[str]) -> bool:
    known = {_canonical_package_name(kind, name) for name in listed}
    return all(_canonical_package_name(kind, name) in known for name in names)


def _listed_versions(updates: list[dict], *, kind: str = "") -> dict[str, set[tuple[str, str]]]:
    found: dict[str, set[tuple[str, str]]] = {}
    for item in updates:
        name = _canonical_package_name(kind, str(item.get("packageName") or "").strip())
        if not name:
            continue
        pair = (str(item.get("fromVersion") or "").strip(), str(item.get("toVersion") or "").strip())
        found.setdefault(name, set()).add(pair)
    return found


def _split_entries(kind: str, text: str) -> tuple[dict[str, tuple[str, str]], str] | None:
    if kind == "json":
        return _json_parts(text)
    if kind == "go.mod":
        return _go_mod_parts(text)
    if kind == "go.sum":
        return _go_sum_parts(text)
    if kind == "requirements":
        return _req_parts(text)
    if kind == "yarn":
        return _yarn_parts(text)
    return None


def _entry_blocks(entry: object) -> list[tuple[str, str]]:
    if isinstance(entry, tuple):
        return [entry]
    if isinstance(entry, list):
        return [item for item in entry if isinstance(item, tuple)]
    return []


def _stanza_entries(blocks: list[tuple[str, str]]) -> dict[str, tuple[str, str]] | None:
    found: dict[str, tuple[str, str]] = {}
    for version, payload in blocks:
        key = payload.split("\n", 1)[0]
        if not key or key in found:
            return None
        found[key] = (version, payload)
    return found


def _blocks_explain(
    name: str,
    old_blocks: list[tuple[str, str]],
    new_blocks: list[tuple[str, str]],
    listed: dict[str, set[tuple[str, str]]],
    locked: Witnesses,
    *,
    keyed: bool,
) -> bool:
    old_left = list(old_blocks)
    new_left = list(new_blocks)
    for block in list(old_left):
        if block in new_left:
            old_left.remove(block)
            new_left.remove(block)
    if not old_left and not new_left:
        return True
    if keyed:
        old_map = _stanza_entries(old_left)
        new_map = _stanza_entries(new_left)
        if old_map is None or new_map is None or set(old_map) != set(new_map):
            return False
        return all(_pair_explained(name, old_map[key], new_map[key], listed, locked) for key in old_map)
    pairs = _pair_blocks(old_left, new_left)
    if pairs is None:
        return False
    return all(_pair_explained(name, old, new, listed, locked) for old, new in pairs)


def _name_explained(
    kind: str,
    name: str,
    old_blocks: list[tuple[str, str]],
    new_blocks: list[tuple[str, str]],
    listed: dict[str, set[tuple[str, str]]],
    locked: Witnesses,
) -> bool:
    if kind == "go.sum":
        return _go_sum_explained(name, old_blocks, new_blocks, listed)
    return _blocks_explain(name, old_blocks, new_blocks, listed, locked, keyed=kind == "yarn")


def _single_version(entry: object) -> str | None:
    """The version when a name has one entry. Several entries are not one label."""
    blocks = _entry_blocks(entry)
    if len(blocks) > 1:
        return None
    if not blocks:
        return ""
    return blocks[0][0]


def _payload_id(payload: str) -> str:
    try:
        doc = json.loads(payload)
    except json.JSONDecodeError:
        return ""
    if not isinstance(doc, dict):
        return ""
    path = doc.get("path")
    if isinstance(path, str) and path:
        return "path:" + path
    if "spec" in doc:
        return "spec:" + str(doc.get("section") or "")
    return ""


def _pair_blocks(
    old: list[tuple[str, str]], new: list[tuple[str, str]]
) -> list[tuple[tuple[str, str], tuple[str, str]]] | None:
    """Pair lock paths. One unkeyed block pairs with one. Added or ambiguous paths do not."""
    if len(old) == 1 and len(new) == 1 and not _payload_id(old[0][1]) and not _payload_id(new[0][1]):
        return [(old[0], new[0])]
    old_ids = [_payload_id(block[1]) for block in old]
    new_ids = [_payload_id(block[1]) for block in new]
    if not all(old_ids) or not all(new_ids):
        return None
    if len(set(old_ids)) != len(old_ids) or len(set(new_ids)) != len(new_ids) or set(old_ids) != set(new_ids):
        return None
    new_by_id = dict(zip(new_ids, new))
    return [(block, new_by_id[key]) for key, block in zip(old_ids, old)]


def _residues_match(kind: str, old: str, new: str) -> bool:
    if old == new:
        return True
    if kind != "json":
        return False
    try:
        old_doc = json.loads(old)
        new_doc = json.loads(new)
    except json.JSONDecodeError:
        return False
    return _fold_residue(old_doc) == _fold_residue(new_doc)


def _payload_kind(payload: str) -> str:
    """'spec' for a manifest string spec, 'json' for a lock record, 'text' otherwise."""
    try:
        doc = json.loads(payload)
    except json.JSONDecodeError:
        return "text"
    if isinstance(doc, dict) and set(doc) == {"section", "spec"} and isinstance(doc.get("spec"), str):
        return "spec"
    return "json"


def _lock_text(payloads: list[str]) -> bool:
    """Yarn stanza text. A module or requirement line keeps its version in the entry label."""
    return any(
        re.search(r"(?m)^\s*(?:version|resolved|integrity|checksum|resolution)\b", payload) for payload in payloads
    )


def _payload_ok(old_blocks: list[tuple[str, str]], new_blocks: list[tuple[str, str]]) -> bool:
    """True when a string spec moved, a manifest line differs only in its version, or lock locators are canonical."""
    payloads = [payload for _version, payload in (*old_blocks, *new_blocks)]
    kinds = {_payload_kind(payload) for payload in payloads}
    if kinds <= {"spec"}:
        return True
    if "text" in kinds and not _lock_text(payloads):
        return _line_rest(old_blocks) == _line_rest(new_blocks)
    return _version_bump(old_blocks, new_blocks)


def _line_rest(blocks: list[tuple[str, str]]) -> list[str] | None:
    """Each manifest line with its version token removed, or None when a token is not on its line."""
    rest: list[str] = []
    for version, payload in blocks:
        if not version or version not in payload:
            return None
        rest.append(payload.replace(version, "\0", 1))
    return rest


def _version_bump(old_blocks: list[tuple[str, str]], new_blocks: list[tuple[str, str]]) -> bool:
    """True when each paired entry matches, or both resolved URLs are the registry tarball."""
    if old_blocks == new_blocks:
        return True
    pairs = _pair_blocks(old_blocks, new_blocks)
    if pairs is None:
        return False
    for old, new in pairs:
        if old == new:
            continue
        if old[0] == new[0] or not _payloads_match(old[1], new[1]):
            return False
    return True


def _pair_explained(
    name: str,
    old: tuple[str, str],
    new: tuple[str, str],
    listed: dict[str, set[tuple[str, str]]],
    locked: Witnesses,
) -> bool:
    """A package.json spec pair may use a shared range prefix when `locked` witnesses the listed version.

    The witness is the top-level npm copy of `name` or the Yarn stanza for
    the new `name@spec`; see Witnesses for why those are two keys.
    """
    specs = _payload_kind(old[1]) == "spec" and _payload_kind(new[1]) == "spec"
    empty: frozenset[str] = frozenset()
    witness = (locked.get(name, empty) | locked.get(f"{name}@{new[0]}", empty)) if specs else empty
    reached = _reached_listed_version(name, old[0], new[0], listed, locked=witness, mode="spec" if specs else "pin")
    return reached and _payload_ok([old], [new])


def _lock_covers_listed(
    kind: str,
    before: str,
    after: str,
    updates: list[dict],
    names: set[str],
    locked: Witnesses | None = None,
) -> bool:
    """True when a listed package moves to its listed version.

    A listed name must be explained; `locked`, the versions a lockfile in HEAD
    resolves each spec to, lets a shared `^` or `~` spec bump count as its
    listed bump. Every other name may change only its version and registry
    locators, or be added or removed as a registry tarball entry. Another
    resolved host, a same-version hash swap, leftover bytes, or a lock that
    never moves a listed package stays unresolved.
    """
    listed = _listed_versions(updates, kind=kind)
    old_split = _split_entries(kind, before)
    new_split = _split_entries(kind, after)
    if old_split is None or new_split is None:
        return False
    old_entries, old_residue = old_split
    new_entries, new_residue = new_split
    listed_moved = any(
        _canonical_package_name(kind, name) in listed
        and _entry_blocks(old_entries.get(name)) != _entry_blocks(new_entries.get(name))
        for name in set(old_entries) | set(new_entries)
    )
    named = any(_canonical_package_name(kind, name) in listed for name in names)
    if not listed_moved and not named:
        return False
    if not _residues_match(kind, old_residue, new_residue):
        return False
    for name in set(old_entries) | set(new_entries):
        old_blocks = _entry_blocks(old_entries.get(name))
        new_blocks = _entry_blocks(new_entries.get(name))
        if _canonical_package_name(kind, name) in listed:
            if not _name_explained(kind, name, old_blocks, new_blocks, listed, locked or {}):
                return False
            continue
        if not _registry_transitive(old_blocks, new_blocks):
            return False
    return True


Blocks = list[tuple[str, str]]


def _registry_transitive(old_blocks: Blocks, new_blocks: Blocks) -> bool:
    """npm and Yarn: paired entries are a registry version bump; a lone entry is a registry tarball."""
    if _version_bump(old_blocks, new_blocks):
        return True
    if not old_blocks or not new_blocks:
        return all(_registry_block(payload) for _version, payload in (*old_blocks, *new_blocks))
    old_by_id = {_payload_id(block[1]): block for block in old_blocks}
    new_by_id = {_payload_id(block[1]): block for block in new_blocks}
    if "" in old_by_id or "" in new_by_id or len(old_by_id) != len(old_blocks) or len(new_by_id) != len(new_blocks):
        return False
    common = old_by_id.keys() & new_by_id.keys()
    paired_old = [old_by_id[key] for key in sorted(common)]
    paired_new = [new_by_id[key] for key in sorted(common)]
    if not _version_bump(paired_old, paired_new):
        return False
    lone = [old_by_id[key] for key in old_by_id.keys() - common] + [new_by_id[key] for key in new_by_id.keys() - common]
    return all(_registry_block(payload) for _version, payload in lone)


def _changed_names(kind: str, before: str, after: str) -> set[str] | None:
    """Names whose one parsed version changed.

    Versions come from the same entries `_entries_explain` reads.
    A name with more than one entry is reported by `_multi_entry_names`.
    """
    old_split = _split_entries(kind, before)
    new_split = _split_entries(kind, after)
    if old_split is None or new_split is None:
        return None
    old_entries = old_split[0]
    new_entries = new_split[0]
    changed: set[str] = set()
    for name in set(old_entries) | set(new_entries):
        old_version = _single_version(old_entries.get(name))
        new_version = _single_version(new_entries.get(name))
        if old_version is None or new_version is None or old_version == new_version:
            continue
        changed.add(name)
    return changed


def _multi_entry_names(
    kind: str, before: str, after: str, updates: list[dict], locked: Witnesses | None = None
) -> set[str]:
    """Multi-entry names whose own blocks are not the listed change.

    Several Yarn descriptors for one package are normal. They are a gap
    only when those blocks are unexplained. An unchanged set is not.
    """
    old_split = _split_entries(kind, before)
    new_split = _split_entries(kind, after)
    if old_split is None or new_split is None:
        return set()
    old_entries = old_split[0]
    new_entries = new_split[0]
    listed = _listed_versions(updates, kind=kind)
    found: set[str] = set()
    for name in set(old_entries) | set(new_entries):
        old_blocks = _entry_blocks(old_entries.get(name))
        new_blocks = _entry_blocks(new_entries.get(name))
        if len(old_blocks) <= 1 and len(new_blocks) <= 1:
            continue
        if _name_explained(kind, name, old_blocks, new_blocks, listed, locked or {}):
            continue
        found.add(name)
    return found


def _entries_explain(kind: str, before: str, after: str, updates: list[dict], locked: Witnesses | None = None) -> bool:
    """True when every changed entry moved to its listed version.

    A lock entry is explained when it moves to the listed version and the
    rest is unchanged aside from registry locators. A same-version entry,
    an unlisted package, a replace line, a yarn key that appears or
    disappears, or any other residue is not.

    `locked` holds the versions a lockfile in HEAD resolves each spec to; a
    shared `^` or `~` spec bump needs the listed version there.

    go.sum keeps one block per version. The listed bump matches when `to`
    is present and `from` was present before, including when `from` was removed.
    """
    old_split = _split_entries(kind, before)
    new_split = _split_entries(kind, after)
    if old_split is None or new_split is None:
        return False
    old_entries, old_residue = old_split
    new_entries, new_residue = new_split
    if not _residues_match(kind, old_residue, new_residue):
        return False
    listed = _listed_versions(updates, kind=kind)
    names = set(old_entries) | set(new_entries)
    return all(
        _name_explained(
            kind,
            name,
            _entry_blocks(old_entries.get(name)),
            _entry_blocks(new_entries.get(name)),
            listed,
            locked or {},
        )
        for name in names
    )


def _locked_versions(kind: str, text: str) -> Witnesses:
    """Versions an npm or Yarn lock resolves each manifest spec to.

    npm keys the top-level `node_modules/<name>` copy by package name; Yarn
    keys each stanza by its `name@range` descriptor. A nested or workspace
    copy, a stanza for another range, and a manifest spec in the lock never witness.
    """
    if kind == "json":
        return _json_witnesses(text)
    if kind == "yarn":
        return _yarn_witnesses(text)
    return {}
