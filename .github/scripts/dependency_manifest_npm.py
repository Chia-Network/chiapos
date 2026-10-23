"""package.json, package-lock.json and yarn.lock grammar for a Dependabot diff."""

from __future__ import annotations

import json
import re

_YARN_DESCRIPTOR_RE = re.compile(r'(@?[^@"\s]+)@')
_LOCK_PREFIX = "node_modules/"
_YARN_VERSION_RE = re.compile(r'^  version "([^"]+)"')


def _direct_lock_name(key: str) -> str:
    """Leaf package name of a lockfile path, or "" when the path is not a package.

    Nested copies (`node_modules/a/node_modules/b`) and workspace paths (such
    as `packages/gui/node_modules/b`) name the leaf package too. A path that
    continues past the package name stays in residue. Whether a path is the
    top-level copy is not decided here; _json_witnesses compares it to
    `node_modules/<name>`.
    """
    index = key.rfind(_LOCK_PREFIX)
    if index < 0:
        return ""
    parts = key[index + len(_LOCK_PREFIX) :].split("/")
    if not parts or not parts[0]:
        return ""
    if parts[0].startswith("@"):
        if len(parts) != 2 or len(parts[0]) < 2 or not parts[1]:
            return ""
        return f"{parts[0]}/{parts[1]}"
    if len(parts) != 1:
        return ""
    return parts[0]


def _take_specs(block: object, parts: dict[str, dict[str, object]], section: str) -> None:
    """Move each spec out of one section. A name listed in several sections keeps one spec per section."""
    if not isinstance(block, dict):
        return
    for name in list(block):
        spec = block[name]
        specs = parts.setdefault(str(name), {}).setdefault("specs", [])
        if isinstance(spec, str):
            specs.append((section, spec))
            del block[name]
            continue
        if not isinstance(spec, dict) or not spec.get("version"):
            continue
        nested = {
            key: spec.pop(key) for key in ("dependencies", "optionalDependencies") if isinstance(spec.get(key), dict)
        }
        specs.append((section, spec))
        if nested:
            block[name] = nested
        else:
            del block[name]


def _json_blocks(blob: dict[str, object], name: str) -> list[tuple[str, str]]:
    """One block per manifest spec and per lockfile path. Paths do not collapse."""
    blocks: list[tuple[str, str]] = []
    for section, spec in blob.get("specs") or ():
        if isinstance(spec, str):
            blocks.append((spec, json.dumps({"section": section, "spec": spec}, separators=(",", ":"))))
        else:
            # `name` is the dependencies key. Lockfile v1 has no packages path.
            body = {"name": name, "section": section, "spec": spec}
            blocks.append((str(spec.get("version") or ""), json.dumps(body, sort_keys=True, separators=(",", ":"))))
    locks = blob.get("locks")
    if isinstance(locks, list):
        ordered = [item for item in locks if isinstance(item, tuple) and len(item) == 2]
        for path, meta in sorted(ordered, key=lambda item: item[0]):
            if not isinstance(path, str) or not isinstance(meta, dict):
                continue
            body = dict(meta)
            body["path"] = path
            blocks.append((str(meta.get("version") or ""), json.dumps(body, sort_keys=True, separators=(",", ":"))))
    return blocks


def _json_split(text: str) -> tuple[dict[str, dict[str, object]], dict[str, object]] | None:
    """Specs and lock paths per package name, and the document left once they are taken out."""
    try:
        doc = json.loads(text)
    except json.JSONDecodeError:
        return None
    if not isinstance(doc, dict):
        return None
    parts: dict[str, dict[str, object]] = {}
    for section in ("dependencies", "devDependencies", "optionalDependencies", "peerDependencies"):
        _take_specs(doc.get(section), parts, section)
        block = doc.get(section)
        if isinstance(block, dict) and not block:
            doc.pop(section, None)
    packages = doc.get("packages")
    if isinstance(packages, dict):
        root = packages.get("")
        if isinstance(root, dict):
            for section in ("dependencies", "devDependencies", "optionalDependencies", "peerDependencies"):
                _take_specs(root.get(section), parts, "packages:" + section)
                block = root.get(section)
                if isinstance(block, dict) and not block:
                    root.pop(section, None)
        for key in list(packages):
            if not isinstance(key, str):
                continue
            name = _direct_lock_name(key)
            meta = packages.get(key)
            if not name or not isinstance(meta, dict):
                continue
            locks = parts.setdefault(name, {}).setdefault("locks", [])
            if isinstance(locks, list):
                locks.append((key, meta))
            del packages[key]
        if not packages:
            doc.pop("packages", None)
    return parts, doc


def _json_parts(text: str) -> tuple[dict[str, tuple[str, str]], str] | None:
    if not text.strip():
        return {}, ""
    split = _json_split(text)
    if split is None:
        return None
    parts, doc = split
    entries: dict[str, tuple[str, str] | list[tuple[str, str]]] = {}
    for name, blob in parts.items():
        blocks = _json_blocks(blob, name)
        if len(blocks) == 1:
            entries[name] = blocks[0]
        elif blocks:
            entries[name] = blocks
    return entries, json.dumps(doc, sort_keys=True, separators=(",", ":"))


def _json_witnesses(text: str) -> dict[str, frozenset[str]]:
    """Versions an npm lock installs each package to directly under its manifest.

    A lockfile v1 `dependencies` record and a `node_modules/<name>` path are
    that copy. A nested or workspace path, and a manifest string spec, are not.
    """
    split = _json_split(text)
    if split is None:
        return {}
    found: dict[str, frozenset[str]] = {}
    for name, blob in split[0].items():
        records = [spec for _section, spec in blob.get("specs") or () if isinstance(spec, dict)]
        records += [meta for path, meta in blob.get("locks") or () if path == _LOCK_PREFIX + name]
        versions = {str(record.get("version") or "") for record in records} - {""}
        if versions:
            found[name] = frozenset(versions)
    return found


def _yarn_descriptors(header: str) -> list[str]:
    """Each comma-separated `name@range` descriptor of a Yarn stanza header, unquoted."""
    return [part.strip().strip('"') for part in header.rstrip().rstrip(":").split(",") if part.strip()]


def _yarn_names(header: str) -> list[str]:
    """Package names of a Yarn stanza header, one per distinct comma-separated descriptor."""
    names: list[str] = []
    for descriptor in _yarn_descriptors(header):
        match = _YARN_DESCRIPTOR_RE.match(descriptor)
        if match and match.group(1) not in names:
            names.append(match.group(1))
    return names


def _yarn_parts(text: str) -> tuple[dict[str, list[tuple[str, str]]], str]:
    entries: dict[str, list[tuple[str, str]]] = {}
    residue: list[str] = []
    names: list[str] = []
    block: list[str] = []
    version = ""

    def flush() -> None:
        nonlocal names, block, version
        if names:
            item = (version, "\n".join(block))
            for name in names:
                entries.setdefault(name, []).append(item)
        elif block:
            residue.append("\n".join(block) + "\n")
        names, block, version = [], [], ""

    for raw in text.splitlines():
        if raw and not raw.startswith((" ", "\t")):
            flush()
            names = _yarn_names(raw)
            block = [raw]
            version = ""
            continue
        block.append(raw)
        match = _YARN_VERSION_RE.match(raw)
        if match:
            version = match.group(1)
    flush()
    return entries, "".join(residue)


def _yarn_witnesses(text: str) -> dict[str, frozenset[str]]:
    """Versions each `name@range` descriptor resolves to in a Yarn lock.

    Keyed by descriptor, so only the stanza for a manifest's exact spec witnesses it.
    """
    found: dict[str, frozenset[str]] = {}
    for blocks in _yarn_parts(text)[0].values():
        for version, payload in blocks:
            for descriptor in _yarn_descriptors(payload.split("\n", 1)[0]):
                found[descriptor] = found.get(descriptor, frozenset()) | {version}
    return found


_SEMVER = r"\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?"
_TARBALL_RE = re.compile(
    r"^https://registry\.(?:npmjs\.org|yarnpkg\.com)/"
    r"(?:(?P<scope>@[^/]+)/)?(?P<name>[^/]+)/-/(?P<base>[^/]+)-(?P<version>" + _SEMVER + r")\.tgz$"
)
_FOLD_SPEC_RE = re.compile(rf"^(?:\^|=)?({_SEMVER})$")
_LOCATOR_KEYS = frozenset({"version", "resolved", "integrity", "checksum"})
_SPEC_SECTIONS = frozenset({"dependencies", "devDependencies", "optionalDependencies", "peerDependencies"})


def _canonical_tarball(url: str, version: str, package: str) -> bool:
    """True when `url` is the npm or Yarn registry tarball for this package version."""
    if not url or not version or not package:
        return False
    match = _TARBALL_RE.fullmatch(url.split("#", 1)[0])
    if not match or match.group("version") != version or match.group("base") != match.group("name"):
        return False
    scope = match.group("scope")
    found = f"{scope}/{match.group('name')}" if scope else match.group("name")
    return found == package


def _fold_spec(value: str) -> str:
    """Bare `X`, `^X`, and `=X` are the same residue spelling. `~`, `v`, and `V` are not."""
    match = _FOLD_SPEC_RE.fullmatch(value)
    return match.group(1) if match else value


def _fold_residue(node: object) -> object:
    """Fold bare, `^`, and `=` spellings in residue maps. Nested lock records stay as written."""
    if isinstance(node, list):
        return [_fold_residue(item) for item in node]
    if not isinstance(node, dict):
        return node
    folded: dict[str, object] = {}
    for key, value in node.items():
        if key in _SPEC_SECTIONS and isinstance(value, dict):
            section: dict[str, object] = {}
            for dep, spec in value.items():
                section[str(dep)] = _fold_spec(spec) if isinstance(spec, str) else _fold_residue(spec)
            folded[key] = section
            continue
        folded[key] = _fold_residue(value)
    return folded


def _lock_record(doc: object) -> tuple[str, dict[str, object]] | None:
    if not isinstance(doc, dict):
        return None
    spec = doc.get("spec")
    if isinstance(spec, str):
        return None
    if isinstance(spec, dict):
        name = doc.get("name") if isinstance(doc.get("name"), str) else ""
        return name, spec
    path = doc.get("path") if isinstance(doc.get("path"), str) else ""
    name = _direct_lock_name(path) if path else ""
    if not name and isinstance(doc.get("name"), str):
        name = str(doc["name"])
    if path or any(key in doc for key in _LOCATOR_KEYS):
        return name, doc
    return None


def _text(value: object) -> str:
    return value if isinstance(value, str) else ""


def _both_resolved_canonical(old_name: str, old: dict[str, object], new_name: str, new: dict[str, object]) -> bool:
    """Both install locators are the registry tarball for that side's name and version."""
    old_resolved, new_resolved = _text(old.get("resolved")), _text(new.get("resolved"))
    if not old_resolved or not new_resolved:
        return False
    return _canonical_tarball(old_resolved, _text(old.get("version")), old_name) and _canonical_tarball(
        new_resolved, _text(new.get("version")), new_name
    )


def _without_locators(record: dict[str, object]) -> dict[str, object]:
    return {key: value for key, value in record.items() if key not in _LOCATOR_KEYS}


def _plain_yarn_line(line: str) -> tuple[str, str] | None:
    for key in ("version", "resolved"):
        prefix = f'  {key} "'
        body = line[len(prefix) : -1]
        if line.startswith(prefix) and line.endswith('"') and '"' not in body:
            return key, body
    for key in ("integrity", "checksum"):
        prefix = f"  {key} "
        body = line[len(prefix) :]
        if line.startswith(prefix) and body and " " not in body and "," not in body:
            return key, body
    return None


def _yarn_keys(line: str) -> set[str]:
    """Key tokens of a body line. A comma joins keys; a colon follows a section key."""
    keys: set[str] = set()
    for part in line.strip().split(","):
        stripped = part.strip()
        if not stripped:
            continue
        head = re.split(r"[\s:]", stripped, maxsplit=1)[0].strip(' :"')
        if head:
            keys.add(head)
    return keys


def _yarn_rest(payload: str) -> str | None:
    """Header and dependency sub-blocks left; plain locator lines dropped.

    A stanza-body line keyed on version, resolved, integrity or checksum must be
    in the plain allowlisted form, or the stanza is not canonical. A line
    indented past the stanza body is a dependency entry and stays, even when a
    package is named like a locator.
    """
    lines = payload.splitlines()
    names = _yarn_names(lines[0]) if lines else []
    if len(names) != 1:
        return None
    version = resolved = ""
    kept = [lines[0]]
    for line in lines[1:]:
        if not line:
            kept.append(line)
            continue
        if line.startswith("   "):
            kept.append(line)
            continue
        if not (_yarn_keys(line) & _LOCATOR_KEYS):
            kept.append(line)
            continue
        field = _plain_yarn_line(line)
        if field is None or (field[0] == "version" and version) or (field[0] == "resolved" and resolved):
            return None
        if field[0] == "version":
            version = field[1]
        elif field[0] == "resolved":
            resolved = field[1]
    return "\n".join(kept) if _canonical_tarball(resolved, version, names[0]) else None


def _payloads_match(old: str, new: str) -> bool:
    """Drop version and registry locators only when `resolved` is canonical on both sides.

    Every other entry is compared as written, version included. `resolution` does not make an entry canonical.
    """
    if old == new:
        return True
    try:
        old_doc = json.loads(old)
        new_doc = json.loads(new)
    except json.JSONDecodeError:
        old_rest, new_rest = _yarn_rest(old), _yarn_rest(new)
        return old_rest is not None and old_rest == new_rest
    old_record, new_record = _lock_record(old_doc), _lock_record(new_doc)
    if old_record is None or new_record is None:
        return False
    old_name, old_body = old_record
    new_name, new_body = new_record
    if not _both_resolved_canonical(old_name, old_body, new_name, new_body):
        return False
    return _without_locators(old_body) == _without_locators(new_body)


def _registry_block(payload: str) -> bool:
    """True when one lock entry resolves to the registry tarball for its name and version."""
    try:
        doc = json.loads(payload)
    except json.JSONDecodeError:
        return _yarn_rest(payload) is not None
    record = _lock_record(doc)
    if record is None:
        return False
    name, body = record
    return _both_resolved_canonical(name, body, name, body)
