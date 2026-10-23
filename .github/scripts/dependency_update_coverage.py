"""Scan every Dependabot update.

A dependency is covered only when its scan is conclusive and both from_tag and
to_tag are set. Anything else makes the group inconclusive.
"""

from __future__ import annotations

import base64
import json
import os
import re
import runpy
import shutil
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path

_REPO_RE = re.compile(r"\A[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+\Z")
_FINDING_KEYS = ("unicode", "confusable", "ioc", "heuristic")


def scannable(update: dict) -> bool:
    """An explicit GitHub repository and an exact destination version. An empty from stays inconclusive.

    Both versions are read in the "dependabot" mode: a range, wildcard, tag
    name, operator or bare major could resolve to a tree other than the one installed.
    """
    repo = str(update.get("upstreamRepo") or "")
    from_version = str(update.get("fromVersion") or "")
    to_version = str(update.get("toVersion") or "")
    if not _REPO_RE.fullmatch(repo) or not _versions._exact_body(to_version, mode="dependabot"):
        return False
    return not from_version or bool(_versions._exact_body(from_version, mode="dependabot"))


def _label(update: dict) -> str:
    return str(update.get("packageName") or update.get("upstreamRepo") or "dependency")


def uncovered(update: dict, reason: str) -> dict:
    name = _label(update)
    return {
        "status": "not_scanned",
        "warn_only": True,
        "scan_conclusive": False,
        "not_scanned_reason": reason,
        "verdict_token_in_tree": False,
        "resolution_strategy": "unresolved",
        "resolved_range": "",
        "resolved_from": "",
        "resolved_to": "",
        "from_tag": "",
        "to_tag": "",
        "changed_files_count": 0,
        "materialized_count": 0,
        "filtered_count": 0,
        "changed_files": [],
        "errors": [f"{name}: {reason}"],
        "findings": {key: [] for key in _FINDING_KEYS},
        "package_name": str(update.get("packageName") or ""),
        "upstream_repo": str(update.get("upstreamRepo") or ""),
        "from_version": str(update.get("fromVersion") or ""),
        "to_version": str(update.get("toVersion") or ""),
    }


def _tag(update: dict, report: dict) -> dict:
    tagged = dict(report)
    tagged["package_name"] = str(update.get("packageName") or tagged.get("package_name") or "")
    tagged["upstream_repo"] = str(update.get("upstreamRepo") or tagged.get("upstream_repo") or "")
    tagged["from_version"] = str(update.get("fromVersion") or "")
    tagged["to_version"] = str(update.get("toVersion") or "")
    if tagged.get("scan_conclusive") is not True:
        tagged["scan_conclusive"] = False
    return tagged


def _tagged(report: dict) -> bool:
    return bool(str(report.get("from_tag") or "").strip()) and bool(str(report.get("to_tag") or "").strip())


def _covered(report: dict) -> bool:
    """Conclusive tag-to-tag scan. A branch, raw SHA, or HEAD does not count."""
    return report.get("scan_conclusive") is True and _tagged(report)


def _apply_coverage(report: dict) -> dict:
    covered = _covered(report)
    stamped = dict(report)
    stamped["scan_conclusive"] = covered
    if not covered:
        if not str(stamped.get("not_scanned_reason") or "").strip():
            stamped["not_scanned_reason"] = "incomplete_coverage"
        if stamped.get("status") not in {"fail", "warn"}:
            stamped["status"] = "not_scanned"
    return stamped


def agent_target(records: list[dict]) -> tuple[str, str, str] | None:
    """Pure (package, repository, resolved commit) selection. Does not clone."""
    for record in records:
        repo = str(record.get("upstream_repo") or "")
        resolved_to = str(record.get("resolved_to") or "")
        if _covered(record) and repo and resolved_to:
            return (str(record.get("package_name") or ""), repo, resolved_to)
    return None


def _identity(record: dict) -> tuple[str, str, str]:
    return (
        str(record.get("package_name") or ""),
        str(record.get("upstream_repo") or ""),
        str(record.get("resolved_to") or ""),
    )


def _candidate(record: dict) -> dict:
    return {
        "package_name": str(record.get("package_name") or ""),
        "upstream_repo": str(record.get("upstream_repo") or ""),
        "resolved_to": str(record.get("resolved_to") or ""),
        "resolved_from": str(record.get("resolved_from") or ""),
        "from_version": str(record.get("from_version") or ""),
        "to_version": str(record.get("to_version") or ""),
        "from_tag": str(record.get("from_tag") or ""),
        "to_tag": str(record.get("to_tag") or ""),
        "scan_conclusive": _covered(record),
    }


def _stamp(report: dict, records: list[dict], target: tuple[str, str, str] | None) -> dict:
    report["agent_candidates"] = [_candidate(record) for record in records]
    report["agent_target"] = list(target) if target else None
    return report


def _selected(records: list[dict], target: tuple[str, str, str] | None) -> dict:
    if target is None:
        return {}
    return next((record for record in records if _identity(record) == target), {})


def agent_fields(records: list[dict]) -> dict[str, str]:
    """Prompt fields for agent_target. Empty when no dependency was scanned conclusively."""
    target = agent_target(records)
    if target is None:
        return {
            "package": "",
            "from_version": "",
            "to_version": "",
            "resolved_from": "",
            "resolved_to": "",
        }
    chosen = _selected(records, target)
    return {
        "package": target[0],
        "from_version": str(chosen.get("from_version") or ""),
        "to_version": str(chosen.get("to_version") or ""),
        "resolved_from": str(chosen.get("resolved_from") or ""),
        "resolved_to": target[2],
    }


def _unread_items(records: list[dict]) -> list[dict]:
    found: list[dict] = []
    seen: set[tuple[str, str]] = set()
    for record in records:
        for item in record.get("unread") or []:
            if not isinstance(item, dict):
                continue
            file_name = item.get("file")
            cause = item.get("cause")
            if not isinstance(file_name, str) or not file_name.strip():
                continue
            why = cause.strip() if isinstance(cause, str) and cause.strip() else "incomplete"
            key = (file_name.strip(), why)
            if key in seen:
                continue
            seen.add(key)
            found.append({"file": file_name.strip(), "cause": why})
    return found


def aggregate(records: list[dict]) -> dict:
    """Covered only when every record was scanned conclusively."""
    target = agent_target(records)
    if len(records) == 1:
        return _stamp(_apply_coverage(records[0]), records, target)
    if not records:
        return _stamp(uncovered({}, "incomplete_coverage"), records, None)

    findings = {key: [] for key in _FINDING_KEYS}
    errors: list[str] = []
    changed: list[str] = []
    for record in records:
        name = str(record.get("package_name") or record.get("upstream_repo") or "dependency")
        if not _covered(record):
            reason = str(record.get("not_scanned_reason") or "not_conclusive")
            errors.append(f"{name}: {reason}")
        for key in _FINDING_KEYS:
            items = (record.get("findings") or {}).get(key) or []
            if isinstance(items, list):
                findings[key].extend(items)
        for item in record.get("errors") or []:
            if item not in errors:
                errors.append(str(item))
        for path in record.get("changed_files") or []:
            changed.append(f"{name}:{path}")

    conclusive = all(_covered(record) for record in records)
    any_findings = any(findings.values())
    warn_only = all(record.get("warn_only") is not False for record in records)
    warned = any(record.get("status") == "warn" or record.get("verdict_token_in_tree") is True for record in records)
    if any(record.get("status") == "fail" for record in records):
        status = "fail"
    elif any_findings:
        status = "warn" if warn_only else "fail"
    elif warned:
        status = "warn"
    elif not conclusive:
        status = "not_scanned"
    else:
        status = "clean"

    chosen = _selected(records, target)
    return _stamp(
        {
            "status": status,
            "warn_only": warn_only,
            "scan_conclusive": conclusive,
            "not_scanned_reason": "" if conclusive else "incomplete_coverage",
            "verdict_token_in_tree": any(record.get("verdict_token_in_tree") is True for record in records),
            "resolution_strategy": "group",
            "package_name": chosen.get("package_name") or "",
            "upstream_repo": chosen.get("upstream_repo") or "",
            "from_version": chosen.get("from_version") or "",
            "to_version": chosen.get("to_version") or "",
            "resolved_range": chosen.get("resolved_range") or "",
            "resolved_from": chosen.get("resolved_from") or "",
            "resolved_to": chosen.get("resolved_to") or "",
            "from_tag": chosen.get("from_tag") or "",
            "to_tag": chosen.get("to_tag") or "",
            "changed_files_count": sum(int(record.get("changed_files_count") or 0) for record in records),
            "materialized_count": sum(int(record.get("materialized_count") or 0) for record in records),
            "filtered_count": sum(int(record.get("filtered_count") or 0) for record in records),
            "changed_files": changed,
            "errors": errors,
            "findings": findings,
            "unread": _unread_items(records),
            "dependencies": [
                {
                    "package_name": record.get("package_name") or "",
                    "upstream_repo": record.get("upstream_repo") or "",
                    "from_version": record.get("from_version") or "",
                    "to_version": record.get("to_version") or "",
                    "scan_conclusive": _covered(record),
                    "not_scanned_reason": record.get("not_scanned_reason") or "",
                }
                for record in records
            ],
        },
        records,
        target,
    )


def render_summary(report: dict, records: list[dict]) -> str:
    lines = ["## Malware Scan Summary", "", f"- Status: **{report.get('status') or 'not_scanned'}**"]
    if report.get("scan_conclusive") is True:
        lines.append("- Scan coverage: **conclusive**.")
    else:
        reason = report.get("not_scanned_reason") or "incomplete_coverage"
        lines.append(f"- Scan coverage: **incomplete** (`{reason}`).")
        lines.append("- A dependency is covered only when its scan is conclusive.")
        lines.append("- This is not a benign result.")
    for record in records:
        name = record.get("package_name") or record.get("upstream_repo") or "dependency"
        span = f"{record.get('from_version') or '?'} -> {record.get('to_version') or '?'}"
        repo = record.get("upstream_repo") or "no repository"
        if _covered(record):
            lines.append(f"- {name} {span} ({repo}): covered")
        else:
            reason = record.get("not_scanned_reason") or "not_conclusive"
            lines.append(f"- {name} {span} ({repo}): not covered ({reason})")
    if not any_read(report):
        lines.append("- This status is inconclusive. It is not evidence the package is benign.")
    return "\n".join(lines) + "\n"


def any_read(report: dict) -> bool:
    return int(report.get("materialized_count") or 0) > 0


# Grammars dependency_manifest_diff imports, in dependency order.
_MANIFEST_GRAMMARS = (
    "dependency_manifest_versions",
    "dependency_manifest_go",
    "dependency_manifest_npm",
    "dependency_manifest_python",
)


def _load_manifest_modules():
    """Load the manifest grammars beside this file under python3 -I; return the diff and version modules.

    The cold-start spine is the only bootstrap. It path-filters the sibling,
    so this script never imports it and never puts the scripts directory on sys.path.
    """
    script_dir = Path(__file__).resolve().parent
    cold = runpy.run_path(str(script_dir / "trusted_formatter_loader_cold_start.py"))
    cold["resolve_loader_for_dir"](script_dir)
    scrub = sys.modules["scripts_dir_path_scrub"]
    loaded = {
        name: scrub.bootstrap_module_from_scripts_dir(script_dir, f"{name}.py", name)
        for name in (*_MANIFEST_GRAMMARS, "dependency_manifest_diff")
    }
    return loaded["dependency_manifest_diff"], loaded["dependency_manifest_versions"]


_diff, _versions = _load_manifest_modules()
_BASE_REF_RE = re.compile(r"\A[A-Za-z0-9._/-]+\Z")


def _git_text(rev: str, path: str) -> str:
    proc = subprocess.run(
        ["git", "show", f"{rev}:{path}"],
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        return ""
    return proc.stdout


def _verify_commit(rev: str) -> str:
    # Concatenate the peel. An f-string brace escape is a Go text/template action.
    proc = subprocess.run(
        ["git", "rev-parse", "--verify", "--end-of-options", rev + "^{commit}"],
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        return ""
    return proc.stdout.strip()


# The workflow passes a read-only token here because checkout runs with
# persist-credentials: false. main() removes it from the environment so no
# child process inherits it.
FETCH_AUTH_VARIABLE = "DCR_FETCH_TOKEN"


def _fetch_env(token: str) -> dict[str, str]:
    """Environment for one authenticated fetch.

    The Basic auth header goes in through GIT_CONFIG_* variables, the same
    one-time header the trusted checkout action uses, so the token is never
    in argv or in the persisted git config.
    """
    env = os.environ.copy()
    env["GIT_TERMINAL_PROMPT"] = "0"
    if not token:
        return env
    try:
        index = int(env.get("GIT_CONFIG_COUNT", "0"))
    except ValueError:
        index = 0
    basic = base64.b64encode(f"x-access-token:{token}".encode()).decode()
    env[f"GIT_CONFIG_KEY_{index}"] = "http.extraheader"
    env[f"GIT_CONFIG_VALUE_{index}"] = f"AUTHORIZATION: basic {basic}"
    env["GIT_CONFIG_COUNT"] = str(index + 1)
    return env


def _fetch_commit(sha: str, token: str = "") -> str:
    found = _verify_commit(sha)
    if found:
        return found
    # Output is discarded rather than logged.
    subprocess.run(
        ["git", "fetch", "--no-tags", "--depth=1", "origin", sha],
        check=False,
        capture_output=True,
        text=True,
        env=_fetch_env(token),
    )
    return _verify_commit(sha)


def _base_commit(token: str = "") -> str:
    """Resolved base commit, or '' when no base is named or the named one cannot be read."""
    if not Path(".git").exists():
        return ""
    sha = os.environ.get("PR_BASE_SHA", "").strip()
    if sha:
        if not re.fullmatch(r"[0-9a-fA-F]{7,64}", sha):
            return ""
        return _fetch_commit(sha, token)
    ref = os.environ.get("GITHUB_BASE_REF", "").strip()
    if not _BASE_REF_RE.fullmatch(ref):
        return ""
    return _verify_commit(ref) or _verify_commit(f"origin/{ref}") or ""


def _changed_paths(base: str) -> list[tuple[str, str, str]] | None:
    """(before path, after path, status letter) for each file changed since base, deletions included."""
    diff = subprocess.run(
        ["git", "diff", "--name-status", "-z", "-M", "--diff-filter=AMRCTD", base, "HEAD"],
        check=False,
        capture_output=True,
        text=True,
    )
    if diff.returncode != 0:
        return None
    fields = diff.stdout.split("\0")
    changed: list[tuple[str, str, str]] = []
    index = 0
    while index + 1 < len(fields):
        status = fields[index][:1]
        if status in {"R", "C"}:
            changed.append((fields[index + 1], fields[index + 2], status))
            index += 3
        else:
            changed.append((fields[index + 1], fields[index + 1], status))
            index += 2
    return changed


# Lockfiles that can witness the version a package.json range resolves to.
_RANGE_WITNESS_LOCKS = (("package-lock.json", "json"), ("npm-shrinkwrap.json", "json"), ("yarn.lock", "yarn"))


def _head_locked_versions(path: str) -> dict[str, frozenset[str]]:
    """Versions the npm or Yarn locks beside a package.json or lockfile path in HEAD resolve each spec to.

    A lockfile is beside itself, so the root specs a lock records are
    witnessed by that lock in HEAD. Only the top-level `node_modules/<name>`
    copy or the Yarn stanza for the exact spec witnesses a range, so a nested,
    workspace or other-range copy never confirms a bump.
    """
    if Path(path).name not in {"package.json", *_diff.TRANSITIVE_LOCK_NAMES}:
        return {}
    found: dict[str, frozenset[str]] = {}
    for lock_name, kind in _RANGE_WITNESS_LOCKS:
        lock_path = str(Path(path).with_name(lock_name))
        for name, versions in _diff._locked_versions(kind, _git_text("HEAD", lock_path)).items():
            found[name] = found.get(name, frozenset()) | versions
    return found


def manifest_gaps(updates: list[dict], token: str = "") -> list[dict]:
    """Direct manifest bumps missing from the parsed list.

    A base that is unnamed or unreadable is itself unresolved: the workflow
    only runs this for a pull request, so a missing base never means "no
    changes". An entry is explained only when its package moved to the
    listed version. Anything else in the file stays unresolved under the
    path. A listed package with that path as its name does not hide the
    path. A dependency with more than one unexplained entry is named as well.

    An opaque lockfile, a deleted path, and any changed path this grammar does
    not parse are unresolved under their path; a changed path that is a
    manifest we have no parser for (see _diff._unparsed_manifest: a workflow,
    composite action, Dockerfile, Cargo, Swift, Terraform or NuGet file) is
    reported as `unparsed_manifest`. A shared `^` or `~`
    package.json bump is explained only when a lockfile beside it in HEAD
    resolves the new spec to the listed version: the top-level npm copy, or
    the Yarn stanza for that exact descriptor. Unlisted names in a npm
    or Yarn lock are transitive residue when every listed name in that lock
    moved to its listed version and each unlisted entry was bumped, added or
    removed as a registry tarball.
    """
    base = _base_commit(token)
    if not base:
        return [uncovered({"packageName": "manifest-diff"}, "unresolved")]
    listed = {str(item.get("packageName") or "").strip() for item in updates}
    changed = _changed_paths(base)
    if changed is None:
        return [uncovered({"packageName": "manifest-diff"}, "unresolved")]
    gaps: list[dict] = []
    seen = set(listed)
    for old_path, path, status in changed:
        kind = _diff._manifest_kind(path)
        if status in {"D", "T"}:
            for label in dict.fromkeys((old_path, path)):
                gaps.append(uncovered({"packageName": label}, "unresolved"))
            continue
        if not kind or kind == "opaque":
            reason = "unparsed_manifest" if _diff._unparsed_manifest(path) else "unresolved"
            for label in dict.fromkeys((old_path, path)):
                gaps.append(uncovered({"packageName": label}, reason))
            continue
        before = _git_text(base, old_path)
        after = _git_text("HEAD", path)
        names = _diff._changed_names(kind, before, after)
        locked = _head_locked_versions(path)
        explained = names is not None and _diff._entries_explain(kind, before, after, updates, locked)
        file_unresolved = names is None or (before != after and not explained)
        if (
            Path(path).name in _diff.TRANSITIVE_LOCK_NAMES
            and names
            and _diff._lock_covers_listed(kind, before, after, updates, names, locked)
        ):
            continue
        if file_unresolved and (names is None or _diff._names_listed(kind, names, listed)):
            gaps.append(uncovered({"packageName": path}, "unresolved"))
        elif names:
            for label in sorted(names):
                if _diff._names_listed(kind, {label}, seen):
                    continue
                gaps.append(uncovered({"packageName": label}, "unresolved"))
                seen.add(label)
        if names is not None and file_unresolved:
            for label in sorted(_diff._multi_entry_names(kind, before, after, updates, locked)):
                if _diff._names_listed(kind, {label}, seen):
                    continue
                gaps.append(uncovered({"packageName": label}, "unresolved"))
                seen.add(label)
    return gaps


def _load_updates(path: Path) -> list[dict]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    if not isinstance(payload, list):
        return []
    return [item for item in payload if isinstance(item, dict)]


def _has_upstream_link(updates: list[dict]) -> bool:
    return any(_REPO_RE.fullmatch(str(item.get("upstreamRepo") or "")) for item in updates)


def publish_text(path: Path, text: str) -> None:
    """Replace the entry at path with a new file, so a link there is replaced, not followed."""
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(text)
    os.replace(tmp, path)


def remove_entry(path: Path) -> None:
    """Remove the entry at path; a link is unlinked, never followed."""
    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path)
    else:
        path.unlink(missing_ok=True)


def _write_report(report: dict, summary: str) -> None:
    publish_text(Path("malware_scan_report.json"), json.dumps(report, indent=2) + "\n")
    publish_text(Path("malware_scan_summary.md"), summary)


def _clone(repo: str, dest: Path) -> bool:
    dest.parent.mkdir(parents=True, exist_ok=True)
    url = f"https://github.com/{repo}.git"
    proc = subprocess.run(
        ["git", "clone", "--", url, str(dest)],
        check=False,
        capture_output=True,
        text=True,
    )
    return proc.returncode == 0 and dest.is_dir()


def _scan_one(update: dict, dest: Path, scan_script: Path) -> dict:
    if not scannable(update):
        return uncovered(update, "unresolved")
    dest = dest.absolute()
    checkout = dest / ".upstream-dependency"
    if not _clone(str(update["upstreamRepo"]), checkout):
        return uncovered(update, "unresolved")
    env = os.environ.copy()
    env["GITHUB_OUTPUT"] = str(dest / "github_output.txt")
    env["PACKAGE_NAME"] = str(update.get("packageName") or "")
    env["FROM_VERSION"] = str(update.get("fromVersion") or "")
    env["TO_VERSION"] = str(update.get("toVersion") or "")
    proc = subprocess.run(
        ["bash", str(scan_script)],
        cwd=dest,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )
    report_path = dest / "malware_scan_report.json"
    if not report_path.is_file():
        return uncovered(update, "not_scanned")
    try:
        report = json.loads(report_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return uncovered(update, "not_scanned")
    if not isinstance(report, dict):
        return uncovered(update, "not_scanned")
    tagged = _tag(update, report)
    if proc.returncode != 0 and tagged.get("status") != "fail":
        tagged["scan_conclusive"] = False
        tagged["not_scanned_reason"] = "scanner_error"
        stderr_tail = "\n".join(proc.stderr.strip().splitlines()[-5:]) if proc.stderr and proc.stderr.strip() else ""
        errors = list(tagged.get("errors") or [])
        if stderr_tail:
            errors.append(stderr_tail)
        tagged["errors"] = errors
    return tagged


def _output_field(value: object) -> str:
    return str(value or "").replace("\r", " ").replace("\n", " ")


def _emitted_fields(report: dict) -> dict:
    """Outputs follow agent_target. A scan record's own commit is not a second choice."""
    target = report.get("agent_target")
    if not isinstance(target, list) or len(target) != 3:
        return {}
    want = (str(target[0]), str(target[1]), str(target[2]))
    for item in report.get("agent_candidates") or []:
        if isinstance(item, dict) and _identity(item) == want:
            return item
    return {"package_name": want[0], "upstream_repo": want[1], "resolved_to": want[2]}


def _emit(report: dict, summary: str) -> None:
    output = os.environ.get("GITHUB_OUTPUT", "").strip()
    if not output:
        return
    fields = _emitted_fields(report)
    delim = f"GHEOF_{uuid.uuid4().hex}"
    conclusive = "true" if report.get("scan_conclusive") is True else "false"
    text = (
        f"status={_output_field(report.get('status'))}\n"
        f"changed_files_count={int(report.get('changed_files_count') or 0)}\n"
        f"package_name={_output_field(fields.get('package_name'))}\n"
        f"upstream_repo={_output_field(fields.get('upstream_repo'))}\n"
        f"from_version={_output_field(fields.get('from_version'))}\n"
        f"to_version={_output_field(fields.get('to_version'))}\n"
        f"resolved_from={_output_field(fields.get('resolved_from'))}\n"
        f"resolved_to={_output_field(fields.get('resolved_to'))}\n"
        f"from_tag={_output_field(fields.get('from_tag'))}\n"
        f"to_tag={_output_field(fields.get('to_tag'))}\n"
        f"scan_conclusive={conclusive}\n"
        f"summary<<{delim}\n"
        f"{summary}"
        f"{delim}\n"
    )
    with open(output, "a", encoding="utf-8") as handle:
        handle.write(text)


def _keep_checkout(records: list[dict], scans: list[tuple[dict, Path]]) -> None:
    target = agent_target(records)
    kept = Path(".upstream-dependency")
    remove_entry(kept)
    if target is None:
        return
    for record, dest in scans:
        if _identity(record) != target:
            continue
        checkout = dest / ".upstream-dependency"
        if checkout.is_dir():
            shutil.move(str(checkout), str(kept))
        return


def main() -> None:
    fetch_token = os.environ.pop(FETCH_AUTH_VARIABLE, "")
    scan_script = Path(__file__).resolve().parent / "upstream_malware_scan.sh"
    updates = _load_updates(Path("dependency_updates.json"))
    if updates and not _has_upstream_link(updates):
        report = uncovered({}, "no_upstream_link")
        summary = render_summary(report, [])
        _write_report(report, summary)
        _emit(report, summary)
        return
    gaps = manifest_gaps(updates, fetch_token)
    work = Path(".dcr-dependency-scans")
    remove_entry(work)
    work.mkdir()
    records: list[dict] = list(gaps)
    scans: list[tuple[dict, Path]] = []
    child_summary = ""
    try:
        for index, update in enumerate(updates):
            dest = work / str(index)
            dest.mkdir()
            record = _scan_one(update, dest, scan_script)
            records.append(record)
            scans.append((record, dest))
            summary_path = dest / "malware_scan_summary.md"
            if len(updates) == 1 and not gaps and summary_path.is_file():
                child_summary = summary_path.read_text(encoding="utf-8")
        report = aggregate(records)
        summary = child_summary if len(records) == 1 and child_summary else render_summary(report, records)
        _write_report(report, summary)
        _keep_checkout(records, scans)
    finally:
        shutil.rmtree(work, ignore_errors=True)
    _emit(report, summary)


if __name__ == "__main__":
    main()
