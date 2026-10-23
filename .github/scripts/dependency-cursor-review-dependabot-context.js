'use strict';

const { spawnSync } = require('child_process');
const fs = require('fs');

// The review marker is produced once, here, and passed through as that single line.

const LEGACY_REVIEW_MARKER = '<!-- cursor-dependabot-review -->';
const ACTIONS_BOT_LOGIN = 'github-actions[bot]';

function escapeRegex(text) {
  return text.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

// [start, end) of each top-level <details> block. An unclosed block runs to the end.
function detailsSpans(text) {
  const tagRe = /<details\b[^>]*>|<\/details>/gi;
  const spans = [];
  let depth = 0;
  let start = 0;
  let tag;
  while ((tag = tagRe.exec(text)) !== null) {
    const opens = tag[0].toLowerCase().startsWith('<details');
    if (!opens && depth === 0) continue; // a close with no open block hides nothing
    if (opens && depth === 0) start = tag.index;
    depth += opens ? 1 : -1;
    if (!opens && depth === 0) spans.push([start, tagRe.lastIndex, true]);
  }
  if (depth > 0) spans.push([start, text.length, false]);
  return spans;
}

function extractDetailsSection(text, summaryLabel) {
  const summaryRe = new RegExp(`^<details[^>]*>\\s*<summary>\\s*${escapeRegex(summaryLabel)}[^<]*</summary>`, 'i');
  for (const [start, end, closed] of detailsSpans(text)) {
    const block = text.slice(start, end);
    if (closed && summaryRe.test(block)) {
      return block
        .replace(summaryRe, '')
        .replace(/<\/details>\s*$/i, '')
        .trim();
    }
  }
  return '';
}

// The body outside every top-level <details> block. Release notes and commit
// lists live there and must not add packages or versions; text after a block still counts.
function withoutDetails(text) {
  let kept = '';
  let from = 0;
  for (const [start, end] of detailsSpans(text)) {
    kept += text.slice(from, start);
    from = end;
  }
  return kept + text.slice(from);
}

// Only an exact pin (`==` or `===`) loses its operator. Range operators stay so a
// range is never taken for a scannable version; a leading v or V stays because tags are looked up as written.
function normalizeVersion(version) {
  return String(version || '')
    .trim()
    .replace(/^(?:===|==)\s*/, '');
}

function sameValue(key, left, right) {
  if (key === 'upstreamRepo') return left === right;
  return left.replace(/^v/i, '') === right.replace(/^v/i, '');
}

function githubRepoFromText(text) {
  const source = String(text || '');
  const url = source.match(/https?:\/\/(?:redirect\.)?github\.com\/([A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+)/i);
  if (url) return url[1].replace(/\.git$/i, '');
  const moduleName = source.match(/^github\.com\/([A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+)(?:\/|$)/i);
  return moduleName ? moduleName[1] : '';
}

// A table Package cell: `name`, `[name](url)`, or Renovate's `[name](url) ([source](repo))`.
function packageCell(cell) {
  const links = [...cell.matchAll(/\[([^\]]*)\]\(([^)]*)\)/g)];
  if (links.length === 0) return { name: cell.trim(), repo: githubRepoFromText(cell.trim()) };
  const source = links.find((link) => link[1].trim().toLowerCase() === 'source');
  return {
    name: links[0][1].trim(),
    repo: githubRepoFromText(source?.[2]) || githubRepoFromText(links[0][2]) || githubRepoFromText(links[0][1]),
  };
}

function agree(record, key, incoming) {
  if (record.disputed[key]) return '';
  if (!incoming) return record[key];
  if (!record[key]) return incoming;
  if (sameValue(key, record[key], incoming)) return record[key];
  record.disputed[key] = true;
  return '';
}

function rememberUpdate(byName, name, fromVersion, toVersion, repo) {
  const packageName = String(name || '').trim();
  if (!packageName || packageName.toLowerCase() === 'package') return;
  const from = normalizeVersion(fromVersion);
  const to = normalizeVersion(toVersion);
  const repoName = String(repo || '')
    .trim()
    .replace(/\.git$/i, '');
  const existing = byName.get(packageName);
  if (!existing) {
    byName.set(packageName, {
      packageName,
      fromVersion: from,
      toVersion: to,
      upstreamRepo: repoName,
      disputed: {},
    });
    return;
  }
  existing.fromVersion = agree(existing, 'fromVersion', from);
  existing.toVersion = agree(existing, 'toVersion', to);
  existing.upstreamRepo = agree(existing, 'upstreamRepo', repoName);
}

// The opening paragraph that names the packages. It ends at the first blank
// line, bullet list (Release notes, Changelog, Commits), table, or heading.
function groupPreamble(visible) {
  if (visible.trimStart().startsWith('|')) return '';
  return visible.trimStart().split(/\n\s*\n|\n\s*[-*] |\n\||\n### |\nUpdates `/)[0];
}

function tableCells(line) {
  return line
    .trim()
    .replace(/^\|/, '')
    .replace(/\|$/, '')
    .split('|')
    .map((cell) => cell.trim());
}

const BACKTICKS_RE = /^`([^`]+)`$/;
const CHANGE_RE = /`([^`]+)`\s*(?:->|→)\s*`([^`]+)`/;

// Rows of the update table: Dependabot's `Package | From | To` or Renovate's
// `Package | ... | Change`. Release-note tables live inside <details>, which is already gone.
function updateTableRows(visible) {
  const lines = visible.split('\n');
  const start = lines.findIndex((line) => /^\|\s*Package\s*\|/i.test(line.trim()));
  if (start === -1) return [];
  const columns = tableCells(lines[start]).map((cell) => cell.toLowerCase());
  const rows = [];
  for (const line of lines.slice(start + 2)) {
    if (!line.trim().startsWith('|')) break;
    const cell = Object.fromEntries(tableCells(line).map((value, index) => [columns[index], value]));
    const change = CHANGE_RE.exec(cell.change || '');
    const from = change ? change[1] : (BACKTICKS_RE.exec(cell.from || '') || [])[1];
    const to = change ? change[2] : (BACKTICKS_RE.exec(cell.to || '') || [])[1];
    if (from && to) rows.push({ ...packageCell(cell.package || ''), from, to });
  }
  return rows;
}

// The repository of the `Bumps [x](github)` link. Any other GitHub URL may name an unrelated project.
function upstreamRepoFromBody(text) {
  const dependencyRepoMatch = text.match(
    /^\s*(?:Bumps|Update(?:s|d)?)\s+(?:the\s+requirements\s+on\s+)?\[[^\]]+\]\(\s*https?:\/\/(?:redirect\.)?github\.com\/([A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+)(?:[\/#?][^)\s]*)?\s*\)/im,
  );
  return dependencyRepoMatch ? dependencyRepoMatch[1].replace(/\.git$/i, '') : '';
}

// One map. Intro links, table rows, Updates lines, and the title all write it.
// A second non-empty repository or version that differs clears that field.
// Only the body outside <details> is read.
function collectPackageUpdates(title, body) {
  const byName = new Map();
  const visible = withoutDetails(body);
  const preamble = groupPreamble(visible);
  const linkRe = /\[([^\]]+)\]\(\s*(https?:\/\/[^)\s]+)[^)]*\)/gi;
  let link;
  while ((link = linkRe.exec(preamble)) !== null) {
    rememberUpdate(byName, link[1], '', '', githubRepoFromText(link[2]));
  }
  const bumpsRe = /\[([^\]]+)\]\(\s*(https?:\/\/[^)\s]+)[^)]*\)\s+from\s+(\S+)\s+to\s+(\S+)/gi;
  let bumps;
  while ((bumps = bumpsRe.exec(preamble)) !== null) {
    rememberUpdate(
      byName,
      bumps[1],
      bumps[3].replace(/[.,;:]+$/, ''),
      bumps[4].replace(/[.,;:]+$/, ''),
      githubRepoFromText(bumps[2]),
    );
  }
  const tableRows = updateTableRows(visible);
  for (const row of tableRows) {
    rememberUpdate(byName, row.name, row.from, row.to, row.repo);
  }
  const lineRe = /^Updates\s+`([^`]+)`\s+from\s+(\S+)\s+to\s+(\S+)/gm;
  let line;
  while ((line = lineRe.exec(visible)) !== null) {
    rememberUpdate(
      byName,
      line[1].trim(),
      line[2].replace(/[.,;:]+$/, ''),
      line[3].replace(/[.,;:]+$/, ''),
      githubRepoFromText(line[1]),
    );
  }

  // A table names every package; the title would add a second, differently spelled one.
  const titleMatch =
    tableRows.length === 0 &&
    (title.match(/[Uu]pdate\s+(.+?)\s+requirement\s+from\s+([^\s]+)\s+to\s+([^\s]+)/) ||
      title.match(/[Bb]ump\s+(.+?)\s+from\s+([^\s]+)\s+to\s+([^\s]+)/) ||
      title.match(/[Uu]pdate\s+(.+?)\s+from\s+([^\s]+)\s+to\s+([^\s]+)/));
  const renovateTitleMatch =
    tableRows.length === 0 &&
    !titleMatch &&
    (title.match(/(?:update|pin)\s+dependency\s+(.+?)\s+to\s+([^\s]+)/i) ||
      title.match(/(?:update|pin)\s+(.+?)\s+(?:action|digest|docker\s+tag)\s+to\s+([^\s]+)/i) ||
      title.match(/(?:update|pin)\s+(.+?)\s+to\s+([^\s]+)/i));
  if (titleMatch) {
    rememberUpdate(byName, titleMatch[1], titleMatch[2], titleMatch[3], '');
  } else if (renovateTitleMatch) {
    rememberUpdate(byName, renovateTitleMatch[1], '', renovateTitleMatch[2], '');
  }

  if (byName.size === 1) {
    const only = [...byName.values()][0];
    if (!only.fromVersion || !only.toVersion) {
      const arrow = visible.match(/`([^`\s]+)`\s*(?:->|→)\s*`([^`\s]+)`/);
      if (arrow) {
        rememberUpdate(
          byName,
          only.packageName,
          only.fromVersion ? '' : arrow[1],
          only.toVersion ? '' : arrow[2],
          '',
        );
      }
    }
  }
  return [...byName.values()].map(({ disputed, ...item }) => item);
}

function parseDependencyUpdate(title, body) {
  const upstreamRepo = upstreamRepoFromBody(body);

  let releaseNotes = extractDetailsSection(body, 'Release notes');
  if (!releaseNotes && upstreamRepo) {
    releaseNotes = extractDetailsSection(body, upstreamRepo);
  }
  if (!releaseNotes) {
    const rnHeading = body.match(/###\s*Release\s+Notes?\s*\n([\s\S]*?)(?=\n---|\n###\s|$)/i);
    if (rnHeading) releaseNotes = rnHeading[1].trim();
  }

  let commits = extractDetailsSection(body, 'Commits');
  if (!commits) {
    const shaMatches = body.match(/\b[0-9a-f]{7,40}\b/gi);
    if (shaMatches && shaMatches.length > 0) commits = shaMatches.join('\n');
  }

  const updates = collectPackageUpdates(title, body);
  const sole = updates.length === 1 ? updates[0] : null;
  return {
    upstreamRepo: sole ? sole.upstreamRepo : '',
    packageName: sole ? sole.packageName : '',
    fromVersion: sole ? sole.fromVersion : '',
    toVersion: sole ? sole.toVersion : '',
    releaseNotes,
    commits,
    updates,
  };
}

// Above a normal GitHub pull-request diff. Larger inputs fail open.
const MAX_DIFF_CHARS = 5000000;

/**
 * Stable id of one unified diff. A multi-commit series (more than one id
 * line) and any git failure return '' so the review runs.
 */
function patchIdFromDiff(diffText) {
  if (typeof diffText !== 'string' || !diffText.trim() || diffText.length > MAX_DIFF_CHARS) return '';
  let result;
  try {
    result = spawnSync('git', ['patch-id', '--stable'], { input: diffText, encoding: 'utf8' });
  } catch (_) {
    return '';
  }
  if (!result || result.error || result.status !== 0) return '';
  const lines = String(result.stdout || '')
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean);
  if (lines.length !== 1) return '';
  const match = lines[0].match(/^([0-9a-f]{40})\s+[0-9a-f]{40}$/);
  return match ? match[1] : '';
}

function reviewMarkerFromPatchId(patchId, resolvedSha) {
  if (typeof patchId !== 'string' || !/^[0-9a-f]{40}$/.test(patchId)) return '';
  if (typeof resolvedSha !== 'string' || !/^[0-9a-f]{40}$/.test(resolvedSha)) return '';
  return `<!-- cursor-dependabot-review patch-id:${patchId} resolved:${resolvedSha} -->`;
}

const RELEASE_REVIEW_MARKER_RE = /^<!-- cursor-dependabot-review patch-id:([0-9a-f]{40}) resolved:([0-9a-f]{40}) -->$/;

function reviewedShasForPatch(comments, patchId) {
  if (typeof patchId !== 'string' || !/^[0-9a-f]{40}$/.test(patchId)) return [];
  if (!Array.isArray(comments)) return [];
  const shas = [];
  for (const comment of comments) {
    if (comment?.user?.login !== ACTIONS_BOT_LOGIN) continue;
    const match = RELEASE_REVIEW_MARKER_RE.exec(commentFirstLine(comment.body));
    if (match && match[1] === patchId) shas.push(match[2]);
  }
  return shas;
}

function skipApplies(reviewedShas, resolvedSha) {
  if (typeof resolvedSha !== 'string' || !/^[0-9a-f]{40}$/.test(resolvedSha)) return false;
  return Array.isArray(reviewedShas) && reviewedShas.includes(resolvedSha);
}

function versionTextCameFromBody(title, body) {
  const fromTitle = parseDependencyUpdate(title, '');
  const parsed = parseDependencyUpdate(title, body);
  return parsed.fromVersion !== fromTitle.fromVersion || parsed.toVersion !== fromTitle.toVersion;
}

function commentFirstLine(body) {
  if (typeof body !== 'string') return '';
  const end = body.indexOf('\n');
  return (end === -1 ? body : body.slice(0, end)).trim();
}

function isActionsBotMarkerComment(comment) {
  if (comment?.user?.login !== ACTIONS_BOT_LOGIN) return false;
  const line = commentFirstLine(comment.body);
  if (line === LEGACY_REVIEW_MARKER) return true;
  return /^<!-- cursor-dependabot-review patch-id:[0-9a-f]{40}(?: resolved:[0-9a-f]{40})? -->$/.test(line);
}

// Replace the entry at name with a new file, so a link there is replaced, not followed.
function publishText(name, text) {
  const dir = fs.mkdtempSync('.dcr-publish-');
  const tmp = `${dir}/out`;
  try {
    fs.writeFileSync(tmp, text, { flag: 'wx' });
    fs.renameSync(tmp, name);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
}

async function runDependabotContext({ core }) {
  const body = process.env.PR_BODY || '';
  const title = process.env.PR_TITLE || '';
  const prNumber = Number(process.env.PR_NUMBER || '0');
  const parsed = parseDependencyUpdate(title, body);
  publishText('dependency_updates.json', JSON.stringify(parsed.updates, null, 2));

  if (!parsed.updates.some((item) => item.upstreamRepo)) {
    core.warning('PR body is missing an upstream GitHub repository link; the scan will record no_upstream_link.');
  }

  const normalizedReleaseNotes = parsed.releaseNotes || 'PR body did not include a Release notes details section.';
  const normalizedCommits = parsed.commits || 'PR body did not include a Commits details section.';

  const out = {
    prNumber,
    upstreamRepo: parsed.upstreamRepo,
    packageName: parsed.packageName,
    fromVersion: parsed.fromVersion,
    toVersion: parsed.toVersion,
    releaseNotes: normalizedReleaseNotes,
    commits: normalizedCommits,
  };
  publishText('dependabot_comment_context.json', JSON.stringify(out, null, 2));
  publishText('dependabot_release_notes.md', normalizedReleaseNotes);
  publishText('dependabot_commits.md', normalizedCommits);
  core.setOutput('upstream_repo', parsed.upstreamRepo);
  core.setOutput('package_name', parsed.packageName);
  core.setOutput('from_version', parsed.fromVersion);
  core.setOutput('to_version', parsed.toVersion);
  core.setOutput('version_range_from_body', versionTextCameFromBody(title, body) ? '1' : '0');
}

module.exports = runDependabotContext;
module.exports.parseDependencyUpdate = parseDependencyUpdate;
module.exports.MAX_DIFF_CHARS = MAX_DIFF_CHARS;
module.exports.patchIdFromDiff = patchIdFromDiff;
module.exports.reviewMarkerFromPatchId = reviewMarkerFromPatchId;
module.exports.reviewedShasForPatch = reviewedShasForPatch;
module.exports.skipApplies = skipApplies;
module.exports.versionTextCameFromBody = versionTextCameFromBody;
module.exports.isActionsBotMarkerComment = isActionsBotMarkerComment;
module.exports.LEGACY_REVIEW_MARKER = LEGACY_REVIEW_MARKER;
