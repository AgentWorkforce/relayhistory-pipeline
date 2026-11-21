/**
 * The frozen session snapshot the replay harness reads (docs/session-intel-replay.md, "Snapshot format").
 * A directory with `manifest.json` and `sessions.jsonl`. The loader verifies every listed file's sha256,
 * refuses a snapshot past its `deleteBy`, and validates every line with bounded sizes and no unknown
 * fields. Real snapshots live outside the repository; only synthetic fixtures are committed.
 */
import { lstat, readFile } from "node:fs/promises";
import { join } from "node:path";
import type { CommitFact, OutcomeFact, TimestampedUsage, TimestampedWrite } from "../../../packages/coordination-detect/src/index.ts";
import { boundedString, isoTime, isRecord, onlyKeys, sha256Hex, textCompare } from "./util.ts";

export const SNAPSHOT_FORMAT = "session-intel-snapshot/1";
export const SESSIONS_FILE = "sessions.jsonl";
export const MANIFEST_FILE = "manifest.json";

export const SNAPSHOT_BOUNDS = {
  manifestBytes: 64 * 1024,
  fileBytes: 256 * 1024 * 1024,
  lineBytes: 4 * 1024 * 1024,
  sessions: 100_000,
  files: 16,
  /** Per session, per fact list (writes, commits, outcomes, usage), and for the summary timeline and claims. */
  facts: 20_000,
  timeline: 1_000,
  claims: 1_000,
  parents: 64,
  evidenceIds: 64,
  id: 512,
  path: 4_096,
  text: 4_000,
  /** The replay window: at most 120 days. */
  windowMs: 120 * 24 * 60 * 60 * 1_000,
} as const;

export type ClaimKind = "intent" | "constraint" | "decision";
/**
 * Who a claim binds (docs/proposals/session-intelligence.md, "Rules that make the classes sharp"): a `session`
 * claim never propagates to another session; `project` and `repo` ones can. Absent, it is `session`.
 */
export type ClaimScope = "session" | "project" | "repo";
export const CLAIM_SCOPES: readonly ClaimScope[] = ["session", "project", "repo"];
/**
 * Read by the M2 conflict detectors (src/session-intel/conflict/) and, when conflicts are on, by the live advisor. `id` is
 * unique within its session (absent, it is `c<index>`, the claim's position in its line). `supersededAt`, when
 * set, is when a later claim or a decision record retired it; an as-of view shows it only once it is past.
 * `scope`, `id` and `supersededAt` are optional in the file, so a snapshot without them still loads as /1.
 */
export interface SnapshotClaim {
  id: string; at: string; kind: ClaimKind; scope: ClaimScope; text: string; confidence: number; evidenceIds: string[]; supersededAt?: string;
  /** The stored claim's topic slug (relayfacts claims carry one); optional. */
  topic?: string;
  /**
   * The stored claim record this claim belongs to (a relayfacts claim or decision id). Two sessions holding a claim
   * with the same recordId share one claim (its evidence spans both): agreement, never an intent_conflict or an
   * overlap; a shared constraint can still be violated by a write. Optional.
   */
  recordId?: string;
}
export interface SummaryEntry { at: string; text: string }

export interface SnapshotSession {
  orgId: string;
  workspaceId: string;
  harness: string;
  sessionId: string;
  projectId: string | null;
  repo: string | null;
  branch: string | null;
  title: string | null;
  userId: string | null;
  taskRef: string | null;
  parentSessionIds: string[];
  createdAt: string;
  lastActivityAt: string;
  /**
   * The short summary as it changed over time, sorted by `at`. A plain `shortSummary` string (or an object
   * without a timeline) becomes one entry at `lastActivityAt`: with no time it is never visible early.
   */
  summaryTimeline: SummaryEntry[];
  writes: TimestampedWrite[];
  commits: CommitFact[];
  outcomes: OutcomeFact[];
  usage: TimestampedUsage[];
  claims: SnapshotClaim[];
}

export interface SnapshotManifest {
  format: typeof SNAPSHOT_FORMAT;
  createdAt: string;
  deleteBy: string;
  window: { since: string; until: string };
  files: Record<string, string>;
}

export interface Snapshot {
  dir: string;
  manifest: SnapshotManifest;
  /** sha256 of manifest.json's bytes: it pins every file through their hashes. */
  manifestSha256: string;
  sessions: SnapshotSession[];
  /** The other listed files (the harvest's coverage.json), as the loader verified them against the manifest. */
  extraFiles?: Record<string, string>;
}

/** The harvest's coverage receipt (docs/session-intel-harvest.md). */
export const COVERAGE_FILE = "coverage.json";
const REASON_CODE = /^[a-z0-9_]{1,64}$/u;

/**
 * The coverage gap a snapshot declares, as one incomplete-replay reason, or null when it declares none.
 * A snapshot without a coverage receipt (an import, a fixture) declares none. A receipt that is not
 * `complete: true`, or cannot be read, is a gap: the replay refuses it unless told otherwise, and then
 * carries the reason in ReplayResult.incomplete, so held-out scoring refuses it.
 */
export function coverageGap(snapshot: Pick<Snapshot, "extraFiles">): string | null {
  const text = snapshot.extraFiles?.[COVERAGE_FILE];
  if (text === undefined) return null;
  let receipt: unknown;
  try { receipt = JSON.parse(text); } catch { return "coverage: the coverage receipt is not valid JSON"; }
  if (!isRecord(receipt)) return "coverage: the coverage receipt is not an object";
  if (receipt.complete === true) {
    // A record-only harvest is complete but not representative: only a verified History API frame or the
    // 96Bulls read API's own marker on every page is. A receipt without the field predates it: unverified.
    const a = receipt.sourceAttribution;
    if (a === "verified" || a === "read_worker") return null;
    return "coverage: the harvest's source attribution is unverified (no --require-list-backend, and not every frame page carried the read API's marker)";
  }
  const reasons = Array.isArray(receipt.incompleteReasons)
    ? receipt.incompleteReasons.filter((x): x is string => typeof x === "string" && REASON_CODE.test(x))
    : [];
  return `coverage: the harvest's coverage receipt says complete:false (${reasons.length ? reasons.join(", ") : "no reason given"})`;
}

export class SnapshotError extends Error {
  constructor(message: string) { super(`snapshot: ${message}`); this.name = "SnapshotError"; }
}

const FILE_NAME = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;
const SHA256 = /^[0-9a-f]{64}$/;

export function parseManifest(raw: unknown): SnapshotManifest {
  if (!isRecord(raw)) throw new SnapshotError("manifest must be an object");
  onlyKeys(raw, ["format", "createdAt", "deleteBy", "window", "files"], "manifest");
  if (raw.format !== SNAPSHOT_FORMAT) throw new SnapshotError(`manifest format must be ${SNAPSHOT_FORMAT}`);
  const createdAt = isoTime(raw.createdAt, "manifest.createdAt");
  const deleteBy = isoTime(raw.deleteBy, "manifest.deleteBy");
  if (deleteBy < createdAt) throw new SnapshotError("manifest.deleteBy is before createdAt");
  if (!isRecord(raw.window)) throw new SnapshotError("manifest.window must be an object");
  onlyKeys(raw.window, ["since", "until"], "manifest.window");
  const since = isoTime(raw.window.since, "manifest.window.since");
  const until = isoTime(raw.window.until, "manifest.window.until");
  const span = Date.parse(until) - Date.parse(since);
  if (!(span > 0) || span > SNAPSHOT_BOUNDS.windowMs) throw new SnapshotError("manifest.window must have since < until, at most 120 days apart");
  if (!isRecord(raw.files)) throw new SnapshotError("manifest.files must be an object");
  const names = Object.keys(raw.files);
  if (names.length > SNAPSHOT_BOUNDS.files) throw new SnapshotError(`manifest.files lists more than ${SNAPSHOT_BOUNDS.files} files`);
  const files: Record<string, string> = {};
  for (const name of names) {
    // A plain name in the snapshot directory: no separators, no "..", no dotfiles.
    if (!FILE_NAME.test(name) || name.includes("..") || name === MANIFEST_FILE) throw new SnapshotError("manifest.files has an invalid file name");
    const digest = raw.files[name];
    if (typeof digest !== "string" || !SHA256.test(digest)) throw new SnapshotError("manifest.files values must be lowercase sha256 hex");
    files[name] = digest;
  }
  if (!files[SESSIONS_FILE]) throw new SnapshotError(`manifest.files must list ${SESSIONS_FILE}`);
  return { format: SNAPSHOT_FORMAT, createdAt, deleteBy, window: { since, until }, files };
}

async function readBounded(path: string, max: number, what: string): Promise<Buffer> {
  let st;
  try { st = await lstat(path); } catch { throw new SnapshotError(`${what} is missing`); }
  if (!st.isFile()) throw new SnapshotError(`${what} must be a regular file (no symlinks)`);
  if (st.size > max) throw new SnapshotError(`${what} exceeds ${max} bytes`);
  const buf = await readFile(path);
  if (buf.length > max) throw new SnapshotError(`${what} exceeds ${max} bytes`);
  return buf;
}

const id = (v: unknown, field: string) => boundedString(v, field, SNAPSHOT_BOUNDS.id)!;
const optId = (v: unknown, field: string) => boundedString(v, field, SNAPSHOT_BOUNDS.id, { nullable: true });
const HARNESS = /^[A-Za-z0-9_.-]{1,64}$/;
/** A claim id: no `#`, `:`, `/` or whitespace, so `<harness>:<sessionId>#<id>` names it unambiguously. */
const CLAIM_ID = /^[A-Za-z0-9_.-]{1,128}$/;
/** A topic slug, as relayfacts stores it (062080/062150 topic CHECK). */
const TOPIC_SLUG = /^[a-z0-9][a-z0-9-]{0,63}$/;

function list(v: unknown, field: string, max: number): unknown[] {
  if (v === undefined) return [];
  if (!Array.isArray(v)) throw new SnapshotError(`${field} must be an array`);
  if (v.length > max) throw new SnapshotError(`${field} has more than ${max} entries`);
  return v;
}

function within(ts: string, s: { createdAt: string; lastActivityAt: string }, field: string) {
  if (ts < s.createdAt || ts > s.lastActivityAt) throw new SnapshotError(`${field} must lie between createdAt and lastActivityAt`);
}

const SESSION_KEYS = [
  "orgId", "workspaceId", "harness", "sessionId", "projectId", "repo", "branch", "title", "userId", "taskRef",
  "parentSessionIds", "createdAt", "lastActivityAt", "shortSummary", "writes", "commits", "outcomes", "usage", "claims",
] as const;

/** One sessions.jsonl line, validated and normalized. `where` names the line in errors (never its content). */
export function parseSession(raw: unknown, where: string): SnapshotSession {
  if (!isRecord(raw)) throw new SnapshotError(`${where} must be an object`);
  try {
    onlyKeys(raw, SESSION_KEYS, where);
    const harness = id(raw.harness, `${where}.harness`);
    if (!HARNESS.test(harness)) throw new SnapshotError(`${where}.harness must match ${HARNESS.source}`);
    const createdAt = isoTime(raw.createdAt, `${where}.createdAt`);
    const lastActivityAt = isoTime(raw.lastActivityAt, `${where}.lastActivityAt`);
    if (lastActivityAt < createdAt) throw new SnapshotError(`${where}.lastActivityAt is before createdAt`);
    const times = { createdAt, lastActivityAt };
    const parents = list(raw.parentSessionIds, `${where}.parentSessionIds`, SNAPSHOT_BOUNDS.parents)
      .map((p, i) => id(p, `${where}.parentSessionIds[${i}]`));

    const writes = list(raw.writes, `${where}.writes`, SNAPSHOT_BOUNDS.facts).map((w, i): TimestampedWrite => {
      const f = `${where}.writes[${i}]`;
      if (!isRecord(w)) throw new SnapshotError(`${f} must be an object`);
      onlyKeys(w, ["path", "ts", "eventId", "kind", "branch"], f);
      const ts = isoTime(w.ts, `${f}.ts`);
      within(ts, times, `${f}.ts`);
      return {
        path: boundedString(w.path, `${f}.path`, SNAPSHOT_BOUNDS.path)!, ts, eventId: id(w.eventId, `${f}.eventId`),
        kind: id(w.kind, `${f}.kind`), branch: optId(w.branch, `${f}.branch`),
      };
    });
    const commits = list(raw.commits, `${where}.commits`, SNAPSHOT_BOUNDS.facts).map((c, i): CommitFact => {
      const f = `${where}.commits[${i}]`;
      if (!isRecord(c)) throw new SnapshotError(`${f} must be an object`);
      onlyKeys(c, ["sha", "ts", "eventId"], f);
      const ts = isoTime(c.ts, `${f}.ts`);
      within(ts, times, `${f}.ts`);
      return { sha: id(c.sha, `${f}.sha`), ts, eventId: id(c.eventId, `${f}.eventId`), reverted: null };
    });
    const outcomes = list(raw.outcomes, `${where}.outcomes`, SNAPSHOT_BOUNDS.facts).map((o, i): OutcomeFact => {
      const f = `${where}.outcomes[${i}]`;
      if (!isRecord(o)) throw new SnapshotError(`${f} must be an object`);
      onlyKeys(o, ["key", "ts", "eventId", "reverted"], f);
      const ts = isoTime(o.ts, `${f}.ts`);
      within(ts, times, `${f}.ts`);
      if (typeof o.reverted !== "boolean") throw new SnapshotError(`${f}.reverted must be a boolean`);
      return { key: id(o.key, `${f}.key`), ts, eventId: id(o.eventId, `${f}.eventId`), reverted: o.reverted };
    });
    const count = (v: unknown, f: string) => {
      if (v === null || v === undefined) return null;
      if (typeof v !== "number" || !Number.isSafeInteger(v) || v < 0) throw new SnapshotError(`${f} must be a non-negative integer or null`);
      return v;
    };
    const usage = list(raw.usage, `${where}.usage`, SNAPSHOT_BOUNDS.facts).map((u, i): TimestampedUsage => {
      const f = `${where}.usage[${i}]`;
      if (!isRecord(u)) throw new SnapshotError(`${f} must be an object`);
      onlyKeys(u, ["ts", "eventId", "tokens", "costUsdMicros"], f);
      const ts = isoTime(u.ts, `${f}.ts`);
      within(ts, times, `${f}.ts`);
      return { ts, eventId: id(u.eventId, `${f}.eventId`), tokens: count(u.tokens, `${f}.tokens`), costUsdMicros: count(u.costUsdMicros, `${f}.costUsdMicros`) };
    });

    const entry = (e: unknown, f: string): SummaryEntry => {
      if (!isRecord(e)) throw new SnapshotError(`${f} must be an object`);
      onlyKeys(e, ["at", "text"], f);
      const at = isoTime(e.at, `${f}.at`);
      if (at < createdAt) throw new SnapshotError(`${f}.at is before createdAt`);
      return { at, text: boundedString(e.text, `${f}.text`, SNAPSHOT_BOUNDS.text)! };
    };
    let summaryTimeline: SummaryEntry[] = [];
    const s = raw.shortSummary;
    if (typeof s === "string") summaryTimeline = [{ at: lastActivityAt, text: boundedString(s, `${where}.shortSummary`, SNAPSHOT_BOUNDS.text)! }];
    else if (isRecord(s)) {
      onlyKeys(s, ["text", "timeline"], `${where}.shortSummary`);
      if (s.timeline !== undefined) summaryTimeline = list(s.timeline, `${where}.shortSummary.timeline`, SNAPSHOT_BOUNDS.timeline)
        .map((e, i) => entry(e, `${where}.shortSummary.timeline[${i}]`));
      else if (s.text !== undefined && s.text !== null)
        summaryTimeline = [{ at: lastActivityAt, text: boundedString(s.text, `${where}.shortSummary.text`, SNAPSHOT_BOUNDS.text)! }];
    } else if (s !== null && s !== undefined) throw new SnapshotError(`${where}.shortSummary must be a string, an object or null`);
    summaryTimeline.sort((a, b) => textCompare(a.at, b.at));

    const claimIds = new Set<string>();
    const claims = list(raw.claims, `${where}.claims`, SNAPSHOT_BOUNDS.claims).map((c, i): SnapshotClaim => {
      const f = `${where}.claims[${i}]`;
      if (!isRecord(c)) throw new SnapshotError(`${f} must be an object`);
      onlyKeys(c, ["id", "at", "kind", "scope", "text", "confidence", "evidenceIds", "supersededAt", "topic", "recordId"], f);
      const at = isoTime(c.at, `${f}.at`);
      if (at < createdAt) throw new SnapshotError(`${f}.at is before createdAt`);
      if (c.kind !== "intent" && c.kind !== "constraint" && c.kind !== "decision") throw new SnapshotError(`${f}.kind must be intent, constraint or decision`);
      if (c.scope !== undefined && !CLAIM_SCOPES.includes(c.scope as ClaimScope)) throw new SnapshotError(`${f}.scope must be session, project or repo`);
      if (typeof c.confidence !== "number" || !(c.confidence >= 0 && c.confidence <= 1)) throw new SnapshotError(`${f}.confidence must be in [0, 1]`);
      const claimId = c.id === undefined ? `c${i}` : id(c.id, `${f}.id`);
      if (!CLAIM_ID.test(claimId)) throw new SnapshotError(`${f}.id must match ${CLAIM_ID.source}`);
      if (claimIds.has(claimId)) throw new SnapshotError(`${f}.id repeats a claim id of this session`);
      claimIds.add(claimId);
      if (c.topic !== undefined && (typeof c.topic !== "string" || !TOPIC_SLUG.test(c.topic))) throw new SnapshotError(`${f}.topic must be a topic slug`);
      if (c.recordId !== undefined && (typeof c.recordId !== "string" || !CLAIM_ID.test(c.recordId))) throw new SnapshotError(`${f}.recordId must match ${CLAIM_ID.source}`);
      let supersededAt: string | undefined;
      if (c.supersededAt !== undefined) {
        supersededAt = isoTime(c.supersededAt, `${f}.supersededAt`);
        if (supersededAt < at) throw new SnapshotError(`${f}.supersededAt is before its at`);
      }
      return {
        id: claimId, at, kind: c.kind, scope: (c.scope as ClaimScope | undefined) ?? "session",
        text: boundedString(c.text, `${f}.text`, SNAPSHOT_BOUNDS.text)!, confidence: c.confidence,
        evidenceIds: list(c.evidenceIds, `${f}.evidenceIds`, SNAPSHOT_BOUNDS.evidenceIds).map((e, j) => id(e, `${f}.evidenceIds[${j}]`)),
        ...(supersededAt === undefined ? {} : { supersededAt }),
        ...(c.topic === undefined ? {} : { topic: c.topic as string }),
        ...(c.recordId === undefined ? {} : { recordId: c.recordId as string }),
      };
    });

    return {
      orgId: id(raw.orgId, `${where}.orgId`), workspaceId: id(raw.workspaceId, `${where}.workspaceId`), harness,
      sessionId: id(raw.sessionId, `${where}.sessionId`), projectId: optId(raw.projectId, `${where}.projectId`),
      repo: optId(raw.repo, `${where}.repo`), branch: optId(raw.branch, `${where}.branch`),
      title: boundedString(raw.title, `${where}.title`, SNAPSHOT_BOUNDS.text, { nullable: true }),
      userId: optId(raw.userId, `${where}.userId`), taskRef: optId(raw.taskRef, `${where}.taskRef`),
      parentSessionIds: parents, createdAt, lastActivityAt, summaryTimeline, writes, commits, outcomes, usage, claims,
    };
  } catch (err) {
    if (err instanceof SnapshotError) throw err;
    throw new SnapshotError((err as Error).message);
  }
}

export interface LoadOptions {
  /** The wall clock for the deleteBy refusal; tests inject it. */
  now?: Date;
}

/** Loads and verifies a snapshot directory. Refuses past deleteBy before reading any session. */
export async function loadSnapshot(dir: string, opts: LoadOptions = {}): Promise<Snapshot> {
  const manifestBytes = await readBounded(join(dir, MANIFEST_FILE), SNAPSHOT_BOUNDS.manifestBytes, MANIFEST_FILE);
  let raw: unknown;
  try { raw = JSON.parse(manifestBytes.toString("utf8")); } catch { throw new SnapshotError(`${MANIFEST_FILE} is not valid JSON`); }
  const manifest = parseManifest(raw);
  const now = opts.now ?? new Date();
  if (now.toISOString() > manifest.deleteBy)
    throw new SnapshotError(`past its deleteBy (${manifest.deleteBy}); delete the snapshot instead of replaying it`);
  const contents = new Map<string, Buffer>();
  for (const [name, digest] of Object.entries(manifest.files)) {
    const buf = await readBounded(join(dir, name), SNAPSHOT_BOUNDS.fileBytes, name);
    if (sha256Hex(buf) !== digest) throw new SnapshotError(`${name} does not match its manifest sha256`);
    contents.set(name, buf);
  }
  const text = contents.get(SESSIONS_FILE)!.toString("utf8");
  const sessions: SnapshotSession[] = [];
  const seen = new Set<string>();
  const lines = text.split("\n");
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i]!;
    if (!line.trim()) continue;
    const where = `${SESSIONS_FILE} line ${i + 1}`;
    if (Buffer.byteLength(line) > SNAPSHOT_BOUNDS.lineBytes) throw new SnapshotError(`${where} exceeds ${SNAPSHOT_BOUNDS.lineBytes} bytes`);
    if (sessions.length >= SNAPSHOT_BOUNDS.sessions) throw new SnapshotError(`more than ${SNAPSHOT_BOUNDS.sessions} sessions`);
    let parsed: unknown;
    try { parsed = JSON.parse(line); } catch { throw new SnapshotError(`${where} is not valid JSON`); }
    const s = parseSession(parsed, where);
    // Labels and alerts name a session by harness and id, so the pair must be unique in the snapshot.
    const k = `${s.harness}\u0000${s.sessionId}`;
    if (seen.has(k)) throw new SnapshotError(`${where} repeats a (harness, sessionId) already in the snapshot`);
    seen.add(k);
    sessions.push(s);
  }
  sessions.sort((a, b) => textCompare(a.harness, b.harness) || textCompare(a.sessionId, b.sessionId));
  const extraFiles: Record<string, string> = {};
  for (const [name, buf] of contents) if (name !== SESSIONS_FILE) extraFiles[name] = buf.toString("utf8");
  return { dir, manifest, manifestSha256: sha256Hex(manifestBytes), sessions, extraFiles };
}
