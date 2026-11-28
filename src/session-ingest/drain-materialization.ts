/**
 * FULL-to-drain materialization (decision 0005, "The buffered backlog"; wave-1 blocker).
 *
 * The FULL's revisions section (upload-fence-backfill.ts) puts every owned lazy native slot into the S3 revision index
 * as [revision, null revision id, current_digest]. For a buffered org those slots name lines that exist only in the
 * staged backlog: the publisher that would have written them never ran. An index entry is therefore NOT proof of
 * canonical bytes. The drain (buffered-drain.ts step 4) always writes a slot-entry line (a replay is a no-op under the
 * manifest's batch-digest dedupe), and trusts a full entry (revision id set) only when the session's canonical
 * manifest holds that exact line (`canonicalHolds`).
 *
 * `verifyDrainMaterialization` proves the result, record by record, from S3 alone (no aggregate check):
 *
 *   allowed    every line the drain may have written: a line SQL kept (the stage's accepted v2 sidecar, live, not
 *              superseded) of a stage marked `drained`, in a session the user's CURRENT S3 fence does not delete.
 *   expected   per (origin, record id), the highest allowed revision (supersession: a newer revision wins).
 *   excluded   every other line of the org's staged backlog (not kept, not live, superseded, unacknowledged, pre-reset,
 *              or in a deleted session): erased or never published.
 *   winners    every digest-bearing index entry of the backlog's users the fence has not forgotten, located by the
 *              owner stubs (revision-only seeded entries are not judged).
 *
 * Judged per record: an expected record at the winner's revision must be canonical as exactly the expected line (the
 * stored, scrubbed chunk line, byte-equal), else `missing` / `mismatch`; an index below it is `index_missing`. Any
 * other winner (a higher revision than the backlog holds, or a record the backlog never staged) must have a canonical
 * line AT that revision: the index is never trusted on its own word; without one it is `unrecoverable` (no staged
 * bytes, no canonical bytes). Every canonical line of a touched session must be allowed, the winner beyond the backlog,
 * or in the pre-drain `baseline` (preserved); else `unexpected`, or `erased_present` for an excluded line. An accepted,
 * live stage without its marker is `undrained`; an org without its export completion is `export_missing`. `ok` only
 * when every list is empty.
 *
 * Nothing here logs. Records in the result are named by sha256 of their identity tuple, never by id or content.
 */
import { createHash } from "node:crypto";
import {
  chunkKeys, isObjectStoreError, objectKey, parseManifest, readAll, type ManifestV1, type ObjectStore,
} from "../shared/vendor.ts";
import { layoutComponent } from "../shared/layout-component.ts";
import { scrubRecords } from "../../packages/pipeline/src/lib/scrub-engine/index.ts";
import { fitLine } from "./oversize.ts";
import { entryForgotten, isDeletedForAdmission, s3LedgerObjects, sessionKeyHash, type FenceDoc, type IndexEntry } from "./ledger-s3.ts";
import type { LedgerTenant } from "./ledger.ts";
import { NO_SESSION_HARNESS, NO_SESSION_ID, liveMachineId } from "./writer.ts";
// A cycle with buffered-drain.ts (which imports canonicalHolds): only called at run time, never at module init.
import { acceptedCompleteKey, parseAcceptedComplete, parseAcceptedMarker } from "./buffered-drain.ts";

const decoder = new TextDecoder("utf-8", { fatal: true });
const encoder = new TextEncoder();
const sha256Hex = (v: string | Uint8Array) => createHash("sha256").update(v).digest("hex");
const MAX_CHUNK_BYTES = 16 * 1024 * 1024;

export class CanonicalReadError extends Error {
  constructor(readonly code: "pointer_invalid" | "manifest_invalid" | "chunk_missing" | "chunk_digest_mismatch" | "chunk_invalid") {
    super(`canonical read: ${code}`);
    this.name = "CanonicalReadError";
  }
}

export interface CanonicalSessionRef {
  keyRoot: string;
  orgId: string;
  workspaceId: string;
  machineId: string;
  source: string;
  sessionId: string;
}

/** One canonical chunk line, as stored (scrubbed), with the fields the fold uses. */
export interface CanonicalLine {
  text: string;
  originId: string;
  recordId: string;
  revision: number;
  operation: string;
}

async function getText(store: ObjectStore, key: string, max: number): Promise<string | null> {
  let object;
  try {
    object = await store.getObject(objectKey(key));
  } catch (error) {
    if (isObjectStoreError(error, "not_found")) return null;
    throw error;
  }
  const bytes = await readAll(object.body);
  if (bytes.byteLength > max) throw new CanonicalReadError("manifest_invalid");
  return decoder.decode(bytes);
}

/**
 * Every canonical line of one session, in manifest order, or null when the session has no pointer (never written).
 * Fails closed (throws) on an unreadable pointer, manifest or chunk: a missing chunk is never read as "absent".
 */
export async function readCanonicalSession(
  store: ObjectStore,
  ref: CanonicalSessionRef,
  decompress: (bytes: Uint8Array) => Uint8Array,
): Promise<CanonicalLine[] | null> {
  const prefix = `${ref.keyRoot}/${ref.orgId}`;
  const pointerKey = `${prefix}/live-sessions/v1/ws=${layoutComponent(ref.workspaceId)}/m=${ref.machineId}/h=${layoutComponent(ref.source)}/s=${layoutComponent(ref.sessionId)}.json`;
  const pointerText = await getText(store, pointerKey, 64 * 1024);
  if (pointerText === null) return null;
  let manifestKey: string;
  try {
    const p = JSON.parse(pointerText) as { v?: unknown; manifest_key?: unknown };
    if (p.v !== 1 || typeof p.manifest_key !== "string") throw new Error();
    manifestKey = p.manifest_key;
  } catch {
    throw new CanonicalReadError("pointer_invalid");
  }
  // The pointer is written before the manifest's first CAS: a pointer without a manifest holds nothing yet.
  const manifestText = await getText(store, manifestKey, 8 * 1024 * 1024);
  if (manifestText === null) return [];
  let manifest: ManifestV1;
  try {
    manifest = await parseManifest(manifestText, manifestKey);
  } catch {
    throw new CanonicalReadError("manifest_invalid");
  }
  const out: CanonicalLine[] = [];
  for (const { entry, key } of chunkKeys(manifest, { prefix })) {
    let stored: Uint8Array;
    try {
      stored = await readAll((await store.getObject(key)).body);
    } catch (error) {
      if (isObjectStoreError(error, "not_found")) throw new CanonicalReadError("chunk_missing");
      throw error;
    }
    if (stored.byteLength !== entry.stored_bytes || sha256Hex(stored) !== entry.stored_sha256) throw new CanonicalReadError("chunk_digest_mismatch");
    let text: string;
    try {
      const raw = decompress(stored);
      if (raw.byteLength > MAX_CHUNK_BYTES || (entry.raw_sha256 && sha256Hex(raw) !== entry.raw_sha256)) throw new Error();
      text = decoder.decode(raw);
    } catch {
      throw new CanonicalReadError("chunk_invalid");
    }
    for (const line of text.split("\n")) {
      if (!line) continue;
      let r: Record<string, unknown>;
      try {
        r = JSON.parse(line) as Record<string, unknown>;
      } catch {
        throw new CanonicalReadError("chunk_invalid");
      }
      const revision = Number(r.revision);
      if (typeof r.record_id !== "string" || typeof r.origin_id !== "string" || !Number.isSafeInteger(revision) || typeof r.operation !== "string")
        throw new CanonicalReadError("chunk_invalid");
      out.push({ text: line, originId: r.origin_id, recordId: r.record_id, revision, operation: r.operation });
    }
  }
  return out;
}

/** The exact canonical form of a staged line: the drain's refit (`fitLine`), scrubbed as the sealer scrubs a chunk. */
export function canonicalFormOf(stagedLine: string): string {
  const scrubbed = decoder.decode(scrubRecords(encoder.encode(fitLine(stagedLine) + "\n")).scrubbed);
  return scrubbed.endsWith("\n") ? scrubbed.slice(0, -1) : scrubbed;
}

/**
 * Whether the canonical session already holds this exact staged line: the same (origin, record, revision, operation)
 * and the same stored bytes. The writer stores lines only through the sealer, so equal bytes are the same record.
 */
export function canonicalHolds(canonical: readonly CanonicalLine[] | null, stagedLine: string): boolean {
  if (!canonical || canonical.length === 0) return false;
  const want = canonicalFormOf(stagedLine);
  return canonical.some((c) => c.text === want);
}

// ---- verification ----

export type VerifyFailure =
  | "export_missing" | "missing" | "mismatch" | "index_missing" | "unrecoverable" | "unexpected" | "erased_present" | "baseline_lost" | "undrained" | "marker_invalid" | "no_verdict" | "unreadable";

export interface VerifyResult {
  ok: boolean;
  /** Expected records whose canonical fold is exactly the expected line. */
  exact: number;
  /** Expected records a later admission superseded (the index holds a higher revision, canonical at that revision). */
  superseded_later: number;
  /** Index winners outside the backlog's expected set that canonical holds (a later admission, or preexisting). */
  index_winners_canonical: number;
  /** Staged lines excluded from the expected set (erased, not kept, not live, pre-reset) and not canonical. */
  excluded_absent: number;
  /** Canonical lines in the baseline (before the drain) that the backlog does not account for: preserved, not judged. */
  baseline_preserved: number;
  counts: Record<VerifyFailure, number>;
  /** Per record (or per stage for `undrained`, per org for `export_missing`): sha256 of its identity tuple. */
  failures: { kind: VerifyFailure; ref: string }[];
}

export interface VerifyOptions {
  store: ObjectStore;
  keyRoot: string;
  orgId: string;
  decompress: (bytes: Uint8Array) => Uint8Array;
  /**
   * The org's canonical lines before the drain (`snapshotCanonical`). With it, a canonical line the backlog does not
   * account for is `unexpected` (or `erased_present`) only when the drain introduced it; lines already there are
   * preserved. Without it every such line fails (fail closed).
   */
  baseline?: CanonicalSnapshot;
}

/** A staged object as the verifier needs it (buffered-drain.ts readLiveStaged). */
export interface StagedForVerify {
  header: { org_id: string; workspace_id: string; origin_id: string; user_id: string };
  groups: { source: string; sessionId: string; lines: string[]; meta?: [string, string, string, string | null][] }[];
  sha256: string;
}

export interface VerifyDeps {
  /** List every key under a prefix (S3 key order). */
  list(prefix: string): Promise<string[]>;
  /** The live staged object at `key`, or null when it is not live (redacted, vanished, corrupt). */
  readStaged(key: string): Promise<StagedForVerify | null>;
  /** buffered-drain.ts parseStagedKey: only exact staged keys of this org are read. */
  parseStagedKey(key: string): { orgId: string } | null;
}

/** Canonical lines per session (sessionKey below), as read before a drain. */
export interface CanonicalSnapshot { sessions: Map<string, string[]> }

type Line = { tenant: LedgerTenant; origin: string; source: string; sessionId: string; line: string; recordId: string; revision: number };
type Session = { tenant: LedgerTenant; origin: string; source: string; sessionId: string };
const sessionKey = (t: LedgerTenant, origin: string, source: string, sessionId: string) => JSON.stringify([t.orgId, t.workspaceId, t.userId, origin, source, sessionId]);
const recordKey = (t: LedgerTenant, origin: string, recordId: string) => JSON.stringify([t.orgId, t.workspaceId, t.userId, origin, recordId]);
const userKey = (t: LedgerTenant) => JSON.stringify([t.orgId, t.workspaceId, t.userId]);
const ref = (s: string) => sha256Hex(s);

interface Backlog {
  allowed: Line[];
  excluded: Line[];
  sessions: Map<string, Session>;
  /** Every (user, origin) of the backlog. */
  origins: Map<string, { tenant: LedgerTenant; origin: string }>;
  fences: Map<string, FenceDoc | null>;
  undrained: string[];
  /** Stages whose sidecar or marker is malformed, bound to other bytes, or carries an unknown outcome: refused. */
  invalid: string[];
  /** Live stages without an exported verdict (no sidecar): refused, never assumed unacknowledged. */
  noVerdict: string[];
  exportComplete: boolean;
  /** Sessions (sessionKey) the user's CURRENT S3 fence deletes, or whose user has no fence (erased). */
  deleted: Set<string>;
}

async function collectBacklog(opts: VerifyOptions, deps: VerifyDeps): Promise<Backlog> {
  const { store, keyRoot, orgId } = opts;
  const io = s3LedgerObjects(store, keyRoot);
  const complete = await getText(store, acceptedCompleteKey(keyRoot, orgId), 64 * 1024);
  const keys = await deps.list(`${keyRoot}/${orgId}/buffered-batches/v1`);
  const all = new Set(keys);
  const staged = keys.filter((k) => k.endsWith(".staged.ndjson.zst") && deps.parseStagedKey(k)?.orgId === orgId);
  const b: Backlog = { allowed: [], excluded: [], sessions: new Map(), origins: new Map(), fences: new Map(), undrained: [], invalid: [], noVerdict: [], exportComplete: complete !== null && parseAcceptedComplete(complete) !== null, deleted: new Set() };
  const fenceOf = async (t: LedgerTenant): Promise<FenceDoc | null> => {
    const k = userKey(t);
    if (!b.fences.has(k)) {
      const read = await io.readFence(io.keysFor(t).fence);
      b.fences.set(k, read.state === "present" ? read.fence : null);
    }
    return b.fences.get(k)!;
  };
  const doc = async (key: string): Promise<{ state: "absent" } | { state: "invalid" } | { state: "ok"; text: string }> => {
    if (!all.has(key)) return { state: "absent" };
    try {
      const text = await getText(store, key, 1024 * 1024);
      return text === null ? { state: "absent" } : { state: "ok", text };
    } catch {
      return { state: "invalid" };
    }
  };
  const markDeleted = async (t: LedgerTenant, origin: string, source: string, sessionId: string) => {
    const fence = await fenceOf(t);
    if (fence === null || (sessionId !== NO_SESSION_ID && isDeletedForAdmission(fence, origin, source, sessionId))) b.deleted.add(sessionKey(t, origin, source, sessionId));
  };
  for (const key of staged) {
    const s = await deps.readStaged(key);
    if (!s) continue; // not live: nothing of it can be canonical through the drain
    const tenant: LedgerTenant = { orgId: s.header.org_id, workspaceId: s.header.workspace_id, userId: s.header.user_id };
    const origin = s.header.origin_id;
    b.origins.set(JSON.stringify([userKey(tenant), origin]), { tenant, origin });
    const items = s.groups.map((g) => g.lines.map((line) => {
      const r = JSON.parse(line) as { record_id: string; revision: string };
      return { tenant, origin, source: g.source, sessionId: g.sessionId, line, recordId: r.record_id, revision: Number(r.revision) } satisfies Line;
    }));
    for (const g of s.groups) {
      b.sessions.set(sessionKey(tenant, origin, g.source, g.sessionId), { tenant, origin, source: g.source, sessionId: g.sessionId });
      await markDeleted(tenant, origin, g.source, g.sessionId);
    }
    // The export's sidecar. Absent proves nothing by itself: no exported verdict (the export has not run, is not
    // complete, or the stage arrived after it). Calling such a stage unacknowledged needs a complete export that
    // provably covered it, and the pinned export records no per-stage coverage, so it is refused as `no_verdict`.
    // Present but malformed, bound to other bytes, or not matching the stage's groups: refused, never silently excluded.
    const sidecar = await doc(key + ".accepted.json");
    if (sidecar.state === "absent" && !all.has(key + ".superseded.json")) { b.noVerdict.push(key); continue; }
    if (sidecar.state === "invalid") { b.invalid.push(key); continue; }
    const marker = sidecar.state === "ok" ? parseAcceptedMarker(sidecar.text) : null;
    if (sidecar.state === "ok" && (!marker || marker.stage_sha256 !== s.sha256 ||
        (marker.live && (marker.kept.length !== s.groups.length || marker.kept.some((ks, gi) => ks.some((j) => j >= s.groups[gi]!.lines.length)))))) {
      b.invalid.push(key);
      continue;
    }
    if (!marker || !marker.live || all.has(key + ".superseded.json")) {
      b.excluded.push(...items.flat());
      continue;
    }
    // The drain's marker. Absent: not drained. Known outcomes only, over these exact bytes: `drained` (allowed) and
    // `pre_reset` (excluded). Anything else (malformed, another kind or digest, an outcome this proof does not know,
    // such as a per-session settlement before its format is agreed) is refused, never silently excluded.
    const drained = await doc(key + ".drained.json");
    if (drained.state === "absent") { b.undrained.push(key); continue; }
    let md: Record<string, unknown> | null = null;
    try {
      md = drained.state === "ok" ? (JSON.parse(drained.text) as Record<string, unknown>) : null;
    } catch {
      md = null;
    }
    const outcome = md && typeof md === "object" && md.kind === "buffered-drained" && md.staged_sha256 === s.sha256 ? md.outcome : undefined;
    if (outcome === "pre_reset") {
      b.excluded.push(...items.flat());
      continue;
    }
    if (outcome !== "drained") {
      b.invalid.push(key);
      continue;
    }
    const fence = await fenceOf(tenant);
    const kept = marker!.kept as number[][];
    items.forEach((lines, gi) => {
      const keep = new Set(kept[gi] ?? []);
      lines.forEach((item, j) => {
        const meta = s.groups[gi]!.meta?.[j];
        const session = meta ? meta[3] : item.sessionId;
        const deleted = fence === null || (session !== null && isDeletedForAdmission(fence, origin, meta ? meta[2] : item.source, session));
        (keep.has(j) && !deleted ? b.allowed : b.excluded).push(item);
      });
    });
  }
  return b;
}

/** Every digest-bearing index entry of the backlog's users and origins, with the session it names (owner stubs). */
async function collectIndex(opts: VerifyOptions, deps: VerifyDeps, b: Backlog) {
  const io = s3LedgerObjects(opts.store, opts.keyRoot);
  const out: { tenant: LedgerTenant; origin: string; recordId: string; entry: IndexEntry; session: Session | null }[] = [];
  const owners = new Map<string, Map<string, Session>>();
  for (const { tenant, origin } of b.origins.values()) {
    const keys = io.keysFor(tenant);
    const fence = b.fences.get(userKey(tenant)) ?? (await (async () => {
      const read = await io.readFence(keys.fence);
      return read.state === "present" ? read.fence : null;
    })());
    if (fence === null) continue; // the user is erased: no winner is owed
    if (!owners.has(userKey(tenant))) {
      const m = new Map<string, Session>();
      for (const d of await io.listOwners(keys)) m.set(JSON.stringify([d.originId, sessionKeyHash(d.source, d.sessionId)]), { tenant, origin: d.originId, source: d.source, sessionId: d.sessionId });
      owners.set(userKey(tenant), m);
    }
    const byHash = owners.get(userKey(tenant))!;
    for (const s of b.sessions.values()) if (userKey(s.tenant) === userKey(tenant)) byHash.set(JSON.stringify([s.origin, sessionKeyHash(s.source, s.sessionId)]), s);
    for (const shardKey of await deps.list(keys.revisionsRoot)) {
      if (!shardKey.endsWith(".json")) continue;
      const doc = (await io.readIndex(shardKey))?.doc;
      if (!doc || doc.originId !== origin) continue;
      for (const [recordId, e] of Object.entries(doc.records)) {
        if (e[2] === null || entryForgotten(fence, origin, e)) continue; // revision-only (seeded) or forgotten (erased)
        const session = e[3] === null
          ? { tenant, origin, source: NO_SESSION_HARNESS, sessionId: NO_SESSION_ID }
          : byHash.get(JSON.stringify([origin, e[3]])) ?? null;
        out.push({ tenant, origin, recordId, entry: e, session });
      }
    }
  }
  return out;
}

async function readSessions(opts: VerifyOptions, sessions: Iterable<[string, Session]>, onError: (sk: string) => void): Promise<Map<string, CanonicalLine[] | null>> {
  const out = new Map<string, CanonicalLine[] | null>();
  for (const [sk, s] of sessions) {
    if (out.has(sk)) continue;
    try {
      out.set(sk, await readCanonicalSession(opts.store, {
        keyRoot: opts.keyRoot, orgId: s.tenant.orgId, workspaceId: s.tenant.workspaceId, machineId: liveMachineId(s.tenant.workspaceId, s.origin, s.tenant.userId),
        source: s.source, sessionId: s.sessionId,
      }, opts.decompress));
    } catch {
      onError(sk);
    }
  }
  return out;
}

/**
 * The org's canonical lines before a drain: every session of its staged backlog and every session its index names.
 * Take it right before the drain and pass it to verifyDrainMaterialization as `baseline`. Throws when any session is
 * unreadable (a baseline must be whole).
 */
export async function snapshotCanonical(opts: Omit<VerifyOptions, "baseline">, deps: VerifyDeps): Promise<CanonicalSnapshot> {
  const b = await collectBacklog(opts, deps);
  const idx = await collectIndex(opts, deps, b);
  const sessions = new Map(b.sessions);
  for (const i of idx) if (i.session) sessions.set(sessionKey(i.session.tenant, i.session.origin, i.session.source, i.session.sessionId), i.session);
  const read = await readSessions(opts, sessions, () => { throw new CanonicalReadError("chunk_invalid"); });
  return { sessions: new Map([...read].map(([sk, lines]) => [sk, (lines ?? []).map((l) => l.text)])) };
}

/**
 * The record-by-record proof (see the header). Expected records, index winners, excluded lines and every canonical
 * line of the touched sessions are each judged; `ok` only when no failure list has an entry.
 */
export async function verifyDrainMaterialization(opts: VerifyOptions, deps: VerifyDeps): Promise<VerifyResult> {
  const result: VerifyResult = {
    ok: false, exact: 0, superseded_later: 0, index_winners_canonical: 0, excluded_absent: 0, baseline_preserved: 0,
    counts: { export_missing: 0, missing: 0, mismatch: 0, index_missing: 0, unrecoverable: 0, unexpected: 0, erased_present: 0, baseline_lost: 0, undrained: 0, marker_invalid: 0, no_verdict: 0, unreadable: 0 },
    failures: [],
  };
  const fail = (kind: VerifyFailure, r: string) => {
    result.counts[kind]++;
    result.failures.push({ kind, ref: r });
  };
  // Without the org's accepted-stage completion the drain writes nothing of it, and no verdict is final: no proof.
  const b = await collectBacklog(opts, deps);
  if (!b.exportComplete) fail("export_missing", ref(opts.orgId));
  for (const k of b.undrained) fail("undrained", ref(k));
  for (const k of b.invalid) fail("marker_invalid", ref(k));
  for (const k of b.noVerdict) fail("no_verdict", ref(k));
  let idx: Awaited<ReturnType<typeof collectIndex>> = [];
  try {
    idx = await collectIndex(opts, deps, b);
  } catch {
    fail("unreadable", ref(JSON.stringify(["index", opts.orgId])));
  }

  // expected: per record, the highest allowed revision; every allowed form per session (any revision).
  const expected = new Map<string, Line>();
  const allowedForms = new Map<string, Set<string>>();
  const allowedAt = new Set<string>();
  for (const l of b.allowed) {
    const rk = recordKey(l.tenant, l.origin, l.recordId);
    const prev = expected.get(rk);
    if (!prev || l.revision > prev.revision) expected.set(rk, l);
    allowedAt.add(JSON.stringify([rk, l.revision]));
    const sk = sessionKey(l.tenant, l.origin, l.source, l.sessionId);
    const set = allowedForms.get(sk) ?? new Set<string>();
    set.add(canonicalFormOf(l.line));
    allowedForms.set(sk, set);
  }
  const sessions = new Map(b.sessions);
  for (const i of idx) if (i.session) sessions.set(sessionKey(i.session.tenant, i.session.origin, i.session.source, i.session.sessionId), i.session);
  for (const sk of opts.baseline?.sessions.keys() ?? []) if (!sessions.has(sk)) {
    const [orgId, workspaceId, userId, origin, source, sessionId] = JSON.parse(sk) as string[];
    sessions.set(sk, { tenant: { orgId: orgId!, workspaceId: workspaceId!, userId: userId! }, origin: origin!, source: source!, sessionId: sessionId! });
  }
  const canonical = await readSessions(opts, sessions, (sk) => fail("unreadable", ref(sk)));
  const winners = new Map(idx.map((i) => [recordKey(i.tenant, i.origin, i.recordId), i]));

  for (const [rk, l] of expected) {
    const sk = sessionKey(l.tenant, l.origin, l.source, l.sessionId);
    if (!canonical.has(sk)) continue; // unreadable, already failed
    const w = winners.get(rk);
    if (!w || w.entry[0] < l.revision) {
      fail("index_missing", ref(rk));
      continue;
    }
    if (w.entry[0] > l.revision) continue; // judged as an index winner below
    const lines = (canonical.get(sk) ?? []).filter((c) => c.originId === l.origin && c.recordId === l.recordId);
    const top = lines.reduce<CanonicalLine | null>((a, c) => (!a || c.revision > a.revision ? c : a), null);
    if (!top) fail("missing", ref(rk));
    else if (top.revision !== l.revision || top.text !== canonicalFormOf(l.line) || lines.some((c) => c.revision === l.revision && c.text !== top.text)) fail("mismatch", ref(rk));
    else result.exact++;
  }

  // Every index winner the backlog does not hold at its revision: canonical must hold it, else nothing can recover it.
  for (const [rk, w] of winners) {
    const e = expected.get(rk);
    if (e && e.revision === w.entry[0]) continue; // judged above
    if (allowedAt.has(JSON.stringify([rk, w.entry[0]]))) continue; // never: expected is the highest allowed
    const sk = w.session && sessionKey(w.session.tenant, w.session.origin, w.session.source, w.session.sessionId);
    if (sk && !canonical.has(sk)) continue; // unreadable, already failed
    const held = sk ? (canonical.get(sk) ?? []).some((c) => c.originId === w.origin && c.recordId === w.recordId && c.revision === w.entry[0]) : false;
    if (!held) fail("unrecoverable", ref(rk));
    else if (e) result.superseded_later++;
    else result.index_winners_canonical++;
  }

  // Nothing the drain introduced outside the allowed set (or an index winner); nothing excluded made canonical.
  const excludedForms = new Map<string, Set<string>>();
  for (const l of b.excluded) {
    const sk = sessionKey(l.tenant, l.origin, l.source, l.sessionId);
    const set = excludedForms.get(sk) ?? new Set<string>();
    set.add(canonicalFormOf(l.line));
    excludedForms.set(sk, set);
  }
  for (const [sk, s] of sessions) {
    const lines = canonical.get(sk);
    if (lines === undefined) continue; // unreadable, already failed
    const ok = allowedForms.get(sk) ?? new Set<string>();
    const bad = excludedForms.get(sk) ?? new Set<string>();
    const before = new Set(opts.baseline?.sessions.get(sk) ?? []);
    // An erased session: every readable line is erased_present, whoever wrote it and whenever (baseline included).
    if (b.deleted.has(sk)) {
      for (const c of lines ?? []) fail("erased_present", ref(JSON.stringify([sk, c.recordId, c.revision])));
      for (const form of bad) if (!(lines ?? []).some((c) => c.text === form)) result.excluded_absent++;
      continue;
    }
    // Canonical is append-only outside an erase: every baseline line of a live session must still be readable as is.
    const now = new Set((lines ?? []).map((c) => c.text));
    for (const t of before) if (!now.has(t)) fail("baseline_lost", ref(JSON.stringify([sk, sha256Hex(t)])));
    for (const c of lines ?? []) {
      if (ok.has(c.text)) continue;
      // An excluded line is erased_present while readable, even if it was there before the drain.
      if (bad.has(c.text)) {
        fail("erased_present", ref(JSON.stringify([sk, c.recordId, c.revision])));
        continue;
      }
      const rk = recordKey(s.tenant, c.originId, c.recordId);
      const w = winners.get(rk);
      // The index's winner beyond the backlog (a later admission). At an expected revision only the expected bytes count.
      if (w && w.entry[0] === c.revision && (expected.get(rk)?.revision ?? -1) < c.revision) continue;
      if (before.has(c.text)) {
        result.baseline_preserved++;
        continue;
      }
      fail("unexpected", ref(JSON.stringify([sk, c.recordId, c.revision])));
    }
    for (const form of bad) if (!ok.has(form) && !(lines ?? []).some((c) => c.text === form)) result.excluded_absent++;
  }
  result.ok = Object.values(result.counts).every((n) => n === 0);
  return result;
}
