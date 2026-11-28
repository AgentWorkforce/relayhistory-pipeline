/**
 * FULL-to-drain materialization (decision 0005, "The buffered backlog"; wave-1 blocker).
 *
 * The FULL's revisions section (upload-fence-backfill.ts) puts every owned lazy native slot into the S3 revision index
 * with the slot's `current_digest`. For a buffered org those slots name lines that exist only in the staged backlog:
 * the publisher that would have written them never ran. An index entry is therefore NOT proof of canonical bytes, and
 * the drain (buffered-drain.ts step 4) materializes an equal-revision, equal-digest line unless the session's canonical
 * manifest already holds that exact record (`canonicalHolds`).
 *
 * `verifyDrainMaterialization` proves the result, record by record, from S3 alone (no aggregate check):
 *
 *   allowed    every line the drain may have written: a line SQL kept (the stage's accepted v2 sidecar, live, not
 *              superseded) of a stage marked `drained`, in a session the user's CURRENT S3 fence does not delete.
 *   expected   per (origin, record id), the highest allowed revision (supersession: a newer revision wins).
 *   excluded   every other line of the org's staged backlog (not kept, not live, superseded, unacknowledged, pre-reset,
 *              or in a deleted session): erased or never published. None may be canonical unless the same
 *              (record, revision) is also allowed.
 *
 * For each expected record: the revision index must hold it (or a higher revision a later admission wrote), and the
 * session's canonical fold must be exactly the expected line (the stored, scrubbed chunk line, byte-equal), unless the
 * index names a higher revision. Every canonical line of a backlog session must be allowed or the index's current
 * revision (a later live admission); an excluded line found canonical is `erased_present`. Any stage of the org that
 * is accepted and live but not marked is `undrained`. The run passes only when every list is empty.
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
import { isDeletedForAdmission, s3LedgerObjects, type FenceDoc } from "./ledger-s3.ts";
import type { LedgerTenant } from "./ledger.ts";
import { liveMachineId } from "./writer.ts";
// A cycle with buffered-drain.ts (which imports canonicalHolds): only called at run time, never at module init.
import { acceptedCompleteKey, parseAcceptedComplete } from "./buffered-drain.ts";

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

export type VerifyFailure = "export_missing" | "missing" | "mismatch" | "index_missing" | "unexpected" | "erased_present" | "undrained" | "unreadable";

export interface VerifyResult {
  ok: boolean;
  /** Expected records whose canonical fold is exactly the expected line. */
  exact: number;
  /** Expected records a later admission superseded (the index holds a higher revision): not compared. */
  superseded_later: number;
  /** Staged lines excluded from the expected set (erased, not kept, not live, pre-reset) and confirmed absent. */
  excluded_absent: number;
  counts: Record<VerifyFailure, number>;
  /** Per record (or per stage for `undrained`): sha256 of its identity tuple. Never an id or content. */
  failures: { kind: VerifyFailure; ref: string }[];
}

export interface VerifyOptions {
  store: ObjectStore;
  keyRoot: string;
  orgId: string;
  decompress: (bytes: Uint8Array) => Uint8Array;
}

/** A staged object as the verifier needs it (buffered-drain.ts readStagedObject's live form). */
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

type Line = { tenant: LedgerTenant; origin: string; source: string; sessionId: string; line: string; recordId: string; revision: number };
const sessionKey = (t: LedgerTenant, origin: string, source: string, sessionId: string) => JSON.stringify([t.orgId, t.workspaceId, t.userId, origin, source, sessionId]);
const recordKey = (t: LedgerTenant, origin: string, recordId: string) => JSON.stringify([t.orgId, t.workspaceId, t.userId, origin, recordId]);
const ref = (s: string) => sha256Hex(s);

export async function verifyDrainMaterialization(opts: VerifyOptions, deps: VerifyDeps): Promise<VerifyResult> {
  const { store, keyRoot, orgId } = opts;
  const io = s3LedgerObjects(store, keyRoot);
  const result: VerifyResult = {
    ok: false, exact: 0, superseded_later: 0, excluded_absent: 0,
    counts: { export_missing: 0, missing: 0, mismatch: 0, index_missing: 0, unexpected: 0, erased_present: 0, undrained: 0, unreadable: 0 },
    failures: [],
  };
  const fail = (kind: VerifyFailure, r: string) => {
    result.counts[kind]++;
    result.failures.push({ kind, ref: r });
  };
  // Without the org's accepted-stage completion the drain writes nothing of it, and no verdict is final: no proof.
  const complete = await getText(store, acceptedCompleteKey(keyRoot, orgId), 64 * 1024);
  if (complete === null || parseAcceptedComplete(complete) === null) fail("export_missing", ref(orgId));
  const prefix = `${keyRoot}/${orgId}/buffered-batches/v1`;
  const keys = await deps.list(prefix);
  const all = new Set(keys);
  const staged = keys.filter((k) => k.endsWith(".staged.ndjson.zst") && deps.parseStagedKey(k)?.orgId === orgId);

  const fences = new Map<string, FenceDoc | null>();
  const fenceOf = async (t: LedgerTenant): Promise<FenceDoc | null> => {
    const k = JSON.stringify([t.orgId, t.workspaceId, t.userId]);
    if (!fences.has(k)) {
      const read = await io.readFence(io.keysFor(t).fence);
      fences.set(k, read.state === "present" ? read.fence : null);
    }
    return fences.get(k)!;
  };

  const allowed: Line[] = [];
  const excluded: Line[] = [];
  for (const key of staged) {
    const s = await deps.readStaged(key);
    if (!s) continue; // never drainable: nothing of it can be canonical through the drain (excluded lines unknown)
    const tenant: LedgerTenant = { orgId: s.header.org_id, workspaceId: s.header.workspace_id, userId: s.header.user_id };
    const origin = s.header.origin_id;
    const lines = (gi: number) => s.groups[gi]!.lines.map((line, j) => {
      const r = JSON.parse(line) as { record_id: string; revision: string };
      return { j, item: { tenant, origin, source: s.groups[gi]!.source, sessionId: s.groups[gi]!.sessionId, line, recordId: r.record_id, revision: Number(r.revision) } satisfies Line };
    });
    const allGroups = () => s.groups.flatMap((_, gi) => lines(gi).map((x) => x.item));
    const accepted = all.has(key + ".accepted.json") ? await getText(store, key + ".accepted.json", 1024 * 1024) : null;
    let marker: { stage_sha256?: string; live?: boolean; kept?: number[][] } | null = null;
    try {
      marker = accepted ? JSON.parse(accepted) : null;
    } catch {
      marker = null;
    }
    const live = !!marker && marker.live === true && marker.stage_sha256 === s.sha256 && Array.isArray(marker.kept) && !all.has(key + ".superseded.json");
    if (!live) {
      excluded.push(...allGroups());
      continue;
    }
    const drainedText = all.has(key + ".drained.json") ? await getText(store, key + ".drained.json", 64 * 1024) : null;
    let outcome: unknown;
    try {
      outcome = drainedText ? (JSON.parse(drainedText) as { outcome?: unknown }).outcome : undefined;
    } catch {
      outcome = undefined;
    }
    if (outcome === "pre_reset") {
      excluded.push(...allGroups());
      continue;
    }
    if (outcome !== "drained") {
      fail("undrained", ref(key));
      continue;
    }
    const fence = await fenceOf(tenant);
    for (let gi = 0; gi < s.groups.length; gi++) {
      const keep = new Set(marker!.kept![gi] ?? []);
      for (const { j, item } of lines(gi)) {
        const meta = s.groups[gi]!.meta?.[j];
        const session = meta ? meta[3] : item.sessionId;
        const deleted = session !== null && (fence === null || isDeletedForAdmission(fence, origin, meta ? meta[2] : item.source, session));
        (keep.has(j) && !deleted ? allowed : excluded).push(item);
      }
    }
  }

  // expected: per record, the highest allowed revision; allowed lines per session (any revision).
  const expected = new Map<string, Line>();
  const allowedBySession = new Map<string, Set<string>>();
  const sessions = new Map<string, { tenant: LedgerTenant; origin: string; source: string; sessionId: string }>();
  for (const l of allowed) {
    const rk = recordKey(l.tenant, l.origin, l.recordId);
    const prev = expected.get(rk);
    if (!prev || l.revision > prev.revision) expected.set(rk, l);
    const sk = sessionKey(l.tenant, l.origin, l.source, l.sessionId);
    sessions.set(sk, { tenant: l.tenant, origin: l.origin, source: l.source, sessionId: l.sessionId });
    const set = allowedBySession.get(sk) ?? new Set<string>();
    set.add(canonicalFormOf(l.line));
    allowedBySession.set(sk, set);
  }
  for (const l of excluded) sessions.set(sessionKey(l.tenant, l.origin, l.source, l.sessionId), { tenant: l.tenant, origin: l.origin, source: l.source, sessionId: l.sessionId });

  // canonical per session
  const canonical = new Map<string, CanonicalLine[] | null>();
  for (const [sk, s] of sessions) {
    try {
      canonical.set(sk, await readCanonicalSession(store, {
        keyRoot, orgId: s.tenant.orgId, workspaceId: s.tenant.workspaceId, machineId: liveMachineId(s.tenant.workspaceId, s.origin, s.tenant.userId),
        source: s.source, sessionId: s.sessionId,
      }, opts.decompress));
    } catch {
      fail("unreadable", ref(sk));
    }
  }

  // index: the revision each expected record is stored at
  const indexRevision = new Map<string, number | null>();
  const indexOf = async (t: LedgerTenant, origin: string, recordId: string): Promise<number | null> => {
    const rk = recordKey(t, origin, recordId);
    if (!indexRevision.has(rk)) {
      const doc = (await io.readIndex(io.keysFor(t).revisions(origin, recordId)))?.doc;
      const e = doc && Object.hasOwn(doc.records, recordId) ? doc.records[recordId] : undefined;
      indexRevision.set(rk, e ? e[0] : null);
    }
    return indexRevision.get(rk)!;
  };

  for (const [rk, l] of expected) {
    const sk = sessionKey(l.tenant, l.origin, l.source, l.sessionId);
    if (!canonical.has(sk)) continue; // unreadable, already failed
    let stored: number | null;
    try {
      stored = await indexOf(l.tenant, l.origin, l.recordId);
    } catch {
      fail("unreadable", ref(rk));
      continue;
    }
    if (stored === null || stored < l.revision) {
      fail("index_missing", ref(rk));
      continue;
    }
    if (stored > l.revision) {
      result.superseded_later++;
      continue;
    }
    const lines = (canonical.get(sk) ?? []).filter((c) => c.originId === l.origin && c.recordId === l.recordId);
    const top = lines.reduce<CanonicalLine | null>((a, c) => (!a || c.revision > a.revision ? c : a), null);
    if (!top) fail("missing", ref(rk));
    else if (top.revision !== l.revision || top.text !== canonicalFormOf(l.line) || lines.filter((c) => c.revision === l.revision).some((c) => c.text !== top.text)) fail("mismatch", ref(rk));
    else result.exact++;
  }

  // nothing outside the allowed set (or a later admission's current revision) is canonical
  const excludedForms = new Map<string, Set<string>>();
  for (const l of excluded) {
    const sk = sessionKey(l.tenant, l.origin, l.source, l.sessionId);
    const set = excludedForms.get(sk) ?? new Set<string>();
    set.add(canonicalFormOf(l.line));
    excludedForms.set(sk, set);
  }
  for (const [sk, s] of sessions) {
    const lines = canonical.get(sk);
    if (lines === undefined) continue; // unreadable, already failed
    if (lines === null) {
      result.excluded_absent += excludedForms.get(sk)?.size ?? 0;
      continue;
    }
    const ok = allowedBySession.get(sk) ?? new Set<string>();
    const bad = excludedForms.get(sk) ?? new Set<string>();
    for (const c of lines) {
      if (ok.has(c.text)) continue;
      const id = ref(JSON.stringify([sk, c.recordId, c.revision]));
      if (bad.has(c.text)) {
        fail("erased_present", id);
        continue;
      }
      let stored: number | null;
      try {
        stored = await indexOf(s.tenant, c.originId, c.recordId);
      } catch {
        fail("unreadable", id);
        continue;
      }
      // A later admission (S3 path after the marker) wrote it and the index names it.
      if (stored !== null && stored === c.revision && (expected.get(recordKey(s.tenant, c.originId, c.recordId))?.revision ?? -1) < c.revision) continue;
      fail("unexpected", id);
    }
    for (const form of bad) if (!ok.has(form) && !lines.some((c) => c.text === form)) result.excluded_absent++;
  }
  result.ok = Object.values(result.counts).every((n) => n === 0);
  return result;
}
