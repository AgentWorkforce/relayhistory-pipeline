/**
 * Babysitter evidence retention on the new store (replaces relayhistory-cloud's Neon
 * `expireBabysitterEvidence`; docs/babysitter-retention.md).
 *
 * Babysitter evidence is the `session_lineage` / `turn_receipt` delivery kinds with
 * `retention_class=standard_30d` and a client `payload.expires_at_ms` (validated
 * at most 31 days ahead by the vendored prepare). After the cutover their payload
 * bytes live only in S3. The eraser key cannot read objects, so it can never take
 * one line out of a chunk: an expired line is purged only when it sits in an object
 * that holds nothing else. Two halves:
 *
 *  - WRITE (ingest Worker, `INGEST_BABYSITTER_RETENTION_ROUTING=on`, replica off),
 *    the same in every acceptance mode (inline, staged, buffered):
 *      1. register the batch's retention buckets (`ingest_babysitter_retention_register`),
 *      2. write each bucket's full Babysitter lines as ONE create-only object
 *           h=_babysitter / d=<erase date> / s=_bx<yyyymmddhh>-<sha16(source, session, reset epoch)>
 *             / raw / b=<sha32(batch digest)>.ndjson.zst
 *         (the bucket is named by the UTC hour the expiry rounds UP to). One PUT per
 *         bucket, no manifest, pointer or CAS: a 500-record batch costs at most 500
 *         PUTs, so every session keeps its own bucket and its own deletion fence.
 *      3. carry on with today's path, in which the record appears in its OWN session
 *         as a payload-free stub (same identity, revision, operation, digest metadata).
 *    So a bucket holds only Babysitter lines, and no session chunk, staged object,
 *    buffered stage or canonical generation ever holds a Babysitter payload. The
 *    SQL ordinal/owner/publication/custody checks compare SQL metadata only, so they
 *    are unchanged. The reset epoch in the key means a write after a reset always
 *    opens a new bucket.
 *  - SWEEP (`babysitterRetentionTick`, `BABYSITTER_RETENTION_SWEEP`): claims due
 *    buckets, erases every version through the eraser key, records the outcome.
 *
 * Nothing here logs a key, an id or a payload: counts and codes only.
 */
import { createHash } from "node:crypto";
import { layoutComponent } from "../shared/layout-component.ts";
import type { EraseCheckpoint, ListedVersion, PagedEraser } from "../migration/neon-s3-erasure/eraser.ts";
import { boundedPut, reconcileObligation, type Cas, type ObligationStore, type WriteInvocation } from "../migration/neon-s3-erasure/write-obligation.ts";
import type { BabysitterRetentionObject } from "./ledger.ts";
import { LedgerUnavailableError } from "./ledger.ts";
import { ledgerErrorCode, type SqlRun } from "./ledger-sql.ts";
import type { PreparedDeliveryRecord } from "./vendor/delivery/prepare.ts";
import { rawZstdFrame, scrubRecords } from "@rhc/lib/pipeline/raw-chunks.ts";
import { isObjectStoreError, objectKey } from "../shared/vendor.ts";
import { fitForScrub } from "./oversize.ts";
import { assertTenantSegment, liveLine, liveMachineId, type WriteTarget } from "./writer.ts";

export const BABYSITTER_KINDS: ReadonlySet<string> = new Set(["session_lineage", "turn_receipt"]);
export const BABYSITTER_HARNESS = "_babysitter";
const HOUR_MS = 3_600_000;
/** The bucket segment the SQL lane parses (`babysitter_retention_bucket_hour`). */
export const BUCKET_SEGMENT = /^_bx[0-9]{10}-[0-9a-f]{16}$/;
const MACHINE = /^srv_[0-9a-f]{32}$/;
/** The sweeper never starts an erase with less lease than this left. */
export const MIN_LEASE_LEFT_MS = 120_000;

const sha256Hex = (value: string) => createHash("sha256").update(value).digest("hex");
const pad = (n: number, w = 2) => String(n).padStart(w, "0");

export interface BabysitterBucket {
  eraseAfterMs: number;
  /** The record's own source and session (null: no session). */
  source: string;
  sessionId: string | null;
}

/** A bucket request's identity within one batch: (source, session, erase hour). */
export const planKey = (b: BabysitterBucket) => JSON.stringify([b.source, b.sessionId, b.eraseAfterMs]);

/**
 * The retention bucket request of one record, or null when it is not routed (other
 * kinds, tombstones, no numeric expiry). The bucket REF is issued by SQL at registration
 * (`ingest_babysitter_retention_register`), bound to the claim's fence and reset epochs.
 */
export function babysitterBucket(record: Pick<PreparedDeliveryRecord, "kind" | "operation" | "payload" | "source" | "session_id">): BabysitterBucket | null {
  if (!BABYSITTER_KINDS.has(record.kind) || record.operation !== "upsert") return null;
  const expires = record.payload?.expires_at_ms;
  if (typeof expires !== "number" || !Number.isFinite(expires) || expires <= 0) return null;
  const hour = Math.ceil(expires / HOUR_MS) * HOUR_MS;
  const d = new Date(hour);
  // Four-digit years only (prepare bounds expiry to 31 days ahead; this is defense in depth).
  if (!Number.isFinite(d.getTime()) || d.getUTCFullYear() > 9999) return null;
  const sessionId = record.session_id === null || record.session_id === "" ? null : record.session_id;
  return { eraseAfterMs: hour, source: record.source, sessionId };
}

/**
 * Whether a record is a Babysitter-kind DELETE: no payload, no object, no bucket. It becomes a
 * PERMANENT payload-free tombstone key-index row (Neon's include_deleted row), independent of
 * every payload's lifetime; only a privacy fence (disable/reset) removes it.
 */
export function isBabysitterTombstone(record: Pick<PreparedDeliveryRecord, "kind" | "operation">): boolean {
  return BABYSITTER_KINDS.has(record.kind) && record.operation === "delete";
}

/** One content-free key-index row of the registration (sessions.babysitter_evidence_index). */
export interface BabysitterIndexEntry {
  /** upsert: index into the registration's `buckets`; delete: null (a tombstone has no bucket). */
  bucket: number | null;
  originId: string;
  recordId: string;
  revision: string;
  revisionId: string;
  /** The record's content digest: the body a commit source must have accepted. */
  digest: string;
  operation: "upsert" | "delete";
  kind: string;
  /** The record's own source and session (its privacy identity). */
  source: string;
  sessionId: string | null;
  /** upsert: the 0-based line in this batch's bucket object (writeBucketObjects order). */
  line: number | null;
  expiresAtMs: number | null;
}

export interface BucketPlan {
  /** The bucket request of each routed UPSERT (by identity), absent: not routed. */
  of(record: PreparedDeliveryRecord): BabysitterBucket | undefined;
  /** The routed upserts, in batch order (their full lines go to the bucket objects). */
  routed: PreparedDeliveryRecord[];
  /** Babysitter-kind deletes: index-only tombstones (no object, the record travels unchanged). */
  tombstones: PreparedDeliveryRecord[];
  /** Distinct bucket requests, in first-seen order. */
  requests: BabysitterBucket[];
  /** The key-index rows of every routed upsert and tombstone. */
  entries: BabysitterIndexEntry[];
}

/**
 * One batch's bucket plan: one bucket per (source, session, expiry hour), and SQL adds the
 * epochs. Never collapsed across sessions, whatever the batch width: the per-session
 * deletion fence must stay immediate (the bucket objects are one PUT each).
 */
export function planBabysitterBuckets(records: readonly PreparedDeliveryRecord[], _receivedAtMs?: number): BucketPlan {
  const byRecord = new Map<PreparedDeliveryRecord, BabysitterBucket>();
  const requests = new Map<string, BabysitterBucket>();
  const order = new Map<string, number>();
  const lines = new Map<string, number>();
  const entries: BabysitterIndexEntry[] = [];
  const tombstones: PreparedDeliveryRecord[] = [];
  for (const r of records) {
    const up = babysitterBucket(r);
    const session = r.session_id === null || r.session_id === "" ? null : r.session_id;
    if (!up && isBabysitterTombstone(r)) {
      tombstones.push(r);
      entries.push({ bucket: null, originId: r.origin_id, recordId: r.record_id, revision: String(r.revision), revisionId: r.revision_id, digest: r.digest,
        operation: "delete", kind: r.kind, source: r.source, sessionId: session, line: null, expiresAtMs: null });
      continue;
    }
    if (!up) continue;
    const key = planKey(up);
    if (!requests.has(key)) { requests.set(key, up); order.set(key, order.size); }
    {
      byRecord.set(r, up);
      // The same order writeBucketObjects writes this bucket's lines in.
      const line = lines.get(key) ?? 0;
      lines.set(key, line + 1);
      entries.push({ bucket: order.get(key)!, originId: r.origin_id, recordId: r.record_id, revision: String(r.revision), revisionId: r.revision_id, digest: r.digest,
        operation: "upsert", kind: r.kind, source: r.source, sessionId: session, line, expiresAtMs: r.payload!.expires_at_ms as number });
    }
  }
  return { of: (r) => byRecord.get(r), routed: records.filter((r) => byRecord.has(r)), tombstones, requests: [...requests.values()], entries };
}

/** The refs SQL issued, by plan key. */
export type BucketRefs = ReadonlyMap<string, string>;

/** The registry response mapped by plan key (refuses a malformed or partial answer). */
export function bucketRefs(plan: BucketPlan, objects: readonly BabysitterRetentionObject[]): BucketRefs {
  const refs = new Map<string, string>();
  for (const o of objects) {
    if (!BUCKET_SEGMENT.test(o.ref)) throw new RetentionRefused("bucket_ref_invalid");
    refs.set(planKey(o), o.ref);
  }
  for (const req of plan.requests) if (!refs.has(planKey(req))) throw new RetentionRefused("bucket_ref_missing");
  return refs;
}

/** The bucket directory of a machine (ends in `/`), the eraser's list base. */
export function bucketBase(keyRoot: string, orgId: string, workspaceId: string, machineId: string): string {
  return `${keyRoot}/${orgId}/v1/ws=${layoutComponent(workspaceId)}/m=${machineId}/h=${BABYSITTER_HARNESS}/`;
}

/** The one object a batch writes into one bucket (create-only; a retry finds it). */
export function bucketObjectKey(base: string, ref: string, eraseAfterMs: number, batchDigest: string): string {
  const d = new Date(eraseAfterMs);
  return `${base}d=${pad(d.getUTCFullYear(), 4)}/${pad(d.getUTCMonth() + 1)}/${pad(d.getUTCDate())}/s=${ref}/raw/b=${sha256Hex(batchDigest).slice(0, 32)}.ndjson.zst`;
}

/**
 * One bucket line: everything the reader returns (Neon's delivery record row) and nothing
 * else. Unlike a session chunk line it carries revision_id, source and session_id.
 */
export function bucketLine(r: PreparedDeliveryRecord, receivedAt: string): string {
  const f = fitForScrub(r);
  return JSON.stringify({ kind: f.kind, origin_id: f.origin_id, record_id: f.record_id, revision_id: f.revision_id, revision: String(f.revision),
    source: f.source, session_id: f.session_id ?? null, operation: f.operation, received_at: receivedAt, payload: f.payload });
}

/**
 * Worst-case Worker subrequests for one routed delivery request: the budget proof.
 * Every S3 call is one fetch() plus up to `redirects` followed redirects (fetch follows
 * at most 20; each hop is a subrequest). The writer's own loops are counted at their
 * maxima (writer.ts): per inline session, GROUP_ATTEMPTS x (pointer resolution 2 x
 * (GET + PUT) + CAS_ATTEMPTS x (manifest GET + chunk PUT (+ conflict GET) + manifest PUT)).
 *
 *   database   1     one Hyperdrive connection per request (every ledger call rides it)
 *   replica    0     routing requires the Neon replica off
 *   buckets    B     one create-only PUT each, no retry (a failure is a 503)
 *   inline     S x GROUP_ATTEMPTS x (4 + CAS_ATTEMPTS x 4)          canonical, S <= inline bound
 *   staged     1 staged PUT + 1 queue send                          canonical, S > inline bound
 *   buffered   1 stage PUT + 1 conflict GET                         buffered acceptance
 *
 * total = 1 + queueSends + (S3 calls) x (1 + redirects). Routing is active only with an
 * explicit, proven cap (`INGEST_SUBREQUEST_CAP`); a batch over it is refused BEFORE the
 * claim (no write anywhere) with a typed 422 the client isolates locally (see deliver.ts).
 */
export const MAX_FETCH_REDIRECTS = 20;
export const POINTER_CALLS = 4;
export const CAS_CALLS = 4;

export interface BudgetInput {
  buckets: number;
  sessions: number;
  mode: "inline" | "staged" | "buffered";
  redirects: number;
}

export function worstCaseSubrequests(i: BudgetInput, groupAttempts: number, casAttempts: number): number {
  if (!Number.isSafeInteger(i.redirects) || i.redirects < 0 || i.redirects > MAX_FETCH_REDIRECTS) throw new Error("babysitter retention: redirects must be 0..20");
  const perSession = groupAttempts * (POINTER_CALLS + casAttempts * CAS_CALLS);
  const s3 = i.buckets + (i.mode === "inline" ? i.sessions * perSession : i.mode === "staged" ? 1 : 2);
  const queue = i.mode === "staged" ? 1 : 0;
  return 1 + queue + s3 * (1 + i.redirects);
}

/** Bucket PUTs in flight at once (the PUT count itself is one per bucket). */
const BUCKET_PUT_CONCURRENCY = 8;
/** One bucket PUT's enforced wall deadline (write-obligation.ts boundedPut), inside a request. */
export const BUCKET_PUT_TIMEOUT_MS = 20_000;

/**
 * The exact bytes of each bucket's create-only object, by plan key (built once, before the PUTs).
 */
export function bucketBodies(plan: BucketPlan, receivedAt: Date): Map<string, Uint8Array> {
  const stamp = receivedAt.toISOString();
  const lines = new Map<string, string[]>();
  for (const r of plan.routed) {
    const k = planKey(plan.of(r)!);
    lines.set(k, [...(lines.get(k) ?? []), bucketLine(r, stamp)]);
  }
  return new Map([...lines].map(([k, l]) => [k, rawZstdFrame(scrubRecords(new TextEncoder().encode(l.join("\n") + "\n")).scrubbed)]));
}

export type BucketPutOutcome = { ref: string; object: string; outcome: "acked"; versionId: string } | { ref: string; object: string; outcome: "refused"; versionId: null };

/**
 * Write each bucket's full lines as one scrubbed, create-only object, each PUT under an enforced
 * wall deadline. Returns every DEFINITE outcome (acked with its own version id, "null" on an
 * unversioned store; refused: an earlier attempt already holds the key) and whether any PUT's
 * outcome is unknown (timeout / error / lost response): those invocations stay dispatched on
 * their obligation and the caller fails the request after recording the definite outcomes.
 */
export async function writeBucketObjects(
  target: WriteTarget,
  batch: { orgId: string; workspaceId: string; userId: string; originId: string; batchDigest: string },
  plan: BucketPlan,
  refs: BucketRefs,
  bodies: Map<string, Uint8Array>,
  timeoutMs = BUCKET_PUT_TIMEOUT_MS,
): Promise<{ written: number; outcomes: BucketPutOutcome[]; unknown: unknown | null }> {
  assertTenantSegment(target.keyRoot, batch.orgId);
  const base = bucketBase(target.keyRoot, batch.orgId, batch.workspaceId, liveMachineId(batch.workspaceId, batch.originId, batch.userId));
  const work = plan.requests.map((b) => ({ ref: refs.get(planKey(b))!, eraseAfterMs: b.eraseAfterMs, body: bodies.get(planKey(b))! }));
  const outcomes: BucketPutOutcome[] = [];
  let written = 0, unknown: unknown | null = null, next = 0;
  await Promise.all(Array.from({ length: Math.min(BUCKET_PUT_CONCURRENCY, work.length) }, async () => {
    while (next < work.length) {
      const { ref, eraseAfterMs, body } = work[next++]!;
      if (!body) continue;
      const key = bucketObjectKey(base, ref, eraseAfterMs, batch.batchDigest);
      const object = key.slice(key.lastIndexOf("/") + 1);
      try {
        const ack = await boundedPut(() => target.store.put(objectKey(key), body, { ifNoneMatch: true, contentType: "application/zstd" }), {
          onUnknown: async () => {},                // the invocation simply stays dispatched (durable since register)
          onRefused: async () => {},
          isRefusal: (e) => isObjectStoreError(e, "already_exists") || isObjectStoreError(e, "precondition_failed"),
        }, timeoutMs);
        if (ack && ack.created === false) outcomes.push({ ref, object, outcome: "refused", versionId: null });
        else { outcomes.push({ ref, object, outcome: "acked", versionId: ack?.version?.version ?? "null" }); written += 1; }
      } catch (error) {
        // A definite refusal of THIS request (the key already holds an earlier attempt's object).
        if (isObjectStoreError(error, "already_exists") || isObjectStoreError(error, "precondition_failed")) outcomes.push({ ref, object, outcome: "refused", versionId: null });
        else unknown ??= error;
      }
    }
  }));
  return { written, outcomes, unknown };
}

/**
 * The stand-in written to the record's OWN session (the full line is in its bucket).
 * Same identity, revision, operation and digest metadata, so the SQL ordinal / owner /
 * publication / custody checks are unchanged; the payload keeps retention metadata
 * only, never a Babysitter reference field.
 */
export function babysitterStub(record: PreparedDeliveryRecord, ref: string): PreparedDeliveryRecord {
  return {
    ...record,
    payload: { retention_class: record.payload?.retention_class ?? null, expires_at_ms: record.payload?.expires_at_ms ?? null, retention_bucket: ref },
  };
}

// ---------------------------------------------------------------------------
// Sweep

export interface ClaimedObject {
  leaseToken: string;
  /** ISO time the lease ends. */
  leaseUntil: string;
  orgId: string;
  workspaceId: string;
  userId: string;
  originId: string;
  machineId: string;
  ref: string;
  reason: "expired" | "fenced";
  /** The bucket's epochs and write sequence at the claim: they bind a resume checkpoint. */
  fenceEpoch: number;
  resetEpoch: number;
  writeSeq: number;
  eraseRuns: number;
  /** The content-free checkpoint of the last bounded run whose deletes succeeded (null: from the start). */
  checkpoint: EraseCheckpoint | null;
}

export interface RetentionClaim {
  enabled: boolean;
  dryRun: boolean;
  objects: ClaimedObject[];
}

export interface BabysitterRetentionLedger {
  claim(limit: number): Promise<RetentionClaim>;
  /** The erase fence: true only while the lease is held (30 s to spare) under the CURRENT generation. */
  fence(leaseToken: string): Promise<boolean>;
  complete(leaseToken: string, outcome: "erased" | "released" | "progress", versions: number, checkpoint?: EraseCheckpoint | null): Promise<{ ok: boolean; state?: string }>;
  /**
   * The leased bucket's write obligations (sessions.babysitter_write_obligations through
   * ingest_babysitter_retention_obligation); each call commits on its own, so `stray` is durable
   * before the deletes that follow it.
   */
  obligations(leaseToken: string, request: ObligationRequest): Promise<ObligationReply>;
}

export type ObligationAction = "list" | "load" | "reserve" | "checkpoint" | "resolve" | "reschedule" | "retire";
export interface ObligationRequest { action: ObligationAction; object?: string; rev?: number; invocations?: string[]; versions?: string[]; checkpoint?: EraseCheckpoint | null }
/** One key's obligation as the SQL adapter loads it (sessions.babysitter_write_obligations / _invocations). */
export interface BucketObligation { rev: number; invocations: WriteInvocation[]; checkpoint: EraseCheckpoint | null; sweeps: number }
export interface ObligationReply { ok: boolean; rev?: number; objects?: string[]; obligation?: BucketObligation | null }

const UUID = /^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$/;
const str = (v: unknown): v is string => typeof v === "string";

/** The sweeper's ledger over SQL, as the `bulls96_session_ingest` login (same role checks as the GC). */
export function sqlBabysitterRetentionLedger(run: SqlRun, generation: string): BabysitterRetentionLedger {
  if (!/^[0-9a-f]{64}$/.test(generation)) throw new Error("babysitter retention: generation required");
  const call = async (fn: string, input: unknown): Promise<Record<string, unknown>> => {
    let rows: Record<string, unknown>[];
    try {
      ({ rows } = await run(`SELECT sessions.${fn}($1::jsonb) AS result`, [JSON.stringify(input)]));
    } catch (error) {
      throw new LedgerUnavailableError(ledgerErrorCode(error));
    }
    let result = rows.length === 1 ? rows[0]!.result : undefined;
    if (typeof result === "string") {
      try { result = JSON.parse(result); } catch { throw new LedgerUnavailableError("ledger_bad_result"); }
    }
    if (result === null || typeof result !== "object" || Array.isArray(result)) throw new LedgerUnavailableError("ledger_bad_result");
    return result as Record<string, unknown>;
  };
  return {
    async claim(limit) {
      const r = await call("ingest_babysitter_retention_claim", { generation, limit });
      if (typeof r.enabled !== "boolean" || typeof r.dryRun !== "boolean" || !Array.isArray(r.objects)) throw new LedgerUnavailableError("ledger_bad_result");
      const objects = r.objects.map((o: unknown) => {
        const x = o as Record<string, unknown>;
        if (!x || !str(x.leaseToken) || !UUID.test(x.leaseToken) || !str(x.leaseUntil) || !Number.isFinite(Date.parse(x.leaseUntil)) ||
            !str(x.orgId) || !str(x.workspaceId) || !str(x.userId) || !str(x.originId) ||
            !str(x.machineId) || !str(x.ref) || (x.reason !== "expired" && x.reason !== "fenced") ||
            ![x.fenceEpoch, x.resetEpoch, x.writeSeq, x.eraseRuns].every((n) => Number.isSafeInteger(n) && (n as number) >= 0) ||
            (x.checkpoint !== null && (typeof x.checkpoint !== "object" || Array.isArray(x.checkpoint))))
          throw new LedgerUnavailableError("ledger_bad_result");
        return x as unknown as ClaimedObject;
      });
      return { enabled: r.enabled, dryRun: r.dryRun, objects };
    },
    async fence(leaseToken) {
      const r = await call("ingest_babysitter_retention_fence", { generation, leaseToken });
      if (typeof r.ok !== "boolean") throw new LedgerUnavailableError("ledger_bad_result");
      return r.ok;
    },
    async obligations(leaseToken, q) {
      const r = await call("ingest_babysitter_retention_obligation", { generation, leaseToken, action: q.action, object: q.object ?? null, rev: q.rev ?? null,
        invocations: q.invocations ?? [], versions: q.versions ?? [], checkpoint: q.checkpoint ?? null });
      if (typeof r.ok !== "boolean") throw new LedgerUnavailableError("ledger_bad_result");
      if (q.action === "list" && r.ok && !Array.isArray(r.objects)) throw new LedgerUnavailableError("ledger_bad_result");
      if (r.ok && q.action !== "list" && q.action !== "load" && typeof r.rev !== "number") throw new LedgerUnavailableError("ledger_bad_result");
      return r as unknown as ObligationReply;
    },
    async complete(leaseToken, outcome, versions, checkpoint = null) {
      const r = await call("ingest_babysitter_retention_complete", { generation, leaseToken, outcome, versions, checkpoint });
      if (typeof r.ok !== "boolean") throw new LedgerUnavailableError("ledger_bad_result");
      return { ok: r.ok, ...(str(r.state) ? { state: r.state } : {}) };
    },
  };
}

export interface RetentionTargets {
  /** The bucket's own directory `{root}/{org}/v1/ws=/m=/h=_babysitter/d=yyyy/mm/dd/s=<bucket>/`. */
  prefix: string;
}

/**
 * The exact directory a claimed bucket may erase, rebuilt from its coordinates: its
 * date comes from the hour the ref names, so the eraser lists and deletes only this
 * bucket's own objects, never a machine-wide listing. A ref that is not a bucket
 * segment of THIS tenant's machine is refused: the sweeper never deletes a key it did
 * not derive itself.
 */
export function retentionTargets(keyRoot: string, o: ClaimedObject): RetentionTargets {
  if (!/^[a-z0-9][a-z0-9-]{0,62}$/.test(keyRoot) || !/^[A-Za-z0-9._-]{1,128}$/.test(o.orgId) || o.orgId.startsWith(".")) throw new RetentionRefused("tenant_not_a_segment");
  const machine = `srv_${sha256Hex(`${o.workspaceId}\u001f${o.originId}\u001f${o.userId}`).slice(0, 32)}`;
  if (!MACHINE.test(o.machineId) || o.machineId !== machine) throw new RetentionRefused("machine_mismatch");
  if (!BUCKET_SEGMENT.test(o.ref)) throw new RetentionRefused("bucket_ref_invalid");
  const y = o.ref.slice(3, 7), m = o.ref.slice(7, 9), d = o.ref.slice(9, 11);
  return { prefix: `${bucketBase(keyRoot, o.orgId, o.workspaceId, machine)}d=${y}/${m}/${d}/s=${o.ref}/` };
}

/** One bucket object's obligation identity (bound into its reconcile checkpoint scope). */
export function obligationIdentity(o: Pick<ClaimedObject, "orgId" | "workspaceId" | "machineId" | "ref">, object: string, generation: string): readonly unknown[] {
  return ["babysitter_write", o.orgId, o.workspaceId, o.machineId, o.ref, object, generation];
}

/**
 * The shared ObligationStore (write-obligation.ts) over the leased bucket's obligation RPC:
 * every mutation is a compare-and-set on the key row's version, each call its own statement
 * (durable before the remote deletes that follow). Babysitter has no owners: nothing is kept.
 */
function obligationStore(ledger: BabysitterRetentionLedger, leaseToken: string, key: string, object: string, identity: readonly unknown[]): ObligationStore {
  const cas = async (q: ObligationRequest): Promise<Cas> => {
    const r = await ledger.obligations(leaseToken, { ...q, object });
    return r.ok && typeof r.rev === "number" ? { ok: true, rev: r.rev } : { ok: false };
  };
  return {
    async load() {
      const r = await ledger.obligations(leaseToken, { action: "load", object });
      if (!r.ok) throw new RetentionRefused("fence_refused");
      if (!r.obligation) return null;
      return { key, identity, rev: r.obligation.rev, invocations: r.obligation.invocations.map((i) => ({ id: i.id, outcome: i.outcome, ...(typeof i.versionId === "string" ? { versionId: i.versionId } : {}) })),
        ownedVersions: [], ownersWithoutVersion: 0, checkpoint: r.obligation.checkpoint };
    },
    reserve: (rev, versionIds) => cas({ action: "reserve", rev, versions: [...versionIds] }),
    saveCheckpoint: (rev, checkpoint) => cas({ action: "checkpoint", rev, checkpoint }),
    resolve: (rev, ids) => cas({ action: "resolve", rev, invocations: [...ids] }),
    reschedule: (rev) => cas({ action: "reschedule", rev }),
    retire: (rev) => cas({ action: "retire", rev }),
  };
}

/** The resume-checkpoint scope: operation, tenant, bucket, generation, epochs and write_seq. */
export function eraseScope(o: Pick<ClaimedObject, "orgId" | "workspaceId" | "machineId" | "ref" | "fenceEpoch" | "resetEpoch" | "writeSeq">, generation: string): string {
  return "babysitter_bucket_erase:" + sha256Hex(JSON.stringify(["babysitter_bucket_erase", o.orgId, o.workspaceId, o.machineId, o.ref, generation, o.fenceEpoch, o.resetEpoch, o.writeSeq]));
}

export class RetentionRefused extends Error {
  constructor(readonly code: string) {
    super(`babysitter retention refused: ${code}`);
    this.name = "RetentionRefused";
  }
}

export type SweepMode = "off" | "dry_run" | "on";

export function sweepMode(raw: string | undefined): SweepMode {
  const v = (raw ?? "").trim().toLowerCase();
  if (v === "" || v === "off") return "off";
  if (v === "dry_run" || v === "on") return v;
  throw new Error("BABYSITTER_RETENTION_SWEEP must be off, dry_run or on");
}

/** INGEST_BABYSITTER_RETENTION_ROUTING: on only for the value `on` (any case, trimmed). */
export function routingFlag(raw: string | undefined): boolean {
  return (raw ?? "").trim().toLowerCase() === "on";
}

export interface TickDeps {
  ledger: BabysitterRetentionLedger;
  eraser: PagedEraser;
  keyRoot: string;
  /** The compiled schema generation the ledger runs under (binds every resume checkpoint). */
  generation: string;
  mode: SweepMode;
  /** Buckets per claim (1..200). */
  limit?: number;
  /** Wall-clock budget for one tick; leases left unprocessed are released. */
  budgetMs?: number;
  /** Least lease left to start an erase (default MIN_LEASE_LEFT_MS); half of it is the erase's margin. */
  minLeaseLeftMs?: number;
  /** Per-run erase bounds below the eraser's hard ceilings (10k versions / 100 pages); tests only. */
  eraseBudget?: { versions?: number; pages?: number };
  clock?: () => number;
}

export interface TickResult {
  mode: SweepMode;
  enabled: boolean;
  dry_run: boolean;
  claimed: number;
  erased: number;
  erased_versions: number;
  released: number;
  /** Bounded runs that persisted a checkpoint (or a restart) and stay pending: never erased. */
  progressed: number;
  raced: number;
  failed: number;
  refused: number;
  lease_lost: number;
  codes: Record<string, number>;
}

/**
 * One bounded sweep: claim, erase every version of each due bucket, record it.
 * `dry_run` (the flag, or the DB setting) never calls the eraser: it releases
 * each lease, which the audit records. An erase never starts with less than
 * MIN_LEASE_LEFT_MS of lease left (registration is refused only while the lease
 * holds). Idempotent: an erase deletes by version id, so a repeated or resumed
 * bucket erases nothing more.
 */
export async function babysitterRetentionTick(deps: TickDeps): Promise<TickResult> {
  const clock = deps.clock ?? Date.now;
  const deadline = clock() + (deps.budgetMs ?? 45_000);
  const result: TickResult = { mode: deps.mode, enabled: false, dry_run: deps.mode !== "on", claimed: 0, erased: 0, erased_versions: 0, released: 0, progressed: 0, raced: 0, failed: 0, refused: 0, lease_lost: 0, codes: {} };
  if (deps.mode === "off") return result;
  const limit = deps.limit ?? 50;
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > 200) throw new Error("babysitter retention: limit must be 1..200");
  const claim = await deps.ledger.claim(limit);
  result.enabled = claim.enabled;
  const dryRun = deps.mode !== "on" || claim.dryRun;
  result.dry_run = dryRun;
  result.claimed = claim.objects.length;
  const count = (code: string) => (result.codes[code] = (result.codes[code] ?? 0) + 1);
  const release = async (o: ClaimedObject, versions: number) => {
    const r = await deps.ledger.complete(o.leaseToken, "released", versions);
    if (r.ok) result.released += 1;
    else result.lease_lost += 1;
  };
  for (const o of claim.objects) {
    try {
      if (clock() > deadline) { count("budget"); await release(o, 0); continue; }
      let targets: RetentionTargets;
      try {
        targets = retentionTargets(deps.keyRoot, o);
      } catch (error) {
        result.refused += 1;
        count(error instanceof RetentionRefused ? error.code : "refused");
        await release(o, 0);
        continue;
      }
      if (dryRun) { await release(o, 0); continue; }
      // A write may be registered again once the lease ends: never erase near its end.
      const minLease = deps.minLeaseLeftMs ?? MIN_LEASE_LEFT_MS;
      if (Date.parse(o.leaseUntil) - clock() < minLease) { count("lease_short"); await release(o, 0); continue; }
      // A durable, resumable, strict erase of this bucket's own directory (never machine-wide),
      // in bounded runs (eraser.ts eraseStep: <= 10k versions / 100 pages per run, cut at the
      // lease end less half the margin). authorize() is the SQL fence (lease held, CURRENT
      // generation) and runs before EVERY page's deletes and before the proof. Registration is
      // refused while the lease holds, so no PUT lands mid-run. The checkpoint is bound to this
      // bucket, the generation, its epochs and write_seq: anything else restarts from the top.
      const deadlineAt = Date.parse(o.leaseUntil) - minLease / 2;
      const scope = eraseScope(o, deps.generation);
      const authorize = async () => { if (!(await deps.ledger.fence(o.leaseToken))) throw new RetentionRefused("fence_refused"); };
      // The bucket's durable write obligations: one per object key, with every PUT invocation
      // dispatched at it (recorded by register BEFORE the PUT; no ETag: content is not identity).
      const listed = await deps.ledger.obligations(o.leaseToken, { action: "list" });
      if (!listed.ok) { count("fence_refused"); result.lease_lost += 1; continue; }
      const raw = `${targets.prefix}raw/`;
      const objects = listed.objects ?? [];
      const stores = new Map(objects.map((object) => [object, obligationStore(deps.ledger, o.leaseToken, `${raw}${object}`, object, obligationIdentity(o, object, deps.generation))]));
      // The bucket-wide erase RESERVES every doomed version of a key with an obligation (CAS under
      // the key row's lock, the shared contract) before its delete. No listed version resolves a
      // dispatched invocation: that needs its writer's own outcome (write-obligation.ts v3).
      const beforeDelete = async (doomed: readonly ListedVersion[]) => {
        const byKey = new Map<string, string[]>();
        for (const v of doomed) if (v.key.startsWith(raw) && stores.has(v.key.slice(raw.length))) byKey.set(v.key, [...(byKey.get(v.key) ?? []), v.versionId]);
        for (const [key, versions] of byKey) {
          const store = stores.get(key.slice(raw.length))!;
          const state = await store.load();
          if (!state) continue;
          const r = await store.reserve(state.rev, versions);
          if (!r.ok) throw new RetentionRefused("obligation_changed");
        }
      };
      let step;
      try {
        step = await withDeadline(deps.eraser.eraseStep(targets.prefix, { scope, checkpoint: o.checkpoint, authorize, beforeDelete, budget: { ...deps.eraseBudget, deadlineAt }, now: clock }), deadlineAt - clock());
      } catch (error) {
        if ((error as { code?: string }).code !== "erasure_checkpoint_scope") throw error;
        count("checkpoint_restart");
        step = await withDeadline(deps.eraser.eraseStep(targets.prefix, { scope, checkpoint: null, authorize, beforeDelete, budget: { ...deps.eraseBudget, deadlineAt }, now: clock }), deadlineAt - clock());
      }
      if (step.status === "incomplete") {
        count(`incomplete_${step.reason}`);
        const done = await deps.ledger.complete(o.leaseToken, "progress", step.deleted, step.checkpoint);
        if (done.ok) result.progressed += 1; else result.lease_lost += 1;
        result.erased_versions += step.deleted;
        continue;
      }
      // Never erased on a traversal alone: a FRESH strict zero inventory under the fence.
      const proof = await withDeadline(deps.eraser.verifyErased(targets.prefix, { authorize, budget: { deadlineAt }, now: clock }), deadlineAt - clock());
      if (proof.status !== "erased") {
        count(`proof_${proof.reason}`);
        // Versions behind the cursor (or an unfinished proof): restart the traversal next lease.
        const done = await deps.ledger.complete(o.leaseToken, "progress", step.deleted, null);
        if (done.ok) result.progressed += 1; else result.lease_lost += 1;
        result.erased_versions += step.deleted;
        continue;
      }
      // Zero versions now is not "no PUT can still commit": every invocation at every key must
      // be resolved on its OWN terminal evidence (write-obligation.ts). A dispatched invocation
      // with nothing attributed stays UNRESOLVED (nonterminal) and the bucket stays tracked,
      // re-swept on the bounded backoff; a late commit is attributed and erased then.
      let open = 0;
      for (const store of stores.values()) {
        const verdict = await withDeadline(reconcileObligation(deps.eraser, store, { budget: { ...deps.eraseBudget, deadlineAt }, now: clock }), deadlineAt - clock());
        if (verdict !== "retired" && verdict !== "gone") { open += 1; count(`obligation_${verdict}`); }
      }
      if (open > 0) {
        const held = await deps.ledger.complete(o.leaseToken, "progress", step.deleted, null);
        if (held.ok) result.progressed += 1; else result.lease_lost += 1;
        result.erased_versions += step.deleted;
        continue;
      }
      const done = await deps.ledger.complete(o.leaseToken, "erased", step.deleted);
      if (!done.ok) result.lease_lost += 1;
      else if (done.state === "erased") { result.erased += 1; result.erased_versions += step.deleted; }
      else { result.raced += 1; result.erased_versions += step.deleted; }
    } catch (error) {
      if (error instanceof RetentionRefused && error.code === "fence_refused") { count("fence_refused"); result.lease_lost += 1; continue; }
      result.failed += 1;
      count(error instanceof LedgerUnavailableError ? error.code : codeOf(error));
      // The lease expires by itself (5 min) if this release fails too.
      await release(o, 0).catch(() => {});
    }
  }
  return result;
}

/** The erase or a `lease_deadline` refusal, whichever comes first. */
async function withDeadline<T>(work: Promise<T>, ms: number): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const deadline = new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new RetentionRefused("lease_deadline")), Math.max(0, ms)); });
  try { return await Promise.race([work, deadline]); } finally { clearTimeout(timer); }
}

function codeOf(error: unknown): string {
  const code = (error as { code?: unknown })?.code;
  if (typeof code === "string" && /^[A-Za-z0-9_]{1,64}$/.test(code)) return code;
  const name = (error as { name?: unknown })?.name;
  return typeof name === "string" && /^[A-Za-z]{1,64}$/.test(name) ? name : "Error";
}
