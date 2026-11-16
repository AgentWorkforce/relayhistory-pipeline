/**
 * The log materializer (docs/design/s3-ledger-log-commit.md, slice 3): applies a log-mode stream's entries to the
 * read-side state (the per-session manifests and chunks, the revision index, the outcome-carrying batch docs) so
 * readers, search and the read API work unchanged.
 *
 * How: each logged batch is replayed, in log order, through the UNCHANGED legacy admission core (deliverPrepared:
 * claim -> canonical writes -> commit) on a ledger WITHOUT logModeGuard. The legacy core is the only writer of
 * those objects, so log mode yields exactly the state legacy admission yields for the same inputs (the
 * equivalence suite pins it byte for byte), and every legacy property carries over: idempotency (the batch docs:
 * a re-applied batch is a replay), resume after a crash at any step (a pending claim resumes), concurrency (two
 * materializers on one stream race on the same claim tokens; the loser retries and finds a replay), and the
 * privacy rules applied at write time.
 *
 * Privacy, re-checked at materialization (never resurrect erased data):
 *   - the user's fence: the legacy claim drops records of sessions deleted since the entry, and retries while a
 *     reset is pending;
 *   - the privacy journal: a reset / re-enable completed after the upload began refuses the batch (the payload
 *     carries its journal seq); refused batches are skipped and their payloads deleted;
 *   - payloads under ledger/log/o=<origin>/p/ live only until their entry is applied: after its marker is written
 *     they are deleted. eraseLogPayloads deletes a user's unapplied payloads outright (a batch whose payload is gone
 *     is skipped as erased, never written).
 *
 * Progress: `m/<seq>.json`, written once (create-if-absent) after entry `seq` and every entry before it are
 * applied. The highest marker is the cursor. A crash before the marker re-applies the entry (idempotent).
 *
 * Streams: the registry `<keyRoot>/.ledger/log-streams/<h>.json` names every log-mode stream (written by the
 * consumer when it takes a stream and by the operator tool), so the sweeper finds streams this process never saw.
 */
import { createHash } from "node:crypto";
import { isObjectStoreError, objectKey, readAll, type ObjectStore } from "../shared/vendor.ts";
import { assertNoLaterPrivacyControl } from "./async-admission.ts";
import { deliverPrepared, type DeliverDeps } from "./deliver.ts";
import type { LedgerTenant } from "./ledger.ts";
import { logKeys, readEntries, type LogEntry, type LogKeys } from "./ledger-log.ts";
import { DeliveryError } from "./vendor/delivery/delivery-contracts.ts";
import type { PreparedDelivery } from "./vendor/delivery/prepare.ts";

const sha256 = (v: string) => createHash("sha256").update(v).digest("hex");
const enc = (v: unknown) => new TextEncoder().encode(JSON.stringify(v));

export const logStreamsRoot = (keyRoot: string) => `${keyRoot}/.ledger/log-streams/`;
export const logStreamRegistryKey = (keyRoot: string, tenant: LedgerTenant, originId: string) =>
  `${logStreamsRoot(keyRoot)}${sha256(JSON.stringify([tenant.orgId, tenant.workspaceId, tenant.userId, originId])).slice(0, 40)}.json`;

export interface LogStreamRef { tenant: LedgerTenant; originId: string }

/** Name a stream in the registry. Idempotent (create-if-absent of identical bytes). */
export async function registerLogStream(store: ObjectStore, keyRoot: string, tenant: LedgerTenant, originId: string): Promise<void> {
  const doc = { v: 1, orgId: tenant.orgId, workspaceId: tenant.workspaceId, userId: tenant.userId, originId };
  try { await store.put(objectKey(logStreamRegistryKey(keyRoot, tenant, originId)), enc(doc), { ifNoneMatch: true, contentType: "application/json" }); }
  catch (error) { if (!(isObjectStoreError(error) && error.code === "already_exists")) throw error; }
}

export async function* listLogStreams(store: ObjectStore, keyRoot: string): AsyncGenerator<LogStreamRef> {
  let cursor: string | undefined;
  do {
    const page = await store.list(objectKey(logStreamsRoot(keyRoot).slice(0, -1)), cursor);
    for (const item of page.items) {
      const key = String(item.key);
      if (!key.startsWith(logStreamsRoot(keyRoot)) || !key.endsWith(".json")) continue;
      const doc = await readJson(store, key);
      const d = doc as { v?: unknown; orgId?: unknown; workspaceId?: unknown; userId?: unknown; originId?: unknown } | undefined;
      if (!d || d.v !== 1 || typeof d.orgId !== "string" || typeof d.workspaceId !== "string" || typeof d.userId !== "string" || typeof d.originId !== "string") continue;
      yield { tenant: { orgId: d.orgId, workspaceId: d.workspaceId, userId: d.userId }, originId: d.originId };
    }
    cursor = page.nextCursor;
  } while (cursor);
}

async function readJson(store: ObjectStore, key: string): Promise<unknown | undefined> {
  try { return JSON.parse(new TextDecoder().decode(await readAll((await store.getObject(objectKey(key))).body))); }
  catch (error) { if (isObjectStoreError(error) && error.code === "not_found") return undefined; throw error; }
}

async function listKeys(store: ObjectStore, prefix: string): Promise<string[]> {
  const out: string[] = [];
  let cursor: string | undefined;
  do {
    const page = await store.list(objectKey(prefix.slice(0, -1)), cursor);
    for (const item of page.items) { const k = String(item.key); if (k.startsWith(prefix)) out.push(k); }
    cursor = page.nextCursor;
  } while (cursor);
  return out;
}

/** The highest applied entry seq of a stream, or -1. */
export async function materializedThrough(store: ObjectStore, keys: LogKeys): Promise<number> {
  let max = -1;
  for (const k of await listKeys(store, keys.markers)) {
    const m = /\/m\/(\d{20})\.json$/.exec(k);
    if (m) max = Math.max(max, Number(m[1]));
  }
  return max;
}

/**
 * Delete every payload of a user's log streams (erasure). A batch whose payload is gone is skipped as erased by the
 * materializer, never written. Returns how many were deleted.
 */
export async function eraseLogPayloads(store: ObjectStore, keyRoot: string, tenant: LedgerTenant): Promise<number> {
  let n = 0;
  for await (const s of listLogStreams(store, keyRoot)) {
    if (s.tenant.orgId !== tenant.orgId || s.tenant.workspaceId !== tenant.workspaceId || s.tenant.userId !== tenant.userId) continue;
    for (const k of await listKeys(store, logKeys(keyRoot, s.tenant, s.originId).payloads)) { await store.delete(objectKey(k)); n++; }
  }
  return n;
}

/** A logged batch's payload (ledger-log-admit.ts): the kept records, the receipt, the upload's journal seq and the
 * admission time (written as received_at, so a record's received_at is when it was accepted, not when applied). */
interface StoredPayload { v: 1; journalSeq?: number; receivedAtMs?: number; receipt: PreparedDelivery["receipt"]; records: PreparedDelivery["records"] }
const validPayload = (v: unknown): v is StoredPayload => {
  const p = v as StoredPayload | undefined;
  return !!p && p.v === 1 && !!p.receipt && typeof p.receipt === "object" && Array.isArray(p.records)
    && (p.journalSeq === undefined || Number.isSafeInteger(p.journalSeq))
    && (p.receivedAtMs === undefined || Number.isSafeInteger(p.receivedAtMs));
};

/** An unreferenced payload older than this is collected (an admission puts its payload, then appends within seconds). */
export const ORPHAN_GRACE_MS = 60 * 60_000;

export type MaterializedOutcome = "accepted" | "accepted_all_deleted" | "replay" | "replay_fast" | "accepted_staged" | `skipped:${string}`;

export interface LogMaterializerDeps {
  store: ObjectStore;
  keyRoot: string;
  /** The legacy admission deps, on a ledger WITHOUT logModeGuard (the materializer is the one legacy writer allowed). */
  deliver: DeliverDeps;
  nowMs?: () => number;
  log?: (fields: Record<string, unknown>) => void;
}

export interface MaterializeResult { applied: number; through: number; outcomes: MaterializedOutcome[] }

export class LogMaterializer {
  readonly #running = new Map<string, Promise<void>>();
  readonly #again = new Set<string>();
  constructor(readonly deps: LogMaterializerDeps) {}

  #id(tenant: LedgerTenant, originId: string) { return logKeys(this.deps.keyRoot, tenant, originId).root; }

  /** Apply every unapplied entry of one stream, in order. Throws on a retryable failure (the cursor stays put). */
  async materialize(tenant: LedgerTenant, originId: string): Promise<MaterializeResult> {
    const { store, keyRoot } = this.deps;
    const now = this.deps.nowMs ?? Date.now;
    const keys = logKeys(keyRoot, tenant, originId);
    let through = await materializedThrough(store, keys);
    const outcomes: MaterializedOutcome[] = [];
    let applied = 0;
    for await (const entry of readEntries(store, keys, Math.max(through, 0))) {
      if (entry.seq <= through) continue;
      if (entry.kind === "batches") for (const b of entry.batches) outcomes.push(await this.#applyBatch(tenant, originId, b, now));
      await this.#mark(keys, entry);
      await this.#dropPayloads(entry);
      through = entry.seq;
      applied++;
    }
    await this.#collectOrphans(keys);
    return { applied, through, outcomes };
  }

  /**
   * Payloads no entry will ever reference: a put whose append failed for good, or one the window answered. Only
   * after the stream is fully applied and only past ORPHAN_GRACE_MS by the store's own clock (lastModified against the
   * wall clock), so an in-flight admission's payload (put, not yet appended) is never collected.
   */
  async #collectOrphans(keys: LogKeys) {
    const { store } = this.deps;
    let cursor: string | undefined;
    do {
      const page = await store.list(objectKey(keys.payloads.slice(0, -1)), cursor);
      for (const item of page.items) {
        if (!String(item.key).startsWith(keys.payloads)) continue;
        if (Date.now() - item.lastModified.getTime() > ORPHAN_GRACE_MS) await store.delete(objectKey(String(item.key))).catch(() => {});
      }
      cursor = page.nextCursor;
    } while (cursor);
  }

  async #applyBatch(tenant: LedgerTenant, originId: string, b: Extract<LogEntry, { kind: "batches" }>["batches"][number], now: () => number): Promise<MaterializedOutcome> {
    const { store, keyRoot } = this.deps;
    const records: PreparedDelivery["records"] = [];
    let receipt: PreparedDelivery["receipt"] | undefined;
    let journalSeq = 0;
    let receivedAtMs: number | undefined;
    for (const ref of b.payloads) {
      const p = await readJson(store, ref.key);
      // Gone before its entry was applied: erased (eraseLogPayloads). Never written.
      if (p === undefined) return "skipped:payload_erased";
      if (!validPayload(p)) return "skipped:payload_invalid";
      receipt ??= p.receipt;
      receivedAtMs ??= p.receivedAtMs;
      journalSeq = Math.max(journalSeq, p.journalSeq ?? 0);
      records.push(...p.records);
    }
    if (!receipt) return "skipped:payload_erased";
    const prepared: PreparedDelivery = { originId, batchId: b.batchId, batchDigest: b.digest, receipt, records };
    try {
      const r = await deliverPrepared({
        ...this.deps.deliver,
        // received_at is the admission time (the append), not the apply time.
        ...(receivedAtMs !== undefined ? { now: () => new Date(receivedAtMs), target: { ...this.deps.deliver.target, now: () => new Date(receivedAtMs) } } : {}),
        // The journal re-check at write time: a reset completed after the upload began refuses the batch.
        admissionGate: () => assertNoLaterPrivacyControl(store, keyRoot, tenant, journalSeq, now()),
      }, tenant, prepared);
      return r.outcome as MaterializedOutcome;
    } catch (error) {
      // A definitive refusal (403/409/413/422/400: batch-id or revision conflict, privacy superseded): the same answer
      // forever, so the batch is skipped and the stream moves on. Anything else is a retry: the cursor stays.
      if (error instanceof DeliveryError && error.status !== 503 && error.status !== 429) {
        this.deps.log?.({ route: "ingest.log_materialize", outcome: "skipped", error_code: error.code });
        return `skipped:${error.code}`;
      }
      throw error;
    }
  }

  async #mark(keys: LogKeys, entry: LogEntry) {
    try { await this.deps.store.put(objectKey(keys.marker(entry.seq)), enc({ v: 1, seq: entry.seq }), { ifNoneMatch: true, contentType: "application/json" }); }
    // Another materializer applied it first: the same state (the legacy core is idempotent).
    catch (error) { if (!(isObjectStoreError(error) && error.code === "already_exists")) throw error; }
  }

  /** After the marker: the entry's payloads are no longer needed (the canonical state holds the records). */
  async #dropPayloads(entry: LogEntry) {
    if (entry.kind !== "batches") return;
    for (const b of entry.batches) for (const ref of b.payloads) await this.deps.store.delete(objectKey(ref.key)).catch(() => {});
  }

  /** Schedule one stream (coalesced: a stream already running runs again once). Never throws. */
  kick(tenant: LedgerTenant, originId: string): void {
    const id = this.#id(tenant, originId);
    if (this.#running.has(id)) { this.#again.add(id); return; }
    const run = (async () => {
      do {
        this.#again.delete(id);
        try { await this.materialize(tenant, originId); }
        catch (error) {
          const code = (error as { code?: unknown })?.code;
          this.deps.log?.({ route: "ingest.log_materialize", outcome: "retry", error_code: typeof code === "string" && /^[A-Za-z0-9_]{1,64}$/.test(code) ? code : "materialize_failed" });
          return;
        }
      } while (this.#again.has(id));
    })().finally(() => this.#running.delete(id));
    this.#running.set(id, run);
  }

  /** Kick every registered stream (the sweeper: streams whose kick was lost, or that another process admitted). */
  async sweep(): Promise<void> {
    for await (const s of listLogStreams(this.deps.store, this.deps.keyRoot)) this.kick(s.tenant, s.originId);
  }

  async settle(): Promise<void> {
    while (this.#running.size > 0) await Promise.allSettled([...this.#running.values()]);
  }
}
