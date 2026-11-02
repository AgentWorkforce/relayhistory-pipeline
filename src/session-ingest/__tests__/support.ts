import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import type { ObjectStore } from "../../shared/vendor.ts";
import type { ClaimInput, ClaimResult, CommitInput, Ledger, LedgerTenant, SlotRequest, SlotResult } from "../ledger.ts";
import { deliveryAccount } from "../vendor/delivery/prepare.ts";
import { DELIVERY_MAPPING_VERSION, type HistoryExportBatch, type HistoryExportRecord } from "../vendor/delivery/delivery-contracts.ts";

export const TENANT = { orgId: "org-a", workspaceId: "ws-a", userId: "user-1" };

/** A scratch fs store under $TMPDIR (the caller's own scratch, never /tmp by default). */
export function scratchStore(): { store: ObjectStore; done: () => void } {
  const base = process.env.TMPDIR ?? join(process.env.HOME ?? ".", ".agent-tmp", "tmp");
  const root = mkdtempSync(join(base, "ingest-"));
  return { store: createFsStore({ root }) as unknown as ObjectStore, done: () => rmSync(root, { recursive: true, force: true }) };
}

export function record(over: Partial<HistoryExportRecord> & { record_id: string }): HistoryExportRecord {
  return {
    schema_version: 1,
    origin_id: "origin-1",
    revision_id: `${over.record_id}:r${over.revision ?? 1}`,
    revision: 1,
    kind: "session_event",
    source: "claude",
    session_id: "sess-1",
    operation: "upsert",
    payload: { text: "hello", ts_ms: 1_761_955_200_000 },
    ...over,
  };
}

export async function batch(records: HistoryExportRecord[], over: Partial<HistoryExportBatch> = {}): Promise<HistoryExportBatch> {
  return {
    schema_version: 1,
    origin_id: "origin-1",
    batch_id: "batch-1",
    job_id: "job-1",
    generation: 1,
    destination_id: "dest-1",
    instance_id: "inst-1",
    account_id: await deliveryAccount(TENANT),
    mapping_version: DELIVERY_MAPPING_VERSION,
    records,
    ...over,
  };
}

/**
 * The ledger's batch semantics in memory (the SQL is proven in
 * ledger.pglite.test.ts). `script` overrides the next claim's answer.
 */
export class MemoryLedger implements Ledger {
  batches = new Map<string, { digest: string; receiptId: string; state: "pending" | "committed" }>();
  deleted = new Set<string>();
  resets = 0;
  claims: ClaimInput[] = [];
  commits: CommitInput[] = [];
  script: ClaimResult[] = [];
  failCommit = 0;
  /** Stored revision per record id (the #250 replay fast path). */
  revisions = new Map<string, number>();
  slots = new Map<string, Set<string>>();
  async claim(input: ClaimInput): Promise<ClaimResult> {
    this.claims.push(input);
    const scripted = this.script.shift();
    if (scripted) return scripted;
    const key = JSON.stringify([input.tenant.orgId, input.tenant.workspaceId, input.originId, input.batchId]);
    const found = this.batches.get(key);
    const deleted = input.sessions.filter(([s, id]) => this.deleted.has(JSON.stringify([s, id])));
    if (found) {
      if (found.digest !== input.batchDigest) return { kind: "batch_conflict", currentDigest: found.digest };
      if (found.state === "committed") return { kind: "replay", receiptId: found.receiptId };
    }
    const isDeleted = (r: ClaimInput["records"][number]) => r.sessionId !== null && this.deleted.has(JSON.stringify([r.source, r.sessionId]));
    const stale: number[] = [];
    input.records.forEach((r, i) => {
      if (!isDeleted(r) && (this.revisions.get(r.recordId) ?? 0) >= r.revision) stale.push(i);
    });
    const writes = input.records.filter((r, i) => !isDeleted(r) && !stale.includes(i));
    if (writes.length === 0) {
      this.batches.set(key, { digest: input.batchDigest, receiptId: input.receiptId, state: "committed" });
      return { kind: "replay_fast", deleted, stale, resets: this.resets };
    }
    if (found) return { kind: "resume", deleted, stale, resets: this.resets };
    this.batches.set(key, { digest: input.batchDigest, receiptId: input.receiptId, state: "pending" });
    return { kind: "claimed", deleted, stale, resets: this.resets };
  }
  async acquireSlot(r: SlotRequest): Promise<SlotResult> {
    const live = this.slots.get(r.tenant.userId) ?? new Set();
    if (live.size >= r.maxPerUser) return { ok: false, retryAfterSeconds: 5 };
    live.add(r.holder);
    this.slots.set(r.tenant.userId, live);
    return { ok: true };
  }
  async releaseSlot(t: LedgerTenant, holder: string) {
    this.slots.get(t.userId)?.delete(holder);
  }
  async commit(input: CommitInput) {
    this.commits.push(input);
    if (this.failCommit > 0) {
      this.failCommit--;
      throw Object.assign(new Error("connection terminated"), { code: "ECONNRESET" });
    }
    const key = JSON.stringify([input.tenant.orgId, input.tenant.workspaceId, input.originId, input.batchId]);
    const found = this.batches.get(key)!;
    found.state = "committed";
    for (const r of input.records) this.revisions.set(r.recordId, Math.max(this.revisions.get(r.recordId) ?? 0, r.revision));
    return { rearmed: 0 };
  }
}
