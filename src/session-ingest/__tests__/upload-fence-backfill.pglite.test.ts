import { afterAll, afterEach, beforeAll, describe, expect, setDefaultTimeout, test } from "bun:test";
import { createHash } from "node:crypto";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { ObjectStoreError, objectKey, readAll, type ObjectStore } from "../../shared/vendor.ts";
import { applyFenceChange, entryForgotten, fenceEpoch, isDeletedForAdmission, s3ControlMirror, s3LedgerKeys, s3LedgerObjects, sessionKeyHash, type FenceDoc } from "../ledger-s3.ts";
import type { OrderedControlInput } from "../ledger.ts";
import { backfillUploadFences, type BackfillOptions, type BackfillSql } from "../upload-fence-backfill.ts";
import { parseBackfillArgs } from "../../../scripts/upload-fence-backfill.ts";
import { applySupabaseSchema } from "./pglite-schema.ts";
import { scratchStore } from "./support.ts";

setDefaultTimeout(60_000);
const ROOT = "96bulls";
// Synthetic tenants and times only (the repo is public).
const A = { orgId: "org-a", workspaceId: "ws-a", userId: "user-1" };
const B = { orgId: "org-a", workspaceId: "ws-a", userId: "user-2" };
const C = { orgId: "org-b", workspaceId: "ws-b", userId: "user-3" };
type T = typeof A;
const dec = new TextDecoder();
const enc = new TextEncoder();
const hex = (c: string) => c.repeat(64);

let db: PGlite;
beforeAll(async () => {
  db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  // upload_deletions / upload_resets / ingest_batches / ingest_record_revisions (022000), ingest_session_ownership
  // (030000), the nullable seed-proof revision columns (040000) and ingest_lazy_native_slots (055000). No later
  // migration alters their columns.
  await applySupabaseSchema(db, { until: "20261104055000_lazy_current_custody.sql" });
});
afterAll(async () => { await db.close(); });

let cleanup: (() => void)[] = [];
afterEach(async () => {
  cleanup.forEach((c) => c());
  cleanup = [];
  await db.exec(`DELETE FROM sessions.upload_deletions; DELETE FROM sessions.upload_resets; DELETE FROM sessions.ingest_session_ownership;
    DELETE FROM sessions.ingest_record_revisions; DELETE FROM sessions.ingest_batches; DELETE FROM sessions.ingest_lazy_native_slots;
    DELETE FROM sessions.ingest_seed_fence_baselines;`);
});
function scratch(): ObjectStore {
  const s = scratchStore();
  cleanup.push(s.done);
  return s.store;
}

const q = (text: string, params: unknown[]) => db.query(text, params);
const del = (t: T, origin: string, source: string, session: string) =>
  q("INSERT INTO sessions.upload_deletions (org_id, workspace_id, user_id, origin_id, source, session_id) VALUES ($1,$2,$3,$4,$5,$6)", [t.orgId, t.workspaceId, t.userId, origin, source, session]);
const reset = (t: T, requestId: string, at: string, deleted = 0) =>
  q("INSERT INTO sessions.upload_resets (org_id, workspace_id, user_id, request_id, reset_at, deleted) VALUES ($1,$2,$3,$4,$5::timestamptz,$6)", [t.orgId, t.workspaceId, t.userId, requestId, at, deleted]);
const own = (t: T, origin: string, source: string, session: string) =>
  q("INSERT INTO sessions.ingest_session_ownership (org_id, workspace_id, user_id, origin_id, source, session_id) VALUES ($1,$2,$3,$4,$5,$6)", [t.orgId, t.workspaceId, t.userId, origin, source, session]);
const rev = (t: T, origin: string, record: string, revision: number, source: string, session: string | null, digest: string | null = hex("a"), revisionId: string | null = `${record}:r${revision}`) =>
  digest === null
    ? q(`INSERT INTO sessions.ingest_record_revisions (org_id, workspace_id, origin_id, record_id, revision, revision_id, digest, user_id, source, session_id, seed_proof_sha256, seed_snapshot_sha256, seed_proof_revision)
         VALUES ($1,$2,$3,$4,$5,NULL,NULL,$6,$7,$8,$9,$9,$5)`, [t.orgId, t.workspaceId, origin, record, revision, t.userId, source, session, hex("e")])
    : q("INSERT INTO sessions.ingest_record_revisions (org_id, workspace_id, origin_id, record_id, revision, revision_id, digest, user_id, source, session_id) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
        [t.orgId, t.workspaceId, origin, record, revision, revisionId, digest, t.userId, source, session]);
/** A lazy native slot; `owner` null is a cleared (or erase-tombstoned) slot. */
const slot = (t: T, origin: string, record: string, owner: string | null, source: string | null, session: string | null, revision: number | null, digest: string | null) =>
  q(`INSERT INTO sessions.ingest_lazy_native_slots (key_sha256, key_material, org_id, workspace_id, origin_id, record_id, source_identity_sha256, control_epoch,
       initialized_witness_sha256, current_owner, current_source, current_session, current_revision, current_digest, current_reset_epoch, current_fence_epoch)
     VALUES ($1, '[]'::jsonb, $2, $3, $4, $5, $6, $6, $6, $7, $8, $9, $10, $11, $12, $12)`,
    [createHash("sha256").update(record).digest("hex"), t.orgId, t.workspaceId, origin, record, hex("e"), owner, source, session, revision, digest, owner === null ? null : 0]);
const batch = (t: T, origin: string, id: string) =>
  q("INSERT INTO sessions.ingest_batches (org_id, workspace_id, origin_id, batch_id, user_id, digest, receipt_id, state, committed_at) VALUES ($1,$2,$3,$4,$5,$6,$7,'committed',now())",
    [t.orgId, t.workspaceId, origin, id, t.userId, hex("b"), `rhr_${hex("c")}`]);

/** Every statement, so the tests can prove the SQL side never writes. */
function recordingSql(log: string[]): BackfillSql {
  return { async query(text, params) { log.push(text.trim()); return { rows: (await db.query(text, params as unknown[])).rows as Record<string, unknown>[] }; } };
}
const run = (store: ObjectStore, over: Partial<BackfillOptions> = {}, log: string[] = []) =>
  backfillUploadFences({ sql: recordingSql(log), store, keyRoot: ROOT, dryRun: false, sleep: async () => {}, ...over });

// Read back through ledger-s3's exports only.
const io = (store: ObjectStore) => s3LedgerObjects(store, ROOT);
async function fenceOf(store: ObjectStore, t: T): Promise<FenceDoc | null> {
  const read = await io(store).readFence(s3LedgerKeys(ROOT, t).fence);
  return read.state === "present" ? read.fence : null;
}
async function entryOf(store: ObjectStore, t: T, origin: string, record: string) {
  const doc = (await io(store).readIndex(s3LedgerKeys(ROOT, t).revisions(origin, record)))?.doc;
  return doc && Object.hasOwn(doc.records, record) ? doc.records[record] : undefined;
}
const ownerOf = async (store: ObjectStore, t: T, origin: string, source: string, session: string) =>
  (await io(store).readOwner(s3LedgerKeys(ROOT, t).owner(origin, source, session)))?.doc;
async function allKeys(store: ObjectStore): Promise<string[]> {
  const out: string[] = [];
  let cursor: string | undefined;
  do {
    const page = await store.list(objectKey(ROOT), cursor);
    out.push(...page.items.map((i) => `${i.key}@${JSON.stringify(i.version)}`));
    cursor = page.nextCursor;
  } while (cursor);
  return out.sort();
}

describe("S3 ledger backfill (0005 step 0, #580 formats)", () => {
  test("fences for every user with upload state: deletions bump epochs, resets carry their time and count", async () => {
    const store = scratch();
    await del(A, "origin-1", "claude", "sess-1");
    await del(A, "origin-1", "codex", "sess-2");
    await reset(B, "req-1", "2030-01-02T00:00:00.000Z", 3);
    await reset(B, "req-2", "2030-01-03T00:00:00.000500Z", 1);
    await batch(C, "origin-3", "batch-1"); // no controls at all: still gets a fence
    const summary = await run(store);
    expect(summary.users).toBe(3);
    expect(summary.fences).toMatchObject({ deletion_rows: 2, reset_rows: 2, created: 3, pairs_added: 2, resets_added: 2, unreadable: 0, failed: 0 });
    expect(summary.sections).toEqual({ fences: "complete", ownership: "complete", revisions: "complete" });
    const a = (await fenceOf(store, A))!;
    expect(a.deleted).toEqual({ "origin-1": [["claude", "sess-1"], ["codex", "sess-2"]] });
    expect(fenceEpoch(a, "origin-1", sessionKeyHash("claude", "sess-1"))).toBe(1);
    const b = (await fenceOf(store, B))!;
    expect(b).toMatchObject({ resets: 2, resetsCompleted: 2, resetRequests: { "req-1": 3, "req-2": 1 }, lastResetAtMs: Date.parse("2030-01-03T00:00:00.000Z") + 1 });
    // A backfilled reset is complete by definition: entries before it are forgotten (resetsCompleted == resets).
    for (const f of [a, b, (await fenceOf(store, C))!]) expect(f.resetsCompleted).toBe(f.resets);
    expect(await fenceOf(store, C)).toMatchObject({ resets: 0, deleted: {}, version: 1 });
    // Counts only: no tenant, session or record ids.
    const text = JSON.stringify(summary);
    for (const needle of ["org-a", "ws-a", "user-1", "origin-1", "sess-1", "req-1", "96bulls"]) expect(text).not.toContain(needle);
  });

  test("finding 8: no long snapshot: every page is its own short read-only transaction, checked, always rolled back", async () => {
    const store = scratch();
    await del(A, "origin-1", "claude", "sess-1");
    await own(A, "origin-1", "claude", "sess-2");
    await rev(A, "origin-1", "rec-1", 1, "claude", "sess-2");
    await del(B, "origin-1", "claude", "sess-9");
    const log: string[] = [];
    await run(store, { pageSize: 1, rowPageSize: 1 }, log);
    const txs: string[][] = [];
    for (const st of log) {
      if (st.startsWith("BEGIN")) txs.push([]);
      txs[txs.length - 1]!.push(st);
    }
    expect(txs.length).toBeGreaterThan(6); // users, controls, ownership, revisions and slots pages, each on its own
    for (const tx of txs) {
      expect(tx[0]).toBe("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
      expect(tx[1]).toBe("SHOW transaction_read_only");
      expect(tx.at(-1)).toBe("ROLLBACK");
      expect(tx.length).toBe(5); // BEGIN, SHOW, SET LOCAL, one SELECT, ROLLBACK
      expect(tx[2]).toMatch(/^SET LOCAL statement_timeout = \d+$/);
      expect(tx[3]).toMatch(/^SELECT/);
    }
    const refused: string[] = [];
    const notReadOnly: BackfillSql = { async query(text) { refused.push(text); return { rows: text.startsWith("SHOW") ? [{ transaction_read_only: "off" }] : [] }; } };
    await expect(backfillUploadFences({ sql: notReadOnly, store, keyRoot: ROOT, dryRun: false })).rejects.toThrow("backfill_not_read_only");
    expect(refused).toEqual(["BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY", "SHOW transaction_read_only", "ROLLBACK"]);
  });

  test("ownership stubs at the backfilled fence's generation; deleted sessions skipped", async () => {
    const store = scratch();
    await reset(A, "req-1", "2030-01-02T00:00:00.000Z");
    await del(A, "origin-1", "claude", "gone");
    await own(A, "origin-1", "claude", "kept");
    await own(A, "origin-1", "claude", "gone");
    const summary = await run(store);
    expect(summary.ownership).toMatchObject({ rows: 2, registered: 1, skipped_deleted: 1, skipped_no_fence: 0, failed: 0 });
    expect(await ownerOf(store, A, "origin-1", "claude", "kept")).toEqual({ v: 1, originId: "origin-1", source: "claude", sessionId: "kept", epoch: 0, resets: 1, machineId: expect.stringMatching(/^srv_[0-9a-f]{32}$/) });
    expect(await ownerOf(store, A, "origin-1", "claude", "gone")).toBeUndefined();
  });

  test("revisions into the per-origin shards as current-generation entries; never lowering a stored one", async () => {
    const store = scratch();
    await reset(A, "req-1", "2030-01-02T00:00:00.000Z");
    await rev(A, "origin-1", "rec-1", 3, "claude", "sess-1", hex("1"));
    await rev(A, "origin-1", "rec-2", 2, "claude", null, hex("2"));        // a session-less record
    await rev(A, "origin-1", "rec-3", 1, "claude", "gone");                 // a deleted session: forgotten
    await del(A, "origin-1", "claude", "gone");
    await rev(A, "origin-1", "rec-4", 7, "claude", "sess-1", null, null);   // a seed-proof row: no id or digest
    // S3 already holds rec-1 at a higher revision (admitted directly): the backfill must not lower it.
    const fenceBefore = await io(store).editFence(s3LedgerKeys(ROOT, A).fence, (f) => {
      applyFenceChange(f, { reset: { requestId: "req-1", atMs: Date.parse("2030-01-02T00:00:00.000Z"), result: 0 } });
      return { next: f, result: f };
    });
    await io(store).mergeIndex(s3LedgerKeys(ROOT, A).revisions("origin-1", "rec-1"), fenceBefore, "origin-1",
      [{ recordId: "rec-1", revision: 9, revisionId: "rec-1:r9", digest: hex("9"), sessionKey: sessionKeyHash("claude", "sess-1"), epoch: 0, resets: 1 }]);

    const summary = await run(store);
    expect(summary.revisions).toMatchObject({ rows: 4, seed_rows: 1, merged: 3, skipped_deleted: 1, invalid: 0, failed: 0 });
    expect(summary.sections.revisions).toBe("complete");
    expect(await entryOf(store, A, "origin-1", "rec-1")).toEqual([9, "rec-1:r9", hex("9"), sessionKeyHash("claude", "sess-1"), 0, 1]);
    expect(await entryOf(store, A, "origin-1", "rec-2")).toEqual([2, "rec-2:r2", hex("2"), null, 0, 0]);
    expect(await entryOf(store, A, "origin-1", "rec-3")).toBeUndefined();
    // The operator seed's row: a revision-only entry (#587), stale by revision alone, as SQL's #250 check.
    expect(await entryOf(store, A, "origin-1", "rec-4")).toEqual([7, null, null, sessionKeyHash("claude", "sess-1"), 0, 1]);
  });

  test("lazy native slots: under their owner and session, forgotten by S3 exactly as ingest_lazy_clear forgets them", async () => {
    const store = scratch();
    const D = { orgId: "org-a", workspaceId: "ws-a", userId: "user-only-in-slots" };
    await slot(A, "origin-1", "s0001", A.userId, "claude", "sess-1", 5, hex("5"));   // a sessioned slot
    await slot(A, "origin-1", "s0002", A.userId, "claude", null, 3, hex("3"));       // a session-less slot
    await slot(A, "origin-1", "s0003", null, null, null, null, null);               // cleared (or erase-tombstoned)
    await slot(A, "origin-1", "s0004", A.userId, "claude", "gone", 2, hex("2"));     // a deleted session
    await del(A, "origin-1", "claude", "gone");
    await slot(D, "origin-2", "s0005", D.userId, "codex", "sess-9", 1, hex("1"));     // a user known only from a slot
    const summary = await run(store);
    expect(summary.revisions).toMatchObject({ slot_rows: 5, slots_empty: 1, skipped_deleted: 1, merged: 3, invalid: 0, failed: 0 });
    expect(summary.sections.revisions).toBe("complete");
    const k = sessionKeyHash("claude", "sess-1");
    const sessioned = (await entryOf(store, A, "origin-1", "s0001"))!;
    expect(sessioned).toEqual([5, null, hex("5"), k, 0, 0]);
    const sessionless = (await entryOf(store, A, "origin-1", "s0002"))!;
    expect(sessionless).toEqual([3, null, hex("3"), null, 0, 0]);
    expect(await entryOf(store, A, "origin-1", "s0004")).toBeUndefined();
    expect(await fenceOf(store, D)).not.toBeNull();
    expect(await entryOf(store, D, "origin-2", "s0005")).toEqual([1, null, hex("1"), sessionKeyHash("codex", "sess-9"), 0, 0]);
    // SQL's ingest_lazy_clear clears sessioned slots on a disable of their session or a reset, never session-less
    // ones: ledger-s3 forgets the copies the same way.
    const fence = (await fenceOf(store, A))!;
    const reset = structuredClone(fence);
    applyFenceChange(reset, { reset: { requestId: "later", atMs: 1, result: 0 } });
    expect(entryForgotten(reset, "origin-1", sessioned)).toBe(true);
    expect(entryForgotten(reset, "origin-1", sessionless)).toBe(false);
    const disabled = structuredClone(fence);
    applyFenceChange(disabled, { deletions: [{ originId: "origin-1", sessions: [["claude", "sess-1"]] }] });
    expect(entryForgotten(disabled, "origin-1", sessioned)).toBe(true);
    expect(entryForgotten(disabled, "origin-1", sessionless)).toBe(false);
  });

  test("a rerun is a no-op; merging keeps everything already in the fence", async () => {
    const store = scratch();
    await del(A, "origin-1", "claude", "sess-1");
    await own(A, "origin-1", "claude", "sess-2");
    await rev(A, "origin-1", "rec-1", 1, "claude", "sess-2");
    await run(store);
    const before = await allKeys(store);
    const again = await run(store);
    expect(again.fences).toMatchObject({ created: 0, updated: 0, unchanged: 1, pairs_added: 0 });
    expect(again.ownership).toMatchObject({ registered: 0, unchanged: 1 });
    expect(await allKeys(store)).toEqual(before); // no object rewritten (versions unchanged)

    // A session re-enabled in SQL since stays fenced in S3 (never dropped); a new deletion is added.
    await db.exec("DELETE FROM sessions.upload_deletions");
    await del(A, "origin-9", "claude", "sess-9");
    const merged = await run(store);
    expect(merged.fences).toMatchObject({ updated: 1, pairs_added: 1 });
    expect((await fenceOf(store, A))!.deleted).toEqual({ "origin-1": [["claude", "sess-1"]], "origin-9": [["claude", "sess-9"]] });
  });

  test("dry run reads and classifies, and writes nothing", async () => {
    const store = scratch();
    await del(A, "origin-1", "claude", "sess-1");
    await own(A, "origin-1", "claude", "sess-2");
    await rev(A, "origin-1", "rec-1", 1, "claude", "sess-2");
    await io(store).editFence(s3LedgerKeys(ROOT, B).fence, (f) => ({ next: f, result: undefined }));
    await reset(B, "req-1", "2030-01-03T00:00:00.000Z");
    const before = await allKeys(store);
    const summary = await run(store, { dryRun: true });
    expect(summary.mode).toBe("dry_run");
    expect(summary.fences).toMatchObject({ would_create: 1, would_update: 1, created: 0, updated: 0, failed: 0 });
    expect(summary.ownership).toMatchObject({ would_register: 1, registered: 0, failed: 0 });
    expect(summary.revisions).toMatchObject({ would_merge: 1, merged: 0, failed: 0 });
    expect(await allKeys(store)).toEqual(before);
  });

  test("org scope: only the named orgs are read and written", async () => {
    const store = scratch();
    await del(A, "origin-1", "claude", "sess-1");
    await del(C, "origin-3", "claude", "sess-3");
    const summary = await run(store, { orgs: ["org-b"] });
    expect(summary.users).toBe(1);
    expect(await fenceOf(store, A)).toBeNull();
    expect(await fenceOf(store, C)).not.toBeNull();
    await expect(run(store, { orgs: ["../x"] })).rejects.toThrow("backfill_org_invalid");
  });

  test("fail closed: an unreadable fence is never rewritten; an unrepresentable row leaves the user without a fence", async () => {
    const store = scratch();
    const bad = "{\"v\":1,\"resets\":\"x\"}";
    await store.put(objectKey(s3LedgerKeys(ROOT, A).fence), enc.encode(bad));
    await del(A, "origin-1", "claude", "sess-1");
    await own(A, "origin-1", "claude", "sess-2");
    await reset(B, "__proto__", "2030-01-03T00:00:00.000Z");
    await rev(B, "origin-2", "rec-1", 1, "claude", "sess-1");
    const summary = await run(store);
    expect(summary.fences).toMatchObject({ unreadable: 1, input_invalid: 1 });
    expect(summary.ownership.skipped_no_fence).toBe(2); // A's claimed session and B's revision session
    expect(summary.revisions.skipped_no_fence).toBe(1);
    expect(summary.sections).toEqual({ fences: "incomplete", ownership: "incomplete", revisions: "incomplete" });
    expect(dec.decode(await readAll(await store.get(objectKey(s3LedgerKeys(ROOT, A).fence))))).toBe(bad);
    expect(await fenceOf(store, B)).toBeNull();
  });

  test("sections run separately; ownership and revisions use the fence an earlier run backfilled", async () => {
    const store = scratch();
    await reset(A, "req-1", "2030-01-02T00:00:00.000Z");
    await own(A, "origin-1", "claude", "sess-1");
    const first = await run(store, { sections: ["fences"] });
    expect(first.sections).toEqual({ fences: "complete", ownership: "not_run", revisions: "not_run" });
    expect(await ownerOf(store, A, "origin-1", "claude", "sess-1")).toBeUndefined();
    const second = await run(store, { sections: ["ownership"] });
    expect(second.sections.ownership).toBe("complete");
    expect(await ownerOf(store, A, "origin-1", "claude", "sess-1")).toMatchObject({ epoch: 0, resets: 1 });
  });

  test("a fence already holding a reset in flight (the mirror's) is counted and leaves the section incomplete", async () => {
    const store = scratch();
    await io(store).editFence(s3LedgerKeys(ROOT, A).fence, (f) => {
      applyFenceChange(f, { reset: { requestId: "mirror-running", atMs: Date.parse("2030-01-02T00:00:00.000Z"), result: null } });
      return { next: f, result: undefined };
    });
    await reset(A, "req-sql", "2030-01-03T00:00:00.000Z", 2);
    const summary = await run(store);
    expect(summary.fences).toMatchObject({ updated: 1, resets_added: 1, resets_in_flight: 1 });
    expect(summary.sections.fences).toBe("incomplete");
    const f = (await fenceOf(store, A))!;
    expect(f.resetsCompleted).toBeLessThan(f.resets); // ledger-s3's rule: never advanced past a reset in flight
  });

  test("main's v:2 fence (a control pending): the backfill merges into it and keeps the pending control and v:2", async () => {
    const store = scratch();
    const control = { epoch: "e".repeat(64), version: "1", digest: `d${"1".padStart(63, "0")}`, kind: "selection" as const };
    await s3ControlMirror(store, ROOT).beginOrdered({ tenant: A, originId: "origin-1", sessions: [["claude", "pending"]], enabled: false, control } as OrderedControlInput);
    expect((await fenceOf(store, A))!.v).toBe(2);
    await del(A, "origin-1", "claude", "sess-1");
    await reset(A, "req-1", "2030-01-02T00:00:00.000Z", 1);
    const summary = await run(store);
    expect(summary.fences).toMatchObject({ updated: 1, pairs_added: 1, resets_added: 1, unreadable: 0 });
    const f = (await fenceOf(store, A))!;
    expect(f.v).toBe(2);
    expect(isDeletedForAdmission(f, "origin-1", "claude", "pending")).toBe(true); // the pending disable survives
    expect(isDeletedForAdmission(f, "origin-1", "claude", "sess-1")).toBe(true);
    expect(f.resetsCompleted).toBe(f.resets);
  });

  test("finding 3: owner stubs also for the sessions of stored revisions and owned lazy slots (SQL's reset owned set)", async () => {
    const store = scratch();
    await rev(A, "origin-1", "rec-1", 1, "claude", "from-revision");
    await slot(A, "origin-1", "s0001", A.userId, "claude", "from-slot", 2, hex("2"));
    await slot(A, "origin-1", "s0002", A.userId, "claude", null, 2, hex("3")); // session-less: no stub
    const summary = await run(store);
    expect(summary.ownership).toMatchObject({ rows: 2, registered: 2, failed: 0 });
    for (const sid of ["from-revision", "from-slot"]) expect(await ownerOf(store, A, "origin-1", "claude", sid)).toMatchObject({ epoch: 0, resets: 0, machineId: expect.stringMatching(/^srv_/) });
  });

  test("finding 5: a rerun does not re-delete a session S3 re-enabled after the snapshot", async () => {
    const store = scratch();
    await del(A, "origin-1", "claude", "sess-1");
    await run(store);
    expect(isDeletedForAdmission((await fenceOf(store, A))!, "origin-1", "claude", "sess-1")).toBe(true);
    // The user re-enables it through the live control mirror; SQL's row is still in the (stale) snapshot.
    await s3ControlMirror(store, ROOT).setSessionUploads({ tenant: A, originId: "origin-1", sessions: [["claude", "sess-1"]], enabled: true });
    expect(isDeletedForAdmission((await fenceOf(store, A))!, "origin-1", "claude", "sess-1")).toBe(false);
    const rerun = await run(store);
    expect(rerun.fences).toMatchObject({ deletions_superseded: 1, pairs_added: 0 });
    expect(isDeletedForAdmission((await fenceOf(store, A))!, "origin-1", "claude", "sess-1")).toBe(false);
  });

  test("finding 8: a cold-imported reset is placed before every batch; one that cannot be is needs_review, never a drop", async () => {
    const store = scratch();
    // The seed's baseline import: the reset row and the baseline in one transaction (reset_at <= imported_at).
    await db.exec(`INSERT INTO sessions.upload_resets (org_id, workspace_id, user_id, request_id, reset_at, deleted)
                     VALUES ('org-a', 'ws-a', 'user-1', 'neon-reset', now() - interval '2 days', 4);
                   INSERT INTO sessions.ingest_seed_fence_baselines (org_id, workspace_id, user_id, fingerprint, imported_at)
                     VALUES ('org-a', 'ws-a', 'user-1', '${hex("f")}', now() - interval '2 days' + interval '1 second');`);
    await batch(A, "origin-1", "after-import"); // claimed now, after the import
    const summary = await run(store);
    expect(summary.fences).toMatchObject({ cold_resets: 1, needs_review: 0, created: 1 });
    expect(await fenceOf(store, A)).toMatchObject({ resets: 1, resetsCompleted: 1, lastResetAtMs: 1, resetRequests: { "neon-reset": 4 } });
    // A user with a batch claimed at or before its import cannot have its cold reset placed: no fence, needs review.
    await db.exec(`INSERT INTO sessions.upload_resets (org_id, workspace_id, user_id, request_id, reset_at, deleted)
                     VALUES ('org-a', 'ws-a', 'user-2', 'neon-reset', now(), 1);
                   INSERT INTO sessions.ingest_seed_fence_baselines (org_id, workspace_id, user_id, fingerprint, imported_at)
                     VALUES ('org-a', 'ws-a', 'user-2', '${hex("f")}', now() + interval '1 minute');`);
    await batch(B, "origin-1", "before-import");
    const second = await run(store);
    expect(second.fences.needs_review).toBe(1);
    expect(second.sections.fences).toBe("incomplete");
    expect(await fenceOf(store, B)).toBeNull();
  });

  test("a contended fence CAS retries and lands; tallies count once", async () => {
    const base = scratch();
    let fail = 1;
    const store = new Proxy(base, {
      get(target, prop) {
        if (prop === "put") return async (...a: Parameters<ObjectStore["put"]>) => {
          if (String(a[0]).endsWith("upload-fences.json") && fail-- > 0) throw new ObjectStoreError("precondition_failed", "fs", "concurrent writer");
          return target.put(...a);
        };
        const v = (target as unknown as Record<PropertyKey, unknown>)[prop];
        return typeof v === "function" ? (v as (...a: unknown[]) => unknown).bind(target) : v;
      },
    });
    await del(A, "origin-1", "claude", "sess-1");
    const summary = await run(store);
    expect(fail).toBe(-1);
    expect(summary.fences).toMatchObject({ created: 1, pairs_added: 1 });
  });

  test("CLI: the mode is never implied, --execute needs the KMS key, sections are a closed set", () => {
    expect(() => parseBackfillArgs(["--bucket", "my-bucket"])).toThrow("exactly one of");
    expect(() => parseBackfillArgs(["--bucket", "my-bucket", "--execute"])).toThrow("--kms-key-arn");
    expect(() => parseBackfillArgs(["--bucket", "my-bucket", "--dry-run", "--org", "../x"])).toThrow("--org");
    expect(() => parseBackfillArgs(["--bucket", "my-bucket", "--dry-run", "--section", "everything"])).toThrow("--section");
    expect(() => parseBackfillArgs(["--bucket", "my-bucket", "--dry-run", "--db-url", "x"])).toThrow("unknown flag");
    expect(parseBackfillArgs(["--dry-run", "--bucket", "my-bucket", "--org", "o1", "--section", "fences"])).toEqual({
      mode: "dry_run", bucket: "my-bucket", region: "us-east-1", orgs: ["o1"], sections: ["fences"], pageSize: 500, rowPageSize: 5_000, concurrency: 4, statementTimeoutMs: 30_000,
    });
  });
});
