import { afterAll, afterEach, beforeAll, describe, expect, setDefaultTimeout, test } from "bun:test";
import { createHash } from "node:crypto";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { ObjectStoreError, objectKey, readAll, type ObjectStore } from "../../shared/vendor.ts";
import { applyFenceChange, entryForgotten, fenceEpoch, isDeletedForAdmission, newFence, revisionShard, s3ControlMirror, s3LedgerKeys, s3LedgerObjects, sessionKeyHash, type FenceDoc } from "../ledger-s3.ts";
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
    DELETE FROM sessions.ingest_seed_fence_baselines; DELETE FROM sessions.ingest_control_receipts;`);
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
/** SQL's ordered-control receipt (052000), as ingest_apply_ordered_control writes it with the control's effect. */
const receipt = (t: T, epoch: string, version: number, kind: "selection" | "reset" = "selection") =>
  q(`INSERT INTO sessions.ingest_control_receipts (org_id, workspace_id, user_id, epoch, version, digest, kind, result, schema_generation)
     VALUES ($1,$2,$3,$4,$5,$6,$7,'{}'::jsonb,$8)`, [t.orgId, t.workspaceId, t.userId, epoch, version, hex("d"), kind, hex("f")]);
const EPOCH = hex("e");
/** An ordered selection through main's control mirror, begun and finished (SQL applied it), as the Worker does. */
async function mirrored(store: ObjectStore, t: T, session: string, enabled: boolean, version: number, epoch = EPOCH) {
  const input = { tenant: t, originId: "origin-1", sessions: [["claude", session]], enabled,
    control: { epoch, version: String(version), digest: hex("d"), kind: "selection" as const } } as OrderedControlInput;
  const mirror = s3ControlMirror(store, ROOT);
  await mirror.beginOrdered(input);
  await mirror.finishOrdered(input, "applied");
}
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
      // BEGIN, SHOW, SET LOCAL timeout, [a scoped slot window's two plan pins], one read (two for a page's controls:
      // deletions and resets, one snapshot; two for a slot window: heap size and rows), ROLLBACK. A read is a SELECT
      // or a users skip-scan (WITH RECURSIVE ... SELECT): nothing that writes.
      expect(tx[2]).toMatch(/^SET LOCAL statement_timeout = \d+$/);
      const pins = tx[3] === "SET LOCAL enable_indexscan = off" ? 2 : 0;
      if (pins) expect(tx[4]).toBe("SET LOCAL enable_bitmapscan = off");
      // The run's first transaction is the input fingerprint: its three counts in one snapshot.
      const fingerprint = tx === txs[0];
      if (fingerprint) expect(tx.length).toBe(7);
      else expect([5, 6]).toContain(tx.length - pins);
      for (const st of tx.slice(3 + pins, -1)) expect(st).toMatch(/^(SELECT|\s*WITH RECURSIVE k AS \()/);
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

  test("org scope: lazy slots are read in heap block windows, every scoped slot exactly once, none of another org", async () => {
    const store = scratch();
    for (let i = 0; i < 150; i++) {
      await slot(i % 3 === 0 ? C : A, "origin-1", `w${String(i).padStart(4, "0")}`, i % 3 === 0 ? C.userId : A.userId, "claude", `sess-${i}`, i + 1, hex(String(i % 10)));
    }
    const blocks = Number((await db.query<{ n: number }>("SELECT (pg_relation_size('sessions.ingest_lazy_native_slots') / current_setting('block_size')::bigint)::int AS n")).rows[0]!.n);
    expect(blocks).toBeGreaterThan(2); // the windows below really cross blocks
    const log: string[] = [];
    const summary = await run(store, { orgs: ["org-a"], slotWindowBlocks: 1 }, log);
    expect(summary.revisions).toMatchObject({ slot_rows: 100, slots_empty: 0, merged: 100, invalid: 0, failed: 0 });
    expect(summary.sections.revisions).toBe("complete");
    expect(log.filter((l) => l.includes("ctid >= $2::tid")).length).toBe(blocks);
    // Each window's transaction pins the heap read (never the org index once per window).
    for (let i = 0; i < log.length; i++) if (log[i]!.includes("ctid >= $2::tid")) {
      const tx = log.slice(log.lastIndexOf("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY", i), i);
      expect(tx).toContain("SET LOCAL enable_indexscan = off");
      expect(tx).toContain("SET LOCAL enable_bitmapscan = off");
    }
    expect(summary.ownership).toMatchObject({ rows: 100, registered: 100, failed: 0 }); // the slots' sessions, from the window pass
    expect(log.some((l) => l.includes("key_sha256 > $2::text"))).toBe(false);
    expect(await entryOf(store, A, "origin-1", "w0001")).toEqual([2, null, hex("1"), sessionKeyHash("claude", "sess-1"), 0, 0]);
    expect(await fenceOf(store, C)).toBeNull();
    await expect(run(store, { orgs: ["org-a"], slotWindowBlocks: 0 })).rejects.toThrow("backfill_slot_window_invalid");
  });

  /** A store that counts puts per key and can fail getObject on a key from its nth call (undefined = pass). */
  const counted = (failGet?: (key: string, n: number) => Error | undefined) => {
    const base = scratch(), puts = new Map<string, number>(), gets = new Map<string, number>();
    const store = new Proxy(base, {
      get(target, prop) {
        if (prop === "put") return async (...a: Parameters<ObjectStore["put"]>) => {
          const r = await target.put(...a);
          puts.set(String(a[0]), (puts.get(String(a[0])) ?? 0) + 1);
          return r;
        };
        if (prop === "getObject") return async (...a: Parameters<ObjectStore["getObject"]>) => {
          const k = String(a[0]), n = (gets.get(k) ?? 0) + 1;
          gets.set(k, n);
          const e = failGet?.(k, n);
          if (e) throw e;
          return target.getObject(...a);
        };
        const v = (target as unknown as Record<PropertyKey, unknown>)[prop];
        return typeof v === "function" ? (v as (...a: unknown[]) => unknown).bind(target) : v;
      },
    });
    return { store, puts, gets };
  };
  const shardPuts = (puts: Map<string, number>) => [...puts].filter(([k]) => /\/r=[0-9a-f]{3}\.json$/.test(k));

  test("merge buffer: block windows and pages are merged together, each shard written once per pass", async () => {
    for (let i = 0; i < 150; i++) {
      await slot(A, "origin-1", `m${String(i).padStart(4, "0")}`, A.userId, "claude", `sess-${i % 5}`, i + 1, hex(String(i % 10)));
      if (i < 60) await rev(A, "origin-2", `n${String(i).padStart(4, "0")}`, i + 1, "claude", "sess-r");
    }
    const one = counted();
    const summary = await run(one.store, { orgs: ["org-a"], slotWindowBlocks: 1, rowPageSize: 7 });
    expect(summary.revisions).toMatchObject({ slot_rows: 150, rows: 60, merged: 210, failed: 0 });
    expect(summary.sections.revisions).toBe("complete");
    const shards = shardPuts(one.puts);
    expect(shards.length).toBe(summary.revisions.shards);
    expect(shards.every(([, n]) => n === 1)).toBe(true); // one write per shard, not one per window or page

    // A small budget merges in several passes and stores the same entries.
    const small = counted();
    const again = await run(small.store, { orgs: ["org-a"], slotWindowBlocks: 1, rowPageSize: 7, mergeBufferEntries: 16 });
    expect(again.revisions).toMatchObject({ merged: 210, failed: 0 });
    expect(again.revisions.shards).toBeGreaterThan(summary.revisions.shards);
    for (let i = 0; i < 150; i += 13) {
      const r = `m${String(i).padStart(4, "0")}`;
      expect(await entryOf(small.store, A, "origin-1", r)).toEqual(await entryOf(one.store, A, "origin-1", r));
    }
    expect(await entryOf(small.store, A, "origin-2", "n0059")).toEqual(await entryOf(one.store, A, "origin-2", "n0059"));
    for (const bad of [0, 1.5, 50_000_001]) await expect(run(scratch(), { mergeBufferEntries: bad })).rejects.toThrow("backfill_merge_buffer_invalid");
  });

  test("merge buffer: a record with several slots (one per source identity) merges once per window, no early flush", async () => {
    /** A slot with its own key and source identity: one record can own several (one per source identity and epoch). */
    const slotOf = (record: string, seed: string, revision: number) =>
      q(`INSERT INTO sessions.ingest_lazy_native_slots (key_sha256, key_material, org_id, workspace_id, origin_id, record_id, source_identity_sha256, control_epoch,
           initialized_witness_sha256, current_owner, current_source, current_session, current_revision, current_digest, current_reset_epoch, current_fence_epoch)
         VALUES ($1, '[]'::jsonb, $2, $3, 'origin-1', $4, $5, $6, $6, $7, 'claude', 'sess-1', $8, $9, 0, 0)`,
        [createHash("sha256").update(`${record}/${seed}`).digest("hex"), A.orgId, A.workspaceId, record, createHash("sha256").update(seed).digest("hex"), hex("e"), A.userId, revision, hex(String(revision % 10))]);
    // dup-1: three slots side by side (one window); dup-2: slots far apart (separate 1-block windows), filler between.
    await slotOf("dup-1", "s1", 3);
    await slotOf("dup-1", "s2", 7);
    await slotOf("dup-1", "s3", 5);
    await slotOf("dup-2", "s1", 4);
    for (let i = 0; i < 120; i++) await slotOf(`fill-${i}`, "s1", i + 1);
    await slotOf("dup-2", "s2", 9);
    for (let i = 120; i < 240; i++) await slotOf(`fill-${i}`, "s1", i + 1);
    await slotOf("dup-2", "s3", 6);
    // dup-2's three slots really sit in three different heap blocks (so three 1-block windows).
    const blocksOf = await db.query<{ n: number }>("SELECT count(DISTINCT (ctid::text::point)[0])::int AS n FROM sessions.ingest_lazy_native_slots WHERE record_id = 'dup-2'");
    expect(blocksOf.rows[0]!.n).toBe(3);
    const c = counted();
    const summary = await run(c.store, { orgs: ["org-a"], slotWindowBlocks: 1 });
    expect(summary.revisions).toMatchObject({ slot_rows: 246, merged: 246, failed: 0 });
    expect(summary.sections.revisions).toBe("complete");
    const gets = new Map(c.gets); // before the reads below
    expect(await entryOf(c.store, A, "origin-1", "dup-1")).toEqual([7, null, hex("7"), sessionKeyHash("claude", "sess-1"), 0, 0]);
    expect(await entryOf(c.store, A, "origin-1", "dup-2")).toEqual([9, null, hex("9"), sessionKeyHash("claude", "sess-1"), 0, 0]);
    // One merge per shard for the whole pass: each shard read once and written once, never flushed per repeat.
    const shards = shardPuts(c.puts);
    expect(summary.revisions.shards).toBe(shards.length);
    expect(shards.every(([, n]) => n === 1)).toBe(true);
    for (const [k] of shards) expect(gets.get(k)).toBe(1);
  });

  test("mergeIndexBatches: the same shard as one mergeIndex call per batch, in order", async () => {
    // A fence with one completed reset: entries at resets 0 are forgotten, so the drop path runs too.
    const fence = { ...newFence(), resetsCompleted: 1 } as FenceDoc;
    let seed = 0x9e3779b9;
    const rnd = (n: number) => { // xorshift32: every bit is usable
      seed ^= seed << 13; seed >>>= 0; seed ^= seed >>> 17; seed ^= seed << 5; seed >>>= 0;
      return seed % n;
    };
    const shardOf = revisionShard("rec-0");
    const records: string[] = [];
    for (let i = 0; records.length < 3 && i < 200_000; i++) if (revisionShard(`rec-${i}`) === shardOf) records.push(`rec-${i}`);
    expect(records.length).toBe(3);
    const sessions = [sessionKeyHash("claude", "s-0"), sessionKeyHash("claude", "s-1"), null];
    const entry = () => {
      const full = rnd(2) === 0;
      return { recordId: records[rnd(records.length)]!, revision: 1 + rnd(4), revisionId: full ? `id-${rnd(3)}` : null,
        digest: full || rnd(2) === 0 ? hex(String(rnd(3))) : null, sessionKey: sessions[rnd(3)]!, epoch: rnd(3), resets: rnd(3) };
    };
    let multi = 0, crossRepeat = 0;
    for (let round = 0; round < 200; round++) {
      const batches = Array.from({ length: 1 + rnd(4) }, () => Array.from({ length: rnd(6) }, entry));
      if (batches.length > 1) multi++;
      const seen = batches.map((b) => new Set(b.map((e) => e.recordId)));
      if (seen.some((a, i) => seen.some((b, j) => j > i && [...a].some((r) => b.has(r))))) crossRepeat++;
      const key = s3LedgerKeys(ROOT, A).revisions("origin-9", records[0]!);
      const one = scratch(), two = scratch();
      const start = Array.from({ length: 3 }, entry).map((e) => ({ ...e, resets: 1 + rnd(2) })); // a non-empty, kept shard
      for (const st of [one, two]) await io(st).mergeIndex(key, fence, "origin-9", start);
      for (const b of batches) await io(one).mergeIndex(key, fence, "origin-9", b);
      await io(two).mergeIndexBatches(key, fence, "origin-9", batches);
      expect((await io(two).readIndex(key))?.doc.records).toEqual((await io(one).readIndex(key))?.doc.records);
    }
    // The generator really reaches the multi-batch path and repeats records across batches.
    expect(multi).toBeGreaterThan(100);
    expect(crossRepeat).toBeGreaterThan(50);
  });

  test("merge buffer: the fence is read again at merge time; unreadable then, nothing merges (incomplete)", async () => {
    await del(A, "origin-1", "claude", "sess-gone");
    for (let i = 0; i < 20; i++) await slot(A, "origin-1", `f${i}`, A.userId, "claude", "sess-1", i + 1, hex("b"));
    const fenceKey = String(objectKey(s3LedgerKeys(ROOT, A).fence));
    let fenceWritten = false;
    // The fence section writes the fence; the slot pass reads it, then the merge reads it again. Fail only reads
    // after the write and past the slot pass's (a slot-pass failure would count skipped_no_fence, not this code).
    const c = counted((k, n) => {
      if (k !== fenceKey) return undefined;
      return fenceWritten && n > 2 ? new ObjectStoreError("unauthorized", "fs", "403") : undefined;
    });
    const store = new Proxy(c.store, {
      get(target, prop) {
        if (prop === "put") return async (...a: Parameters<ObjectStore["put"]>) => {
          const r = await (target.put as ObjectStore["put"])(...a);
          if (String(a[0]) === fenceKey) fenceWritten = true;
          return r;
        };
        const v = (target as unknown as Record<PropertyKey, unknown>)[prop];
        return typeof v === "function" ? (v as (...a: unknown[]) => unknown).bind(target) : v;
      },
    });
    const summary = await run(store, { orgs: ["org-a"], sections: ["fences", "revisions"] });
    expect(summary.revisions).toMatchObject({ slot_rows: 20, merged: 0, failed: 20, failed_codes: { fence_unreadable: 20 } });
    expect(summary.sections.revisions).toBe("incomplete");
    expect(shardPuts(c.puts)).toEqual([]);
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

  test("a session in ownership AND an owned slot is visited once (counters are per session)", async () => {
    const store = scratch();
    await own(A, "origin-1", "claude", "both");
    await rev(A, "origin-1", "rec-9", 1, "claude", "both");
    await slot(A, "origin-1", "s0001", A.userId, "claude", "both", 2, hex("2"));
    await slot(A, "origin-1", "s0002", A.userId, "claude", "slot-only", 2, hex("3"));
    for (const orgs of [undefined, ["org-a"]]) {
      const dry = await backfillUploadFences({ sql: recordingSql([]), store, keyRoot: ROOT, dryRun: true, sleep: async () => {}, ...(orgs ? { orgs } : {}) });
      expect(dry.ownership).toMatchObject({ rows: 2, would_register: 2, skipped_no_fence: 0, failed: 0 });
    }
  });

  test("users: a cleared (owner-less) slot is no user; a scoped run reads no other org's batch users", async () => {
    const store = scratch();
    await slot(A, "origin-1", "s0001", null, null, null, null, null);
    expect((await run(store, { sections: ["fences"] })).users).toBe(0);
    await batch(C, "origin-1", "b-1");
    await batch(A, "origin-1", "b-2");
    expect((await run(store, { sections: ["fences"], orgs: ["org-a"] })).users).toBe(1);
    expect(await fenceOf(store, C)).toBeNull();
    expect((await run(store, { sections: ["fences"] })).users).toBe(2);
  });

  test("concurrent dry-run fence reads never interleave transactions on the one connection", async () => {
    const store = scratch();
    for (let i = 0; i < 12; i++) await own({ ...A, userId: `user-${i}` }, "origin-1", "claude", `sess-${i}`);
    const log: string[] = [], inner = recordingSql(log);
    // Every statement yields first, as a real connection does, so the 8 workers really overlap.
    const slow: BackfillSql = { async query(text, params) { await new Promise((r) => setTimeout(r, 2)); return inner.query(text, params); } };
    await backfillUploadFences({ sql: slow, store, keyRoot: ROOT, dryRun: true, concurrency: 8, sleep: async () => {}, sections: ["ownership"] });
    let open = false;
    for (const st of log) {
      if (st.startsWith("BEGIN")) { expect(open).toBe(false); open = true; }
      else if (st === "ROLLBACK") { expect(open).toBe(true); open = false; }
      else expect(open).toBe(true); // every read inside a transaction: never autocommit, never unbounded
    }
  });

  test("slot owners come from the slot pass: once per session, also when ownership runs without revisions", async () => {
    const store = scratch();
    await slot(A, "origin-1", "s0001", A.userId, "claude", "two-slots", 2, hex("2"));
    await slot(A, "origin-1", "s0002", A.userId, "claude", "two-slots", 3, hex("3"));
    await slot(A, "origin-1", "s0003", A.userId, "claude", "one-slot", 2, hex("4"));
    await run(store, { sections: ["fences"] });
    const own = await run(store, { sections: ["ownership"] });
    expect(own.ownership).toMatchObject({ rows: 2, registered: 2, failed: 0 });
    expect(own.sections).toEqual({ fences: "not_run", ownership: "complete", revisions: "not_run" });
    expect(own.revisions).toMatchObject({ rows: 0, merged: 0 });
    for (const sid of ["two-slots", "one-slot"]) expect(await ownerOf(store, A, "origin-1", "claude", sid)).toMatchObject({ epoch: 0 });
    expect(await entryOf(store, A, "origin-1", "s0001")).toBeUndefined();
  });

  test("finding 5: a rerun does not re-delete a session an ordered control re-enabled after SQL's snapshot", async () => {
    const store = scratch();
    // SQL applied the disable (receipt v1, its deletion row); S3 has it too, then the user's re-enable (v2), which SQL's
    // snapshot predates: S3's control is provably newer than everything the snapshot holds.
    await receipt(A, EPOCH, 1);
    await del(A, "origin-1", "claude", "sess-1");
    await mirrored(store, A, "sess-1", false, 1);
    await mirrored(store, A, "sess-1", true, 2);
    expect(isDeletedForAdmission((await fenceOf(store, A))!, "origin-1", "claude", "sess-1")).toBe(false);
    const rerun = await run(store);
    expect(rerun.fences).toMatchObject({ deletions_superseded: 1, deletions_unproven: 0, pairs_added: 0 });
    expect(isDeletedForAdmission((await fenceOf(store, A))!, "origin-1", "claude", "sess-1")).toBe(false);
  });

  test("third review: a page's deletions and resets are read in ONE snapshot: a reset landing between them is seen whole or not at all", async () => {
    const store = scratch();
    await del(A, "origin-1", "claude", "other"); // the user exists (a deletion page row)
    // A reset that tombstones `S` commits right after the deletions are read (the first transaction that read them
    // ends): one snapshot sees neither its tombstone nor its reset; two separate reads would see only the reset.
    let armed = false, landed = false;
    const sql: BackfillSql = {
      async query(text, params) {
        const rows = (await db.query(text, params as unknown[])).rows as Record<string, unknown>[];
        if (text.includes("FROM sessions.upload_deletions AS d")) armed = true;
        else if (armed && !landed && text === "ROLLBACK") {
          landed = true;
          await del(A, "origin-1", "claude", "S");
          await reset(A, "req-x", "2030-01-02T00:00:00.000Z", 1);
        }
        return { rows };
      },
    };
    await backfillUploadFences({ sql, store, keyRoot: ROOT, dryRun: false, sleep: async () => {}, sections: ["fences"] });
    expect(landed).toBe(true);
    const f = (await fenceOf(store, A))!;
    // Consistent: the reset is not recorded as complete while its tombstone is missing.
    expect([f.resets, isDeletedForAdmission(f, "origin-1", "claude", "S")]).toEqual([0, false]);
    // A rerun sees both.
    await run(store, { sections: ["fences"] });
    const g = (await fenceOf(store, A))!;
    expect([g.resets, g.resetsCompleted, isDeletedForAdmission(g, "origin-1", "claude", "S")]).toEqual([1, 1, true]);
  });

  test("second review 8: mirror disable then re-enable, then a SQL reset tombstones the session: it ends deleted in S3", async () => {
    const store = scratch();
    await mirrored(store, A, "S", false, 1);
    await mirrored(store, A, "S", true, 2);
    expect(isDeletedForAdmission((await fenceOf(store, A))!, "origin-1", "claude", "S")).toBe(false);
    // SQL's reset (not yet seen by S3) tombstones S. With no receipt at all the version rule alone could not order it,
    // and a reset S3 has not completed for its request id never lets a deletion be skipped.
    await del(A, "origin-1", "claude", "S");
    await reset(A, "req-9", "2030-01-02T00:00:00.000Z", 1);
    const summary = await run(store);
    expect(summary.fences).toMatchObject({ deletions_superseded: 0, deletions_unproven: 1, pairs_added: 1, resets_added: 1 });
    expect(isDeletedForAdmission((await fenceOf(store, A))!, "origin-1", "claude", "S")).toBe(true);
  });

  test("second review 8: S3's control no newer than SQL's latest receipt never supersedes SQL's deletion", async () => {
    const store = scratch();
    await mirrored(store, A, "S", false, 1);
    await mirrored(store, A, "S", true, 2);
    // SQL went on to v3, a disable of S S3 has not seen yet: S3's v2 is OLDER than the deletion.
    for (const v of [1, 2, 3]) await receipt(A, EPOCH, v);
    await del(A, "origin-1", "claude", "S");
    const summary = await run(store);
    expect(summary.fences).toMatchObject({ deletions_superseded: 0, deletions_unproven: 1, pairs_added: 1 });
    expect(isDeletedForAdmission((await fenceOf(store, A))!, "origin-1", "claude", "S")).toBe(true);
  });

  test("second review 8: across control epochs the order is unknown, so SQL's deletion is applied", async () => {
    const store = scratch();
    await mirrored(store, A, "S", false, 1, hex("7"));
    await mirrored(store, A, "S", true, 2, hex("7"));
    await receipt(A, EPOCH, 1); // SQL's receipts are on another epoch than S3's control
    await del(A, "origin-1", "claude", "S");
    const summary = await run(store);
    expect(summary.fences).toMatchObject({ deletions_superseded: 0, deletions_unproven: 1 });
    expect(isDeletedForAdmission((await fenceOf(store, A))!, "origin-1", "claude", "S")).toBe(true);
  });

  test("second review 8: an unordered S3 re-enable (no control version) cannot prove it is newer: re-applied, counted", async () => {
    const store = scratch();
    await del(A, "origin-1", "claude", "sess-1");
    await run(store);
    await s3ControlMirror(store, ROOT).setSessionUploads({ tenant: A, originId: "origin-1", sessions: [["claude", "sess-1"]], enabled: true });
    expect(isDeletedForAdmission((await fenceOf(store, A))!, "origin-1", "claude", "sess-1")).toBe(false);
    const rerun = await run(store);
    expect(rerun.fences).toMatchObject({ deletions_superseded: 0, deletions_unproven: 1, pairs_added: 1 });
    expect(isDeletedForAdmission((await fenceOf(store, A))!, "origin-1", "claude", "sess-1")).toBe(true);
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

  test("owner and fence reads retry a transient store failure; a lasting one is counted by its code", async () => {
    // getObject fails `budget` times per key with `code`, for owner stubs and fences only.
    const flaky = (code: "transient" | "unauthorized", budget: number, fences = true) => {
      const base = scratch(), left = new Map<string, number>();
      const store = new Proxy(base, {
        get(target, prop) {
          if (prop === "getObject") return async (...a: Parameters<ObjectStore["getObject"]>) => {
            const k = String(a[0]);
            if (k.includes("/s=") || (fences && k.endsWith("upload-fences.json"))) {
              const n = left.get(k) ?? budget;
              if (n > 0) { left.set(k, n - 1); throw new ObjectStoreError(code, "fs", "flaky"); }
            }
            return target.getObject(...a);
          };
          const v = (target as unknown as Record<PropertyKey, unknown>)[prop];
          return typeof v === "function" ? (v as (...a: unknown[]) => unknown).bind(target) : v;
        },
      });
      return store;
    };
    await own(A, "origin-1", "claude", "sess-1");
    await own(A, "origin-1", "claude", "sess-2");
    const dry = (store: ObjectStore) => backfillUploadFences({ sql: recordingSql([]), store, keyRoot: ROOT, dryRun: true, sleep: async () => {}, sections: ["ownership"] });
    // Two transient failures per key, then success: nothing failed, the section complete.
    const ok = await dry(flaky("transient", 2));
    expect(ok.ownership).toMatchObject({ rows: 2, would_register: 2, failed: 0, skipped_no_fence: 0, failed_codes: {} });
    expect(ok.sections.ownership).toBe("complete");
    // Past the attempt budget: counted by code, the section incomplete.
    const owners = flaky("transient", 0), stuck = new Proxy(owners, {
      get(target, prop) {
        if (prop === "getObject") return async (...a: Parameters<ObjectStore["getObject"]>) => {
          if (String(a[0]).includes("/s=")) throw new ObjectStoreError("transient", "fs", "down");
          return target.getObject(...a);
        };
        const v = (target as unknown as Record<PropertyKey, unknown>)[prop];
        return typeof v === "function" ? (v as (...a: unknown[]) => unknown).bind(target) : v;
      },
    });
    const bad = await dry(stuck);
    expect(bad.ownership).toMatchObject({ failed: 2, failed_codes: { ledger_store_transient: 2 } });
    expect(bad.sections.ownership).toBe("incomplete");
    // Not retryable (unauthorized) on the owner read: one attempt, counted under its own code. (A failed FENCE read
    // leaves the user without a fence instead: skipped_no_fence, incomplete either way.)
    const denied = await dry(flaky("unauthorized", 1, false));
    expect(denied.ownership).toMatchObject({ failed: 2, failed_codes: { ledger_store_unauthorized: 2 } });
  });

  test("execute: owner, revision-shard and fence writes ride out transient store failures", async () => {
    const base = scratch(), left = new Map<string, number>();
    // Every put fails twice per key with a transient error, then lands.
    const store = new Proxy(base, {
      get(target, prop) {
        if (prop === "put") return async (...a: Parameters<ObjectStore["put"]>) => {
          const k = String(a[0]), n = left.get(k) ?? 2;
          if (n > 0) { left.set(k, n - 1); throw new ObjectStoreError("transient", "fs", "503"); }
          return target.put(...a);
        };
        const v = (target as unknown as Record<PropertyKey, unknown>)[prop];
        return typeof v === "function" ? (v as (...a: unknown[]) => unknown).bind(target) : v;
      },
    });
    await del(A, "origin-1", "claude", "sess-x");
    await own(A, "origin-1", "claude", "sess-1");
    await rev(A, "origin-1", "rec-1", 1, "claude", "sess-1");
    const summary = await run(store);
    expect(summary.sections).toEqual({ fences: "complete", ownership: "complete", revisions: "complete" });
    expect(summary.ownership).toMatchObject({ failed: 0, failed_codes: {} });
    expect(summary.revisions).toMatchObject({ merged: 1, failed: 0, failed_codes: {} });
    expect(await ownerOf(store, A, "origin-1", "claude", "sess-1")).toMatchObject({ epoch: expect.any(Number) });
  });

  /** A store whose `op` fails per key by `fail(key, n)` (n = calls so far for that key; undefined = pass through). */
  const failing = (op: "getObject" | "put", fail: (key: string, n: number) => Error | undefined) => {
    const base = scratch(), calls = new Map<string, number>();
    const store = new Proxy(base, {
      get(target, prop) {
        if (prop === op) return async (...a: unknown[]) => {
          const k = String(a[0]), n = calls.get(k) ?? 0;
          calls.set(k, n + 1);
          const e = fail(k, n);
          if (e) throw e;
          return (target[op] as (...x: unknown[]) => unknown)(...a);
        };
        const v = (target as unknown as Record<PropertyKey, unknown>)[prop];
        return typeof v === "function" ? (v as (...a: unknown[]) => unknown).bind(target) : v;
      },
    });
    return { store, calls };
  };

  test("retried: a backend status, a plain (mid-body) error, and the fences section's own dry read", async () => {
    await own(A, "origin-1", "claude", "sess-1");
    await reset(A, "req-1", "2030-01-02T00:00:00.000Z");
    const dry = (store: ObjectStore) => backfillUploadFences({ sql: recordingSql([]), store, keyRoot: ROOT, dryRun: true, sleep: async () => {} });
    for (const make of [() => new ObjectStoreError("backend", "fs", "400 RequestTimeout"), () => new Error("socket reset mid-body")]) {
      const { store } = failing("getObject", (k, n) => ((k.includes("/s=") || k.endsWith("upload-fences.json")) && n < 2 ? make() : undefined));
      const out = await dry(store);
      expect(out.sections).toEqual({ fences: "complete", ownership: "complete", revisions: "complete" });
      expect(out.fences).toMatchObject({ would_create: 1, failed: 0, unreadable: 0 });
      expect(out.ownership).toMatchObject({ would_register: 1, failed: 0, skipped_no_fence: 0 });
    }
  });

  test("a contended CAS is not retried by the backfill: counted once per entry under its code", async () => {
    // Two records in ONE revision shard, so one merge carries two entries.
    const ids: string[] = [];
    for (let i = 0; ids.length < 2; i++) if (revisionShard(`rec-${i}`) === revisionShard("rec-0")) ids.push(`rec-${i}`);
    await own(A, "origin-1", "claude", "sess-1");
    for (const id of ids) await rev(A, "origin-1", id, 1, "claude", "sess-1");
    await run(scratch(), { sections: ["fences"] });
    // Every shard write loses its compare-and-swap (precondition_failed): the ledger retries its CAS, then contended.
    const { store, calls } = failing("put", (k) => (k.includes("/r=") ? new ObjectStoreError("precondition_failed", "fs", "lost race") : undefined));
    await run(store, { sections: ["fences"] });
    const out = await run(store, { sections: ["revisions"] });
    expect(out.revisions).toMatchObject({ failed: 2, merged: 0, failed_codes: { ledger_revision_contended: 2 } });
    expect(out.sections.revisions).toBe("incomplete");
    const shardPuts = [...calls].filter(([k]) => k.includes("/r=")).map(([, n]) => n);
    expect(shardPuts).toEqual([8]); // the ledger's own 8 CAS attempts, not 8 x STORE_ATTEMPTS
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

  test("input fingerprint: counts only, read first in one snapshot, unchanged across runs with no change", async () => {
    const store = scratch();
    await own(A, "origin-1", "claude", "sess-1");
    await rev(A, "origin-1", "rec-1", 3, "claude", "sess-1");
    await rev(C, "origin-3", "rec-9", 1, "claude", "sess-9");
    await slot(A, "origin-1", "rec-2", A.userId, "claude", "sess-1", 4, hex("a"));
    await slot(A, "origin-1", "rec-3", A.userId, "claude", "sess-1", 5, hex("b"));
    await slot(A, "origin-1", "rec-4", null, null, null, null, null); // cleared: not owned, not counted
    const log: string[] = [];
    const first = await run(store, {}, log);
    const maxMs = Number((await db.query<{ ms: string }>(
      "SELECT floor(extract(epoch FROM max(updated_at)) * 1000)::bigint::text AS ms FROM sessions.ingest_record_revisions")).rows[0]!.ms);
    expect(first.fingerprint).toEqual({ revisions_count: 2, revisions_max_updated_ms: maxMs, revisions_hash: expect.any(Number), ownership_count: 1, ownership_hash: expect.any(Number),
      slots_owned_count: 2, slots_revision_sum: 9, slots_hash: expect.any(Number) });
    for (const v of Object.values(first.fingerprint)) expect(Number.isSafeInteger(v) && v >= 0).toBe(true);
    for (const h of [first.fingerprint.revisions_hash, first.fingerprint.ownership_hash, first.fingerprint.slots_hash]) expect(h).toBeLessThan(2 ** 40);
    // Read before any section: the run's first transaction holds the three counts.
    expect(log.slice(0, 7).filter((l) => l.includes("count(*)")).length).toBe(3);
    // Dry and execute, full and fences+ownership only: the same inputs give the same fingerprint.
    expect((await run(store)).fingerprint).toEqual(first.fingerprint);
    expect((await run(store, { dryRun: true })).fingerprint).toEqual(first.fingerprint);
    expect((await run(store, { sections: ["fences", "ownership"] })).fingerprint).toEqual(first.fingerprint);
    // Scoped: only the scope's rows.
    expect((await run(store, { orgs: ["org-b"] })).fingerprint).toEqual({ revisions_count: 1, revisions_max_updated_ms: expect.any(Number), revisions_hash: expect.any(Number),
      ownership_count: 0, ownership_hash: 0, slots_owned_count: 0, slots_revision_sum: 0, slots_hash: 0 });
  });

  test("input fingerprint hashes: equal counts with a swapped ownership or slot row change the hash; NULL columns still count", async () => {
    const store = scratch();
    await own(A, "origin-1", "claude", "sess-1");
    await slot(A, "origin-1", "rec-2", A.userId, "claude", null, 4, hex("a")); // owned, current_session NULL
    const fp = async () => (await run(store, { dryRun: true })).fingerprint;
    const base = await fp();
    expect(base.slots_hash).not.toBe(0); // the NULL-session row is hashed, not dropped
    // The ownership row swapped for another session: same count, other hash.
    await db.exec("UPDATE sessions.ingest_session_ownership SET session_id = 'sess-2'");
    const swapped = await fp();
    expect(swapped.ownership_count).toBe(base.ownership_count);
    expect(swapped.ownership_hash).not.toBe(base.ownership_hash);
    // The NULL-session slot's digest changes: same count and revision sum, other hash.
    await db.exec(`UPDATE sessions.ingest_lazy_native_slots SET current_digest = '${hex("b")}'`);
    const redigest = await fp();
    expect([redigest.slots_owned_count, redigest.slots_revision_sum]).toEqual([base.slots_owned_count, base.slots_revision_sum]);
    expect(redigest.slots_hash).not.toBe(swapped.slots_hash);
    // The NULL session filled in: other hash again.
    await db.exec("UPDATE sessions.ingest_lazy_native_slots SET current_session = 'sess-1'");
    expect((await fp()).slots_hash).not.toBe(redigest.slots_hash);
    // The slot's owner swapped for another user of the same org: same count and sum, other hash.
    const beforeOwner = await fp();
    await db.exec(`UPDATE sessions.ingest_lazy_native_slots SET current_owner = '${B.userId}'`);
    const owner = await fp();
    expect(owner.slots_owned_count).toBe(beforeOwner.slots_owned_count);
    expect(owner.slots_hash).not.toBe(beforeOwner.slots_hash);
  });

  test("input fingerprint changes with a revision row, an ownership row, or an owned slot's current_revision", async () => {
    const store = scratch();
    await own(A, "origin-1", "claude", "sess-1");
    await rev(A, "origin-1", "rec-1", 3, "claude", "sess-1");
    await slot(A, "origin-1", "rec-2", A.userId, "claude", "sess-1", 4, hex("a"));
    const base = (await run(store, { dryRun: true })).fingerprint;
    const fp = async () => (await run(store, { dryRun: true })).fingerprint;
    // A revision upsert (same row count): updated_at moves.
    await db.exec("UPDATE sessions.ingest_record_revisions SET revision = revision + 1, updated_at = updated_at + interval '1 second'");
    const afterRev = await fp();
    expect(afterRev.revisions_max_updated_ms).toBeGreaterThan(base.revisions_max_updated_ms);
    expect(afterRev.revisions_count).toBe(base.revisions_count);
    expect(afterRev.revisions_hash).not.toBe(base.revisions_hash);
    // The reviewer's case: a row rewritten with updated_at NOT moving (a writer whose tx began before the snapshot, or
    // two updates in one floored millisecond): count and max are unchanged, only the content hash sees it.
    await db.exec("UPDATE sessions.ingest_record_revisions SET digest = md5(coalesce(digest, '') || 'x') || md5('y')");
    const afterSameStamp = await fp();
    expect(afterSameStamp.revisions_count).toBe(afterRev.revisions_count);
    expect(afterSameStamp.revisions_max_updated_ms).toBe(afterRev.revisions_max_updated_ms);
    expect(afterSameStamp.revisions_hash).not.toBe(afterRev.revisions_hash);
    // A new revision row.
    await rev(A, "origin-1", "rec-5", 1, "claude", "sess-1");
    const afterNewRev = await fp();
    expect(afterNewRev.revisions_count).toBe(base.revisions_count + 1);
    // A new ownership row.
    await own(A, "origin-1", "claude", "sess-2");
    const afterOwn = await fp();
    expect(afterOwn.ownership_count).toBe(base.ownership_count + 1);
    expect(afterOwn).not.toEqual(afterNewRev);
    // An owned slot's current_revision (same owned count).
    await db.exec("UPDATE sessions.ingest_lazy_native_slots SET current_revision = current_revision + 1");
    const afterSlot = await fp();
    expect(afterSlot.slots_revision_sum).toBe(afterOwn.slots_revision_sum + 1);
    expect(afterSlot.slots_owned_count).toBe(afterOwn.slots_owned_count);
    // A new owned slot.
    await slot(A, "origin-1", "rec-6", A.userId, "claude", "sess-1", 2, hex("c"));
    expect((await fp()).slots_owned_count).toBe(afterSlot.slots_owned_count + 1);
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
