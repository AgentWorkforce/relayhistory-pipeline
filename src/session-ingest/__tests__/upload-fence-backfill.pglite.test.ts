import { afterAll, afterEach, beforeAll, describe, expect, setDefaultTimeout, test } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { ObjectStoreError, objectKey, readAll, type ObjectStore } from "../../shared/vendor.ts";
import { applyFenceChange, fenceEpoch, s3LedgerKeys, s3LedgerObjects, sessionKeyHash, type FenceDoc } from "../ledger-s3.ts";
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
  // (030000), and the nullable seed-proof revision columns (040000). No later migration alters them.
  await applySupabaseSchema(db, { until: "20261104040000_ingest_s3_proof_seed.sql" });
});
afterAll(async () => { await db.close(); });

let cleanup: (() => void)[] = [];
afterEach(async () => {
  cleanup.forEach((c) => c());
  cleanup = [];
  await db.exec(`DELETE FROM sessions.upload_deletions; DELETE FROM sessions.upload_resets; DELETE FROM sessions.ingest_session_ownership;
    DELETE FROM sessions.ingest_record_revisions; DELETE FROM sessions.ingest_batches;`);
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
    expect(b).toMatchObject({ resets: 2, resetRequests: { "req-1": 3, "req-2": 1 }, lastResetAtMs: Date.parse("2030-01-03T00:00:00.000Z") + 1 });
    expect(await fenceOf(store, C)).toMatchObject({ resets: 0, deleted: {}, version: 1 });
    // Counts only: no tenant, session or record ids.
    const text = JSON.stringify(summary);
    for (const needle of ["org-a", "ws-a", "user-1", "origin-1", "sess-1", "req-1", "96bulls"]) expect(text).not.toContain(needle);
  });

  test("SQL is one read-only snapshot, checked, and always rolled back", async () => {
    const store = scratch();
    await del(A, "origin-1", "claude", "sess-1");
    await own(A, "origin-1", "claude", "sess-2");
    await rev(A, "origin-1", "rec-1", 1, "claude", "sess-2");
    const log: string[] = [];
    await run(store, {}, log);
    expect(log[0]).toBe("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
    expect(log[1]).toBe("SHOW transaction_read_only");
    expect(log.at(-1)).toBe("ROLLBACK");
    for (const s of log.slice(2, -1)) expect(s).toMatch(/^(SELECT|SET LOCAL statement_timeout = \d+$)/);
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
    expect(await ownerOf(store, A, "origin-1", "claude", "kept")).toEqual({ v: 1, originId: "origin-1", source: "claude", sessionId: "kept", epoch: 0, resets: 1 });
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
    expect(summary.revisions).toMatchObject({ rows: 4, merged: 2, skipped_deleted: 1, unrepresentable: 1, invalid: 0, failed: 0 });
    // A seed-proof row #580's entry cannot express leaves the section incomplete (reported, not guessed).
    expect(summary.sections.revisions).toBe("incomplete");
    expect(await entryOf(store, A, "origin-1", "rec-1")).toEqual([9, "rec-1:r9", hex("9"), sessionKeyHash("claude", "sess-1"), 0, 1]);
    expect(await entryOf(store, A, "origin-1", "rec-2")).toEqual([2, "rec-2:r2", hex("2"), null, 0, 0]);
    expect(await entryOf(store, A, "origin-1", "rec-3")).toBeUndefined();
    expect(await entryOf(store, A, "origin-1", "rec-4")).toBeUndefined();
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
    expect(summary.ownership.skipped_no_fence).toBe(1);
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
