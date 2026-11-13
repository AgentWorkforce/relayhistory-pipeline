import { afterAll, afterEach, beforeAll, describe, expect, setDefaultTimeout, test } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { ObjectStoreError, objectKey, readAll, type ObjectStore } from "../../shared/vendor.ts";
import { backfillFenceKey, backfillUploadFences, mergeFence, type BackfillOptions, type BackfillSql } from "../upload-fence-backfill.ts";
import { parseBackfillArgs } from "../../../scripts/upload-fence-backfill.ts";
import { applySupabaseSchema } from "./pglite-schema.ts";
import { scratchStore } from "./support.ts";

setDefaultTimeout(60_000);
const ROOT = "96bulls";
// Synthetic tenants and times only (the repo is public).
const A = { orgId: "org-a", workspaceId: "ws-a", userId: "user-1" };
const B = { orgId: "org-a", workspaceId: "ws-a", userId: "user-2" };
const C = { orgId: "org-b", workspaceId: "ws-b", userId: "user-3" };
const enc = new TextEncoder();
const dec = new TextDecoder();

let db: PGlite;
beforeAll(async () => {
  db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  // Both tables are created here and never altered by a later migration.
  await applySupabaseSchema(db, { until: "20261104022000_session_ingest_ledger.sql" });
});
afterAll(async () => { await db.close(); });

let cleanup: (() => void)[] = [];
afterEach(async () => {
  cleanup.forEach((c) => c());
  cleanup = [];
  await db.exec("DELETE FROM sessions.upload_deletions; DELETE FROM sessions.upload_resets;");
});
function scratch(): ObjectStore {
  const s = scratchStore();
  cleanup.push(s.done);
  return s.store;
}

async function del(t: typeof A, origin: string, source: string, session: string) {
  await db.query("INSERT INTO sessions.upload_deletions (org_id, workspace_id, user_id, origin_id, source, session_id) VALUES ($1,$2,$3,$4,$5,$6)", [t.orgId, t.workspaceId, t.userId, origin, source, session]);
}
async function reset(t: typeof A, requestId: string, at: string) {
  await db.query("INSERT INTO sessions.upload_resets (org_id, workspace_id, user_id, request_id, reset_at) VALUES ($1,$2,$3,$4,$5::timestamptz)", [t.orgId, t.workspaceId, t.userId, requestId, at]);
}

/** Every statement, so the tests can prove the SQL side never writes. */
function recordingSql(log: string[]): BackfillSql {
  return { async query(text, params) { log.push(text.trim()); return { rows: (await db.query(text, params as unknown[])).rows as Record<string, unknown>[] }; } };
}
const run = (store: ObjectStore, over: Partial<BackfillOptions> = {}, log: string[] = []) =>
  backfillUploadFences({ sql: recordingSql(log), store, keyRoot: ROOT, dryRun: false, sleep: async () => {}, ...over });

async function fence(store: ObjectStore, t: typeof A): Promise<unknown | null> {
  if (!(await store.head(objectKey(backfillFenceKey(ROOT, t))))) return null;
  return JSON.parse(dec.decode(await readAll(await store.get(objectKey(backfillFenceKey(ROOT, t))))));
}
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

describe("upload fence backfill (0005 step 0)", () => {
  test("writes each user's fence from SQL: deletions per origin, reset count, last reset rounded up", async () => {
    const store = scratch();
    await del(A, "origin-1", "claude", "sess-1");
    await del(A, "origin-1", "codex", "sess-2");
    await del(A, "origin-2", "claude", "sess-1");
    await reset(B, "req-1", "2030-01-02T00:00:00.000Z");
    await reset(B, "req-2", "2030-01-03T00:00:00.000500Z");
    const summary = await run(store);
    expect(summary).toMatchObject({ mode: "execute", users: 2, deletion_rows: 3, reset_rows: 2, pairs_added: 3, stopped: "complete" });
    expect(summary.fences.created).toBe(2);
    expect(await fence(store, A)).toEqual({ v: 1, resets: 0, deleted: { "origin-1": [["claude", "sess-1"], ["codex", "sess-2"]], "origin-2": [["claude", "sess-1"]] } });
    expect(await fence(store, B)).toEqual({ v: 1, resets: 2, deleted: {}, lastResetAtMs: Date.parse("2030-01-03T00:00:00.000Z") + 1 });
    // Counts only: no tenant or session ids.
    const text = JSON.stringify(summary);
    for (const needle of ["org-a", "ws-a", "user-1", "origin-1", "sess-1", "96bulls"]) expect(text).not.toContain(needle);
  });

  test("SQL is one read-only snapshot, checked, and always rolled back", async () => {
    const store = scratch();
    await del(A, "origin-1", "claude", "sess-1");
    const log: string[] = [];
    await run(store, {}, log);
    expect(log[0]).toBe("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
    expect(log[1]).toBe("SHOW transaction_read_only");
    expect(log.at(-1)).toBe("ROLLBACK");
    for (const s of log.slice(2, -1)) expect(s).toMatch(/^(SELECT|SET LOCAL statement_timeout = \d+$)/);

    // A session that is not read-only refuses before any query, and still rolls back.
    const refused: string[] = [];
    const notReadOnly: BackfillSql = { async query(text) { refused.push(text); return { rows: text.startsWith("SHOW") ? [{ transaction_read_only: "off" }] : [] }; } };
    await expect(backfillUploadFences({ sql: notReadOnly, store, keyRoot: ROOT, dryRun: false })).rejects.toThrow("backfill_not_read_only");
    expect(refused).toEqual(["BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY", "SHOW transaction_read_only", "ROLLBACK"]);
  });

  test("a rerun is a no-op; a re-enabled session stays fenced (merge never drops)", async () => {
    const store = scratch();
    await del(A, "origin-1", "claude", "sess-1");
    await del(A, "origin-1", "claude", "sess-2");
    await run(store);
    const before = await allKeys(store);
    const again = await run(store);
    expect(again.fences.unchanged).toBe(1);
    expect(again.pairs_added).toBe(0);
    expect(await allKeys(store)).toEqual(before);

    await db.exec("DELETE FROM sessions.upload_deletions WHERE session_id = 'sess-2'"); // re-enabled in SQL
    await del(A, "origin-9", "claude", "sess-9");
    const merged = await run(store);
    expect(merged.fences.updated).toBe(1);
    expect(merged.pairs_added).toBe(1);
    expect(await fence(store, A)).toEqual({ v: 1, resets: 0, deleted: { "origin-1": [["claude", "sess-1"], ["claude", "sess-2"]], "origin-9": [["claude", "sess-9"]] } });
  });

  test("merges into an existing fence: keeps its entries, max resets and last reset", async () => {
    const store = scratch();
    await store.put(objectKey(backfillFenceKey(ROOT, B)), enc.encode(JSON.stringify({ v: 1, resets: 5, deleted: { "origin-x": [["claude", "kept"]] }, lastResetAtMs: Date.parse("2030-02-01T00:00:00.000Z") })));
    await reset(B, "req-1", "2030-01-03T00:00:00.000Z");
    await del(B, "origin-x", "claude", "new");
    const summary = await run(store);
    expect(summary.fences.updated).toBe(1);
    expect(await fence(store, B)).toEqual({ v: 1, resets: 5, deleted: { "origin-x": [["claude", "kept"], ["claude", "new"]] }, lastResetAtMs: Date.parse("2030-02-01T00:00:00.000Z") });
  });

  test("dry run reads and classifies, and writes nothing", async () => {
    const store = scratch();
    await del(A, "origin-1", "claude", "sess-1");
    await store.put(objectKey(backfillFenceKey(ROOT, B)), enc.encode(JSON.stringify({ v: 1, resets: 0, deleted: {} })));
    await reset(B, "req-1", "2030-01-03T00:00:00.000Z");
    const before = await allKeys(store);
    const summary = await run(store, { dryRun: true });
    expect(summary.mode).toBe("dry_run");
    expect(summary.fences.would_create).toBe(1);
    expect(summary.fences.would_update).toBe(1);
    expect(summary.fences.created + summary.fences.updated + summary.fences.failed).toBe(0);
    expect(summary.pairs_added).toBe(1);
    expect(await allKeys(store)).toEqual(before);
  });

  test("org scope: only the named orgs are read and written", async () => {
    const store = scratch();
    await del(A, "origin-1", "claude", "sess-1");
    await del(C, "origin-3", "claude", "sess-3");
    const summary = await run(store, { orgs: ["org-b"] });
    expect(summary.users).toBe(1);
    expect(await fence(store, A)).toBeNull();
    expect(await fence(store, C)).not.toBeNull();
    await expect(run(store, { orgs: ["../x"] })).rejects.toThrow("backfill_org_invalid");
  });

  test("a foreign-format or unreadable fence is never rewritten", async () => {
    const store = scratch();
    const foreign = JSON.stringify({ v: 1, resets: 0, deleted: {}, version: 3, epochs: {} });
    await store.put(objectKey(backfillFenceKey(ROOT, A)), enc.encode(foreign));
    await store.put(objectKey(backfillFenceKey(ROOT, B)), enc.encode("{\"v\":1,\"resets\":\"x\",\"deleted\":{}}"));
    await del(A, "origin-1", "claude", "sess-1");
    await del(B, "origin-1", "claude", "sess-1");
    const summary = await run(store);
    expect(summary.fences.fence_foreign_format).toBe(1);
    expect(summary.fences.fence_unreadable).toBe(1);
    expect(dec.decode(await readAll(await store.get(objectKey(backfillFenceKey(ROOT, A)))))).toBe(foreign);
  });

  test("pages by user with a keyset cursor; max-users stops early and a rerun finishes", async () => {
    const store = scratch();
    for (const t of [A, B, C]) await del(t, "origin-1", "claude", `s-${t.userId}`);
    const first = await run(store, { pageSize: 1, maxUsers: 2 });
    expect(first.users).toBe(2);
    expect(first.stopped).toBe("max_users");
    const rest = await run(store, { pageSize: 1 });
    expect(rest.users).toBe(3);
    expect(rest.fences.unchanged).toBe(2);
    expect(rest.fences.created).toBe(1);
  });

  test("a contended CAS retries and lands", async () => {
    const base = scratch();
    let fail = 1;
    const store = new Proxy(base, {
      get(target, prop) {
        if (prop === "put") return async (...a: Parameters<ObjectStore["put"]>) => {
          if (fail-- > 0) throw new ObjectStoreError("precondition_failed", "fs", "concurrent writer");
          return target.put(...a);
        };
        const v = (target as unknown as Record<PropertyKey, unknown>)[prop];
        return typeof v === "function" ? (v as (...a: unknown[]) => unknown).bind(target) : v;
      },
    });
    await del(A, "origin-1", "claude", "sess-1");
    const summary = await run(store);
    expect(summary.fences.created).toBe(1);
    expect(fail).toBe(-1); // one refused put, then the retry landed
    expect(await fence(base, A)).not.toBeNull();
  });

  test("mergeFence is idempotent and order-independent", () => {
    const state = { tenant: A, deleted: new Map([["o", [["s", "2"], ["s", "1"]] as [string, string][]]]), resetRows: 1, lastResetAtMs: 10 };
    const once = mergeFence(null, state).fence;
    expect(mergeFence(once, state)).toEqual({ fence: once, added: 0 });
    expect(once.deleted.o).toEqual([["s", "1"], ["s", "2"]]);
  });

  test("CLI: the mode is never implied and --execute needs the KMS key", () => {
    expect(() => parseBackfillArgs(["--bucket", "my-bucket"])).toThrow("exactly one of");
    expect(() => parseBackfillArgs(["--bucket", "my-bucket", "--execute"])).toThrow("--kms-key-arn");
    expect(() => parseBackfillArgs(["--bucket", "my-bucket", "--dry-run", "--org", "../x"])).toThrow("--org");
    expect(() => parseBackfillArgs(["--bucket", "my-bucket", "--dry-run", "--db-url", "x"])).toThrow("unknown flag");
    expect(parseBackfillArgs(["--dry-run", "--bucket", "my-bucket", "--org", "o1"])).toEqual({ mode: "dry_run", bucket: "my-bucket", region: "us-east-1", orgs: ["o1"], pageSize: 500, concurrency: 4, statementTimeoutMs: 30_000 });
  });
});
