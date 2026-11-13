// Causal tests of the resumable copy against the real migration chain in PGlite (047030 creates the progress table). Fake values only.
import { test, expect, afterEach, setDefaultTimeout } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { SESSION_BOOTSTRAP_FILES } from "../../hoopdb/session-bootstrap-chain.ts";
import { migrationFiles } from "../../session-ingest/__tests__/pglite-schema.ts";
import { FrozenCopyUnknownCommit, jobBudget, type Queryable } from "../frozen-source-copy-seal.ts";
import {
  PROGRESS_TABLE, PROGRESS_TABLE_DDL, abortController, copyBinding, copyDataset, loadCheckpoint, progressTracker, reconcileInDoubt,
  requireCopyComplete, requireProgressTable, requireResumeBinding, serialQueue, commitPage, type CopyBinding, type Dataset,
} from "../frozen-source-copy-progress.ts";

setDefaultTimeout(60000);
const root = join(import.meta.dir, "../../..");
const generation = "a".repeat(64), epoch = "3".repeat(64), identity = "c".repeat(64), baseline = "a".repeat(64), OP = "0".repeat(64);
const status = { phase: "active", epoch, privacyBaselineSha256: baseline };
const open: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  for (const close of open.splice(0).reverse()) await close();
});

type Row = Record<string, unknown>;
/** Seven fake source records for org-a, keyset-ordered by (workspace_id, origin_id, record_id). */
const SOURCE: Row[] = Array.from({ length: 7 }, (_, i) => ({ org_id: "org-a", workspace_id: "ws", origin_id: "origin", record_id: `r${i}`, digest: `d${i}` }));
const key = (r: Row) => [String(r.workspace_id), String(r.origin_id), String(r.record_id)];
const after = (a: string[], b: string[]) => JSON.stringify(a) > JSON.stringify(b);

async function fixture(o: { table?: boolean } = {}) {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  open.push(() => db.close());
  const read = (p: string) => readFileSync(join(root, p), "utf8");
  await db.exec(read("supabase/tests/supabase_shim.sql"));
  for (const f of SESSION_BOOTSTRAP_FILES) await db.exec(read(f));
  for (const f of migrationFiles(root)) await db.exec(read("supabase/migrations/" + f));
  await db.exec(`CREATE TABLE sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text);INSERT INTO sessions.__96bulls_role_validation VALUES('session_directory_search_worker','${generation}','pass')`);
  await db.query("SELECT sessions.ingest_lazy_policy_set($1::jsonb)", [
    JSON.stringify({ baselineMode: "lazy-current", generation, sourceIdentitySha256: identity, controlEpoch: epoch, privacyBaselineSha256: baseline, retentionContractSha256: "b".repeat(64), sourceProofSha256: "d".repeat(64), validUntil: new Date(Date.now() + 3600000).toISOString(), issuer: "relayhistory-cloud" }),
  ]);
  // 20261105047030 creates the progress table in the chain; table:false removes it so the missing-table path is exercised.
  if (o.table === false) await db.exec(`DROP TABLE ${PROGRESS_TABLE}`);
  const log: string[] = [];
  const before: Array<(sql: string) => Promise<void> | void> = [];
  let commit: ((run: () => Promise<unknown>) => Promise<unknown>) | null = null;
  const supa: Queryable = {
    async query(sql: string, params?: unknown[]) {
      log.push(sql.split("\n")[0]!.slice(0, 60));
      for (const h of before) await h(sql);
      if (sql === "COMMIT" && commit) return (await commit(() => db.query(sql))) as never;
      return (await db.query(sql, params as never)) as never;
    },
  };
  const count = async () => (await db.query<{ n: number }>("SELECT count(*)::int n FROM sessions.ingest_frozen_source_records")).rows[0]!.n;
  let inserts = 0;
  const deps = (binding: CopyBinding, over: Partial<Parameters<typeof copyDataset>[0]> = {}) => ({
    target: supa, binding, org: "org-a", dataset: "records" as Dataset, pageSize: 3,
    readPage: async (cursor: string[], limit: number) => SOURCE.filter((r) => after(key(r), cursor)).slice(0, limit),
    keyOf: key,
    insert: async (rows: Row[]) => {
      inserts++;
      for (const r of rows) await supa.query("INSERT INTO sessions.ingest_frozen_source_records(org_id,workspace_id,origin_id,record_id,digest) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING", [r.org_id, r.workspace_id, r.origin_id, r.record_id, r.digest]);
    },
    check: () => {},
    serialize: serialQueue(),
    ...over,
  });
  const binding = await copyBinding(supa, status, OP);
  return { db, supa, log, before, setCommit: (f: typeof commit) => void (commit = f), count, deps, binding, inserts: () => inserts };
}
const code = async (p: Promise<unknown>) => {
  try {
    await p;
    return "no-refusal";
  } catch (e) {
    return (e as { code?: string }).code ?? String(e);
  }
};
const inTx = async (db: PGlite) => (await db.query<{ x: string | null }>("SELECT txid_current_if_assigned()::text x")).rows[0]!.x;

test("the proposed DDL is owner-only: RLS on, nothing granted to PUBLIC or any runtime role; the operator accepts it", async () => {
  const f = await fixture();
  await requireProgressTable(f.supa);
  for (const role of ["anon", "authenticated", "service_role", "bulls96_session_ingest", "bulls96_session_ingest_seeder"])
    for (const priv of ["SELECT", "INSERT", "UPDATE", "DELETE"])
      expect((await f.db.query<{ ok: boolean }>(`SELECT has_table_privilege($1,'${PROGRESS_TABLE}',$2) ok`, [role, priv])).rows[0]!.ok).toBe(false);
});

test("progress table absent -> frozen_copy_progress_table_missing; widened -> frozen_copy_progress_table_unsafe", async () => {
  const f = await fixture({ table: false });
  expect(await code(requireProgressTable(f.supa))).toBe("frozen_copy_progress_table_missing");
  await f.db.exec(PROGRESS_TABLE_DDL);
  await requireProgressTable(f.supa);
  for (const [widen, undo] of [
    [`GRANT SELECT ON ${PROGRESS_TABLE} TO anon`, `REVOKE SELECT ON ${PROGRESS_TABLE} FROM anon`],
    [`ALTER TABLE ${PROGRESS_TABLE} DISABLE ROW LEVEL SECURITY`, `ALTER TABLE ${PROGRESS_TABLE} ENABLE ROW LEVEL SECURITY`],
    [`ALTER TABLE ${PROGRESS_TABLE} ADD COLUMN note text`, `ALTER TABLE ${PROGRESS_TABLE} DROP COLUMN note`],
  ]) {
    await f.db.exec(widen!);
    expect(await code(requireProgressTable(f.supa))).toBe("frozen_copy_progress_table_unsafe");
    await f.db.exec(undo!);
    await requireProgressTable(f.supa);
  }
});

test("full copy: every page commits with its checkpoint; an empty dataset still records a complete 0-row checkpoint", async () => {
  const f = await fixture();
  const p = progressTracker(1);
  const cp = await copyDataset(f.deps(f.binding, { progress: p }));
  expect(cp).toMatchObject({ pageSeq: 3, rows: 7, status: "complete", cursor: ["ws", "origin", "r6"] });
  expect(await f.count()).toBe(7);
  expect(f.log.filter((s) => s === "COMMIT").length).toBe(3);
  expect(p.state).toMatchObject({ pages: 3, rows: 7, resumeCount: 0, inFlight: 0 });
  const empty = await copyDataset(f.deps(f.binding, { dataset: "receipts", readPage: async () => [] }));
  expect(empty).toMatchObject({ pageSeq: 1, rows: 0, status: "complete" });
  await requireCopyComplete(f.supa, f.binding, ["org-a"]);
  // Already complete: a rerun reads only the checkpoint and writes nothing.
  const n = f.log.length;
  expect((await copyDataset(f.deps(f.binding))).pageSeq).toBe(3);
  expect(f.log.slice(n).some((s) => s.startsWith("BEGIN") || s.startsWith("INSERT"))).toBe(false);
});

test("same transaction: a failure at the checkpoint rolls the page back too (a page without its checkpoint is impossible)", async () => {
  const f = await fixture();
  let pages = 0;
  f.before.push((sql) => {
    if (sql.startsWith(`INSERT INTO ${PROGRESS_TABLE}`) && ++pages === 2) throw new Error("socket closed before the checkpoint");
  });
  expect(await code(copyDataset(f.deps(f.binding)))).toBe("frozen_copy_page_not_committed");
  expect(await f.count()).toBe(3); // page 2's three rows were inserted in the transaction, then rolled back with it
  expect((await loadCheckpoint(f.supa, "org-a", "records"))).toMatchObject({ pageSeq: 1, rows: 3, status: "in_progress" });
  expect(await inTx(f.db)).toBeNull();
  // A server refusal (SQLSTATE) at the checkpoint behaves the same and keeps its code.
  f.before.length = 0;
  f.before.push((sql) => {
    if (sql.startsWith(`INSERT INTO ${PROGRESS_TABLE}`)) throw Object.assign(new Error("deadlock"), { code: "40P01" });
  });
  expect(await code(copyDataset(f.deps(f.binding)))).toBe("40P01");
  expect(await f.count()).toBe(3);
});

test("crash before COMMIT: resume re-reads exactly the uncommitted page and completes without gaps or duplicates", async () => {
  const f = await fixture();
  let pages = 0;
  f.before.push((sql) => {
    if (sql.startsWith("INSERT INTO sessions.ingest_frozen_source_records") && ++pages === 5) throw new Error("process killed");
  });
  expect(await code(copyDataset(f.deps(f.binding)))).toBe("frozen_copy_page_not_committed");
  expect(await f.count()).toBe(3);
  f.before.length = 0;
  const p = progressTracker(1);
  const cp = await copyDataset(f.deps(f.binding, { progress: p }));
  expect(cp).toMatchObject({ pageSeq: 3, rows: 7, status: "complete" });
  expect(p.state.resumeCount).toBe(1);
  expect((await f.db.query<{ id: string }>("SELECT record_id id FROM sessions.ingest_frozen_source_records ORDER BY 1")).rows.map((r) => r.id)).toEqual(SOURCE.map((r) => r.record_id));
});

test("COMMIT lost before the server ran it: UNKNOWN_COMMIT, readback NOT_COMMITTED, resume re-reads that page", async () => {
  const f = await fixture();
  let commits = 0;
  f.setCommit(async (run) => {
    if (++commits === 2) {
      await f.db.query("ROLLBACK"); // the server drops the dead session's open transaction
      throw new Error("Connection terminated unexpectedly");
    }
    return run();
  });
  const err = await copyDataset(f.deps(f.binding)).catch((e) => e);
  expect(err).toBeInstanceOf(FrozenCopyUnknownCommit);
  expect(err.context).toMatchObject({ phase: "copy", dataset: "records", checkpointSeq: 2, org: "org-a" });
  expect(await reconcileInDoubt(f.supa, err.context)).toBe("NOT_COMMITTED");
  expect(await f.count()).toBe(3);
  f.setCommit(null);
  expect(await copyDataset(f.deps(f.binding))).toMatchObject({ pageSeq: 3, rows: 7, status: "complete" });
  expect(await f.count()).toBe(7);
});

test("COMMIT applied but the answer was lost: readback COMMITTED, resume continues AFTER that page (no re-insert)", async () => {
  const f = await fixture();
  let commits = 0;
  f.setCommit(async (run) => {
    const r = await run();
    if (++commits === 2) throw new Error("Connection terminated unexpectedly");
    return r;
  });
  const err = await copyDataset(f.deps(f.binding)).catch((e) => e);
  expect(err).toBeInstanceOf(FrozenCopyUnknownCommit);
  expect(f.log.at(-1)).toBe("COMMIT"); // no ROLLBACK and no retry after an in-doubt COMMIT
  expect(await reconcileInDoubt(f.supa, err.context)).toBe("COMMITTED");
  expect(f.inserts()).toBe(2);
  f.setCommit(null);
  expect(await copyDataset(f.deps(f.binding))).toMatchObject({ pageSeq: 3, rows: 7, status: "complete" });
  expect(f.inserts()).toBe(3); // only page 3 was written by the resume
});

test("the checkpoint only advances from exactly the previous page under the same binding (stale or foreign writer conflicts)", async () => {
  const f = await fixture();
  const first = await commitPage(f.supa, { binding: f.binding, org: "org-a", dataset: "records", from: null, cursor: ["ws", "origin", "r2"], rows: 3, last: false }, () => f.deps(f.binding).insert(SOURCE.slice(0, 3)));
  expect(first.pageSeq).toBe(1);
  // A second writer that still believes it starts from nothing: conflict, its page rolled back.
  expect(await code(commitPage(f.supa, { binding: f.binding, org: "org-a", dataset: "records", from: null, cursor: ["ws", "origin", "r4"], rows: 2, last: false }, () => f.deps(f.binding).insert(SOURCE.slice(3, 5))))).toBe("frozen_copy_progress_conflict");
  // A writer under another binding: conflict even from the right page.
  const other = { ...f.binding, operatorSha256: "1".repeat(64) };
  expect(await code(commitPage(f.supa, { binding: other, org: "org-a", dataset: "records", from: first, cursor: ["ws", "origin", "r4"], rows: 2, last: false }, () => f.deps(f.binding).insert(SOURCE.slice(3, 5))))).toBe("frozen_copy_progress_conflict");
  expect(await f.count()).toBe(3);
  expect(await inTx(f.db)).toBeNull();
});

test("resume re-proof: another generation / epoch / operator build refuses; the same binding resumes", async () => {
  const f = await fixture();
  await copyDataset(f.deps(f.binding, { pageSize: 3, readPage: async (c, l) => SOURCE.filter((r) => after(key(r), c)).slice(0, Math.min(l, 3)).filter((r) => String(r.record_id) < "r3") }));
  expect(await requireResumeBinding(f.supa, f.binding)).toBe(1);
  expect(await code(requireResumeBinding(f.supa, { ...f.binding, operatorSha256: "1".repeat(64) }))).toBe("frozen_copy_resume_operator_mismatch");
  expect(await code(requireResumeBinding(f.supa, { ...f.binding, generation: "e".repeat(64) }))).toBe("frozen_copy_resume_binding_mismatch");
  expect(await code(requireResumeBinding(f.supa, { ...f.binding, controlEpoch: "4".repeat(64) }))).toBe("frozen_copy_resume_binding_mismatch");
  // The live policy itself must still equal the frozen descriptor before any binding is formed.
  expect(await code(copyBinding(f.supa, { ...status, epoch: "4".repeat(64) }, OP))).toBe("frozen_copy_stale_epoch");
  expect(await code(copyBinding(f.supa, { ...status, privacyBaselineSha256: "4".repeat(64) }, OP))).toBe("frozen_copy_stale_privacy_baseline");
  // Seal precondition under a different binding: incomplete.
  expect(await code(requireCopyComplete(f.supa, { ...f.binding, operatorSha256: "1".repeat(64) }, ["org-a"]))).toBe("frozen_copy_progress_incomplete");
});

test("abort: stops before the next page, nothing open, the committed checkpoint is kept and resume completes", async () => {
  const f = await fixture();
  const abort = abortController(() => 7);
  let reads = 0;
  const d = f.deps(f.binding, {
    check: abort.check,
    readPage: async (c: string[], l: number) => {
      if (++reads === 2) abort.request("SIGTERM"); // arrives while page 2 is being read
      return SOURCE.filter((r) => after(key(r), c)).slice(0, l);
    },
  });
  expect(await code(copyDataset(d))).toBe("frozen_copy_aborted");
  expect(abort.state).toEqual({ requested: true, observed: true, reason: "SIGTERM", requestedAt: 7 });
  expect(await f.count()).toBe(3);
  expect(await loadCheckpoint(f.supa, "org-a", "records")).toMatchObject({ pageSeq: 1, status: "in_progress" });
  expect(await inTx(f.db)).toBeNull();
  expect(await copyDataset(f.deps(f.binding))).toMatchObject({ pageSeq: 3, rows: 7 });
});

test("budget exceeded: same bounded stop (frozen_copy_budget_exhausted), checkpoint kept, no delete/reset needed to resume", async () => {
  const f = await fixture();
  let t = 0;
  const budget = jobBudget(100, () => t);
  let commits = 0;
  f.setCommit(async (run) => {
    const r = await run();
    if (++commits === 1) t = 100;
    return r;
  });
  expect(await code(copyDataset(f.deps(f.binding, { check: budget.check })))).toBe("frozen_copy_budget_exhausted");
  expect(await f.count()).toBe(3);
  expect((await loadCheckpoint(f.supa, "org-a", "records"))!.pageSeq).toBe(1);
  expect(await inTx(f.db)).toBeNull();
  f.setCommit(null);
  expect(await copyDataset(f.deps(f.binding))).toMatchObject({ pageSeq: 3, rows: 7 });
});
