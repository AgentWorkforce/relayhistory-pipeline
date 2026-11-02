/**
 * The session ingest ledger (20261102010000_session_ingest_ledger.sql) in PGlite
 * with every Supabase migration and the role bootstrap, called through
 * `sqlLedger` AS bulls96_session_ingest (SET ROLE), as production logs in.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { LedgerUnavailableError, type ClaimInput, type CommitInput, type LedgerRecord } from "../ledger.ts";
import { sqlLedger } from "../ledger-sql.ts";

const root = join(import.meta.dir, "../../..");
const read = (path: string) => readFileSync(join(root, path), "utf8");
/** psql meta-commands (\set) are not SQL. */
const sql = (path: string) => read(path).split("\n").filter((l) => !l.startsWith("\\")).join("\n");
const ROLE = sql("supabase/bootstrap/session_ingest_role.sql");
const VERIFY = sql("supabase/bootstrap/session_ingest_role_verify.sql");

const open: PGlite[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((db) => db.close()));
});

async function database(): Promise<PGlite> {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  open.push(db);
  await db.exec(read("supabase/tests/supabase_shim.sql"));
  await db.exec(read("supabase/bootstrap/session_directory_search_worker.sql"));
  for (const file of readdirSync(join(root, "supabase/migrations")).sort()) await db.exec(read(`supabase/migrations/${file}`));
  await db.exec(ROLE);
  return db;
}

/** Each call runs as `role`, the way one pooled login would. */
function runAs(db: PGlite, role = "bulls96_session_ingest") {
  return async (text: string, params: unknown[]) => {
    await db.exec(`SET ROLE ${role}`);
    try {
      return (await db.query(text, params)) as { rows: Record<string, unknown>[] };
    } finally {
      await db.exec("RESET ROLE");
    }
  };
}

const hex = (c: string) => c.repeat(64);
const tenant = { orgId: "org-a", workspaceId: "ws-a", userId: "user-1" };
const MACHINE = `srv_${"ab".repeat(16)}`;
const rec = (
  recordId: string,
  revision: number,
  digestChar: string,
  revisionId = `${recordId}:r${revision}`,
  session: [string, string | null] = ["claude", "s-1"],
): LedgerRecord => ({
  source: session[0],
  sessionId: session[1],
  recordId,
  revisionId,
  revision,
  digest: hex(digestChar),
});
function claimInput(over: Partial<ClaimInput> = {}): ClaimInput {
  return {
    tenant,
    originId: "origin-1",
    batchId: "batch-1",
    batchDigest: hex("a"),
    receiptId: `rhr_${hex("e")}`,
    sessions: [["claude", "s-1"]],
    records: [rec("r1", 1, "1")],
    ...over,
  };
}
const commitInput = (c: ClaimInput, sessions = c.sessions, records = c.records): CommitInput => ({
  tenant: c.tenant,
  originId: c.originId,
  batchId: c.batchId,
  batchDigest: c.batchDigest,
  sessions,
  records,
  machineId: MACHINE,
});
const MIGRATION = read("supabase/migrations/20261102010000_session_ingest_ledger.sql");
const revisions = async (db: PGlite) =>
  (await db.query("SELECT record_id, revision, revision_id, digest FROM sessions.ingest_record_revisions ORDER BY record_id")).rows;
const state = async (db: PGlite, batchId = "batch-1") =>
  (await db.query<{ state: string }>("SELECT state FROM sessions.ingest_batches WHERE batch_id = $1", [batchId])).rows[0]?.state;
/** claim then commit with every record written: the records become committed revisions. */
async function land(ledger: ReturnType<typeof sqlLedger>, c: ClaimInput) {
  const r = await ledger.claim(c);
  if (r.kind !== "claimed" && r.kind !== "resume") throw new Error(`expected a claim, got ${r.kind}`);
  await ledger.commit(commitInput(c));
}

describe("session ingest ledger", () => {
  test("no advisory lock of any kind in the migration", () => {
    expect(MIGRATION).not.toMatch(/pg_advisory|pg_try_advisory/i);
  });

  test("the bootstrap gives the role exactly the approved shape", async () => {
    const db = await database();
    await db.exec(VERIFY);
    await db.exec("GRANT SELECT ON sessions.ingest_batches TO bulls96_session_ingest");
    await expect(db.exec(VERIFY)).rejects.toThrow(/privilege on sessions.ingest_batches/);
  });

  test("the verify script catches EXECUTE on another definer function (the seed included)", async () => {
    const db = await database();
    await db.exec("GRANT EXECUTE ON FUNCTION sessions.ingest_seed_records(jsonb) TO bulls96_session_ingest");
    await expect(db.exec(VERIFY)).rejects.toThrow(/EXECUTE on sessions.ingest_seed_records\(jsonb\)/);
  });

  test("the role reads no ledger table directly", async () => {
    const db = await database();
    await expect(runAs(db)("SELECT * FROM sessions.ingest_batches", [])).rejects.toThrow(/permission denied/);
  });

  test("claim, commit, then replay", async () => {
    const db = await database();
    const ledger = sqlLedger(runAs(db));
    const c = claimInput();
    expect(await ledger.claim(c)).toEqual({ kind: "claimed", deleted: [], stale: [], resets: 0 });
    expect(await state(db)).toBe("pending");
    expect(await revisions(db)).toEqual([]);
    expect(await ledger.commit(commitInput(c))).toEqual({ rearmed: 0 });
    expect(await state(db)).toBe("committed");
    expect(await revisions(db)).toEqual([{ record_id: "r1", revision: 1, revision_id: "r1:r1", digest: hex("1") }]);
    expect(await ledger.claim(c)).toEqual({ kind: "replay", receiptId: c.receiptId });
  });

  test("a resume after a failed commit still reports its records as not stale", async () => {
    const db = await database();
    const ledger = sqlLedger(runAs(db));
    const c = claimInput({ records: [rec("r1", 1, "1"), rec("r2", 1, "1")] });
    await ledger.claim(c);
    // The commit never ran (the writer stopped): nothing is stored, so a retry writes again.
    expect(await ledger.claim(c)).toEqual({ kind: "resume", deleted: [], stale: [], resets: 0 });
  });

  test("a record of a still-pending batch is not stale for another batch; after commit it is", async () => {
    const db = await database();
    const ledger = sqlLedger(runAs(db));
    const c = claimInput();
    await ledger.claim(c);
    const other = claimInput({ batchId: "batch-2", batchDigest: hex("b"), records: [rec("r1", 1, "1"), rec("r5", 1, "5")] });
    expect(await ledger.claim(other)).toEqual({ kind: "claimed", deleted: [], stale: [], resets: 0 });
    await ledger.commit(commitInput(c));
    const third = claimInput({ batchId: "batch-3", batchDigest: hex("c"), records: [rec("r5", 1, "5"), rec("r1", 1, "1")] });
    expect(await ledger.claim(third)).toEqual({ kind: "claimed", deleted: [], stale: [1], resets: 0 });
  });

  test("another digest under a batch id is a batch conflict, pending or committed", async () => {
    const db = await database();
    const ledger = sqlLedger(runAs(db));
    const c = claimInput();
    await ledger.claim(c);
    const other = claimInput({ batchDigest: hex("b") });
    expect(await ledger.claim(other)).toEqual({ kind: "batch_conflict", currentDigest: c.batchDigest });
    await ledger.commit(commitInput(c));
    expect(await ledger.claim(other)).toEqual({ kind: "batch_conflict", currentDigest: c.batchDigest });
    expect((await db.query("SELECT * FROM sessions.ingest_batches")).rows.length).toBe(1);
  });

  test("#250: an equal stored revision with another digest is a replay and keeps the stored row", async () => {
    const db = await database();
    const ledger = sqlLedger(runAs(db));
    await land(ledger, claimInput({ records: [rec("r1", 1, "1"), rec("r2", 1, "1")] }));
    const c = claimInput({ batchId: "batch-2", batchDigest: hex("b"), records: [rec("r1", 1, "9", "r1:other"), rec("r3", 1, "3")] });
    expect(await ledger.claim(c)).toEqual({ kind: "claimed", deleted: [], stale: [0], resets: 0 });
    // Even if a caller committed it as written, the equal revision keeps the stored row.
    await ledger.commit(commitInput(c));
    expect(await revisions(db)).toEqual([
      { record_id: "r1", revision: 1, revision_id: "r1:r1", digest: hex("1") },
      { record_id: "r2", revision: 1, revision_id: "r2:r1", digest: hex("1") },
      { record_id: "r3", revision: 1, revision_id: "r3:r1", digest: hex("3") },
    ]);
  });

  test("a lower revision is stale and never moves the record back", async () => {
    const db = await database();
    const ledger = sqlLedger(runAs(db));
    await land(ledger, claimInput({ records: [rec("r1", 5, "5"), rec("r2", 1, "1")] }));
    // Stale even when reused with other content in the batch: no twin 409 below the stored revision.
    const c = claimInput({ batchId: "batch-2", batchDigest: hex("b"), records: [rec("r2", 2, "2"), rec("r1", 3, "3", "x"), rec("r1", 3, "4", "y")] });
    expect(await ledger.claim(c)).toEqual({ kind: "claimed", deleted: [], stale: [1, 2], resets: 0 });
    await ledger.commit(commitInput(c));
    expect((await revisions(db)).map((r) => [r.record_id, r.revision])).toEqual([["r1", 5], ["r2", 2]]);
  });

  test("a higher revision moves the record forward at commit, the highest of the batch", async () => {
    const db = await database();
    const ledger = sqlLedger(runAs(db));
    await land(ledger, claimInput({ records: [rec("r1", 1, "1")] }));
    await land(ledger, claimInput({ batchId: "batch-2", batchDigest: hex("b"), records: [rec("r1", 2, "2"), rec("r1", 4, "4")] }));
    expect(await revisions(db)).toEqual([{ record_id: "r1", revision: 4, revision_id: "r1:r4", digest: hex("4") }]);
  });

  test("a new revision reused in the batch with other content is a 409 against its first peer", async () => {
    const db = await database();
    const ledger = sqlLedger(runAs(db));
    const r = await ledger.claim(claimInput({ records: [rec("r1", 2, "2", "r1:b"), rec("r1", 2, "3", "r1:a")] }));
    expect(r).toEqual({
      kind: "revision_conflict",
      count: 1,
      conflicts: [
        { recordId: "r1", submittedRevisionId: "r1:b", submittedRevision: 2, submittedDigest: hex("2"), currentRevisionId: "r1:a", currentRevision: 2, currentDigest: hex("3") },
      ],
    });
    expect(await state(db)).toBeUndefined();
    // Above a stored lower revision it is still new, so still a 409.
    await land(ledger, claimInput({ batchId: "batch-0", batchDigest: hex("0"), records: [rec("r1", 1, "1")] }));
    expect((await ledger.claim(claimInput({ records: [rec("r1", 2, "2", "r1:b"), rec("r1", 2, "3", "r1:a")] }))).kind).toBe("revision_conflict");
  });

  test("twin conflicts are capped at 100, in identity order, with the total count", async () => {
    const db = await database();
    const ledger = sqlLedger(runAs(db));
    const ids = Array.from({ length: 150 }, (_, i) => `r${String(i).padStart(3, "0")}`);
    const records = [...ids].reverse().flatMap((id) => [rec(id, 1, "1", `${id}:b`), rec(id, 1, "2", `${id}:a`)]);
    const r = await ledger.claim(claimInput({ records: records.slice(0, 300) }));
    if (r.kind !== "revision_conflict") throw new Error(`expected a conflict, got ${r.kind}`);
    expect(r.count).toBe(150);
    expect(r.conflicts.length).toBe(100);
    expect(r.conflicts.map((c) => c.recordId)).toEqual(ids.slice(0, 100));
  });

  test("replay_fast: every record stale or deleted commits the batch in one call, and a re-send replays", async () => {
    const db = await database();
    const ledger = sqlLedger(runAs(db));
    await land(ledger, claimInput({ records: [rec("r1", 3, "3"), rec("r2", 1, "1")] }));
    await db.query(`INSERT INTO sessions.upload_deletions VALUES ('org-a', 'ws-a', 'user-1', 'origin-1', 'claude', 's-9', now())`);
    const old = claimInput({
      batchId: "batch-old",
      batchDigest: hex("d"),
      sessions: [["claude", "s-1"], ["claude", "s-9"]],
      records: [rec("r1", 2, "7"), rec("r2", 1, "8"), rec("r7", 1, "7", "r7:r1", ["claude", "s-9"])],
    });
    let calls = 0;
    const counting = sqlLedger(async (text, params) => {
      calls++;
      return runAs(db)(text, params);
    });
    expect(await counting.claim(old)).toEqual({ kind: "replay_fast", deleted: [["claude", "s-9"]], stale: [0, 1], resets: 0 });
    expect(calls).toBe(1);
    expect(await state(db, "batch-old")).toBe("committed");
    expect(await ledger.claim(old)).toEqual({ kind: "replay", receiptId: old.receiptId });
    // Nothing moved.
    expect((await revisions(db)).map((r) => [r.record_id, r.revision])).toEqual([["r1", 3], ["r2", 1]]);
  });

  test("a resumed pending batch whose records were committed meanwhile turns into replay_fast", async () => {
    const db = await database();
    const ledger = sqlLedger(runAs(db));
    const c = claimInput();
    await ledger.claim(c);
    await land(ledger, claimInput({ batchId: "batch-2", batchDigest: hex("b") }));
    expect(await ledger.claim(c)).toEqual({ kind: "replay_fast", deleted: [], stale: [0], resets: 0 });
    expect(await state(db)).toBe("committed");
  });

  test("an empty batch (every record retired) is replay_fast", async () => {
    const db = await database();
    const ledger = sqlLedger(runAs(db));
    expect(await ledger.claim(claimInput({ records: [], sessions: [] }))).toEqual({ kind: "replay_fast", deleted: [], stale: [], resets: 0 });
  });

  test("the batch's deleted sessions come back; other users' and origins' tombstones do not", async () => {
    const db = await database();
    await db.query(
      `INSERT INTO sessions.upload_deletions (org_id, workspace_id, user_id, origin_id, source, session_id) VALUES
         ('org-a', 'ws-a', 'user-1', 'origin-1', 'claude', 's-2'),
         ('org-a', 'ws-a', 'user-2', 'origin-1', 'claude', 's-1'),
         ('org-a', 'ws-a', 'user-1', 'origin-2', 'claude', 's-1')`,
    );
    const ledger = sqlLedger(runAs(db));
    const r = await ledger.claim(claimInput({ sessions: [["claude", "s-1"], ["claude", "s-2"], ["codex", "s-2"]] }));
    expect(r).toEqual({ kind: "claimed", deleted: [["claude", "s-2"]], stale: [], resets: 0 });
  });

  test("records of a deleted session are not judged: a twin there is no 409", async () => {
    const db = await database();
    await db.query(`INSERT INTO sessions.upload_deletions VALUES ('org-a', 'ws-a', 'user-1', 'origin-1', 'claude', 's-2', now())`);
    const ledger = sqlLedger(runAs(db));
    const c = claimInput({
      sessions: [["claude", "s-1"], ["claude", "s-2"]],
      records: [rec("r1", 1, "1"), rec("r9", 1, "1", "r9:a", ["claude", "s-2"]), rec("r9", 1, "2", "r9:b", ["claude", "s-2"])],
    });
    expect(await ledger.claim(c)).toEqual({ kind: "claimed", deleted: [["claude", "s-2"]], stale: [], resets: 0 });
    // A record with no session is judged as before.
    const none = claimInput({ batchId: "batch-3", batchDigest: hex("c"), sessions: [], records: [rec("r8", 1, "1", "a", ["claude", null]), rec("r8", 1, "2", "b", ["claude", null])] });
    expect((await ledger.claim(none)).kind).toBe("revision_conflict");
  });

  test("claim reports the user's reset count", async () => {
    const db = await database();
    await db.query(`INSERT INTO sessions.upload_resets (org_id, workspace_id, user_id, request_id) VALUES
      ('org-a', 'ws-a', 'user-1', 'q1'), ('org-a', 'ws-a', 'user-1', 'q2'), ('org-a', 'ws-a', 'user-2', 'q3')`);
    const ledger = sqlLedger(runAs(db));
    expect(await ledger.claim(claimInput())).toEqual({ kind: "claimed", deleted: [], stale: [], resets: 2 });
  });

  test("a tombstone after the claim re-arms erasure at commit, once", async () => {
    const db = await database();
    const ledger = sqlLedger(runAs(db));
    const c = claimInput({ sessions: [["claude", "s-1"], ["claude", "s-2"]] });
    await ledger.claim(c);
    await db.query(`INSERT INTO sessions.upload_deletions VALUES ('org-a', 'ws-a', 'user-1', 'origin-1', 'claude', 's-2', now())`);
    expect(await ledger.commit(commitInput(c))).toEqual({ rearmed: 1 });
    expect((await db.query("SELECT machine_id, source, session_id FROM sessions.upload_erasure_rearm")).rows).toEqual([
      { machine_id: MACHINE, source: "claude", session_id: "s-2" },
    ]);
    expect(await ledger.commit(commitInput(c))).toEqual({ rearmed: 1 });
    expect((await db.query("SELECT * FROM sessions.upload_erasure_rearm")).rows.length).toBe(1);
    expect((await revisions(db)).length).toBe(1);
  });

  test("a reset after the claim re-arms every written session; one before it does not", async () => {
    const db = await database();
    await db.query(`INSERT INTO sessions.upload_resets (org_id, workspace_id, user_id, request_id, reset_at) VALUES ('org-a', 'ws-a', 'user-1', 'old', now() - interval '1 hour')`);
    const ledger = sqlLedger(runAs(db));
    const c = claimInput({ sessions: [["claude", "s-1"], ["claude", "s-2"]] });
    await ledger.claim(c);
    expect(await ledger.commit(commitInput(c))).toEqual({ rearmed: 0 });
    const c2 = claimInput({ batchId: "batch-2", batchDigest: hex("b"), records: [rec("r2", 1, "1")], sessions: [["claude", "s-1"], ["claude", "s-2"]] });
    await ledger.claim(c2);
    await db.query(`UPDATE sessions.ingest_batches SET claimed_at = now() - interval '1 minute' WHERE batch_id = 'batch-2'`);
    await db.query(`INSERT INTO sessions.upload_resets (org_id, workspace_id, user_id, request_id) VALUES ('org-a', 'ws-a', 'user-1', 'new')`);
    expect(await ledger.commit(commitInput(c2))).toEqual({ rearmed: 2 });
  });

  test("commit without a claim for this digest is refused", async () => {
    const db = await database();
    const ledger = sqlLedger(runAs(db));
    const c = claimInput();
    await expect(ledger.commit(commitInput(c))).rejects.toMatchObject({ code: "ledger_no_claim" });
    await ledger.claim(c);
    await expect(ledger.commit({ ...commitInput(c), batchDigest: hex("b") })).rejects.toMatchObject({ code: "ledger_no_claim" });
    expect(await revisions(db)).toEqual([]);
  });

  test("slots: limited per user and per org, refreshed by their holder, freed by release and by TTL", async () => {
    const db = await database();
    const ledger = sqlLedger(runAs(db));
    const req = (holder: string, userId = "user-1", over: Partial<{ maxPerUser: number; maxPerOrg: number; ttlSeconds: number }> = {}) => ({
      tenant: { ...tenant, userId },
      holder,
      maxPerUser: 2,
      maxPerOrg: 3,
      ttlSeconds: 90,
      ...over,
    });
    expect(await ledger.acquireSlot(req("h1"))).toEqual({ ok: true });
    expect(await ledger.acquireSlot(req("h2"))).toEqual({ ok: true });
    const busy = await ledger.acquireSlot(req("h3"));
    expect(busy.ok).toBe(false);
    if (!busy.ok) expect(busy.retryAfterSeconds).toBeGreaterThanOrEqual(2);
    if (!busy.ok) expect(busy.retryAfterSeconds).toBeLessThanOrEqual(10);
    // The holder itself re-acquires (a retry) at the limit.
    expect(await ledger.acquireSlot(req("h2"))).toEqual({ ok: true });
    // Per org: a second user takes the third org slot, a fourth is refused.
    expect(await ledger.acquireSlot(req("h4", "user-2"))).toEqual({ ok: true });
    expect((await ledger.acquireSlot(req("h5", "user-3"))).ok).toBe(false);
    // Another org is not affected.
    expect(await ledger.acquireSlot({ ...req("h6"), tenant: { ...tenant, orgId: "org-b" } })).toEqual({ ok: true });
    await ledger.releaseSlot(tenant, "h1");
    expect(await ledger.acquireSlot(req("h3"))).toEqual({ ok: true });
    // A crashed holder's lease expires.
    await db.query("UPDATE sessions.ingest_tenant_slots SET expires_at = now() - interval '1 second' WHERE holder = 'h2'");
    await ledger.releaseSlot(tenant, "h3");
    expect(await ledger.acquireSlot(req("h7"))).toEqual({ ok: true });
    expect((await db.query("SELECT holder FROM sessions.ingest_tenant_slots WHERE holder = 'h2'")).rows).toEqual([]);
    await expect(ledger.acquireSlot(req("h8", "user-1", { ttlSeconds: 0 }))).rejects.toMatchObject({ code: "ledger_input_refused" });
  });

  test("seed: only the seeder role, keeps the max, never lowers, equal keeps the stored row", async () => {
    const db = await database();
    const ledger = sqlLedger(runAs(db));
    await land(ledger, claimInput({ records: [rec("r1", 3, "3"), rec("r2", 1, "1")] }));
    const row = (record_id: string, revision: number, d: string) => ({
      org_id: "org-a", workspace_id: "ws-a", origin_id: "origin-1", record_id, revision, revision_id: `${record_id}:seed${revision}`, digest: hex(d),
    });
    const seed = [row("r1", 2, "a"), row("r2", 1, "b"), row("r2", 4, "c"), row("r3", 1, "d"), row("r3", 2, "e")];
    const call = (role: string) => runAs(db, role)("SELECT sessions.ingest_seed_records($1::jsonb) AS r", [JSON.stringify(seed)]);
    await expect(call("bulls96_session_ingest")).rejects.toThrow(/permission denied|caller/);
    const r = await call("bulls96_session_ingest_seeder");
    expect(r.rows[0]!.r).toEqual({ upserted: 2 });
    expect(await revisions(db)).toEqual([
      { record_id: "r1", revision: 3, revision_id: "r1:r3", digest: hex("3") },
      { record_id: "r2", revision: 4, revision_id: "r2:seed4", digest: hex("c") },
      { record_id: "r3", revision: 2, revision_id: "r3:seed2", digest: hex("e") },
    ]);
    // Seeding again changes nothing; an equal revision with another digest keeps the row.
    const again = await runAs(db, "bulls96_session_ingest_seeder")("SELECT sessions.ingest_seed_records($1::jsonb) AS r", [JSON.stringify([row("r3", 2, "f")])]);
    expect(again.rows[0]!.r).toEqual({ upserted: 0 });
    expect((await revisions(db))[2]!.digest).toBe(hex("e"));
  });

  test("another role is refused even with EXECUTE", async () => {
    const db = await database();
    await db.exec("GRANT USAGE ON SCHEMA sessions TO service_role; GRANT EXECUTE ON FUNCTION sessions.ingest_claim_batch(jsonb) TO service_role");
    await expect(sqlLedger(runAs(db, "service_role")).claim(claimInput())).rejects.toMatchObject({ code: "ledger_caller_refused" });
    await db.exec("GRANT EXECUTE ON FUNCTION sessions.ingest_claim_batch(jsonb) TO bulls96_session_ingest_seeder");
    await expect(sqlLedger(runAs(db, "bulls96_session_ingest_seeder")).claim(claimInput())).rejects.toMatchObject({ code: "ledger_caller_refused" });
  });

  test("malformed input is refused with 22023 and writes nothing", async () => {
    const db = await database();
    const ledger = sqlLedger(runAs(db));
    const bad: unknown[] = [
      { ...claimInput(), extra: 1 },
      { ...claimInput(), batchDigest: "nope" },
      { ...claimInput(), receiptId: "rhr_x" },
      { ...claimInput(), tenant: { ...tenant, orgId: "" } },
      { ...claimInput(), originId: "a\u0001b" },
      { ...claimInput(), originId: "x".repeat(513) },
      { ...claimInput(), records: [{ ...rec("r1", 1, "1"), revision: 0 }] },
      { ...claimInput(), records: [{ ...rec("r1", 1, "1"), revision: 1.5 }] },
      { ...claimInput(), records: [{ ...rec("r1", 1, "1"), revision: "1" }] },
      { ...claimInput(), records: [{ ...rec("r1", 1, "1"), digest: "z" }] },
      { ...claimInput(), records: [{ ...rec("r1", 1, "1"), source: "" }] },
      { ...claimInput(), records: [{ ...rec("r1", 1, "1"), sessionId: 7 }] },
      { ...claimInput(), records: [{ recordId: "r1", revisionId: "x", revision: 1, digest: hex("1") }] },
      { ...claimInput(), sessions: [["claude"]] },
      { ...claimInput(), sessions: [["claude", "s-1"], ["claude", "s-1"]] },
    ];
    for (const input of bad) await expect(ledger.claim(input as ClaimInput)).rejects.toMatchObject({ code: "ledger_input_refused" });
    await ledger.claim(claimInput());
    await expect(ledger.commit({ ...commitInput(claimInput()), records: [{ ...rec("r1", 1, "1"), revision: -1 }] })).rejects.toMatchObject({ code: "ledger_input_refused" });
    expect((await db.query("SELECT * FROM sessions.ingest_batches")).rows.length).toBe(1);
    expect(await revisions(db)).toEqual([]);
  });

  test("a workspace may be empty", async () => {
    const db = await database();
    const ledger = sqlLedger(runAs(db));
    expect((await ledger.claim(claimInput({ tenant: { ...tenant, workspaceId: "" } }))).kind).toBe("claimed");
  });

  test("a driver failure is a code, never its message", async () => {
    const ledger = sqlLedger(async () => {
      throw Object.assign(new Error("connection to secret-host failed with token abc"), { code: "ECONNRESET" });
    });
    const error = await ledger.claim(claimInput()).catch((e) => e);
    expect(error).toBeInstanceOf(LedgerUnavailableError);
    expect(error.code).toBe("ledger_connection");
    expect(String(error.message)).not.toContain("secret-host");
  });
});
