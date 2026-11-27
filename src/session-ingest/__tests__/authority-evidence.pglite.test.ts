/** AUTHORITY_SQL against a real Postgres engine (pglite), over the three ledger tables' released column shapes. */
import { expect, setDefaultTimeout, test } from "bun:test";

setDefaultTimeout(30_000);
import { PGlite } from "@electric-sql/pglite";
import {
  AUTHORITY_SQL, AUTHORITY_VISIBILITY_SQL, authorityBaseline, authorityEvidence, evidenceHash, type AuthorityEvidenceSources,
} from "../authority-evidence.ts";

const ORG = "11111111-2222-4333-8444-555555555555", OTHER = "66666666-7777-4888-9999-aaaaaaaaaaaa";
const STOP = Date.parse("2026-11-26T10:00:00Z"), HOUR = 3_600_000;
const ts = (ms: number) => new Date(ms).toISOString();

async function db() {
  const pg = new PGlite();
  await pg.exec(`
    CREATE SCHEMA sessions;
    CREATE TABLE sessions.ingest_batches (org_id text NOT NULL, workspace_id text NOT NULL, origin_id text NOT NULL, batch_id text NOT NULL,
      user_id text NOT NULL, state text NOT NULL CHECK (state IN ('pending', 'committed')), claimed_at timestamptz NOT NULL DEFAULT now(),
      committed_at timestamptz, PRIMARY KEY (org_id, workspace_id, origin_id, batch_id));
    CREATE TABLE sessions.ingest_record_revisions (org_id text NOT NULL, workspace_id text NOT NULL, origin_id text NOT NULL, record_id text NOT NULL,
      revision bigint NOT NULL, revision_id text NOT NULL, digest text NOT NULL, user_id text NOT NULL, source text NOT NULL, session_id text,
      updated_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY (org_id, workspace_id, origin_id, record_id));
    CREATE TABLE sessions.ingest_session_ownership (org_id text NOT NULL, workspace_id text NOT NULL, user_id text NOT NULL, origin_id text NOT NULL,
      source text NOT NULL, session_id text NOT NULL, PRIMARY KEY (org_id, workspace_id, user_id, origin_id, source, session_id));
  `);
  return pg;
}
const read = async (pg: PGlite, org = ORG, at = STOP, base: string | null = null) =>
  (await pg.query<Record<string, unknown>>(AUTHORITY_SQL, [org, String(at), base])).rows;

test("empty org: one row of zeros", async () => {
  const pg = await db();
  const rows = await read(pg);
  expect(rows).toHaveLength(1);
  expect(rows[0]).toMatchObject({
    visible_tables: 3, tables: 3, batches_claimed_since: 0, batches_committed_since: 0, batches_pending: 0, batches_new_since_base: 0,
    revisions_updated_since: 0, revisions_count: 0, revisions_new_since_base: 0, ownership_count: 0, ownership_new_since_base: 0,
    isolation: "read committed", read_only: "off",
  });
  expect(rows[0]!.revisions_digest).toMatch(/^h[0-9a-f]{32}$/u);
  expect(rows[0]!.snapshot).toMatch(/^\d+:\d+:/u);
});

test("counts since the anchor (inclusive), the org only; hashes move on a rewrite and stay put otherwise", async () => {
  const pg = await db();
  await pg.query(`INSERT INTO sessions.ingest_batches VALUES
    ($1,'w','o','b1','u','committed',$3,$3), ($1,'w','o','b2','u','committed',$3,$4), ($1,'w','o','b3','u','pending',$4,NULL),
    ($1,'w','o','b4','u','committed',$5,$5), ($2,'w','o','b5','u','pending',$4,NULL)`,
    [ORG, OTHER, ts(STOP - 60_000), ts(STOP), ts(STOP + 1)]);
  await pg.query(`INSERT INTO sessions.ingest_record_revisions VALUES
    ($1,'w','o','r1',1,'v1','${"a".repeat(64)}','u','s','sess',$3), ($1,'w','o','r2',1,'v1','${"b".repeat(64)}','u','s',NULL,$4),
    ($2,'w','o','r3',1,'v1','${"c".repeat(64)}','u','s','sess',$4)`, [ORG, OTHER, ts(STOP - 1), ts(STOP)]);
  await pg.query(`INSERT INTO sessions.ingest_session_ownership VALUES ($1,'w','u','o','s','sess'), ($2,'w','u','o','s','sess')`, [ORG, OTHER]);
  const [r] = await read(pg);
  expect(Object.fromEntries(Object.entries(r!).map(([k, v]) => [k, typeof v]))).toMatchObject({ batches_claimed_since: "number" });
  expect(r).toMatchObject({
    batches_claimed_since: 2, // b3 (at STOP) and b4 (after)
    batches_committed_since: 2, // b2 (at STOP) and b4
    batches_pending: 1,
    revisions_updated_since: 1, // r2 at STOP; r1 one ms before
    revisions_count: 2,
    ownership_count: 1,
  });
  expect((await read(pg))[0]!.revisions_digest).toBe(r!.revisions_digest);
  // Another org's change moves nothing; this org's rewrite moves its hash.
  await pg.query(`UPDATE sessions.ingest_record_revisions SET revision = 9 WHERE org_id = $1`, [OTHER]);
  expect((await read(pg))[0]!.revisions_digest).toBe(r!.revisions_digest);
  await pg.query(`UPDATE sessions.ingest_record_revisions SET revision = 2 WHERE org_id = $1 AND record_id = 'r1'`, [ORG]);
  expect((await read(pg))[0]!.revisions_digest).not.toBe(r!.revisions_digest);
  const own0 = (await read(pg))[0]!.ownership_digest;
  await pg.query(`UPDATE sessions.ingest_session_ownership SET session_id = 'other' WHERE org_id = $1`, [ORG]);
  const own1 = (await read(pg))[0]!;
  expect(own1.ownership_count).toBe(1);
  expect(own1.ownership_digest).not.toBe(own0);
});

test("read-only: runs inside a READ ONLY transaction", async () => {
  const pg = await db();
  await pg.exec("BEGIN TRANSACTION READ ONLY");
  expect(await read(pg)).toHaveLength(1);
  await pg.exec("ROLLBACK");
});

const visibility = async (pg: PGlite) =>
  Object.fromEntries((await pg.query<{ table_name: string; visible: boolean }>(AUTHORITY_VISIBILITY_SQL)).rows.map((r) => [r.table_name, r.visible]));

test("review 4: RLS filtering is detected, not read as an empty org", async () => {
  const pg = await db();
  await pg.query(`INSERT INTO sessions.ingest_batches VALUES ($1,'w','o','b1','u','pending',now(),NULL)`, [ORG]);
  await pg.exec(`
    ALTER TABLE sessions.ingest_batches ENABLE ROW LEVEL SECURITY;
    CREATE ROLE reader; CREATE ROLE bypass BYPASSRLS; CREATE ROLE noselect;
    GRANT USAGE ON SCHEMA sessions TO reader, bypass, noselect;
    GRANT SELECT ON ALL TABLES IN SCHEMA sessions TO reader, bypass;
    GRANT SELECT ON sessions.ingest_batches, sessions.ingest_record_revisions TO noselect;
  `);
  // The owner sees every row; RLS does not apply to it.
  expect(await visibility(pg)).toEqual({ ingest_batches: true, ingest_record_revisions: true, ingest_session_ownership: true });
  expect(Number((await read(pg))[0]!.batches_pending)).toBe(1);
  // The hazard: an RLS-filtered reader gets 0 for the same row...
  await pg.exec("SET ROLE reader");
  expect(Number((await read(pg))[0]!.batches_pending)).toBe(0);
  // ...and the visibility check names exactly that table.
  expect(await visibility(pg)).toEqual({ ingest_batches: false, ingest_record_revisions: true, ingest_session_ownership: true });
  await pg.exec("RESET ROLE; SET ROLE bypass");
  expect(await visibility(pg)).toEqual({ ingest_batches: true, ingest_record_revisions: true, ingest_session_ownership: true });
  expect(Number((await read(pg))[0]!.batches_pending)).toBe(1);
  await pg.exec("RESET ROLE; SET ROLE noselect");
  expect((await visibility(pg)).ingest_session_ownership).toBe(false);
  // A superuser bypasses RLS even when FORCE'd: visible, and the engine agrees.
  await pg.exec("RESET ROLE; ALTER TABLE sessions.ingest_batches FORCE ROW LEVEL SECURITY");
  expect((await visibility(pg)).ingest_batches).toBe(true);
  expect(Number((await read(pg))[0]!.batches_pending)).toBe(1);
  // A non-superuser owner: FORCE applies RLS to it (filtered and reported so); without FORCE it sees every row.
  await pg.exec("CREATE ROLE tabowner; GRANT USAGE ON SCHEMA sessions TO tabowner; GRANT SELECT ON ALL TABLES IN SCHEMA sessions TO tabowner; ALTER TABLE sessions.ingest_batches OWNER TO tabowner; SET ROLE tabowner");
  expect((await visibility(pg)).ingest_batches).toBe(false);
  expect(Number((await read(pg))[0]!.batches_pending)).toBe(0);
  await pg.exec("ALTER TABLE sessions.ingest_batches NO FORCE ROW LEVEL SECURITY");
  expect((await visibility(pg)).ingest_batches).toBe(true);
  expect(Number((await read(pg))[0]!.batches_pending)).toBe(1);
  // A member inheriting the owner's privileges is the owner for RLS (check_enable_rls uses has_privs_of_role).
  await pg.exec("RESET ROLE; CREATE ROLE member IN ROLE tabowner; SET ROLE member");
  expect((await visibility(pg)).ingest_batches).toBe(true);
  expect(Number((await read(pg))[0]!.batches_pending)).toBe(1);
});

test("visibility answers exactly the three tables (a missing table shows as a missing row)", async () => {
  const pg = await db();
  await pg.exec("DROP TABLE sessions.ingest_session_ownership");
  expect(Object.keys(await visibility(pg))).toEqual(["ingest_batches", "ingest_record_revisions"]);
});

test("review 5c067d90 #3: delete and restore leaves every count and digest equal, and is still caught by xmin", async () => {
  const pg = await db();
  await pg.query(`INSERT INTO sessions.ingest_session_ownership VALUES ($1,'w','u','o','s','sess'), ($1,'w','u','o','s','other')`, [ORG]);
  await pg.query(`INSERT INTO sessions.ingest_record_revisions VALUES ($1,'w','o','r1',1,'v1','${"a".repeat(64)}','u','s','sess',$2)`, [ORG, ts(STOP - HOUR)]);
  const base = (await read(pg))[0]!;
  // The intervening backfill would read the table without the row...
  await pg.query(`DELETE FROM sessions.ingest_session_ownership WHERE org_id = $1 AND session_id = 'sess'`, [ORG]);
  expect(Number((await read(pg))[0]!.ownership_count)).toBe(1);
  // ...then it is restored before the bound run.
  await pg.query(`INSERT INTO sessions.ingest_session_ownership VALUES ($1,'w','u','o','s','sess')`, [ORG]);
  const now = (await read(pg, ORG, STOP, base.snapshot as string))[0]!;
  expect(now.ownership_count).toBe(base.ownership_count);
  expect(now.ownership_digest).toBe(base.ownership_digest); // the endpoint fingerprint alone would pass
  expect(Number(now.ownership_new_since_base)).toBe(1);   // the row version cannot
  expect(Number(now.revisions_new_since_base)).toBe(0);
  // An update back to the same value is a new version too.
  await pg.query(`UPDATE sessions.ingest_record_revisions SET revision = revision WHERE org_id = $1`, [ORG]);
  expect(Number((await read(pg, ORG, STOP, base.snapshot as string))[0]!.revisions_new_since_base)).toBe(1);
  // Untouched rows, and another org's writes, are not counted.
  const later = (await read(pg))[0]!;
  await pg.query(`INSERT INTO sessions.ingest_session_ownership VALUES ($1,'w','u','o','s','sess')`, [OTHER]);
  const r = (await read(pg, ORG, STOP, later.snapshot as string))[0]!;
  expect([Number(r.ownership_new_since_base), Number(r.revisions_new_since_base), Number(r.batches_new_since_base)]).toEqual([0, 0, 0]);
});

test("review 5c067d90 #4: tuples that a tab-joined encoding confuses have different digests", async () => {
  const a = await db(), b = await db();
  await a.query(`INSERT INTO sessions.ingest_session_ownership VALUES ($1, E'w\tu', 'x', 'o', 's', 'sess')`, [ORG]);
  await b.query(`INSERT INTO sessions.ingest_session_ownership VALUES ($1, 'w', E'u\tx', 'o', 's', 'sess')`, [ORG]);
  expect((await read(a))[0]!.ownership_digest).not.toBe((await read(b))[0]!.ownership_digest);
  // NULL and the string 'null' / '~' are distinct too.
  const c = await db(), d = await db();
  await c.query(`INSERT INTO sessions.ingest_record_revisions VALUES ($1,'w','o','r',1,'v','${"a".repeat(64)}','u','s',NULL,$2)`, [ORG, ts(STOP - HOUR)]);
  await d.query(`INSERT INTO sessions.ingest_record_revisions VALUES ($1,'w','o','r',1,'v','${"a".repeat(64)}','u','s','~',$2)`, [ORG, ts(STOP - HOUR)]);
  expect((await read(c))[0]!.revisions_digest).not.toBe((await read(d))[0]!.revisions_digest);
});

test("end to end on the engine: baseline in a REPEATABLE READ transaction, then a bound run; a restore in between refuses", async () => {
  const pg = await db();
  await pg.query(`INSERT INTO sessions.ingest_session_ownership VALUES ($1,'w','u','o','s','sess')`, [ORG]);
  // Real time: the baseline's read_at_ms is the engine's statement_timestamp().
  const T = Date.now() - 3_600_000, NOWMS = Date.now();
  const uuid = (n: number) => `00000000-0000-4000-8000-${String(n).padStart(12, "0")}`;
  const Q = { name: "INGEST_ADMISSION", type: "queue", queue_name: "96bulls-session-ingest-admission" };
  const versions: Record<string, unknown[]> = {
    [uuid(1)]: [{ name: "INGEST_HTTP_ADMISSION_STOPPED_ORGS", type: "plain_text", text: ORG }, { name: "INGEST_S3_FENCE_MIRROR", type: "plain_text", text: "on" }, Q],
    [uuid(2)]: [Q], [uuid(3)]: [],
  };
  const consumer = { type: "worker", script_name: "96bulls-session-ingest", dead_letter_queue: "96bulls-session-ingest-admission-dlq" };
  const main = { queue_id: "a".repeat(32), queue_name: "96bulls-session-ingest-admission", consumers: [consumer], consumers_total_count: 1,
    producers: [{ type: "worker", script: "96bulls-session-ingest" }], producers_total_count: 1 };
  const dlq = { queue_id: "b".repeat(32), queue_name: "96bulls-session-ingest-admission-dlq", consumers: [], consumers_total_count: 0, producers: [], producers_total_count: 0 };
  let clock = NOWMS;
  const result = async (path: string): Promise<unknown> => {
    {
      if (path.endsWith("96bulls-session-ingest/deployments")) return { deployments: [
        { id: uuid(101), created_on: new Date(T).toISOString(), versions: [{ version_id: uuid(1), percentage: 100 }] },
        { id: uuid(102), created_on: new Date(T - 3_600_000).toISOString(), versions: [{ version_id: uuid(2), percentage: 100 }] }] };
      if (path.endsWith("relayhistory-api/deployments")) return { deployments: [
        { id: uuid(201), created_on: new Date(T).toISOString(), versions: [{ version_id: uuid(3), percentage: 100 }] }] };
      const v = /versions\/(.+)$/u.exec(path);
      if (v) return { id: v[1], resources: { bindings: versions[v[1]!] } };
      if (path.startsWith("/queues?")) return [main, dlq];
      if (path.endsWith("/metrics")) return { backlog_count: 0 };
      if (path === `/queues/${main.queue_id}`) return main;
      if (path === `/queues/${dlq.queue_id}`) return dlq;
      throw new Error(path);
    }
  };
  const src: AuthorityEvidenceSources = {
    async cfGet(path) {
      const r = await result(path);
      return { success: true, result: r, ...(path.startsWith("/queues?") ? { result_info: { page: 1, total_count: 2, total_pages: 1 } } : {}) };
    },
    // Each round in its own READ ONLY transaction, as the caller runs it.
    async sql(text, params) {
      await pg.exec("BEGIN TRANSACTION READ ONLY");
      try { return (await pg.query<Record<string, unknown>>(text, params as unknown[])).rows; } finally { await pg.exec("ROLLBACK"); }
    },
    nowMs: () => clock,
    // Test fixture only: the sleep FORCES a statistics flush, so the counter read after it is deterministic here. The
    // runtime does not force one; it only waits AUTHORITY_STATS_SETTLE_MS, a heuristic (see the module).
    async sleep(ms) { clock += ms; await pg.exec("SELECT pg_stat_force_next_flush()"); await pg.exec("SELECT 1"); },
  };
  const opts = { observationWindowMs: 30_000 };
  const unbound = await authorityEvidence([ORG], src, opts);
  expect(unbound.status).toBe("authority_evidence_unbound");
  // The baseline, in the backfill's REPEATABLE READ READ ONLY snapshot.
  await pg.exec("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY");
  const baseline = await authorityBaseline([ORG], unbound, async (t, p) => (await pg.query<Record<string, unknown>>(t, p as unknown[])).rows);
  await pg.exec("COMMIT");
  expect(baseline.status).toBe("authority_baseline_ready");
  expect(typeof baseline.orgs[0]!.stats?.batchesDeleted).toBe("number");
  expect(baseline.orgs[0]!.orgHash).toBe(evidenceHash("org", ORG));
  // A READ COMMITTED baseline is refused.
  await pg.exec("BEGIN TRANSACTION READ ONLY");
  const rc = await authorityBaseline([ORG], unbound, async (t, p) => (await pg.query<Record<string, unknown>>(t, p as unknown[])).rows);
  await pg.exec("COMMIT");
  expect(rc.orgs[0]!.refusals).toEqual(["baseline_not_snapshot"]);
  // Nothing written since: the bound run passes on the real engine...
  const passed = await authorityEvidence([ORG], src, { ...opts, bound: baseline });
  expect(passed.orgs[0]!.refusals).toEqual([]);
  expect(passed.status).toBe("authority_evidence_corroborated");
  // ...a delete-and-restore since the baseline refuses.
  await pg.query(`DELETE FROM sessions.ingest_session_ownership WHERE org_id = $1`, [ORG]);
  await pg.query(`INSERT INTO sessions.ingest_session_ownership VALUES ($1,'w','u','o','s','sess')`, [ORG]);
  const after = await authorityEvidence([ORG], src, { ...opts, bound: baseline });
  expect(after.status).toBe("authority_evidence_refused");
  expect(after.orgs[0]!.refusals).toEqual(["sql_rows_rewritten"]);

  // Review 949a2a62 #1: a fresh baseline, then a pending admission committed and deleted before the bound run. Every
  // surviving count, digest and row version is unchanged; the deletion counter is not.
  await pg.exec("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY");
  const fresh = await authorityBaseline([ORG], unbound, async (t, p) => (await pg.query<Record<string, unknown>>(t, p as unknown[])).rows);
  await pg.exec("COMMIT");
  expect(fresh.status).toBe("authority_baseline_ready");
  expect((await authorityEvidence([ORG], src, { ...opts, bound: fresh })).status).toBe("authority_evidence_corroborated");
  await pg.query(`INSERT INTO sessions.ingest_batches VALUES ($1,'w','o','transient','u','pending',now(),NULL)`, [ORG]);
  await pg.query(`DELETE FROM sessions.ingest_batches WHERE batch_id = 'transient'`);
  const transient = await authorityEvidence([ORG], src, { ...opts, bound: fresh });
  expect(transient.status).toBe("authority_evidence_refused");
  expect(transient.orgs[0]!.refusals).toEqual(["sql_batches_deleted"]);
  expect(transient.orgs[0]!.sqlQuiet.counts).toMatchObject({ batchesClaimedSince: 0, batchesPending: 0 });
  // And outside a READ ONLY transaction the evidence run refuses.
  const rw = await authorityEvidence([ORG], { ...src, sql: async (t, p) => (await pg.query<Record<string, unknown>>(t, p as unknown[])).rows }, opts);
  expect(rw.orgs[0]!.refusals).toContain("sql_not_read_only");
});

test("review 949a2a62 #2: a row written in a subtransaction of a then in-progress parent counts as new", async () => {
  const pg = await db();
  await pg.exec("BEGIN");
  const parent = Number((await pg.query<{ x: string }>("SELECT pg_current_xact_id()::text AS x")).rows[0]!.x);
  await pg.exec("SAVEPOINT a");
  await pg.query(`INSERT INTO sessions.ingest_session_ownership VALUES ($1,'w','u','o','s','sess')`, [ORG]);
  await pg.exec("RELEASE a; COMMIT");
  const rowXmin = Number((await pg.query<{ x: string }>("SELECT xmin::text AS x FROM sessions.ingest_session_ownership")).rows[0]!.x);
  expect(rowXmin).toBeGreaterThan(parent); // the tuple carries the subxid
  // A base snapshot taken while the parent was still running: the parent is in xip, xmax beyond both.
  const base = `${parent}:${rowXmin + 1}:${parent}`;
  const check = (await pg.query<{ sub: boolean; top: boolean }>(
    `SELECT pg_visible_in_snapshot($1::text::xid8, $3::pg_snapshot) AS sub, pg_visible_in_snapshot($2::text::xid8, $3::pg_snapshot) AS top`,
    [String(rowXmin), String(parent), base])).rows[0]!;
  expect(check).toEqual({ sub: true, top: false }); // the visibility test alone would call the row old
  expect(Number((await read(pg, ORG, STOP, base))[0]!.ownership_new_since_base)).toBe(1);
  // A snapshot taken after the parent finished does not count it.
  const after = (await read(pg))[0]!.snapshot as string;
  expect(Number((await read(pg, ORG, STOP, after))[0]!.ownership_new_since_base)).toBe(0);
});
