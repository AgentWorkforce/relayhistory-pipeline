/** 062040 summary regeneration: audit table grants, append-only, forget, retention purge, CHECKs; brief columns; 0006. */
import { afterAll, beforeAll, describe, expect, setDefaultTimeout, test } from "bun:test";
import { PGlite, type Transaction } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/index.js";
import { btree_gin } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/contrib/btree_gin.js";
import { pg_trgm } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/contrib/pg_trgm.js";
import { runMigrations, type MigrationClient, type MigrationProfile } from "../migrate.ts";
import { MIGRATION_SENTINELS } from "../migration-sentinels.ts";

setDefaultTimeout(300_000);
const LOCAL: MigrationProfile = { mode: "local", shim: true, bootstrap: true, verify: false, runtimeRole: "postgres" };

function client(db: PGlite): MigrationClient {
  return { async query<R>(text: string, params?: unknown[]) {
    if (params?.length) return (await db.query<R>(text, params)) as { rows: R[] };
    const results = await db.exec(text);
    return { rows: (results.at(-1)?.rows ?? []) as R[] };
  } };
}
const code = async (p: Promise<unknown>) =>
  ((await p.then(() => null, (e: unknown) => e)) as { code?: string } | null)?.code ?? null;

const AUDIT_COLS = "org_id, workspace_id, harness, session_id, system, kind, reason";
type Row = { org?: string; ws?: string; harness?: string; session: string; system?: string; kind?: string; reason?: string; noPointer?: boolean };
const STEP = "upstream:20261105062040_summary_regeneration.sql";

describe("20261105062040 summary regeneration", () => {
  let db: PGlite;
  const as = <R,>(role: string, run: (tx: Transaction) => Promise<R>) =>
    db.transaction(async (tx) => { await tx.exec(`SET LOCAL ROLE ${role}`); return run(tx); });
  // The writer path: the SECURITY DEFINER batch append (the runtime has no table write grant). The
  // append writes only for a session with a live pointer, so one is ensured first unless `noPointer`.
  const insertAudit = async (r: Row, role = "service_role") => {
    if (!r.noPointer && (r.ws ?? "ws-a") === "ws-a" && (r.harness ?? "codex") === "codex") await pointer(r.session, "m-1", r.org ?? "org-a");
    return as(role, (tx) => tx.query<{ n: number }>(
      "SELECT sessions.summary_regeneration_audit_append_v1($1::jsonb) AS n",
      [JSON.stringify([{ org_id: r.org ?? "org-a", workspace_id: r.ws ?? "ws-a", harness: r.harness ?? "codex", session_id: r.session,
        system: r.system ?? "bob_costas", kind: r.kind ?? "short", reason: r.reason ?? "turns_gained" }])]));
  };
  const appended = async (r: Row) => Number((await insertAudit(r)).rows[0]!.n);
  const count = async (session?: string) => Number((await db.query<{ n: number }>(
    `SELECT count(*)::int AS n FROM sessions.summary_regeneration_audit ${session ? "WHERE session_id=$1" : ""}`,
    session ? [session] : [])).rows[0]!.n);
  const pointer = (session: string, machine = "m-1", org = "org-a") => db.query(
    `INSERT INTO sessions.session_index
       (org_id, workspace_id, harness, session_id, machine_id, user_id, created_at, last_activity_at,
        storage_provider, bucket, manifest_key, content_sha256, raw_format, trace_id, revision)
     VALUES ($3,'ws-a','codex',$1,$2,'u',now(),now(),'s3','bucket','k/'||$3||$1||$2,
             decode(repeat('ab',32),'hex'),'jsonl','019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18',1)
     ON CONFLICT DO NOTHING`, [session, machine, org]);
  const directory = async (session: string) => {
    if ((await db.query("SELECT 1 FROM sessions.session_directory WHERE org_id='org-a' AND session_id=$1", [session])).rows.length) return;
    await db.query(
      `INSERT INTO sessions.session_directory (org_id, workspace_id, harness, session_id, created_at, last_activity_at)
       VALUES ('org-a','ws-a','codex',$1,now(),now())`, [session]);
  };

  beforeAll(async () => {
    db = new PGlite({ extensions: { btree_gin, pg_trgm } });
    await runMigrations(client(db), { profile: LOCAL });
  });
  afterAll(async () => { await db?.close(); });

  test("service_role inserts and reads; anon, authenticated and the read API are denied", async () => {
    await insertAudit({ session: "grant-1" });
    expect(await as("service_role", async (tx) => (await tx.query("SELECT id FROM sessions.summary_regeneration_audit WHERE session_id='grant-1'")).rows.length)).toBe(1);
    for (const role of ["anon", "authenticated", "bulls96_read_api", "search_consumer_runtime"]) {
      expect(await code(insertAudit({ session: "grant-2" }, role))).toBe("42501");
      expect(await code(as(role, (tx) => tx.query("SELECT 1 FROM sessions.summary_regeneration_audit")))).toBe("42501");
      expect(await code(as(role, (tx) => tx.query("SELECT sessions.summary_regeneration_audit_purge_v1(90)")))).toBe("42501");
    }
    // No direct table write for the runtime: only the append function writes.
    expect(await code(as("service_role", (tx) => tx.query(
      `INSERT INTO sessions.summary_regeneration_audit (${AUDIT_COLS}) VALUES ('org-a','ws-a','codex','grant-3','brief','brief','turns_gained')`)))).toBe("42501");
    // The append refuses a non-array and a batch over 1000 rows; CHECKs still apply to each row.
    const append = (rows: unknown) => as("service_role", (tx) => tx.query("SELECT sessions.summary_regeneration_audit_append_v1($1::jsonb)", [JSON.stringify(rows)]));
    expect(await code(append({ org_id: "org-a" }))).toBe("22023");
    expect(await code(append(Array.from({ length: 1001 }, () => ({ org_id: "org-a" }))))).toBe("22023");
    await pointer("grant-4");
    expect(await code(append([{ org_id: "org-a", workspace_id: "ws-a", harness: "codex", session_id: "grant-4", system: "brief", kind: "short", reason: "turns_gained" }]))).toBe("23514");
    expect(await count("grant-4")).toBe(0);
  });

  test("append-only: UPDATE, DELETE and TRUNCATE are refused, also for the owner without the marker", async () => {
    await insertAudit({ session: "append-1" });
    expect(await code(as("service_role", (tx) => tx.query("UPDATE sessions.summary_regeneration_audit SET reason='config_changed' WHERE session_id='append-1'")))).toBe("42501");
    expect(await code(db.query("UPDATE sessions.summary_regeneration_audit SET reason='config_changed' WHERE session_id='append-1'"))).toBe("55000");
    expect(await code(db.query("DELETE FROM sessions.summary_regeneration_audit WHERE session_id='append-1'"))).toBe("55000");
    expect(await code(db.exec("TRUNCATE sessions.summary_regeneration_audit"))).toBe("55000");
    // A session-level marker set by a non-owner role does not open DELETE.
    expect(await code(as("service_role", async (tx) => {
      await tx.query("SELECT set_config('sessions.summary_regeneration_audit_delete','on',true)");
      return tx.query("DELETE FROM sessions.summary_regeneration_audit WHERE session_id='append-1'");
    }))).toBe("42501");
    expect(await count("append-1")).toBe(1);
  });

  test("CHECKs: reason, system/kind pairing, request_id/scope_hash pairing and value shapes", async () => {
    expect(await code(insertAudit({ session: "chk", reason: "because" }))).toBe("23514");
    expect(await code(insertAudit({ session: "chk", system: "brief", kind: "short" }))).toBe("23514");
    expect(await code(insertAudit({ session: "chk", system: "bob_costas", kind: "brief" }))).toBe("23514");
    expect(await code(insertAudit({ session: "chk", kind: "medium" }))).toBe("23514");
    // Direct INSERTs run as the owner: the runtime role has no table write grant.
    expect(await code(db.query(
      `INSERT INTO sessions.summary_regeneration_audit (${AUDIT_COLS}, request_id) VALUES ('o','w','codex','chk','brief','brief','operator_requeue','req-1')`))).toBe("23514");
    expect(await code(db.query(
      `INSERT INTO sessions.summary_regeneration_audit (${AUDIT_COLS}, prior_failure_code) VALUES ('o','w','codex','chk','brief','brief','retry_backoff','Not A Code')`))).toBe("23514");
    expect(await count("chk")).toBe(0);
  });

  test("no summary text or prompt column exists", async () => {
    const cols = (await db.query<{ column_name: string }>(
      "SELECT column_name FROM information_schema.columns WHERE table_schema='sessions' AND table_name='summary_regeneration_audit' ORDER BY ordinal_position")).rows.map((r) => r.column_name);
    expect(cols).toEqual(["id", "org_id", "workspace_id", "harness", "session_id", "system", "kind", "reason", "prior_status",
      "prior_failure_code", "prior_pipeline_version", "request_id", "scope_hash", "requested_by", "created_at"]);
  });

  test("operator requeue idempotency: one row per (request_id, scope_hash)", async () => {
    // Owner INSERT: push B's idempotent writer is not built yet; this pins the unique index only.
    const ins = () => db.query(
      `INSERT INTO sessions.summary_regeneration_audit (${AUDIT_COLS}, request_id, scope_hash) VALUES ('org-a','ws-a','codex','req','brief','brief','operator_requeue','req-9',repeat('a',64))`);
    await ins();
    expect(await code(ins())).toBe("23505");
  });

  test("forget: tombstoning or deleting a pointer removes the session's audit rows only", async () => {
    await pointer("forget-t"); await pointer("forget-d"); await pointer("forget-keep");
    for (const s of ["forget-t", "forget-d", "forget-keep"]) await insertAudit({ session: s });
    await insertAudit({ session: "forget-t", org: "org-b" });
    await db.query("UPDATE sessions.session_index SET deleted_at=now() WHERE org_id='org-a' AND session_id='forget-t'");
    expect(await count("forget-t")).toBe(1); // org-b row with the same session id stays
    await db.query("DELETE FROM sessions.session_index WHERE session_id='forget-d'");
    expect(await count("forget-d")).toBe(0);
    expect(await count("forget-keep")).toBe(1);
  });

  test("forget: tombstoning or deleting the directory row removes the session's audit rows", async () => {
    await pointer("dir-t"); await pointer("dir-d");
    await insertAudit({ session: "dir-t" }); await insertAudit({ session: "dir-d" });
    const dir = (await db.query("SELECT 1 FROM sessions.session_directory WHERE session_id IN ('dir-t','dir-d')")).rows.length;
    if (dir === 0) {
      for (const s of ["dir-t", "dir-d"]) await db.query(
        `INSERT INTO sessions.session_directory (org_id, workspace_id, harness, session_id, created_at, last_activity_at)
         VALUES ('org-a','ws-a','codex',$1,now(),now())`, [s]);
    }
    await db.query("UPDATE sessions.session_directory SET deleted_at=now() WHERE session_id='dir-t'");
    expect(await count("dir-t")).toBe(0);
    await db.query("DELETE FROM sessions.session_directory WHERE session_id='dir-d'");
    expect(await count("dir-d")).toBe(0);
  });

  test("append: a session that is absent, tombstoned or deleted (pointer or directory) gains no row", async () => {
    // Absent: no pointer at all.
    expect(await appended({ session: "skip-absent", noPointer: true })).toBe(0);
    // Tombstoned pointer.
    await pointer("skip-t");
    await db.query("UPDATE sessions.session_index SET deleted_at=now() WHERE session_id='skip-t'");
    expect(await appended({ session: "skip-t", noPointer: true })).toBe(0);
    // Deleted pointer (hard delete).
    await pointer("skip-d");
    await db.query("DELETE FROM sessions.session_index WHERE session_id='skip-d'");
    expect(await appended({ session: "skip-d", noPointer: true })).toBe(0);
    // One machine's pointer tombstoned, another live: forgotten, as the forget trigger treats it.
    await pointer("skip-m", "m-1"); await pointer("skip-m", "m-2");
    await db.query("UPDATE sessions.session_index SET deleted_at=now() WHERE session_id='skip-m' AND machine_id='m-1'");
    expect(await appended({ session: "skip-m", noPointer: true })).toBe(0);
    // Live pointer, tombstoned directory row.
    await pointer("skip-dir"); await directory("skip-dir");
    await db.query("UPDATE sessions.session_directory SET deleted_at=now() WHERE org_id='org-a' AND session_id='skip-dir'");
    expect(await appended({ session: "skip-dir", noPointer: true })).toBe(0);
    for (const s of ["skip-absent", "skip-t", "skip-d", "skip-m", "skip-dir"]) expect(await count(s)).toBe(0);
    // A mixed batch writes only the live session's row; a live directory row does not block.
    await pointer("skip-live"); await directory("skip-live");
    const row = (session: string) => ({ org_id: "org-a", workspace_id: "ws-a", harness: "codex", session_id: session,
      system: "brief", kind: "brief", reason: "retry_backoff" });
    const n = await as("service_role", async (tx) => Number((await tx.query<{ n: number }>(
      "SELECT sessions.summary_regeneration_audit_append_v1($1::jsonb) AS n",
      [JSON.stringify([row("skip-live"), row("skip-t"), row("skip-absent"), row("skip-dir")])])).rows[0]!.n));
    expect(n).toBe(1);
    expect(await count("skip-live")).toBe(1);
    // Same session id in another org, with no pointer of its own: nothing.
    expect(await appended({ session: "skip-live", org: "org-z", noPointer: true })).toBe(0);
    expect(await count("skip-live")).toBe(1);
  });

  test("sentinel: true when applied; false when append_v1, purge_v1, the table or any session_briefs column is absent", async () => {
    const applied = async (q: Pick<PGlite, "query">) => (await q.query<{ applied: boolean }>(MIGRATION_SENTINELS[STEP]!.sql)).rows[0]!.applied;
    expect(await applied(db)).toBe(true);
    const ROLLBACK = new Error("rollback");
    const without = async (ddl: string) => {
      let seen: boolean | null = null;
      await db.transaction(async (tx) => {
        await tx.exec(ddl);
        seen = await applied(tx);
        throw ROLLBACK;
      }).catch((e: unknown) => { if (e !== ROLLBACK) throw e; });
      return seen;
    };
    for (const ddl of [
      "DROP FUNCTION sessions.summary_regeneration_audit_append_v1(jsonb)",
      "DROP FUNCTION sessions.summary_regeneration_audit_purge_v1(integer)",
      "DROP TABLE sessions.summary_regeneration_audit CASCADE",
      ...["covered_turns", "retry_attempts", "next_retry_at", "config_hash", "pipeline_version"].map(
        (c) => `ALTER TABLE sessions.session_briefs DROP COLUMN ${c}`),
    ]) expect(await without(ddl), ddl).toBe(false);
    expect(await applied(db)).toBe(true);
  });

  test("purge_v1: bounds refused with 22023, deletes only rows older than the retention", async () => {
    const purge = (days: number | null) => as("service_role", async (tx) =>
      Number((await tx.query<{ n: string | number }>("SELECT sessions.summary_regeneration_audit_purge_v1($1::int) AS n", [days])).rows[0]!.n));
    for (const bad of [0, -1, 3651, null]) expect(await code(purge(bad))).toBe("22023");
    await insertAudit({ session: "purge-old" }); await insertAudit({ session: "purge-new" });
    // Backdate as the owner with the marker (the only way past the append-only trigger).
    await db.transaction(async (tx) => {
      await tx.query("ALTER TABLE sessions.summary_regeneration_audit DISABLE TRIGGER summary_regeneration_audit_append_only");
      await tx.query("UPDATE sessions.summary_regeneration_audit SET created_at=now()-interval '91 days' WHERE session_id='purge-old'");
      await tx.query("ALTER TABLE sessions.summary_regeneration_audit ENABLE TRIGGER summary_regeneration_audit_append_only");
    });
    expect(await purge(90)).toBe(1);
    expect(await count("purge-old")).toBe(0);
    expect(await count("purge-new")).toBe(1);
    expect(await purge(90)).toBe(0);
    expect(await purge(3650)).toBe(0);
    // The marker does not leak out of the purge call.
    expect(await code(db.query("DELETE FROM sessions.summary_regeneration_audit WHERE session_id='purge-new'"))).toBe("55000");
  });

  test("session_briefs and session_summaries gain nullable or defaulted coverage/retry columns", async () => {
    const cols = async (table: string) => (await db.query<{ column_name: string; is_nullable: string; column_default: string | null }>(
      "SELECT column_name, is_nullable, column_default FROM information_schema.columns WHERE table_schema='sessions' AND table_name=$1",
      [table])).rows;
    const briefs = Object.fromEntries((await cols("session_briefs")).map((c) => [c.column_name, c]));
    for (const c of ["covered_turns", "next_retry_at", "config_hash", "pipeline_version"]) expect(briefs[c]?.is_nullable).toBe("YES");
    expect(briefs.retry_attempts?.column_default).toBe("0");
    const sums = Object.fromEntries((await cols("session_summaries")).map((c) => [c.column_name, c]));
    for (const c of ["covered_turns", "failure_code", "next_retry_at", "config_hash", "pipeline_version"]) expect(sums[c]?.is_nullable).toBe("YES");
    expect(sums.degraded?.column_default).toBe("false");
    expect(sums.attempts?.column_default).toBe("0");
    expect((await db.query("SELECT 1 FROM pg_indexes WHERE schemaname='sessions' AND indexname='session_briefs_next_retry'")).rows.length).toBe(1);
  });
});
