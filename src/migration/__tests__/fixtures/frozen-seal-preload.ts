// Runs the real scripts/frozen-source-copy.ts against disposable databases. No provider CLI or
// network: the CLI login is synthetic and every pg.Client is redirected to the fixture.
//   SEAL_TEST_BACKEND=pglite  SEAL_TEST_SRC_DIR / SEAL_TEST_TGT_DIR (file-backed PGlite, one session emulated per client)
//   SEAL_TEST_BACKEND=pg      SEAL_TEST_SRC_URL / SEAL_TEST_TGT_URL (real PG17 sessions)
// Hooks (fixture only): SEAL_TEST_WRITE_WHEN_ORG + SEAL_TEST_WRITE (disable|empty-reset) [+ SEAL_TEST_WRITE_MIRROR=1],
//   SEAL_TEST_FAIL_PAGE=n (the nth Neon record page fails: PG terminates that backend), SEAL_TEST_PROBE=1 (pg: read
//   transaction_read_only before every SELECT). The statement log goes to SEAL_TEST_LOG at exit.
import pg from "pg";
import { writeFileSync } from "node:fs";
import { createRequire, syncBuiltinESMExports } from "node:module";
const RealClient = pg.Client;
const cp = createRequire(import.meta.url)("node:child_process");
cp.execFileSync = (exe: string) => {
  if (exe !== "/synthetic-supabase-refuse-network") throw Error("unexpected_process_refused");
  return ['PGHOST="db.pyamkrhoyszgzirqqffq.supabase.co"', 'PGPORT="5432"', 'PGUSER="cli_login_postgres"', 'PGDATABASE="postgres"', 'PGPASSWORD="synthetic-cli-password"'].join("\n");
};
syncBuiltinESMExports();
globalThis.fetch = (async () => { throw Error("network_refused_in_seal_fixture"); }) as any;

const env = process.env, backend = env.SEAL_TEST_BACKEND;
type Stmt = { side: "source" | "target"; client: number; sql: string; inTx: boolean; txReadOnly: boolean; sessionReadOnly: boolean; probe?: string };
const statements: Stmt[] = [];
let clientSeq = 0, neonPages = 0, wrote = false;
const live = new Set<number>();
const head = (sql: string) => sql.replace(/\s+/g, " ").trim().slice(0, 160);

class Lock { private tail = Promise.resolve(); acquire() { let release!: () => void; const p = new Promise<void>((r) => (release = r)); const prev = this.tail; this.tail = prev.then(() => p); return prev.then(() => release); } }

let pglite: Record<"source" | "target", { db: any; lock: Lock }> | undefined;
if (backend === "pglite") {
  const { PGlite } = await import("@electric-sql/pglite");
  pglite = { source: { db: new PGlite(env.SEAL_TEST_SRC_DIR!), lock: new Lock() }, target: { db: new PGlite(env.SEAL_TEST_TGT_DIR!), lock: new Lock() } };
  // PGlite's runtime sets process.exitCode=99 while open (and 0 on close); start from a clean status.
  await pglite.source.db.waitReady; await pglite.target.db.waitReady;
  process.exitCode = 0;
} else if (backend !== "pg" && backend !== "none") throw Error("SEAL_TEST_BACKEND required");

async function adminWrite(fn: (q: (sql: string, p?: unknown[]) => Promise<any>, side: "source" | "target") => Promise<void>, side: "source" | "target") {
  if (pglite) {
    const s = pglite[side], release = await s.lock.acquire();
    try { await s.db.query("BEGIN"); await fn((sql, p) => s.db.query(sql, p), side); await s.db.query("COMMIT"); }
    catch (e) { await s.db.query("ROLLBACK").catch(() => {}); throw e; } finally { release(); }
  } else {
    const c = new RealClient({ connectionString: side === "source" ? env.SEAL_TEST_SRC_URL : env.SEAL_TEST_TGT_URL });
    await c.connect();
    try { await c.query("BEGIN"); await fn((sql, p) => c.query(sql, p), side); await c.query("COMMIT"); } finally { await c.end(); }
  }
}

/** A permitted control on another session (relayhistory selection/reset), optionally mirrored to the target head. */
async function concurrentControl() {
  const org = env.SEAL_TEST_WRITE_WHEN_ORG_TARGET ?? "Org-B";
  let headRow: any;
  await adminWrite(async (q) => {
    if (env.SEAL_TEST_WRITE === "halt") { await q("UPDATE sessions.delivery_cutover_state SET phase='halted'"); return; }
    if (env.SEAL_TEST_WRITE === "unguard") { await q("ALTER TABLE sessions.session_resets DISABLE TRIGGER delivery_cutover_row"); return; }
    const r = env.SEAL_TEST_WRITE === "empty-reset"
      ? await q("SELECT sessions.fixture_empty_reset_v1($1,'ws-1','u9','req-concurrent') AS h", [org])
      : await q("SELECT sessions.fixture_disable_session_v1($1,'ws-1','u1','origin-1','claude','s1') AS h", [org]);
    headRow = r.rows[0].h;
  }, "source");
  if (env.SEAL_TEST_WRITE_MIRROR === "1" && headRow) await adminWrite(async (q) => {
    await q(`INSERT INTO sessions.ingest_source_control_heads(org_id,workspace_id,user_id,epoch,version,digest,kind,payload,pending)
      VALUES($1,$2,$3,$4,$5::bigint,$6,$7,$8::jsonb,false) ON CONFLICT (org_id,workspace_id,user_id) DO UPDATE SET version=EXCLUDED.version,digest=EXCLUDED.digest,pending=false`,
      [headRow.org_id, headRow.workspace_id, headRow.user_id, headRow.epoch, String(headRow.version), headRow.digest, headRow.kind, JSON.stringify(headRow.payload)]);
  }, "target");
}

class FixtureClient {
  id = ++clientSeq; side: "source" | "target"; inTx = false; txReadOnly = false; sessionReadOnly = false; ended = false;
  real?: pg.Client; release?: () => void; pid = 0;
  constructor(cfg: any) {
    this.side = cfg.host ? "target" : "source";
    if (!pglite) { this.real = new RealClient({ connectionString: this.side === "source" ? env.SEAL_TEST_SRC_URL : env.SEAL_TEST_TGT_URL, application_name: "seal-fixture-" + this.side }); this.real.on("error", () => {}); }
  }
  on() { return this; }
  async connect() {
    live.add(this.id);
    if (this.real) { await this.real.connect(); this.pid = (await this.real.query("SELECT pg_backend_pid() AS p")).rows[0].p; }
  }
  private async raw(sql: string, params?: unknown[]) {
    if (this.real) return this.real.query(sql, params as any[]);
    const s = pglite![this.side];
    // Emulate one PostgreSQL session per client on a shared PGlite.
    if (/^SET SESSION ROLE /i.test(sql)) return { rows: [] };
    if (/^SET default_transaction_read_only = on$/i.test(sql)) { return { rows: [] }; }
    if (/^BEGIN/i.test(sql)) { this.release = await s.lock.acquire(); return s.db.query(sql); }
    if (/^(COMMIT|ROLLBACK)$/i.test(sql)) { try { return await s.db.query(sql); } finally { const r = this.release; this.release = undefined; r?.(); } }
    if (this.release) return s.db.query(sql, params);
    const release = await s.lock.acquire();
    try {
      if (!this.sessionReadOnly) return await s.db.query(sql, params);
      await s.db.query("BEGIN READ ONLY");
      try { const r = await s.db.query(sql, params); await s.db.query("COMMIT"); return r; } catch (e) { await s.db.query("ROLLBACK").catch(() => {}); throw e; }
    } finally { release(); }
  }
  async query(sql: string, params?: unknown[]) {
    if (this.ended) throw Error("query_after_end");
    const st: Stmt = { side: this.side, client: this.id, sql: head(sql), inTx: this.inTx, txReadOnly: this.txReadOnly, sessionReadOnly: this.sessionReadOnly };
    if (/^\s*SELECT/i.test(sql) && env.SEAL_TEST_PROBE === "1" && this.real) st.probe = (await this.real.query("SELECT current_setting('transaction_read_only') AS ro")).rows[0].ro;
    statements.push(st);
    const isNeonPage = this.side === "source" && sql.includes("FROM sessions.delivery_records r");
    if (isNeonPage && ++neonPages === Number(env.SEAL_TEST_FAIL_PAGE ?? 0)) {
      if (this.real) { const a = new RealClient({ connectionString: env.SEAL_TEST_SRC_URL }); await a.connect(); await a.query("SELECT pg_terminate_backend($1)", [this.pid]); await a.end(); }
      else throw Object.assign(Error("synthetic_worker_failure"), { code: "57P01" });
    }
    const r = await this.raw(sql, params);
    if (/^SET default_transaction_read_only = on$/i.test(sql)) this.sessionReadOnly = true;
    if (/^BEGIN/i.test(sql)) { this.inTx = true; this.txReadOnly = /READ ONLY/i.test(sql); }
    if (/^(COMMIT|ROLLBACK)$/i.test(sql)) { this.inTx = false; this.txReadOnly = false; }
    if (isNeonPage && !wrote && env.SEAL_TEST_WRITE && params?.[0] === env.SEAL_TEST_WRITE_WHEN_ORG) {
      wrote = true;
      // PG: committed on another session before this page returns. PGlite: queued behind this
      // reader's transaction, so it lands before the next statement batch of any session.
      if (pglite) void concurrentControl().catch((e) => { console.error("fixture write failed", e); process.exit(99); });
      else await concurrentControl();
    }
    return { rows: r.rows };
  }
  async end() {
    if (this.ended) return;
    this.ended = true; live.delete(this.id);
    // A session that ends inside a transaction aborts it (as a real backend exit would).
    if (pglite && this.release) await pglite[this.side].db.query("ROLLBACK").catch(() => {});
    const r = this.release; this.release = undefined; r?.();
    if (this.real) await this.real.end().catch(() => {});
  }
}
pg.Client = FixtureClient as any;
let exiting = false;
process.on("beforeExit", async () => {
  if (exiting) return;
  exiting = true;
  const code = typeof process.exitCode === "number" ? process.exitCode : 0;
  if (env.SEAL_TEST_LOG) writeFileSync(env.SEAL_TEST_LOG, JSON.stringify({ statements, clients: clientSeq, live: [...live], neonPages, wrote }));
  // PGlite's close() rewrites the exit status; exit explicitly with the script's own.
  if (pglite) { await pglite.source.db.close(); await pglite.target.db.close(); }
  process.exit(code);
});
