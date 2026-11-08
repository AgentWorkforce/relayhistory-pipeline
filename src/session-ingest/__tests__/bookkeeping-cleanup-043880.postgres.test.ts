/** Opt-in OWNED PG17 (SESSION_INGEST_TEST_PG_URL, loopback, user postgres, no password): every migration through
 * the real migrator, INCLUDING 20261105043880_ingest_bookkeeping_cleanup (Phase A of PR #323).
 * Fixtures come from the real lazy-mode producer (deliverBatch, real witness, real ACK trigger). The
 * publication/GC lifecycle (outbox erased, stage redacted) is modelled exactly as the 038000 test does.
 * Design: docs/design/ingest-bookkeeping-cleanup.md §8. */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import pg, { type PoolClient } from "pg";
import { readFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { deliverBatch } from "../deliver.ts";
import { sqlLedger } from "../ledger-sql.ts";
import { deliveryAccount, prepareDelivery } from "../vendor/delivery/prepare.ts";
import { batch, record, scratchStore, TENANT } from "./support.ts";
import { runMigrations, type MigrationProfile } from "../../hoopdb/migrate.ts";
import { derivedSchemaAssets } from "../../session-pipeline/derived-schema.ts";

const url = process.env.SESSION_INGEST_TEST_PG_URL;
if (url) {
  const u = new URL(url);
  if (!["postgresql:", "postgres:"].includes(u.protocol) || !["127.0.0.1", "localhost", "[::1]"].includes(u.hostname) || u.username !== "postgres" ||
      u.password || u.pathname !== "/postgres" || u.search || u.hash) throw Error("owned_loopback_fixture_required");
}
const root = join(import.meta.dir, "../../..");
const text = (path: string) => readFileSync(resolve(root, path), "utf8");
const INDEX_JOB = "scripts/ops/ingest-bookkeeping-cleanup/phase_a_index.sql";
const indexStatements = text(INDEX_JOB).split("\n").filter((l) => !l.startsWith("--")).join("\n").split(";").map((s) => s.trim()).filter((s) => s.startsWith("CREATE INDEX"));
const NAME = "upstream:20261105043880_ingest_bookkeeping_cleanup.sql", ROLLBACK = "supabase/rollbacks/20261105043880_ingest_bookkeeping_cleanup.down.sql";
/** 043890 replaces the search gate to read 043880's tombstones; it is later, so its down must run first. */
const GR_NAME = "upstream:20261105043890_buffered_search_gate_retired_witness.sql", GR_ROLLBACK = "supabase/rollbacks/20261105043890_buffered_search_gate_retired_witness.down.sql";
const epoch = "3".repeat(64), identity = "c".repeat(64), hex = (c: string) => c.repeat(64);
const profile: MigrationProfile = { mode: "local", shim: true, bootstrap: true, verify: true, runtimeRole: "postgres" };
type Current = { revision: number; digest?: string };
type Rec = { id: string; revision?: number; session?: string; payload?: string; current?: Current };
type Seed = { tenant: typeof TENANT; batchId: string; stage: string | null; claim: string | null; outcome: string; again: () => Promise<string> };

describe.skipIf(!url)("ingest bookkeeping cleanup 043880 (Phase A) on owned PG17", () => {
  let admin: pg.Client, pool: pg.Pool, name: string, generation: string, serial = 0;
  let scratch: ReturnType<typeof scratchStore>;
  const open = new Set<PoolClient>();
  const connect = async () => { const c = await pool.connect(); open.add(c); return c; };
  const release = async (c: PoolClient) => { await c.query("ROLLBACK").catch(() => {}); open.delete(c); c.release(); };
  /** One cleanup page in its own transaction with the bounded statement_timeout the functions require. */
  const page = async (fn: string, body: unknown, timeout: number | null = 3000) => {
    const c = await connect();
    try {
      await c.query("BEGIN");
      if (timeout !== null) await c.query(`SET LOCAL statement_timeout=${timeout}`);
      const r = (await c.query(`SELECT sessions.ingest_bookkeeping_cleanup_${fn}($1::jsonb) AS r`, [JSON.stringify(body)])).rows[0].r;
      await c.query("COMMIT"); return r;
    } finally { await release(c); }
  };
  const decisions = (execute: boolean, after: string | null = null, rowBudget = 5000) => page("decisions", { after, rowBudget, execute });
  const configure = (o: Partial<Record<"enabled" | "dryRun" | "decisions", boolean>> & { minAgeDays?: number } = {}) =>
    pool.query("SELECT sessions.ingest_bookkeeping_cleanup_configure($1::jsonb)", [JSON.stringify({ enabled: false, dryRun: true, decisions: false,
      minAgeDays: 7, ...o })]);
  const allOn = () => configure({ enabled: true, dryRun: false, decisions: true });
  const migrate = async () => { const c = await connect(); try { return await runMigrations(c, { profile }); } finally { await release(c); } };
  const one = async <T = any>(sql: string, params: unknown[] = []) => (await pool.query(sql, params as never)).rows[0] as T;
  const count = async (sql: string, params: unknown[] = []) => Number((await one<{ n: string }>(sql, params)).n);
  const nativeKey = (t: typeof TENANT, origin: string, recordId: string) =>
    one<{ k: string }>("SELECT encode(sha256(convert_to(jsonb_build_array('global-native-custody-v1',$1::text,$2::text,$3::text,$4::text)::text,'UTF8')),'hex') k",
      [t.orgId, t.workspaceId, origin, recordId]).then((r) => r.k);
  const slot = async (t: typeof TENANT, recordId: string) => one("SELECT * FROM sessions.ingest_lazy_native_slots WHERE key_sha256=$1", [await nativeKey(t, "origin-1", recordId)]);
  const decisionRows = (claim: string) => count("SELECT count(*) n FROM sessions.ingest_lazy_ack_decisions WHERE claim_token=$1", [claim]);

  /** The real producer: lazy-current witness (optionally carrying a Neon `current` row), claim, stage, ACK. */
  async function seed(recs: Rec[], tenant?: typeof TENANT, batchId?: string): Promise<Seed> {
    const suffix = `bk-${++serial}`;
    const t = tenant ?? { ...TENANT, userId: "bk-user-" + suffix };
    const records = recs.map((r) => record({ record_id: r.id, revision: r.revision ?? 1, session_id: r.session ?? `${r.id}-session`,
      payload: { text: r.payload ?? "hello", ts_ms: 1_761_955_200_000 } }));
    const raw = await batch(records, { batch_id: batchId ?? suffix, account_id: await deliveryAccount(t) });
    const prepared = await prepareDelivery(t, raw);
    const byId = new Map(recs.map((r) => [r.id, r]));
    const witness = {
      version: 1, issuer: "relayhistory-cloud", sourceIdentitySha256: identity, controlEpoch: epoch,
      privacyBaselineSha256: hex("a"), tenant: t, originId: prepared.originId, batchId: prepared.batchId,
      batchDigest: prepared.batchDigest, sourceControlVersion: "0", pending: false, pendingControl: null,
      privacyVersion: "0", originalRecordCount: records.length, candidateCount: prepared.records.length,
      records: prepared.records.map((r, i) => {
        const cur = byId.get(r.record_id)?.current;
        return { preparedOrd: i, processingRank: i, candidateDigest: r.digest, recordId: r.record_id, revision: String(r.revision),
          revisionId: r.revision_id, candidateFenced: false, currentFenced: false,
          current: cur ? { userId: t.userId, revision: String(cur.revision), digest: cur.digest ?? hex("e"), revisionId: `${r.record_id}:neon${cur.revision}`,
            kind: "session_event", source: r.source, sessionId: r.session_id, operation: "upsert", catalogAcceptedSeq: "7", projectionAcceptedSeq: null } : null };
      }),
      privacy: { complete: true, deletions: [], resets: [] },
    };
    const sourceDigest = (await one<{ d: string }>("SELECT encode(sha256(convert_to($1::jsonb::text,'UTF8')),'hex') AS d", [JSON.stringify(witness)])).d;
    const ledger = sqlLedger(async (sql, params) => {
      const c = await connect();
      try { await c.query("BEGIN;SET LOCAL ROLE bulls96_session_ingest"); const r = await c.query(sql, params as never); await c.query("COMMIT"); return r as never; }
      finally { await release(c); }
    }, { acceptanceMode: "buffered", expectedGeneration: generation });
    const deliver = async () => (await deliverBatch({ ledger, acceptanceMode: "buffered", target: { store: scratch.store, keyRoot: "96bulls" },
      custody: { witness, sourceDigest } } as never, t, raw)).outcome as string;
    const outcome = await deliver();
    const s = (await pool.query("SELECT stage_token::text,claim_token::text FROM sessions.ingest_stage_intents WHERE batch_id=$1 AND parent_stage_token IS NULL",
      [raw.batch_id])).rows[0] as { stage_token: string; claim_token: string } | undefined;
    return { tenant: t, batchId: raw.batch_id, stage: s?.stage_token ?? null, claim: s?.claim_token ?? null, outcome, again: deliver };
  }
  /** Model publication + GC barrier finish (the 038000 test's lifecycle) and age the commit beyond min_age. */
  async function terminal(s: Seed, o: { cleanup?: "retired" | "redacted"; age?: string } = {}) {
    await pool.query("UPDATE sessions.ingest_buffer_outbox SET active_stage_token=NULL,state='erased',row_version=row_version+1 WHERE batch_id=$1", [s.batchId]);
    await pool.query("UPDATE sessions.ingest_stage_intents SET cleanup_state=$2,row_version=row_version+1 WHERE claim_token=$1", [s.claim, o.cleanup ?? "redacted"]);
    await pool.query("UPDATE sessions.ingest_batches SET committed_at=clock_timestamp()-$2::interval WHERE batch_id=$1", [s.batchId, o.age ?? "8 days"]);
  }
  const plan = async (q: string) => {
    const c = await connect();
    try { await c.query("BEGIN;SET LOCAL enable_seqscan=off"); return (await c.query("EXPLAIN " + q)).rows.map((r) => r["QUERY PLAN"]).join("\n"); }
    finally { await release(c); }
  };
  const waitForBlocker = async (waiter: number, blocker: number) => {
    const end = Date.now() + 4000;
    while (Date.now() < end) {
      if (((await pool.query("SELECT pg_blocking_pids($1) AS pids", [waiter])).rows[0].pids as number[]).includes(blocker)) return;
      await Bun.sleep(10);
    }
    throw Error("expected_fixture_lock_wait_missing");
  };
  const pid = async (c: PoolClient) => (await c.query("SELECT pg_backend_pid() AS pid")).rows[0].pid as number;
  /** A real, bucketless Babysitter DELETE candidate for the claim's own first record (accepted digest). */
  const registration = async (s: Seed) => {
    const a = await one(`SELECT a.origin_id,a.batch_id,b.digest,r FROM sessions.ingest_claim_attempts a JOIN sessions.ingest_batches b
      ON b.org_id=a.org_id AND b.workspace_id=a.workspace_id AND b.origin_id=a.origin_id AND b.batch_id=a.batch_id
      CROSS JOIN LATERAL jsonb_array_elements(a.records) r WHERE a.claim_token=$1 ORDER BY (r->>'ord')::int LIMIT 1`, [s.claim]);
    return { tenant: s.tenant, originId: a.origin_id, batchId: a.batch_id, batchDigest: a.digest, claimToken: s.claim, generation, resetEpoch: 0, buckets: [],
      records: [{ bucket: null, originId: a.origin_id, recordId: a.r.record_id, revision: String(a.r.revision), revisionId: a.r.revision_id, digest: a.r.digest,
        operation: "delete", kind: "turn_receipt", source: a.r.source, sessionId: a.r.session_id, line: null, expiresAtMs: null }] };
  };
  const register = async (c: PoolClient, body: unknown) => {
    await c.query("SET LOCAL ROLE bulls96_session_ingest");
    try { return (await c.query("SELECT sessions.ingest_babysitter_retention_register($1::jsonb) r", [JSON.stringify(body)])).rows[0].r; }
    finally { await c.query("RESET ROLE").catch(() => {}); }
  };
  /** The real Babysitter reader (read-API role, tenant-bound): the served tombstones for this record. */
  const served = async (s: Seed, recordId: string, inTx?: PoolClient) => {
    const c = inTx ?? await connect();
    try {
      if (!inTx) await c.query("BEGIN READ ONLY");
      await c.query("SET LOCAL ROLE bulls96_read_api");
      await c.query("SELECT set_config('bulls96_read.org_id',$1,true),set_config('bulls96_read.workspace_id',$2,true)", [s.tenant.orgId, s.tenant.workspaceId]);
      const v = (await c.query("SELECT sessions.babysitter_evidence_page_for($1,$2::jsonb) AS v", [s.tenant.workspaceId,
        JSON.stringify({ kind: "turn_receipt", source: null, sessionId: null, after: null, limit: 101, includeDeleted: true })])).rows[0].v;
      await c.query("RESET ROLE");
      return (v.rows as Array<{ recordId: string }>).filter((r) => r.recordId === recordId);
    } finally { if (!inTx) await release(c); }
  };

  beforeAll(async () => {
    admin = new pg.Client({ connectionString: url }); await admin.connect();
    const version = Number((await admin.query("SHOW server_version_num")).rows[0].server_version_num);
    expect(version).toBeGreaterThanOrEqual(170000); expect(version).toBeLessThan(180000);
    name = "bk_cleanup_" + crypto.randomUUID().replaceAll("-", "");
    await admin.query("CREATE DATABASE " + name);
    const target = new URL(url!); target.pathname = "/" + name;
    pool = new pg.Pool({ connectionString: target.href, max: 8 });
    scratch = scratchStore();
    await pool.query("CREATE EXTENSION pg_trgm;CREATE EXTENSION btree_gin");
    await migrate();
    generation = (await derivedSchemaAssets()).expected.generation;
    await pool.query("SELECT sessions.ingest_lazy_policy_set($1::jsonb)", [JSON.stringify({ baselineMode: "lazy-current", generation,
      sourceIdentitySha256: identity, controlEpoch: epoch, privacyBaselineSha256: hex("a"), retentionContractSha256: hex("b"),
      sourceProofSha256: hex("d"), validUntil: new Date(Date.now() + 3600000).toISOString(), issuer: "relayhistory-cloud" })]);
    await pool.query("SELECT sessions.ingest_receipt_seat_initialize($1::jsonb)", [JSON.stringify({ protocol: "delivery-acceptance-floor-v1", controlEpoch: epoch,
      capturedAt: "2026-11-04T00:00:00.000000Z", sourceSequences: ["sessions.delivery_catalog_acceptance_seq", "sessions.delivery_projection_acceptance_seq"].map((sequenceRegclass, i) => ({ sequenceRegclass, sequenceOid: String(i + 1), allocatedHighWater: "9007199254740993", isCalled: true, incrementBy: "1", cycle: false })),
      allocatedHighWater: "9007199254740993", generation, sourceIdentitySha256: identity, sourceProofSha256: hex("d") })]);
    // The read role's schema USAGE is its operator step (supabase/bootstrap/session_read_api_role.sql), not a migration.
    await pool.query("GRANT USAGE ON SCHEMA sessions TO bulls96_read_api");
    expect((await one("SELECT count(*)::int n FROM sessions.__96bulls_migrations WHERE name=$1", [NAME])).n).toBe(1);
    console.log(JSON.stringify({ proof: "bookkeeping-cleanup-043880-fixture", serverVersionNum: version, generation, migration: NAME }));
  }, 180000);
  afterAll(async () => {
    for (const c of open) await release(c);
    await pool?.end(); scratch?.done();
    if (admin) { if (name) await admin.query("DROP DATABASE IF EXISTS " + name + " WITH(FORCE)"); await admin.end(); }
  }, 30000);


  test("dormant default and gates: settings off, no unbounded timeout, phase indexes required, owner-only", async () => {
    const s = await one("SELECT enabled,dry_run,decisions_enabled,min_age::text FROM sessions.ingest_bookkeeping_cleanup_settings");
    expect(s).toEqual({ enabled: false, dry_run: true, decisions_enabled: false, min_age: "7 days" });
    // Phase A refuses (even a dry run) until its index exists.
    await expect(decisions(false)).rejects.toMatchObject({ code: "55000", message: expect.stringContaining("phase A index") });
    // A same-name index with the WRONG definition is refused, and the probe it should serve does seq-scan.
    await pool.query("CREATE INDEX babysitter_evidence_index_claim_idx ON sessions.babysitter_evidence_index(user_id)");
    await expect(decisions(false)).rejects.toMatchObject({ code: "55000", message: expect.stringContaining("phase A index") });
    // (No other index LEADS with claim_token: the probe can only scan, never seek, until the exact index exists.)
    const probe = "SELECT 1 FROM sessions.babysitter_evidence_index WHERE claim_token='00000000-0000-0000-0000-000000000001'";
    expect(await plan(probe)).not.toContain("using babysitter_evidence_index_claim_idx");
    // The operator file has no IF NOT EXISTS: building over the drifted name fails loudly instead of keeping it.
    await expect(pool.query(indexStatements[0]!)).rejects.toMatchObject({ code: "42P07" });
    await pool.query("DROP INDEX sessions.babysitter_evidence_index_claim_idx");
    await pool.query(indexStatements[0]!);
    expect(await plan(probe)).toContain("using babysitter_evidence_index_claim_idx");
    expect((await decisions(false)).mode).toBe("dry-run");
    // statement_timeout must be bounded by the caller: unset (0) and >5s are refused.
    await expect(page("decisions", { after: null, rowBudget: 500, execute: false }, null)).rejects.toMatchObject({ code: "55000" });
    await expect(page("decisions", { after: null, rowBudget: 500, execute: false }, 6000)).rejects.toMatchObject({ code: "55000" });
    // Execute is refused while disabled, while dry_run, and while the phase flag is off.
    await expect(decisions(true)).rejects.toMatchObject({ code: "55000", message: expect.stringContaining("disabled") });
    await configure({ enabled: true, dryRun: true, decisions: true });
    await expect(decisions(true)).rejects.toMatchObject({ code: "55000" });
    await configure({ enabled: true, dryRun: false, decisions: false });
    await expect(decisions(true)).rejects.toMatchObject({ code: "55000" });
    // Owner-only: the ingest runtime role can execute none of it.
    const c = await connect();
    try {
      await c.query("BEGIN;SET LOCAL statement_timeout=2000;SET LOCAL ROLE bulls96_session_ingest");
      await expect(c.query("SELECT sessions.ingest_bookkeeping_cleanup_decisions('{}'::jsonb)")).rejects.toMatchObject({ code: "42501" });
    } finally { await release(c); }
    // The audit is append-only.
    await expect(pool.query("DELETE FROM sessions.ingest_bookkeeping_cleanup_audit")).rejects.toMatchObject({ code: "55000" });
    await configure();
  }, 60000);

  test("guarded down/up: the down restores 042000's exact register on a never-used install; migrate reapplies", async () => {
    const regSrc = async () => (await one("SELECT prosrc,prosecdef,proacl::text acl FROM pg_proc WHERE oid='sessions.ingest_babysitter_retention_register(jsonb)'::regprocedure"));
    const before = await regSrc();
    expect(before.prosrc).toContain("ingest_bookkeeping_claim_lock_key");
    const original = text("supabase/migrations/20261105042000_babysitter_evidence_retention.sql");
    const at = original.indexOf("CREATE FUNCTION sessions.ingest_babysitter_retention_register(p jsonb)");
    const end = original.indexOf("END $register$;", at) + "END ".length;
    const body = original.slice(original.indexOf("$register$", at) + "$register$".length, end);
    // 043880's down is refused while the later 043890 is applied; 043890's own down (no tombstones yet) goes first.
    await expect(pool.query(text(ROLLBACK))).rejects.toMatchObject({ code: "55000", message: expect.stringContaining("a later migration is applied") });
    await pool.query(text(GR_ROLLBACK));
    expect((await one("SELECT count(*)::int n FROM sessions.__96bulls_migrations WHERE name=$1", [GR_NAME])).n).toBe(0);
    await pool.query(text(ROLLBACK));
    const restored = await regSrc();
    try { expect(restored.prosrc).toBe(body); } catch (e) { await migrate(); throw e; }
    expect([restored.prosecdef, restored.acl]).toEqual([before.prosecdef, before.acl]);
    expect((await one("SELECT to_regclass('sessions.ingest_bookkeeping_retired_claims') t")).t).toBeNull();
    expect((await one("SELECT count(*)::int n FROM sessions.__96bulls_migrations WHERE name=$1", [NAME])).n).toBe(0);
    await migrate();
    // The down also dropped the Phase A operator index; the operator job rebuilds it after a reapply.
    await pool.query(indexStatements[0]!);
    expect(await regSrc()).toEqual(before);
    expect((await one("SELECT count(*)::int n FROM sessions.__96bulls_migrations WHERE name=$1", [NAME])).n).toBe(1);
    expect((await one("SELECT count(*)::int n FROM sessions.__96bulls_migrations WHERE name=$1", [GR_NAME])).n).toBe(1);
  }, 120000);

  test("Phase A: only terminal claims lose their decisions; each condition is causal", async () => {
    // Five claims from the real producer, then exactly one condition varied per claim.
    const ok = await seed([{ id: "a-ok-1" }, { id: "a-ok-2" }]);
    const live = await seed([{ id: "a-live" }]);           // backlog: stage live, outbox buffered
    const retired = await seed([{ id: "a-retired" }]);     // retired, barrier not finished
    const young = await seed([{ id: "a-young" }]);         // redacted but committed inside min_age
    const sitter = await seed([{ id: "a-sitter" }]);       // a Babysitter evidence row names the claim
    for (const s of [ok, live, retired, young, sitter]) { expect(s.outcome).toBe("accepted_buffered"); expect(await decisionRows(s.claim!)).toBeGreaterThan(0); }
    await terminal(ok); await terminal(retired, { cleanup: "retired" }); await terminal(young, { age: "1 hour" }); await terminal(sitter);
    await pool.query(`INSERT INTO sessions.babysitter_evidence_index(org_id,workspace_id,origin_id,record_id,revision,revision_id,digest,operation,kind,claim_token,
      user_id,source,session_id,reset_epoch,fence_epoch) VALUES($1,$2,'origin-1','a-sitter',1,'a-sitter:r1',repeat('f',64),'delete','turn_receipt',$3,$4,'claude','a-sitter-session',0,0)`,
      [sitter.tenant.orgId, sitter.tenant.workspaceId, sitter.claim, sitter.tenant.userId]);
    const replayBefore = await ok.again();
    const before = Object.fromEntries(await Promise.all([ok, live, retired, young, sitter].map(async (s) => [s.claim, await decisionRows(s.claim!)])));

    const dry = await decisions(false);
    expect(dry).toMatchObject({ mode: "dry-run", terminalClaims: 1, eligibleRows: 2, deleted: 0, done: true,
      blocked: { liveStage: 2, uncommittedOrYoung: 2, babysitter: 1 } });
    expect(Object.fromEntries(await Promise.all(Object.keys(before).map(async (k) => [k, await decisionRows(k)])))).toEqual(before);

    await configure({ enabled: true, dryRun: false, decisions: true });
    const run = await decisions(true);
    expect(run).toMatchObject({ mode: "execute", terminalClaims: 1, deleted: 2 });
    expect(await decisionRows(ok.claim!)).toBe(0);
    for (const s of [live, retired, young, sitter]) expect(await decisionRows(s.claim!)).toBe(before[s.claim!]);
    expect(await count("SELECT count(*) n FROM sessions.ingest_bookkeeping_cleanup_audit WHERE action='deleted' AND phase='decisions' AND deleted=2")).toBe(1);

    // Flip each blocker off and the claim becomes eligible: the conditions are causal, not incidental.
    await pool.query("UPDATE sessions.ingest_stage_intents SET cleanup_state='redacted' WHERE claim_token=$1", [retired.claim]);
    await pool.query("UPDATE sessions.ingest_batches SET committed_at=clock_timestamp()-interval '8 days' WHERE batch_id=$1", [young.batchId]);
    await pool.query("DELETE FROM sessions.babysitter_evidence_index WHERE claim_token=$1", [sitter.claim]);
    expect(await decisions(true)).toMatchObject({ terminalClaims: 3, blocked: { liveStage: 1, uncommittedOrYoung: 1, babysitter: 0 } });
    for (const s of [retired, young, sitter]) expect(await decisionRows(s.claim!)).toBe(0);
    expect(await decisionRows(live.claim!)).toBe(before[live.claim!]);

    // ACK/replay safety: the original batch replays exactly as before, and every slot is intact.
    expect(await ok.again()).toBe(replayBefore);
    expect((await slot(ok.tenant, "a-ok-1")).current_owner).toBe(ok.tenant.userId);
    // The live (backlog) stage keeps its full reader coverage.
    const seatsForLive = await one("SELECT sessions.ingest_receipt_seats_for_stage($1::uuid) r", [live.stage]);
    expect(seatsForLive.r.records).toHaveLength(1);
    expect(await one("SELECT sessions.ingest_buffer_gc_ordinals($1::uuid) r", [live.stage])).toEqual({ r: [0] });
    await configure();
  }, 120000);

  test("proof by violation: a live stage's decisions are load-bearing for publication, GC and history", async () => {
    const live = await seed([{ id: "v-live" }]);
    const c = await connect();
    try {
      await c.query("BEGIN");
      await c.query("DELETE FROM sessions.ingest_lazy_ack_decisions WHERE claim_token=$1", [live.claim]);
      await c.query("SAVEPOINT a");
      await expect(c.query("SELECT sessions.ingest_receipt_seats_for_stage($1::uuid)", [live.stage])).rejects.toMatchObject({ code: "55000", message: expect.stringContaining("ACK decisions missing") });
      await c.query("ROLLBACK TO SAVEPOINT a");
      await expect(c.query("SELECT sessions.ingest_buffer_gc_ordinals($1::uuid)", [live.stage])).rejects.toMatchObject({ code: "55000", message: expect.stringContaining("immutable ACK decisions required") });
    } finally { await release(c); }
    expect(await decisionRows(live.claim!)).toBe(1);
  }, 60000);

  test("P1 sequential: retired decisions close registration; without the interlock a registration would be invisible", async () => {
    await configure({ enabled: true, dryRun: false, decisions: true });
    const control = await seed([{ id: "p1-control" }]), victim = await seed([{ id: "p1-victim" }]);
    await terminal(control); await terminal(victim);
    const controlReg = await registration(control), victimReg = await registration(victim);
    // Control (no cleanup of this claim): a committed registration is served by the reader...
    const c0 = await connect();
    try { await c0.query("BEGIN"); await register(c0, controlReg); await c0.query("COMMIT"); } finally { await release(c0); }
    await decisions(true);
    expect(await decisionRows(control.claim!)).toBeGreaterThan(0);   // ...and keeps its decisions (evidence row present)
    expect(await served(control, controlReg.records[0]!.recordId)).toHaveLength(1);
    // Victim: cleanup retires it first, irreversibly. A later registration for it is REFUSED.
    expect(await decisionRows(victim.claim!)).toBe(0);
    expect(await count("SELECT count(*) n FROM sessions.ingest_bookkeeping_retired_claims WHERE claim_token=$1", [victim.claim])).toBe(1);
    await expect(pool.query("DELETE FROM sessions.ingest_bookkeeping_retired_claims")).rejects.toMatchObject({ code: "55000" });
    const c1 = await connect();
    try {
      await c1.query("BEGIN");
      await expect(register(c1, victimReg)).rejects.toMatchObject({ code: "55000", message: expect.stringContaining("registration closed") });
    } finally { await release(c1); }
    expect(await count("SELECT count(*) n FROM sessions.babysitter_evidence_index WHERE claim_token=$1", [victim.claim])).toBe(0);
    // Proof by violation: with 042000's original register (no interlock), the same call SUCCEEDS but the
    // reader cannot serve it, because the decision it needs is gone. Rolled back.
    const original = text("supabase/migrations/20261105042000_babysitter_evidence_retention.sql");
    const at = original.indexOf("CREATE FUNCTION sessions.ingest_babysitter_retention_register(p jsonb)");
    const body = original.slice(at, original.indexOf("END $register$;", at) + "END $register$;".length).replace("CREATE FUNCTION", "CREATE OR REPLACE FUNCTION");
    const c2 = await connect();
    try {
      await c2.query("BEGIN");
      await c2.query(body);
      expect(await register(c2, victimReg)).toMatchObject({ objects: [] });
      expect(await count("SELECT count(*) n FROM sessions.babysitter_evidence_index WHERE claim_token=$1", [victim.claim])).toBe(0); // other conn: uncommitted
      expect(await served(victim, victimReg.records[0]!.recordId, c2)).toHaveLength(0);
      expect(await served(control, controlReg.records[0]!.recordId, c2)).toHaveLength(1);
    } finally { await release(c2); }
    await configure();
  }, 120000);

  test("P1 race, both orders: registration in flight => cleanup skips the claim; cleanup in flight => registration waits, then fails", async () => {
    await configure({ enabled: true, dryRun: false, decisions: true });
    // Order 1: register (uncommitted) BEFORE the cleanup evaluates NOT EXISTS.
    const z = await seed([{ id: "p1-race-z" }]); await terminal(z);
    const zReg = await registration(z), before = await decisionRows(z.claim!);
    const r = await connect();
    try {
      await r.query("BEGIN");
      await register(r, zReg);
      const started = Date.now();
      const page1 = await decisions(true);
      expect(Date.now() - started).toBeLessThan(2000);                  // try-lock: skipped, not waited
      expect(page1).toMatchObject({ done: false });
      expect(page1.skippedClaims).toBeGreaterThanOrEqual(1);
      expect(await decisionRows(z.claim!)).toBe(before);
      await r.query("COMMIT");
    } finally { await release(r); }
    expect((await decisions(true)).blocked.babysitter).toBeGreaterThanOrEqual(1);
    expect(await decisionRows(z.claim!)).toBe(before);
    expect(await served(z, zReg.records[0]!.recordId)).toHaveLength(1);
    expect(await count("SELECT count(*) n FROM sessions.ingest_bookkeeping_retired_claims WHERE claim_token=$1", [z.claim])).toBe(0);
    // Order 2: the cleanup retires (uncommitted) BEFORE the registration; the registration blocks on the
    // claim lock, then sees the committed retirement and fails. Nothing invisible is ever registered.
    const w = await seed([{ id: "p1-race-w" }]); await terminal(w);
    const wReg = await registration(w);
    const cl = await connect(), rg = await connect();
    try {
      await cl.query("BEGIN;SET LOCAL statement_timeout=3000");
      const res = (await cl.query("SELECT sessions.ingest_bookkeeping_cleanup_decisions($1::jsonb) r", [JSON.stringify({ after: null, rowBudget: 5000, execute: true })])).rows[0].r;
      expect(res.deleted).toBeGreaterThanOrEqual(1);
      await rg.query("BEGIN;SET LOCAL lock_timeout='5s'");
      const pending = register(rg, wReg).then(() => "ok", (e) => e.code + ":" + e.message);
      await waitForBlocker(await pid(rg), await pid(cl));
      await cl.query("COMMIT");
      expect(await pending).toBe("55000:babysitter retention: claim decisions retired; registration closed");
    } finally { await release(cl); await release(rg); }
    expect(await decisionRows(w.claim!)).toBe(0);
    expect(await count("SELECT count(*) n FROM sessions.babysitter_evidence_index WHERE claim_token=$1", [w.claim])).toBe(0);
    await configure();
  }, 120000);

  test("non-contention: an in-flight claim's locks never block a page; a locked row is skipped and counted, never lost", async () => {
    await allOn();
    const s = await seed([{ id: "n-1" }]);
    await terminal(s);
    const holder = await connect();
    try {
      // Everything an ingest claim/ACK/GC transaction holds for this user and batch.
      await holder.query("BEGIN");
      await holder.query("SELECT sessions.ingest_lock_fences($1,$2,$3)", [s.tenant.orgId, s.tenant.workspaceId, s.tenant.userId]);
      await holder.query("SELECT 1 FROM sessions.ingest_batches WHERE batch_id=$1 FOR UPDATE", [s.batchId]);
      await holder.query("SELECT 1 FROM sessions.ingest_stage_intents WHERE claim_token=$1 FOR UPDATE", [s.claim]);
      await holder.query("SELECT 1 FROM sessions.ingest_buffer_outbox WHERE batch_id=$1 FOR UPDATE", [s.batchId]);
      await holder.query("SELECT 1 FROM sessions.ingest_claim_attempts WHERE claim_token=$1 FOR UPDATE", [s.claim]);
      await holder.query("SELECT 1 FROM sessions.ingest_receipt_seat_clock WHERE singleton FOR UPDATE");
      const started = Date.now();
      const r = await decisions(true, null, 5000);
      expect(Date.now() - started).toBeLessThan(2000);
      expect(r.deleted).toBeGreaterThanOrEqual(1);
      expect(await decisionRows(s.claim!)).toBe(0);
    } finally { await release(holder); }
    // Same for decisions: one locked decision row of a terminal claim keeps the run incomplete.
    const d = await seed([{ id: "k-dec-1" }, { id: "k-dec-2" }]);
    await terminal(d);
    const lock = await connect();
    try {
      await lock.query("BEGIN");
      await lock.query("SELECT 1 FROM sessions.ingest_lazy_ack_decisions WHERE claim_token=$1 AND ord=0 FOR UPDATE", [d.claim]);
      // A page that reached the END but skipped a locked row is never done: the skip is counted.
      const held = await decisions(true);
      expect(held).toMatchObject({ end: true, done: false });
      expect(held.skipped).toBeGreaterThanOrEqual(1);
      expect(await decisionRows(d.claim!)).toBe(1);
    } finally { await release(lock); }
    const after = await decisions(true);
    expect(after).toMatchObject({ end: true, skipped: 0, done: true });
    expect(await decisionRows(d.claim!)).toBe(0);
    // One cleaner per phase: a held phase lock answers busy instead of waiting.
    const other = await connect();
    try {
      await other.query("BEGIN");
      await other.query("SELECT sessions.ingest_bookkeeping_cleanup_try_lock('decisions')");
      expect(await decisions(true)).toEqual({ phase: "decisions", busy: true });
    } finally { await release(other); }
    await configure();
  }, 120000);

  test("the down is refused once any claim was retired (registration must stay closed)", async () => {
    expect((await one("SELECT count(*)::int n FROM sessions.ingest_bookkeeping_retired_claims")).n).toBeGreaterThan(0);
    // 043890's down is refused first (the gate must keep hiding retired claims) ...
    await expect(pool.query(text(GR_ROLLBACK))).rejects.toMatchObject({ code: "55000", message: expect.stringContaining("claims were retired") });
    await expect(pool.query(text(ROLLBACK))).rejects.toMatchObject({ code: "55000", message: expect.stringContaining("a later migration is applied") });
    // ... and 043880's own tombstone guard still holds with 043890's ledger row out of the way (rolled back, never committed).
    const c = await connect();
    try {
      await c.query("BEGIN");
      await c.query("DELETE FROM sessions.__96bulls_migrations WHERE name=$1", [GR_NAME]);
      await expect(c.query(text(ROLLBACK))).rejects.toMatchObject({ code: "55000", message: expect.stringContaining("claims were retired") });
    } finally { await release(c); }
    expect((await one("SELECT count(*)::int n FROM sessions.__96bulls_migrations WHERE name=$1", [NAME])).n).toBe(1);
    expect((await one("SELECT count(*)::int n FROM sessions.__96bulls_migrations WHERE name=$1", [GR_NAME])).n).toBe(1);
  }, 60000);

});
