/** Opt-in OWNED PG17 (SESSION_INGEST_TEST_PG_URL, loopback, user postgres, no password): every migration on
 * main through the real migrator, then the DRAFT docs/proposals/ingest-bookkeeping-cleanup.draft.sql.
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
import { runBookkeepingCleanup } from "../bookkeeping-cleanup.ts";

const url = process.env.SESSION_INGEST_TEST_PG_URL;
if (url) {
  const u = new URL(url);
  if (!["postgresql:", "postgres:"].includes(u.protocol) || !["127.0.0.1", "localhost", "[::1]"].includes(u.hostname) || u.username !== "postgres" ||
      u.password || u.pathname !== "/postgres" || u.search || u.hash) throw Error("owned_loopback_fixture_required");
}
const root = join(import.meta.dir, "../../..");
const text = (path: string) => readFileSync(resolve(root, path), "utf8");
// Overridable only so a mutation run can prove each predicate is load-bearing (see the PR evidence).
const DRAFT = process.env.BOOKKEEPING_CLEANUP_DRAFT_SQL ?? "docs/proposals/ingest-bookkeeping-cleanup.draft.sql";
const INDEXES = "docs/proposals/ingest-bookkeeping-cleanup.indexes.draft.sql";
const PHASE_A_DOWN = "supabase/rollbacks/20261105043880_ingest_bookkeeping_cleanup.down.sql";
const indexStatements = text(INDEXES).split("\n").filter((l) => !l.startsWith("--")).join("\n").split(";").map((s) => s.trim()).filter((s) => s.startsWith("CREATE INDEX"));
const epoch = "3".repeat(64), identity = "c".repeat(64), hex = (c: string) => c.repeat(64);
const profile: MigrationProfile = { mode: "local", shim: true, bootstrap: true, verify: true, runtimeRole: "postgres" };
type Current = { revision: number; digest?: string };
type Rec = { id: string; revision?: number; session?: string; payload?: string; current?: Current };
type Seed = { tenant: typeof TENANT; batchId: string; stage: string | null; claim: string | null; outcome: string; again: () => Promise<string> };

describe.skipIf(!url)("ingest bookkeeping cleanup (draft) on owned PG17", () => {
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
  const seats = (execute: boolean, after: string | null = null, limit = 1000) => page("seats", { after, limit, execute });
  const custody = (execute: boolean, after: string | null = null, limit = 1000) => page("custody", { after, limit, execute });
  const configure = (o: Partial<Record<"enabled" | "dryRun" | "decisions" | "seats" | "custody", boolean>> & { minAgeDays?: number } = {}) =>
    pool.query("SELECT sessions.ingest_bookkeeping_cleanup_configure($1::jsonb)", [JSON.stringify({ enabled: false, dryRun: true, decisions: false,
      seats: false, custody: false, minAgeDays: 7, ...o })]);
  const allOn = () => configure({ enabled: true, dryRun: false, decisions: true, seats: true, custody: true });
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
  const ageSeats = () => pool.query("UPDATE sessions.ingest_receipt_seats SET created_at=clock_timestamp()-interval '8 days'");
  // Fixture only: custody age is its slot's initialized_at (the cleanup itself never writes slots).
  const ageSlots = () => pool.query("UPDATE sessions.ingest_lazy_native_slots SET initialized_at=clock_timestamp()-interval '8 days'");
  const plan = async (q: string) => {
    const c = await connect();
    try { await c.query("BEGIN;SET LOCAL enable_seqscan=off"); return (await c.query("EXPLAIN " + q)).rows.map((r) => r["QUERY PLAN"]).join("\n"); }
    finally { await release(c); }
  };
  const txSql = { transaction: async <T>(work: (q: (s: string, p: unknown[]) => Promise<{ rows: Record<string, unknown>[] }>) => Promise<T>) => {
    const c = await connect();
    try { await c.query("BEGIN"); const r = await work((s, p) => c.query(s, p as never) as never); await c.query("COMMIT"); return r; }
    finally { await release(c); }
  } };
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
    const c = await connect(); try { await runMigrations(c, { profile }); } finally { await release(c); }
    generation = (await derivedSchemaAssets()).expected.generation;
    await pool.query("SELECT sessions.ingest_lazy_policy_set($1::jsonb)", [JSON.stringify({ baselineMode: "lazy-current", generation,
      sourceIdentitySha256: identity, controlEpoch: epoch, privacyBaselineSha256: hex("a"), retentionContractSha256: hex("b"),
      sourceProofSha256: hex("d"), validUntil: new Date(Date.now() + 3600000).toISOString(), issuer: "relayhistory-cloud" })]);
    await pool.query("SELECT sessions.ingest_receipt_seat_initialize($1::jsonb)", [JSON.stringify({ protocol: "delivery-acceptance-floor-v1", controlEpoch: epoch,
      capturedAt: "2026-11-04T00:00:00.000000Z", sourceSequences: ["sessions.delivery_catalog_acceptance_seq", "sessions.delivery_projection_acceptance_seq"].map((sequenceRegclass, i) => ({ sequenceRegclass, sequenceOid: String(i + 1), allocatedHighWater: "9007199254740993", isCalled: true, incrementBy: "1", cycle: false })),
      allocatedHighWater: "9007199254740993", generation, sourceIdentitySha256: identity, sourceProofSha256: hex("d") })]);
    // The read role's schema USAGE is its operator step (supabase/bootstrap/session_read_api_role.sql), not a migration.
    await pool.query("GRANT USAGE ON SCHEMA sessions TO bulls96_read_api");
    // The draft is the full Phase A+B+C proposal; its Phase A is registered as 043880, which it supersedes on this
    // scratch database. 043880's own guarded down (a never-used install, 043880 the latest) removes it first,
    // then the draft is loaded AFTER the real migrations, exactly as a registered slot would be.
    await pool.query(text(PHASE_A_DOWN));
    await pool.query(text(DRAFT));
    console.log(JSON.stringify({ proof: "bookkeeping-cleanup-fixture", serverVersionNum: version, generation, draft: DRAFT }));
  }, 180000);
  afterAll(async () => {
    for (const c of open) await release(c);
    await pool?.end(); scratch?.done();
    if (admin) { if (name) await admin.query("DROP DATABASE IF EXISTS " + name + " WITH(FORCE)"); await admin.end(); }
  }, 30000);

  test("dormant default and gates: settings off, no unbounded timeout, phase indexes required, owner-only", async () => {
    const s = await one("SELECT enabled,dry_run,decisions_enabled,seats_enabled,custody_enabled,min_age::text FROM sessions.ingest_bookkeeping_cleanup_settings");
    expect(s).toEqual({ enabled: false, dry_run: true, decisions_enabled: false, seats_enabled: false, custody_enabled: false, min_age: "7 days" });
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
    // Phase B refuses (dry run included) while any FK-side index is missing.
    await expect(seats(false)).rejects.toMatchObject({ code: "55000", message: expect.stringContaining("phase B indexes") });
    await expect(custody(false)).rejects.toMatchObject({ code: "55000", message: expect.stringContaining("phase B indexes") });
    // Frozen has no delete at all: execute is never permitted, census is read-only.
    await expect(page("frozen_census", { after: null, limit: 10 })).resolves.toMatchObject({ mode: "census", scannedRows: 0 });
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

  test("Phase B seats: unreferenced, non-current, aged seats only; the current seat and FK stay authoritative", async () => {
    // All other exact Phase B indexes plus ONE same-name wrong one: the gate refuses, and the RI/predicate
    // probe that index should serve seq-scans (the hazard the definition check closes).
    const wrong = "CREATE INDEX ingest_lazy_ack_decisions_seat_key_idx ON sessions.ingest_lazy_ack_decisions(ord)";
    await pool.query(wrong);
    for (const s of indexStatements.slice(2)) await pool.query(s);
    await expect(seats(false)).rejects.toMatchObject({ code: "55000", message: expect.stringContaining("phase B indexes") });
    await expect(custody(false)).rejects.toMatchObject({ code: "55000", message: expect.stringContaining("phase B indexes") });
    expect(await plan("SELECT 1 FROM sessions.ingest_lazy_ack_decisions WHERE seat_key_sha256=repeat('a',64)")).toContain("Seq Scan");
    await pool.query("DROP INDEX sessions.ingest_lazy_ack_decisions_seat_key_idx");
    await pool.query(indexStatements[1]!);
    expect(await plan("SELECT 1 FROM sessions.ingest_lazy_ack_decisions WHERE seat_key_sha256=repeat('a',64)")).not.toContain("Seq Scan");
    // A wrong PREDICATE on the same name is drift too.
    await pool.query("DROP INDEX sessions.ingest_lazy_native_slots_current_seat_idx");
    await pool.query("CREATE INDEX ingest_lazy_native_slots_current_seat_idx ON sessions.ingest_lazy_native_slots(current_seat_key) WHERE current_owner IS NOT NULL");
    await expect(seats(false)).rejects.toMatchObject({ code: "55000" });
    await pool.query("DROP INDEX sessions.ingest_lazy_native_slots_current_seat_idx");
    await pool.query(indexStatements[2]!);
    const t = { ...TENANT, userId: "bk-seat-user" };
    const first = await seed([{ id: "s-rec", revision: 1 }], t);
    const firstSeat = (await one("SELECT seat_key_sha256 k FROM sessions.ingest_lazy_ack_decisions WHERE claim_token=$1", [first.claim])).k as string;
    await terminal(first);
    const second = await seed([{ id: "s-rec", revision: 2 }], t);          // higher revision: new current seat
    expect(second.outcome).toBe("accepted_buffered");
    const current = (await slot(t, "s-rec")).current_seat_key as string;
    expect(current).not.toBe(firstSeat);
    await ageSeats();
    const seatRow = (k: string) => count("SELECT count(*) n FROM sessions.ingest_receipt_seats WHERE key_sha256=$1", [k]);
    // Still referenced by the (terminal, not yet cleaned) decision: kept.
    let dry = await seats(false);
    expect(dry.blocked.decision).toBeGreaterThanOrEqual(1); expect(dry.blocked.currentSlot).toBeGreaterThanOrEqual(1);
    await allOn();
    await seats(true);
    expect(await seatRow(firstSeat)).toBe(1);
    // Phase A removes the reference; now the superseded seat is eligible, the current one never.
    await decisions(true);
    expect(await decisionRows(first.claim!)).toBe(0);
    await seats(true);
    expect(await seatRow(firstSeat)).toBe(0);
    expect(await seatRow(current)).toBe(1);
    // A seat inside min_age is kept even when unreferenced.
    const third = await seed([{ id: "s-young", revision: 1 }], t);
    const youngSeat = (await one("SELECT seat_key_sha256 k FROM sessions.ingest_lazy_ack_decisions WHERE claim_token=$1", [third.claim])).k as string;
    await terminal(third); await seed([{ id: "s-young", revision: 2 }], t); await decisions(true);
    expect(await decisionRows(third.claim!)).toBe(0);
    expect((await slot(t, "s-young")).current_seat_key).not.toBe(youngSeat);
    await pool.query("UPDATE sessions.ingest_receipt_seats SET created_at=clock_timestamp() WHERE key_sha256=$1", [youngSeat]);
    dry = await seats(false);
    expect(dry.blocked.young).toBeGreaterThanOrEqual(1);
    await seats(true);
    expect(await seatRow(youngSeat)).toBe(1);
    // Proof by violation: the FK refuses deleting the slot's current seat directly.
    const c = await connect();
    try {
      await c.query("BEGIN");
      await expect(c.query("DELETE FROM sessions.ingest_receipt_seats WHERE key_sha256=$1", [current])).rejects.toMatchObject({ code: "23503" });
    } finally { await release(c); }
    // After cleanup the record still behaves: an equal revision is stale, a higher one is accepted.
    const slotBefore = await slot(t, "s-rec");
    await seed([{ id: "s-rec", revision: 2, payload: "different body" }], t);
    expect(await slot(t, "s-rec")).toEqual(slotBefore);
    const fourth = await seed([{ id: "s-rec", revision: 3 }], t);
    expect(fourth.outcome).toBe("accepted_buffered");
    expect(Number((await slot(t, "s-rec")).current_revision)).toBe(3);
    await configure();
  }, 120000);

  test("Phase B custody: detached live-witness rows only; current and imported custody are kept", async () => {
    const t = { ...TENANT, userId: "bk-custody-user" };
    // r-up has a Neon current rev1 and is uploaded at rev2 (accepted: custody detaches at ACK);
    // r-stale has a Neon current rev3 and is uploaded at rev1 (stale: custody stays current).
    const s = await seed([{ id: "c-up", revision: 2, current: { revision: 1 } }, { id: "c-stale", revision: 1, current: { revision: 3 } }], t);
    expect(s.outcome).toBe("accepted_buffered");
    const up = await slot(t, "c-up"), stale = await slot(t, "c-stale");
    expect(up.current_legacy_key).toBeNull(); expect(stale.current_legacy_key).not.toBeNull();
    const detached = (await one("SELECT key_sha256 k FROM sessions.ingest_custody_records WHERE user_id=$1 AND record_id='c-up'", [t.userId])).k as string;
    const custodyRow = (k: string) => count("SELECT count(*) n FROM sessions.ingest_custody_records WHERE key_sha256=$1", [k]);
    // An imported-kind row (eager ceremony shape), unreferenced: excluded by kind, never deleted.
    const imp = (await one(`INSERT INTO sessions.ingest_custody_imports(org_id,generation,control_epoch,source_identity_sha256,source_proof_sha256,
      expected_records,expected_pages,expected_chain,status) VALUES('bk-import-org',repeat('a',64),repeat('3',64),repeat('c',64),repeat('d',64),1,1,repeat('0',64),'complete') RETURNING import_token::text t`)).t;
    await pool.query(`INSERT INTO sessions.ingest_custody_records(key_sha256,import_token,page,source_key_sha256,org_id,workspace_id,user_id,origin_id,record_id,revision,
      digest,source,session_id,metadata,reset_epoch,fence_epoch,baseline_fingerprint) VALUES(repeat('9',64),$1,0,repeat('8',64),'bk-import-org','ws','u','o','r',1,
      repeat('7',64),'claude','s','{}',0,0,repeat('6',64))`, [imp]);
    // Give it an old, unrelated slot so ONLY its kind keeps it (age and references would otherwise allow it).
    await pool.query(`INSERT INTO sessions.ingest_lazy_native_slots(key_sha256,key_material,org_id,workspace_id,origin_id,record_id,source_identity_sha256,
      control_epoch,initialized_witness_sha256,initialized_at) SELECT encode(sha256(convert_to(m::text,'UTF8')),'hex'),m,'bk-import-org','ws','o','r',repeat('c',64),
      repeat('3',64),repeat('5',64),clock_timestamp()-interval '30 days' FROM (SELECT jsonb_build_array('global-native-custody-v1','bk-import-org','ws','o','r') m) q`);
    await allOn();
    // Inside min_age (slot just initialized) a detached row is kept: custody ages like seats.
    const young = await custody(true);
    expect(young.blocked.young).toBeGreaterThanOrEqual(1);
    expect(await custodyRow(detached)).toBe(1);
    await ageSlots();
    const run = await custody(true);
    expect(run.blocked.currentSlot).toBeGreaterThanOrEqual(1); expect(run.blocked.imported).toBe(1);
    // Nothing is locked here, so every eligible row is deleted: an imported row may never count as eligible.
    expect(run.deleted).toBe(run.eligibleRows);
    expect(await custodyRow(detached)).toBe(0);
    expect(await custodyRow(stale.current_legacy_key)).toBe(1);
    expect(await custodyRow("9".repeat(64))).toBe(1);
    // A later accepted revision detaches the stale record's custody; the next page removes it.
    await seed([{ id: "c-stale", revision: 5 }], t);
    expect((await slot(t, "c-stale")).current_legacy_key).toBeNull();
    await custody(true);
    expect(await custodyRow(stale.current_legacy_key)).toBe(0);
    // Native slots are never touched by any phase.
    expect(await count("SELECT count(*) n FROM sessions.ingest_lazy_native_slots WHERE key_sha256 IN($1,$2)", [up.key_sha256, stale.key_sha256])).toBe(2);
    await configure();
  }, 120000);

  test("non-contention: an in-flight claim's locks never block a page; KEY SHARE on a seat means skip, not wait", async () => {
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
    // A seat whose row is KEY SHARE-locked (what an ACK inserting a reference holds) is skipped at once, and a
    // single multi-page operator run that skipped it can NOT report done: it says incomplete and where to resume.
    const t = { ...TENANT, userId: "bk-skip-user" };
    const a = await seed([{ id: "k-rec", revision: 1 }], t);
    const seatA = (await one("SELECT seat_key_sha256 k FROM sessions.ingest_lazy_ack_decisions WHERE claim_token=$1", [a.claim])).k as string;
    await terminal(a); await seed([{ id: "k-rec", revision: 2 }], t); await decisions(true); await ageSeats();
    const seatRow = () => count("SELECT count(*) n FROM sessions.ingest_receipt_seats WHERE key_sha256=$1", [seatA]);
    const run = (phase: "seats" | "decisions", after: unknown = null) => runBookkeepingCleanup({ sql: txSql, phase, limit: phase === "seats" ? 1 : 500,
      maxPages: 10000, execute: true, pauseMs: 0, statementTimeoutMs: 3000, after });
    const share = await connect();
    let held;
    try {
      await share.query("BEGIN");
      await share.query("SELECT 1 FROM sessions.ingest_receipt_seats WHERE key_sha256=$1 FOR KEY SHARE", [seatA]);
      const started = Date.now();
      held = await run("seats");
      expect(Date.now() - started).toBeLessThan(10000);
      expect(held).toMatchObject({ done: false, incomplete: "locked", next: null });
      expect(held.skipped).toBeGreaterThanOrEqual(1);
      expect(held.pages).toBeGreaterThan(1);
      expect(await seatRow()).toBe(1);
    } finally { await release(share); }
    // The resume cursor revisits the skipped seat: the rerun deletes it and only then reports done.
    expect(await run("seats", held!.resumeFrom)).toMatchObject({ done: true, incomplete: null, skipped: 0 });
    expect(await seatRow()).toBe(0);
    // Same for decisions: one locked decision row of a terminal claim keeps the run incomplete.
    const d = await seed([{ id: "k-dec-1" }, { id: "k-dec-2" }]);
    await terminal(d);
    const lock = await connect();
    let heldD;
    try {
      await lock.query("BEGIN");
      await lock.query("SELECT 1 FROM sessions.ingest_lazy_ack_decisions WHERE claim_token=$1 AND ord=0 FOR UPDATE", [d.claim]);
      heldD = await run("decisions");
      expect(heldD).toMatchObject({ done: false, incomplete: "locked" });
      expect(await decisionRows(d.claim!)).toBe(1);
    } finally { await release(lock); }
    expect(await run("decisions", heldD!.resumeFrom)).toMatchObject({ done: true, skipped: 0 });
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

  test("Phase C frozen census is read-only: counts slot-initialized and owned-covered rows, deletes nothing", async () => {
    const t = { ...TENANT, userId: "bk-frozen-user" };
    await seed([{ id: "f-slot" }], t);
    await pool.query(`INSERT INTO sessions.ingest_frozen_source_records(org_id,workspace_id,origin_id,record_id,user_id,source,session_id,revision,digest,operation)
      VALUES($1,$2,'origin-1','f-slot',$3,'claude','f-owned',1,repeat('1',64),'upsert'),($1,$2,'origin-1','f-noslot-owned',$3,'claude','f-owned',1,repeat('2',64),'upsert'),
      ($1,$2,'origin-1','f-slot-unowned',$3,'claude','f-unowned',1,repeat('3',64),'upsert'),($1,$2,'origin-1','f-null-session',$3,'claude',NULL,1,repeat('4',64),'upsert')`,
      [t.orgId, t.workspaceId, t.userId]);
    await pool.query("INSERT INTO sessions.ingest_frozen_source_owned_sessions VALUES($1,$2,$3,'origin-1','claude','f-owned')", [t.orgId, t.workspaceId, t.userId]);
    const before = await count("SELECT count(*) n FROM sessions.ingest_frozen_source_records");
    const r = await page("frozen_census", { after: null, limit: 5000 });
    expect(r).toMatchObject({ mode: "census", scannedRows: 4, slotInitialized: 1, ownedCovered: 3, bothConditions: 1, done: true });
    expect(await count("SELECT count(*) n FROM sessions.ingest_frozen_source_records")).toBe(before);
    await expect(page("frozen_census", { after: null, limit: 5000, execute: true })).rejects.toMatchObject({ code: "22023" });
  }, 60000);

  test("every keyset window and reference probe is index-servable (no seq scan, no sort) once its indexes exist", async () => {
    const plans: Record<string, string> = {
      decisionsWindow: "SELECT claim_token FROM sessions.ingest_lazy_ack_decisions WHERE claim_token>'00000000-0000-0000-0000-000000000000' ORDER BY claim_token,ord LIMIT 5000",
      seatsWindow: "SELECT key_sha256 FROM sessions.ingest_receipt_seats WHERE seat>0 ORDER BY seat LIMIT 1000",
      custodyWindow: "SELECT key_sha256 FROM sessions.ingest_custody_records WHERE key_sha256>'' ORDER BY key_sha256 LIMIT 1000",
      frozenWindow: "SELECT * FROM sessions.ingest_frozen_source_records WHERE (org_id,workspace_id,origin_id,record_id)>('a','b','c','d') ORDER BY org_id,workspace_id,origin_id,record_id LIMIT 10",
      stageProbe: "SELECT 1 FROM sessions.ingest_stage_intents WHERE claim_token='00000000-0000-0000-0000-000000000001' AND cleanup_state<>'redacted'",
      babysitterProbe: "SELECT 1 FROM sessions.babysitter_evidence_index WHERE claim_token='00000000-0000-0000-0000-000000000001'",
      seatByDecision: "SELECT 1 FROM sessions.ingest_lazy_ack_decisions WHERE seat_key_sha256=repeat('a',64)",
      seatBySlot: "SELECT 1 FROM sessions.ingest_lazy_native_slots WHERE current_seat_key=repeat('a',64)",
      custodyBySlot: "SELECT 1 FROM sessions.ingest_lazy_native_slots WHERE current_legacy_key=repeat('a',64)",
      seatByHead: "SELECT 1 FROM sessions.ingest_receipt_record_heads WHERE winner_key_sha256=repeat('a',64)",
      seatByOrd: "SELECT 1 FROM sessions.ingest_receipt_ord_seats WHERE key_sha256=repeat('a',64)",
    };
    const c = await connect();
    try {
      await c.query("BEGIN;SET LOCAL enable_seqscan=off;SET LOCAL enable_sort=off");
      for (const [k, q] of Object.entries(plans)) {
        const plan = (await c.query("EXPLAIN (FORMAT TEXT) " + q)).rows.map((r) => r["QUERY PLAN"]).join("\n");
        expect({ k, seq: /Seq Scan/.test(plan), sort: /\bSort\b/.test(plan) }).toEqual({ k, seq: false, sort: false });
      }
    } finally { await release(c); }
  }, 60000);

  test("operator tool: dry run by default, keyset paging, execute still refused while SQL is disabled", async () => {
    await configure();
    const sql = { transaction: async <T>(work: (q: (s: string, p: unknown[]) => Promise<{ rows: Record<string, unknown>[] }>) => Promise<T>) => {
      const c = await connect();
      try { await c.query("BEGIN"); const r = await work((s, p) => c.query(s, p as never) as never); await c.query("COMMIT"); return r; }
      finally { await release(c); }
    } };
    const report = await runBookkeepingCleanup({ sql, phase: "decisions", limit: 500, maxPages: 50, execute: false, pauseMs: 0, statementTimeoutMs: 3000 });
    expect(report).toMatchObject({ phase: "decisions", mode: "dry-run", deleted: 0, busy: false });
    expect(report.pages).toBeGreaterThanOrEqual(1);
    await expect(runBookkeepingCleanup({ sql, phase: "decisions", limit: 500, maxPages: 1, execute: true, pauseMs: 0, statementTimeoutMs: 3000 }))
      .rejects.toMatchObject({ code: "55000" });
  }, 60000);
});
