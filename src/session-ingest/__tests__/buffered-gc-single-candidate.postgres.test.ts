/** Opt-in OWNED PG17: actual registered migration/current generation, no cloud or real credential. */
import { afterAll, beforeAll, beforeEach, describe, expect, test } from "bun:test";
import pg, { type PoolClient } from "pg";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { createHash } from "node:crypto";
import { deliverBatch } from "../deliver.ts";
import { sqlLedger } from "../ledger-sql.ts";
import { deliveryAccount, prepareDelivery } from "../vendor/delivery/prepare.ts";
import { bufferedGcTick, collectBufferedTicket, sqlBufferedGcLedger } from "../buffered-gc.ts";
import { SemanticPostgresExecutor } from "../../search-features/semantic-copy-postgres.ts";
import { batch, record, scratchStore, TENANT } from "./support.ts";
import { runMigrations, type MigrationProfile } from "../../hoopdb/migrate.ts";
import { derivedSchemaAssets } from "../../session-pipeline/derived-schema.ts";
import { schemaVerdict, type Transact } from "../../shared/db.ts";

const url = process.env.SESSION_INGEST_TEST_PG_URL;
// Refuse credentials, remote endpoints and nonfixture database identities before connecting.
if (url) {
  const u = new URL(url);
  if (!["postgresql:", "postgres:"].includes(u.protocol) || !["127.0.0.1", "localhost", "[::1]"].includes(u.hostname) || u.username !== "postgres" ||
      u.password || u.pathname !== "/postgres" || u.search || u.hash) throw Error("owned_loopback_fixture_required");
}
const root = join(import.meta.dir, "../../..");
let generation: string;
const epoch = "3".repeat(64), identity = "c".repeat(64);
const fixturePath = "supabase/migrations/20261105039000_buffered_gc_single_candidate.sql";
const migrationName = "upstream:20261105039000_buffered_gc_single_candidate.sql";
const rollbackPath = "supabase/rollbacks/20261105039000_buffered_gc_single_candidate.down.sql";
const profile: MigrationProfile = { mode: "local", shim: true, bootstrap: true, verify: true, runtimeRole: "postgres" };
const text = (path: string) => readFileSync(join(root, path), "utf8").split("\n").filter((l) => !l.startsWith("\\")).join("\n");
const originalMigration = text("supabase/migrations/20261104055000_lazy_current_custody.sql");
const start = originalMigration.indexOf("CREATE OR REPLACE FUNCTION sessions.ingest_buffer_gc_claim(p jsonb)");
const originalClaim = originalMigration.slice(start, originalMigration.indexOf("END $claim$;", start) + "END $claim$;".length);
const advisory = "SELECT pg_advisory_xact_lock(hashtextextended(jsonb_build_array('96bulls:buffer-gc-user-v1',$1::text,$2::text,$3::text)::text,0))";
type Seed = { tenant: typeof TENANT; stage: string; batchId: string; records: ReturnType<typeof record>[] };

describe.skipIf(!url)("offline one-candidate GC SQL prototype", () => {
  let admin: pg.Client, pool: pg.Pool, runtime: pg.Pool, name: string;
  let scratch: ReturnType<typeof scratchStore>, serial = 0;
  const sessions = new Set<PoolClient>();
  const connect = async () => { const c = await pool.connect(); sessions.add(c); return c; };
  const release = async (c: PoolClient) => { await c.query("ROLLBACK").catch(() => {}); sessions.delete(c); c.release(); };
  const rpc = async (fn: string, body: unknown, timeout = 5000) => {
    const c = await connect();
    try {
      await c.query(`BEGIN;SET LOCAL statement_timeout=${timeout};SET LOCAL ROLE bulls96_session_ingest`);
      const r = await c.query(`SELECT sessions.${fn}($1::jsonb) AS result`, [JSON.stringify(body)]);
      await c.query("COMMIT"); return r;
    } finally { await release(c); }
  };
  const claim = (limit = 1, timeout = 5000) => rpc("ingest_buffer_gc_claim", { limit, generation }, timeout);
  const meta = async (stage: string) => (await pool.query("SELECT gc_checked_at::text,gc_lease_token::text,gc_lease_until::text,row_version::text,cleanup_state,receipt_state FROM sessions.ingest_stage_intents WHERE stage_token=$1", [stage])).rows[0];
  const prioritize = async (...seeds: Seed[]) => {
    await pool.query("UPDATE sessions.ingest_stage_intents SET gc_checked_at=clock_timestamp()+interval '1 day'");
    for (const [i, s] of seeds.entries()) await pool.query("UPDATE sessions.ingest_stage_intents SET gc_checked_at='2000-01-01'::timestamptz+$2::int*interval '1 second' WHERE stage_token=$1", [s.stage, i]);
  };
  const waitForBlocker = async (waiter: number, blocker: number) => {
    const end = Date.now() + 4000;
    while (Date.now() < end) {
      const pids = (await pool.query("SELECT pg_blocking_pids($1) AS pids", [waiter])).rows[0].pids as number[];
      if (pids.includes(blocker)) return;
      await Bun.sleep(10);
    }
    throw Error("expected_fixture_lock_wait_missing");
  };
  async function seed(label: string, count = 1, sameSession = false, previous?: Seed, options: { tenant?: typeof TENANT; beforeReceipt?: boolean } = {}): Promise<Seed> {
    const suffix = `${label}-${++serial}`;
    const tenant = options.tenant ?? previous?.tenant ?? { ...TENANT, userId: "gc-prototype-" + suffix };
    const records = Array.from({ length: count }, (_, i) => record({
      record_id: previous?.records[i]?.record_id ?? `${suffix}-${String(i).padStart(4, "0")}`,
      revision: previous ? 2 : 1,
      session_id: sameSession ? `${label}-session` : `${suffix}-session-${i}`,
    }));
    const raw = await batch(records, { batch_id: suffix, account_id: await deliveryAccount(tenant) });
    const prepared = await prepareDelivery(tenant, raw);
    const witness = {
      version: 1, issuer: "relayhistory-cloud", sourceIdentitySha256: identity, controlEpoch: epoch,
      privacyBaselineSha256: "a".repeat(64), tenant, originId: prepared.originId, batchId: prepared.batchId,
      batchDigest: prepared.batchDigest, sourceControlVersion: "0", pending: false, pendingControl: null,
      privacyVersion: "0", originalRecordCount: records.length, candidateCount: prepared.records.length,
      records: prepared.records.map((r, i) => ({ preparedOrd: i, processingRank: i, candidateDigest: r.digest,
        recordId: r.record_id, revision: String(r.revision), revisionId: r.revision_id,
        candidateFenced: false, currentFenced: false, current: null })),
      privacy: { complete: true, deletions: [], resets: [] },
    };
    const sourceDigest = (await pool.query("SELECT encode(sha256(convert_to($1::jsonb::text,'UTF8')),'hex') AS d", [JSON.stringify(witness)])).rows[0].d;
    const ledger = sqlLedger(async (sql, params) => {
      const c = await connect();
      try { await c.query("BEGIN;SET LOCAL ROLE bulls96_session_ingest"); const r = await c.query(sql, params as never); await c.query("COMMIT"); return r as never; }
      finally { await release(c); }
    }, { acceptanceMode: "buffered", expectedGeneration: generation });
    if (options.beforeReceipt) ledger.commitBuffered = async () => { throw Error("synthetic pause before durable receipt"); };
    const delivery = deliverBatch({ ledger, acceptanceMode: "buffered", target: { store: scratch.store, keyRoot: "96bulls" }, custody: { witness, sourceDigest } } as never, tenant, raw);
    if (options.beforeReceipt) await expect(delivery).rejects.toMatchObject({ stage: "buffer_receipt" });
    else expect((await delivery).outcome).toBe("accepted_buffered");
    const stage = (await pool.query("SELECT stage_token::text FROM sessions.ingest_stage_intents WHERE batch_id=$1", [suffix])).rows[0].stage_token as string;
    return { tenant, stage, batchId: suffix, records };
  }
  async function fence(c: PoolClient, s: Seed) {
    await c.query("SELECT sessions.ingest_lock_fences($1,$2,$3)", [s.tenant.orgId, s.tenant.workspaceId, s.tenant.userId]);
  }
  const clear = (c: PoolClient, s: Seed) => c.query("SELECT sessions.ingest_lazy_clear($1::jsonb,'reset')", [JSON.stringify({ tenant: s.tenant })]);
  const migrate = async () => { const c = await connect(); try { return await runMigrations(c, { profile }); } finally { await release(c); } };
  const transact: Transact = async (work) => {
    const c = await connect(); try { await c.query("BEGIN"); const result = await work(c as never); await c.query("COMMIT"); return result; }
    finally { await release(c); }
  };

  beforeAll(async () => {
    admin = new pg.Client({ connectionString: url }); await admin.connect();
    const version = Number((await admin.query("SHOW server_version_num")).rows[0].server_version_num);
    expect(version).toBeGreaterThanOrEqual(170000); expect(version).toBeLessThan(180000);
    expect((await admin.query("SHOW server_encoding")).rows[0].server_encoding).toBe("UTF8");
    console.log(JSON.stringify({ proof: "gc-single-candidate-fixture", serverVersionNum: version, source: "all currently landed migrations plus registered 039" }));
    name = "gc_single_" + crypto.randomUUID().replaceAll("-", "");
    await admin.query("CREATE DATABASE " + name);
    const target = new URL(url!); target.pathname = "/" + name;
    pool = new pg.Pool({ connectionString: target.href, max: 8 });
    runtime = new pg.Pool({ connectionString: target.href, max: 2, connectionTimeoutMillis: 1000, options: "-c role=bulls96_session_ingest" });
    scratch = scratchStore();
    await pool.query("CREATE EXTENSION pg_trgm;CREATE EXTENSION btree_gin");
    await migrate();
    const assets = await derivedSchemaAssets(); generation = assets.expected.generation;
    expect(assets.expected.names).toContain(migrationName);
    expect(await schemaVerdict({ ...assets.expected, dbMode: "local" }, transact)).toEqual({ ok: true });
    // Exercise the same read-only four-verifier path as the actual GC login.
    const verifier = new SemanticPostgresExecutor(runtime, true);
    await verifier.queryBounded("SET LOCAL hoopdb.db_mode='local';" + Object.values(assets.verifiers).join("\n"), [], { signal: new AbortController().signal, deadline: Date.now() + 5000 });
    console.log(JSON.stringify({ proof: "gc-current-generation", generation, migrationCount: assets.expected.names.length, fourConsumerVerifiers: "pass", schemaVerdict: "pass" }));
    await pool.query("SELECT sessions.ingest_lazy_policy_set($1::jsonb)", [JSON.stringify({ baselineMode: "lazy-current", generation,
      sourceIdentitySha256: identity, controlEpoch: epoch, privacyBaselineSha256: "a".repeat(64), retentionContractSha256: "b".repeat(64),
      sourceProofSha256: "d".repeat(64), validUntil: new Date(Date.now() + 3600000).toISOString(), issuer: "relayhistory-cloud" })]);
    await pool.query("SELECT sessions.ingest_receipt_seat_initialize($1::jsonb)", [JSON.stringify({ protocol: "delivery-acceptance-floor-v1", controlEpoch: epoch,
      capturedAt: "2026-11-04T00:00:00.000000Z", sourceSequences: ["sessions.delivery_catalog_acceptance_seq", "sessions.delivery_projection_acceptance_seq"].map((sequenceRegclass, i) => ({ sequenceRegclass, sequenceOid: String(i + 1), allocatedHighWater: "9007199254740993", isCalled: true, incrementBy: "1", cycle: false })),
      allocatedHighWater: "9007199254740993", generation, sourceIdentitySha256: identity, sourceProofSha256: "d".repeat(64) })]);
  }, 120000);
  beforeEach(async () => { await pool.query(text(fixturePath)); });
  afterAll(async () => {
    for (const c of sessions) await release(c);
    await runtime?.end(); await pool?.end(); scratch?.done();
    if (admin) { if (name) await admin.query("DROP DATABASE IF EXISTS " + name + " WITH(FORCE)"); await admin.end(); }
  }, 30000);

  test("registered 039 preserves ACL and current verifiers; guarded down restores exact latest prior body and reapply restores PASS", async () => {
    const predecessors = readdirSync(join(root, "supabase/migrations")).filter((f) => f.endsWith(".sql") && f < "20261105039000_buffered_gc_single_candidate.sql").sort();
    const latest = predecessors.filter((f) => /CREATE(?: OR REPLACE)? FUNCTION sessions\.ingest_buffer_gc_claim\(/.test(text("supabase/migrations/" + f))).at(-1);
    expect(latest).toBe("20261104055000_lazy_current_custody.sql");
    const view = async () => (await pool.query("SELECT prosrc,proacl::text,proowner,prosecdef,proconfig FROM pg_proc WHERE oid='sessions.ingest_buffer_gc_claim(jsonb)'::regprocedure")).rows[0];
    const before = await view();
    const down = async () => { const c = await connect(); try { await c.query(text(rollbackPath)); } finally { await release(c); } };
    const checksum = createHash("sha256").update(text(fixturePath)).digest("hex");
    await pool.query("UPDATE sessions.__96bulls_migrations SET sha256=repeat('f',64) WHERE name=$1", [migrationName]);
    await expect(down()).rejects.toMatchObject({ code: "55000" }); expect(await view()).toEqual(before);
    await pool.query("UPDATE sessions.__96bulls_migrations SET sha256=$2 WHERE name=$1", [migrationName, checksum]);
    const later = "upstream:20261105099000_gc_rollback_guard_probe.sql";
    await pool.query("INSERT INTO sessions.__96bulls_migrations(name,sha256) VALUES($1,repeat('0',64))", [later]);
    await expect(down()).rejects.toMatchObject({ code: "55000" }); expect(await view()).toEqual(before);
    expect((await pool.query("SELECT status FROM sessions.__96bulls_role_validation WHERE name='session_directory_search_worker'")).rows[0].status).toBe("pass");
    await pool.query("DELETE FROM sessions.__96bulls_migrations WHERE name=$1", [later]);
    // Lanes registered after 039 (e.g. 046000) are applied too: the guard above rightly refuses
    // while they exist, so they roll back first, in descending order, through their own downs.
    const laterApplied = (await pool.query<{ name: string }>("SELECT name FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE \"C\" > $1 ORDER BY name COLLATE \"C\" DESC", [migrationName])).rows.map((r) => r.name);
    // 043000 (F2 brief titles) is one: while it is applied, the 039 down refuses (55000), unchanged.
    expect(laterApplied).toContain("upstream:20261105043000_brief_titles.sql");
    // 044000 (F3 PR evidence) is another; it rolls back first (descending order), before 043000.
    expect(laterApplied).toContain("upstream:20261105044000_pr_evidence.sql");
    await expect(down()).rejects.toMatchObject({ code: "55000" }); expect(await view()).toEqual(before);
    for (const n of laterApplied) {
      const c = await connect();
      try { await c.query(text(`supabase/rollbacks/${n.slice("upstream:".length).replace(/\.sql$/, ".down.sql")}`)); } finally { await release(c); }
    }
    const ledgerBefore = (await pool.query("SELECT name,sha256 FROM sessions.__96bulls_migrations ORDER BY name")).rows;
    await down();
    expect((await pool.query("SELECT name,sha256 FROM sessions.__96bulls_migrations ORDER BY name")).rows).toEqual(ledgerBefore.filter((r) => r.name !== migrationName));
    expect(await view()).toEqual({ ...before, prosrc: originalClaim.split("$claim$")[1] });
    expect((await pool.query("SELECT status FROM sessions.__96bulls_role_validation WHERE name='session_directory_search_worker'")).rows[0].status).toBe("pending");
    await expect(claim()).rejects.toMatchObject({ code: "55000" });
    await expect(down()).rejects.toMatchObject({ code: "55000" }); // absent own ledger is not permission to restore
    const assets = await derivedSchemaAssets();
    expect(await schemaVerdict({ ...assets.expected, dbMode: "local" }, transact)).toMatchObject({ ok: false, reason: "schema_pending" });
    expect((await migrate()).applied).toBe(1 + laterApplied.length);
    expect(await view()).toEqual(before);
    expect(await schemaVerdict({ ...assets.expected, dbMode: "local" }, transact)).toEqual({ ok: true });
    console.log(JSON.stringify({ proof: "gc-039-down-up", generation, ownLedgerOnly: true, laterMigrationRefused: true, checksumDriftRefused: true, exactPriorBodyAndAcl: true, pendingBeforeReapply: true, finalVerifier: "pass" }));
  }, 30000);

  test("unmodified G2 reproduces the cross-user fence/clock deadlock; prototype removes the cycle", async () => {
    const a = await seed("cycle-a"), b = await seed("cycle-b");
    await prioritize(a, b); await pool.query(originalClaim);
    const gc = await connect(), producer = await connect();
    try {
      await producer.query("BEGIN;SET LOCAL statement_timeout='5s';SET LOCAL deadlock_timeout='100ms'"); await fence(producer, b);
      await gc.query("BEGIN;SET LOCAL statement_timeout='5s';SET LOCAL deadlock_timeout='100ms';SET LOCAL ROLE bulls96_session_ingest");
      const gcPid = (await gc.query("SELECT pg_backend_pid() AS pid")).rows[0].pid;
      const producerPid = (await producer.query("SELECT pg_backend_pid() AS pid")).rows[0].pid;
      const legacy = gc.query("SELECT sessions.ingest_buffer_gc_claim($1::jsonb)", [JSON.stringify({ limit: 1, generation })])
        .then(() => "ok", (e) => e.code).finally(() => gc.query("ROLLBACK"));
      await waitForBlocker(gcPid, producerPid);
      const write = clear(producer, b).then(() => "ok", (e) => e.code).finally(() => producer.query("ROLLBACK"));
      const cycle = await Promise.all([legacy, write]);
      expect(cycle).toContain("40P01");
      console.log(JSON.stringify({ proof: "unmodified-g2-gc-deadlock", gcAndProducerOutcomes: cycle, fixtureDeadlockTimeoutMs: 100 }));
      await pool.query(text(fixturePath));
      await prioritize(a, b);
      await producer.query("BEGIN;SET LOCAL statement_timeout='5s'"); await fence(producer, b);
      await gc.query("BEGIN;SET LOCAL statement_timeout='5s';SET LOCAL ROLE bulls96_session_ingest");
      expect((await gc.query("SELECT sessions.ingest_buffer_gc_claim($1::jsonb) AS r", [JSON.stringify({ limit: 100, generation })])).rows[0].r).toEqual({ tickets: [] });
      const nextWrite = clear(producer, b).then(() => "ok", (e) => e.code);
      await waitForBlocker(producerPid, gcPid); // actual producer waits for A's SHARE, GC never waits for B
      await gc.query("COMMIT"); expect(await nextWrite).toBe("ok");
    } finally { await release(gc); await release(producer); }
    expect((await meta(a.stage)).gc_checked_at).not.toContain("2000-01-01");
    expect((await meta(b.stage)).gc_checked_at).toContain("2000-01-01");
  }, 20000);

  for (const lock of ["advisory", "fence", "batch", "intent", "outbox"]) test(`busy ${lock} head skips without advancement and releases attempted locks`, async () => {
    const busy = await seed("busy-" + lock), next = await seed("next-" + lock);
    await prioritize(busy, next); const before = await meta(busy.stage), holder = await connect(), gc = await connect();
    try {
      await holder.query("BEGIN");
      const t = busy.tenant;
      if (lock === "advisory") await holder.query(advisory, [t.orgId, t.workspaceId, t.userId]);
      else if (lock === "fence") await fence(holder, busy);
      else if (lock === "batch") await holder.query("SELECT 1 FROM sessions.ingest_batches WHERE batch_id=$1 FOR UPDATE", [busy.batchId]);
      else if (lock === "intent") await holder.query("SELECT 1 FROM sessions.ingest_stage_intents WHERE stage_token=$1 FOR UPDATE", [busy.stage]);
      else await holder.query("SELECT 1 FROM sessions.ingest_buffer_outbox WHERE batch_id=$1 FOR UPDATE", [busy.batchId]);
      await gc.query("BEGIN;SET LOCAL statement_timeout='750ms';SET LOCAL ROLE bulls96_session_ingest");
      expect((await gc.query("SELECT sessions.ingest_buffer_gc_claim($1::jsonb) AS result", [JSON.stringify({ limit: 1, generation })])).rows[0].result).toEqual({ tickets: [] });
      // Crucially, GC's outer transaction is STILL OPEN. A failed acquisition
      // must release its hint/fence/batch locks before it evaluates `next`.
      if (lock !== "advisory") expect((await holder.query("SELECT pg_try_advisory_xact_lock(hashtextextended(jsonb_build_array('96bulls:buffer-gc-user-v1',$1::text,$2::text,$3::text)::text,0)) AS ok", [t.orgId, t.workspaceId, t.userId])).rows[0].ok).toBe(true);
      await holder.query("SET LOCAL lock_timeout='250ms'");
      await fence(holder, busy);
      await gc.query("COMMIT");
      expect(await meta(busy.stage)).toEqual(before);
      expect((await meta(next.stage)).gc_checked_at).not.toContain("2000-01-01");
    } finally { await release(gc); await release(holder); }
  });

  test("all four busy acquisition candidates return no ticket and change no metadata", async () => {
    const seeds = await Promise.all([0, 1, 2, 3, 4].map((i) => seed("all-busy-" + i)));
    await prioritize(...seeds); const before = await Promise.all(seeds.map((s) => meta(s.stage))), holder = await connect();
    try {
      await holder.query("BEGIN"); for (const s of seeds.slice(0, 4)) await fence(holder, s);
      expect((await claim(100, 750)).rows[0].result).toEqual({ tickets: [] });
      expect(await Promise.all(seeds.map((s) => meta(s.stage)))).toEqual(before);
    } finally { await release(holder); }
  });

  test("a healthy preparing intent ends the claim after its one committed rotation", async () => {
    const preparing = await seed("preparing", 1, false, undefined, { beforeReceipt: true }), next = await seed("after-preparing");
    await prioritize(preparing, next); const beforeNext = await meta(next.stage);
    expect((await claim(100)).rows[0].result).toEqual({ tickets: [] });
    const prepared = await meta(preparing.stage);
    expect(prepared).toMatchObject({ receipt_state: "preparing", cleanup_state: "live", gc_lease_token: null });
    expect(prepared.gc_checked_at).not.toContain("2000-01-01"); expect(await meta(next.stage)).toEqual(beforeNext);
  });

  test("500 ordinals preserve every retained byte decision; profile is measured, not a production SLA", async () => {
    const s = await seed("profile", 500); const ms: number[] = [];
    for (let i = 0; i < 7; i++) {
      await prioritize(s); const c = await connect();
      try {
        await c.query("BEGIN;SET LOCAL statement_timeout='5s';SET LOCAL ROLE bulls96_session_ingest");
        const r = await c.query("WITH started AS MATERIALIZED(SELECT clock_timestamp() AS t), result AS MATERIALIZED(SELECT sessions.ingest_buffer_gc_claim($1::jsonb) AS r FROM started) SELECT r,extract(epoch FROM clock_timestamp()-t)*1000 AS ms FROM result,started", [JSON.stringify({ limit: 100, generation })]);
        expect(r.rows[0].r).toEqual({ tickets: [] }); ms.push(Number(r.rows[0].ms)); await c.query("COMMIT");
      } finally { await release(c); }
    }
    const sorted = [...ms].sort((a, b) => a - b);
    console.log(JSON.stringify({ proof: "gc-single-candidate-owned-pg17", records: 500, samplesMs: ms, medianMs: sorted[3], maxMs: sorted[6], productionDeadlineProven: false }));
    await prioritize(s);
    const exec = new SemanticPostgresExecutor(runtime);
    expect(await sqlBufferedGcLedger({ query: (sql, params) => exec.queryBounded(sql, params, { signal: new AbortController().signal, deadline: Date.now() + 1800 }) }, generation).claim(1)).toEqual([]);
    await seed("profile-successor", 1, false, s);
    await prioritize(s);
    const partial = (await claim()).rows[0].result.tickets;
    expect(partial).toHaveLength(1);
    expect(partial[0].keepOrdinals).toEqual(Array.from({ length: 499 }, (_, i) => i + 1));
    expect(partial[0].keepGroupIndexes).toEqual(Array.from({ length: 499 }, (_, i) => i + 1));
  }, 30000);

  test("same-session mixed group keeps only current winner ordinals and original lineage decisions", async () => {
    const old = await seed("mixed", 2, true); await seed("mixed-new", 1, true, old);
    await prioritize(old);
    const before = (await pool.query("SELECT n.lineage::text,d.ord,d.native_lineage::text FROM sessions.ingest_lazy_ack_decisions d JOIN sessions.ingest_stage_intents s USING(claim_token) JOIN sessions.ingest_lazy_native_slots n ON n.key_sha256=d.native_key_sha256 WHERE s.stage_token=$1 ORDER BY d.ord", [old.stage])).rows;
    expect(before.map((r) => [r.ord, r.lineage, r.native_lineage])).toEqual([[0, "2", "1"], [1, "1", "1"]]);
    const tickets = (await claim()).rows[0].result.tickets;
    expect(tickets).toHaveLength(1); expect(tickets[0]).toMatchObject({ stageToken: old.stage, groupOrdinals: [[0, 1]], keepOrdinals: [1], keepGroupIndexes: [0], phase: "plan", cleanupState: "live" });
    expect((await pool.query("SELECT native_lineage::text FROM sessions.ingest_lazy_ack_decisions WHERE claim_token=(SELECT claim_token FROM sessions.ingest_stage_intents WHERE stage_token=$1) ORDER BY ord", [old.stage])).rows.map((r) => r.native_lineage)).toEqual(["1", "1"]);
  });

  test("a registered prospective child survives a pre-PUT crash and ends its own scan without touching another candidate", async () => {
    const old = await seed("child", 2, true); await seed("child-successor", 1, true, old);
    const next = await seed("after-child"); await prioritize(old);
    const exec = new SemanticPostgresExecutor(runtime);
    const ledger = sqlBufferedGcLedger({ query: (sql, params) => exec.queryBounded(sql, params, { signal: new AbortController().signal, deadline: Date.now() + 1800 }) }, generation);
    const ticket = (await ledger.claim(1))[0]!; expect(ticket.stageToken).toBe(old.stage);
    const crashStore = new Proxy(scratch.store, { get(target, property) {
      if (property === "put") return async () => { throw Error("synthetic crash after replacement registration"); };
      const value = Reflect.get(target, property); return typeof value === "function" ? value.bind(target) : value;
    } });
    await expect(collectBufferedTicket(ticket, { ledger, store: crashStore, eraser: { eraseObjectExcept: async () => { throw Error("unexpected erase"); } }, keyRoot: "96bulls" })).rejects.toBeDefined();
    const child = (await pool.query("SELECT gc_replacement_stage_token::text AS id FROM sessions.ingest_stage_intents WHERE stage_token=$1", [old.stage])).rows[0].id as string;
    expect(child).toBeTruthy(); const childSeed = { ...old, stage: child };
    await prioritize(childSeed, next); const beforeNext = await meta(next.stage);
    expect((await claim(100)).rows[0].result).toEqual({ tickets: [] });
    expect(await meta(child)).toMatchObject({ cleanup_state: "live", receipt_state: "preparing", gc_lease_token: null });
    expect((await meta(child)).gc_checked_at).not.toContain("2000-01-01"); expect(await meta(next.stage)).toEqual(beforeNext);
  });

  for (const kind of ["selection", "reset"]) test(`${kind} privacy fences remove kept ordinals without changing receipt/lineage provenance`, async () => {
    const s = await seed(kind, 2, true), c = await connect();
    try {
      await c.query("BEGIN");
      const t = s.tenant;
      if (kind === "selection") await c.query("SELECT sessions.ingest_apply_selection($1,$2,$3,'origin-1',$4::jsonb,false)", [t.orgId, t.workspaceId, t.userId, JSON.stringify([["claude", `${kind}-session`]])]);
      else await c.query("SELECT sessions.ingest_apply_reset($1,$2,$3,'synthetic-reset')", [t.orgId, t.workspaceId, t.userId]);
      await c.query("SELECT sessions.ingest_lazy_clear($1::jsonb,$2)", [JSON.stringify({ tenant: t, originId: "origin-1", enabled: false, sessions: [["claude", `${kind}-session`]] }), kind]);
      await c.query("COMMIT");
    } finally { await release(c); }
    await prioritize(s); const tickets = (await claim()).rows[0].result.tickets;
    expect(tickets).toHaveLength(1); expect(tickets[0]).toMatchObject({ stageToken: s.stage, keepOrdinals: [], keepGroupIndexes: [], phase: "plan", receiptState: "acknowledged" });
    expect((await pool.query("SELECT bool_and(n.current_owner IS NULL) AS cleared,min(n.lineage)::text AS lineage FROM sessions.ingest_lazy_ack_decisions d JOIN sessions.ingest_stage_intents s USING(claim_token) JOIN sessions.ingest_lazy_native_slots n ON n.key_sha256=d.native_key_sha256 WHERE s.stage_token=$1", [s.stage])).rows[0]).toEqual({ cleared: true, lineage: "2" });
  });

  test("post-reset foreign owner has a new lineage; old owner's bytes never regain retention", async () => {
    const old = await seed("owner-old", 2, true);
    await pool.query("SELECT sessions.ingest_apply_reset($1,$2,$3,'owner-reset')", [old.tenant.orgId, old.tenant.workspaceId, old.tenant.userId]);
    await pool.query("SELECT sessions.ingest_lazy_clear($1::jsonb,'reset')", [JSON.stringify({ tenant: old.tenant })]);
    const successor = await seed("owner-new", 1, true, old, { tenant: { ...TENANT, userId: "gc-prototype-foreign-owner" } });
    await prioritize(old, successor);
    const result = (await claim(100)).rows[0].result.tickets;
    expect(result).toHaveLength(1);
    expect(result[0]).toMatchObject({ stageToken: old.stage, keepOrdinals: [], keepGroupIndexes: [], phase: "plan" });
    expect((await pool.query("SELECT current_owner,lineage::text FROM sessions.ingest_lazy_native_slots WHERE record_id=$1", [old.records[0]!.record_id])).rows[0])
      .toEqual({ current_owner: successor.tenant.userId, lineage: "3" });
    expect((await meta(successor.stage)).gc_lease_token).toBeNull();
  });

  test("a post-mutation statement timeout rolls back checked metadata and is never treated as a busy skip", async () => {
    const s = await seed("timeout"), c = await connect();
    await prioritize(s); const before = await meta(s.stage);
    await pool.query("CREATE FUNCTION sessions.__gc_fixture_delay() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.stage_token=current_setting('gc_fixture.stage',true)::uuid THEN PERFORM pg_sleep(0.5); END IF;RETURN NEW; END $$;CREATE TRIGGER gc_fixture_delay AFTER UPDATE OF gc_checked_at ON sessions.ingest_stage_intents FOR EACH ROW EXECUTE FUNCTION sessions.__gc_fixture_delay()");
    try {
      await c.query("BEGIN;SET LOCAL statement_timeout='100ms'");
      await c.query("SELECT set_config('gc_fixture.stage',$1,true)", [s.stage]);
      await c.query("SET LOCAL ROLE bulls96_session_ingest");
      const error = await c.query("SELECT sessions.ingest_buffer_gc_claim($1::jsonb)", [JSON.stringify({ limit: 1, generation })]).then(() => null, (e) => e);
      expect(error).toMatchObject({ code: "57014" });
      expect(String(error.where)).toContain("__gc_fixture_delay");
      await c.query("ROLLBACK"); expect(await meta(s.stage)).toEqual(before);
    } finally {
      await release(c);
      await pool.query("DROP TRIGGER gc_fixture_delay ON sessions.ingest_stage_intents;DROP FUNCTION sessions.__gc_fixture_delay()");
    }
  });

  test("aborted claim and unknown committed response never reach S3 or falsely finish", async () => {
    const s = await seed("unknown"), c = await connect();
    await pool.query("SELECT sessions.ingest_lazy_clear($1::jsonb,'reset')", [JSON.stringify({ tenant: s.tenant })]);
    await prioritize(s); const before = await meta(s.stage);
    try {
      await c.query("BEGIN;SET LOCAL ROLE bulls96_session_ingest");
      expect((await c.query("SELECT sessions.ingest_buffer_gc_claim($1::jsonb) AS r", [JSON.stringify({ limit: 1, generation })])).rows[0].r.tickets).toHaveLength(1);
      await c.query("ROLLBACK"); expect(await meta(s.stage)).toEqual(before);
    } finally { await release(c); }
    let io = 0;
    const ledger = sqlBufferedGcLedger({ query: async (sql, params) => {
      const owned = await connect();
      try { await owned.query("BEGIN;SET LOCAL ROLE bulls96_session_ingest"); await owned.query(sql, params as never); await owned.query("COMMIT"); throw Error("synthetic lost committed claim response"); }
      finally { await release(owned); }
    } }, generation);
    await expect(bufferedGcTick({ ledger, store: new Proxy({}, { get() { io++; throw Error("unexpected object IO"); } }) as never, eraser: { eraseObjectExcept: async () => { io++; } }, keyRoot: "96bulls" }, 1)).rejects.toMatchObject({ code: "sql_failed" });
    expect(io).toBe(0); const after = await meta(s.stage);
    expect(after).toMatchObject({ cleanup_state: "live", receipt_state: "acknowledged" }); expect(after.gc_lease_token).not.toBeNull();
    await expect(rpc("ingest_buffer_gc_finish", { stageToken: s.stage, leaseToken: after.gc_lease_token, barrierVersionId: "synthetic-no-barrier" })).rejects.toMatchObject({ code: "40001" });
    expect((await meta(s.stage)).cleanup_state).toBe("live");
  });

  test("generation and immutable-decision corruption errors propagate without checked/lease advancement", async () => {
    const good = await seed("committed-rotation"), s = await seed("corrupt");
    await prioritize(good, s); const before = await meta(s.stage), goodBefore = await meta(good.stage);
    await expect(rpc("ingest_buffer_gc_claim", { limit: 1, generation: "f".repeat(64) })).rejects.toMatchObject({ code: "55000" });
    expect(await meta(s.stage)).toEqual(before); expect(await meta(good.stage)).toEqual(goodBefore);
    await pool.query("DELETE FROM sessions.ingest_lazy_ack_decisions WHERE claim_token=(SELECT claim_token FROM sessions.ingest_stage_intents WHERE stage_token=$1)", [s.stage]);
    expect((await claim()).rows[0].result).toEqual({ tickets: [] });
    const committed = await meta(good.stage); expect(committed.gc_checked_at).not.toContain("2000-01-01");
    await expect(claim()).rejects.toMatchObject({ code: "55000" }); expect(await meta(s.stage)).toEqual(before);
    expect(await meta(good.stage)).toEqual(committed); // the prior independent RPC remains committed
  });

  test("a missing authoritative user fence is corruption, never implicit enrollment or a busy skip", async () => {
    const s = await seed("missing-fence"); await prioritize(s); const before = await meta(s.stage);
    // A valid G2 privacy seal FK prevents this state. Deliberately damage both
    // owner-only fixture rows; the prototype must not repair/enroll the subject.
    await pool.query("DELETE FROM sessions.ingest_lazy_user_seals WHERE org_id=$1 AND workspace_id=$2 AND user_id=$3", [s.tenant.orgId, s.tenant.workspaceId, s.tenant.userId]);
    await pool.query("DELETE FROM sessions.ingest_fence_owners WHERE org_id=$1 AND workspace_id=$2 AND user_id=$3", [s.tenant.orgId, s.tenant.workspaceId, s.tenant.userId]);
    await expect(claim()).rejects.toMatchObject({ code: "55000" }); expect(await meta(s.stage)).toEqual(before);
    expect((await pool.query("SELECT count(*)::int AS n FROM sessions.ingest_fence_owners WHERE org_id=$1 AND workspace_id=$2 AND user_id=$3", [s.tenant.orgId, s.tenant.workspaceId, s.tenant.userId])).rows[0].n).toBe(0);
  });
});
