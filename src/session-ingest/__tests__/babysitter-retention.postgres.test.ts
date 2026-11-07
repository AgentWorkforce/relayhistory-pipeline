/** Opt-in OWNED PG17 (SESSION_INGEST_TEST_PG_URL): the 042000 lane against every landed migration. No cloud, no credential. */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import pg from "pg";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { createHash } from "node:crypto";
import { zstdDecompressSync } from "node:zlib";
import { runMigrations, type MigrationProfile } from "../../hoopdb/migrate.ts";
import { derivedSchemaAssets } from "../../session-pipeline/derived-schema.ts";
import { sqlLedger } from "../ledger-sql.ts";
import { babysitterRetentionTick, bucketBodies, bucketRefs, eraseScope, planBabysitterBuckets, planKey, retentionTargets, sqlBabysitterRetentionLedger, writeBucketObjects } from "../babysitter-retention.ts";
import { evidenceAccount, readBabysitterEvidence, sqlEvidenceDeps, type EvidenceDeps } from "../../session-read-api/babysitter-evidence.ts";
import { createReadApi } from "../../session-read-api/app.ts";
import { FakeS3Versions } from "../../migration/__tests__/fake-s3-versions.ts";
import type { EraseStepOptions } from "../../migration/neon-s3-erasure/eraser.ts";
import { liveMachineId } from "../writer.ts";
import { readBufferedStage } from "../staging.ts";
import { deliverBatch } from "../deliver.ts";
import { deliveryAccount, prepareDelivery } from "../vendor/delivery/prepare.ts";
import { batch, record, scratchStore } from "./support.ts";
import { objectKey, readAll } from "../../shared/vendor.ts";
import type { BabysitterRetentionRequest } from "../ledger.ts";

const url = process.env.SESSION_INGEST_TEST_PG_URL;
if (url) {
  const u = new URL(url);
  if (!["postgresql:", "postgres:"].includes(u.protocol) || !["127.0.0.1", "localhost", "[::1]"].includes(u.hostname) || u.username !== "postgres" ||
      u.password || u.pathname !== "/postgres" || u.search || u.hash) throw Error("owned_loopback_fixture_required");
}
const root = join(import.meta.dir, "../../..");
const NAME = "upstream:20261105042000_babysitter_evidence_retention.sql";
const ROLLBACK = "supabase/rollbacks/20261105042000_babysitter_evidence_retention.down.sql";
const profile: MigrationProfile = { mode: "local", shim: true, bootstrap: true, verify: true, runtimeRole: "postgres" };
const ORIGIN = "origin-1";
const EPOCH = "3".repeat(64), IDENTITY = "c".repeat(64);
type Tenant = { orgId: string; workspaceId: string; userId: string };
const tenantOf = (userId: string): Tenant => ({ orgId: "org-a", workspaceId: "ws-a", userId });
/** The test records' content digest (prepare's per-record sha256 stand-in). */
const dig = (revisionId: string) => createHash("sha256").update(revisionId).digest("hex");
const hourAhead = (h: number) => Math.ceil((Date.now() + 1) / 3_600_000) * 3_600_000 + h * 3_600_000;
/** The ref SQL issues: the hour, and sha16(source, session, reset epoch, fence epoch). */
const refOf = (ms: number, source: string, session: string | null, reset = 0, fence = 0) => {
  const d = new Date(ms), p = (n: number, w = 2) => String(n).padStart(w, "0");
  const tag = createHash("sha256").update(`${source}\u001f${session ?? ""}\u001f${reset}\u001f${fence}`).digest("hex").slice(0, 16);
  return `_bx${p(d.getUTCFullYear(), 4)}${p(d.getUTCMonth() + 1)}${p(d.getUTCDate())}${p(d.getUTCHours())}-${tag}`;
};

describe.skipIf(!url)("babysitter retention SQL lane (PG17)", () => {
  let admin: pg.Client, pool: pg.Pool, name: string, generation: string;
  let serial = 0;
  const asRole = (role: string) => async (sql: string, params: unknown[] = []) => {
    const c = await pool.connect();
    try {
      await c.query(`BEGIN;SET LOCAL ROLE ${role}`);
      const r = await c.query(sql, params as never[]);
      await c.query("COMMIT");
      return r;
    } catch (e) { await c.query("ROLLBACK").catch(() => {}); throw e; } finally { c.release(); }
  };
  const asIngest = asRole("bulls96_session_ingest");
  const asRetention = asRole("bulls96_babysitter_retention");
  const run = async (sql: string, params: unknown[]) => (await asIngest(sql, params)) as unknown as { rows: Record<string, unknown>[] };
  const runRetention = async (sql: string, params: unknown[]) => (await asRetention(sql, params)) as unknown as { rows: Record<string, unknown>[] };
  // The sweeper runs as its own least-privilege role, never the ingest login.
  const sweeper = () => sqlBabysitterRetentionLedger(runRetention, generation);
  const configure = (enabled: boolean, dryRun: boolean) => pool.query("SELECT sessions.babysitter_retention_configure($1::jsonb)", [JSON.stringify({ enabled, dryRun })]);
  const resets = async (t: Tenant) => Number((await pool.query("SELECT count(*)::int AS n FROM sessions.upload_resets WHERE org_id=$1 AND workspace_id=$2 AND user_id=$3", [t.orgId, t.workspaceId, t.userId])).rows[0].n);
  const fenceEpoch = async (t: Tenant, session: string) => Number((await pool.query("SELECT coalesce(max(epoch),0)::int AS e FROM sessions.ingest_fence_versions WHERE org_id=$1 AND workspace_id=$2 AND user_id=$3 AND origin_id=$4 AND source='claude' AND session_id=$5", [t.orgId, t.workspaceId, t.userId, ORIGIN, session])).rows[0].e);
  /** A pending claim attempt as the ingest claim writes it: its epoch snapshot is taken NOW. */
  const seedClaim = async (t: Tenant, sessions: string[]) => {
    const batchId = `seed-${++serial}`, digest = createHash("sha256").update(batchId).digest("hex"), token = crypto.randomUUID();
    const reset = await resets(t);
    const epochs = [];
    for (const s of sessions) epochs.push({ source: "claude", session_id: s, epoch: await fenceEpoch(t, s), reset_epoch: reset });
    await pool.query("INSERT INTO sessions.ingest_batches(org_id,workspace_id,origin_id,batch_id,user_id,digest,receipt_id,state) VALUES($1,$2,$3,$4,$5,$6,$7,'pending')",
      [t.orgId, t.workspaceId, ORIGIN, batchId, t.userId, digest, "rhr_" + digest]);
    await pool.query("INSERT INTO sessions.ingest_claim_attempts(claim_token,org_id,workspace_id,user_id,origin_id,batch_id,digest,epochs) VALUES($1,$2,$3,$4,$5,$6,$7,$8::jsonb)",
      [token, t.orgId, t.workspaceId, t.userId, ORIGIN, batchId, digest, JSON.stringify(epochs)]);
    return { batchId, batchDigest: digest, claimToken: token, resetEpoch: reset };
  };
  const bucket = (h: number, session: string | null, source = "claude"): BabysitterRetentionRequest => ({ source, sessionId: session, eraseAfterMs: hourAhead(h) });
  /** Commit (owner) the revisions a delivery would have committed: the reader serves only committed revisions. */
  const commitRevision = (t: Tenant, recordId: string, revision: number, revisionId: string, source: string, session: string | null) => pool.query(
    `INSERT INTO sessions.ingest_record_revisions AS r(org_id,workspace_id,origin_id,record_id,revision,revision_id,digest,user_id,source,session_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
     ON CONFLICT(org_id,workspace_id,origin_id,record_id) DO UPDATE SET revision=EXCLUDED.revision,revision_id=EXCLUDED.revision_id,digest=EXCLUDED.digest`,
    [t.orgId, t.workspaceId, ORIGIN, recordId, revision, revisionId, dig(revisionId), t.userId, source, session]);
  /** The batch of a claim becomes committed (what ingest's commit does to ingest_batches). */
  const commitBatch = (t: Tenant, batchId: string) => pool.query("UPDATE sessions.ingest_batches SET state='committed',committed_at=now() WHERE org_id=$1 AND workspace_id=$2 AND origin_id=$3 AND batch_id=$4",
    [t.orgId, t.workspaceId, ORIGIN, batchId]);
  /** The bucket objects' PUT outcome, as deliver records it (ingest role, writer-guarded by the claim). */
  const written = (t: Tenant, claimToken: string, objects: ({ ref: string; object: string; outcome: "acked"; versionId: string } | { ref: string; object: string; outcome: "refused"; versionId: null })[]) =>
    sqlLedger(run).babysitterRetentionWritten!({ tenant: t, originId: ORIGIN, generation, claimToken, objects });
  const objectOf = (batchDigest: string) => `b=${createHash("sha256").update(batchDigest).digest("hex").slice(0, 32)}.ndjson.zst`;
  type Rec = { bucket: number | null; originId: string; recordId: string; revision: string; revisionId: string; digest: string; operation: "upsert" | "delete"; kind: string; source: string; sessionId: string | null; line: number | null; expiresAtMs: number | null };
  /**
   * Register under a FRESH claim (snapshot now), or under a given one. By default: one upsert per
   * bucket, its PUT acked, its batch committed and its revision committed (an accepted write).
   */
  const register = async (t: Tenant, buckets: BabysitterRetentionRequest[], claim?: Awaited<ReturnType<typeof seedClaim>>, gen = generation, records?: Rec[]) => {
    const c = claim ?? await seedClaim(t, [...new Set(buckets.filter((b) => b.sessionId).map((b) => b.sessionId!))]);
    const n = ++serial;
    const recs: Rec[] = records ?? buckets.map((b, i) => ({ bucket: i, originId: ORIGIN, recordId: `rec-${n}-${i}`, revision: "1", revisionId: `rec-${n}-${i}:1`, digest: dig(`rec-${n}-${i}:1`), operation: "upsert" as const,
      kind: "turn_receipt", source: b.source, sessionId: b.sessionId, line: 0, expiresAtMs: b.eraseAfterMs }));
    const out = await sqlLedger(run).registerBabysitterRetention!({ tenant: t, originId: ORIGIN, ...c, generation: gen, buckets, records: recs });
    if (!records) {
      if (out.objects.length) await written(t, c.claimToken, out.objects.map((o) => ({ ref: o.ref, object: objectOf(c.batchDigest), outcome: "acked" as const, versionId: `v-${n}` })));
      await commitBatch(t, c.batchId);
      for (const [i, r] of recs.entries()) await commitRevision(t, r.recordId, 1, r.revisionId, buckets[i]!.source, buckets[i]!.sessionId);
    }
    return out;
  };
  const refOfReg = async (t: Tenant, b: BabysitterRetentionRequest) => (await register(t, [b])).objects[0]!.ref;
  const row = async (ref: string) => (await pool.query("SELECT * FROM sessions.babysitter_retention_objects WHERE object_ref=$1", [ref])).rows[0];
  /** Past the 15 min hold-off; `expired` also moves the erase hour into the past. */
  const age = (ref: string, expired = true) => pool.query(
    "UPDATE sessions.babysitter_retention_objects SET last_written_at=clock_timestamp()-interval '1 hour'" + (expired ? ",erase_after=clock_timestamp()-interval '1 hour'" : "") + " WHERE object_ref=$1", [ref]);
  const reset = (t: Tenant, id: string) => pool.query("INSERT INTO sessions.upload_resets(org_id,workspace_id,user_id,request_id) VALUES($1,$2,$3,$4)", [t.orgId, t.workspaceId, t.userId, id]);
  // The exact effects of sessions.ingest_set_session_uploads (20261104030000:405-418): a disable bumps the
  // session's fence epoch (which survives enable) and tombstones it; an enable removes the tombstone only.
  const disable = async (t: Tenant, session: string) => {
    await pool.query(`INSERT INTO sessions.ingest_fence_versions AS f(org_id,workspace_id,user_id,origin_id,source,session_id,epoch) VALUES($1,$2,$3,$4,'claude',$5,1)
      ON CONFLICT(org_id,workspace_id,user_id,origin_id,source,session_id) DO UPDATE SET epoch=f.epoch+1`, [t.orgId, t.workspaceId, t.userId, ORIGIN, session]);
    await pool.query("INSERT INTO sessions.upload_deletions(org_id,workspace_id,user_id,origin_id,source,session_id) VALUES($1,$2,$3,$4,'claude',$5) ON CONFLICT DO NOTHING", [t.orgId, t.workspaceId, t.userId, ORIGIN, session]);
  };
  const enable = (t: Tenant, session: string) => pool.query("DELETE FROM sessions.upload_deletions WHERE org_id=$1 AND workspace_id=$2 AND user_id=$3 AND origin_id=$4 AND source='claude' AND session_id=$5", [t.orgId, t.workspaceId, t.userId, ORIGIN, session]);
  /** The read API's tenant runner: read role, verified binding, READ ONLY (exactly as the worker runs it). */
  const readRun = (bound: { org: string; ws: string } | null) => async <T>(_o: string, _w: string, work: (q: { query<R = Record<string, unknown>>(sql: string, params?: unknown[]): Promise<{ rows: R[] }> }) => Promise<T>): Promise<T> => {
    const c = await pool.connect();
    try {
      await c.query("BEGIN READ ONLY;SET LOCAL ROLE bulls96_read_api");
      if (bound) await c.query("SELECT set_config('bulls96_read.org_id',$1,true),set_config('bulls96_read.workspace_id',$2,true)", [bound.org, bound.ws]);
      const out = await work(c as never);
      await c.query("COMMIT");
      return out;
    } catch (e) { await c.query("ROLLBACK").catch(() => {}); throw e; } finally { c.release(); }
  };
  type PageReq = { kind?: string; source?: string | null; sessionId?: string | null; after?: [string, string] | null; limit?: number; includeDeleted?: boolean };
  const readerPage = (bound: { org: string; ws: string } | null, ws: string, req: PageReq = {}) => readRun(bound)("", "", async (q) =>
    (await q.query<{ v: { rows: { originId: string; recordId: string; operation: string; ref?: string; revision: string }[]; more: boolean; scannedTo: [string, string] | null } }>(
      "SELECT sessions.babysitter_evidence_page_for($1,$2::jsonb) AS v",
      [ws, JSON.stringify({ kind: "turn_receipt", source: null, sessionId: null, after: null, limit: 101, includeDeleted: false, ...req })])).rows[0]!.v);
  /** The buckets whose live rows the reader serves (walks every page). */
  const visible = async (t: Tenant) => {
    const refs: string[] = [];
    let after: [string, string] | null = null;
    for (let i = 0; i < 200; i++) {
      const p = await readerPage({ org: t.orgId, ws: t.workspaceId }, t.workspaceId, { after });
      refs.push(...p.rows.filter((r) => r.operation === "upsert").map((r) => r.ref!));
      if (!p.more) break;
      after = p.rows.length > 100 ? [p.rows[99]!.originId, p.rows[99]!.recordId] : p.scannedTo;
    }
    return refs;
  };
  const audit = async (ref: string) => (await pool.query("SELECT action FROM sessions.babysitter_retention_audit WHERE object_ref=$1 ORDER BY id", [ref])).rows.map((r) => r.action);
  const code = async (p: Promise<unknown>) => { try { await p; return "none"; } catch (e) { return (e as { code?: string }).code ?? "error"; } };
  const migrate = async () => { const c = await pool.connect(); try { return await runMigrations(c, { profile }); } finally { c.release(); } };
  const claimAll = async () => (await sweeper().claim(200)).objects;
  const releaseAll = async (objs: { leaseToken: string }[]) => { for (const o of objs) await sweeper().complete(o.leaseToken, "released", 0); };
  /** A PagedEraser double recording the directory it erased (after authorize), then a zero proof. */
  const recEraser = () => {
    const removed: string[] = [];
    return { removed, eraser: {
      eraseStep: async (p: string, o: EraseStepOptions) => { await o.authorize(); removed.push(`${p}raw/b=x.ndjson.zst`); return { status: "traversed" as const, deleted: 1, listed: 1 }; },
      verifyErased: async (_p: string, o: { authorize: () => Promise<void> }) => { await o.authorize(); return { status: "erased" as const, listed: 0 }; },
    } };
  };

  beforeAll(async () => {
    admin = new pg.Client({ connectionString: url }); await admin.connect();
    const version = Number((await admin.query("SHOW server_version_num")).rows[0].server_version_num);
    expect(version).toBeGreaterThanOrEqual(170000); expect(version).toBeLessThan(180000);
    name = "babysitter_ret_" + crypto.randomUUID().replaceAll("-", "");
    await admin.query("CREATE DATABASE " + name);
    const target = new URL(url!); target.pathname = "/" + name;
    pool = new pg.Pool({ connectionString: target.href, max: 6 });
    await pool.query("CREATE EXTENSION pg_trgm;CREATE EXTENSION btree_gin");
    await migrate();
    // The read role's schema USAGE is its operator step (supabase/bootstrap/session_read_api_role.sql:57), not a migration.
    await pool.query("GRANT USAGE ON SCHEMA sessions TO bulls96_read_api");
    const assets = await derivedSchemaAssets();
    generation = assets.expected.generation;
    expect(assets.expected.names).toContain(NAME);
    console.log(JSON.stringify({ proof: "babysitter-retention-pg17", serverVersionNum: version, generation, migrationCount: assets.expected.names.length }));
    // Buffered admission on the fresh database, before any test writes privacy rows.
    await pool.query("SELECT sessions.ingest_lazy_policy_set($1::jsonb)", [JSON.stringify({ baselineMode: "lazy-current", generation,
      sourceIdentitySha256: IDENTITY, controlEpoch: EPOCH, privacyBaselineSha256: "a".repeat(64), retentionContractSha256: "b".repeat(64),
      sourceProofSha256: "d".repeat(64), validUntil: new Date(Date.now() + 3600000).toISOString(), issuer: "relayhistory-cloud" })]);
    await pool.query("SELECT sessions.ingest_receipt_seat_initialize($1::jsonb)", [JSON.stringify({ protocol: "delivery-acceptance-floor-v1", controlEpoch: EPOCH,
      capturedAt: "2026-11-04T00:00:00.000000Z", sourceSequences: ["sessions.delivery_catalog_acceptance_seq", "sessions.delivery_projection_acceptance_seq"].map((sequenceRegclass, i) => ({ sequenceRegclass, sequenceOid: String(i + 1), allocatedHighWater: "9007199254740993", isCalled: true, incrementBy: "1", cycle: false })),
      allocatedHighWater: "9007199254740993", generation, sourceIdentitySha256: IDENTITY, sourceProofSha256: "d".repeat(64) })]);
  }, 180000);
  afterAll(async () => {
    await pool?.end().catch(() => {});
    if (name) await admin.query("DROP DATABASE IF EXISTS " + name + " WITH (FORCE)").catch(() => {});
    await admin?.end().catch(() => {});
  });

  test("installs dormant; each role holds exactly its own SECURITY DEFINER calls (P1)", async () => {
    expect((await pool.query("SELECT enabled,dry_run FROM sessions.babysitter_retention_settings")).rows).toEqual([{ enabled: false, dry_run: true }]);
    const secdef = async (role: string) => (await pool.query(`SELECT p.oid::regprocedure::text AS f FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
      WHERE n.nspname='sessions' AND p.prosecdef AND (p.proname LIKE '%babysitter%') AND has_function_privilege($1,p.oid,'EXECUTE') ORDER BY 1`, [role])).rows.map((r) => r.f);
    expect(await secdef("bulls96_session_ingest")).toEqual(["sessions.ingest_babysitter_retention_register(jsonb)", "sessions.ingest_babysitter_retention_written(jsonb)"]);
    expect(await secdef("bulls96_babysitter_retention")).toEqual(["sessions.ingest_babysitter_retention_claim(jsonb)", "sessions.ingest_babysitter_retention_complete(jsonb)", "sessions.ingest_babysitter_retention_fence(jsonb)", "sessions.ingest_babysitter_retention_obligation(jsonb)"]);
    expect(await secdef("bulls96_read_api")).toEqual(["sessions.babysitter_evidence_page_for(text,jsonb)"]);
    const all = (await pool.query(`SELECT p.oid::regprocedure::text AS f FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
      WHERE n.nspname='sessions' AND p.prosecdef AND has_function_privilege('bulls96_babysitter_retention',p.oid,'EXECUTE') ORDER BY 1`)).rows.map((r) => r.f);
    expect(all).toEqual(["sessions.ingest_babysitter_retention_claim(jsonb)", "sessions.ingest_babysitter_retention_complete(jsonb)", "sessions.ingest_babysitter_retention_fence(jsonb)", "sessions.ingest_babysitter_retention_obligation(jsonb)"]);
    expect((await pool.query("SELECT rolcanlogin FROM pg_roles WHERE rolname='bulls96_babysitter_retention'")).rows[0].rolcanlogin).toBe(false);
    for (const r of ["bulls96_session_ingest", "bulls96_babysitter_retention", "bulls96_read_api"])
      for (const t of ["babysitter_retention_settings", "babysitter_retention_objects", "babysitter_retention_audit", "babysitter_evidence_index", "babysitter_write_obligations"])
        expect((await pool.query("SELECT has_table_privilege($1,'sessions.'||$2,'SELECT') AS s", [r, t])).rows[0].s).toBe(false);
    expect(await code(asIngest("SELECT sessions.ingest_babysitter_retention_claim($1::jsonb)", [JSON.stringify({ generation, limit: 1 })]))).toBe("42501");
    expect(await code(asRetention("SELECT sessions.ingest_babysitter_retention_register($1::jsonb)", [JSON.stringify({})]))).toBe("42501");
    const sql = readFileSync(join(root, "supabase/migrations/20261105042000_babysitter_evidence_retention.sql"), "utf8");
    expect(sql.split("\n").filter((l) => !l.trimStart().startsWith("--")).join("\n")).not.toMatch(/advisory|CONCURRENTLY|VACUUM/i);
  });

  test("rollback is reversible on an empty registry, then migrate reapplies it", async () => {
    await pool.query(readFileSync(join(root, ROLLBACK), "utf8"));
    expect((await pool.query("SELECT to_regclass('sessions.babysitter_retention_objects') AS t")).rows[0].t).toBeNull();
    await migrate();
    expect((await pool.query("SELECT count(*)::int AS n FROM sessions.__96bulls_migrations WHERE name=$1", [NAME])).rows[0].n).toBe(1);
  }, 120000);

  test("M1: rollback refuses (55000) while a delete-only batch's permanent tombstone exists, though no bucket was ever registered", async () => {
    const t = { orgId: "org-m1", workspaceId: "ws-m1", userId: "u-m1" };
    const claim = await seedClaim(t, ["m1-sess"]);
    const out = await register(t, [], claim, generation, [{ bucket: null, originId: ORIGIN, recordId: "m1-r", revision: "2", revisionId: "m1-r:2", digest: dig("m1-r:2"),
      operation: "delete", kind: "turn_receipt", source: "claude", sessionId: "m1-sess", line: null, expiresAtMs: null }]);
    expect(out.objects).toEqual([]);
    expect((await pool.query("SELECT count(*)::int n FROM sessions.babysitter_retention_objects WHERE org_id=$1", [t.orgId])).rows[0].n).toBe(0);
    expect((await pool.query("SELECT count(*)::int n FROM sessions.babysitter_evidence_index WHERE org_id=$1 AND operation='delete'", [t.orgId])).rows[0].n).toBe(1);
    // Causal: NO bucket exists anywhere yet (this runs before any test registers one) and no erase is
    // audited, so only the tombstone (index) / obligation clauses can refuse.
    expect((await pool.query("SELECT count(*)::int n FROM sessions.babysitter_retention_objects")).rows[0].n).toBe(0);
    expect(await code(pool.query(readFileSync(join(root, ROLLBACK), "utf8")))).toBe("55000");
    expect((await pool.query("SELECT count(*)::int n FROM sessions.babysitter_evidence_index WHERE org_id=$1", [t.orgId])).rows[0].n).toBe(1);
    expect(readFileSync(join(root, ROLLBACK), "utf8")).toContain("EXISTS(SELECT 1 FROM sessions.babysitter_evidence_index)");
  });

  // Runs before any test writes raw privacy rows for the org (the lazy-custody baseline would refuse).
  test("buffered acceptance end to end on real SQL: bucket registered, the SQL-accepted stage holds only stubs", async () => {
    const scratch = scratchStore();
    try {
      const t = tenantOf("buffered-user");
      const expires = Date.now() + 20 * 86_400_000;
      const records = [
        record({ record_id: "bf-receipt", kind: "turn_receipt", payload: { id: "tr-1", lineage_id: "lin-secret", delivery_id: "del-secret", status: "delivered", head_sha: "a".repeat(40), sequence: 1,
          queued_at_ms: 1, delivered_at_ms: 2, completed_at_ms: 3, coding_session_harness: "claude", coding_session_id: "sess-1", retention_class: "standard_30d", expires_at_ms: expires } }),
        record({ record_id: "bf-event" }),
      ];
      const raw = await batch(records, { batch_id: "bf-batch-1", account_id: await deliveryAccount(t) });
      const prepared = await prepareDelivery(t, raw);
      const witness = witnessFor(t, prepared, records.length, []);
      const sourceDigest = (await pool.query("SELECT encode(sha256(convert_to($1::jsonb::text,'UTF8')),'hex') AS d", [JSON.stringify(witness)])).rows[0].d;
      const result = await deliverBatch({ ledger: sqlLedger(run, { acceptanceMode: "buffered", expectedGeneration: generation }), acceptanceMode: "buffered", babysitterRetention: true,
        subrequestCap: 10_000, subrequestRedirects: 0, retentionGeneration: generation, target: { store: scratch.store, keyRoot: "96bulls" }, custody: { witness, sourceDigest } } as never, t, raw)
        .catch((e: { stage?: string; cause_code?: string }) => { throw new Error(`deliver refused at ${e.stage}/${e.cause_code}`); });
      expect(result.outcome).toBe("accepted_buffered");
      const reg = (await pool.query("SELECT object_ref,source,session_id,state,reset_epoch,fence_epoch FROM sessions.babysitter_retention_objects WHERE user_id=$1", [t.userId])).rows;
      expect(reg).toEqual([{ object_ref: refOf(Math.ceil(expires / 3_600_000) * 3_600_000, "claude", "sess-1", 0, 0), source: "claude", session_id: "sess-1", state: "pending", reset_epoch: "0", fence_epoch: "0" }]);
      const stage = (await pool.query("SELECT stage_key,stage_sha256,stage_bytes::int AS bytes,group_ordinals FROM sessions.ingest_stage_intents WHERE batch_id=$1", ["bf-batch-1"])).rows[0];
      const decoded = await readBufferedStage(scratch.store, { key: stage.stage_key, sha256: stage.stage_sha256, bytes: stage.bytes, groupOrdinals: stage.group_ordinals }, (b) => new Uint8Array(zstdDecompressSync(b)));
      expect(decoded.groups.map((g) => [g.source, g.sessionId])).toEqual([["claude", "sess-1"]]);
      expect(JSON.stringify(decoded)).not.toContain("lin-secret");
      // The reader sees the live bucket (tenant-pinned).
      expect(await visible(t)).toContain(reg[0].object_ref);
      console.log(JSON.stringify({ proof: "babysitter-buffered-pg17", sqlPrepareAccepted: true, receipt: result.outcome, stageHoldsPayload: false }));
    } finally { scratch.done(); }
  }, 60000);

  test("P3: buffered delivery after a real reset row keys the bucket by the claim's reset epoch (1)", async () => {
    const scratch = scratchStore();
    try {
      const t = tenantOf("u-claim-reset");
      const expires = Date.now() + 10 * 86_400_000;
      const records = [record({ record_id: "cr-receipt", kind: "turn_receipt", payload: { id: "tr-cr", lineage_id: "lin-cr", delivery_id: "del-cr", status: "delivered", head_sha: "b".repeat(40), sequence: 1,
        queued_at_ms: 1, delivered_at_ms: 2, completed_at_ms: 3, coding_session_harness: "claude", coding_session_id: "sess-1", retention_class: "standard_30d", expires_at_ms: expires } })];
      const raw = await batch(records, { batch_id: "cr-batch-1", account_id: await deliveryAccount(t) });
      const prepared = await prepareDelivery(t, raw);
      const witness = witnessFor(t, prepared, records.length, [{ request_id: "claim-reset-1", deleted: "0" }]);
      const sourceDigest = (await pool.query("SELECT encode(sha256(convert_to($1::jsonb::text,'UTF8')),'hex') AS d", [JSON.stringify(witness)])).rows[0].d;
      const result = await deliverBatch({ ledger: sqlLedger(run, { acceptanceMode: "buffered", expectedGeneration: generation }), acceptanceMode: "buffered", babysitterRetention: true,
        subrequestCap: 10_000, subrequestRedirects: 0, retentionGeneration: generation, target: { store: scratch.store, keyRoot: "96bulls" }, custody: { witness, sourceDigest } } as never, t, raw)
        .catch((e: { stage?: string; cause_code?: string }) => { throw new Error(`deliver refused at ${e.stage}/${e.cause_code}`); });
      expect(result.outcome).toBe("accepted_buffered");
      expect(await resets(t)).toBe(1);
      const rows = (await pool.query("SELECT object_ref,reset_epoch FROM sessions.babysitter_retention_objects WHERE user_id=$1", [t.userId])).rows;
      expect(rows).toEqual([{ object_ref: refOf(Math.ceil(expires / 3_600_000) * 3_600_000, "claude", "sess-1", 1, 0), reset_epoch: "1" }]);
      const keys: string[] = [];
      let cursor: string | undefined;
      do { const page = await scratch.store.list(objectKey("96bulls/org-a"), cursor); keys.push(...page.items.map((i) => String(i.key))); cursor = page.nextCursor; } while (cursor);
      expect(keys.some((k) => k.includes("/h=_babysitter/") && k.includes(`/s=${rows[0].object_ref}/raw/`))).toBe(true);
      expect(await visible(t)).toContain(rows[0].object_ref);
    } finally { scratch.done(); }
  }, 60000);

  test("P2: rollback refuses while any bucket is registered", async () => {
    const t = tenantOf("u-p2");
    const ref = await refOfReg(t, bucket(500, "p2-sess"));
    expect(await code(pool.query(readFileSync(join(root, ROLLBACK), "utf8")))).toBe("55000");
    expect(await row(ref)).toBeDefined();
  });

  test("register: SQL issues the ref from the claim's epochs; shape, epochs, claim and caller are checked", async () => {
    const t = tenantOf("u-reg");
    const b = bucket(1, "sess-1");
    const first = await register(t, [b]);
    expect(first.objects).toEqual([{ ...b, ref: refOf(b.eraseAfterMs, "claude", "sess-1") }]);
    expect(await row(first.objects[0]!.ref)).toMatchObject({ machine_id: liveMachineId(t.workspaceId, ORIGIN, t.userId), state: "pending", write_seq: "1", reset_epoch: "0", fence_epoch: "0" });
    await register(t, [b]);
    expect((await row(first.objects[0]!.ref)).write_seq).toBe("2");
    for (const bad of [{ ...b, eraseAfterMs: b.eraseAfterMs + 1 }, { ...b, source: "" }, { ...b, extra: 1 }])
      expect(await code(register(t, [bad as never]))).toBe("ledger_input_refused");
    const claim = await seedClaim(t, ["sess-1"]);
    // A reset since the claim, a stale generation, or a foreign claim token: refused, nothing registered.
    const one: Rec[] = [{ bucket: 0, originId: ORIGIN, recordId: "reg-r", revision: "1", revisionId: "reg-r:1", digest: dig("reg-r:1"), operation: "upsert" as const, kind: "turn_receipt", source: "claude", sessionId: "sess-1", line: 0, expiresAtMs: b.eraseAfterMs }];
    expect(await code(sqlLedger(run).registerBabysitterRetention!({ tenant: t, originId: ORIGIN, ...claim, resetEpoch: 7, generation, buckets: [b], records: one }))).toBe("ledger_sqlstate_40001");
    // Index rows are validated too: a record outside the buckets, a bad position or operation, a changed revision identity.
    for (const bad of [{ ...one[0]!, bucket: 1 }, { ...one[0]!, line: null }, { ...one[0]!, operation: "delete" }, { ...one[0]!, kind: "session_event" }, { ...one[0]!, revision: "0" },
      { ...one[0]!, bucket: null }])
      expect(await code(register(t, [b], undefined, generation, [bad as never]))).toBe("ledger_input_refused");
    // A record of a session its claim never saw is refused like any fenced/foreign session (retry re-claims).
    expect(await code(register(t, [b], undefined, generation, [{ ...one[0]!, sessionId: "other-session" }]))).toBe("ledger_sqlstate_40001");
    // A record outside its own (source, session) bucket is refused.
    const two = await seedClaim(t, ["sess-1", "sess-2"]);
    expect(await code(register(t, [b], two, generation, [{ ...one[0]!, sessionId: "sess-2" }]))).toBe("ledger_input_refused");
    // Candidates are immutable per claim: the same claim re-registering a different body is refused.
    const c1 = await seedClaim(t, ["sess-1"]);
    await register(t, [b], c1, generation, one);
    await register(t, [b], c1, generation, one);   // idempotent
    expect(await code(register(t, [b], c1, generation, [{ ...one[0]!, digest: dig("other") }]))).toBe("ledger_input_refused");
    // Cross-user: a record of another origin is refused; another user's candidate is allowed but never
    // served for a revision committed by someone else.
    expect(await code(register(t, [b], undefined, generation, [{ ...one[0]!, originId: "origin-other" }]))).toBe("ledger_input_refused");
    const mallory = tenantOf("u-mallory");
    await commitRevision(mallory, "x-steal", 1, "x-steal:1", "claude", "sess-1");
    await pool.query("UPDATE sessions.ingest_record_revisions SET user_id='u-other' WHERE record_id='x-steal'");
    await register(mallory, [b], undefined, generation, [{ ...one[0]!, recordId: "x-steal", revisionId: "x-steal:1", digest: dig("x-steal:1") }]);
    expect((await readerPage({ org: t.orgId, ws: t.workspaceId }, t.workspaceId, { includeDeleted: true, limit: 101 })).rows.some((r) => r.recordId === "x-steal")).toBe(false);
    expect(await code(register(t, [b], claim, "f".repeat(64)))).toBe("ledger_sqlstate_55000");
    expect(await code(register(t, [b], { ...claim, claimToken: crypto.randomUUID() }))).toBe("ledger_sqlstate_40001");
    expect(await code(pool.query("SELECT sessions.ingest_babysitter_retention_register($1::jsonb)", [JSON.stringify({})]))).toBe("42501");
  });

  test("(2) a claim taken before a disable can never write evidence after it, even across a re-enable", async () => {
    const t = tenantOf("u-claim-fence");
    const b = bucket(50, "cf-sess");
    const claim = await seedClaim(t, ["cf-sess"]);
    await disable(t, "cf-sess");
    expect(await code(register(t, [b], claim))).toBe("ledger_sqlstate_40001");
    await enable(t, "cf-sess");
    // Re-enabled, but the claim's fence epoch is older: still refused (no pre-disable data under a post-enable key).
    expect(await code(register(t, [b], claim))).toBe("ledger_sqlstate_40001");
    // A fresh post-enable claim registers under the new fence epoch.
    const after = await register(t, [b]);
    expect(after.objects[0]!.ref).toBe(refOf(b.eraseAfterMs, "claude", "cf-sess", 0, 1));
  });

  test("(2)+(4) disable -> logical delete at once -> sweep -> re-enable: pre-disable stays erased and unreadable; new writes are kept", async () => {
    await configure(true, false);
    const t = tenantOf("u-reenable");
    const b = bucket(300, "re-sess");
    const before = await refOfReg(t, b);
    // The reader is workspace-scoped (org + workspace, as Neon's auth context): assert this bucket.
    expect(await visible(t)).toContain(before);
    await disable(t, "re-sess");
    // (4) Logical delete: hidden the moment the disable commits, before any physical erase.
    expect(await visible(t)).not.toContain(before);
    await enable(t, "re-sess");
    // Not resurrected by the re-enable either, swept or not.
    expect(await visible(t)).not.toContain(before);
    await age(before, false);
    const { removed, eraser } = recEraser();
    expect((await babysitterRetentionTick({ ledger: sweeper(), eraser, keyRoot: "96bulls", generation, mode: "on", limit: 200 })).erased).toBeGreaterThanOrEqual(1);
    expect(removed.some((k) => k.includes(`/s=${before}/`))).toBe(true);
    expect(await row(before)).toMatchObject({ state: "erased", erase_reason: "fenced" });
    // New writes after the re-enable: a new bucket (new fence epoch), kept and readable.
    const after = await refOfReg(t, b);
    expect(after).not.toBe(before);
    expect(await visible(t)).toContain(after);
    expect(await visible(t)).not.toContain(before);
    await age(after, false);
    expect((await claimAll()).some((o) => o.ref === after)).toBe(false);
    expect(await row(before)).toMatchObject({ state: "erased" });
    await configure(false, true);
  });

  test("(3) a stale generation is refused at the erase fence even with a valid lease: nothing is deleted", async () => {
    await configure(true, false);
    const t = tenantOf("u-gen");
    const ref = await refOfReg(t, bucket(400, "gen-sess"));
    await age(ref);
    const objs = await claimAll();
    const mine = objs.find((o) => o.ref === ref)!;
    expect(await sweeper().fence(mine.leaseToken)).toBe(true);
    // A newer schema generation is installed (the PASS marker moves) while the lease is still valid.
    const marker = (await pool.query("SELECT generation FROM sessions.__96bulls_role_validation WHERE name='session_directory_search_worker'")).rows[0].generation;
    await pool.query("UPDATE sessions.__96bulls_role_validation SET generation=$1 WHERE name='session_directory_search_worker'", ["9".repeat(64)]);
    try {
      expect(await code(sweeper().fence(mine.leaseToken))).toBe("ledger_sqlstate_55000");
      expect(await code(sweeper().complete(mine.leaseToken, "erased", 1))).toBe("ledger_sqlstate_55000");
      expect(await code(register(t, [bucket(401, "gen-sess")]))).toBe("ledger_sqlstate_55000");
      // The whole tick is refused at its claim under a stale generation: nothing listed, nothing deleted.
      const { removed, eraser } = recEraser();
      expect(await code(babysitterRetentionTick({ ledger: sweeper(), eraser, keyRoot: "96bulls", generation, mode: "on", limit: 200 }))).toBe("ledger_sqlstate_55000");
      expect(removed).toEqual([]);
    } finally {
      await pool.query("UPDATE sessions.__96bulls_role_validation SET generation=$1 WHERE name='session_directory_search_worker'", [marker]);
    }
    expect((await row(ref)).state).toBe("leased");
    await releaseAll(objs);
    await configure(false, true);
  });

  test("reader RPC: tenant-pinned to the verified binding; expired, fenced and erased buckets are never served live; bounds refused", async () => {
    const t = tenantOf("u-reader");
    const live = await refOfReg(t, bucket(600, "rd-sess"));
    const exp = await refOfReg(t, bucket(601, "rd-exp"));
    await pool.query("UPDATE sessions.babysitter_retention_objects SET erase_after=clock_timestamp()-interval '1 minute' WHERE object_ref=$1", [exp]);
    const v = await visible(t);
    expect(v).toContain(live);
    expect(v).not.toContain(exp);
    const bound = { org: t.orgId, ws: t.workspaceId };
    expect(await code(readerPage(null, t.workspaceId))).toBe("42501");
    expect(await code(readerPage({ org: t.orgId, ws: "ws-other" }, t.workspaceId))).toBe("42501");
    expect((await readerPage({ org: "org-other", ws: t.workspaceId }, t.workspaceId)).rows).toEqual([]);
    expect((await readerPage(bound, t.workspaceId, { sessionId: "rd-exp" })).rows).toEqual([]);
    // Expired under include_deleted: Neon's read-time tombstone (no payload position).
    expect((await readerPage(bound, t.workspaceId, { sessionId: "rd-exp", includeDeleted: true })).rows).toEqual([expect.objectContaining({ operation: "delete", revision: "1" })]);
    for (const bad of [{ limit: 0 }, { limit: 102 }, { after: ["only-one"] as never }, { kind: "session_event" }])
      expect(await code(readerPage(bound, t.workspaceId, bad))).toBe("22023");
    expect(await code(asIngest("SELECT sessions.babysitter_evidence_page_for($1,$2::jsonb)", [t.workspaceId, JSON.stringify({ kind: "turn_receipt", source: null, sessionId: null, after: null, limit: 1, includeDeleted: false })]))).toBe("42501");
  });

  // ---- Lead 64317 / 30f59 rulings through the REAL buffered path (lazy-current ACK decisions) ----
  const BW = { orgId: "org-bw", workspaceId: "ws-bw", userId: "u-bw" };
  const trPayload = (id: string, session = "bw-sess") => ({ id, lineage_id: `lin-${id}`, delivery_id: `del-${id}`, status: "delivered", head_sha: "c".repeat(40), sequence: 1,
    queued_at_ms: 1, delivered_at_ms: 2, completed_at_ms: 3, coding_session_harness: "claude", coding_session_id: session, retention_class: "standard_30d", expires_at_ms: Date.now() + 20 * 86_400_000 });
  const trUp = (id: string, rev: number, body: string, revisionId = `${id}:${rev}`) => record({ record_id: id, kind: "turn_receipt", revision: rev, revision_id: revisionId, session_id: "bw-sess", payload: trPayload(body) });
  const trDel = (id: string, rev: number, revisionId = `${id}:${rev}`) => record({ record_id: id, kind: "turn_receipt", revision: rev, revision_id: revisionId, session_id: "bw-sess", operation: "delete", payload: null });
  const deliverBuffered = async (t: Tenant, store: Parameters<typeof writeBucketObjects>[0]["store"], recs: ReturnType<typeof record>[], batchId: string, deps: Record<string, unknown> = {}) => {
    const raw = await batch(recs, { batch_id: batchId, account_id: await deliveryAccount(t) });
    const prepared = await prepareDelivery(t, raw);
    const witness = witnessFor(t, prepared, recs.length, []);
    const sourceDigest = (await pool.query("SELECT encode(sha256(convert_to($1::jsonb::text,'UTF8')),'hex') AS d", [JSON.stringify(witness)])).rows[0].d;
    return deliverBatch({ ledger: sqlLedger(run, { acceptanceMode: "buffered", expectedGeneration: generation }), acceptanceMode: "buffered", babysitterRetention: true,
      subrequestCap: 10_000, subrequestRedirects: 0, retentionGeneration: generation, target: { store, keyRoot: "96bulls" }, custody: { witness, sourceDigest }, ...deps } as never, t, raw);
  };
  const bwRead = (store: Parameters<typeof writeBucketObjects>[0]["store"], t: Tenant = BW) => sqlEvidenceDeps(readRun({ org: t.orgId, ws: t.workspaceId }) as never, () => store as never,
    (b) => readAll(b as never), (bytes) => new Uint8Array(zstdDecompressSync(bytes)), (k) => objectKey(k) as never, "96bulls");
  const view = async (d: EvidenceDeps, t: Tenant, includeDeleted: boolean) => (await readBabysitterEvidence(d, t, { kind: "turn_receipt", ...(includeDeleted ? { include_deleted: "true" } : {}) }, evidenceAccount(t)))
    .records.map((r) => ({ id: r.record_id, op: r.operation, rev: r.revision, rid: r.revision_id, body: r.payload === null ? null : (r.payload as { id: string }).id }));

  test("accepted winner (real buffered path): a refused equal-revision changed body and delete (same revision_id) never hide or replace the accepted body's ORIGINAL line, including across a restart", async () => {
    const scratch = scratchStore();
    try {
      const d = bwRead(scratch.store);
      expect((await deliverBuffered(BW, scratch.store, [trUp("aw-r", 5, "D1")], "aw-1")).outcome).toBe("accepted_buffered");
      const accepted = [{ id: "aw-r", op: "upsert", rev: 5, rid: "aw-r:5", body: "D1" }];
      expect(await view(d, BW, false)).toEqual(accepted);
      const line = (await pool.query("SELECT machine_id,object_ref,object_name,line,claim_token FROM sessions.babysitter_evidence_index WHERE record_id='aw-r'")).rows;
      // Equal revision, same revision_id, CHANGED body: kept by the buffered claim, registered, ACK-refused.
      await deliverBuffered(BW, scratch.store, [trUp("aw-r", 5, "D2")], "aw-2");
      // Equal revision, same revision_id, a DELETE: also ACK-refused.
      await deliverBuffered(BW, scratch.store, [trDel("aw-r", 5)], "aw-3");
      // A retry of the accepted body in a new batch (stale: not re-accepted, nothing changes).
      await deliverBuffered(BW, scratch.store, [trUp("aw-r", 5, "D1")], "aw-4");
      const decisions = (await pool.query(`SELECT i.digest,d.body_accepted FROM sessions.babysitter_evidence_index i JOIN sessions.ingest_lazy_ack_decisions d
        ON d.claim_token=i.claim_token AND d.candidate_digest=i.digest WHERE i.record_id='aw-r' ORDER BY i.registered_at`)).rows.map((r) => r.body_accepted);
      expect(decisions).toEqual([true, false, false, false]);
      for (const inc of [false, true]) expect(await view(d, BW, inc)).toEqual(accepted);
      // The accepted candidate row is untouched (no re-point): same object and line as at acceptance.
      expect((await pool.query("SELECT machine_id,object_ref,object_name,line,claim_token FROM sessions.babysitter_evidence_index WHERE record_id='aw-r' AND claim_token=$1", [line[0].claim_token])).rows).toEqual(line);
      // "Restart": a fresh reader (new deps, new connections) reads the same winner from that line.
      expect(await view(bwRead(scratch.store), BW, true)).toEqual(accepted);
      // Newer ACCEPTED delete wins (exactly once, payload-free); then a newer ACCEPTED live revision wins.
      await deliverBuffered(BW, scratch.store, [trDel("aw-r", 6)], "aw-5");
      expect(await view(d, BW, false)).toEqual([]);
      expect(await view(d, BW, true)).toEqual([{ id: "aw-r", op: "delete", rev: 6, rid: "aw-r:6", body: null }]);
      await deliverBuffered(BW, scratch.store, [trUp("aw-r", 7, "D7")], "aw-6");
      expect(await view(d, BW, true)).toEqual([{ id: "aw-r", op: "upsert", rev: 7, rid: "aw-r:7", body: "D7" }]);
    } finally { scratch.done(); }
  }, 120000);

  test("lost ACK / crash around registration and the ACK: a registered-but-uncommitted candidate is never served; the retry is", async () => {
    const scratch = scratchStore();
    const BW = { orgId: "org-bw", workspaceId: "ws-bw-crash", userId: "u-bw" };
    try {
      const d = bwRead(scratch.store, BW);
      // The commit fails after registration and the bucket PUT (a crash between PUT and ACK).
      const crashing = { ...sqlLedger(run, { acceptanceMode: "buffered", expectedGeneration: generation }) } as Record<string, unknown>;
      crashing.commitBuffered = async () => { throw Object.assign(new Error("connection lost"), { code: "ECONNRESET" }); };
      await expect(deliverBuffered(BW, scratch.store, [trUp("crash-r", 1, "C1")], "crash-1", { ledger: crashing })).rejects.toBeDefined();
      expect((await pool.query("SELECT count(*)::int n FROM sessions.babysitter_evidence_index WHERE record_id='crash-r'")).rows[0].n).toBe(1);
      expect((await view(d, BW, true)).some((r) => r.id === "crash-r")).toBe(false);
      // Its write obligation survives as recorded (acked or unknown), never retired by the crash.
      expect((await pool.query("SELECT count(*)::int n FROM sessions.babysitter_write_invocations w JOIN sessions.babysitter_evidence_index i ON i.claim_token=w.invocation_id WHERE i.record_id='crash-r'")).rows[0].n).toBe(1);
      expect((await deliverBuffered(BW, scratch.store, [trUp("crash-r", 1, "C1")], "crash-2")).outcome).toBe("accepted_buffered");
      expect((await view(d, BW, false)).filter((r) => r.id === "crash-r")).toEqual([{ id: "crash-r", op: "upsert", rev: 1, rid: "crash-r:1", body: "C1" }]);
    } finally { scratch.done(); }
  }, 120000);

  test("another user cannot take over the record, and nothing of theirs becomes visible", async () => {
    const scratch = scratchStore();
    const BW = { orgId: "org-bw", workspaceId: "ws-bw-own", userId: "u-bw" };
    try {
      await deliverBuffered(BW, scratch.store, [trUp("own-r", 1, "O1")], "own-1");
      const other = { ...BW, userId: "u-bw-other" };
      await expect(deliverBuffered(other, scratch.store, [trUp("own-r", 2, "X2")], "own-2")).rejects.toBeDefined();
      expect((await view(bwRead(scratch.store, BW), BW, true)).filter((r) => r.id === "own-r")).toEqual([{ id: "own-r", op: "upsert", rev: 1, rid: "own-r:1", body: "O1" }]);
    } finally { scratch.done(); }
  }, 120000);

  test("accepted winner (revisions-table path): an equal-revision changed body with the same revision_id, registered later, never hides the committed body's original line", async () => {
    const scratch = scratchStore();
    const T = { orgId: "org-rv", workspaceId: "ws-rv", userId: "u-rv" };
    try {
      const h = hourAhead(40) - 1;
      await deliverIndexed(T, scratch.store, [ev("rv-r", "rs", 5, h, "D1")]);
      const d = sqlEvidenceDeps(readRun({ org: T.orgId, ws: T.workspaceId }) as never, () => scratch.store as never, (b) => readAll(b as never),
        (bytes) => new Uint8Array(zstdDecompressSync(bytes)), (k) => objectKey(k) as never, "96bulls");
      const read = async () => (await readBabysitterEvidence(d, T, { kind: "turn_receipt", include_deleted: "true" }, evidenceAccount(T))).records.map((r) => `${r.operation}:${r.revision}:${r.payload === null ? null : (r.payload as { id: string }).id}`);
      expect(await read()).toEqual(["upsert:5:D1"]);
      // Same revision + revision_id, another body; and a delete with the same revision_id: the
      // canonical commit keeps D1 (stale), so the revisions table never moves to them.
      const changed = { ...ev("rv-r", "rs", 5, h, "D2"), digest: dig("rv-r:5:D2") };
      const del = { ...gone("rv-r", "rs", 5), digest: dig("rv-r:5:del") };
      await deliverIndexed(T, scratch.store, [changed], () => false);
      await deliverIndexed(T, scratch.store, [del], () => false);
      expect(await read()).toEqual(["upsert:5:D1"]);
    } finally { scratch.done(); }
  }, 60000);

  test("unresolved PUT on real SQL: a dispatched invocation (lost response) keeps the bucket tracked forever; a late identical commit is erased as cleanup but never resolves it; only its own outcome does", async () => {
    await configure(true, false);
    const t = tenantOf("u-late");
    const b = bucket(710, "late-sess");
    const claim = await seedClaim(t, ["late-sess"]);
    const reg = await register(t, [b], claim, generation, [{ bucket: 0, originId: ORIGIN, recordId: "late-r", revision: "1", revisionId: "late-r:1",
      digest: dig("late-r:1"), operation: "upsert", kind: "turn_receipt", source: "claude", sessionId: "late-sess", line: 0, expiresAtMs: b.eraseAfterMs }]);
    const ref = reg.objects[0]!.ref;
    await age(ref);
    const d = new Date(b.eraseAfterMs), p2 = (n: number) => String(n).padStart(2, "0");
    const key = `96bulls/org-a/v1/ws=ws-a/m=${liveMachineId(t.workspaceId, ORIGIN, t.userId)}/h=_babysitter/d=${d.getUTCFullYear()}/${p2(d.getUTCMonth() + 1)}/${p2(d.getUTCDate())}/s=${ref}/raw/${objectOf(claim.batchDigest)}`;
    const s3 = new FakeS3Versions();
    const sweepOnce = async () => {
      const r = await babysitterRetentionTick({ ledger: sweeper(), eraser: s3.eraser(), keyRoot: "96bulls", generation, mode: "on", limit: 200 });
      await pool.query("UPDATE sessions.babysitter_write_obligations SET next_sweep_at=NULL WHERE object_ref=$1", [ref]);   // fast-forward the backoff
      return r;
    };
    for (let i = 0; i < 2; i++) { expect((await sweepOnce()).codes.obligation_rescheduled ?? 0).toBeGreaterThanOrEqual(1); expect((await row(ref)).state).toBe("pending"); }
    // The late commit lands (identical content to what the PUT sent): erased as cleanup (reserved first), never "resolved".
    const late = s3.put(key, Date.now(), "5d41402abc4b2a76b9719d911017c592");
    await sweepOnce();
    expect(s3.deletes).toContain(`${key}@${late}`);
    expect((await pool.query("SELECT count(*)::int n FROM sessions.babysitter_write_reservations WHERE object_ref=$1 AND version_id=$2", [ref, late])).rows[0].n).toBe(1);
    expect((await row(ref)).state).toBe("pending");
    expect((await pool.query("SELECT outcome FROM sessions.babysitter_write_invocations WHERE object_ref=$1", [ref])).rows).toEqual([{ outcome: "dispatched" }]);
    // Only the writer's OWN definite outcome resolves it (here: a refusal of that very request).
    await written(t, claim.claimToken, [{ ref, object: objectOf(claim.batchDigest), outcome: "refused", versionId: null }]);
    await sweepOnce();
    expect(await row(ref)).toMatchObject({ state: "erased" });
    expect((await pool.query("SELECT count(*)::int n FROM sessions.babysitter_write_obligations WHERE object_ref=$1", [ref])).rows[0].n).toBe(0);
    // A stale row version is refused (compare-and-set) by every obligation mutation.
    await configure(false, true);
  }, 60000);

  test("finn P1: a FENCED accepted candidate is never decisive by itself, yet nothing older than it is exposed; a re-sent equal revision after re-enable serves; ties pick a stable winner", async () => {
    const scratch = scratchStore();
    const T = { orgId: "org-fx", workspaceId: "ws-fx", userId: "u-fx" };
    try {
      const h = hourAhead(45) - 1;
      const d = sqlEvidenceDeps(readRun({ org: T.orgId, ws: T.workspaceId }) as never, () => scratch.store as never, (b) => readAll(b as never),
        (bytes) => new Uint8Array(zstdDecompressSync(bytes)), (k) => objectKey(k) as never, "96bulls");
      void d;
      // (a) Through the REAL buffered path (immutable ACK decisions): rev 1 accepted in session fa, rev 2
      //     accepted in session fb. fb is disabled (its fence epoch moves and, as ingest_lazy_clear does, its
      //     native custody slot is cleared): the fenced rev 2 must NOT expose the superseded rev 1 body.
      const X = { orgId: "org-fx", workspaceId: "ws-fx-buf", userId: "u-fx" };
      const up = (rev: number, body: string, session: string) => record({ record_id: "fx-r", kind: "turn_receipt", revision: rev, revision_id: `fx-r:${rev}`, session_id: session, payload: trPayload(body, session) });
      await deliverBuffered(X, scratch.store, [up(1, "OLD", "fa")], "fx-1");
      await deliverBuffered(X, scratch.store, [up(2, "NEW", "fb")], "fx-2");
      const dx = sqlEvidenceDeps(readRun({ org: X.orgId, ws: X.workspaceId }) as never, () => scratch.store as never, (b) => readAll(b as never),
        (bytes) => new Uint8Array(zstdDecompressSync(bytes)), (k) => objectKey(k) as never, "96bulls");
      const readX = async () => (await readBabysitterEvidence(dx, X, { kind: "turn_receipt", include_deleted: "true" }, evidenceAccount(X))).records.map((r) => `${r.record_id}:${r.revision}:${(r.payload as { id: string } | null)?.id ?? null}`);
      expect(await readX()).toEqual(["fx-r:2:NEW"]);
      await pool.query(`INSERT INTO sessions.ingest_fence_versions AS f(org_id,workspace_id,user_id,origin_id,source,session_id,epoch) VALUES($1,$2,$3,$4,'claude','fb',1)
        ON CONFLICT(org_id,workspace_id,user_id,origin_id,source,session_id) DO UPDATE SET epoch=f.epoch+1`, [X.orgId, X.workspaceId, X.userId, ORIGIN]);
      await pool.query(`UPDATE sessions.ingest_lazy_native_slots SET lineage=lineage+1,current_owner=NULL,current_source=NULL,current_session=NULL,current_revision=NULL,
        current_digest=NULL,current_reset_epoch=NULL,current_fence_epoch=NULL,current_seat_key=NULL,current_legacy_key=NULL WHERE org_id=$1 AND workspace_id=$2 AND record_id='fx-r'`, [X.orgId, X.workspaceId]);
      expect(await readX()).toEqual([]);
      // (b) a re-sent EQUAL revision after the session is usable again (a new claim, a new fence epoch) is
      //     an unfenced accepted candidate of the same revision: it serves, the fenced one does not hide it.
      const S = { ...T, workspaceId: "ws-fx-2" };
      const d2 = sqlEvidenceDeps(readRun({ org: S.orgId, ws: S.workspaceId }) as never, () => scratch.store as never, (b) => readAll(b as never),
        (bytes) => new Uint8Array(zstdDecompressSync(bytes)), (k) => objectKey(k) as never, "96bulls");
      const read2 = async () => (await readBabysitterEvidence(d2, S, { kind: "turn_receipt", include_deleted: "true" }, evidenceAccount(S))).records.map((r) => `${r.record_id}:${r.revision}:${(r.payload as { id: string } | null)?.id ?? null}`);
      await deliverIndexed(S, scratch.store, [ev("eq-r", "fe", 3, h, "FIRST")]);
      await disable(S, "fe"); await enable(S, "fe");
      expect(await read2()).toEqual([]);
      await deliverIndexed(S, scratch.store, [ev("eq-r", "fe", 3, h, "FIRST")]);
      expect(await read2()).toEqual(["eq-r:3:FIRST"]);
      // (c) a tie (two accepted, unfenced candidates of one revision): the same inputs always choose the same one.
      const U = { ...T, workspaceId: "ws-fx-3" };
      await deliverIndexed(U, scratch.store, [ev("tie-r", "ft", 1, h, "T")]);
      await deliverIndexed(U, scratch.store, [ev("tie-r", "ft", 1, h, "T")]);
      const pick = async () => (await readerPage({ org: U.orgId, ws: U.workspaceId }, U.workspaceId)).rows.map((r) => `${r.ref}/${(r as unknown as { object: string }).object}`);
      const first = await pick();
      for (let i = 0; i < 3; i++) expect(await pick()).toEqual(first);
      const newest = (await pool.query("SELECT object_ref||'/'||object_name AS k FROM sessions.babysitter_evidence_index WHERE record_id='tie-r' ORDER BY registered_at DESC,claim_token LIMIT 1")).rows[0].k;
      expect(first).toEqual([newest]);
    } finally { scratch.done(); }
  }, 60000);

  test("finn P3: a real obligation checkpoint (the key's own raw/ directory) saves and resumes; another directory is refused", async () => {
    await configure(true, false);
    const t = tenantOf("u-ocp");
    const ref = await refOfReg(t, bucket(715, "ocp-sess"));
    await age(ref);
    const objs = await claimAll();
    const mine = objs.find((o) => o.ref === ref)!;
    const object = (await sweeper().obligations(mine.leaseToken, { action: "list" })).objects![0]!;
    const st = (await sweeper().obligations(mine.leaseToken, { action: "load", object })).obligation!;
    const prefix = `${retentionTargets("96bulls", mine).prefix}raw/`;
    const cp = { v: 1, bucket: "b", prefix, scope: "write-obligation:x", afterKey: `${prefix}${object}`, inKey: null };
    const saved = await sweeper().obligations(mine.leaseToken, { action: "checkpoint", object, rev: st.rev, checkpoint: cp as never });
    expect(saved.ok).toBe(true);
    expect((await sweeper().obligations(mine.leaseToken, { action: "load", object })).obligation!.checkpoint).toEqual(cp);
    expect(await code(sweeper().obligations(mine.leaseToken, { action: "checkpoint", object, rev: saved.rev!, checkpoint: { ...cp, prefix: prefix.replace(ref, "_bx2026010100-0000000000000000") } as never }))).toBe("ledger_input_refused");
    await releaseAll(objs);
    await configure(false, true);
  });

  test("PERMANENT tombstones: beyond day 30 and after the payload bucket's physical erase, an old cursor still gets the delete; no bytes read; ordinary reads hide it; the privacy fence purges it", async () => {
    const scratch = scratchStore();
    const T = { orgId: "org-tomb", workspaceId: "ws-tomb", userId: "u-tomb" };
    try {
      const s1 = hourAhead(20) - 1;
      await deliverIndexed(T, scratch.store, [ev("t-a", "ts", 1, s1), ev("t-b", "ts", 1, s1), ev("t-c", "ts", 1, s1)]);
      await deliverIndexed(T, scratch.store, [gone("t-b", "ts", 2)]);          // a delete after an old upsert
      await deliverIndexed(T, scratch.store, [gone("t-z", "ts", 1)]);          // a delete-only record
      const d = sqlEvidenceDeps(readRun({ org: T.orgId, ws: T.workspaceId }) as never, () => scratch.store as never, (b) => readAll(b as never),
        (bytes) => new Uint8Array(zstdDecompressSync(bytes)), (k) => objectKey(k) as never, "96bulls");
      const first = await readBabysitterEvidence(d, T, { kind: "turn_receipt", include_deleted: "true", limit: "1" }, evidenceAccount(T));
      expect(first.records.map((r) => r.record_id)).toEqual(["t-a"]);
      // Day 31+: every payload bucket is expired AND physically erased (rows erased, objects gone).
      await pool.query("UPDATE sessions.babysitter_retention_objects SET erase_after=clock_timestamp()-interval '31 days',last_written_at=clock_timestamp()-interval '31 days' WHERE org_id=$1", [T.orgId]);
      await configure(true, false);
      const gets: string[] = [];
      const counted = { ...scratch.store, getObject: async (k: never) => { gets.push(String(k)); return scratch.store.getObject(k); } };
      const s3 = new FakeS3Versions();   // the provider shows nothing left under those buckets
      const tick = await babysitterRetentionTick({ ledger: sweeper(), eraser: s3.eraser(), keyRoot: "96bulls", generation, mode: "on", limit: 200 });
      expect(tick.erased).toBeGreaterThanOrEqual(1);
      expect((await pool.query("SELECT count(*)::int n FROM sessions.babysitter_retention_objects WHERE org_id=$1 AND state<>'erased'", [T.orgId])).rows[0].n).toBe(0);
      const d2 = sqlEvidenceDeps(readRun({ org: T.orgId, ws: T.workspaceId }) as never, () => counted as never, (b) => readAll(b as never),
        (bytes) => new Uint8Array(zstdDecompressSync(bytes)), (k) => objectKey(k) as never, "96bulls");
      // The offline client resumes with its OLD cursor: still gets t-b's delete (rev 2) and t-z's, once each.
      const rest: string[] = [];
      let cursor: string | null = first.nextCursor;
      while (cursor) {
        const p = await readBabysitterEvidence(d2, T, { kind: "turn_receipt", include_deleted: "true", limit: "1", cursor }, evidenceAccount(T));
        rest.push(...p.records.map((r) => `${r.record_id}:${r.operation}:${r.revision}:${r.payload === null ? "null" : "body"}`));
        cursor = p.nextCursor;
      }
      expect(rest).toEqual(["t-b:delete:2:null", "t-z:delete:1:null"]);
      expect(gets).toEqual([]);   // no expired bytes read
      expect((await readBabysitterEvidence(d2, T, { kind: "turn_receipt" }, evidenceAccount(T))).records).toEqual([]);
      // Clock boundary at the old 30-day expiry: nothing changes for the tombstone either side of it.
      await pool.query("UPDATE sessions.babysitter_evidence_index SET registered_at=clock_timestamp()-interval '30 days' WHERE org_id=$1 AND operation='delete'", [T.orgId]);
      expect((await readBabysitterEvidence(d2, T, { kind: "turn_receipt", include_deleted: "true" }, evidenceAccount(T))).records.map((r) => r.record_id)).toEqual(["t-b", "t-z"]);
      // A newer committed live revision wins over the tombstone.
      await deliverIndexed(T, scratch.store, [ev("t-z", "ts", 2, hourAhead(30) - 1)]);
      expect((await readBabysitterEvidence(d, T, { kind: "turn_receipt", include_deleted: "true" }, evidenceAccount(T))).records.map((r) => `${r.record_id}:${r.operation}`)).toEqual(["t-b:delete", "t-z:upsert"]);
      // Privacy: a reset fences every identity at once, and the sweeper purges the tombstones (C3).
      await reset(T, "tomb-reset-1");
      expect((await readBabysitterEvidence(d, T, { kind: "turn_receipt", include_deleted: "true" }, evidenceAccount(T))).records).toEqual([]);
      await sweeper().claim(1);
      expect((await pool.query("SELECT count(*)::int n FROM sessions.babysitter_evidence_index WHERE org_id=$1 AND operation='delete'", [T.orgId])).rows[0].n).toBe(0);
      await configure(false, true);
    } finally { scratch.done(); }
  }, 120000);

  // ---- §1/§2 differential: storage order != native order; tombstones; supersession; expiry; disable/re-enable ----
  const DT = { orgId: "org-a", workspaceId: "ws-diff", userId: "u-diff" };
  const ev = (id: string, session: string, rev: number, expiresAtMs: number, payloadId = id) => ({ kind: "turn_receipt", operation: "upsert", origin_id: ORIGIN, record_id: id,
    revision: rev, revision_id: `${id}:${rev}`, digest: dig(`${id}:${rev}`), source: "claude", session_id: session, payload: { id: payloadId, retention_class: "standard_30d", expires_at_ms: expiresAtMs } });
  const gone = (id: string, session: string, rev: number) => ({ kind: "turn_receipt", operation: "delete", origin_id: ORIGIN, record_id: id, revision: rev, revision_id: `${id}:${rev}`,
    digest: dig(`${id}:${rev}`), source: "claude", session_id: session, payload: null });
  /** One batch exactly as deliver routes it: plan -> register (index rows) -> bucket objects -> commit. */
  const deliverIndexed = async (t: Tenant, store: Parameters<typeof writeBucketObjects>[0]["store"], records: Record<string, unknown>[], commit = (_r: Record<string, unknown>) => true) => {
    const claim = await seedClaim(t, [...new Set(records.map((r) => r.session_id as string))]);
    const plan = planBabysitterBuckets(records as never, Date.now());
    const bodies = bucketBodies(plan, new Date());
    const reg = await register(t, plan.requests, claim, generation, plan.entries as Rec[]);
    const wrote = await writeBucketObjects({ store, keyRoot: "96bulls" }, { orgId: t.orgId, workspaceId: t.workspaceId, userId: t.userId, originId: ORIGIN, batchDigest: claim.batchDigest }, plan, bucketRefs(plan, reg.objects), bodies);
    if (wrote.outcomes.length) await written(t, claim.claimToken, wrote.outcomes);
    await commitBatch(t, claim.batchId);
    for (const r of records) if (commit(r)) await commitRevision(t, r.record_id as string, r.revision as number, r.revision_id as string, "claude", r.session_id as string);
    return claim;
  };
  const evidence = (store: Parameters<typeof writeBucketObjects>[0]["store"]): EvidenceDeps => sqlEvidenceDeps(readRun({ org: DT.orgId, ws: DT.workspaceId }) as never, () => store as never,
    (b) => readAll(b as never), (bytes) => new Uint8Array(zstdDecompressSync(bytes)), (k) => objectKey(k) as never, "96bulls");
  const walk = async (d: EvidenceDeps, q: Record<string, string>, limit: number) => {
    const out: { id: string; op: string; rev: number; payload: unknown }[] = [];
    let cursor: string | null = null;
    for (let i = 0; i < 50; i++) {
      const p = await readBabysitterEvidence(d, DT, { ...q, limit: String(limit), ...(cursor ? { cursor } : {}) }, evidenceAccount(DT));
      out.push(...p.records.map((r) => ({ id: r.record_id, op: r.operation, rev: r.revision, payload: r.payload === null ? null : (r.payload as { id: string }).id })));
      cursor = p.nextCursor;
      if (!cursor) break;
    }
    return out;
  };

  test("§1/§2 native (origin_id, record_id) order over storage order; a latest delete is Neon's tombstone exactly once; newer live wins; expiry, disable and re-enable keep privacy and stable pages (direct and through the route)", async () => {
    const scratch = scratchStore();
    try {
      const h50 = hourAhead(50) - 1, h60 = hourAhead(60) - 1;
      await deliverIndexed(DT, scratch.store, [ev("r-c", "s1", 1, h50, "c1"), ev("r-a", "s1", 1, h50)]);         // storage: r-c before r-a
      await deliverIndexed(DT, scratch.store, [ev("r-b", "s2", 1, h60)]);
      await deliverIndexed(DT, scratch.store, [ev("r-aa", "s1", 1, h50), ev("r-e", "s1", 1, Date.now() - 1_000)]);  // r-e already expired
      await deliverIndexed(DT, scratch.store, [gone("r-b", "s2", 2)]);                                          // a later delete
      await deliverIndexed(DT, scratch.store, [ev("r-c", "s2", 2, h60, "c2")]);                                 // a newer live revision
      const d = evidence(scratch.store);
      const live = [{ id: "r-a", op: "upsert", rev: 1, payload: "r-a" }, { id: "r-aa", op: "upsert", rev: 1, payload: "r-aa" }, { id: "r-c", op: "upsert", rev: 2, payload: "c2" }];
      const all = [live[0], live[1], { id: "r-b", op: "delete", rev: 2, payload: null }, live[2], { id: "r-e", op: "delete", rev: 1, payload: null }];
      for (const limit of [1, 2, 3, 100]) {
        expect(await walk(d, { kind: "turn_receipt" }, limit)).toEqual(live);
        expect(await walk(d, { kind: "turn_receipt", include_deleted: "true" }, limit)).toEqual(all);
      }
      expect(await walk(d, { kind: "session_lineage", include_deleted: "true" }, 10)).toEqual([]);
      expect(await walk(d, { kind: "turn_receipt", session_id: "s2", include_deleted: "true" }, 1)).toEqual([all[2], all[3]]);
      // No superseded payload bytes anywhere: c1 is never served, not even as a tombstone.
      expect(JSON.stringify(await walk(d, { kind: "turn_receipt", include_deleted: "true" }, 100))).not.toContain("c1");
      // The actual route/worker composition returns exactly the direct reader's pages.
      const app = createReadApi({ verify: async () => ({ orgId: DT.orgId, workspaceId: DT.workspaceId, userId: DT.userId, scopes: ["rth:read"] }) as never, ch: {} as never, liveness: {} as never, completeSinceMs: 0, babysitter: d });
      let cursor = "";
      for (let i = 0; i < 10; i++) {
        const res = await app.fetch(new Request(`https://read.example/v1/delivery/records?kind=turn_receipt&include_deleted=true&limit=2${cursor ? `&cursor=${cursor}` : ""}`,
          { headers: { authorization: "Bearer t", "X-RelayHistory-Expected-Account": evidenceAccount(DT) } }));
        expect(res.status).toBe(200);
        const body = await res.json() as { nextCursor: string | null };
        expect(body).toEqual(await readBabysitterEvidence(d, DT, { kind: "turn_receipt", include_deleted: "true", limit: "2", ...(cursor ? { cursor } : {}) }, evidenceAccount(DT)) as never);
        if (!body.nextCursor) break;
        cursor = body.nextCursor;
      }
      // Disable s1: everything of s1 is hidden at once (even its tombstones), the rest pages stably.
      await disable(DT, "s1");
      const s2Only = [all[2], all[3]];
      for (const limit of [1, 2]) expect(await walk(d, { kind: "turn_receipt", include_deleted: "true" }, limit)).toEqual(s2Only);
      await enable(DT, "s1");
      for (const limit of [1, 2]) expect(await walk(d, { kind: "turn_receipt", include_deleted: "true" }, limit)).toEqual(s2Only);
      expect(await walk(d, { kind: "turn_receipt" }, 1)).toEqual([live[2]]);
    } finally { scratch.done(); }
  }, 60000);

  test("§1 bounded scan: 1,100 uncommitted index rows before a live one end page 1 with a resumable cursor (no false end); page 2 serves it; the index serves the order", async () => {
    const scratch = scratchStore();
    const T = { ...DT, workspaceId: "ws-scan", userId: "u-scan" };
    try {
      const recs = Array.from({ length: 1100 }, (_, i) => ev(`n-${String(i).padStart(5, "0")}`, "s9", 1, hourAhead(70) - 1));
      await deliverIndexed(T, scratch.store, [...recs, ev("z-live", "s9", 1, hourAhead(70) - 1)], (r) => r.record_id === "z-live");
      const d = sqlEvidenceDeps(readRun({ org: T.orgId, ws: T.workspaceId }) as never, () => scratch.store as never, (b) => readAll(b as never),
        (bytes) => new Uint8Array(zstdDecompressSync(bytes)), (k) => objectKey(k) as never, "96bulls");
      const p1 = await readBabysitterEvidence(d, T, { kind: "turn_receipt" }, evidenceAccount(T));
      expect(p1.records).toEqual([]);
      expect(p1.nextCursor).not.toBeNull();
      const p2 = await readBabysitterEvidence(d, T, { kind: "turn_receipt", cursor: p1.nextCursor! }, evidenceAccount(T));
      expect(p2.records.map((r) => r.record_id)).toEqual(["z-live"]);
      expect(p2.nextCursor).toBeNull();
      const c = await pool.connect();
      let plan = "";
      try {
        await c.query("BEGIN;SET LOCAL enable_seqscan=off");
        plan = (await c.query(`EXPLAIN SELECT DISTINCT ON (i.origin_id COLLATE "C",i.record_id COLLATE "C") i.revision FROM sessions.babysitter_evidence_index i
          WHERE i.org_id=$1 AND i.workspace_id=$2 AND i.kind='turn_receipt' AND (i.origin_id COLLATE "C",i.record_id COLLATE "C")>($3 COLLATE "C",$4 COLLATE "C")
          ORDER BY i.origin_id COLLATE "C",i.record_id COLLATE "C",i.revision DESC`, [T.orgId, T.workspaceId, ORIGIN, "n-00500"])).rows.map((r) => r["QUERY PLAN"]).join("\n");
        await c.query("ROLLBACK");
      } finally { c.release(); }
      expect(plan).toContain("babysitter_evidence_index_page_idx");
      expect(plan).not.toMatch(/\bSort\b/);
    } finally { scratch.done(); }
  }, 60000);

  test("nothing is claimed while disabled; dry run cannot record an erase or pass the fence", async () => {
    const t = tenantOf("u-dis");
    const ref = await refOfReg(t, bucket(2, "dis-1"));
    await age(ref);
    await configure(false, true);
    expect(await sweeper().claim(10)).toEqual({ enabled: false, dryRun: true, objects: [] });
    await configure(true, true);
    const objs = await claimAll();
    const mine = objs.find((o) => o.ref === ref)!;
    expect(await sweeper().fence(mine.leaseToken)).toBe(false);
    expect(await code(sweeper().complete(mine.leaseToken, "erased", 1))).toBe("ledger_sqlstate_55000");
    await releaseAll(objs);
    await configure(false, true);
  });

  test("due rules: nothing within the 15 min hold-off; expired after it", async () => {
    await configure(true, false);
    const t = tenantOf("u-due");
    const fresh = await refOfReg(t, bucket(3, "hold-1")), expired = await refOfReg(t, bucket(4, "exp-1"));
    await pool.query("UPDATE sessions.babysitter_retention_objects SET erase_after=clock_timestamp()-interval '1 hour' WHERE object_ref=$1", [fresh]);
    await age(expired);
    const objs = await claimAll();
    const reasons = new Map(objs.map((o) => [o.ref, o.reason]));
    expect(reasons.get(expired)).toBe("expired");
    expect(reasons.has(fresh)).toBe(false);
    await releaseAll(objs);
    await configure(false, true);
  });

  test("P1-b: a reset fences the pre-reset bucket; post-reset evidence opens a new bucket that stays live", async () => {
    await configure(true, false);
    const t = tenantOf("u-reset");
    const before = await refOfReg(t, bucket(600, "r-sess"));
    await reset(t, "reset-1");
    expect(await visible(t)).not.toContain(before);
    const after = await refOfReg(t, bucket(600, "r-sess"));
    expect(after).not.toBe(before);
    await pool.query("UPDATE sessions.babysitter_retention_objects SET last_written_at=clock_timestamp()-interval '49 hours' WHERE object_ref=ANY($1)", [[before, after]]);
    const objs = await claimAll();
    const reasons = new Map(objs.map((o) => [o.ref, o.reason]));
    expect(reasons.get(before)).toBe("fenced");
    expect(reasons.has(after)).toBe(false);
    expect(await visible(t)).toContain(after);
    await releaseAll(objs);
    await configure(false, true);
  });

  test("P1-c: a registration into a leased bucket is refused; an erase is recorded once; append-only audit", async () => {
    await configure(true, false);
    const t = tenantOf("u-lease");
    const b = bucket(700, "l-sess");
    const ref = await refOfReg(t, b);
    await age(ref);
    const objs = await claimAll();
    const mine = objs.find((o) => o.ref === ref)!;
    expect(await code(register(t, [b]))).toBe("ledger_sqlstate_40001");
    // Never erased while a write obligation remains (no unresolved-PUT proof otherwise).
    expect(await code(sweeper().complete(mine.leaseToken, "erased", 3))).toBe("ledger_sqlstate_55000");
    const objs2 = (await sweeper().obligations(mine.leaseToken, { action: "list" })).objects!;
    expect(objs2).toHaveLength(1);
    for (const object of objs2) {
      const st = (await sweeper().obligations(mine.leaseToken, { action: "load", object })).obligation!;
      expect(st.invocations).toEqual([expect.objectContaining({ outcome: "acked" })]);
      // retire refuses while an invocation is unresolved, and on a stale row version
      expect((await sweeper().obligations(mine.leaseToken, { action: "retire", object, rev: st.rev })).ok).toBe(false);
      const r = await sweeper().obligations(mine.leaseToken, { action: "resolve", object, rev: st.rev, invocations: st.invocations.map((i) => i.id) });
      expect((await sweeper().obligations(mine.leaseToken, { action: "retire", object, rev: st.rev })).ok).toBe(false);
      expect((await sweeper().obligations(mine.leaseToken, { action: "retire", object, rev: r.rev! })).ok).toBe(true);
    }
    expect(await sweeper().complete(mine.leaseToken, "erased", 3)).toEqual({ ok: true, state: "erased" });
    expect(await sweeper().complete(mine.leaseToken, "erased", 3)).toEqual({ ok: false });
    await releaseAll(objs.filter((o) => o.ref !== ref));
    expect((await register(t, [b])).reopened).toBe(1);
    expect(await audit(ref)).toEqual(["claimed", "erased", "reopened"]);
    expect(await code(pool.query("UPDATE sessions.babysitter_retention_audit SET reason='x'"))).toBe("55000");
    expect((await pool.query("SELECT count(*)::int AS n FROM sessions.babysitter_retention_audit WHERE object_ref LIKE '%/%'")).rows[0].n).toBe(0);
    await configure(false, true);
  });

  test("SKIP LOCKED: two concurrent claims never share a bucket", async () => {
    await configure(true, false);
    const t = tenantOf("u-par");
    for (let i = 0; i < 6; i++) await age(await refOfReg(t, bucket(100 + i, `par-${i}`)));
    const [a, c] = await Promise.all([sweeper().claim(3), sweeper().claim(3)]);
    const all = [...a.objects, ...c.objects];
    expect(new Set(all.map((o) => o.ref)).size).toBe(all.length);
    await releaseAll(all);
    await configure(false, true);
  });

  test("the tick against real SQL: dry run releases; on erases the derived directory under the fence, proves zero, records erased and drops its index rows", async () => {
    const t = tenantOf("u-tick");
    const b = bucket(120, "tick-1");
    const ref = await refOfReg(t, b);
    await age(ref);
    await configure(true, true);
    const dry = recEraser();
    expect((await babysitterRetentionTick({ ledger: sweeper(), eraser: dry.eraser, keyRoot: "96bulls", generation, mode: "on", limit: 200 })).dry_run).toBe(true);
    expect(dry.removed).toEqual([]);
    await configure(true, false);
    const live = recEraser();
    expect((await babysitterRetentionTick({ ledger: sweeper(), eraser: live.eraser, keyRoot: "96bulls", generation, mode: "on", limit: 200 })).erased).toBeGreaterThanOrEqual(1);
    const d = new Date(b.eraseAfterMs), p2 = (n: number) => String(n).padStart(2, "0");
    expect(live.removed).toContain(`96bulls/org-a/v1/ws=ws-a/m=${liveMachineId(t.workspaceId, ORIGIN, t.userId)}/h=_babysitter/d=${d.getUTCFullYear()}/${p2(d.getUTCMonth() + 1)}/${p2(d.getUTCDate())}/s=${ref}/raw/b=x.ndjson.zst`);
    expect((await row(ref)).state).toBe("erased");
    expect((await pool.query("SELECT count(*)::int AS n FROM sessions.babysitter_evidence_index WHERE object_ref=$1", [ref])).rows[0].n).toBe(0);
    await configure(false, true);
  });

  test("§3 durable resumable erase on real SQL: 12,500 versions persist a bound checkpoint and stay pending; a new write clears it; only a fresh zero proof records erased", async () => {
    await configure(true, false);
    const t = tenantOf("u-big");
    const b = bucket(720, "big-sess");
    const ref = await refOfReg(t, b);
    await age(ref);
    const d = new Date(b.eraseAfterMs), p2 = (n: number) => String(n).padStart(2, "0");
    const prefix = `96bulls/org-a/v1/ws=ws-a/m=${liveMachineId(t.workspaceId, ORIGIN, t.userId)}/h=_babysitter/d=${d.getUTCFullYear()}/${p2(d.getUTCMonth() + 1)}/${p2(d.getUTCDate())}/s=${ref}/`;
    const s3 = new FakeS3Versions();
    for (let i = 0; i < 12_500; i++) s3.put(`${prefix}raw/b=${String(i).padStart(32, "0")}.ndjson.zst`, 1_000);
    const eraser = s3.eraser();
    const r1 = await babysitterRetentionTick({ ledger: sweeper(), eraser, keyRoot: "96bulls", generation, mode: "on", limit: 200 });
    expect(r1.codes.incomplete_versions).toBe(1);
    let r = await row(ref);
    expect(r).toMatchObject({ state: "pending", erase_runs: 1 });
    expect(r.erase_checkpoint).toMatchObject({ v: 1, prefix, scope: eraseScope({ orgId: t.orgId, workspaceId: t.workspaceId, machineId: r.machine_id, ref, fenceEpoch: 0, resetEpoch: 0, writeSeq: 1 }, generation) });
    expect(s3.count(prefix)).toBe(2_500);
    expect(await audit(ref)).toContain("progress");
    // A registration between runs (allowed: not leased) clears the checkpoint and bumps write_seq: never adopted.
    await register(t, [b]);
    r = await row(ref);
    expect(r).toMatchObject({ erase_checkpoint: null, write_seq: "2" });
    await age(ref);
    const r2 = await babysitterRetentionTick({ ledger: sweeper(), eraser, keyRoot: "96bulls", generation, mode: "on", limit: 200 });
    expect(r2.erased).toBeGreaterThanOrEqual(1);
    expect(await row(ref)).toMatchObject({ state: "erased", erase_checkpoint: null });
    expect(s3.count(prefix)).toBe(0);
    expect((await pool.query("SELECT count(*)::int AS n FROM sessions.babysitter_evidence_index WHERE object_ref=$1", [ref])).rows[0].n).toBe(0);
    await configure(false, true);
  }, 120000);

  test("§3 complete: progress is refused in dry run, for another bucket's checkpoint, or with a checkpoint on erased/released; a scope-mismatched stored checkpoint restarts", async () => {
    await configure(true, false);
    const t = tenantOf("u-cp");
    const ref = await refOfReg(t, bucket(730, "cp-sess"));
    const other = await refOfReg(t, bucket(731, "cp-other"));
    await age(ref);
    const objs = await claimAll();
    const mine = objs.find((o) => o.ref === ref)!;
    const cp = (r: string) => ({ v: 1, bucket: "b", prefix: `96bulls/org-a/v1/ws=ws-a/m=${mine.machineId}/h=_babysitter/d=2026/01/01/s=${r}/`, scope: "s", afterKey: null, inKey: null });
    expect(await code(sweeper().complete(mine.leaseToken, "progress", 1, cp(other) as never))).toBe("ledger_input_refused");
    expect(await code(sweeper().complete(mine.leaseToken, "erased", 1, cp(ref) as never))).toBe("ledger_input_refused");
    expect(await code(sweeper().complete(mine.leaseToken, "released", 0, cp(ref) as never))).toBe("ledger_input_refused");
    expect(await code(sweeper().complete(mine.leaseToken, "progress", 1, { v: 2 } as never))).toBe("ledger_input_refused");
    await configure(true, true);
    expect(await code(sweeper().complete(mine.leaseToken, "progress", 1, cp(ref) as never))).toBe("ledger_sqlstate_55000");
    await configure(true, false);
    expect(await sweeper().complete(mine.leaseToken, "progress", 1, cp(ref) as never)).toEqual({ ok: true, state: "pending" });
    expect((await row(ref)).erase_checkpoint).toMatchObject({ scope: "s" });
    await releaseAll(objs.filter((o) => o.ref !== ref));
    // The stored checkpoint's scope is not this bucket/generation/epochs/write_seq: the tick restarts, never adopts it.
    const s3 = new FakeS3Versions();
    const res = await babysitterRetentionTick({ ledger: sweeper(), eraser: s3.eraser(), keyRoot: "96bulls", generation, mode: "on", limit: 200 });
    expect(res.codes.checkpoint_restart).toBeGreaterThanOrEqual(1);
    expect((await row(ref)).state).toBe("erased");
    await configure(false, true);
  }, 60000);
});

function witnessFor(t: Tenant, prepared: Awaited<ReturnType<typeof prepareDelivery>>, count: number, resets: { request_id: string; deleted: string }[]) {
  return {
    version: 1, issuer: "relayhistory-cloud", sourceIdentitySha256: IDENTITY, controlEpoch: EPOCH,
    privacyBaselineSha256: "a".repeat(64), tenant: t, originId: prepared.originId, batchId: prepared.batchId,
    batchDigest: prepared.batchDigest, sourceControlVersion: "0", pending: false, pendingControl: null,
    privacyVersion: "0", originalRecordCount: count, candidateCount: prepared.records.length,
    records: prepared.records.map((r, i) => ({ preparedOrd: i, processingRank: i, candidateDigest: r.digest,
      recordId: r.record_id, revision: String(r.revision), revisionId: r.revision_id, candidateFenced: false, currentFenced: false, current: null })),
    privacy: { complete: true, deletions: [], resets },
  };
}
