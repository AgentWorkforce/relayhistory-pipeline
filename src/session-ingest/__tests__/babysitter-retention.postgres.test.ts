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
import { babysitterRetentionTick, bucketRefs, eraseScope, planBabysitterBuckets, retentionTargets, sqlBabysitterRetentionLedger, writeBucketObjects } from "../babysitter-retention.ts";
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
  /** Register under a FRESH claim (snapshot now), or under a given one; by default one committed upsert row per bucket. */
  const register = async (t: Tenant, buckets: BabysitterRetentionRequest[], claim?: Awaited<ReturnType<typeof seedClaim>>, gen = generation,
    records?: { bucket: number; originId: string; recordId: string; revision: string; revisionId: string; digest: string; operation: "upsert" | "delete"; kind: string; line: number | null; expiresAtMs: number | null }[]) => {
    const c = claim ?? await seedClaim(t, [...new Set(buckets.filter((b) => b.sessionId).map((b) => b.sessionId!))]);
    const n = ++serial;
    const recs = records ?? buckets.map((b, i) => ({ bucket: i, originId: ORIGIN, recordId: `rec-${n}-${i}`, revision: "1", revisionId: `rec-${n}-${i}:1`, digest: dig(`rec-${n}-${i}:1`), operation: "upsert" as const,
      kind: "turn_receipt", line: 0, expiresAtMs: b.eraseAfterMs }));
    const out = await sqlLedger(run).registerBabysitterRetention!({ tenant: t, originId: ORIGIN, ...c, generation: gen, buckets, records: recs });
    if (!records) for (const [i, r] of recs.entries()) await commitRevision(t, r.recordId, 1, r.revisionId, buckets[i]!.source, buckets[i]!.sessionId);
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
    expect(await secdef("bulls96_session_ingest")).toEqual(["sessions.ingest_babysitter_retention_register(jsonb)"]);
    expect(await secdef("bulls96_babysitter_retention")).toEqual(["sessions.ingest_babysitter_retention_claim(jsonb)", "sessions.ingest_babysitter_retention_complete(jsonb)", "sessions.ingest_babysitter_retention_fence(jsonb)"]);
    expect(await secdef("bulls96_read_api")).toEqual(["sessions.babysitter_evidence_page_for(text,jsonb)"]);
    const all = (await pool.query(`SELECT p.oid::regprocedure::text AS f FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
      WHERE n.nspname='sessions' AND p.prosecdef AND has_function_privilege('bulls96_babysitter_retention',p.oid,'EXECUTE') ORDER BY 1`)).rows.map((r) => r.f);
    expect(all).toEqual(["sessions.ingest_babysitter_retention_claim(jsonb)", "sessions.ingest_babysitter_retention_complete(jsonb)", "sessions.ingest_babysitter_retention_fence(jsonb)"]);
    expect((await pool.query("SELECT rolcanlogin FROM pg_roles WHERE rolname='bulls96_babysitter_retention'")).rows[0].rolcanlogin).toBe(false);
    for (const r of ["bulls96_session_ingest", "bulls96_babysitter_retention", "bulls96_read_api"])
      for (const t of ["babysitter_retention_settings", "babysitter_retention_objects", "babysitter_retention_audit", "babysitter_evidence_index"])
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
    const one = [{ bucket: 0, originId: ORIGIN, recordId: "reg-r", revision: "1", revisionId: "reg-r:1", digest: dig("reg-r:1"), operation: "upsert" as const, kind: "turn_receipt", line: 0, expiresAtMs: b.eraseAfterMs }];
    expect(await code(sqlLedger(run).registerBabysitterRetention!({ tenant: t, originId: ORIGIN, ...claim, resetEpoch: 7, generation, buckets: [b], records: one }))).toBe("ledger_sqlstate_40001");
    // Index rows are validated too: a record outside the buckets, a bad position or operation, a changed revision identity.
    for (const bad of [{ ...one[0]!, bucket: 1 }, { ...one[0]!, line: null }, { ...one[0]!, operation: "delete" }, { ...one[0]!, kind: "session_event" }, { ...one[0]!, revision: "0" }])
      expect(await code(register(t, [b], undefined, generation, [bad as never]))).toBe("ledger_input_refused");
    await register(t, [b], undefined, generation, one);
    expect(await code(register(t, [b], undefined, generation, [{ ...one[0]!, revisionId: "other" }]))).toBe("ledger_input_refused");
    // Cross-user: a record of another origin, or another user's registration of the same revision, is refused;
    // and a revision committed by another user is never served from this user's bucket.
    expect(await code(register(t, [b], undefined, generation, [{ ...one[0]!, originId: "origin-other" }]))).toBe("ledger_input_refused");
    const mallory = tenantOf("u-mallory");
    expect(await code(register(mallory, [b], undefined, generation, one))).toBe("ledger_input_refused");
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

  test("finn P1 (30f59f23): buffered custody at an EQUAL revision serves only the body the ack accepted (digest), never a refused delete or payload", async () => {
    const t = { orgId: "org-a", workspaceId: "ws-cust", userId: "u-cust" };
    const h = hourAhead(90) - 1;
    const slot = (record: string, rev: number, digest: string) => {
      const material = ["global-native-custody-v1", t.orgId, t.workspaceId, ORIGIN, record];
      return pool.query(`INSERT INTO sessions.ingest_lazy_native_slots(key_sha256,key_material,org_id,workspace_id,origin_id,record_id,source_identity_sha256,control_epoch,
        initialized_witness_sha256,current_owner,current_source,current_session,current_revision,current_digest,current_reset_epoch,current_fence_epoch)
        VALUES(encode(sha256(convert_to($1::jsonb::text,'UTF8')),'hex'),$1::jsonb,$2,$3,$4,$5,$6,$6,$6,$7,'claude','cs',$8,$9,0,0)`,
        [JSON.stringify(material), t.orgId, t.workspaceId, ORIGIN, record, "a".repeat(64), t.userId, rev, digest]);
    };
    // U holds native custody at rev 5 with body D1 for each record (an upsert, no index row).
    for (const r of ["cust-del", "cust-body", "cust-ok"]) await slot(r, 5, dig(`${r}:5`));
    const b = { source: "claude", sessionId: "cs", eraseAfterMs: h + 1 } as BabysitterRetentionRequest;
    const row = (recordId: string, op: "upsert" | "delete", digest: string) => ({ bucket: 0, originId: ORIGIN, recordId, revision: "5", revisionId: `${recordId}:5`, digest, operation: op,
      kind: "turn_receipt", line: op === "upsert" ? 0 : null, expiresAtMs: op === "upsert" ? h : null });
    // A later buffered batch re-sends rev 5 as a DELETE / with another payload: kept and registered, but the ack refuses the body.
    await register(t, [b], undefined, generation, [row("cust-del", "delete", dig("cust-del:5:other")), row("cust-body", "upsert", dig("cust-body:5:other")), row("cust-ok", "upsert", dig("cust-ok:5"))]);
    const page = await readerPage({ org: t.orgId, ws: t.workspaceId }, t.workspaceId, { includeDeleted: true });
    // Only the accepted body is served; the refused delete and the refused payload are not.
    expect(page.rows.map((r) => [r.recordId, r.operation])).toEqual([["cust-ok", "upsert"]]);
    // Same through ingest_record_revisions: an equal revision with another digest is not this row.
    await commitRevision(t, "cust-del", 5, "cust-del:5", "claude", "cs");
    expect((await readerPage({ org: t.orgId, ws: t.workspaceId }, t.workspaceId, { includeDeleted: true })).rows.map((r) => r.recordId)).toEqual(["cust-ok"]);
  });

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
    const reg = await register(t, plan.requests, claim, generation, plan.entries);
    await writeBucketObjects({ store, keyRoot: "96bulls" }, { orgId: t.orgId, workspaceId: t.workspaceId, userId: t.userId, originId: ORIGIN, batchDigest: claim.batchDigest }, plan, bucketRefs(plan, reg.objects), new Date());
    for (const r of records) if (commit(r)) await commitRevision(t, r.record_id as string, r.revision as number, r.revision_id as string, "claude", r.session_id as string);
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
