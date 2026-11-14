/**
 * presence-dry against the full migrated schema in pglite: one session seeded through the real buffered upload path
 * (ownership, claims, claim sessions with the 044080 write policy on, stages, outbox, lazy slots) plus test-only
 * owner rows for record revisions, the publication tables and the canonical pointer; the counts, the org-scoped counts and an unknown session (all 0);
 * and the no-leak run: backlogOpsRun end to end (OIDC/STS mocked, the tool in-process against pglite), with the
 * receipt file and every log line checked for the session value.
 */
import { afterEach, expect, setDefaultTimeout, test } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { mkdtempSync, readFileSync, readdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { backlogOpsRun, type ChildSpec } from "../../../scripts/backlog-ops.ts";
import { SCHEMA_RELEASE as P } from "../../hoopdb/schema-release-contract.ts";
import { SESSION_BOOTSTRAP_FILES } from "../../hoopdb/session-bootstrap-chain.ts";
import { sqlLedger } from "../ledger-sql.ts";
import { countsOnly } from "../backlog-ops.ts";
import { sessionPresence, sessionPresenceMain, type PresenceSql } from "../session-presence.ts";
import { DELIVERY_MAPPING_VERSION } from "../vendor/delivery/delivery-contracts.ts";
import { deliveryAccount } from "../vendor/delivery/prepare.ts";
import { TURN_KIND, TURN_ORIGIN_ID, turnRecordId } from "../turns.ts";
import { scratchStore } from "./support.ts";

setDefaultTimeout(300000);
const root = join(import.meta.dir, "../../.."), generation = "4".repeat(64), epoch = "1".repeat(64), identity = "c".repeat(64), baseline = "a".repeat(64);
// Test fixtures only.
const ORG = "00000000-0000-4000-8000-0000000000a1", OTHER_ORG = "00000000-0000-4000-8000-0000000000b2";
const TENANT = { orgId: ORG, workspaceId: "ws-a", userId: "user-1" };
const SESSION = "5e551011-fa11-4ce5-9d00-0000000000c3", NEIGHBOUR = "5e551011-fa11-4ce5-9d00-0000000000d4", UNKNOWN = "5e551011-fa11-4ce5-9d00-0000000000e5";
const open: Array<() => unknown> = [];
afterEach(async () => { for (const close of open.splice(0).reverse()) await close(); });

async function fixture() {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  open.push(() => db.close());
  const read = (p: string) => readFileSync(join(root, p), "utf8");
  await db.exec(read("supabase/tests/supabase_shim.sql"));
  for (const f of SESSION_BOOTSTRAP_FILES) await db.exec(read(f));
  for (const f of readdirSync(join(root, "supabase/migrations")).filter((f) => f.endsWith(".sql")).sort()) await db.exec(read("supabase/migrations/" + f));
  await db.exec(read("supabase/bootstrap/session_read_api_role.sql").split("\n").filter((l) => !l.startsWith("\\")).join("\n"));
  await db.exec("CREATE TABLE sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text);INSERT INTO sessions.__96bulls_role_validation VALUES('session_directory_search_worker','" + generation + "','pass')");
  const owner = async (name: string, p: unknown) => (await db.query("SELECT sessions." + name + "($1::jsonb) AS r", [JSON.stringify(p)])).rows[0];
  await owner("ingest_lazy_policy_set", { baselineMode: "lazy-current", generation, sourceIdentitySha256: identity, controlEpoch: epoch, privacyBaselineSha256: baseline,
    retentionContractSha256: "b".repeat(64), sourceProofSha256: "d".repeat(64), validUntil: new Date(Date.now() + 3600000).toISOString(), issuer: "relayhistory-cloud" });
  await owner("ingest_receipt_seat_initialize", { protocol: "delivery-acceptance-floor-v1", controlEpoch: epoch, capturedAt: "2026-11-04T00:00:00.000000Z",
    sourceSequences: ["sessions.delivery_catalog_acceptance_seq", "sessions.delivery_projection_acceptance_seq"].map((sequenceRegclass, i) => ({ sequenceRegclass,
      sequenceOid: String(i + 1), allocatedHighWater: "9007199254740993", isCalled: true, incrementBy: "1", cycle: false })),
    allocatedHighWater: "9007199254740993", generation, sourceIdentitySha256: identity, sourceProofSha256: "d".repeat(64) });
  const run = async (s: string, p: unknown[] = []) => {
    await db.exec("BEGIN;SET LOCAL ROLE bulls96_session_ingest");
    try { const r = await db.query(s, p as never); await db.exec("COMMIT"); return r as never; } catch (e) { await db.exec("ROLLBACK"); throw e; }
  };
  const ledger = sqlLedger(run, { acceptanceMode: "buffered", expectedGeneration: generation });
  const fs = scratchStore(); open.push(fs.done);
  await db.query("INSERT INTO sessions.ingest_frozen_source_completeness(source_identity_sha256,control_epoch,privacy_baseline_sha256,org_count,record_rows,manifest_sha256,neon_snapshot) VALUES($1,$2,$3,0,0,$4,'{}')",
    [identity, epoch, baseline, "e".repeat(64)]);
  const { createIngestApp } = await import("../app.ts");
  const app = createIngestApp({ identify: () => ({ ...TENANT, scopes: ["rth:sync"] }), deliver: { ledger, acceptanceMode: "buffered" as const, target: { store: fs.store, keyRoot: "96bulls" } },
    log: () => {}, custodySource: "frozen-supabase" });
  let serial = 0;
  /** One accepted buffered turn batch for the session through the real ingest route. */
  async function upload(session: string, turns = 2) {
    const batchId = "b-" + (++serial);
    const records = Array.from({ length: turns }, (_, i) => {
      const record_id = turnRecordId(TENANT.workspaceId, "claude", session, i), revision = 1_793_000_000_000_000 + serial * 100 + i;
      return { schema_version: 1 as const, origin_id: TURN_ORIGIN_ID, record_id, revision_id: `turnrev:${record_id.slice(5)}:${revision}`, revision, kind: TURN_KIND as never,
        source: "claude", session_id: session, operation: "upsert" as const,
        payload: { id: `${String(serial * 100 + i).padStart(8, "0")}-aaaa-4bbb-8ccc-dddddddddddd`, org_id: TENANT.orgId, workspace_id: TENANT.workspaceId, source: "claude",
          session_id: session, turn_index: i, session_owner: "user-1", role: "user", content: `turn ${i}`, actor_name: "Ada", actor_role: "owner", metadata_json: "{}",
          ts: "2026-11-05T12:00:00.123Z", tagged: true } };
    });
    const b = { schema_version: 1 as const, origin_id: TURN_ORIGIN_ID, batch_id: batchId, job_id: "relayhistory-turns", generation: 1, destination_id: "relayhistory",
      instance_id: "relayhistory-api", account_id: await deliveryAccount(TENANT), mapping_version: DELIVERY_MAPPING_VERSION, records };
    const r = await app.request("/v1/turns/batches", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ protocolVersion: 1, batch: b }) });
    if (r.status !== 200) throw new Error("upload " + r.status);
  }
  /** Test-only owner rows: one live publication head + live generation + one live object, and one live canonical pointer. */
  async function publish(session: string) {
    const g = (await db.query<{ g: string }>("SELECT gen_random_uuid()::text AS g")).rows[0]!.g;
    await db.query(`INSERT INTO sessions.ingest_publication_heads(org_id,workspace_id,user_id,origin_id,source,session_id,physical_workspace,physical_source,physical_session,machine_id,generation,fence_epoch,reset_epoch,pointer_revision,current_key,state)
      VALUES($1,$2,$3,$4,'claude',$5,$2,'claude',$5,'m-1',$6::uuid,1,0,1,'k/manifest.json','live')`, [TENANT.orgId, TENANT.workspaceId, TENANT.userId, TURN_ORIGIN_ID, session, g]);
    await db.query(`INSERT INTO sessions.ingest_publication_generations(generation,prefix,org_id,workspace_id,user_id,origin_id,source,session_id,fence_epoch,reset_epoch,state,target_bucket,target_region,key_root)
      VALUES($6::uuid,'p/'||$6,$1,$2,$3,$4,'claude',$5,1,0,'live','b','us-east-1','96bulls')`, [TENANT.orgId, TENANT.workspaceId, TENANT.userId, TURN_ORIGIN_ID, session, g]);
    await db.query("INSERT INTO sessions.ingest_publication_objects(object_key,generation,sha256,bytes,kind) VALUES('p/'||$1||'/m',$1::uuid,$2,10,'manifest')", [g, "f".repeat(64)]);
    await db.query(`INSERT INTO sessions.session_index(org_id,workspace_id,harness,session_id,machine_id,user_id,created_at,last_activity_at,storage_provider,bucket,manifest_key,trace_id)
      VALUES($1,$2,'claude',$3,'m-1',$4,now(),now(),'s3','b','k/manifest.json',gen_random_uuid())`, [TENANT.orgId, TENANT.workspaceId, session, TENANT.userId]);
  }
  const sql: PresenceSql = { query: (text, params) => db.query(text, params as never) as never };
  return { db, upload, publish, sql };
}

const opts = { orgs: null, statementTimeoutMs: 60000, totalBudgetMs: 180000 };
const ZERO = { ownership: 0, claims: 0, claim_sessions: 0, buffered_stages: 0, outbox: 0, outbox_buffered: 0, lazy_slots: 0, revisions: 0, heads: 0, published_heads: 0,
  published_objects: 0, canonical_pointers: 0, canonical_pointers_live: 0, dispositions: { heads_building: 0, heads_live: 0, heads_retired: 0 } };

/** Test-only ingest_record_revisions rows (the buffered path writes none; the backfill/legacy writer does). */
async function revisions(db: PGlite, session: string, n: number) {
  for (let i = 0; i < n; i++)
    await db.query("INSERT INTO sessions.ingest_record_revisions(org_id,workspace_id,origin_id,record_id,revision,revision_id,digest,user_id,source,session_id) VALUES($1,$2,$3,$4,1,$5,$6,$7,'claude',$8)",
      [TENANT.orgId, TENANT.workspaceId, TURN_ORIGIN_ID, `rec-${session.slice(-2)}-${i}`, `rev-${session.slice(-2)}-${i}`, "a".repeat(64), TENANT.userId, session]);
}

test("seeded counts: buffered-only, then published; org scope in and out; an unknown session is all zero", async () => {
  const f = await fixture();
  // The v2 claim-session index is written by the claim path once its policy enables writes (044080).
  await f.db.query("UPDATE sessions.ingest_claim_sessions_policy SET write_enabled=true");
  await f.upload(SESSION, 2); await f.upload(SESSION, 3); await f.upload(NEIGHBOUR, 1);
  await revisions(f.db, SESSION, 2);
  // Buffered, never published: the "No conversation recorded yet" shape (ownership 1, buffered_stages > 0, published 0).
  const buffered = await sessionPresence(f.sql, { ...opts, session: SESSION });
  expect(buffered).toEqual({ ...ZERO, ownership: 1, claims: 2, claim_sessions: 2, buffered_stages: 2, outbox: 2, outbox_buffered: 2,
    lazy_slots: 3, revisions: 2, scope: "all", generation_stable: true, complete: true, incomplete_reason: "none" });
  // The neighbour's rows never count for the session.
  const neighbour = await sessionPresence(f.sql, { ...opts, session: NEIGHBOUR });
  expect([neighbour.ownership, neighbour.claims, neighbour.buffered_stages, neighbour.lazy_slots, neighbour.revisions]).toEqual([1, 1, 1, 1, 0]);
  // Published: one live head, generation, object and live canonical pointer.
  await f.publish(SESSION);
  const published = await sessionPresence(f.sql, { ...opts, session: SESSION });
  expect(published).toEqual({ ...buffered, heads: 1, published_heads: 1, published_objects: 1, canonical_pointers: 1, canonical_pointers_live: 1,
    dispositions: { heads_building: 0, heads_live: 1, heads_retired: 0 } });
  // A retired object is not a published one; an erased outbox row is still outbox, no longer buffered (nor its stage).
  await f.db.query("INSERT INTO sessions.ingest_publication_objects(object_key,generation,sha256,bytes,kind,cleanup_state) SELECT 'p/old',generation,$2,10,'manifest','retired' FROM sessions.ingest_publication_generations WHERE session_id=$1",
    [SESSION, "f".repeat(64)]);
  await f.db.query(`UPDATE sessions.ingest_buffer_outbox SET state='erased', active_stage_token=NULL
    WHERE (org_id,workspace_id,origin_id,batch_id) = (SELECT org_id,workspace_id,origin_id,batch_id FROM sessions.ingest_buffer_outbox WHERE state='buffered' AND batch_id='b-1')`);
  const erased = await sessionPresence(f.sql, { ...opts, session: SESSION });
  expect(erased).toEqual({ ...published, buffered_stages: 1, outbox: 2, outbox_buffered: 1 });
  // A tombstoned pointer and a retired head: still counted, no longer live/published.
  await f.db.query("UPDATE sessions.session_index SET deleted_at=now() WHERE session_id=$1", [SESSION]);
  await f.db.query("UPDATE sessions.ingest_publication_heads SET state='retired', current_key=NULL WHERE session_id=$1", [SESSION]);
  const retired = await sessionPresence(f.sql, { ...opts, session: SESSION });
  expect([retired.heads, retired.published_heads, retired.canonical_pointers, retired.canonical_pointers_live, retired.dispositions])
    .toEqual([1, 0, 1, 0, { heads_building: 0, heads_live: 0, heads_retired: 1 }]);
  // Org scope: the session's org sees the same counts; another org sees none.
  const inScope = await sessionPresence(f.sql, { ...opts, session: SESSION, orgs: [ORG] });
  expect(inScope).toEqual({ ...retired, scope: "orgs_1" });
  const outScope = await sessionPresence(f.sql, { ...opts, session: SESSION, orgs: [OTHER_ORG] });
  expect(outScope).toEqual({ ...ZERO, scope: "orgs_1", generation_stable: true, complete: true, incomplete_reason: "none" });
  expect((await sessionPresence(f.sql, { ...opts, session: SESSION, orgs: [ORG, OTHER_ORG] })).scope).toBe("orgs_2");
  // Unknown session: every count 0, complete.
  expect(await sessionPresence(f.sql, { ...opts, session: UNKNOWN })).toEqual({ ...ZERO, scope: "all", generation_stable: true, complete: true, incomplete_reason: "none" });
  // A second ownership row (another user claims the same session id): fail closed, counts still emitted.
  await f.db.query("INSERT INTO sessions.ingest_session_ownership VALUES($1,$2,'user-2',$3,'claude',$4)", [TENANT.orgId, TENANT.workspaceId, TURN_ORIGIN_ID, SESSION]);
  const multi = await sessionPresence(f.sql, { ...opts, session: SESSION });
  expect(multi).toEqual({ ...retired, ownership: 2, complete: false, incomplete_reason: "multi_ownership" });
  // Every run ended in ROLLBACK: the connection is out of the read-only transaction (a write succeeds).
  expect((await f.db.query<{ transaction_read_only: string }>("SHOW transaction_read_only")).rows[0]!.transaction_read_only).toBe("off");
  await f.db.query("CREATE TEMP TABLE after_presence(x int)");
});

test("predicates: the buffered-stage joins, the canonical pointer's session_id arm, and the session under two orgs", async () => {
  const f = await fixture();
  await f.db.query("UPDATE sessions.ingest_claim_sessions_policy SET write_enabled=true");
  await f.upload(SESSION, 1); await f.upload(SESSION, 1); await f.upload(SESSION, 1);
  const base = await sessionPresence(f.sql, { ...opts, session: SESSION });
  expect([base.claims, base.buffered_stages, base.outbox, base.outbox_buffered, base.complete]).toEqual([3, 3, 3, 3, true]);
  // b-1: the batch is no longer publication_state 'buffered' (canonical): its stage is not a buffered stage.
  await f.db.query("UPDATE sessions.ingest_batches SET publication_state='canonical' WHERE batch_id='b-1'");
  // b-2: the stage row's user does not match its outbox row's user: not a buffered stage of that outbox row.
  await f.db.query("UPDATE sessions.ingest_stage_intents SET user_id='user-other' WHERE claim_token IN (SELECT claim_token FROM sessions.ingest_claim_attempts WHERE batch_id='b-2')");
  const narrowed = await sessionPresence(f.sql, { ...opts, session: SESSION });
  expect([narrowed.claims, narrowed.buffered_stages, narrowed.outbox, narrowed.outbox_buffered]).toEqual([3, 1, 3, 3]);
  // A pointer that names the session only by session_id (its canonical_session_id is another id) still counts.
  await f.publish(SESSION);
  await f.db.query(`INSERT INTO sessions.session_index(org_id,workspace_id,harness,session_id,machine_id,user_id,created_at,last_activity_at,storage_provider,bucket,manifest_key,trace_id,canonical_session_id)
    VALUES($1,$2,'claude',$3,'m-2',$4,now(),now(),'s3','b','k2/manifest.json',gen_random_uuid(),$5)`, [TENANT.orgId, TENANT.workspaceId, SESSION, TENANT.userId, NEIGHBOUR]);
  const pointers = await sessionPresence(f.sql, { ...opts, session: SESSION });
  expect([pointers.canonical_pointers, pointers.canonical_pointers_live, pointers.ownership, pointers.complete, pointers.incomplete_reason]).toEqual([2, 2, 1, true, "none"]);
  // The same session id under a second org (a session_index row only; one ownership row): multi_ownership.
  await f.db.query(`INSERT INTO sessions.session_index(org_id,workspace_id,harness,session_id,machine_id,user_id,created_at,last_activity_at,storage_provider,bucket,manifest_key,trace_id)
    VALUES($1,'ws-b','claude',$2,'m-9','user-9',now(),now(),'s3','b','k9/manifest.json',gen_random_uuid())`, [OTHER_ORG, SESSION]);
  const twoOrgs = await sessionPresence(f.sql, { ...opts, session: SESSION });
  expect([twoOrgs.ownership, twoOrgs.canonical_pointers, twoOrgs.generation_stable, twoOrgs.complete, twoOrgs.incomplete_reason]).toEqual([1, 3, true, false, "multi_ownership"]);
  expect(Object.keys(twoOrgs).sort()).toEqual(Object.keys(pointers).sort());
  // Scoped to one org, the session is single-org again.
  const scoped = await sessionPresence(f.sql, { ...opts, session: SESSION, orgs: [ORG] });
  expect([scoped.canonical_pointers, scoped.complete, scoped.incomplete_reason]).toEqual([2, true, "none"]);
  const other = await sessionPresence(f.sql, { ...opts, session: SESSION, orgs: [OTHER_ORG] });
  expect([other.ownership, other.canonical_pointers, other.complete, other.incomplete_reason]).toEqual([0, 1, true, "none"]);
});

test("REPEATABLE READ READ ONLY: a write inside the transaction is refused (25006) and refuses the run", async () => {
  const f = await fixture();
  let n = 0;
  const writer: PresenceSql = { query: (text, params) => (++n === 6 ? f.db.query("INSERT INTO sessions.ingest_session_ownership VALUES('o','w','u','g','s','x')") : f.db.query(text, params as never)) as never };
  const e = await sessionPresence(writer, { ...opts, session: SESSION }).then(() => null, (e) => e as { code?: string });
  expect(e?.code).toBe("25006");
  expect((await f.db.query<{ n: number }>("SELECT count(*)::int n FROM sessions.ingest_session_ownership")).rows[0]!.n).toBe(0);
});

test("no leak: backlogOpsRun end to end; the receipt and every log line never contain the session value", async () => {
  const f = await fixture();
  await f.db.query("UPDATE sessions.ingest_claim_sessions_policy SET write_enabled=true");
  await f.upload(SESSION, 2);
  const sha = "2".repeat(40);
  const xml = "<AssumeRoleWithWebIdentityResponse><Credentials><AccessKeyId>ASIAAAAAAAAAAAAAAAAA</AccessKeyId><SecretAccessKey>secret</SecretAccessKey>" +
    `<SessionToken>token</SessionToken><Expiration>${new Date(Date.now() + 6 * 3600_000).toISOString()}</Expiration></Credentials></AssumeRoleWithWebIdentityResponse>`;
  const fetcher = (async (url: string) => url.startsWith("https://token.example/") ? new Response(JSON.stringify({ value: "a.b.c" })) : new Response(xml)) as unknown as typeof fetch;
  const lines: string[] = [];
  const saved = { log: console.log, error: console.error, warn: console.warn, info: console.info, write: process.stdout.write, ewrite: process.stderr.write };
  const capture = (...a: unknown[]) => { lines.push(a.map(String).join(" ")); };
  const temps: string[] = [];
  const runOnce = async (session: string, extra: Record<string, string> = {}, child?: (c: ChildSpec) => Promise<{ code: number; stdout: string }>) => {
    const temp = mkdtempSync(join(process.env.TMPDIR ?? tmpdir(), "presence-")); temps.push(temp);
    const env: Record<string, string> = {
      BACKLOG_OPS_MODE: "presence-dry", BACKLOG_OPS_SHA: sha, GITHUB_SHA: sha, GITHUB_RUN_ID: "77", RUNNER_TEMP: temp, PATH: "/usr/bin",
      BACKLOG_OPS_BUCKET: "96bulls-aws-sessions-prod-transcripts-000000000000", BACKLOG_OPS_REGION: "us-east-1",
      BACKLOG_OPS_KMS_KEY_ARN: "arn:aws:kms:us-east-1:000000000000:key/00000000-0000-4000-8000-000000000000",
      BACKLOG_OPS_DRY_ROLE_ARN: "arn:aws:iam::000000000000:role/96bulls-aws-sessions-prod-backlog-ops-read",
      BACKLOG_OPS_EXECUTE_ROLE_ARN: "arn:aws:iam::000000000000:role/96bulls-aws-sessions-prod-backlog-ops-write",
      SUPABASE_MIGRATION_OWNER_URL: `postgresql://${P.owner}.${P.project}:pw@${P.host}:${P.port}/${P.database}?sslmode=verify-full`,
      ACTIONS_ID_TOKEN_REQUEST_URL: "https://token.example/x", ACTIONS_ID_TOKEN_REQUEST_TOKEN: "req", SESSION_PRESENCE_SESSION: session, ...extra,
    };
    const inProcess = async (c: ChildSpec) => {
      // The real tool body on the child's own argv and stdin, against pglite (the URL is never dialled).
      const out = await sessionPresenceMain(c.args.slice(1), c.input ?? "", async () => f.sql);
      process.stdout.write(out.line + "\n");
      return { code: out.code, stdout: out.line + "\n" };
    };
    const result = await backlogOpsRun(env, { fetcher, runChild: child ?? inProcess }).then((c) => ({ c }), (e) => ({ refused: (e as { code?: string }).code ?? "?" }));
    let receipt = "";
    try { receipt = readFileSync(join(temp, "backlog-ops-out", readdirSync(join(temp, "backlog-ops-out"))[0]!), "utf8"); } catch { /* refused before a receipt */ }
    return { result, receipt };
  };
  console.log = capture; console.error = capture; console.warn = capture; console.info = capture;
  process.stdout.write = ((chunk: unknown) => { lines.push(String(chunk)); return true; }) as typeof process.stdout.write;
  process.stderr.write = ((chunk: unknown) => { lines.push(String(chunk)); return true; }) as typeof process.stderr.write;
  const results: Array<{ result: unknown; receipt: string }> = [];
  const LOWER = "fixture_session_token_lower";
  try {
    results.push(await runOnce(SESSION));                                         // counts
    results.push(await runOnce(LOWER));                                           // a token that is also a valid countsOnly code
    results.push(await runOnce(SESSION, { BACKLOG_OPS_ORGS: ORG }));              // org scope
    results.push(await runOnce(SESSION + " x"));                                  // malformed secret
    results.push(await runOnce(SESSION, { BACKLOG_OPS_MODE: "presence-execute" })); // execute refused
    results.push(await runOnce(SESSION, {}, async () => {                         // a tool error naming the session
      const out = await sessionPresenceMain(["--dry-run", "--statement-timeout-ms", "60000", "--total-budget-ms", "180000"], JSON.stringify({ dbUrl: "x", session: SESSION }),
        async () => ({ query: async () => { throw Object.assign(new Error(`boom ${SESSION}`), { code: "XX000" }); } }));
      return { code: out.code, stdout: out.line };
    }));
  } finally {
    Object.assign(console, { log: saved.log, error: saved.error, warn: saved.warn, info: saved.info });
    process.stdout.write = saved.write; process.stderr.write = saved.ewrite;
    for (const t of temps) rmSync(t, { recursive: true, force: true });
  }
  expect(results.map((r) => r.result)).toEqual([{ c: 0 }, { c: 0 }, { c: 0 }, { refused: "backlog_ops_presence_session_invalid" }, { refused: "backlog_ops_presence_dry_only" }, { c: 1 }]);
  const ok = JSON.parse(results[0]!.receipt);
  expect([ok.mode, ok.exitCode, ok.summary.ownership, ok.summary.buffered_stages, ok.summary.complete]).toEqual(["presence-dry", 0, 1, 1, true]);
  expect(countsOnly(ok.summary)).toEqual(ok.summary);
  expect(JSON.parse(results[1]!.receipt).summary.ownership).toBe(0);
  expect(JSON.parse(results[2]!.receipt).summary.scope).toBe("orgs_1");
  expect(JSON.parse(results[5]!.receipt).summary).toEqual({ refused: "sqlstate_xx000" });
  expect(lines.some((l) => l.startsWith("backlog_ops_receipt "))).toBe(true);
  for (const value of [SESSION, LOWER, SESSION.slice(0, 8), "5e551011"]) {
    for (const r of results) expect(r.receipt).not.toContain(value);
    for (const l of lines) expect(l).not.toContain(value);
  }
});
