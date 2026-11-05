/** DESTRUCTIVE SYNTHETIC FIXTURE ONLY: rehearses generation window #1 on an owned local PostgreSQL 17
 * container (label migration.owner=codex-window-rehearsal, 127.0.0.1 only). No production path.
 *
 *   bun scripts/pg-proof/window-rehearsal.ts --owned-fixture --base-root <checkout at the live source, G1>
 *
 * 1. Builds the live shape from the BASE checkout: bootstrap, then base owner migrations -> readiness PASS(G1),
 *    the lazy policy at G1, and 965 admission rows at G1 (production's count).
 * 2. W1 with THIS checkout's real owner-apply code: inspectOwner -> read-only plan -> ownerCapture ->
 *    approval (the runbook's exact field set) -> applyOwnerRelease -> owner_apply_verified, PASS(G2).
 * 3. Uploads refuse for an old (G1) Worker and for a new (G2) Worker until the rebind.
 * 4. W4 rebind dry run, execute, idempotent rerun; then a G2 upload (frozen witness) is accepted.
 * Writes .96bulls/window-rehearsal/result.json and prints it.
 */
import pg from "pg";
import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { readFile, writeFile, mkdir } from "node:fs/promises";
import { createHash } from "node:crypto";

const root = fileURLToPath(new URL("../..", import.meta.url));
const argv = process.argv.slice(2);
if (argv[0] !== "--owned-fixture" || argv[1] !== "--base-root" || !argv[2]) throw Error("owned_fixture_argument_required");
const base = argv[2].replace(/\/$/, "");
const container = "window-rehearsal-pg17-20261105";
const meta = JSON.parse(execFileSync("docker", ["inspect", "--format", "{{json .}}", container], { encoding: "utf8", maxBuffer: 1 << 20 }));
const binding = meta.NetworkSettings?.Ports?.["5432/tcp"];
if (meta.Config?.Labels?.["migration.owner"] !== "codex-window-rehearsal" || !Array.isArray(binding) || binding.length !== 1 || binding[0].HostIp !== "127.0.0.1")
  throw Error("owned_fixture_identity");
const port = Number(binding[0].HostPort);
const sh = (cwd: string, ...a: string[]) => execFileSync("git", ["-C", cwd, ...a], { encoding: "utf8" }).trim();
const BASE_SHA = sh(base, "rev-parse", "HEAD"), HEAD_SHA = sh(root, "rev-parse", "HEAD");
const LIVE_LEDGER = "de0e2fa02ba662bbc1e0ec23e1f619100d3b7cd824e8d055b04c44076d452729";

const B = { op: await import(base + "/src/hoopdb/owner-operator-apply.ts"), mig: await import(base + "/src/hoopdb/migrate.ts") };
const M = {
  op: await import(root + "/src/hoopdb/owner-operator-apply.ts"), mig: await import(root + "/src/hoopdb/migrate.ts"),
  rebind: await import(root + "/src/hoopdb/ingest-generation-rebind.ts"), ledgerSql: await import(root + "/src/session-ingest/ledger-sql.ts"),
  deliver: await import(root + "/src/session-ingest/deliver.ts"), prepare: await import(root + "/src/session-ingest/vendor/delivery/prepare.ts"),
  support: await import(root + "/src/session-ingest/__tests__/support.ts"),
};
const G1: string = await B.mig.generation(), G2: string = await M.mig.generation();
const events: Record<string, unknown>[] = [];
const ev = (step: string, f: Record<string, unknown> = {}) => { const e = { step, ...f }; events.push(e); console.error(JSON.stringify(e)); };

const admin = new pg.Client({ host: "127.0.0.1", port, user: "proof_admin", database: "postgres", password: "synthetic-owner-proof-only" });
await admin.connect();
const v = (await admin.query("SELECT current_user me,current_setting('server_version_num')::int v")).rows[0];
if (v.me !== "proof_admin" || v.v < 170000 || v.v >= 180000) throw Error("owned_fixture_database_identity");

// ---- 1. live shape from the base checkout (same bootstrap as owner-apply-proof.ts) ----
const readB = (p: string) => readFile(base + "/" + p, "utf8");
await admin.query("DROP SCHEMA IF EXISTS hoopdb_boundary CASCADE;DROP SCHEMA IF EXISTS sessions CASCADE;DROP SCHEMA IF EXISTS history CASCADE;ALTER ROLE postgres SUPERUSER;SET ROLE postgres;CREATE SCHEMA sessions;GRANT CREATE ON SCHEMA public TO postgres;REVOKE TEMPORARY ON DATABASE postgres FROM PUBLIC;ALTER DEFAULT PRIVILEGES FOR ROLE postgres REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;");
await admin.query(await readB("supabase/tests/supabase_shim.sql"));
for (const f of ["session_directory_search_worker", "session_token_verifier", "session_ingest_roles","session_read_api_roles"]) await admin.query(await readB(`supabase/bootstrap/${f}.sql`));
await admin.query("DO $$BEGIN IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='bulls96_runtime') THEN CREATE ROLE bulls96_runtime LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS NOREPLICATION; END IF; END $$");
await admin.query("CREATE EXTENSION IF NOT EXISTS vector;CREATE EXTENSION IF NOT EXISTS pg_trgm;CREATE EXTENSION IF NOT EXISTS btree_gin;");
await admin.query((await readB("supabase/bootstrap/hoopdb_sessions_function_boundary_supabase.sql")).split("\n").filter((l) => !l.startsWith("\\")).join("\n").replaceAll(":'runtime_role'", "'bulls96_runtime'"));
await admin.query("RESET ROLE;ALTER ROLE postgres NOSUPERUSER;ALTER DEFAULT PRIVILEGES FOR ROLE proof_admin REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC");
const owner = () => new pg.Client({ host: "127.0.0.1", port, user: "cli_login_postgres", database: "postgres", password: "synthetic-cli-proof-only" });

let baseLedger = "";
{
  const c = owner();
  await B.op.withOwnerSession(c, async (pid: number) => {
    await B.mig.runMigrations(B.op.ownerFencedClient(c, pid), { profile: B.op.ownerProfile() });
    baseLedger = (await B.op.inspectOwner(c, pid)).ledgerSha256;
  });
}
const marker = async () => (await admin.query("SELECT generation,status FROM sessions.__96bulls_role_validation WHERE name='session_directory_search_worker'")).rows[0];
ev("base-applied", { baseSha: BASE_SHA, G1, marker: await marker(), ledgerSha256: baseLedger, matchesLiveLedger: baseLedger === LIVE_LEDGER });
if ((await marker()).generation !== G1 || (await marker()).status !== "pass") throw Error("base readiness not PASS(G1)");

const epoch = "3".repeat(64), identity = "7891c81907fd9db0e69a01c59a1a58bcd8c65b2d766dba9adb7fba793382acc4", baselineSha = "c".repeat(64);
const ownerFn = async (name: string, p: unknown) => (await admin.query(`SET ROLE postgres;`), (await admin.query(`SELECT sessions.${name}($1::jsonb) AS r`, [JSON.stringify(p)])).rows[0].r);
await ownerFn("ingest_lazy_policy_set", { baselineMode: "lazy-current", generation: G1, sourceIdentitySha256: identity, controlEpoch: epoch, privacyBaselineSha256: baselineSha,
  retentionContractSha256: "b".repeat(64), sourceProofSha256: "d".repeat(64), validUntil: new Date(Date.now() + 6 * 3600000).toISOString(), issuer: "relayhistory-cloud" });
await ownerFn("ingest_receipt_seat_initialize", { protocol: "delivery-acceptance-floor-v1", controlEpoch: epoch, capturedAt: "2026-11-04T00:00:00.000000Z",
  sourceSequences: ["sessions.delivery_catalog_acceptance_seq", "sessions.delivery_projection_acceptance_seq"].map((sequenceRegclass, i) => ({ sequenceRegclass, sequenceOid: String(i + 1), allocatedHighWater: "9007199254740993", isCalled: true, incrementBy: "1", cycle: false })),
  allocatedHighWater: "9007199254740993", generation: G1, sourceIdentitySha256: identity, sourceProofSha256: "d".repeat(64) });
await admin.query(`INSERT INTO sessions.ingest_buffer_admission(org_id,privacy_baseline_sha256,retention_contract_sha256,schema_generation,valid_until,control_epoch)
  SELECT 'org-'||g,$1,$2,$3,now()+interval '6 hours',$4 FROM generate_series(1,965) g`, [baselineSha, "b".repeat(64), G1, epoch]);
await admin.query("RESET ROLE");
ev("live-shape-seeded", { policy: G1, admissions: 965 });

// ---- 2. W1: the real owner-apply flow from THIS checkout ----
const pin = { sha: HEAD_SHA, generation: G2 };
const wrapper = await M.op.wrapperSource();
let w1: any;
{
  const c = owner();
  await M.op.withOwnerSession(c, async (pid: number) => {
    const fresh = await M.op.inspectOwner(c, pid);
    const pending = fresh.inventory.filter((r: any) => r.pending).map((r: any) => r.name);
    const planText = JSON.stringify({ version: 1, status: "planned_readonly_not_apply_approval", project: M.op.OWNER_APPLY.project, sourceSha: pin.sha, generation: pin.generation,
      ledgerSha256: fresh.ledgerSha256, boundary: "global-default-revoke", inventory: fresh.inventory, acl: fresh.acl, at: new Date().toISOString() }, null, 2) + "\n";
    ev("w1-plan", { pending, ledgerSha256: fresh.ledgerSha256, pinnedLedger: M.op.OWNER_APPLY.ledger });
    const plan = M.op.validateReadonlyPlan(planText, pin);
    const capture = M.op.ownerCapture(planText, plan, pin, fresh, wrapper);
    const captureText = JSON.stringify(capture, null, 2) + "\n";
    const approvalText = JSON.stringify({ version: 1, decision: "approved", by: "Khaliq Gant", at: new Date(Date.now() + 1000).toISOString(),
      quote: "everythning is pre approved. I want tihs transition to happen asap and the switchoer asap", execution: M.op.OWNER_APPLY.execution,
      project: M.op.OWNER_APPLY.project, sourceSha: capture.source.sha, generation: capture.source.generation, ledgerSha256: capture.ledgerSha256,
      planSha256: createHash("sha256").update(planText).digest("hex"), captureSha256: createHash("sha256").update(captureText).digest("hex"),
      captureGeneration: capture.captureGeneration, wrapperSha256: capture.wrapperSha256 }, null, 2) + "\n";
    w1 = await M.op.applyOwnerRelease(c, pid, { pin, planText, captureText, approvalText, wrapper, recheckSource: async () => {} });
  });
}
ev("w1-applied", { status: w1.status, generation: w1.generation, marker: await marker() });
if (w1.status !== "owner_apply_verified" || (await marker()).generation !== G2) throw Error("W1 did not reach PASS(G2)");

// ---- W2: history owner-apply with the real plan/approval/apply code (separate track) ----
{
  const H = await import(root + "/scripts/history-owner-apply.ts"), HM = await import(root + "/src/history-db/migrate.ts");
  const ingestBefore = await marker();
  const c = owner();
  const out = await M.op.withOwnerSession(c, async (pid: number) => {
    const fenced = M.op.ownerFencedClient(c, pid);
    await fenced.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
    let report: any;
    try { report = await HM.inspectHistory(fenced); } finally { await c.query("ROLLBACK"); }
    const planText = JSON.stringify({ version: 1, kind: H.HISTORY_APPLY.protocol, status: "planned_readonly_not_apply_approval",
      at: new Date().toISOString(), project: M.op.OWNER_APPLY.project, sourceSha: HEAD_SHA, ...report }, null, 2) + "\n";
    const plan = JSON.parse(planText);
    // The exact approval the G2 brief's Step 1 writes.
    const approvalText = JSON.stringify({ version: 1, decision: "approved", by: "Khaliq Gant", execution: "ssh-sf-frame-history-apply",
      quote: "everythning is pre approved. I want tihs transition to happen asap and the switchoer asap", project: plan.project, sourceSha: plan.sourceSha,
      generation: plan.generation, ledgerSha256: plan.ledgerSha256, planSha256: createHash("sha256").update(planText).digest("hex"),
      at: new Date(Date.now() + 1000).toISOString() }, null, 2) + "\n";
    H.validateHistoryApproval(planText, approvalText, HEAD_SHA, Date.now() + 2000);
    const result = await HM.runHistoryMigrations(fenced, { expectedLedgerSha256: plan.ledgerSha256 });
    if (!(await HM.historyReady(fenced, plan.generation))) throw Error("history_apply_verification");
    return { pending: report.inventory.filter((x: any) => x.pending).map((x: any) => x.name), ...result };
  });
  const ingestAfter = await marker();
  ev("w2-history-applied", { ...out, ingestMarkerUnchanged: JSON.stringify(ingestBefore) === JSON.stringify(ingestAfter) });
  if (JSON.stringify(ingestBefore) !== JSON.stringify(ingestAfter)) throw Error("history apply touched the ingest marker");
}

// ---- 3/4. uploads around the rebind ----
const { TENANT, batch, record, scratchStore } = M.support;
const fs = scratchStore();
const rt = new pg.Client({ host: "127.0.0.1", port, user: "proof_admin", database: "postgres", password: "synthetic-owner-proof-only" }); await rt.connect();
const run = async (s: string, p: unknown[] = []) => { await rt.query("BEGIN"); await rt.query("SET LOCAL ROLE bulls96_session_ingest"); try { const r = await rt.query(s, p); await rt.query("COMMIT"); return r as never; } catch (e) { await rt.query("ROLLBACK"); throw e; } };
const depsFor = (g: string) => ({ ledger: M.ledgerSql.sqlLedger(run, { acceptanceMode: "buffered", expectedGeneration: g }), acceptanceMode: "buffered" as const, target: { store: fs.store, keyRoot: "96bulls" } });
await admin.query("SET ROLE postgres");
await admin.query("INSERT INTO sessions.ingest_frozen_source_completeness(source_identity_sha256,control_epoch,privacy_baseline_sha256,org_count,record_rows,manifest_sha256,neon_snapshot) VALUES($1,$2,$3,0,0,$4,'{}')", [identity, epoch, baselineSha, "e".repeat(64)]);
await admin.query("RESET ROLE");
const witness = async (raw: any) => { const p = await M.prepare.prepareDelivery(TENANT, raw);
  return (await run("SELECT sessions.ingest_frozen_source_witness($1::jsonb) AS r", [JSON.stringify({ kind: "batch", tenant: TENANT, originId: p.originId, batchId: p.batchId, batchDigest: p.batchDigest, originalRecordCount: raw.records.length,
    candidates: p.records.map((r: any, i: number) => ({ preparedOrd: i, recordId: r.record_id, revision: String(r.revision), revisionId: r.revision_id, candidateDigest: r.digest, source: r.source, sessionId: r.session_id })) })]) as any).rows[0].r; };
const attempt = async (g: string, id: string) => { const raw = await batch([record({ record_id: id })], { batch_id: id });
  try { const r = await M.deliver.deliverBatch({ ...depsFor(g), custody: await witness(raw) }, TENANT, raw); return { ok: true, ack: !!r.custodyAck }; }
  catch (e: any) { return { ok: false, why: String(e?.message ?? e).slice(0, 160) }; } };
const oldWorker = await attempt(G1, "w-old"), newWorkerBefore = await attempt(G2, "w-new-before");
ev("uploads-during-window", { oldWorker, newWorkerBefore });
if (oldWorker.ok || newWorkerBefore.ok) throw Error("an upload was accepted before the rebind");
const rc = { query: (t: string, p?: unknown[]) => admin.query(t, p as never) as never };
await admin.query("SET ROLE postgres");
const dry = await M.rebind.rebindIngestGeneration(rc, { from: G1, to: G2, dryRun: true });
const real = await M.rebind.rebindIngestGeneration(rc, { from: G1, to: G2 });
const again = await M.rebind.rebindIngestGeneration(rc, { from: G1, to: G2 });
await admin.query("RESET ROLE");
const adm = (await admin.query("SELECT schema_generation g,count(*)::int n FROM sessions.ingest_buffer_admission GROUP BY 1")).rows;
ev("w4-rebind", { dry, real, again, admissions: adm });
const after = await attempt(G2, "w-new-after"), oldAfter = await attempt(G1, "w-old-after");
ev("w5-uploads", { newWorker: after, oldWorkerStillRefused: !oldAfter.ok });
if (!after.ok || oldAfter.ok) throw Error("W5 upload expectations failed");
fs.done(); await rt.end(); await admin.end();

const result = { head: HEAD_SHA, base: BASE_SHA, G1, G2, passed: true, events };
await mkdir(root + "/.96bulls/window-rehearsal", { recursive: true });
await writeFile(root + "/.96bulls/window-rehearsal/result.json", JSON.stringify(result, null, 2) + "\n");
console.log(JSON.stringify({ passed: true, G1, G2, steps: events.map((e) => e.step) }));
