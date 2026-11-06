/** DESTRUCTIVE SYNTHETIC FIXTURE ONLY: Window #2 rehearsal.
 * Fixed owned local PostgreSQL 17 container, exact ownership label and loopback only.
 * Builds the verified Window #1 (G2) base, applies the eight Window #2 files through the real
 * owner path, checks brief publication remains off, rebinds 974 synthetic admissions,
 * and proves old/new Worker refusal before rebind and fresh ACK after it.
 * This fixture proves correctness, not production data-size or index-build timing.
 * No production credentials, network endpoint or apply authorization are accepted.
 * bun scripts/pg-proof/window2-rehearsal.ts --owned-fixture --base-root <G2 checkout>
 */
import pg from "pg";
import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { readFile, writeFile, mkdir } from "node:fs/promises";
import { createHash } from "node:crypto";

const root = fileURLToPath(new URL("../..", import.meta.url));
const argv = process.argv.slice(2);
if (argv[0] !== "--owned-fixture" || argv[1] !== "--base-root" || !argv[2] || argv.length !== 3) throw Error("owned_fixture_argument_required");
const base = argv[2].replace(/\/$/, "");
const container = "handover-w2-rehearsal-pg17-20261105";
const meta = JSON.parse(execFileSync("docker", ["inspect", "--format", "{{json .}}", container], { encoding: "utf8", maxBuffer: 1 << 20 }));
const binding = meta.NetworkSettings?.Ports?.["5432/tcp"];
if (meta.Config?.Labels?.["migration.owner"] !== "handover-window2-rehearsal" || !Array.isArray(binding) || binding.length !== 1 || binding[0].HostIp !== "127.0.0.1" || !/^\d+$/u.test(binding[0].HostPort))
  throw Error("owned_fixture_identity");
const port = Number(binding[0].HostPort);
const sh = (cwd: string, ...a: string[]) => execFileSync("git", ["-C", cwd, ...a], { encoding: "utf8" }).trim();
const BASE_SHA = sh(base, "rev-parse", "HEAD"), HEAD_SHA = sh(root, "rev-parse", "HEAD");
// Hash evidence must name the code that actually ran. Refuse dirty tracked inputs.
sh(base, "diff", "--quiet", "HEAD");
sh(root, "diff", "--quiet", "HEAD");
const LIVE_LEDGER = "3173f2df7f749fd79c340286f3284d91d8f5a8b988b7f1b1a11e01c3a3307641";

const B = { op: await import(base + "/src/hoopdb/owner-operator-apply.ts"), mig: await import(base + "/src/hoopdb/migrate.ts") };
const M = {
  op: await import(root + "/src/hoopdb/owner-operator-apply.ts"), mig: await import(root + "/src/hoopdb/migrate.ts"),
  rebind: await import(root + "/src/hoopdb/ingest-generation-rebind.ts"), ledgerSql: await import(root + "/src/session-ingest/ledger-sql.ts"),
  deliver: await import(root + "/src/session-ingest/deliver.ts"), prepare: await import(root + "/src/session-ingest/vendor/delivery/prepare.ts"),
  support: await import(root + "/src/session-ingest/__tests__/support.ts"),
};
const baseGeneration: string = await B.mig.generation(), targetGeneration: string = await M.mig.generation();
if (baseGeneration !== "78c7b28e5ebce7f5f534a1f3f41a55845251e950b6c4c49f676f2bda358c91d6" ||
    BASE_SHA !== "8f087322c9dc63ad1eae267167b9d93ffc6988cb" || baseGeneration === targetGeneration)
  throw Error("window2_base_pin");
const expectedPending = [
  "upstream:20261105020000_session_replay_pages.sql",
  "upstream:20261105021000_session_large_read_jobs.sql",
  "upstream:20261105022000_session_desktop_catalog.sql",
  "upstream:20261105023000_session_analysis_publications.sql",
  "upstream:20261105024000_session_brief_documents.sql",
  "upstream:20261105025000_canonical_feature_journal.sql",
  "upstream:20261105026000_canonical_history_lineage.sql",
  "upstream:20261105030000_canonical_feature_consumer_acks.sql",
];
const events: Record<string, unknown>[] = [];
const ev = (step: string, f: Record<string, unknown> = {}) => { const e = { step, ...f }; events.push(e); console.error(JSON.stringify(e)); };

const admin = new pg.Client({ host: "127.0.0.1", port, user: "proof_admin", database: "postgres", password: "synthetic-owner-proof-only" });
await admin.connect();
const v = (await admin.query("SELECT current_user me,current_setting('server_version_num')::int v")).rows[0];
if (v.me !== "proof_admin" || v.v < 170000 || v.v >= 180000) throw Error("owned_fixture_database_identity");

// ---- 1. live shape from the base checkout (same bootstrap as owner-apply-proof.ts) ----
const readB = (p: string) => readFile(base + "/" + p, "utf8");
await admin.query("DROP SCHEMA IF EXISTS hoopdb_boundary CASCADE;DROP SCHEMA IF EXISTS sessions CASCADE;DROP SCHEMA IF EXISTS history CASCADE;GRANT CREATE ON DATABASE postgres TO postgres;ALTER ROLE postgres SUPERUSER;SET ROLE postgres;CREATE SCHEMA sessions;GRANT CREATE ON SCHEMA public TO postgres;REVOKE TEMPORARY ON DATABASE postgres FROM PUBLIC;ALTER DEFAULT PRIVILEGES FOR ROLE postgres REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;");
await admin.query(await readB("supabase/tests/supabase_shim.sql"));
for (const f of ["session_directory_search_worker", "session_token_verifier", "session_ingest_roles"]) await admin.query(await readB(`supabase/bootstrap/${f}.sql`));
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
ev("base-applied", { baseSha: BASE_SHA, baseGeneration, marker: await marker(), ledgerSha256: baseLedger, matchesLiveLedger: baseLedger === LIVE_LEDGER });
if (baseLedger !== LIVE_LEDGER) throw Error("window2_base_ledger");
if ((await marker()).generation !== baseGeneration || (await marker()).status !== "pass") throw Error("base readiness not PASS(baseGeneration)");

const epoch = "3".repeat(64), identity = "7891c81907fd9db0e69a01c59a1a58bcd8c65b2d766dba9adb7fba793382acc4", baselineSha = "c".repeat(64);
const ownerFn = async (name: string, p: unknown) => (await admin.query(`SET ROLE postgres;`), (await admin.query(`SELECT sessions.${name}($1::jsonb) AS r`, [JSON.stringify(p)])).rows[0].r);
await ownerFn("ingest_lazy_policy_set", { baselineMode: "lazy-current", generation: baseGeneration, sourceIdentitySha256: identity, controlEpoch: epoch, privacyBaselineSha256: baselineSha,
  retentionContractSha256: "b".repeat(64), sourceProofSha256: "d".repeat(64), validUntil: new Date(Date.now() + 6 * 3600000).toISOString(), issuer: "relayhistory-cloud" });
await ownerFn("ingest_receipt_seat_initialize", { protocol: "delivery-acceptance-floor-v1", controlEpoch: epoch, capturedAt: "2026-11-04T00:00:00.000000Z",
  sourceSequences: ["sessions.delivery_catalog_acceptance_seq", "sessions.delivery_projection_acceptance_seq"].map((sequenceRegclass, i) => ({ sequenceRegclass, sequenceOid: String(i + 1), allocatedHighWater: "9007199254740993", isCalled: true, incrementBy: "1", cycle: false })),
  allocatedHighWater: "9007199254740993", generation: baseGeneration, sourceIdentitySha256: identity, sourceProofSha256: "d".repeat(64) });
await admin.query(`INSERT INTO sessions.ingest_buffer_admission(org_id,privacy_baseline_sha256,retention_contract_sha256,schema_generation,valid_until,control_epoch)
  SELECT 'org-'||g,$1,$2,$3,now()+interval '6 hours',$4 FROM generate_series(1,974) g`, [baselineSha, "b".repeat(64), baseGeneration, epoch]);
await admin.query("RESET ROLE");
ev("live-shape-seeded", { policy: baseGeneration, admissions: 974 });

// The separate history ledger is already complete at the G2 base; do not confuse its
// migrations with the Window #2 sessions generation change.
const HM = await import(base + "/src/history-db/migrate.ts");
{
  const c = owner();
  await B.op.withOwnerSession(c, async (pid: number) => {
    await HM.runHistoryMigrations(B.op.ownerFencedClient(c, pid));
  });
}
const historyBefore = JSON.stringify((await admin.query("SELECT name,sha256 FROM history.__history_migrations ORDER BY name")).rows);

// Window #2 adds a NOLOGIN canonical producer role. Its reviewed privileged
// bootstrap is a prerequisite, separate from the non-CREATEROLE owner apply.
// This fixture's proof_admin is the privileged bootstrap actor.
await admin.query(await readFile(root + "/supabase/bootstrap/session_ingest_roles.sql", "utf8"));
await admin.query(await readFile(root + "/supabase/bootstrap/session_read_api_roles.sql", "utf8"));
const producer = (await admin.query("SELECT rolcanlogin,rolsuper,rolinherit,rolcreaterole,rolcreatedb,rolreplication,rolbypassrls FROM pg_roles WHERE rolname='bulls96_canonical_feature_producer'")).rows;
if (producer.length !== 1 || Object.values(producer[0]).some(Boolean)) throw Error("window2_producer_bootstrap_attrs");
ev("canonical-producer-bootstrap", { noLogin: true, elevated: false });

// ---- 2. Window #2: the real owner-apply flow from THIS checkout ----
const pin = { sha: HEAD_SHA, generation: targetGeneration };
const wrapper = await M.op.wrapperSource();
let release: any;
{
  const c = owner();
  await M.op.withOwnerSession(c, async (pid: number) => {
    const fresh = await M.op.inspectOwner(c, pid);
    const pending = fresh.inventory.filter((r: any) => r.pending).map((r: any) => r.name);
    const planText = JSON.stringify({ version: 1, status: "planned_readonly_not_apply_approval", project: M.op.OWNER_APPLY.project, sourceSha: pin.sha, generation: pin.generation,
      ledgerSha256: fresh.ledgerSha256, boundary: "global-default-revoke", inventory: fresh.inventory, acl: fresh.acl, at: new Date().toISOString() }, null, 2) + "\n";
    ev("window2-plan", { pending, ledgerSha256: fresh.ledgerSha256, pinnedLedger: M.op.OWNER_APPLY.ledger });
    if (JSON.stringify(pending) !== JSON.stringify(expectedPending)) throw Error("window2_pending_inventory");
    const plan = M.op.validateReadonlyPlan(planText, pin);
    const capture = M.op.ownerCapture(planText, plan, pin, fresh, wrapper);
    const captureText = JSON.stringify(capture, null, 2) + "\n";
    const approvalText = JSON.stringify({ version: 1, decision: "approved", by: "Khaliq Gant", at: new Date(Date.now() + 1000).toISOString(),
      quote: "Synthetic offline fixture only; not authorization", execution: M.op.OWNER_APPLY.execution,
      project: M.op.OWNER_APPLY.project, sourceSha: capture.source.sha, generation: capture.source.generation, ledgerSha256: capture.ledgerSha256,
      planSha256: createHash("sha256").update(planText).digest("hex"), captureSha256: createHash("sha256").update(captureText).digest("hex"),
      captureGeneration: capture.captureGeneration, wrapperSha256: capture.wrapperSha256 }, null, 2) + "\n";
    release = await M.op.applyOwnerRelease(c, pid, { pin, planText, captureText, approvalText, wrapper, recheckSource: async () => {} });
  });
}
ev("window2-applied", { status: release.status, generation: release.generation, marker: await marker() });
if (release.status !== "owner_apply_verified" || (await marker()).generation !== targetGeneration) throw Error("Window #2 did not reach the target generation");

const control = (await admin.query("SELECT live FROM sessions.session_brief_document_control WHERE singleton")).rows;
const briefCounts = (await admin.query("SELECT (SELECT count(*)::int FROM sessions.session_brief_document_scopes) AS scopes,(SELECT count(*)::int FROM sessions.session_brief_document_outbox) AS outbox")).rows[0];
if (control.length !== 1 || control[0].live !== false || briefCounts.scopes !== 0 || briefCounts.outbox !== 0)
  throw Error("window2_brief_gate_not_dormant");
ev("brief-publication-default-off", briefCounts);
const historyAfter = JSON.stringify((await admin.query("SELECT name,sha256 FROM history.__history_migrations ORDER BY name")).rows);
if (historyAfter !== historyBefore) throw Error("window2_changed_history_ledger");
ev("history-ledger-unchanged");
// ---- 3/4. uploads around the rebind ----
const { TENANT, batch, record, scratchStore } = M.support;
const fs = scratchStore();
const rt = new pg.Client({ host: "127.0.0.1", port, user: "proof_admin", database: "postgres", password: "synthetic-owner-proof-only" }); await rt.connect();
// Capture the database refusal before the production adapter deliberately
// redacts SQL detail. This fixed, synthetic-only fixture has no customer input.
let sqlFailure: { code: string; message: string } | undefined;
const run = async (s: string, p: unknown[] = []) => { await rt.query("BEGIN"); await rt.query("SET LOCAL ROLE bulls96_session_ingest"); try { const r = await rt.query(s, p); await rt.query("COMMIT"); return r as never; } catch (e: any) { sqlFailure = { code: String(e?.code), message: String(e?.message) }; await rt.query("ROLLBACK"); throw e; } };
const depsFor = (g: string) => ({ ledger: M.ledgerSql.sqlLedger(run, { acceptanceMode: "buffered", expectedGeneration: g }), acceptanceMode: "buffered" as const, target: { store: fs.store, keyRoot: "96bulls" } });
await admin.query("SET ROLE postgres");
await admin.query("INSERT INTO sessions.ingest_frozen_source_completeness(source_identity_sha256,control_epoch,privacy_baseline_sha256,org_count,record_rows,manifest_sha256,neon_snapshot) VALUES($1,$2,$3,0,0,$4,'{}')", [identity, epoch, baselineSha, "e".repeat(64)]);
await admin.query("RESET ROLE");
const witness = async (raw: any) => { const p = await M.prepare.prepareDelivery(TENANT, raw);
  return (await run("SELECT sessions.ingest_frozen_source_witness($1::jsonb) AS r", [JSON.stringify({ kind: "batch", tenant: TENANT, originId: p.originId, batchId: p.batchId, batchDigest: p.batchDigest, originalRecordCount: raw.records.length,
    candidates: p.records.map((r: any, i: number) => ({ preparedOrd: i, recordId: r.record_id, revision: String(r.revision), revisionId: r.revision_id, candidateDigest: r.digest, source: r.source, sessionId: r.session_id })) })]) as any).rows[0].r; };
const attempt = async (g: string, id: string) => { sqlFailure = undefined; const raw = await batch([record({ record_id: id })], { batch_id: id });
  try { const r = await M.deliver.deliverBatch({ ...depsFor(g), custody: await witness(raw) }, TENANT, raw); return { ok: true, ack: !!r.custodyAck }; }
  catch (e: any) { return { ok: false, why: String(e?.message ?? e).slice(0, 160), sqlFailure }; } };
const oldWorker = await attempt(baseGeneration, "w2-old"), newWorkerBefore = await attempt(targetGeneration, "w2-new-before");
ev("uploads-during-window", { oldWorker, newWorkerBefore });
const refusedFor = (result: Awaited<ReturnType<typeof attempt>>, message: string) =>
  !result.ok && "sqlFailure" in result && result.sqlFailure?.code === "55000" && result.sqlFailure.message === message;
if (!refusedFor(oldWorker, "session ingest: generation not ready") ||
    !refusedFor(newWorkerBefore, "session ingest: current custody policy unavailable"))
  throw Error("window2_expected_generation_policy_refusal");
const rc = { query: (t: string, p?: unknown[]) => admin.query(t, p as never) as never };
await admin.query("SET ROLE postgres");
const dry = await M.rebind.rebindIngestGeneration(rc, { from: baseGeneration, to: targetGeneration, dryRun: true });
const real = await M.rebind.rebindIngestGeneration(rc, { from: baseGeneration, to: targetGeneration });
const again = await M.rebind.rebindIngestGeneration(rc, { from: baseGeneration, to: targetGeneration });
await admin.query("RESET ROLE");
const adm = (await admin.query("SELECT schema_generation g,count(*)::int n FROM sessions.ingest_buffer_admission GROUP BY 1")).rows;
if (adm.length !== 1 || adm[0].g !== targetGeneration || adm[0].n !== 974)
  throw Error("window2_admission_rebind");
if (dry.admissionsRebound !== 974 || real.admissionsRebound !== 974 ||
    again.admissionsRebound !== 0 || again.policy !== "already")
  throw Error("window2_rebind_idempotency");
ev("generation-rebind", { dry, real, again, admissions: adm });
const after = await attempt(targetGeneration, "w2-new-after"), oldAfter = await attempt(baseGeneration, "w2-old-after");
ev("uploads-after-rebind", { newWorker: after, oldWorkerStillRefused: !oldAfter.ok });
if (!after.ok || !("ack" in after) || !after.ack || !refusedFor(oldAfter, "session ingest: generation not ready")) throw Error("window2_upload_expectations");
fs.done(); await rt.end(); await admin.end();

const result = { syntheticOnly: true, head: HEAD_SHA, base: BASE_SHA, baseGeneration, targetGeneration, passed: true, events };
await mkdir(root + "/.96bulls/window2-rehearsal", { recursive: true });
await writeFile(root + "/.96bulls/window2-rehearsal/result.json", JSON.stringify(result, null, 2) + "\n");
console.log(JSON.stringify({ passed: true, baseGeneration, targetGeneration, steps: events.map((e) => e.step) }));
