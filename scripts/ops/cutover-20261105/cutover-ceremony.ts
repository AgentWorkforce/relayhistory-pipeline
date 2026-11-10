/**
 * Buffered-upload cutover ceremony runner (lazy-current custody), steps 2-6 of
 * relayhistory-cloud docs/buffered-delivery-cutover.md. Source-authoritative SQL and the
 * reviewed exporter are IMPORTED from the pinned worktrees, never copied.
 *
 *   bun cutover-ceremony.ts --dry-run   [--retention-sha <64hex>] [--runtime-host <host>]
 *   bun cutover-ceremony.ts --execute    --retention-sha <64hex>  --runtime-host <host> [--valid-days 30]
 *
 * stdin (one JSON line, RAM only): {"neonOwnerUrl":"postgresql://neondb_owner:...","neonRuntimeUrl":"postgresql://relayhistory_app_runtime:..."}
 * Supabase owner: the existing Supabase CLI temporary login (cli_login_postgres + SET ROLE postgres), no password input.
 * State (non-secret, resumable): ~/relay-briefs/bulls-resume-20261104/ceremony/state.json
 *
 * Preconditions the CALLER owns (Cloudflare side, not done here):
 *   - relayhistory-api runs a Worker version that is DELIVERY_CUTOVER {"version":1,"mode":"hold"} OR accepts DB-held 5xx;
 *   - relayhistory-neon-replica queue delivery is PAUSED for the whole window (seeding refuses replica writes);
 *   - after step 6, deploy the ingest Worker version with the printed DELIVERY_* values.
 */
import { readFileSync, writeFileSync, mkdirSync, existsSync, openSync, writeSync, closeSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { createHash, randomBytes } from "node:crypto";

const RHC = "/home/khaliqgant/Projects/AgentWorkforce/relayhistory-cloud-worktrees/delivery-cutover-0078-release-20261104";
const SP = "/home/khaliqgant/Projects/AgentWorkforce/sessions-pipeline-worktrees/private-ingest-operator-final-462-20261104";
const DIR = process.env.HOME + "/relay-briefs/bulls-resume-20261104/" + (process.argv.includes("--rehearsal") ? "ceremony-rehearsal" : "ceremony");
// --rehearsal <neon-branch-endpoint> <local-fixture-db>: Neon writes go to that BRANCH endpoint and
// the target is the local docker PG17 fixture (127.0.0.1:32842). Never production.
const REHEARSAL = process.argv.includes("--rehearsal");
const REH_ENDPOINT = REHEARSAL ? process.argv[process.argv.indexOf("--rehearsal") + 1] : undefined;
const REH_DB = REHEARSAL ? process.argv[process.argv.indexOf("--rehearsal") + 2] : undefined;
// Production Neon identifiers come from the environment (never committed); the runner refuses without them.
const fail = (m: string): never => { console.error(m); process.exit(64); };
const PROD_ENDPOINT = process.env.NEON_ENDPOINT ?? fail("NEON_ENDPOINT required (production Neon endpoint id, ep-...)");
const NEON_PROJECT_ID = process.env.NEON_PROJECT_ID ?? fail("NEON_PROJECT_ID required");
const NEON_BRANCH_ID = process.env.NEON_BRANCH_ID ?? fail("NEON_BRANCH_ID required");
if (!PROD_ENDPOINT.startsWith("ep-")) fail("NEON_ENDPOINT must be an ep-... endpoint id");
if (REHEARSAL && (!REH_ENDPOINT?.startsWith("ep-") || REH_ENDPOINT.startsWith(PROD_ENDPOINT) || !/^rehearsal_target_[0-9]+$/.test(REH_DB ?? ""))) { console.error("bad --rehearsal args"); process.exit(64); }
const PROD_NEON_ENDPOINT = REHEARSAL ? REH_ENDPOINT! : PROD_ENDPOINT;
const NEON = { project: NEON_PROJECT_ID, branch: NEON_BRANCH_ID };
const SUPA = { project: "pyamkrhoyszgzirqqffq", host: "aws-0-us-east-1.pooler.supabase.com" };
const GENERATION = "d2569f3498e6b66302aef514a04711304e2ee263b2e413f7488f8d142a8958f0";
const RHC_MAIN = "e6bf92647ff6d326d67f108dc13fa8e6649f549a";
const PRIVACY_PACKET_MAX = 512 * 1024, PRIVACY_ENTRIES_MAX = 5000;

const pg = (await import(SP + "/node_modules/pg/lib/index.js")).default;
const { exportPrivacyBaseline } = await import(RHC + "/scripts/lib/delivery-cutover-baseline.mjs");
const { CURRENT_CUSTODY_AUTHORITY_SQL } = await import(RHC + "/packages/relayhistory/src/lib/delivery-custody-witness.ts");
const { parseCliLogin } = await import(SP + "/scripts/schema-release-owner-operator-plan.ts");
const { SUPABASE_PROD_CA_2021 } = await import(SP + "/src/session-pipeline/supabase-ca.ts");
const { generation } = await import(SP + "/src/hoopdb/migrate.ts");

const sha = (s: string | Uint8Array) => createHash("sha256").update(s).digest("hex");
const hex64 = (s: unknown): s is string => typeof s === "string" && /^[0-9a-f]{64}$/.test(s);
const arg = (k: string) => { const i = process.argv.indexOf(k); return i > 0 ? process.argv[i + 1] : undefined; };
const ADMIT = process.argv.includes("--admit-new-orgs");
const EXECUTE = process.argv.includes("--execute") || ADMIT, DRY = process.argv.includes("--dry-run");
if ([process.argv.includes("--execute"), DRY, ADMIT].filter(Boolean).length !== 1) { console.error("exactly one of --execute / --dry-run / --admit-new-orgs"); process.exit(64); }
mkdirSync(DIR, { recursive: true, mode: 0o700 });
const STATE = DIR + "/state.json", LOG = DIR + (EXECUTE ? "/execute.log" : "/dry-run.log");
const state: any = existsSync(STATE) && EXECUTE ? JSON.parse(readFileSync(STATE, "utf8")) : { steps: {} };
const save = () => EXECUTE && writeFileSync(STATE, JSON.stringify(state, null, 1) + "\n", { mode: 0o600 });
const t0 = Date.now();
const log = (m: string, f: Record<string, unknown> = {}) => {
  const l = JSON.stringify({ at: new Date().toISOString(), tMs: Date.now() - t0, m, ...f });
  console.log(l); writeFileSync(LOG, l + "\n", { flag: "a", mode: 0o600 });
};
class Stop extends Error { constructor(readonly step: string, readonly why: string) { super(step + ": " + why); } }
const stop = (step: string, why: string): never => { throw new Stop(step, why); };

// ---- inputs ----
const input = JSON.parse(readFileSync(0, "utf8").split("\n")[0] || "{}");
const ownerUrl: string = input.neonOwnerUrl ?? "", runtimeUrl: string = input.neonRuntimeUrl ?? "";
const retention = arg("--retention-sha"), runtimeHost = arg("--runtime-host");
const validDays = Number(arg("--valid-days") ?? 30);
if (!ownerUrl) stop("inputs", "neonOwnerUrl missing on stdin");
if (EXECUTE && !ADMIT && (!hex64(retention) || !runtimeHost || !runtimeUrl)) stop("inputs", "--execute needs --retention-sha, --runtime-host and neonRuntimeUrl");
if (!(validDays >= 1 && validDays <= 30)) stop("inputs", "--valid-days 1..30");

// ---- connections ----
const neonClient = (url: string, app: string) => new pg.Client({ connectionString: url, ssl: { rejectUnauthorized: true }, application_name: app, statement_timeout: 0 });
async function supabaseOwner() {
  if (REHEARSAL) {
    const c = new pg.Client({ host: "127.0.0.1", port: 32842, user: "postgres", database: REH_DB, application_name: "96bulls-cutover-rehearsal" });
    await c.connect(); return c;
  }
  const raw = execFileSync("/home/khaliqgant/.local/bin/supabase", ["db", "dump", "--linked", "--project-ref", SUPA.project, "--dry-run"],
    { cwd: SP, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"], timeout: 30000, maxBuffer: 65536 });
  const login = parseCliLogin(raw);
  const c = new pg.Client({ host: SUPA.host, port: 5432, database: "postgres", user: "cli_login_postgres." + SUPA.project, password: login.password,
    ssl: { ca: SUPABASE_PROD_CA_2021, rejectUnauthorized: true, servername: SUPA.host }, connectionTimeoutMillis: 10000,
    options: "-c statement_timeout=60000 -c lock_timeout=5000 -c idle_in_transaction_session_timeout=60000", application_name: "96bulls-cutover-ceremony" });
  login.password = "";
  await c.connect(); await c.query("SET SESSION ROLE postgres");
  const id = (await c.query("SELECT current_user u, current_database() d")).rows[0];
  if (id.u !== "postgres" || id.d !== "postgres") stop("supabase", "owner identity " + JSON.stringify(id));
  return c;
}

let owner: any, runtime: any, supa: any;
try {
  // ---- S0 preflight (read-only in both modes) ----
  if (new URL(ownerUrl).hostname.split(".")[0] !== PROD_NEON_ENDPOINT) stop("S0", "owner URL is not the production endpoint " + PROD_NEON_ENDPOINT);
  const rhcHead = execFileSync("git", ["-C", RHC, "rev-parse", "HEAD"], { encoding: "utf8" }).trim();
  const rhcDiff = execFileSync("git", ["-C", RHC, "diff", "--stat", RHC_MAIN, "HEAD", "--", "packages/relayhistory/db", "packages/relayhistory/src/lib/delivery-custody-witness.ts", "scripts/lib/delivery-cutover-baseline.mjs"], { encoding: "utf8" }).trim();
  if (rhcDiff) stop("S0", "RHC worktree differs from main " + RHC_MAIN + " in ceremony sources");
  const G = await generation();
  if (G !== GENERATION) stop("S0", "sessions-pipeline generation " + G);
  owner = neonClient(ownerUrl, "96bulls-cutover-ceremony"); await owner.connect();
  const nid = (await owner.query("SELECT current_user u, current_database() d")).rows[0];
  if (nid.u !== "neondb_owner" || nid.d !== "neondb") stop("S0", "neon owner identity");
  const status0 = (await owner.query("SELECT sessions.delivery_cutover_status() s")).rows[0].s;
  log("S0 neon", { rhcHead, status: status0 });
  supa = await supabaseOwner();
  const ready = (await supa.query("SELECT generation,status FROM sessions.__96bulls_role_validation WHERE name='session_directory_search_worker'")).rows[0];
  if (ready?.generation !== G || ready.status !== "pass") stop("S0", "supabase readiness not PASS for " + G);
  const tgt = (await supa.query(`SELECT (SELECT count(*) FROM sessions.ingest_lazy_policy)::int pol, (SELECT count(*) FROM sessions.ingest_receipt_seat_clock)::int seat,
     (SELECT count(*) FROM sessions.ingest_buffer_admission)::int adm, (SELECT count(*) FROM sessions.ingest_fence_owners)::int owners,
     (SELECT count(*) FROM sessions.ingest_batches)::int batches, (SELECT count(*) FROM sessions.ingest_record_revisions)::int revs`)).rows[0];
  log("S0 supabase", { readiness: ready, target: tgt });
  if (ADMIT) {
    // Post-switch provisioning: orgs created after the ceremony stay closed (lazy guard) until admitted
    // with the SAME stored epoch/privacy/retention/generation. Idempotent; never changes existing rows.
    const s4 = state.steps.S4, s2 = state.steps.S2;
    if (!state.steps.S6 || !s4 || !s2) stop("admit", "ceremony not complete in state.json");
    const pol = (await supa.query("SELECT control_epoch, privacy_baseline_sha256, retention_contract_sha256, generation FROM sessions.ingest_lazy_policy WHERE singleton")).rows[0];
    if (!pol || pol.control_epoch !== state.epoch || pol.privacy_baseline_sha256 !== s2.descriptor.sha256) stop("admit", "live policy differs from state");
    const registry = (await owner.query("SELECT id::text AS id FROM public.organizations ORDER BY id")).rows.map((r: any) => r.id);
    let added = 0;
    await supa.query("BEGIN");
    for (const org of registry)
      added += (await supa.query("INSERT INTO sessions.ingest_buffer_admission(org_id,privacy_baseline_sha256,retention_contract_sha256,schema_generation,valid_until,control_epoch) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT (org_id) DO NOTHING",
        [org, pol.privacy_baseline_sha256, pol.retention_contract_sha256, pol.generation, s4.validUntil, pol.control_epoch])).rowCount;
    await supa.query("COMMIT");
    log("admit-new-orgs", { registry: registry.length, added });
    process.exit(0);
  }
  const resuming = Object.keys(state.steps).length > 0;
  if (!resuming && (tgt.pol || tgt.seat || tgt.adm || tgt.owners || tgt.batches || tgt.revs)) stop("S0", "target already holds authority " + JSON.stringify(tgt));
  if (!resuming && status0?.phase !== "legacy") stop("S0", "neon phase is " + status0?.phase + " with no local resume state");
  let S: string | undefined;
  if (runtimeUrl) {
    if (!runtimeHost) stop("S0", "--runtime-host required with neonRuntimeUrl");
    runtime = neonClient(runtimeUrl, "96bulls-cutover-identity"); await runtime.connect();
    const rr = (await runtime.query("SELECT current_user u, session_user s")).rows[0];
    if (rr.u !== "relayhistory_app_runtime" || rr.s !== "relayhistory_app_runtime") stop("S0", "runtime connection role " + JSON.stringify(rr));
    const a = (await runtime.query("WITH authority AS(" + CURRENT_CUSTODY_AUTHORITY_SQL + ") SELECT value AS authority, encode(sha256(convert_to(value::text,'UTF8')),'hex') AS id FROM authority", [runtimeHost])).rows[0];
    S = a.id;
    writeFileSync(DIR + "/source-authority.json", JSON.stringify(a.authority, null, 1) + "\n", { mode: 0o600 });
    log("S0 source identity", { sourceIdentitySha256: S, hostname: runtimeHost, connectionRole: a.authority?.connectionRole });
  }
  let R = retention;
  if (R) {
    log("S0 retention", { retentionContractSha256: R, note: "must be the GC lane's ARMED_HEALTHY artifact sha, never the DORMANT one" });
  }
  const orgRegistry = (await owner.query("SELECT id::text AS id FROM public.organizations ORDER BY id")).rows.map((r: any) => r.id);
  log("S0 org registry", { organizations: orgRegistry.length });
  if (DRY) { log("DRY-RUN complete: no writes", { wouldUseGeneration: G, sourceIdentitySha256: S ?? null }); process.exit(0); }

  // ---- S1 seeding ----
  state.epoch ??= randomBytes(32).toString("hex"); save();
  const E: string = state.epoch;
  if (!state.steps.S1) {
    const r = (await owner.query("SELECT sessions.owner_delivery_cutover_v1('seeding',$1,NULL) AS r", [E])).rows[0].r;
    const back = (await owner.query("SELECT sessions.delivery_cutover_status() s")).rows[0].s;
    if (back?.phase !== "seeding" || back.epoch !== E) stop("S1", "seeding readback " + JSON.stringify(back));
    state.steps.S1 = { at: new Date().toISOString(), result: r }; save(); log("S1 seeding committed", { epoch: E });
  }

  // ---- S2 held privacy export (reviewed exporter, sticky RR READ ONLY) ----
  if (!state.steps.S2) {
    const source = { async readOnly(cb: any) {
      const c = neonClient(ownerUrl, "96bulls-cutover-export"); await c.connect();
      try { await c.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY"); const v = await cb(c); await c.query("COMMIT"); return v; }
      catch (e) { await c.query("ROLLBACK").catch(() => {}); throw e; } finally { await c.end().catch(() => {}); } } };
    const pk = DIR + "/privacy-packets.jsonl"; const fd = openSync(pk, "w", 0o600);
    const orgs = new Set<string>(); let maxBytes = 0, maxEntries = 0;
    const desc = await exportPrivacyBaseline(source, E, async (packet: any, meta: any) => {
      const j = JSON.stringify(packet), b = Buffer.byteLength(j), n = packet.deletions.length + packet.resets.length;
      if (b > PRIVACY_PACKET_MAX || n > PRIVACY_ENTRIES_MAX) stop("S2", "lazy privacy invariant: packet " + meta.ordinal + " is " + b + " B / " + n + " entries");
      maxBytes = Math.max(maxBytes, b); maxEntries = Math.max(maxEntries, n); orgs.add(packet.org_id);
      writeSync(fd, j + "\n");
    }, { page: 500, captureAcceptanceFloor: true });
    closeSync(fd);
    if (!desc?.complete || !hex64(desc.sha256) || !desc.baselineAcceptanceFloor) stop("S2", "incomplete descriptor");
    state.steps.S2 = { at: new Date().toISOString(), descriptor: desc, packetsSha256: sha(readFileSync(pk)), maxPacketBytes: maxBytes, maxEntries, exportOrgs: [...orgs].sort() };
    save(); log("S2 export complete", { scopes: desc.scopes, deletions: desc.deletions, resets: desc.resets, emptyScopes: desc.emptyScopes, privacyBaselineSha256: desc.sha256, maxPacketBytes: maxBytes });
  }
  const D = state.steps.S2.descriptor;

  // ---- S3 capture artifact -> sourceProofSha256 ----
  if (!S) stop("S3", "sourceIdentitySha256 needs neonRuntimeUrl");
  if (!state.steps.S3) {
    const { baselineAcceptanceFloor: floor, ...descNoFloor } = D;
    const artifact = JSON.stringify({ protocol: "96bulls-cutover-capture-v1", provider: "neon", project: NEON.project, branch: NEON.branch,
      endpoint: PROD_NEON_ENDPOINT, hostname: runtimeHost, sourceIdentitySha256: S, epoch: E, generation: G, rhcMain: RHC_MAIN,
      descriptor: descNoFloor, packetsSha256: state.steps.S2.packetsSha256, floor, exportOrgs: state.steps.S2.exportOrgs.length });
    writeFileSync(DIR + "/capture-artifact.json", artifact, { mode: 0o600 });
    state.steps.S3 = { at: new Date().toISOString(), sourceProofSha256: sha(artifact), sourceIdentitySha256: S }; save();
    log("S3 capture", { sourceProofSha256: state.steps.S3.sourceProofSha256 });
  }
  const P = state.steps.S3.sourceProofSha256;

  // ---- S4 target owner writes, ONE transaction: admission rows -> lazy policy -> seat clock ----
  if (!state.steps.S4) {
    const orgs = [...new Set([...state.steps.S2.exportOrgs, ...orgRegistry])].sort();
    const validUntil = new Date(Date.now() + validDays * 86400000).toISOString();
    const floor = D.baselineAcceptanceFloor;
    await supa.query("BEGIN");
    try {
      for (const org of orgs)
        await supa.query("INSERT INTO sessions.ingest_buffer_admission(org_id,privacy_baseline_sha256,retention_contract_sha256,schema_generation,valid_until,control_epoch) VALUES($1,$2,$3,$4,$5,$6)",
          [org, D.sha256, R, G, validUntil, E]);
      const pol = (await supa.query("SELECT sessions.ingest_lazy_policy_set($1::jsonb) r", [JSON.stringify({ baselineMode: "lazy-current", generation: G,
        sourceIdentitySha256: S, controlEpoch: E, privacyBaselineSha256: D.sha256, retentionContractSha256: R, sourceProofSha256: P, validUntil, issuer: "relayhistory-cloud" })])).rows[0].r;
      const seat = (await supa.query("SELECT sessions.ingest_receipt_seat_initialize($1::jsonb) r", [JSON.stringify({ protocol: floor.protocol, controlEpoch: floor.controlEpoch,
        capturedAt: floor.capturedAt, sourceSequences: floor.sourceSequences, allocatedHighWater: floor.allocatedHighWater, generation: G, sourceIdentitySha256: S, sourceProofSha256: P })])).rows[0].r;
      await supa.query("SELECT sessions.ingest_lazy_guard($1)", [orgs[0]]);
      await supa.query("COMMIT");
      state.steps.S4 = { at: new Date().toISOString(), admittedOrgs: orgs.length, orgsSha256: sha(orgs.join("\n")), validUntil, policy: pol, seat }; save();
      log("S4 target authority committed", state.steps.S4);
    } catch (e: any) { await supa.query("ROLLBACK").catch(() => {}); stop("S4", e?.message ?? String(e)); }
  }

  // ---- S5 activation ----
  if (!state.steps.S5) {
    const r = (await owner.query("SELECT sessions.owner_delivery_cutover_v1('active',$1,$2) AS r", [E, D.sha256])).rows[0].r;
    const back = (await owner.query("SELECT sessions.delivery_cutover_status() s")).rows[0].s;
    if (back?.phase !== "active" || back.privacyBaselineSha256 !== D.sha256) stop("S5", "active readback " + JSON.stringify(back));
    state.steps.S5 = { at: new Date().toISOString(), result: r }; save(); log("S5 active committed");
  }

  // ---- S6 RHC configuration to deploy (printed; the caller deploys the ingest Worker version) ----
  const config = {
    DELIVERY_CUTOVER: JSON.stringify({ version: 1, mode: "ingest", epoch: E, privacyBaselineSha256: D.sha256 }),
    DELIVERY_CUSTODY_SOURCE: JSON.stringify({ version: 1, hostname: runtimeHost, sourceIdentitySha256: S }),
    DELIVERY_FORWARD: JSON.stringify({ mode: "on", percent: 100, onUnreachable: "503" }),
    NEON_APP_DATABASE_URL_role: "relayhistory_app_runtime @ " + runtimeHost,
  };
  writeFileSync(DIR + "/rhc-ingest-config.json", JSON.stringify(config, null, 1) + "\n", { mode: 0o600 });
  state.steps.S6 = { at: new Date().toISOString(), config }; save();
  log("S6 READY: deploy the ingest Worker version with rhc-ingest-config.json", config);
} catch (e: any) {
  const step = e instanceof Stop ? e.step : "unexpected";
  log("STOPPED", { step, why: e instanceof Stop ? e.why : String(e?.message ?? e).slice(0, 300), resume: "fix the cause, then rerun the same --execute command; completed steps in state.json are skipped" });
  process.exitCode = 2;
} finally {
  await Promise.all([owner, runtime, supa].filter(Boolean).map((c) => c.end().catch(() => {})));
}
