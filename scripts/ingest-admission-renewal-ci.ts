/**
 * CI proof of the dormant admission renewal operator (src/hoopdb/ingest-admission-renewal-operator.ts) on PGlite
 * over EVERY repository migration: a synthetic custody policy, seat clock, readiness marker and N admission rows;
 * a production-shaped migration ledger (every step recorded except the last two upstream steps, which are held);
 * the marker carries an opaque historical generation that nothing in this tree reproduces (the operator takes the
 * live PASS marker's generation as-is and proves the ledger prefix separately);
 * readback, plan (writes nothing), renew (policy and every row move together; ingest_lazy_guard admits every org
 * afterwards), and a refusal on drift that leaves both tables unchanged. Synthetic identifiers and a fixture project
 * ref only. Exports the cached fixture the pglite test reuses. Run: bun scripts/ingest-admission-renewal-ci.ts
 */
import { initScrubEngine } from "../packages/pipeline/src/lib/scrub-engine/index.ts";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { createHash } from "node:crypto";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { SESSION_BOOTSTRAP_FILES } from "../src/hoopdb/session-bootstrap-chain.ts";
import { expectedMigrationNames, schemaStepDigests } from "../src/hoopdb/migrate.ts";
import {
  admissionRenewalConfirmation,
  admissionRenewalInputs,
  runAdmissionRenewal,
  type AdmissionRenewalMode,
  type AdmissionRenewalPlan,
} from "../src/hoopdb/ingest-admission-renewal-operator.ts";

const root = join(import.meta.dir, "..");
const steps = await expectedMigrationNames();
const upstream = steps.filter((n) => n.startsWith("upstream:"));
/** The production shape: the last two upstream steps are held (unrecorded); every other step is recorded. */
const HELD = upstream.slice(-2);
/** The recorded ledger rows, in migrator order, with the file sha256 the migrator records. */
const LEDGER = await schemaStepDigests(HELD);
export const RENEWAL_FIXTURE = {
  // An opaque historical PASS generation: deliberately not generation() or any recomputation of this tree.
  generation: "a".repeat(64),
  held: HELD, ledger: LEDGER, ledgerTail: upstream.at(-3)!, stepCount: LEDGER.length,
  // Independent of the operator: sha256 over the JSON of the recorded [name, file sha256] pairs.
  ledgerPrefixDigest: createHash("sha256").update(JSON.stringify(LEDGER)).digest("hex"),
  source: "c".repeat(64), epoch: "3".repeat(64), privacy: "5".repeat(64),
  retention: "b".repeat(64), proof: "d".repeat(64), sha: "1".repeat(40), project: "fixtureprojectref000",
  planRun: "424242", renewRun: "434343",
  // Mixed case, digits and punctuation: C (byte) order differs from a locale collation.
  orgs: ["org_b", "Org-A", "org-a", "org10", "org9", "ORG.Z"],
} as const;
const F = RENEWAL_FIXTURE;
let cached: File | Blob | undefined;

async function template(): Promise<File | Blob> {
  if (cached) return cached;
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  const read = (p: string) => readFileSync(join(root, p), "utf8");
  await db.exec(read("supabase/tests/supabase_shim.sql"));
  for (const f of SESSION_BOOTSTRAP_FILES) await db.exec(read(f));
  for (const f of readdirSync(join(root, "supabase/migrations")).filter((f) => f.endsWith(".sql")).sort())
    await db.exec(read("supabase/migrations/" + f));
  await db.exec(`CREATE TABLE sessions.__96bulls_migrations(name text PRIMARY KEY, sha256 text NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT now())`);
  for (const [name, sha] of F.ledger)
    await db.query("INSERT INTO sessions.__96bulls_migrations(name, sha256) VALUES($1, $2)", [name, sha]);
  await db.exec(`CREATE TABLE sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text);
    INSERT INTO sessions.__96bulls_role_validation VALUES('session_directory_search_worker','${F.generation}','pass')`);
  await db.query(`INSERT INTO sessions.ingest_receipt_seat_clock(singleton,control_epoch,source_identity_sha256,
    allocated_high_water,last_seat,baseline,initial_generation) VALUES(true,$1,$2,0,0,$3::jsonb,$4)`,
    [F.epoch, F.source, JSON.stringify({ sourceProofSha256: F.proof }), F.generation]);
  cached = await db.dumpDataDir("none");
  await db.close();
  return cached;
}

export interface RenewalFixture {
  db: PGlite;
  client: { query: (text: string, params?: unknown[]) => Promise<{ rows: any[] }> };
  validUntil: string;
  /** Run one mode through admissionRenewalInputs (the confirmation is built from the plan unless overridden). */
  op: (mode: AdmissionRenewalMode, options?: {
    plan?: AdmissionRenewalPlan | null; confirmation?: string; enabled?: boolean;
    now?: number; client?: RenewalFixture["client"];
  }) => ReturnType<typeof runAdmissionRenewal>;
  state: () => Promise<{ policy: string; policyUpdated: string; rows: { org_id: string; v: string }[] }>;
  close: () => Promise<void>;
}

/** A fresh database from the cached template: policy at `validUntil` (default now + 2h, whole ms) and N rows. */
export async function renewalFixture(options: { validUntil?: string; orgs?: readonly string[] } = {}): Promise<RenewalFixture> {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm }, loadDataDir: await template() });
  const validUntil = options.validUntil ?? new Date(Math.floor(Date.now() / 1000) * 1000 + 2 * 3600_000).toISOString();
  await db.query("SELECT sessions.ingest_lazy_policy_set($1::jsonb)", [JSON.stringify({
    baselineMode: "lazy-current", generation: F.generation, sourceIdentitySha256: F.source, controlEpoch: F.epoch,
    privacyBaselineSha256: F.privacy, retentionContractSha256: F.retention, sourceProofSha256: F.proof, validUntil,
    issuer: "relayhistory-cloud",
  })]);
  for (const org of options.orgs ?? F.orgs)
    await db.query(`INSERT INTO sessions.ingest_buffer_admission(org_id,privacy_baseline_sha256,retention_contract_sha256,
      schema_generation,valid_until,control_epoch) VALUES($1,$2,$3,$4,$5::timestamptz,$6)`,
      [org, F.privacy, F.retention, F.generation, validUntil, F.epoch]);
  const client = {
    query: async (text: string, params?: unknown[]) =>
      (params?.length ? db.query(text, params as never) : db.query(text)) as Promise<{ rows: any[] }>,
  };
  const op: RenewalFixture["op"] = (mode, o = {}) => {
    const plan = o.plan ?? null;
    const input = admissionRenewalInputs({
      RENEWAL_MODE: mode, RENEWAL_SHA: F.sha, RENEWAL_PLAN_RUN: mode === "renew" ? (plan?.planRun ?? F.planRun) : "",
      RENEWAL_CONFIRMATION: o.confirmation ?? admissionRenewalConfirmation(mode, F.project, plan),
      GITHUB_RUN_ID: mode === "plan" ? F.planRun : F.renewRun,
    }, {
      plan: plan ?? undefined, enabled: o.enabled ?? true, project: F.project, now: o.now,
    });
    return runAdmissionRenewal((o.client ?? client) as never, input);
  };
  const ts = (c: string) => `to_char(${c} AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"')`;
  const state = async () => ({
    policy: (await db.query<{ v: string }>(`SELECT ${ts("valid_until")} v FROM sessions.ingest_lazy_policy`)).rows.map((r) => r.v).join(","),
    policyUpdated: (await db.query<{ v: string }>(`SELECT ${ts("updated_at")} v FROM sessions.ingest_lazy_policy`)).rows.map((r) => r.v).join(","),
    rows: (await db.query<{ org_id: string; v: string }>(
      `SELECT org_id,${ts("valid_until")} v FROM sessions.ingest_buffer_admission ORDER BY org_id COLLATE "C"`)).rows,
  });
  return { db, client, validUntil, op, state, close: () => db.close() };
}

async function main() {
  const f = await renewalFixture();
  const fail = (what: string) => { throw new Error("admission_renewal_ci_" + what); };
  try {
    const read = await f.op("readback");
    if (read.receipt.drift !== null || read.receipt.admissionRows !== F.orgs.length ||
        read.receipt.ledgerTail !== F.ledgerTail) fail("readback");
    const before = await f.state();
    const planned = await f.op("plan");
    if (!planned.plan || JSON.stringify(await f.state()) !== JSON.stringify(before)) fail("plan_wrote");
    if (planned.plan.generation !== F.generation || planned.plan.ledgerTail !== F.ledgerTail ||
        planned.plan.stepCount !== F.stepCount || planned.plan.ledgerPrefixDigest !== F.ledgerPrefixDigest)
      fail("plan_ledger");
    // Drift between plan and renew (an extra org) refuses and leaves both tables unchanged.
    await f.db.query(`INSERT INTO sessions.ingest_buffer_admission SELECT 'org-late',privacy_baseline_sha256,
      retention_contract_sha256,schema_generation,valid_until,control_epoch FROM sessions.ingest_buffer_admission LIMIT 1`);
    const drifted = await f.state();
    let code: unknown = null;
    try { await f.op("renew", { plan: planned.plan }); } catch (e) { code = (e as { code?: unknown }).code; }
    if (code !== "admission_renewal_plan_roster_mismatch" || JSON.stringify(await f.state()) !== JSON.stringify(drifted))
      fail("drift_not_refused");
    await f.db.query("DELETE FROM sessions.ingest_buffer_admission WHERE org_id='org-late'");
    const renewed = await f.op("renew", { plan: planned.plan });
    const after = await f.state();
    const want = planned.plan.renewTo.replace("Z", "000Z");
    if (!renewed.receipt.committed || after.policy !== want || after.rows.length !== F.orgs.length ||
        after.rows.some((r) => r.v !== want))
      fail("renew");
    for (const org of F.orgs) await f.db.query("SELECT sessions.ingest_lazy_guard($1)", [org]);
    console.log(JSON.stringify({
      status: "ADMISSION_RENEWAL_CI_PASS", rows: after.rows.length, from: planned.plan.oldValidUntil,
      to: planned.plan.renewTo, refusedDrift: code, ledgerTail: planned.plan.ledgerTail,
      stepCount: planned.plan.stepCount, held: F.held.length, generationPrefix: planned.plan.generation.slice(0, 8),
    }));
  } finally {
    await f.close();
  }
}

if (import.meta.main)
  // Scrub engine (docs/scrub-engine.md, "Default flip"): the pinned wasm-bindgen engine, read from the repo checkout.
  initScrubEngine().then(() => main()).catch((error) => {
    console.error((error as Error).message);
    process.exitCode = 1;
  });
