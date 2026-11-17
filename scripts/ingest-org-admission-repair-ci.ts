/**
 * CI proof of the dormant org admission repair operator (src/hoopdb/ingest-org-admission-repair-operator.ts) on
 * PGlite over EVERY repository migration, on the admission renewal fixture (scripts/ingest-admission-renewal-ci.ts:
 * synthetic custody policy, seat clock, PASS marker at an opaque historical generation, production-shaped migration
 * ledger) plus a frozen-source seal so the real claim path runs. Orgs: two repair targets whose only prior rows are
 * in sessions.ingest_seed_source_epochs, one already admitted org, one org with a real target row
 * (sessions.ingest_seed_fence_baselines). Proves: the first claim of a target refuses 55000 "unbound prior org
 * authority"; readback and plan write nothing; plan refuses the admitted and the foreign org; admit inserts exactly
 * the targets at the policy's identity and expiry; the SAME claim then applies. Synthetic identifiers and a fixture
 * project ref only. Exports the fixture the pglite test reuses. Run: bun scripts/ingest-org-admission-repair-ci.ts
 */
import { renewalFixture, RENEWAL_FIXTURE as R, type RenewalFixture } from "./ingest-admission-renewal-ci.ts";
import {
  orgAdmissionRepairConfirmation,
  orgAdmissionRepairInputs,
  runOrgAdmissionRepair,
  type OrgAdmissionRepairMode,
  type OrgAdmissionRepairPlan,
} from "../src/hoopdb/ingest-org-admission-repair-operator.ts";

export const ORG_REPAIR_FIXTURE = {
  ...R,
  admitRun: "444444",
  /** Mixed case: C (byte) order differs from a locale collation. */
  targets: ["org-seed-b", "Org-Seed-A"],
  admitted: "org-admitted",
  foreign: "org-foreign",
  workspace: "ws-a",
  user: "user-1",
} as const;
const F = ORG_REPAIR_FIXTURE;

export interface OrgRepairFixture {
  db: RenewalFixture["db"];
  client: RenewalFixture["client"];
  validUntil: string;
  /** Run one mode through orgAdmissionRepairInputs (the confirmation is built from the plan unless overridden). */
  op: (mode: OrgAdmissionRepairMode, options?: {
    orgs?: string; plan?: OrgAdmissionRepairPlan | null; confirmation?: string; enabled?: boolean; now?: number;
    client?: RenewalFixture["client"];
  }) => ReturnType<typeof runOrgAdmissionRepair>;
  /** Every admission row (C order) with every column, and the policy's valid_until and updated_at (microseconds). */
  state: () => Promise<{ policy: string; rows: Record<string, string>[] }>;
  /**
   * One reset control for (org, workspace, user) through the ingest role: control begin, frozen witness, then
   * ingest_reset_uploaded_sessions, whose first call for an org runs ingest_lazy_begin's org admission. Returns
   * the outcome, or `<sqlstate>:<message>`. A refused control stays pending and is retried with the same id.
   */
  claim: (org: string, requestId: string) => Promise<string>;
  close: () => Promise<void>;
}

/** The renewal fixture with `admitted` admitted, seed-epoch rows for every fixture org and a target row for `foreign`. */
export async function orgRepairFixture(): Promise<OrgRepairFixture> {
  const r = await renewalFixture({ orgs: [F.admitted] });
  const db = r.db;
  await db.query(`INSERT INTO sessions.ingest_frozen_source_completeness(source_identity_sha256,control_epoch,
    privacy_baseline_sha256,org_count,record_rows,manifest_sha256,neon_snapshot) VALUES($1,$2,$3,0,0,$4,'{}')`,
  [F.source, F.epoch, F.privacy, "e".repeat(64)]);
  for (const org of [...F.targets, F.admitted, F.foreign])
    for (const session of ["s1", "s2"])
      await db.query("INSERT INTO sessions.ingest_seed_source_epochs VALUES($1,$2,'claude',$3,1)", [org, F.workspace, session]);
  await db.query("INSERT INTO sessions.ingest_seed_fence_baselines(org_id,workspace_id,user_id,fingerprint) VALUES($1,$2,$3,$4)",
    [F.foreign, F.workspace, F.user, "c".repeat(64)]);
  const op: OrgRepairFixture["op"] = (mode, o = {}) => {
    const plan = o.plan ?? null;
    const input = orgAdmissionRepairInputs({
      ORG_REPAIR_MODE: mode, ORG_REPAIR_SHA: F.sha, ORG_REPAIR_ORGS: o.orgs ?? F.targets.join(","),
      ORG_REPAIR_PLAN_RUN: mode === "admit" ? (plan?.planRun ?? F.planRun) : "",
      ORG_REPAIR_CONFIRMATION: o.confirmation ?? orgAdmissionRepairConfirmation(mode, F.project, plan),
      GITHUB_RUN_ID: mode === "plan" ? F.planRun : F.admitRun,
    }, { plan: plan ?? undefined, enabled: o.enabled ?? true, project: F.project, now: o.now });
    return runOrgAdmissionRepair((o.client ?? r.client) as never, input);
  };
  const t = (c: string) => `to_char(${c} AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"')`;
  const state = async () => ({
    policy: (await db.query<{ v: string }>(`SELECT ${t("valid_until")}||' '||${t("updated_at")} v FROM sessions.ingest_lazy_policy`))
      .rows.map((x) => x.v).join(","),
    rows: (await db.query<Record<string, string>>(`SELECT org_id,privacy_baseline_sha256,retention_contract_sha256,
      schema_generation,control_epoch,${t("valid_until")} valid_until FROM sessions.ingest_buffer_admission
      ORDER BY org_id COLLATE "C"`)).rows,
  });
  const run = async (sql: string, p: unknown[]) => {
    await db.exec("BEGIN;SET LOCAL ROLE bulls96_session_ingest");
    try {
      const out = await db.query<{ r: any }>(sql, p as never);
      await db.exec("COMMIT");
      return out.rows[0]!.r;
    } catch (e) {
      await db.exec("ROLLBACK");
      throw e;
    }
  };
  const rpc = (fn: string, p: unknown) => run(`SELECT sessions.${fn}($1::jsonb) AS r`, [JSON.stringify(p)]);
  const claim = async (org: string, requestId: string) => {
    const tenant = { orgId: org, workspaceId: F.workspace, userId: F.user };
    try {
      const head = await rpc("ingest_source_control_begin", { epoch: F.epoch, tenant, kind: "reset", payload: { request_id: requestId } });
      const w = await rpc("ingest_frozen_source_witness", { kind: "reset", tenant, originId: null, batchId: null,
        batchDigest: null, originalRecordCount: 0, candidates: [] });
      const out = await rpc("ingest_reset_uploaded_sessions", { tenant, requestId, custody: w, generation: F.generation,
        control: { epoch: F.epoch, version: head.version, digest: head.digest, kind: "reset" } });
      return String(out.control.outcome);
    } catch (e) {
      return `${(e as { code?: string }).code}:${(e as Error).message}`;
    }
  };
  return { db, client: r.client, validUntil: r.validUntil, op, state, claim, close: r.close };
}

export const UNBOUND = "55000:session ingest: unbound prior org authority requires recovery";

async function main() {
  const f = await orgRepairFixture();
  const fail = (what: string) => { throw new Error("org_admission_repair_ci_" + what); };
  const code = async (p: Promise<unknown>) => p.then(() => null, (e) => (e as { code?: string }).code ?? String(e));
  try {
    for (const org of F.targets) if (await f.claim(org, "first") !== UNBOUND) fail("claim_not_refused");
    const before = await f.state();
    const read = await f.op("readback");
    if (read.receipt.drift !== null || read.receipt.orgs.some((o) => o.admitted ||
        JSON.stringify(o.tablesWithRows) !== '["ingest_seed_source_epochs"]' || "counts" in o))
      fail("readback");
    const planned = await f.op("plan");
    if (!planned.plan || JSON.stringify(await f.state()) !== JSON.stringify(before)) fail("plan_wrote");
    const admittedCode = await code(f.op("plan", { orgs: [...F.targets, F.admitted].join(",") }));
    const foreignCode = await code(f.op("plan", { orgs: [...F.targets, F.foreign].join(",") }));
    if (admittedCode !== "org_admission_repair_already_admitted" || foreignCode !== "org_admission_repair_unbound_authority")
      fail("refusals");
    const admitted = await f.op("admit", { plan: planned.plan });
    const after = await f.state();
    if (!admitted.receipt.committed || after.rows.length !== before.rows.length + F.targets.length) fail("admit");
    for (const org of F.targets) if (await f.claim(org, "first") !== "applied") fail("claim_after_admit");
    if (await f.claim(F.foreign, "first") !== UNBOUND) fail("foreign_claim");
    console.log(JSON.stringify({
      status: "ORG_ADMISSION_REPAIR_CI_PASS", admitted: F.targets.length, rows: after.rows.length,
      refused: [admittedCode, foreignCode], validUntil: planned.plan.validUntil, ledgerTail: planned.plan.ledgerTail,
      scanTables: admitted.receipt.scanTableCount, generationPrefix: planned.plan.generation.slice(0, 8),
    }));
  } finally {
    await f.close();
  }
}

if (import.meta.main)
  main().catch((error) => {
    console.error((error as Error).message);
    process.exitCode = 1;
  });
