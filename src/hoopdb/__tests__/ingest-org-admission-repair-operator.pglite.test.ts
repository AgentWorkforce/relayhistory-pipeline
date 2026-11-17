/**
 * The dormant org admission repair operator (src/hoopdb/ingest-org-admission-repair-operator.ts) on PGlite over
 * EVERY migration (fixture: scripts/ingest-org-admission-repair-ci.ts, on the admission renewal fixture). One named
 * test per case of the repair spec; the GitHub gate and workflow-shape cases are in
 * ingest-org-admission-repair-gate.test.ts. Every refusal asserts its exact code AND that the policy and every
 * admission row are unchanged (transaction rollback is the only inverse).
 */
import { afterEach, expect, setDefaultTimeout, test } from "bun:test";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import {
  ORG_REPAIR_FIXTURE as F,
  orgRepairFixture,
  UNBOUND,
  type OrgRepairFixture,
} from "../../../scripts/ingest-org-admission-repair-ci.ts";
import {
  LAZY_BEGIN_SCAN_LOOP,
  orgAdmissionRepairConfirmation,
  orgAdmissionRepairDispatch,
  orgAdmissionRepairInputs,
  parseOrgAdmissionRepairOrgs,
  parseOrgAdmissionRepairPlan,
  type OrgAdmissionRepairPlan,
} from "../ingest-org-admission-repair-operator.ts";
import { ORG_ADMISSION_REPAIR_ALLOWLIST, ORG_ADMISSION_REPAIR_ENABLED } from "../org-admission-repair-contract.ts";
setDefaultTimeout(120_000);

const open: OrgRepairFixture[] = [];
afterEach(async () => { for (const f of open.splice(0)) await f.close(); });
const fixture = async () => { const f = await orgRepairFixture(); open.push(f); return f; };
const code = async (p: Promise<unknown> | (() => unknown)) => {
  try { await (typeof p === "function" ? p() : p); return null; } catch (e) { return (e as { code?: string }).code ?? String(e); }
};
const sha256 = (t: string) => createHash("sha256").update(t).digest("hex");
const cOrder = (xs: readonly string[]) => [...xs].sort((a, b) => Buffer.compare(Buffer.from(a), Buffer.from(b)));
type Hook = (text: string, params: unknown[] | undefined, next: () => Promise<{ rows: any[] }>) => Promise<{ rows: any[] }>;
const hooked = (f: OrgRepairFixture, hook: Hook) =>
  ({ query: (text: string, params?: unknown[]) => hook(text, params, () => f.client.query(text, params)) });
const INSERT = "INSERT INTO sessions.ingest_buffer_admission";
/** `run` refuses with `expected` (and `detail`, when given) and the policy and every admission row are unchanged. */
async function refusesUnchanged(f: OrgRepairFixture, run: () => Promise<unknown>, expected: string, detail?: string) {
  const before = await f.state();
  let error: { code?: string; detail?: string | null } | null = null;
  try { await run(); } catch (e) { error = e as typeof error; }
  expect(error?.code).toBe(expected);
  if (detail !== undefined) expect(error?.detail).toBe(detail);
  expect(await f.state()).toEqual(before);
}
const policySet = (f: OrgRepairFixture, validUntil: string) =>
  f.db.query("SELECT sessions.ingest_lazy_policy_set($1::jsonb)", [JSON.stringify({
    baselineMode: "lazy-current", generation: F.generation, sourceIdentitySha256: F.source, controlEpoch: F.epoch,
    privacyBaselineSha256: F.privacy, retentionContractSha256: F.retention, sourceProofSha256: F.proof, validUntil,
    issuer: "relayhistory-cloud",
  })]);

test("the operator's scan is 041000's lazy_begin loop verbatim, and the allowlist is exactly the seed epoch counter", () => {
  const mig = readFileSync(join(import.meta.dir, "../../../supabase/migrations/20261105041000_ingest_privacy_authority.sql"), "utf8");
  const body = mig.slice(mig.indexOf("CREATE OR REPLACE FUNCTION sessions.ingest_lazy_begin("), mig.indexOf("END $lazy_begin$;"));
  expect(body).toContain(LAZY_BEGIN_SCAN_LOOP);
  expect(body.split("FOR k IN ").length).toBe(2);
  expect([...ORG_ADMISSION_REPAIR_ALLOWLIST]).toEqual(["ingest_seed_source_epochs"]);
});

test("dormant: plan and admit refuse while ORG_ADMISSION_REPAIR_ENABLED is false; readback is allowed", async () => {
  expect(ORG_ADMISSION_REPAIR_ENABLED).toBe(false);
  const env = (mode: string, extra: Record<string, string> = {}) =>
    ({ ORG_REPAIR_MODE: mode, ORG_REPAIR_SHA: F.sha, ORG_REPAIR_ORGS: "org-a", ...extra });
  expect(await code(() => orgAdmissionRepairDispatch(env("plan")))).toBe("org_admission_repair_dormant");
  expect(await code(() => orgAdmissionRepairDispatch(env("admit", { ORG_REPAIR_PLAN_RUN: F.planRun, ORG_REPAIR_CONFIRMATION: "ADMIT-ORGS x" }))))
    .toBe("org_admission_repair_dormant");
  expect(orgAdmissionRepairDispatch(env("readback")).mode).toBe("readback");
  const f = await fixture();
  expect(await code(() => f.op("plan", { enabled: false }))).toBe("org_admission_repair_dormant");
  expect((await f.op("readback", { enabled: false })).receipt).toMatchObject({ status: "ORG_ADMISSION_REPAIR_READBACK", drift: null, committed: false });
});

test("inputs: the org list is validated, deduplicated and C ordered (1..50); mode, sha, plan run and confirmation refuse before any database work", async () => {
  expect(parseOrgAdmissionRepairOrgs("org-b, Org-A,org-b,org_9,org10")).toEqual(["Org-A", "org-b", "org10", "org_9"]);
  expect(parseOrgAdmissionRepairOrgs(Array.from({ length: 50 }, (_, i) => "o" + i).join(",")).length).toBe(50);
  for (const bad of ["", ",", "org-a,", "org a", "org-a;org-b", "-org", "orgé", "org\nb", "org\u0000", "a".repeat(129),
    Array.from({ length: 51 }, (_, i) => "o" + i).join(","), undefined])
    expect(await code(() => parseOrgAdmissionRepairOrgs(bad))).toBe("org_admission_repair_orgs");
  const d = (e: Record<string, string>) => code(() => orgAdmissionRepairDispatch({ ORG_REPAIR_SHA: F.sha, ORG_REPAIR_ORGS: "org-a", ...e }, true));
  expect(await d({ ORG_REPAIR_MODE: "undo" })).toBe("org_admission_repair_inputs");
  expect(await d({ ORG_REPAIR_MODE: "readback", ORG_REPAIR_SHA: "A".repeat(40) })).toBe("org_admission_repair_inputs");
  expect(await d({ ORG_REPAIR_MODE: "readback", ORG_REPAIR_CONFIRMATION: "yes" })).toBe("org_admission_repair_confirmation");
  expect(await d({ ORG_REPAIR_MODE: "plan", ORG_REPAIR_PLAN_RUN: "1" })).toBe("org_admission_repair_plan_run");
  expect(await d({ ORG_REPAIR_MODE: "admit", ORG_REPAIR_CONFIRMATION: "ADMIT-ORGS x" })).toBe("org_admission_repair_plan_run");
  expect(await d({ ORG_REPAIR_MODE: "admit", ORG_REPAIR_PLAN_RUN: "7", ORG_REPAIR_CONFIRMATION: "" })).toBe("org_admission_repair_confirmation");
  expect(await d({ ORG_REPAIR_MODE: "plan", ORG_REPAIR_ORGS: "" })).toBe("org_admission_repair_orgs");
});

test("readback writes nothing and reports, per org index, admission and which scanned tables hold rows (never counts); drift is reported, not refused", async () => {
  const f = await fixture();
  const before = await f.state();
  const r = (await f.op("readback", { orgs: [...F.targets, F.admitted, F.foreign].join(",") })).receipt;
  expect(await f.state()).toEqual(before);
  const sorted = cOrder([...F.targets, F.admitted, F.foreign]);
  expect(r.orgCount).toBe(4);
  expect(r.orgsSha256).toBe(sha256(sorted.join("\n")));
  const at = (org: string) => r.orgs[sorted.indexOf(org)]!;
  for (const org of F.targets) {
    expect(at(org)).toMatchObject({ admitted: false, tablesWithRows: ["ingest_seed_source_epochs"] });
    expect(at(org)).not.toHaveProperty("counts");
  }
  expect(at(F.admitted)).toMatchObject({ admitted: true, tablesWithRows: ["ingest_seed_source_epochs"] });
  expect(at(F.foreign).tablesWithRows).toEqual(["ingest_seed_fence_baselines", "ingest_seed_source_epochs"]);
  expect(r).toMatchObject({ drift: "org_admission_repair_already_admitted", driftDetail: `org#${sorted.indexOf(F.admitted)}`,
    policyValidUntil: f.validUntil, generationPrefix: F.generation.slice(0, 8), ledgerTail: F.ledgerTail, committed: false });
  expect(r.verdicts).toMatchObject({ scanMatchesLive: true, markerPass: true, generationMatchesMarker: true, ledgerProven: true,
    unexpired: true, noneAdmitted: false, onlyAllowlistedRows: false, everyOrgHasRows: true });
  expect(r.scanTableCount).toBeGreaterThan(10);
  const text = JSON.stringify(r);
  for (const secret of [...sorted, F.source, F.epoch, F.privacy, F.retention, F.proof, F.generation]) expect(text).not.toContain(secret);
});

test("plan writes nothing and binds the sorted roster, per-org scans, the policy descriptor, the generation, the expiry and the ledger proof", async () => {
  const f = await fixture();
  const before = await f.state();
  const { plan, receipt } = await f.op("plan", { orgs: `${F.targets[0]},${F.targets[1]},${F.targets[0]}` });
  expect(await f.state()).toEqual(before);
  const p = plan!;
  expect(Object.keys(p).sort()).toEqual(["capturedAt", "generation", "ledgerPrefixDigest", "ledgerTail", "orgCount", "orgScanSha256",
    "orgsSha256", "planRun", "policyDescriptorSha256", "scanTablesSha256", "sha", "stepCount", "validUntil"]);
  expect(p).toMatchObject({ orgCount: 2, orgsSha256: sha256(cOrder(F.targets).join("\n")), generation: F.generation,
    validUntil: f.validUntil, ledgerTail: F.ledgerTail, stepCount: F.stepCount, ledgerPrefixDigest: F.ledgerPrefixDigest,
    sha: F.sha, planRun: F.planRun });
  expect(p.orgScanSha256).toHaveLength(2);
  expect(Math.abs(Date.parse(p.capturedAt) - Date.now())).toBeLessThan(60_000);
  expect(receipt).toMatchObject({ status: "ORG_ADMISSION_REPAIR_PLANNED", drift: null, committed: false });
  expect(parseOrgAdmissionRepairPlan(JSON.parse(JSON.stringify(p)))).toEqual(p);
  for (const bad of [{ ...p, extra: 1 }, { ...p, orgScanSha256: p.orgScanSha256.slice(1) }, { ...p, validUntil: p.capturedAt },
    { ...p, validUntil: p.validUntil.replace(".000Z", "Z") }, { ...p, orgCount: "2" }])
    expect(await code(() => parseOrgAdmissionRepairPlan(bad))).toBe("org_admission_repair_plan");
});

test("admit: an org whose only prior rows are seed epochs is admitted at the live policy, and its refused claim then applies", async () => {
  const f = await fixture();
  for (const org of F.targets) expect(await f.claim(org, "first")).toBe(UNBOUND);
  const before = await f.state();
  const { plan } = await f.op("plan");
  const seen: string[] = [];
  const { receipt } = await f.op("admit", { plan, client: hooked(f, (t, _p, next) => { seen.push(t.replace(/\s+/gu, " ").trim()); return next(); }) });
  const after = await f.state();
  expect(after.policy).toBe(before.policy);
  expect(after.rows.filter((r) => r.org_id === F.admitted)).toEqual(before.rows.filter((r) => r.org_id === F.admitted));
  const added = after.rows.filter((r) => (F.targets as readonly string[]).includes(r.org_id));
  expect(added.map((r) => r.org_id)).toEqual(cOrder(F.targets));
  for (const r of added)
    expect(r).toEqual({ org_id: r.org_id, privacy_baseline_sha256: F.privacy, retention_contract_sha256: F.retention,
      schema_generation: F.generation, control_epoch: F.epoch, valid_until: f.validUntil.replace("Z", "000Z") });
  expect(after.rows.length).toBe(before.rows.length + 2);
  expect(receipt).toMatchObject({ status: "ORG_ADMISSION_REPAIRED", committed: true, admissionRowsBefore: 1, admissionRowsAfter: 3,
    drift: null, orgCount: 2, policyValidUntil: f.validUntil });
  expect(receipt.orgs.every((o) => o.admitted)).toBe(true);
  expect(receipt.approvalSha256).toBe(sha256(orgAdmissionRepairConfirmation("admit", F.project, plan!)));
  const text = JSON.stringify(receipt);
  for (const secret of [...F.targets, F.source, F.epoch, F.privacy, F.retention, F.proof, F.generation]) expect(text).not.toContain(secret);
  // The SAME pending control now passes ingest_lazy_begin; the foreign org still refuses.
  for (const org of F.targets) expect(await f.claim(org, "first")).toBe("applied");
  expect(await f.claim(F.foreign, "first")).toBe(UNBOUND);
  // One transaction, the renewal's lock order, plain INSERTs, the guard per org before COMMIT.
  const at = (s: string) => { const i = seen.findIndex((t) => t.includes(s)); expect(i).toBeGreaterThanOrEqual(0); return i; };
  expect(seen[0]).toBe("BEGIN");
  expect(seen.at(-1)).toBe("COMMIT");
  expect(seen.filter((t) => t === "BEGIN" || t === "COMMIT").length).toBe(2);
  expect(at("pg_advisory_xact_lock(961996, 1)")).toBeLessThan(at("FROM sessions.__96bulls_migrations"));
  expect(at("FROM sessions.__96bulls_migrations")).toBeLessThan(at("WHERE name=$1 FOR SHARE"));
  expect(at("WHERE name=$1 FOR SHARE")).toBeLessThan(at("FROM sessions.ingest_lazy_policy FOR SHARE"));
  expect(at("FROM sessions.ingest_lazy_policy FOR SHARE")).toBeLessThan(at("IN SHARE ROW EXCLUSIVE MODE"));
  expect(at("IN SHARE ROW EXCLUSIVE MODE")).toBeLessThan(at(INSERT));
  expect(at(INSERT)).toBeLessThan(at("ingest_lazy_guard"));
  expect(seen.filter((t) => t.startsWith(INSERT)).length).toBe(2);
  expect(seen.some((t) => t.startsWith(INSERT) && t.includes("ON CONFLICT"))).toBe(false);
  expect(seen.filter((t) => t.includes("ingest_lazy_guard")).length).toBe(2);
  expect(seen.some((t) => /UPDATE sessions\.ingest_lazy_policy\b|ingest_lazy_policy_set/u.test(t))).toBe(false);
  expect(seen).toContain("SET LOCAL lock_timeout = '5s'");
  // Replaying the same plan after commit refuses: the orgs are admitted now.
  await refusesUnchanged(f, () => f.op("admit", { plan }), "org_admission_repair_already_admitted", "org#0");
});

test("already admitted: plan refuses, and an admission that lands between plan and admit refuses admit", async () => {
  const f = await fixture();
  const sorted = cOrder([...F.targets, F.admitted]);
  await refusesUnchanged(f, () => f.op("plan", { orgs: sorted.join(",") }), "org_admission_repair_already_admitted",
    `org#${sorted.indexOf(F.admitted)}`);
  const { plan } = await f.op("plan");
  // A claim-path admission of one target (as lazy_begin would write it) after the plan.
  await f.db.query(`INSERT INTO sessions.ingest_buffer_admission SELECT $1,privacy_baseline_sha256,retention_contract_sha256,
    schema_generation,valid_until,control_epoch FROM sessions.ingest_buffer_admission WHERE org_id=$2`, [F.targets[0], F.admitted]);
  await refusesUnchanged(f, () => f.op("admit", { plan }), "org_admission_repair_already_admitted",
    `org#${cOrder(F.targets).indexOf(F.targets[0])}`);
});

test("foreign authority: rows in any other ingest_/upload_ table refuse plan and admit, and are never adopted", async () => {
  const f = await fixture();
  const sorted = cOrder([...F.targets, F.foreign]);
  await refusesUnchanged(f, () => f.op("plan", { orgs: sorted.join(",") }), "org_admission_repair_unbound_authority",
    `ingest_seed_fence_baselines org#${sorted.indexOf(F.foreign)}`);
  // An upload_ table created after the migrations is found through pg_catalog exactly as lazy_begin finds it.
  const { plan } = await f.op("plan");
  await f.db.exec("CREATE TABLE sessions.upload_fixture_authority(org_id text NOT NULL)");
  const t = cOrder(F.targets);
  await f.db.query("INSERT INTO sessions.upload_fixture_authority VALUES($1)", [t[1]]);
  await refusesUnchanged(f, () => f.op("admit", { plan }), "org_admission_repair_unbound_authority", "upload_fixture_authority org#1");
  // The claim path agrees: the org with the upload_ row is refused by lazy_begin too.
  expect(await f.claim(t[1]!, "x")).toBe(UNBOUND);
  // Removing the row restores the plan's preconditions except the scanned table set (a new table): still refused.
  await f.db.query("DELETE FROM sessions.upload_fixture_authority");
  await refusesUnchanged(f, () => f.op("admit", { plan }), "org_admission_repair_plan_scan_tables_mismatch");
  // An org with no scanned rows at all (a typo) is never admitted.
  await refusesUnchanged(f, () => f.op("plan", { orgs: "org-typo" }), "org_admission_repair_nothing_to_repair", "org#0");
});

test("confirmation: any byte change, a different org list or a forged plan refuses admit", async () => {
  const f = await fixture();
  const p = (await f.op("plan")).plan!;
  const phrase = orgAdmissionRepairConfirmation("admit", F.project, p);
  expect(phrase).toBe(`ADMIT-ORGS ${F.project} ${F.sha} GENERATION ${F.generation} VALID_UNTIL ${p.validUntil}` +
    ` ORGS ${p.orgsSha256} COUNT 2 PLAN ${F.planRun}`);
  for (const forged of [phrase + " ", " " + phrase, phrase.toLowerCase(), phrase.replace(F.project, "otherprojectref00000"),
    phrase.replace(" COUNT 2 ", " COUNT 02 "), phrase.replace(p.validUntil, p.validUntil.replace(".000Z", "Z")),
    phrase.replace(` PLAN ${F.planRun}`, " PLAN 1"), phrase.replace(` GENERATION ${F.generation}`, ""),
    phrase.replace(p.orgsSha256, "0".repeat(64))])
    await refusesUnchanged(f, () => f.op("admit", { plan: p, confirmation: forged }), "org_admission_repair_confirmation");
  // The plan was for the targets; admit with a different list (even a subset) refuses before the database.
  await refusesUnchanged(f, () => f.op("admit", { plan: p, orgs: F.targets[0] }), "org_admission_repair_plan_orgs");
  await refusesUnchanged(f, () => f.op("admit", { plan: p, orgs: [...F.targets, F.foreign].join(",") }), "org_admission_repair_plan_orgs");
  // A self-consistent forged plan (phrase rebuilt from it) is refused by the database re-check.
  const forge = (o: Partial<OrgAdmissionRepairPlan>) => ({ ...p, ...o });
  await refusesUnchanged(f, () => f.op("admit", { plan: forge({ policyDescriptorSha256: "0".repeat(64) }) }), "org_admission_repair_plan_policy_mismatch");
  await refusesUnchanged(f, () => f.op("admit", { plan: forge({ orgScanSha256: [p.orgScanSha256[0]!, "0".repeat(64)] }) }),
    "org_admission_repair_plan_scan_mismatch", "org#1");
  await refusesUnchanged(f, () => f.op("admit", { plan: forge({ ledgerPrefixDigest: "0".repeat(64) }) }), "org_admission_repair_ledger_drift");
  const base = { ORG_REPAIR_MODE: "admit", ORG_REPAIR_SHA: F.sha, ORG_REPAIR_ORGS: F.targets.join(","), ORG_REPAIR_PLAN_RUN: F.planRun, ORG_REPAIR_CONFIRMATION: phrase };
  const inputs = (env: Record<string, string>, plan: unknown = p, now?: number) =>
    code(() => orgAdmissionRepairInputs(env, { plan, enabled: true, project: F.project, now }));
  expect(await inputs({ ...base, ORG_REPAIR_PLAN_RUN: "424243" })).toBe("org_admission_repair_plan_run");
  expect(await inputs({ ...base, ORG_REPAIR_SHA: "2".repeat(40) })).toBe("org_admission_repair_plan_sha");
  expect(await inputs(base, null)).toBe("org_admission_repair_plan");
  expect(await inputs(base, p, Date.parse(p.capturedAt) + 24 * 3600_000)).toBe("org_admission_repair_plan_stale");
});

test("policy or generation drift between plan and admit refuses with its own code and writes nothing", async () => {
  const f = await fixture();
  const { plan } = await f.op("plan");
  // A renewal (new expiry) between plan and admit.
  await policySet(f, new Date(Date.parse(f.validUntil) + 3600_000).toISOString());
  await refusesUnchanged(f, () => f.op("admit", { plan }), "org_admission_repair_plan_expiry_mismatch");
  // The same expiry re-set (updated_at moves): the descriptor no longer matches.
  await policySet(f, f.validUntil);
  await refusesUnchanged(f, () => f.op("admit", { plan }), "org_admission_repair_plan_policy_mismatch");
  // Marker and policy moved to another generation together.
  const g = await fixture();
  const p2 = (await g.op("plan")).plan!;
  await g.db.query("UPDATE sessions.__96bulls_role_validation SET generation=$1", ["e".repeat(64)]);
  await refusesUnchanged(g, () => g.op("admit", { plan: p2 }), "org_admission_repair_generation_drift");
  await g.db.query("UPDATE sessions.ingest_lazy_policy SET generation=$1", ["e".repeat(64)]);
  await refusesUnchanged(g, () => g.op("admit", { plan: p2 }), "org_admission_repair_generation_stale");
  await g.db.query("UPDATE sessions.__96bulls_role_validation SET status='fail'");
  await refusesUnchanged(g, () => g.op("plan"), "org_admission_repair_marker_not_pass");
});

test("expired policy and a changed live scan refuse plan", async () => {
  const f = await fixture();
  await f.db.query("UPDATE sessions.ingest_lazy_policy SET valid_until=date_trunc('milliseconds',clock_timestamp())-interval '1 second'");
  await refusesUnchanged(f, () => f.op("plan"), "org_admission_repair_policy_expired");
  const g = await fixture();
  const { plan } = await g.op("plan");
  // A forward migration that changes lazy_begin's scan (e.g. the class fix) retires the operator.
  await g.db.exec(`CREATE OR REPLACE FUNCTION sessions.ingest_lazy_begin(p jsonb,p_kind text) RETURNS jsonb
    LANGUAGE plpgsql AS $x$ BEGIN RETURN '{}'::jsonb; END $x$`);
  expect((await g.op("readback")).receipt).toMatchObject({ drift: "org_admission_repair_scan_drift", verdicts: { scanMatchesLive: false } });
  await refusesUnchanged(g, () => g.op("plan"), "org_admission_repair_scan_drift");
  await refusesUnchanged(g, () => g.op("admit", { plan }), "org_admission_repair_scan_drift");
});

test("rollback: a failure after the first INSERT, in the postcondition, in the guard or at COMMIT leaves no partial rows", async () => {
  const f = await fixture();
  const { plan } = await f.op("plan");
  let inserts = 0;
  const failSecondInsert = hooked(f, (t, p, next) => {
    if (t.trimStart().startsWith(INSERT) && ++inserts === 2) return Promise.reject(new Error("injected"));
    return next();
  });
  await refusesUnchanged(f, () => f.op("admit", { plan, client: failSecondInsert }), "org_admission_repair_insert");
  expect(inserts).toBe(2);
  // The first INSERT really happened inside the transaction before the rollback.
  let total = 0;
  const lying = hooked(f, async (t, _p, next) => {
    const r = await next();
    if (t.includes("SELECT count(*)::text AS n FROM sessions.ingest_buffer_admission") && ++total === 2) return { rows: [{ n: "999" }] };
    return r;
  });
  await refusesUnchanged(f, () => f.op("admit", { plan, client: lying }), "org_admission_repair_postcondition");
  const guard = hooked(f, (t, _p, next) => (t.includes("ingest_lazy_guard") ? Promise.reject(new Error("closed")) : next()));
  await refusesUnchanged(f, () => f.op("admit", { plan, client: guard }), "org_admission_repair_postcondition_guard");
  // A COMMIT that fails is reported as unknown; the transaction is rolled back here.
  const commit = hooked(f, (t, _p, next) => (t === "COMMIT" ? Promise.reject(new Error("lost")) : next()));
  await refusesUnchanged(f, () => f.op("admit", { plan, client: commit }), "org_admission_repair_commit_unknown");
  // The same plan still admits once nothing interferes.
  expect((await f.op("admit", { plan })).receipt.committed).toBe(true);
});
