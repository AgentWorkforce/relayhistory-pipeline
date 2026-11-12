/**
 * The dormant admission renewal operator (src/hoopdb/ingest-admission-renewal-operator.ts) on PGlite over EVERY
 * migration (fixture: scripts/ingest-admission-renewal-ci.ts). One named test per causal case of the renewal spec;
 * the GitHub gate, connection and workflow-shape cases are in ingest-admission-renewal-gate.test.ts. Every refusal
 * asserts its exact code AND that the policy and every admission row are unchanged (transaction rollback is the only
 * inverse).
 */
import { test, expect, afterEach, setDefaultTimeout } from "bun:test";
import { createHash } from "node:crypto";
import { renewalFixture, RENEWAL_FIXTURE as F, type RenewalFixture } from "../../../scripts/ingest-admission-renewal-ci.ts";
import {
  admissionRenewalConfirmation,
  admissionRenewalDispatch,
  admissionRenewalInputs,
  admissionRenewalLedger,
  parseAdmissionRenewalPlan,
  type AdmissionRenewalPlan,
} from "../ingest-admission-renewal-operator.ts";
import { ADMISSION_RENEWAL_ENABLED, ADMISSION_RENEWAL_HORIZON_MS } from "../admission-renewal-contract.ts";
import { generation, RELEASE_TREE } from "../migrate.ts";
import { appendFileSync, cpSync, mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
setDefaultTimeout(120_000);

const open: RenewalFixture[] = [];
afterEach(async () => { for (const f of open.splice(0)) await f.close(); });
const fixture = async (o?: Parameters<typeof renewalFixture>[0]) => { const f = await renewalFixture(o); open.push(f); return f; };
const code = async (p: Promise<unknown> | (() => unknown)) => {
  try { await (typeof p === "function" ? p() : p); return null; } catch (e) { return (e as { code?: string }).code ?? String(e); }
};
const sha256 = (t: string) => createHash("sha256").update(t).digest("hex");
type Hook = (text: string, params: unknown[] | undefined, next: () => Promise<{ rows: any[] }>) => Promise<{ rows: any[] }>;
const hooked = (f: RenewalFixture, hook: Hook) => ({ query: (text: string, params?: unknown[]) => hook(text, params, () => f.client.query(text, params)) });
const ADMISSION_UPDATE = "UPDATE sessions.ingest_buffer_admission SET valid_until=$1";
/** Marker, policy and every admission row at generation `g` (consistent with each other). */
const atGeneration = async (f: RenewalFixture, g: string) => {
  await f.db.query("UPDATE sessions.__96bulls_role_validation SET generation=$1", [g]);
  await f.db.query("UPDATE sessions.ingest_lazy_policy SET generation=$1", [g]);
  await f.db.query("UPDATE sessions.ingest_buffer_admission SET schema_generation=$1", [g]);
};
const LEDGER = "sessions.__96bulls_migrations";
/** Renew refuses with `expected` and both tables are byte-identical (microseconds) afterwards. */
async function refusesUnchanged(f: RenewalFixture, run: () => Promise<unknown>, expected: string) {
  const before = await f.state();
  expect(await code(run)).toBe(expected);
  expect(await f.state()).toEqual(before);
}

test("dormant: plan and renew refuse while ADMISSION_RENEWAL_ENABLED is false; readback is allowed", async () => {
  expect(ADMISSION_RENEWAL_ENABLED).toBe(false);
  const env = (mode: string, extra: Record<string, string> = {}) => ({ RENEWAL_MODE: mode, RENEWAL_SHA: F.sha, ...extra });
  expect(await code(() => admissionRenewalDispatch(env("plan")))).toBe("admission_renewal_dormant");
  expect(await code(() => admissionRenewalDispatch(env("renew", { RENEWAL_PLAN_RUN: F.planRun, RENEWAL_CONFIRMATION: "RENEW-ADMISSION x" }))))
    .toBe("admission_renewal_dormant");
  expect(admissionRenewalDispatch(env("readback")).mode).toBe("readback");
  const f = await fixture();
  expect(await code(() => f.op("plan", { enabled: false }))).toBe("admission_renewal_dormant");
  expect((await f.op("readback", { enabled: false })).receipt).toMatchObject({ status: "ADMISSION_RENEWAL_READBACK", drift: null, committed: false });
}, 120_000);

test("inputs: mode, sha, plan run and confirmation shape refuse before any database work", () => {
  const d = (e: Record<string, string>) => code(() => admissionRenewalDispatch({ RENEWAL_SHA: F.sha, ...e }, true));
  return Promise.all([
    d({ RENEWAL_MODE: "undo" }).then((c) => expect(c).toBe("admission_renewal_inputs")),
    d({ RENEWAL_MODE: "readback", RENEWAL_SHA: "1".repeat(39) }).then((c) => expect(c).toBe("admission_renewal_inputs")),
    d({ RENEWAL_MODE: "readback", RENEWAL_SHA: "A".repeat(40) }).then((c) => expect(c).toBe("admission_renewal_inputs")),
    d({ RENEWAL_MODE: "readback", RENEWAL_CONFIRMATION: "yes" }).then((c) => expect(c).toBe("admission_renewal_confirmation")),
    d({ RENEWAL_MODE: "plan", RENEWAL_PLAN_RUN: "1" }).then((c) => expect(c).toBe("admission_renewal_plan_run")),
    d({ RENEWAL_MODE: "renew", RENEWAL_CONFIRMATION: "RENEW-ADMISSION x" }).then((c) => expect(c).toBe("admission_renewal_plan_run")),
    d({ RENEWAL_MODE: "renew", RENEWAL_PLAN_RUN: "0", RENEWAL_CONFIRMATION: "RENEW-ADMISSION x" }).then((c) => expect(c).toBe("admission_renewal_plan_run")),
    d({ RENEWAL_MODE: "renew", RENEWAL_PLAN_RUN: "7", RENEWAL_CONFIRMATION: "" }).then((c) => expect(c).toBe("admission_renewal_confirmation")),
  ]);
});

test("readback reports verdicts without writing; with no admission rows it reports admission_renewal_no_rows", async () => {
  const f = await fixture();
  const before = await f.state();
  const r = (await f.op("readback")).receipt;
  expect(r).toMatchObject({ drift: null, admissionRows: F.orgs.length, policyValidUntilBefore: f.validUntil,
    admissionMinValidUntil: f.validUntil, admissionMaxValidUntil: f.validUntil, generationPrefix: F.generation.slice(0, 8), ledgerTail: F.ledgerTail });
  expect(Object.values(r.verdicts).every(Boolean)).toBe(true);
  expect(await f.state()).toEqual(before);
  const empty = await fixture({ orgs: [] });
  expect((await empty.op("readback")).receipt).toMatchObject({ drift: "admission_renewal_no_rows", admissionRows: 0 });
  expect(await code(() => empty.op("plan"))).toBe("admission_renewal_no_rows");
}, 120_000);

test("plan writes nothing and binds capturedAt + exactly 30 days, the count, the C-ordered roster digest and the descriptor", async () => {
  const f = await fixture();
  const before = await f.state();
  const { plan, receipt } = await f.op("plan");
  expect(await f.state()).toEqual(before);
  expect(plan).not.toBeNull();
  const p = plan!;
  expect(Object.keys(p).sort()).toEqual(["admissionCount", "capturedAt", "descriptorSha256", "generation",
    "ledgerPrefixDigest", "ledgerTail", "oldValidUntil", "orgsSha256", "planRun", "renewTo", "sha", "stepCount"]);
  expect(Date.parse(p.renewTo) - Date.parse(p.capturedAt)).toBe(ADMISSION_RENEWAL_HORIZON_MS);
  expect(new Date(Date.parse(p.capturedAt)).toISOString()).toBe(p.capturedAt);
  expect(Math.abs(Date.parse(p.capturedAt) - Date.now())).toBeLessThan(60_000);
  expect(p).toMatchObject({ oldValidUntil: f.validUntil, admissionCount: F.orgs.length, generation: F.generation, ledgerTail: F.ledgerTail,
    stepCount: F.stepCount, ledgerPrefixDigest: F.ledgerPrefixDigest, sha: F.sha, planRun: F.planRun });
  const cOrder = [...F.orgs].sort((a, b) => Buffer.compare(Buffer.from(a), Buffer.from(b)));
  expect(p.orgsSha256).toBe(sha256(cOrder.join("\n")));
  expect(p.descriptorSha256).toMatch(/^[0-9a-f]{64}$/);
  expect(receipt).toMatchObject({ status: "ADMISSION_RENEWAL_PLANNED", renewTo: p.renewTo, committed: false, ledgerTail: F.ledgerTail });
  expect(parseAdmissionRenewalPlan(JSON.parse(JSON.stringify(p)))).toEqual(p);
}, 120_000);

test("renew moves the policy and every admission row together through ingest_lazy_policy_set; the guard admits every org; the receipt is sanitized", async () => {
  const f = await fixture();
  const { plan } = await f.op("plan");
  const seen: string[] = [];
  const { receipt } = await f.op("renew", { plan, client: hooked(f, (t, _p, next) => { seen.push(t.replace(/\s+/gu, " ").trim()); return next(); }) });
  const after = await f.state();
  const want = plan!.renewTo.replace("Z", "000Z");
  expect(after.policy).toBe(want);
  expect(after.rows.map((r) => r.v)).toEqual(F.orgs.map(() => want));
  expect(receipt).toMatchObject({ status: "ADMISSION_RENEWED", committed: true, policyValidUntilBefore: f.validUntil,
    policyValidUntilAfter: plan!.renewTo, admissionRows: F.orgs.length, admissionMinValidUntil: plan!.renewTo,
    admissionMaxValidUntil: plan!.renewTo, drift: null, orgsSha256: plan!.orgsSha256, descriptorSha256: plan!.descriptorSha256 });
  expect(receipt.approvalSha256).toBe(sha256(admissionRenewalConfirmation("renew", F.project, plan!)));
  for (const org of F.orgs) await f.db.query("SELECT sessions.ingest_lazy_guard($1)", [org]);
  const text = JSON.stringify(receipt);
  for (const secret of [F.source, F.epoch, F.privacy, F.retention, F.proof, F.generation, ...F.orgs]) expect(text).not.toContain(secret);
  // Lock order (the ingest admission path's: marker, policy, admission table, admission rows), the owner-only
  // setter (no raw policy UPDATE), then the CAS update, in one transaction.
  const at = (s: string) => { const i = seen.findIndex((t) => t.includes(s)); expect(i).toBeGreaterThanOrEqual(0); return i; };
  expect(seen[0]).toBe("BEGIN");
  expect(seen.at(-1)).toBe("COMMIT");
  expect(at("pg_advisory_xact_lock(961996, 1)")).toBeLessThan(at("FROM sessions.__96bulls_migrations"));
  expect(at("FROM sessions.__96bulls_migrations")).toBeLessThan(at("WHERE name=$1 FOR SHARE"));
  expect(at("WHERE name=$1 FOR SHARE")).toBeLessThan(at("FROM sessions.ingest_lazy_policy FOR UPDATE"));
  expect(at("FROM sessions.ingest_lazy_policy FOR UPDATE")).toBeLessThan(at("IN SHARE ROW EXCLUSIVE MODE"));
  expect(at("IN SHARE ROW EXCLUSIVE MODE")).toBeLessThan(at('COLLATE "C" FOR UPDATE'));
  expect(at('COLLATE "C" FOR UPDATE')).toBeLessThan(at("ingest_lazy_policy_set"));
  expect(at("ingest_lazy_policy_set")).toBeLessThan(at(ADMISSION_UPDATE));
  expect(seen.some((t) => /UPDATE sessions\.ingest_lazy_policy\b/u.test(t))).toBe(false);
  expect(seen.filter((t) => t === "COMMIT" || t === "BEGIN").length).toBe(2);
  expect(seen).toContain("SET LOCAL lock_timeout = '5s'");
  expect(seen).toContain("SET LOCAL statement_timeout = '30s'");
  // Replaying the same plan after commit refuses: the old expiry is gone.
  await refusesUnchanged(f, () => f.op("renew", { plan }), "admission_renewal_plan_expiry_mismatch");
}, 120_000);

test("forged approval or timestamp normalization refuses", async () => {
  const f = await fixture();
  const p = (await f.op("plan")).plan!;
  const phrase = admissionRenewalConfirmation("renew", F.project, p);
  expect(phrase).toBe(`RENEW-ADMISSION ${F.project} ${F.sha} GENERATION ${F.generation} TAIL ${F.ledgerTail}` +
    ` FROM ${p.oldValidUntil} TO ${p.renewTo}` +
    ` ROWS ${F.orgs.length} ORGS ${p.orgsSha256} DESCRIPTOR ${p.descriptorSha256} PLAN ${F.planRun}`);
  const noMs = (t: string) => t.replace(/\.000Z$/u, "Z").replace(/\.([0-9]{3})Z$/u, ".$1000Z");
  for (const forged of [phrase + " ", " " + phrase, phrase.toLowerCase(), phrase.replace(F.project, "otherprojectref00000"),
    phrase.replace(` PLAN ${F.planRun}`, " PLAN 1"), phrase.replace(p.oldValidUntil, noMs(p.oldValidUntil)),
    phrase.replace(p.renewTo, noMs(p.renewTo)), phrase.replace(p.renewTo, p.renewTo.replace("Z", "+00:00")),
    phrase.replace(` ROWS ${F.orgs.length} `, ` ROWS 0${F.orgs.length} `), phrase.replace(` TAIL ${F.ledgerTail}`, ""),
    phrase.replace(` TAIL ${F.ledgerTail}`, ` TAIL ${F.held[0]}`)])
    await refusesUnchanged(f, () => f.op("renew", { plan: p, confirmation: forged }), "admission_renewal_confirmation");
  // A plan artifact whose timestamps are not the canonical bytes, or whose target is not capturedAt + 30 days.
  for (const bad of [{ ...p, renewTo: noMs(p.renewTo) }, { ...p, oldValidUntil: p.oldValidUntil.replace("Z", "+00:00") },
    { ...p, capturedAt: p.capturedAt.replace("T", " ") }, { ...p, admissionCount: String(p.admissionCount) }, { ...p, extra: 1 }])
    expect(await code(() => parseAdmissionRenewalPlan(bad))).toBe("admission_renewal_plan");
  const shifted = { ...p, renewTo: new Date(Date.parse(p.renewTo) + 1).toISOString() };
  expect(await code(() => parseAdmissionRenewalPlan(shifted))).toBe("admission_renewal_plan_target");
  // A self-consistent forged plan (re-signed phrase) is still refused by the database recomputation.
  const forgedDescriptor = { ...p, descriptorSha256: "0".repeat(64) };
  await refusesUnchanged(f, () => f.op("renew", { plan: forgedDescriptor }), "admission_renewal_plan_descriptor_mismatch");
  const forgedOld = { ...p, oldValidUntil: new Date(Date.parse(p.oldValidUntil) + 1).toISOString() };
  await refusesUnchanged(f, () => f.op("renew", { plan: forgedOld }), "admission_renewal_plan_expiry_mismatch");
}, 120_000);

test("target <= old expiry refuses", async () => {
  const far = new Date(Math.floor(Date.now() / 1000) * 1000 + ADMISSION_RENEWAL_HORIZON_MS + 3600_000).toISOString();
  const f = await fixture({ validUntil: far });
  expect(await code(() => f.op("plan"))).toBe("admission_renewal_not_extension");
  const near = await fixture();
  const p = (await near.op("plan")).plan!;
  const equal = { ...p, oldValidUntil: p.renewTo };
  expect(await code(() => parseAdmissionRenewalPlan(equal))).toBe("admission_renewal_not_extension");
}, 120_000);

test("stale plan, plan generation not the live marker's, ledger proof not the live ledger's, wrong plan run or sha refuse", async () => {
  const f = await fixture();
  const p = (await f.op("plan")).plan!;
  const generation_ = await generation();
  const captured = Date.parse(p.capturedAt);
  expect(await code(() => f.op("renew", { plan: p, now: captured + 24 * 3600_000 }))).toBe("admission_renewal_plan_stale");
  expect(await code(() => f.op("renew", { plan: p, now: captured - 6 * 60_000 }))).toBe("admission_renewal_plan_stale");
  // A self-consistent plan (phrase rebuilt from it) bound to another generation or ledger tail than the live one.
  await refusesUnchanged(f, () => f.op("renew", { plan: { ...p, generation: "e".repeat(64) } }), "admission_renewal_generation_stale");
  await refusesUnchanged(f, () => f.op("renew", { plan: { ...p, generation: generation_ } }), "admission_renewal_generation_stale");
  for (const forged of [{ ...p, ledgerTail: F.held[0]! }, { ...p, stepCount: p.stepCount + 1 }, { ...p, ledgerPrefixDigest: "0".repeat(64) }])
    await refusesUnchanged(f, () => f.op("renew", { plan: forged }), "admission_renewal_ledger_drift");
  const base = { RENEWAL_MODE: "renew", RENEWAL_SHA: F.sha, RENEWAL_PLAN_RUN: F.planRun, RENEWAL_CONFIRMATION: admissionRenewalConfirmation("renew", F.project, p) };
  const inputs = (env: Record<string, string>, plan: unknown = p) =>
    code(() => admissionRenewalInputs(env, { plan, enabled: true, project: F.project }));
  expect(await inputs({ ...base, RENEWAL_PLAN_RUN: "424243" })).toBe("admission_renewal_plan_run");
  expect(await inputs({ ...base, RENEWAL_SHA: "2".repeat(40) })).toBe("admission_renewal_plan_sha");
  expect(await inputs(base, null)).toBe("admission_renewal_plan");
  // The plan's own age is rechecked against the database clock inside the renew transaction.
  const old = new Date(Date.now() - 24 * 3600_000 - 1000);
  const aged = { ...p, capturedAt: old.toISOString(), renewTo: new Date(old.getTime() + ADMISSION_RENEWAL_HORIZON_MS).toISOString() };
  await refusesUnchanged(f, () => f.op("renew", { plan: aged, now: Date.now() - 2 * 3600_000 }), "admission_renewal_plan_stale");
}, 120_000);

test("stale PASS or generation drift refuses: marker not pass, policy or an admission row not at the marker generation, marker moved since plan", async () => {
  const f = await fixture();
  const p = (await f.op("plan")).plan!;
  const marker = (g: string, s: string) => f.db.query("UPDATE sessions.__96bulls_role_validation SET generation=$1,status=$2", [g, s]);
  await marker(F.generation, "pending");
  expect((await f.op("readback")).receipt.drift).toBe("admission_renewal_marker_not_pass");
  await refusesUnchanged(f, () => f.op("renew", { plan: p }), "admission_renewal_marker_not_pass");
  await marker("e".repeat(64), "pass");
  expect((await f.op("readback")).receipt.drift).toBe("admission_renewal_generation_drift");
  await refusesUnchanged(f, () => f.op("renew", { plan: p }), "admission_renewal_generation_drift");
  await marker(F.generation, "pass");
  // One admission row not at the marker generation.
  await f.db.query("UPDATE sessions.ingest_buffer_admission SET schema_generation=$1 WHERE org_id=$2", ["e".repeat(64), F.orgs[1]]);
  expect((await f.op("readback")).receipt.drift).toBe("admission_renewal_identity_drift");
  await refusesUnchanged(f, () => f.op("renew", { plan: p }), "admission_renewal_identity_drift");
  await atGeneration(f, F.generation);
  // Marker, policy and rows moved together to another PASS generation after plan: the old plan is stale; a new
  // plan takes the new marker generation as-is.
  await atGeneration(f, "e".repeat(64));
  expect((await f.op("readback")).receipt.drift).toBeNull();
  await refusesUnchanged(f, () => f.op("renew", { plan: p }), "admission_renewal_generation_stale");
  // (The policy generation is forward-only: the overlap trigger retires the old one, so this block comes last.)
  const moved = (await f.op("plan")).plan!;
  expect(moved.generation).toBe("e".repeat(64));
  expect((await f.op("renew", { plan: moved })).receipt.committed).toBe(true);
  await f.db.query("DELETE FROM sessions.__96bulls_role_validation");
  expect((await f.op("readback")).receipt.drift).toBe("admission_renewal_marker_not_pass");
}, 120_000);

test("missing, extra or swapped org with the same count refuses renew (roster digest)", async () => {
  const f = await fixture();
  const p = (await f.op("plan")).plan!;
  const copy = (org: string) => f.db.query(`INSERT INTO sessions.ingest_buffer_admission SELECT $1,privacy_baseline_sha256,
    retention_contract_sha256,schema_generation,valid_until,control_epoch FROM sessions.ingest_buffer_admission WHERE org_id=$2`, [org, F.orgs[0]]);
  const del = (org: string) => f.db.query("DELETE FROM sessions.ingest_buffer_admission WHERE org_id=$1", [org]);
  await del(F.orgs[1]);
  await refusesUnchanged(f, () => f.op("renew", { plan: p }), "admission_renewal_plan_roster_mismatch");
  await copy("org-new");
  expect((await f.state()).rows.length).toBe(F.orgs.length);
  await refusesUnchanged(f, () => f.op("renew", { plan: p }), "admission_renewal_plan_roster_mismatch");
  await copy(F.orgs[1]);
  await refusesUnchanged(f, () => f.op("renew", { plan: p }), "admission_renewal_plan_roster_mismatch");
  await del("org-new");
  expect((await f.op("renew", { plan: p })).receipt.committed).toBe(true);
}, 120_000);

test("a concurrent new-org insert between plan and renew refuses, and renew holds the advisory lock and SHARE ROW EXCLUSIVE on admission", async () => {
  const f = await fixture();
  const p = (await f.op("plan")).plan!;
  await f.db.query(`INSERT INTO sessions.ingest_buffer_admission SELECT 'org-late',privacy_baseline_sha256,retention_contract_sha256,
    schema_generation,valid_until,control_epoch FROM sessions.ingest_buffer_admission LIMIT 1`);
  await refusesUnchanged(f, () => f.op("renew", { plan: p }), "admission_renewal_plan_roster_mismatch");
  await f.db.query("DELETE FROM sessions.ingest_buffer_admission WHERE org_id='org-late'");
  let locks: { mode: string; granted: boolean }[] = [], advisory = 0;
  await f.op("renew", { plan: p, client: hooked(f, async (t, _p, next) => {
    if (t.includes("ingest_lazy_guard")) {
      locks = (await f.db.query<{ mode: string; granted: boolean }>(
        "SELECT mode,granted FROM pg_locks WHERE locktype='relation' AND relation='sessions.ingest_buffer_admission'::regclass")).rows;
      advisory = (await f.db.query("SELECT 1 FROM pg_locks WHERE locktype='advisory' AND classid=961996 AND objid=1 AND objsubid=2 AND granted")).rows.length;
    }
    return next();
  }) });
  // SHARE ROW EXCLUSIVE conflicts with ROW EXCLUSIVE (INSERT/UPDATE/DELETE) and with itself; reads proceed.
  expect(locks).toContainEqual({ mode: "ShareRowExclusiveLock", granted: true });
  expect(advisory).toBe(1);
}, 120_000);

test("mixed or expired expiry refuses; a sub-millisecond expiry is refused as non-canonical", async () => {
  const f = await fixture();
  const p = (await f.op("plan")).plan!;
  await f.db.query("UPDATE sessions.ingest_buffer_admission SET valid_until=valid_until+interval '1 millisecond' WHERE org_id=$1", [F.orgs[2]]);
  expect((await f.op("readback")).receipt).toMatchObject({ drift: "admission_renewal_mixed_expiry" });
  expect(await code(() => f.op("plan"))).toBe("admission_renewal_mixed_expiry");
  await refusesUnchanged(f, () => f.op("renew", { plan: p }), "admission_renewal_mixed_expiry");
  await f.db.query("UPDATE sessions.ingest_buffer_admission SET valid_until=valid_until-interval '1 millisecond' WHERE org_id=$1", [F.orgs[2]]);
  await f.db.query("UPDATE sessions.ingest_buffer_admission SET valid_until=valid_until+interval '500 microseconds'");
  await f.db.query("UPDATE sessions.ingest_lazy_policy SET valid_until=valid_until+interval '500 microseconds'");
  expect((await f.op("readback")).receipt.drift).toBe("admission_renewal_timestamp_precision");
  await refusesUnchanged(f, () => f.op("renew", { plan: p }), "admission_renewal_timestamp_precision");
  const g = await fixture({ validUntil: new Date(Math.floor(Date.now() / 1000) * 1000 - 3600_000).toISOString() });
  expect((await g.op("readback")).receipt.drift).toBe("admission_renewal_expired");
  expect(await code(() => g.op("plan"))).toBe("admission_renewal_expired");
  await refusesUnchanged(g, () => g.op("renew", { plan: p }), "admission_renewal_expired");
}, 120_000);

test("epoch, privacy, retention, source or updated_at drift refuses", async () => {
  const f = await fixture();
  const p = (await f.op("plan")).plan!;
  for (const column of ["control_epoch", "privacy_baseline_sha256", "retention_contract_sha256", "schema_generation"]) {
    await f.db.query(`UPDATE sessions.ingest_buffer_admission SET ${column}=$1 WHERE org_id=$2`, ["9".repeat(64), F.orgs[3]]);
    expect((await f.op("readback")).receipt.drift).toBe("admission_renewal_identity_drift");
    await refusesUnchanged(f, () => f.op("renew", { plan: p }), "admission_renewal_identity_drift");
    await f.db.query(`UPDATE sessions.ingest_buffer_admission SET ${column}=(SELECT ${column === "schema_generation" ? "generation" : column}
      FROM sessions.ingest_lazy_policy) WHERE org_id=$1`, [F.orgs[3]]);
  }
  // Immutable policy fields the rows do not carry are bound only by the descriptor.
  for (const column of ["source_identity_sha256", "source_proof_sha256"]) {
    const was = (await f.db.query<{ v: string }>(`SELECT ${column} v FROM sessions.ingest_lazy_policy`)).rows[0]!.v;
    await f.db.query(`UPDATE sessions.ingest_lazy_policy SET ${column}=$1`, ["9".repeat(64)]);
    await refusesUnchanged(f, () => f.op("renew", { plan: p }), "admission_renewal_plan_descriptor_mismatch");
    await f.db.query(`UPDATE sessions.ingest_lazy_policy SET ${column}=$1`, [was]);
  }
  await f.db.query("UPDATE sessions.ingest_lazy_policy SET control_epoch=$1", ["9".repeat(64)]);
  await refusesUnchanged(f, () => f.op("renew", { plan: p }), "admission_renewal_identity_drift");
  await f.db.query("UPDATE sessions.ingest_lazy_policy SET control_epoch=$1", [F.epoch]);
  await f.db.query("UPDATE sessions.ingest_lazy_policy SET updated_at=updated_at+interval '1 microsecond'");
  await refusesUnchanged(f, () => f.op("renew", { plan: p }), "admission_renewal_plan_descriptor_mismatch");
}, 120_000);

test("policy-only or admission-only update rolls back both tables", async () => {
  const f = await fixture();
  const p = (await f.op("plan")).plan!;
  // The admission UPDATE silently does nothing: the policy already moved inside the transaction, so it rolls back.
  const noAdmission = hooked(f, (t, params, next) =>
    t.includes(ADMISSION_UPDATE) ? f.client.query("SELECT org_id FROM sessions.ingest_buffer_admission WHERE false") : next());
  await refusesUnchanged(f, () => f.op("renew", { plan: p, client: noAdmission }), "admission_renewal_partial_update");
  // The policy setter is replaced by a fake acceptance: rows move, the postcondition sees the old policy expiry.
  const noPolicy = hooked(f, (t, params, next) => t.includes("ingest_lazy_policy_set")
    ? f.client.query("SELECT jsonb_build_object('accepted',true,'generation',($1::jsonb)->>'generation') AS r", params)
    : next());
  await refusesUnchanged(f, () => f.op("renew", { plan: p, client: noPolicy }), "admission_renewal_postcondition");
  expect((await f.op("renew", { plan: p })).receipt.committed).toBe(true);
}, 120_000);

test("partial update, postcondition drift and postcondition guard drift refuse and roll back", async () => {
  const f = await fixture();
  const p = (await f.op("plan")).plan!;
  const partial = hooked(f, async (t, _p, next) => {
    if (t.includes(ADMISSION_UPDATE))
      await f.db.query("UPDATE sessions.ingest_buffer_admission SET valid_until=valid_until+interval '1 millisecond' WHERE org_id=$1", [F.orgs[4]]);
    return next();
  });
  await refusesUnchanged(f, () => f.op("renew", { plan: p, client: partial }), "admission_renewal_partial_update");
  let after = false;
  const drift = hooked(f, async (t, _p, next) => {
    const r = await next();
    if (t.includes(ADMISSION_UPDATE) && !after) {
      after = true;
      await f.db.query("UPDATE sessions.ingest_buffer_admission SET valid_until=valid_until+interval '1 millisecond' WHERE org_id=$1", [F.orgs[5]]);
    }
    return r;
  });
  await refusesUnchanged(f, () => f.op("renew", { plan: p, client: drift }), "admission_renewal_postcondition");
  const guard = hooked(f, async (t, _p, next) => {
    if (t.includes("ingest_lazy_guard")) await f.db.query("UPDATE sessions.ingest_receipt_seat_clock SET control_epoch=$1", ["9".repeat(64)]);
    return next();
  });
  await refusesUnchanged(f, () => f.op("renew", { plan: p, client: guard }), "admission_renewal_postcondition_guard");
  const commit = hooked(f, (t, _p, next) => (t === "COMMIT" ? Promise.reject(new Error("connection lost")) : next()));
  await refusesUnchanged(f, () => f.op("renew", { plan: p, client: commit }), "admission_renewal_commit_unknown");
  expect((await f.op("renew", { plan: p })).receipt.committed).toBe(true);
}, 120_000);

test("extra or missing policy row refuses", async () => {
  const f = await fixture();
  const p = (await f.op("plan")).plan!;
  // The singleton primary key forbids a second row; a reader that returns two rows models a broken singleton.
  const extra = hooked(f, async (t, params, next) => {
    const r = await next();
    return t.includes("FROM sessions.ingest_lazy_policy") && !t.includes("ingest_lazy_policy_set") ? { rows: [...r.rows, ...r.rows] } : r;
  });
  await refusesUnchanged(f, () => f.op("renew", { plan: p, client: extra }), "admission_renewal_policy_extra");
  const missing = hooked(f, async (t, params, next) => {
    const r = await next();
    return t.includes("FROM sessions.ingest_lazy_policy") && !t.includes("ingest_lazy_policy_set") ? { rows: [] } : r;
  });
  expect((await f.op("readback", { client: missing })).receipt.drift).toBe("admission_renewal_policy_missing");
  await refusesUnchanged(f, () => f.op("renew", { plan: p, client: missing }), "admission_renewal_policy_missing");
}, 120_000);

test("generation: the live PASS marker generation is taken as-is (never recomputed); a shorter checksum-equal ledger passes and binds its tail", async () => {
  const f = await fixture();
  // The marker generation is opaque: not this tree's generation(), not any recomputation; plan binds it unchanged.
  expect(F.generation).not.toBe(await generation());
  const { plan, receipt } = await f.op("plan");
  expect(plan).toMatchObject({ generation: F.generation, ledgerTail: F.ledgerTail, stepCount: F.stepCount, ledgerPrefixDigest: F.ledgerPrefixDigest });
  expect(receipt).toMatchObject({ ledgerTail: F.ledgerTail, drift: null });
  expect(F.held.length).toBe(2);
  // Shorter still (the tail row removed, checksums unchanged): passes with the earlier tail; the earlier plan
  // was bound to the longer prefix and refuses as ledger drift.
  const upstream = F.ledger.filter(([n]) => n.startsWith("upstream:"));
  await f.db.query(`DELETE FROM ${LEDGER} WHERE name=$1`, [F.ledgerTail]);
  const shorter = (await f.op("plan")).plan!;
  expect(shorter).toMatchObject({ generation: F.generation, ledgerTail: upstream.at(-2)![0], stepCount: F.stepCount - 1 });
  expect(shorter.descriptorSha256).not.toBe(plan!.descriptorSha256);
  await refusesUnchanged(f, () => f.op("renew", { plan }), "admission_renewal_ledger_drift");
  await f.db.query(`INSERT INTO ${LEDGER}(name, sha256) VALUES($1, $2)`, upstream.at(-1)!);
  expect((await f.op("renew", { plan })).receipt).toMatchObject({ committed: true, ledgerTail: F.ledgerTail });
}, 120_000);

test("ledger proof reads the dispatched tree: verifier-file drift and a held-step edit still pass; a recorded migration file edit refuses", async () => {
  const f = await fixture();
  const repo = join(import.meta.dir, "../../..");
  const tree = mkdtempSync(join(tmpdir(), "renewal-tree-"));
  try {
    for (const lane of RELEASE_TREE.lanes) cpSync(join(repo, lane.dir), join(tree, lane.dir), { recursive: true });
    for (const v of RELEASE_TREE.verifiers) {
      mkdirSync(dirname(join(tree, v.path)), { recursive: true });
      cpSync(join(repo, v.path), join(tree, v.path));
    }
    const live = await admissionRenewalLedger(f.client as never);
    expect(live).toEqual({ ledgerTail: F.ledgerTail, stepCount: F.stepCount, ledgerPrefixDigest: F.ledgerPrefixDigest });
    expect(await admissionRenewalLedger(f.client as never, tree)).toEqual(live);
    // Every verifier file drifts (generation() of the copy changes); the ledger prefix is unchanged: still passes.
    for (const v of RELEASE_TREE.verifiers) appendFileSync(join(tree, v.path), "\n-- drift\n");
    expect(await admissionRenewalLedger(f.client as never, tree)).toEqual(live);
    // A held (unrecorded) step's file changes: not part of the prefix, still passes.
    const laneOf = (n: string) => RELEASE_TREE.lanes.find((l) => n.startsWith(l.prefix))!;
    const file = (n: string) => join(tree, laneOf(n).dir, n.slice(laneOf(n).prefix.length));
    appendFileSync(file(F.held[0]!), "\n-- held edit\n");
    expect(await admissionRenewalLedger(f.client as never, tree)).toEqual(live);
    // A recorded step's file changes: migration_changed.
    appendFileSync(file(F.ledger[3]![0]), "\n-- recorded edit\n");
    expect(await code(() => admissionRenewalLedger(f.client as never, tree))).toBe("migration_changed");
  } finally {
    rmSync(tree, { recursive: true, force: true });
  }
}, 120_000);

test("ledger proof: a changed ledgered checksum, an unknown step, a ledger longer than the tree or a gap refuses with the migrator's preflight codes", async () => {
  const f = await fixture();
  const p = (await f.op("plan")).plan!;
  const cases: [string, () => Promise<unknown>, () => Promise<unknown>][] = [
    ["migration_changed",
      () => f.db.query(`UPDATE ${LEDGER} SET sha256=$1 WHERE name=$2`, ["0".repeat(64), F.ledger[3]![0]]),
      () => f.db.query(`UPDATE ${LEDGER} SET sha256=$1 WHERE name=$2`, [F.ledger[3]![1], F.ledger[3]![0]])],
    ["ledger_unknown_step",
      () => f.db.query(`INSERT INTO ${LEDGER}(name, sha256) VALUES('96bulls:9999_not_in_this_tree.sql', $1)`, ["0".repeat(64)]),
      () => f.db.query(`DELETE FROM ${LEDGER} WHERE name='96bulls:9999_not_in_this_tree.sql'`)],
    ["ledger_gap",
      () => f.db.query(`DELETE FROM ${LEDGER} WHERE name=$1`, [F.ledger[5]![0]]),
      () => f.db.query(`INSERT INTO ${LEDGER}(name, sha256) VALUES($1, $2)`, F.ledger[5]!)],
  ];
  for (const [expected, damage, repair] of cases) {
    await damage();
    expect((await f.op("readback")).receipt.drift).toBe(expected);
    expect(await code(() => f.op("plan"))).toBe(expected);
    await refusesUnchanged(f, () => f.op("renew", { plan: p }), expected);
    await repair();
  }
  // A ledger longer than the dispatched tree (every tree step recorded, plus a later upstream step this tree
  // does not have) refuses as an unknown step.
  const tree = (await (await import("../migrate.ts")).schemaStepDigests([]));
  for (const [name, sha] of tree.filter(([n]) => F.held.includes(n)))
    await f.db.query(`INSERT INTO ${LEDGER}(name, sha256) VALUES($1, $2)`, [name, sha]);
  expect((await f.op("plan")).plan!.ledgerTail).toBe(tree.at(-1)![0]);
  await f.db.query(`INSERT INTO ${LEDGER}(name, sha256) VALUES('upstream:99991231000000_future.sql', $1)`, ["0".repeat(64)]);
  expect(await code(() => f.op("plan"))).toBe("ledger_unknown_step");
  await refusesUnchanged(f, () => f.op("renew", { plan: p }), "ledger_unknown_step");
  await f.db.query(`DELETE FROM ${LEDGER} WHERE name='upstream:99991231000000_future.sql' OR name=ANY($1)`, [F.held]);
  // A recorded held step (upstream recorded past an unrecorded one) is a gap too.
  await f.db.query(`INSERT INTO ${LEDGER}(name, sha256) SELECT $1, sha256 FROM ${LEDGER} LIMIT 1`, [F.held[1]!]);
  expect(await code(() => f.op("plan"))).toBe("ledger_gap");
  await f.db.query(`DELETE FROM ${LEDGER} WHERE name=$1`, [F.held[1]!]);
  await f.db.query(`DROP TABLE ${LEDGER}`);
  expect(await code(() => f.op("plan"))).toBe("admission_renewal_ledger_empty");
  expect((await f.op("readback")).receipt.drift).toBe("admission_renewal_ledger_empty");
}, 120_000);

/** The ingest admission insert exactly as ingest_lazy_begin issues it (columns from the policy; ON CONFLICT DO NOTHING). */
const INGEST_INSERT = `INSERT INTO sessions.ingest_buffer_admission(org_id,privacy_baseline_sha256,retention_contract_sha256,
  schema_generation,valid_until,control_epoch) SELECT $1,privacy_baseline_sha256,retention_contract_sha256,generation,valid_until,
  control_epoch FROM sessions.ingest_lazy_policy ON CONFLICT DO NOTHING`;

test("lock order matches the ingest admission path (read from the live function bodies), so the two cannot deadlock", async () => {
  const f = await fixture();
  const body = async (name: string) => (await f.db.query<{ src: string }>(
    "SELECT prosrc AS src FROM pg_proc WHERE oid=$1::regprocedure", [name])).rows[0]!.src.replace(/\s+/gu, " ");
  const begin = await body("sessions.ingest_lazy_begin(jsonb,text)");
  const require = await body("sessions.ingest_require_generation(text)");
  const guard = await body("sessions.ingest_lazy_guard(text)");
  const pos = (src: string, needle: string) => { const i = src.indexOf(needle); expect(i).toBeGreaterThanOrEqual(0); return i; };
  // Ingest takes no 961996 advisory lock; it locks the marker, then the policy, then the admission table and row.
  for (const src of [begin, require, guard]) expect(src).not.toContain("961996");
  expect(require).toMatch(/__96bulls_role_validation[^;]*FOR SHARE/u);
  expect(pos(begin, "ingest_require_generation")).toBeLessThan(pos(begin, "FROM sessions.ingest_lazy_policy WHERE singleton FOR SHARE"));
  expect(pos(begin, "FROM sessions.ingest_lazy_policy WHERE singleton FOR SHARE")).toBeLessThan(pos(begin, "INSERT INTO sessions.ingest_buffer_admission"));
  expect(pos(guard, "__96bulls_role_validation")).toBeLessThan(pos(guard, "FROM sessions.ingest_lazy_policy WHERE singleton FOR SHARE"));
  expect(pos(guard, "FROM sessions.ingest_lazy_policy WHERE singleton FOR SHARE")).toBeLessThan(pos(guard, "FROM sessions.ingest_buffer_admission"));
  // renew: advisory, marker, policy, admission table, admission rows (the same relative order).
  const p = (await f.op("plan")).plan!;
  const seen: string[] = [];
  await f.op("renew", { plan: p, client: hooked(f, (t, _p, next) => { seen.push(t.replace(/\s+/gu, " ")); return next(); }) });
  const at = (s: string) => { const i = seen.findIndex((t) => t.includes(s)); expect(i).toBeGreaterThanOrEqual(0); return i; };
  const order = ["pg_advisory_xact_lock(961996, 1)", "WHERE name=$1 FOR SHARE", "FROM sessions.ingest_lazy_policy FOR UPDATE",
    "IN SHARE ROW EXCLUSIVE MODE", 'COLLATE "C" FOR UPDATE'].map(at);
  expect(order).toEqual([...order].sort((a, b) => a - b));
}, 120_000);

test("concurrent ingest admission insert vs renew: an insert that wins the race is refused by the locked re-check; one that waits serializes after commit; never a partial set", async () => {
  const f = await fixture();
  const p = (await f.op("plan")).plan!;
  // PGlite has one session. The two interleavings PostgreSQL permits are modelled at their boundary:
  // (1) the ingest insert lands before renew's SHARE ROW EXCLUSIVE is granted (it is visible to the locked read):
  //     renew refuses on the roster re-check and rolls back whole.
  const won = hooked(f, async (t, _p, next) => {
    if (t.includes("IN SHARE ROW EXCLUSIVE MODE")) await f.db.query(INGEST_INSERT, ["org-race"]);
    return next();
  });
  await refusesUnchanged(f, () => f.op("renew", { plan: p, client: won }), "admission_renewal_plan_roster_mismatch");
  // (2) the insert waits on renew's table lock and runs after COMMIT: it copies the renewed policy expiry, so the
  //     whole set stays uniform and admitted.
  const renewed = await f.op("renew", { plan: p });
  expect(renewed.receipt.committed).toBe(true);
  await f.db.query(INGEST_INSERT, ["org-late"]);
  const after = await f.state();
  const want = p.renewTo.replace("Z", "000Z");
  expect(after.policy).toBe(want);
  expect(after.rows.length).toBe(F.orgs.length + 1);
  expect(after.rows.every((r) => r.v === want)).toBe(true);
  expect((await f.op("readback")).receipt).toMatchObject({ drift: null, admissionRows: F.orgs.length + 1 });
  for (const org of [...F.orgs, "org-late"]) await f.db.query("SELECT sessions.ingest_lazy_guard($1)", [org]);
}, 120_000);
