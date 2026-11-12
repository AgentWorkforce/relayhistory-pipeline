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
  parseAdmissionRenewalPlan,
  type AdmissionRenewalPlan,
} from "../ingest-admission-renewal-operator.ts";
import { ADMISSION_RENEWAL_ENABLED, ADMISSION_RENEWAL_HORIZON_MS } from "../admission-renewal-contract.ts";
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
    admissionMinValidUntil: f.validUntil, admissionMaxValidUntil: f.validUntil, generationPrefix: F.generation.slice(0, 8) });
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
  expect(Object.keys(p).sort()).toEqual(["admissionCount", "capturedAt", "descriptorSha256", "generation", "oldValidUntil",
    "orgsSha256", "planRun", "renewTo", "sha"]);
  expect(Date.parse(p.renewTo) - Date.parse(p.capturedAt)).toBe(ADMISSION_RENEWAL_HORIZON_MS);
  expect(new Date(Date.parse(p.capturedAt)).toISOString()).toBe(p.capturedAt);
  expect(Math.abs(Date.parse(p.capturedAt) - Date.now())).toBeLessThan(60_000);
  expect(p).toMatchObject({ oldValidUntil: f.validUntil, admissionCount: F.orgs.length, generation: F.generation, sha: F.sha, planRun: F.planRun });
  const cOrder = [...F.orgs].sort((a, b) => Buffer.compare(Buffer.from(a), Buffer.from(b)));
  expect(p.orgsSha256).toBe(sha256(cOrder.join("\n")));
  expect(p.descriptorSha256).toMatch(/^[0-9a-f]{64}$/);
  expect(receipt).toMatchObject({ status: "ADMISSION_RENEWAL_PLANNED", renewTo: p.renewTo, committed: false });
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
  // Lock order, the owner-only setter (no raw policy UPDATE), then the CAS update, in one transaction.
  const at = (s: string) => seen.findIndex((t) => t.includes(s));
  expect(seen[0]).toBe("BEGIN");
  expect(seen.at(-1)).toBe("COMMIT");
  expect(at("pg_advisory_xact_lock(961996, 1)")).toBeLessThan(at("IN SHARE ROW EXCLUSIVE MODE"));
  expect(at("IN SHARE ROW EXCLUSIVE MODE")).toBeLessThan(at("FROM sessions.ingest_lazy_policy FOR UPDATE"));
  expect(at("FROM sessions.ingest_lazy_policy FOR UPDATE")).toBeLessThan(at('COLLATE "C" FOR UPDATE'));
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
  expect(phrase).toBe(`RENEW-ADMISSION ${F.project} ${F.sha} GENERATION ${F.generation} FROM ${p.oldValidUntil} TO ${p.renewTo}` +
    ` ROWS ${F.orgs.length} ORGS ${p.orgsSha256} DESCRIPTOR ${p.descriptorSha256} PLAN ${F.planRun}`);
  const noMs = (t: string) => t.replace(/\.000Z$/u, "Z").replace(/\.([0-9]{3})Z$/u, ".$1000Z");
  for (const forged of [phrase + " ", " " + phrase, phrase.toLowerCase(), phrase.replace(F.project, "otherprojectref00000"),
    phrase.replace(` PLAN ${F.planRun}`, " PLAN 1"), phrase.replace(p.oldValidUntil, noMs(p.oldValidUntil)),
    phrase.replace(p.renewTo, noMs(p.renewTo)), phrase.replace(p.renewTo, p.renewTo.replace("Z", "+00:00")),
    phrase.replace(` ROWS ${F.orgs.length} `, ` ROWS 0${F.orgs.length} `)])
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

test("stale plan, stale generation, wrong plan run or sha refuse", async () => {
  const f = await fixture();
  const p = (await f.op("plan")).plan!;
  const captured = Date.parse(p.capturedAt);
  expect(await code(() => f.op("renew", { plan: p, now: captured + 24 * 3600_000 }))).toBe("admission_renewal_plan_stale");
  expect(await code(() => f.op("renew", { plan: p, now: captured - 6 * 60_000 }))).toBe("admission_renewal_plan_stale");
  expect(await code(() => f.op("renew", { plan: p, expectedGeneration: "e".repeat(64) }))).toBe("admission_renewal_generation_stale");
  const base = { RENEWAL_MODE: "renew", RENEWAL_SHA: F.sha, RENEWAL_PLAN_RUN: F.planRun, RENEWAL_CONFIRMATION: admissionRenewalConfirmation("renew", F.project, p) };
  const inputs = (env: Record<string, string>, plan: unknown = p) =>
    code(() => admissionRenewalInputs(env, { expectedGeneration: F.generation, plan, enabled: true, project: F.project }));
  expect(await inputs({ ...base, RENEWAL_PLAN_RUN: "424243" })).toBe("admission_renewal_plan_run");
  expect(await inputs({ ...base, RENEWAL_SHA: "2".repeat(40) })).toBe("admission_renewal_plan_sha");
  expect(await inputs(base, null)).toBe("admission_renewal_plan");
  // The plan's own age is rechecked against the database clock inside the renew transaction.
  const old = new Date(Date.now() - 24 * 3600_000 - 1000);
  const aged = { ...p, capturedAt: old.toISOString(), renewTo: new Date(old.getTime() + ADMISSION_RENEWAL_HORIZON_MS).toISOString() };
  await refusesUnchanged(f, () => f.op("renew", { plan: aged, now: Date.now() - 2 * 3600_000 }), "admission_renewal_plan_stale");
}, 120_000);

test("stale PASS or generation drift refuses: marker not pass, marker at another generation, policy not at the exact-SHA generation", async () => {
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
  expect((await f.op("readback", { expectedGeneration: "e".repeat(64) })).receipt.drift).toBe("admission_renewal_generation_stale");
  expect(await code(() => f.op("plan", { expectedGeneration: "e".repeat(64) }))).toBe("admission_renewal_generation_stale");
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
