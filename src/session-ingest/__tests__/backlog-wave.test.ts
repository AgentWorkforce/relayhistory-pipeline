/**
 * Per-org wave runs (backlog-wave.ts; the credential step's `per_org` path in scripts/backlog-ops.ts): one child and
 * one verdict per scope org, per-org bindings on views, exact wave receipts, and a two-org chain FULL -> catch-up ->
 * export -> drain where one org refuses and the other completes. Fake OIDC/STS and tool children; no credentials.
 * Also pins a single-org (non-wave) receipt byte for byte.
 */
import { afterEach, expect, test } from "bun:test";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createHash } from "node:crypto";
import { evidenceHash } from "../authority-evidence.ts";
import { toReceiptShape } from "../backlog-authority.ts";
import { BACKLOG_OPS_PER_ORG_MODES, backlogOpsDispatch, backlogOpsReceipt, backlogOpsScope, type BacklogOpsMode } from "../backlog-ops.ts";
import { isWaveReceipt, orgScopeId, waveConcurrencyFor, waveReceiptSummary, waveSummary, waveView } from "../backlog-wave.ts";
import { backlogOpsRun, markerWaveBound } from "../../../scripts/backlog-ops.ts";
import { SCHEMA_RELEASE as P } from "../../hoopdb/schema-release-contract.ts";
import { ORGS, SHA, baseline, evidenceReport } from "./marker-support.ts";

const cleanups: (() => void)[] = [];
afterEach(() => { for (const c of cleanups.splice(0)) c(); });
const code = (fn: () => unknown) => { try { fn(); return null; } catch (e) { return (e as { code?: string }).code ?? String(e); } };
const [A, B] = ORGS as [string, string];
const TWO = [A, B].sort();
const SCOPE_ENV = { BACKLOG_OPS_ORGS: TWO.join(",") };
const SCOPE_ID = backlogOpsScope(SCOPE_ENV).id;
const H = (o: string) => evidenceHash("org", o);
const SINCE = 1_700_000_000_000;
const target = {
  BACKLOG_OPS_BUCKET: "96bulls-aws-sessions-prod-transcripts-000000000000", BACKLOG_OPS_REGION: "us-east-1",
  BACKLOG_OPS_KMS_KEY_ARN: "arn:aws:kms:us-east-1:000000000000:key/00000000-0000-4000-8000-000000000000",
  BACKLOG_OPS_DRY_ROLE_ARN: "arn:aws:iam::000000000000:role/dry", BACKLOG_OPS_EXECUTE_ROLE_ARN: "arn:aws:iam::000000000000:role/exec",
};
const zeros = (...k: string[]) => Object.fromEntries(k.map((x) => [x, 0]));
const backfill = (mode: "dry_run" | "execute", over: Record<string, unknown> = {}) => ({ mode, users: 2,
  fences: zeros("deletion_rows", "reset_rows", "created", "updated", "unchanged", "would_create", "would_update", "pairs_added", "resets_added", "deletions_superseded",
    "deletions_unproven", "cold_resets", "needs_review", "unreadable", "contended", "tenant_invalid", "input_invalid", "failed", "resets_in_flight"),
  ownership: { ...zeros("rows", "registered", "would_register", "unchanged", "skipped_deleted", "skipped_no_fence", "invalid", "failed"), failed_codes: {} },
  revisions: { ...zeros("rows", "seed_rows", "slot_rows", "slots_empty", "merged", "would_merge", "skipped_deleted", "skipped_no_fence", "invalid", "shards", "failed"), failed_codes: {} },
  sections: { fences: "complete", ownership: "complete", revisions: "complete" },
  fingerprint: zeros("revisions_count", "revisions_max_updated_ms", "revisions_hash", "ownership_count", "ownership_hash", "slots_owned_count", "slots_revision_sum", "slots_hash"),
  ...over });
const exportOk = (mode: "dry_run" | "execute") => ({ mode, failed: 0, fence_late: 0, orgs: { complete: 1, would_complete: 0, already_complete: 0, incomplete: 0 } });

// ---- the run ----------------------------------------------------------------------------------------------------------

const RUN_IDS: Partial<Record<BacklogOpsMode, string>> = { "backfill-dry": "701", "backfill-execute": "702", "backfill-catchup-dry": "703", "backfill-catchup-execute": "704",
  "export-dry": "705", "export-execute": "706", "drain-dry": "707", "drain-execute": "708" };
const xml = `<AssumeRoleWithWebIdentityResponse><Credentials><AccessKeyId>ASIAAAAAAAAAAAAAAAAA</AccessKeyId><SecretAccessKey>s</SecretAccessKey>
  <SessionToken>t</SessionToken><Expiration>${new Date(Date.now() + 6 * 3600_000).toISOString()}</Expiration></Credentials></AssumeRoleWithWebIdentityResponse>`;
const fetcher = (async (url: string) => url.startsWith("https://token.example/") ? new Response(JSON.stringify({ value: "a.b.c" })) : new Response(xml)) as unknown as typeof fetch;
const ownerUrl = `postgresql://${P.owner}.${P.project}:p%40ss@${P.host}:${P.port}/${P.database}?sslmode=verify-full`;
const receipts: Partial<Record<BacklogOpsMode, string>> = {};

/** One credential step: `bind` maps a binding kind to the mode whose persisted receipt it gets. */
async function step(mode: BacklogOpsMode, env: Record<string, string>, bind: Record<string, BacklogOpsMode>, child: (org: string) => { code: number; stdout: string },
  extra: Record<string, string> = {}) {
  const temp = mkdtempSync(join(tmpdir(), "wave-step-"));
  cleanups.push(() => rmSync(temp, { recursive: true, force: true }));
  for (const [kind, m] of Object.entries(bind)) {
    mkdirSync(join(temp, "backlog-ops-bound", kind), { recursive: true });
    writeFileSync(join(temp, "backlog-ops-bound", kind, "backlog-ops-receipt.json"), receipts[m]!);
  }
  const lines: string[] = [], seen: string[] = [];
  let inFlight = 0, peak = 0;
  const log = console.log;
  console.log = (...a: unknown[]) => { lines.push(a.join(" ")); };
  let exit: number;
  try {
    exit = await backlogOpsRun({ ...target, ...SCOPE_ENV, BACKLOG_OPS_MODE: mode, BACKLOG_OPS_SHA: SHA, BACKLOG_OPS_PER_ORG: "true", ...env, GITHUB_SHA: SHA,
      GITHUB_RUN_ID: RUN_IDS[mode]!, RUNNER_TEMP: temp, SUPABASE_MIGRATION_OWNER_URL: ownerUrl, ACTIONS_ID_TOKEN_REQUEST_URL: "https://token.example/x",
      ACTIONS_ID_TOKEN_REQUEST_TOKEN: "bearer", ...extra },
      { fetcher, runChild: async (c) => {
        inFlight++; peak = Math.max(peak, inFlight);
        const orgs = c.args.flatMap((a, i) => (c.args[i - 1] === "--org" ? [a] : []));
        expect(orgs).toHaveLength(1); // one org per child
        seen.push(orgs[0]!);
        await new Promise((r) => setTimeout(r, 5));
        inFlight--;
        return child(orgs[0]!);
      } });
  } finally { console.log = log; }
  const file = readFileSync(join(temp, "backlog-ops-out", "backlog-ops-receipt.json"), "utf8");
  receipts[mode] = file;
  const receipt = JSON.parse(file);
  const by = Object.fromEntries((receipt.summary.per_org ?? []).map((p: { org_hash: string }) => [p.org_hash, p]));
  return { exit, receipt, by, seen, peak, lines, file };
}

test("dispatch: per_org is \"true\" on the per-org modes only, a flag (never an org); absent by default", () => {
  for (const mode of BACKLOG_OPS_PER_ORG_MODES.filter((m) => m.endsWith("-dry") && !m.startsWith("backfill-catchup") ))
    expect(backlogOpsDispatch({ BACKLOG_OPS_MODE: mode, BACKLOG_OPS_SHA: SHA, BACKLOG_OPS_PER_ORG: "true" }).perOrg).toBe(true);
  expect(backlogOpsDispatch({ BACKLOG_OPS_MODE: "backfill-dry", BACKLOG_OPS_SHA: SHA }).perOrg).toBeUndefined();
  for (const mode of ["presence-dry", "reshard-dry", "authority-dry"] as BacklogOpsMode[])
    expect(code(() => backlogOpsDispatch({ BACKLOG_OPS_MODE: mode, BACKLOG_OPS_SHA: SHA, BACKLOG_OPS_PER_ORG: "true" }))).toBe("backlog_ops_per_org");
  for (const bad of ["1", "TRUE", "yes", A]) expect(code(() => backlogOpsDispatch({ BACKLOG_OPS_MODE: "backfill-dry", BACKLOG_OPS_SHA: SHA, BACKLOG_OPS_PER_ORG: bad }))).toBe("backlog_ops_per_org");
  expect(waveConcurrencyFor("drain-execute")).toBe(1);
  expect(waveConcurrencyFor("export-execute")).toBe(1);
  expect(waveConcurrencyFor("backfill-execute")).toBe(2);
});

test("a single-org (non-wave) receipt is byte-identical: pinned", () => {
  const d = backlogOpsDispatch({ BACKLOG_OPS_MODE: "export-dry", BACKLOG_OPS_SHA: SHA });
  const r = backlogOpsReceipt(d, "9", 0, JSON.stringify(exportOk("dry_run")), backlogOpsScope({ BACKLOG_OPS_ORGS: A }).id);
  expect(JSON.stringify(r)).toBe(`{"contract":"backlog-ops-receipt/v1","mode":"export-dry","sha":"${SHA}","run":"9","scope":"${orgScopeId(A)}","ledger":"none",`
    + `"exitCode":0,"summary":{"mode":"dry_run","failed":0,"fence_late":0,"orgs":{"complete":1,"would_complete":0,"already_complete":0,"incomplete":0}}}`);
  expect(orgScopeId(A)).toBe(backlogOpsScope({ BACKLOG_OPS_ORGS: A }).id);
});

test("the wave receipt: exact, hashes exactly the scope's each once, ok_count derived, every slice its mode's check", () => {
  const slice = (s: unknown) => { if (JSON.stringify(s).includes("-")) throw new Error("not a count"); return s; };
  const w = waveSummary(TWO, TWO.map((o, i) => ({ org: o, exitCode: i, summary: i === 0 ? { mode: "dry_run", listed: 1 } : { refused: "backlog_ops_x" } })));
  expect(w.ok_count).toBe(1);
  const ok = (s: unknown, orgs: readonly string[] = TWO) => waveReceiptSummary(s, orgs, slice).ok;
  expect(ok(w)).toBe(true);
  expect(ok(w, [A])).toBe(false); // another scope
  expect(ok({ ...w, ok_count: 2 })).toBe(false); // an aggregate is derived, never free
  expect(ok({ ...w, org_count: 3 })).toBe(false);
  expect(ok({ ...w, per_org: [w.per_org[0], w.per_org[0]] })).toBe(false); // each org exactly once
  expect(ok({ ...w, per_org: [w.per_org[0]] })).toBe(false);
  expect(ok({ ...w, extra: 1 })).toBe(false);
  expect(ok({ ...w, per_org: w.per_org.map((p, i) => i ? { ...p, org_hash: A } : p) })).toBe(false); // a raw org id
  expect(ok({ ...w, per_org: w.per_org.map((p, i) => i ? { ...p, summary: { id: A } } : p) })).toBe(false); // a slice its check refuses
  expect(ok({ ...w, per_org: w.per_org.map((p) => ({ ...p, extra: 1 })) })).toBe(false);
  expect(ok({ ...w, ok_count: 0, per_org: w.per_org.map((p) => ({ ...p, exit_code: 1 })) })).toBe(false); // no org ok: the run fails
  expect(code(() => waveSummary(TWO, [{ org: B, exitCode: 0, summary: {} }, { org: A, exitCode: 0, summary: {} }]))).toBe("backlog_ops_wave_slices");
});

test("views: a wave slice becomes a single-org receipt; a non-wave receipt of the wave's scope is re-scoped and its per-org lists sliced", () => {
  const top = { contract: "backlog-ops-receipt/v1", mode: "backfill-execute", sha: SHA, run: "1", scope: SCOPE_ID, ledger: "none", home: "home_" + "a".repeat(32) };
  const wave = JSON.stringify({ ...top, exitCode: 0, summary: waveSummary(TWO, TWO.map((o) => ({ org: o, exitCode: o === A ? 0 : 1, summary: { o: o === A ? 1 : 2 } }))) });
  expect(isWaveReceipt(wave)).toBe(true);
  expect(JSON.parse(waveView(wave, A, SCOPE_ID))).toEqual({ ...top, scope: orgScopeId(A), exitCode: A === TWO[0] ? 0 : 0, summary: { o: 1 } });
  expect(JSON.parse(waveView(wave, B, SCOPE_ID))).toMatchObject({ scope: orgScopeId(B), exitCode: 1, summary: { o: 2 } });
  expect(code(() => waveView(wave, "dddddddd-0000-4000-8000-000000000000", SCOPE_ID))).toBe("backlog_ops_wave_view");
  const authority = JSON.stringify({ ...top, mode: "authority-dry", exitCode: 0, summary: { mode: "dry_run", evidence: toReceiptShape(evidenceReport(TWO)) } });
  const v = JSON.parse(waveView(authority, B, SCOPE_ID));
  expect(v.scope).toBe(orgScopeId(B));
  expect(v.summary.evidence.org_count).toBe(1);
  expect(v.summary.evidence.orgs.map((o: { org_hash: string }) => o.org_hash)).toEqual([H(B)]);
  // Another scope is kept as is, so the exact scope check refuses it.
  const other = JSON.stringify({ ...top, scope: "orgs_" + "f".repeat(32), exitCode: 0, summary: {} });
  expect(JSON.parse(waveView(other, A, SCOPE_ID)).scope).toBe("orgs_" + "f".repeat(32));
});

test("two orgs, one refuses: FULL, catch-up, export and drain each give B its own refusal and complete A; children bounded", async () => {
  const fine = (s: unknown) => () => ({ code: 0, stdout: JSON.stringify(s) });
  // backfill-dry: both fine.
  const bd = await step("backfill-dry", {}, {}, fine(backfill("dry_run")));
  expect(bd.exit).toBe(0);
  expect(bd.receipt.summary).toMatchObject({ kind: "wave", org_count: 2, ok_count: 2 });
  expect(bd.peak).toBeLessThanOrEqual(2);
  // FULL: B's revisions section fails (the tool exits 1); A completes.
  const full = await step("backfill-execute", { BACKLOG_OPS_DRY_RUN: "701", BACKLOG_OPS_CONFIRMATION: `BACKFILL-EXECUTE ${SHA} DRY 701 PER_ORG` }, { dry: "backfill-dry" },
    (o) => o === B ? { code: 1, stdout: JSON.stringify(backfill("execute", { sections: { fences: "complete", ownership: "complete", revisions: "incomplete" } })) }
      : { code: 0, stdout: JSON.stringify(backfill("execute")) });
  expect(full.exit).toBe(0);
  expect(full.by[H(A)]).toMatchObject({ exit_code: 0 });
  expect(full.by[H(B)]).toMatchObject({ exit_code: 1 });
  expect(full.receipt.summary.ok_count).toBe(1);
  expect(full.receipt).toMatchObject({ mode: "backfill-execute", scope: SCOPE_ID, exitCode: 0 });
  // catch-up dry and execute: B's FULL slice refuses B's binding, so no child runs for B.
  const cu = (mode: "dry_run" | "execute") => fine(backfill(mode, { revisions_since_ms: SINCE }));
  const since = { BACKLOG_OPS_REVISIONS_SINCE_MS: String(SINCE), BACKLOG_OPS_FULL_BACKFILL_RUN: "702" };
  const cd = await step("backfill-catchup-dry", since, { full: "backfill-execute" }, cu("dry_run"));
  expect(cd.seen).toEqual([A]);
  expect(cd.by[H(B)]).toMatchObject({ exit_code: 1, summary: { refused: "backlog_ops_bound_backfill_receipt" } });
  const ce = await step("backfill-catchup-execute", { ...since, BACKLOG_OPS_DRY_RUN: "703", BACKLOG_OPS_CONFIRMATION: `BACKFILL-CATCHUP-EXECUTE ${SHA} DRY 703 FULL 702 PER_ORG` },
    { dry: "backfill-catchup-dry", full: "backfill-execute" }, cu("execute"));
  expect(ce.seen).toEqual([A]);
  expect(ce.receipt.summary.ok_count).toBe(1);
  // export: dry is fine for both (it binds nothing); execute binds the FULL wave, so B refuses again.
  const ed = await step("export-dry", {}, {}, fine(exportOk("dry_run")));
  expect(ed.seen.sort()).toEqual(TWO);
  const ee = await step("export-execute", { BACKLOG_OPS_DRY_RUN: "705", BACKLOG_OPS_BACKFILL_RUN: "702", BACKLOG_OPS_CONFIRMATION: `EXPORT-EXECUTE ${SHA} DRY 705 BACKFILL 702 PER_ORG` },
    { dry: "export-dry", backfill: "backfill-execute" }, fine(exportOk("execute")));
  expect(ee.seen).toEqual([A]);
  expect(ee.peak).toBe(1); // the export's lease: one at a time
  // drain: BACKFILL the catch-up, FULL, EXPORT; A drains, B is refused.
  const dd = await step("drain-dry", {}, {}, fine({ mode: "dry_run", listed: 3 }));
  expect(dd.exit).toBe(0);
  const de = await step("drain-execute", { BACKLOG_OPS_DRY_RUN: "707", BACKLOG_OPS_BACKFILL_RUN: "704", BACKLOG_OPS_FULL_BACKFILL_RUN: "702", BACKLOG_OPS_EXPORT_RUN: "706",
    BACKLOG_OPS_REVISIONS_SINCE_MS: String(SINCE), BACKLOG_OPS_CONFIRMATION: `DRAIN-EXECUTE ${SHA} DRY 707 BACKFILL 704 FULL 702 EXPORT 706 PER_ORG` },
    { dry: "drain-dry", backfill: "backfill-catchup-execute", full: "backfill-execute", export: "export-execute" }, fine({ mode: "execute", listed: 3 }));
  expect(de.seen).toEqual([A]);
  expect(de.peak).toBe(1); // the drain's lease
  expect(de.by[H(A)]).toMatchObject({ exit_code: 0, summary: { mode: "execute", listed: 3 } });
  expect(de.by[H(B)]!.exit_code).toBe(1);
  expect(de.exit).toBe(0);
  // Every receipt and log line names no org.
  for (const r of [bd, full, cd, ce, ed, ee, dd, de]) {
    for (const o of TWO) { expect(r.file).not.toContain(o); expect(r.lines.filter((l) => !l.startsWith("::add-mask::")).join("\n")).not.toContain(o); }
  }
});

test("wave refusals: every org refused fails the run (no child for a refused binding); a wave run needs a scope", async () => {
  const saved = { ...receipts };
  try {
    const bad = await step("backfill-dry", {}, {}, () => ({ code: 1, stdout: JSON.stringify({ refused: "internal_error" }) }));
    expect(bad.exit).toBe(1);
    expect(bad.receipt.summary.ok_count).toBe(0);
    await expect(step("backfill-dry", {}, {}, () => ({ code: 0, stdout: "{}" }), { BACKLOG_OPS_ORGS: "" })).rejects.toMatchObject({ code: "backlog_ops_scope_required" });
    // A missing binding refuses each org on its own: no child runs and the run fails.
    const none = await step("backfill-execute", { BACKLOG_OPS_DRY_RUN: "701", BACKLOG_OPS_CONFIRMATION: `BACKFILL-EXECUTE ${SHA} DRY 701 PER_ORG` }, {}, () => ({ code: 0, stdout: "{}" }));
    expect(none.seen).toEqual([]);
    expect(none.exit).toBe(1);
    expect(Object.values(none.by).map((p: any) => p.summary.refused)).toEqual(["backlog_ops_bound_dry_missing", "backlog_ops_bound_dry_missing"]);
  } finally { Object.assign(receipts, saved); }
});

test("a marker binding a per-org FULL and catch-up: per-org checks, the failing org refused, one merged baseline, evidence over the exact bytes", async () => {
  const fine = (s: unknown) => () => ({ code: 0, stdout: JSON.stringify(s) });
  const AUTH = "709";
  // authority-dry (scope-level, non-wave) as the marker binds it.
  const unbound = (() => { const r = evidenceReport(TWO); return { ...r, status: "authority_evidence_unbound", orgs: r.orgs.map((o) => ({ ...o, ok: false, refusals: ["sql_unbound"] })) }; })();
  const home = JSON.parse(receipts["backfill-execute"]!).home;
  receipts["authority-dry"] = JSON.stringify({ contract: "backlog-ops-receipt/v1", mode: "authority-dry", sha: SHA, run: AUTH, scope: SCOPE_ID, ledger: "none", home, exitCode: 0,
    summary: { mode: "dry_run", evidence: toReceiptShape(unbound) } });
  // A catch-up wave WITH the baseline: each child records its own org's baseline (it was given only its org's evidence).
  const since = { BACKLOG_OPS_REVISIONS_SINCE_MS: String(SINCE), BACKLOG_OPS_FULL_BACKFILL_RUN: "702", BACKLOG_OPS_AUTHORITY_RUN: AUTH };
  const childBase = (o: string) => ({ code: 0, stdout: JSON.stringify(backfill("dry_run", { revisions_since_ms: SINCE, authority_baseline: toReceiptShape(baseline([o])) })) });
  const cd = await step("backfill-catchup-dry", since, { full: "backfill-execute", authority: "authority-dry" }, childBase);
  expect(cd.seen).toEqual([A]); // B's FULL slice failed
  const ce = await step("backfill-catchup-execute", { ...since, BACKLOG_OPS_DRY_RUN: "703", BACKLOG_OPS_CONFIRMATION: `BACKFILL-CATCHUP-EXECUTE ${SHA} DRY 703 FULL 702 AUTHORITY ${AUTH} PER_ORG` },
    { dry: "backfill-catchup-dry", full: "backfill-execute", authority: "authority-dry" },
    (o) => ({ code: 0, stdout: JSON.stringify(backfill("execute", { revisions_since_ms: SINCE, authority_baseline: toReceiptShape(baseline([o])) })) }));
  expect(ce.receipt).toMatchObject({ authority_run: AUTH, full_run: "702" });
  expect(ce.by[H(A)]!.exit_code).toBe(0);
  // reshard-dry (non-wave) after it.
  receipts["reshard-dry"] = JSON.stringify({ contract: "backlog-ops-receipt/v1", mode: "reshard-dry", sha: SHA, run: "710", scope: SCOPE_ID, ledger: "none", home, exitCode: 0,
    after_run: RUNS_CE, summary: { mode: "dry_run", tool: "reshard_verify", org_count: 2, verify_passed: true, per_org: TWO.map((o) => ({ org_hash: H(o), verify_passed: true,
      listed: 1, legacy_shards: 0, entries: 1, verified: 1, missing: 0, invalid: 0, failed: 0, merged: 0, would_merge: 0, shards: 0, refused: null })) } });
  const temp = mkdtempSync(join(tmpdir(), "wave-marker-"));
  cleanups.push(() => rmSync(temp, { recursive: true, force: true }));
  for (const [kind, m] of Object.entries({ authority: "authority-dry", backfill: "backfill-catchup-execute", full: "backfill-execute", reshard: "reshard-dry" })) {
    mkdirSync(join(temp, "backlog-ops-bound", kind), { recursive: true });
    writeFileSync(join(temp, "backlog-ops-bound", kind, "backlog-ops-receipt.json"), receipts[m as BacklogOpsMode]!);
  }
  const env = { ...target, ...SCOPE_ENV, RUNNER_TEMP: temp, BACKLOG_OPS_REVISIONS_SINCE_MS: String(SINCE) };
  const d = backlogOpsDispatch({ BACKLOG_OPS_MODE: "marker-dry", BACKLOG_OPS_SHA: SHA, BACKLOG_OPS_AUTHORITY_RUN: AUTH, BACKLOG_OPS_BACKFILL_RUN: "704",
    BACKLOG_OPS_FULL_BACKFILL_RUN: "702", BACKLOG_OPS_RESHARD_RUN: "710" });
  const b = markerWaveBound(env, d, SINCE);
  expect(b.marker!.refusedOrgs).toEqual([H(B)]);
  expect(b.marker!.reshardVerified).toEqual([H(A)]);
  expect(b.marker!.sections).toEqual({ fences: true, ownership: true, revisions: true });
  expect(b.marker!.catchupEvidence).toBe(createHash("sha256").update(readFileSync(join(temp, "backlog-ops-bound", "backfill", "backlog-ops-receipt.json"))).digest("hex"));
  expect((b.baseline as { orgs: { orgHash: string }[] }).orgs.map((o) => o.orgHash)).toEqual([H(A)]);
  // Every org failing its bindings refuses the run.
  writeFileSync(join(temp, "backlog-ops-bound", "reshard", "backlog-ops-receipt.json"), receipts["reshard-dry"]!.replace(`"after_run":"${RUNS_CE}"`, `"after_run":"999"`));
  expect(code(() => markerWaveBound(env, d, SINCE))).toBe("backlog_ops_wave_no_org");
});
const RUNS_CE = "704";

test("the approval names the run's shape: PER_ORG in the confirmation exactly when per_org is set", () => {
  const base = { BACKLOG_OPS_MODE: "backfill-execute", BACKLOG_OPS_SHA: SHA, BACKLOG_OPS_DRY_RUN: "701" };
  expect(backlogOpsDispatch({ ...base, BACKLOG_OPS_PER_ORG: "true", BACKLOG_OPS_CONFIRMATION: `BACKFILL-EXECUTE ${SHA} DRY 701 PER_ORG` }).perOrg).toBe(true);
  expect(code(() => backlogOpsDispatch({ ...base, BACKLOG_OPS_PER_ORG: "true", BACKLOG_OPS_CONFIRMATION: `BACKFILL-EXECUTE ${SHA} DRY 701` }))).toBe("backlog_ops_confirmation");
  expect(code(() => backlogOpsDispatch({ ...base, BACKLOG_OPS_CONFIRMATION: `BACKFILL-EXECUTE ${SHA} DRY 701 PER_ORG` }))).toBe("backlog_ops_confirmation");
  // Without per_org the confirmation is byte-identical to before.
  expect(backlogOpsDispatch({ ...base, BACKLOG_OPS_CONFIRMATION: `BACKFILL-EXECUTE ${SHA} DRY 701` }).confirmation).toBe(`BACKFILL-EXECUTE ${SHA} DRY 701`);
  // A dry run's confirmation stays empty either way.
  expect(code(() => backlogOpsDispatch({ BACKLOG_OPS_MODE: "backfill-dry", BACKLOG_OPS_SHA: SHA, BACKLOG_OPS_PER_ORG: "true", BACKLOG_OPS_CONFIRMATION: "PER_ORG" }))).toBe("backlog_ops_confirmation");
});

test("the drain's max_objects is a per-run cap: a wave splits it across its orgs (sum exact), and a budget below the org count refuses", async () => {
  const { waveBudgets } = await import("../../../scripts/backlog-ops.ts");
  expect(waveBudgets(null, TWO)).toBeNull();
  expect([...waveBudgets(5, TWO)!.values()]).toEqual([3, 2]);
  expect([...waveBudgets(1_000_000, ORGS)!.values()].reduce((a, b) => a + b, 0)).toBe(1_000_000);
  expect(code(() => waveBudgets(1, TWO))).toBe("backlog_ops_wave_max_objects");
  // End to end: each drain child gets its share as --max-objects, never the whole budget.
  const saved = { ...receipts };
  try {
    const shares: number[] = [];
    const temp = mkdtempSync(join(tmpdir(), "wave-budget-"));
    cleanups.push(() => rmSync(temp, { recursive: true, force: true }));
    const lines: string[] = [];
    const log = console.log;
    console.log = (...a: unknown[]) => { lines.push(a.join(" ")); };
    try {
      await backlogOpsRun({ ...target, ...SCOPE_ENV, BACKLOG_OPS_MODE: "drain-dry", BACKLOG_OPS_SHA: SHA, BACKLOG_OPS_PER_ORG: "true", BACKLOG_OPS_MAX_OBJECTS: "5",
        GITHUB_SHA: SHA, GITHUB_RUN_ID: "720", RUNNER_TEMP: temp, ACTIONS_ID_TOKEN_REQUEST_URL: "https://token.example/x", ACTIONS_ID_TOKEN_REQUEST_TOKEN: "bearer" },
        { fetcher, runChild: async (c) => { shares.push(Number(c.args[c.args.indexOf("--max-objects") + 1])); return { code: 0, stdout: JSON.stringify({ mode: "dry_run", listed: 1 }) }; } });
    } finally { console.log = log; }
    expect(shares.sort()).toEqual([2, 3]);
    expect(lines).toContain("backlog_ops_wave_result ok_count=2 org_count=2 complete");
    await expect(backlogOpsRun({ ...target, ...SCOPE_ENV, BACKLOG_OPS_MODE: "drain-dry", BACKLOG_OPS_SHA: SHA, BACKLOG_OPS_PER_ORG: "true", BACKLOG_OPS_MAX_OBJECTS: "1",
      GITHUB_SHA: SHA, GITHUB_RUN_ID: "721", RUNNER_TEMP: temp }, { fetcher, runChild: async () => ({ code: 0, stdout: "{}" }) })).rejects.toMatchObject({ code: "backlog_ops_wave_max_objects" });
  } finally { Object.assign(receipts, saved); }
});
