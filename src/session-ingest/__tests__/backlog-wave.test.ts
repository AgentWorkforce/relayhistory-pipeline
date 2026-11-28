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

/** step(), with the child's argv handed to `child`. */
const stepWithArgs = (mode: BacklogOpsMode, env: Record<string, string>, bind: Record<string, BacklogOpsMode>, child: (org: string, args: string[]) => { code: number; stdout: string },
  extra: Record<string, string> = {}) => step(mode, env, bind, child as never, extra, true);

/** One credential step: `bind` maps a binding kind to the mode whose persisted receipt it gets. */
async function step(mode: BacklogOpsMode, env: Record<string, string>, bind: Record<string, BacklogOpsMode>, child: (org: string) => { code: number; stdout: string },
  extra: Record<string, string> = {}, withArgs = false) {
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
        return withArgs ? (child as unknown as (o: string, a: string[]) => { code: number; stdout: string })(orgs[0]!, c.args) : child(orgs[0]!);
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
  // Any other scope is refused before slicing: another wave's, one org alone, unscoped, missing; wave or not.
  for (const sc of ["orgs_" + "f".repeat(32), orgScopeId(A), "all", undefined])
    for (const summary of [{}, waveSummary([A], [{ org: A, exitCode: 0, summary: {} }])])
      expect(code(() => waveView(JSON.stringify({ ...top, scope: sc, exitCode: 0, summary }), A, SCOPE_ID))).toBe("backlog_ops_wave_scope");
  // The single-org wave: its scope is that org's, exactly.
  expect(JSON.parse(waveView(JSON.stringify({ ...top, scope: orgScopeId(A), exitCode: 0, summary: {} }), A, orgScopeId(A))).scope).toBe(orgScopeId(A));
});

/** The --revisions-since-ms a child was given (null when none). */
const sinceArg = (args: string[]) => { const i = args.indexOf("--revisions-since-ms"); return i < 0 ? null : Number(args[i + 1]); };

test("an incomplete execute exits nonzero and keeps every slice; a retry runs only the outstanding orgs; the chain then completes with each org's own t", async () => {
  const fine = (s: unknown) => () => ({ code: 0, stdout: JSON.stringify(s) });
  const bd = await step("backfill-dry", {}, {}, fine(backfill("dry_run")));
  expect(bd.exit).toBe(0);
  expect(bd.receipt.summary).toMatchObject({ kind: "wave", org_count: 2, ok_count: 2 });
  expect(bd.peak).toBeLessThanOrEqual(2);
  // FULL: B fails transiently; A completes. Incomplete: exit 1, both slices kept.
  const fullEnv = { BACKLOG_OPS_DRY_RUN: "701", BACKLOG_OPS_CONFIRMATION: `BACKFILL-EXECUTE ${SHA} DRY 701 PER_ORG` };
  const full = await step("backfill-execute", fullEnv, { dry: "backfill-dry" },
    (o) => o === B ? { code: 1, stdout: JSON.stringify(backfill("execute", { sections: { fences: "complete", ownership: "complete", revisions: "incomplete" } })) }
      : { code: 0, stdout: JSON.stringify(backfill("execute")) });
  expect(full.exit).toBe(1);
  expect(full.receipt.exitCode).toBe(1);
  expect(full.by[H(A)]).toMatchObject({ exit_code: 0 });
  expect(full.by[H(B)]).toMatchObject({ exit_code: 1 });
  expect(full.receipt.bindings).toEqual({ dry: "701" });
  expect(full.lines).toContain("backlog_ops_wave_result ok_count=1 org_count=2 incomplete");
  // The retry (PREVIOUS 702): only B runs; A is carried with the previous run's creation time.
  const P = Date.now() - 60 * 60_000;
  const retry = await step("backfill-execute", { ...fullEnv, BACKLOG_OPS_PREVIOUS_RUN: "702", BACKLOG_OPS_CONFIRMATION: `BACKFILL-EXECUTE ${SHA} DRY 701 PREVIOUS 702 PER_ORG` },
    { dry: "backfill-dry", previous: "backfill-execute" }, fine(backfill("execute")), { GITHUB_RUN_ID: "712", BACKLOG_OPS_PREVIOUS_CREATED_MS: String(P) });
  expect(retry.seen).toEqual([B]);
  expect(retry.exit).toBe(0);
  expect(retry.by[H(A)]).toMatchObject({ exit_code: 0, carried_ms: P });
  expect(retry.by[H(B)]!.carried_ms).toBeUndefined();
  expect(retry.receipt).toMatchObject({ run: "712", previous_run: "702", bindings: { dry: "701" } });
  // catch-up: t from the gate for the retry FULL; A's FULL ran earlier, so A's child reads since A's own FULL.
  const T = Date.now() - 30 * 60_000;
  const echo = (mode: "dry_run" | "execute") => (o: string) => ({ code: 0, stdout: JSON.stringify(backfill(mode, { revisions_since_ms: givenSince.get(o) })) });
  const givenSince = new Map<string, number | null>();
  const sinceEnv = { BACKLOG_OPS_REVISIONS_SINCE_MS: String(T), BACKLOG_OPS_FULL_BACKFILL_RUN: "712" };
  const spy = (f: (o: string) => { code: number; stdout: string }) => f;
  const cdRun = async () => {
    const r = await stepWithArgs("backfill-catchup-dry", sinceEnv, { full: "backfill-execute" }, (o, args) => { givenSince.set(o, sinceArg(args)); return echo("dry_run")(o); });
    return r;
  };
  const cd = await cdRun();
  expect(givenSince.get(A)).toBe(P - 15 * 60_000);
  expect(givenSince.get(B)).toBe(T);
  expect(cd.exit).toBe(0);
  const ce = await stepWithArgs("backfill-catchup-execute", { ...sinceEnv, BACKLOG_OPS_DRY_RUN: "703", BACKLOG_OPS_CONFIRMATION: `BACKFILL-CATCHUP-EXECUTE ${SHA} DRY 703 FULL 712 PER_ORG` },
    { dry: "backfill-catchup-dry", full: "backfill-execute" }, (o, args) => { givenSince.set(o, sinceArg(args)); return echo("execute")(o); });
  expect(ce.exit).toBe(0);
  expect(ce.seen.sort()).toEqual(TWO);
  void spy;
  // export and drain, complete for both.
  const ed = await step("export-dry", {}, {}, fine(exportOk("dry_run")));
  const ee = await step("export-execute", { BACKLOG_OPS_DRY_RUN: "705", BACKLOG_OPS_BACKFILL_RUN: "712", BACKLOG_OPS_CONFIRMATION: `EXPORT-EXECUTE ${SHA} DRY 705 BACKFILL 712 PER_ORG` },
    { dry: "export-dry", backfill: "backfill-execute" }, fine(exportOk("execute")));
  expect([ed.exit, ee.exit, ee.peak]).toEqual([0, 0, 1]); // the export's lease: one at a time
  const dd = await step("drain-dry", {}, {}, fine({ mode: "dry_run", listed: 3 }));
  const de = await step("drain-execute", { BACKLOG_OPS_DRY_RUN: "707", BACKLOG_OPS_BACKFILL_RUN: "704", BACKLOG_OPS_FULL_BACKFILL_RUN: "712", BACKLOG_OPS_EXPORT_RUN: "706",
    BACKLOG_OPS_REVISIONS_SINCE_MS: String(T), BACKLOG_OPS_CONFIRMATION: `DRAIN-EXECUTE ${SHA} DRY 707 BACKFILL 704 FULL 712 EXPORT 706 PER_ORG` },
    { dry: "drain-dry", backfill: "backfill-catchup-execute", full: "backfill-execute", export: "export-execute" }, fine({ mode: "execute", listed: 3 }));
  expect([dd.exit, de.exit, de.peak]).toEqual([0, 0, 1]); // the drain's lease
  expect(de.seen.sort()).toEqual(TWO);
  for (const r of [bd, full, retry, cd, ce, ed, ee, dd, de])
    for (const o of TWO) { expect(r.file).not.toContain(o); expect(r.lines.filter((l) => !l.startsWith("::add-mask::")).join("\n")).not.toContain(o); }
});

test("independent refusals: an org refused at its binding gets no child; the execute is incomplete (nonzero) while the dry shows the eligible subset", async () => {
  const saved = { ...receipts };
  try {
    const bd = await step("backfill-dry", {}, {}, (o) => o === B ? { code: 1, stdout: JSON.stringify({ refused: "internal_error" }) } : { code: 0, stdout: JSON.stringify(backfill("dry_run")) });
    expect([bd.exit, bd.receipt.summary.ok_count]).toEqual([0, 1]);
    const fe = await step("backfill-execute", { BACKLOG_OPS_DRY_RUN: "701", BACKLOG_OPS_CONFIRMATION: `BACKFILL-EXECUTE ${SHA} DRY 701 PER_ORG` }, { dry: "backfill-dry" },
      () => ({ code: 0, stdout: JSON.stringify(backfill("execute")) }));
    expect(fe.seen).toEqual([A]);
    expect(fe.by[H(B)]!.exit_code).toBe(1);
    expect(fe.by[H(B)]!.summary.refused).toMatch(/^backlog_ops_bound_backfill_/u);
    expect([fe.exit, fe.receipt.exitCode]).toEqual([1, 1]);
  } finally { Object.assign(receipts, saved); }
});

test("retry negatives: the previous run must be this execute's, incomplete, same bindings, with the gate's creation time; carried slices age from it", async () => {
  const saved = { ...receipts };
  try {
    const fullEnv = { BACKLOG_OPS_DRY_RUN: "701", BACKLOG_OPS_CONFIRMATION: `BACKFILL-EXECUTE ${SHA} DRY 701 PER_ORG` };
    await step("backfill-dry", {}, {}, () => ({ code: 0, stdout: JSON.stringify(backfill("dry_run")) }));
    await step("backfill-execute", fullEnv, { dry: "backfill-dry" }, (o) => ({ code: o === B ? 1 : 0, stdout: JSON.stringify(backfill("execute")) }));
    const P = String(Date.now() - 60_000);
    const retryEnv = { ...fullEnv, BACKLOG_OPS_PREVIOUS_RUN: "702", BACKLOG_OPS_CONFIRMATION: `BACKFILL-EXECUTE ${SHA} DRY 701 PREVIOUS 702 PER_ORG` };
    const go = (extra: Record<string, string>, bind: Record<string, BacklogOpsMode> = { dry: "backfill-dry", previous: "backfill-execute" }) =>
      step("backfill-execute", retryEnv, bind, () => ({ code: 0, stdout: JSON.stringify(backfill("execute")) }), { GITHUB_RUN_ID: "713", ...extra }).then(() => null, (e) => e.code);
    expect(await go({})).toBe("backlog_ops_previous_created"); // no gate time
    expect(await go({ BACKLOG_OPS_PREVIOUS_CREATED_MS: P }, { dry: "backfill-dry" })).toBe("backlog_ops_bound_previous_missing");
    const prev = receipts["backfill-execute"]!;
    const edit = (f: (r: any) => void) => { const r = JSON.parse(prev); f(r); return JSON.stringify(r); };
    receipts["backfill-execute"] = edit((r) => { r.bindings = { dry: "799" }; });
    expect(await go({ BACKLOG_OPS_PREVIOUS_CREATED_MS: P })).toBe("backlog_ops_bound_previous_receipt"); // other bindings
    receipts["backfill-execute"] = edit((r) => { r.exitCode = 0; });
    expect(await go({ BACKLOG_OPS_PREVIOUS_CREATED_MS: P })).toBe("backlog_ops_bound_previous_receipt"); // a complete run is not retried
    receipts["backfill-execute"] = prev;
    // Dispatch: PREVIOUS only on a per-org execute, named in the confirmation.
    const base = { BACKLOG_OPS_MODE: "backfill-execute", BACKLOG_OPS_SHA: SHA, BACKLOG_OPS_DRY_RUN: "701" };
    expect(code(() => backlogOpsDispatch({ ...base, BACKLOG_OPS_PREVIOUS_RUN: "702", BACKLOG_OPS_CONFIRMATION: `BACKFILL-EXECUTE ${SHA} DRY 701 PREVIOUS 702` }))).toBe("backlog_ops_previous_run");
    expect(code(() => backlogOpsDispatch({ BACKLOG_OPS_MODE: "backfill-dry", BACKLOG_OPS_SHA: SHA, BACKLOG_OPS_PER_ORG: "true", BACKLOG_OPS_PREVIOUS_RUN: "702" }))).toBe("backlog_ops_previous_run");
    expect(code(() => backlogOpsDispatch({ ...base, BACKLOG_OPS_PER_ORG: "true", BACKLOG_OPS_PREVIOUS_RUN: "702", BACKLOG_OPS_CONFIRMATION: `BACKFILL-EXECUTE ${SHA} DRY 701 PER_ORG` }))).toBe("backlog_ops_confirmation");
    expect(code(() => backlogOpsDispatch({ ...base, BACKLOG_OPS_PER_ORG: "true", BACKLOG_OPS_PREVIOUS_RUN: "701", BACKLOG_OPS_CONFIRMATION: `BACKFILL-EXECUTE ${SHA} DRY 701 PREVIOUS 701 PER_ORG` }))).toBe("backlog_ops_bindings");
    // A carried slice older than the consumer's freshness rule refuses that org (here: the catch-up's FULL, 24 h).
    const stale = await step("backfill-execute", retryEnv, { dry: "backfill-dry", previous: "backfill-execute" }, () => ({ code: 0, stdout: JSON.stringify(backfill("execute")) }),
      { GITHUB_RUN_ID: "714", BACKLOG_OPS_PREVIOUS_CREATED_MS: String(Date.now() - 25 * 3600_000) });
    expect(stale.exit).toBe(0);
    const cd = await step("backfill-catchup-dry", { BACKLOG_OPS_REVISIONS_SINCE_MS: String(Date.now() - 3600_000), BACKLOG_OPS_FULL_BACKFILL_RUN: "714" },
      { full: "backfill-execute" }, () => ({ code: 0, stdout: JSON.stringify(backfill("dry_run", { revisions_since_ms: 1 })) }));
    expect(cd.by[H(A)]).toMatchObject({ summary: { refused: "backlog_ops_bound_full_carried" } });
  } finally { Object.assign(receipts, saved); }
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
  // Its own FULL (run 702), where B's org failed (the credential step's per-org mechanism; the gate would also refuse
  // binding a failed run, which is the stricter production case).
  await step("backfill-dry", {}, {}, fine(backfill("dry_run")));
  await step("backfill-execute", { BACKLOG_OPS_DRY_RUN: "701", BACKLOG_OPS_CONFIRMATION: `BACKFILL-EXECUTE ${SHA} DRY 701 PER_ORG` }, { dry: "backfill-dry" },
    (o) => ({ code: o === B ? 1 : 0, stdout: JSON.stringify(backfill("execute")) }));
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

test("gate: previous_run is this execute at this sha, FAILED, first attempt, by the dispatcher, after every binding, recent; its creation goes out as previous_created_ms", async () => {
  const { backlogOpsGithubGate } = await import("../../../scripts/backlog-ops-github.ts");
  const { PRIVATE_INGEST_RELEASE } = await import("../private-ingest-release.ts");
  const WF = ".github/workflows/backlog-ops.yml";
  const actor = { id: P.dispatcher.id, login: P.dispatcher.login, type: "User" };
  const hoursAgo = (h: number) => new Date(Date.now() - h * 3600_000).toISOString();
  const runOf = (id: string, mode: string, h: number, conclusion = "success") => ({ id: Number(id), head_sha: SHA, head_branch: "main", event: "workflow_dispatch", path: WF,
    run_attempt: 1, status: "completed", conclusion, actor: { ...actor }, triggering_actor: { ...actor }, display_title: `backlog ops ${mode} ${SHA}`, created_at: hoursAgo(h) });
  const fx = () => ({ bound: { "701": runOf("701", "backfill-dry", 3), "702": runOf("702", "backfill-execute", 1, "failure") } as Record<string, any> });
  const get = (f: ReturnType<typeof fx>) => async (p: string): Promise<any> => {
    if (p.endsWith("/git/ref/heads/main")) return { object: { sha: SHA } };
    if (p.endsWith("/actions/runs/100")) return { id: 100, head_sha: SHA, head_branch: "main", event: "workflow_dispatch", path: WF, run_attempt: 1, actor, triggering_actor: { ...actor },
      display_title: `backlog ops backfill-execute ${SHA}` };
    for (const id of Object.keys(f.bound)) {
      if (p.endsWith(`/actions/runs/${id}`)) return f.bound[id];
      if (p.endsWith(`/actions/runs/${id}/artifacts?per_page=100`)) return { total_count: 1, artifacts: [{ name: "backlog-ops-receipt", expired: false }] };
    }
    if (p.includes("deployment-branch-policies")) return { total_count: 1, branch_policies: [{ name: "main", type: "branch" }] };
    if (p.includes("/environments/")) return { id: 7, name: "session-backlog-execute", protection_rules: [{ type: "branch_policy" }], deployment_branch_policy: { protected_branches: false, custom_branch_policies: true } };
    if (p.includes("/actions/workflows/")) return { workflow_runs: [{ id: 10, head_sha: SHA, head_branch: "main", event: "push", status: "completed", conclusion: "success", run_attempt: 1 }] };
    if (p.includes("/runs/10/")) return { total_count: 5, jobs: ["checks", "session-read-worker / worker", "real PostgreSQL 15", "real PostgreSQL 16", "real PostgreSQL 17"]
      .map((name) => ({ name, head_sha: SHA, status: "completed", conclusion: "success" })) };
    throw new Error("unexpected " + p);
  };
  const env = { GITHUB_REPOSITORY: P.repository, GITHUB_REPOSITORY_ID: PRIVATE_INGEST_RELEASE.repositoryId, GITHUB_EVENT_NAME: "workflow_dispatch", GITHUB_REF: "refs/heads/main",
    GITHUB_SHA: SHA, GITHUB_RUN_ATTEMPT: "1", GITHUB_RUN_ID: "100", BACKLOG_OPS_MODE: "backfill-execute", BACKLOG_OPS_SHA: SHA, BACKLOG_OPS_DRY_RUN: "701",
    BACKLOG_OPS_PER_ORG: "true", BACKLOG_OPS_PREVIOUS_RUN: "702", BACKLOG_OPS_CONFIRMATION: `BACKFILL-EXECUTE ${SHA} DRY 701 PREVIOUS 702 PER_ORG` };
  const gate = async (m: (f: ReturnType<typeof fx>) => void = () => {}) => {
    const f = fx(); m(f);
    try { return await backlogOpsGithubGate(env, get(f) as never, false, { enabled: true }); } catch (e) { return (e as { code?: string }).code; }
  };
  const ok = await gate();
  expect(typeof ok === "string" ? ok : "object").toBe("object"); // (a refusal code here names what failed)
  const pc = (ok as { previousCreatedMs: number }).previousCreatedMs;
  expect(Math.abs(pc - (Date.now() - 3600_000))).toBeLessThan(60_000);
  expect(await gate((f) => { f.bound["702"].conclusion = "success"; })).toBe("backlog_ops_gate_previous_run"); // a complete run is not retried
  expect(await gate((f) => { f.bound["702"].display_title = `backlog ops backfill-dry ${SHA}`; })).toBe("backlog_ops_gate_previous_run");
  expect(await gate((f) => { f.bound["702"].created_at = hoursAgo(3.5); f.bound["701"].created_at = hoursAgo(4); })).toBe("backlog_ops_gate_previous_run"); // too old
  expect(await gate((f) => { f.bound["702"].created_at = hoursAgo(3.2); })).toBe("backlog_ops_gate_previous_run"); // before its DRY
  expect(await gate((f) => { f.bound["702"].run_attempt = 2; })).toBe("backlog_ops_gate_previous_run");
  expect(await gate((f) => { f.bound["702"].head_sha = "2".repeat(40); })).toBe("backlog_ops_gate_previous_run");
});

test("consumer hardening: a carried slice only with previous_run, and never after the receipt was written", () => {
  const slice = (s: unknown) => s;
  const W = Date.now();
  const w = waveSummary(TWO, TWO.map((o) => ({ org: o, exitCode: 0, summary: { listed: 1 }, ...(o === A ? { carriedMs: W - 1000 } : {}) })));
  expect(waveReceiptSummary(w, TWO, slice, { previousRun: "702", writtenMs: W }).ok).toBe(true);
  expect(waveReceiptSummary(w, TWO, slice, { previousRun: null, writtenMs: W }).ok).toBe(false);
  expect(waveReceiptSummary(w, TWO, slice).ok).toBe(false);
  expect(waveReceiptSummary(w, TWO, slice, { previousRun: "702", writtenMs: W - 2000 }).ok).toBe(false);
  const top = { contract: "backlog-ops-receipt/v1", mode: "backfill-execute", sha: SHA, run: "712", scope: SCOPE_ID, ledger: "none", exitCode: 0, summary: w };
  expect(JSON.parse(waveView(JSON.stringify({ ...top, previous_run: "702", written_ms: W }), A, SCOPE_ID)).carried_ms).toBe(W - 1000);
  expect(code(() => waveView(JSON.stringify({ ...top, written_ms: W }), A, SCOPE_ID))).toBe("backlog_ops_wave_view");
  expect(code(() => waveView(JSON.stringify({ ...top, previous_run: "702", written_ms: W - 5000 }), A, SCOPE_ID))).toBe("backlog_ops_wave_view");
  expect(code(() => waveView(JSON.stringify({ ...top, previous_run: "702" }), A, SCOPE_ID))).toBe("backlog_ops_wave_view");
  // A non-carried slice needs neither.
  expect(JSON.parse(waveView(JSON.stringify(top), B, SCOPE_ID)).carried_ms).toBeUndefined();
});

test("credential step: evidence scoped to A alone never feeds an A+B wave (neither child executes); the exact-wave and single-org paths still bind", async () => {
  const saved = { ...receipts };
  try {
    const ok = () => ({ code: 0, stdout: JSON.stringify(backfill("dry_run")) });
    // A single-org (A only) wave: its own receipts, scope orgs_<A>.
    const single = await step("backfill-dry", {}, {}, ok, { BACKLOG_OPS_ORGS: A });
    expect(single.receipt.scope).toBe(orgScopeId(A));
    expect(single.exit).toBe(0);
    // An A+B execute bound to that A-only dry: refused for both orgs, no child runs.
    const ex = await step("backfill-execute", { BACKLOG_OPS_DRY_RUN: "701", BACKLOG_OPS_CONFIRMATION: `BACKFILL-EXECUTE ${SHA} DRY 701 PER_ORG` }, { dry: "backfill-dry" },
      () => ({ code: 0, stdout: JSON.stringify(backfill("execute")) }));
    expect(ex.seen).toEqual([]);
    expect(Object.values(ex.by).map((p: any) => p.summary.refused)).toEqual(["backlog_ops_wave_scope", "backlog_ops_wave_scope"]);
    expect(ex.exit).toBe(1);
    // A non-wave receipt scoped to A alone, bound by an A+B wave: the same.
    const nonWave = JSON.parse(single.file);
    nonWave.summary = backfill("dry_run");
    receipts["backfill-dry"] = JSON.stringify(nonWave);
    const ex2 = await step("backfill-execute", { BACKLOG_OPS_DRY_RUN: "701", BACKLOG_OPS_CONFIRMATION: `BACKFILL-EXECUTE ${SHA} DRY 701 PER_ORG` }, { dry: "backfill-dry" },
      () => ({ code: 0, stdout: JSON.stringify(backfill("execute")) }));
    expect(ex2.seen).toEqual([]);
    expect(ex2.exit).toBe(1);
    // The single-org path: an A-only execute binds the A-only dry and runs A.
    receipts["backfill-dry"] = single.file;
    const ex3 = await step("backfill-execute", { BACKLOG_OPS_DRY_RUN: "701", BACKLOG_OPS_CONFIRMATION: `BACKFILL-EXECUTE ${SHA} DRY 701 PER_ORG` }, { dry: "backfill-dry" },
      () => ({ code: 0, stdout: JSON.stringify(backfill("execute")) }), { BACKLOG_OPS_ORGS: A });
    expect([ex3.seen, ex3.exit]).toEqual([[A], 0]);
    // The exact-wave path (A+B dry bound by an A+B execute) is the chain test above.
  } finally { Object.assign(receipts, saved); }
});
