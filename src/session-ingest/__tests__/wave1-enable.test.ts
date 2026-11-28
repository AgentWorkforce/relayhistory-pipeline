/**
 * The wave-1 enable (backlog-ops.ts): the marker and hold executes run only for a run whose secret scope is EXACTLY
 * the reviewed two-org-hash pair (Pipeline 7c3ea742b1fa / d2ffe3631e70), never ids. The lists' shape, exact-set
 * matching (singleton, duplicate, substituted, extra and unset scopes refuse), the dispatch hold, the credential step's
 * refusal before any credential, the tools' own check, and the dry's `enable_scope` record.
 */
import { afterEach, expect, test } from "bun:test";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { evidenceHash } from "../authority-evidence.ts";
import {
  BACKLOG_OPS_HOLD_ENABLED_ORG_HASHES, BACKLOG_OPS_MARKER_ENABLED_ORG_HASHES, backlogOpsDispatch, enabledListsAgree, enabledScope, requireScopeEnabled,
  validEnabledOrgHashes,
} from "../backlog-ops.ts";
import { backlogOpsRun } from "../../../scripts/backlog-ops.ts";

const cleanups: (() => void)[] = [];
afterEach(() => { for (const c of cleanups.splice(0)) c(); });
const code = (fn: () => unknown) => { try { fn(); return null; } catch (e) { return (e as { code?: string }).code ?? String(e); } };
const SHA = "1".repeat(40);
const A = "5e5e5e5e-0000-4000-8000-00000000000a", B = "5e5e5e5e-0000-4000-8000-00000000000b", C = "5e5e5e5e-0000-4000-8000-00000000000c";
const H = (o: string) => evidenceHash("org", o);
const PAIR = [H(A), H(B)];

test("the lists: empty, or exactly two distinct org hashes; the hold and marker lists are the same pair", () => {
  expect(validEnabledOrgHashes([])).toBe(true);
  expect(validEnabledOrgHashes(PAIR)).toBe(true);
  for (const bad of [[H(A)], [H(A), H(B), H(C)], [H(A), H(A)], [A, B], ["h_" + "z".repeat(32), H(B)], ["H_" + "a".repeat(32), H(B)]])
    expect([bad, validEnabledOrgHashes(bad)]).toEqual([bad, false]);
  expect(enabledListsAgree([], [])).toBe(true);
  expect(enabledListsAgree(PAIR, [...PAIR].reverse())).toBe(true);
  expect(enabledListsAgree(PAIR, [])).toBe(false);
  expect(enabledListsAgree(PAIR, [H(A), H(C)])).toBe(false);
  // The shipped lists are well formed and agree; and they carry hashes only, never an org id.
  expect(validEnabledOrgHashes(BACKLOG_OPS_MARKER_ENABLED_ORG_HASHES) && validEnabledOrgHashes(BACKLOG_OPS_HOLD_ENABLED_ORG_HASHES)).toBe(true);
  expect(enabledListsAgree(BACKLOG_OPS_MARKER_ENABLED_ORG_HASHES, BACKLOG_OPS_HOLD_ENABLED_ORG_HASHES)).toBe(true);
  const src = readFileSync(new URL("../backlog-ops.ts", import.meta.url), "utf8");
  expect(src).not.toMatch(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/u);
});

test("exact-set scope: only the pair itself matches; a singleton, substituted, extra, repeated or unset scope does not; an empty list is disabled", () => {
  expect(enabledScope(PAIR, PAIR)).toBe("match");
  expect(enabledScope([...PAIR].reverse(), PAIR)).toBe("match");
  for (const [name, scope] of [["singleton", [H(A)]], ["substituted", [H(A), H(C)]], ["extra", [H(A), H(B), H(C)]], ["repeated", [H(A), H(A)]], ["unset", null], ["empty", []]] as const)
    expect([name, enabledScope(scope as never, PAIR)]).toEqual([name, "mismatch"]);
  expect(enabledScope(PAIR, [])).toBe("disabled");
  expect(enabledScope(PAIR, [H(A)])).toBe("mismatch"); // a malformed list never matches
  // requireScopeEnabled: active agreeing lists AND an exact match; there is no switch path.
  const L = { marker: PAIR, hold: PAIR };
  expect(code(() => requireScopeEnabled(PAIR, L, "marker", "x_refused"))).toBeNull();
  expect(code(() => requireScopeEnabled(PAIR, L, "hold", "x_refused"))).toBeNull();
  expect(code(() => requireScopeEnabled(PAIR, { marker: [], hold: [] }, "marker", "x_refused"))).toBe("x_refused");
  expect(code(() => requireScopeEnabled(PAIR, { marker: PAIR, hold: [] }, "marker", "x_refused"))).toBe("x_refused"); // lists disagree
  expect(code(() => requireScopeEnabled(PAIR, { marker: PAIR, hold: [H(A), H(C)] }, "hold", "x_refused"))).toBe("x_refused");
  for (const scope of [[H(A)], [H(A), H(C)], [H(A), H(B), H(C)], null])
    expect(code(() => requireScopeEnabled(scope, L, "marker", "x_refused"))).toBe("x_refused");
});

test("dispatch: the executes stay held while the shipped list is empty; a non-empty well-formed list passes the dispatch (the scope is checked later)", () => {
  const marker = { BACKLOG_OPS_MODE: "marker-execute", BACKLOG_OPS_SHA: SHA, BACKLOG_OPS_DRY_RUN: "605", BACKLOG_OPS_AUTHORITY_RUN: "601", BACKLOG_OPS_BACKFILL_RUN: "602",
    BACKLOG_OPS_FULL_BACKFILL_RUN: "603", BACKLOG_OPS_RESHARD_RUN: "604", BACKLOG_OPS_WAVE_ID: "h_" + "7".repeat(32),
    BACKLOG_OPS_CONFIRMATION: `MARKER-EXECUTE ${SHA} DRY 605 BACKFILL 602 FULL 603 RESHARD 604 AUTHORITY 601 WAVE h_${"7".repeat(32)}` };
  const hold = { BACKLOG_OPS_MODE: "hold-install-execute", BACKLOG_OPS_SHA: SHA, BACKLOG_OPS_DRY_RUN: "801", BACKLOG_OPS_CONFIRMATION: `HOLD-INSTALL-EXECUTE ${SHA} DRY 801` };
  const d = (env: Record<string, string>, m: readonly string[], h: readonly string[]) => code(() => backlogOpsDispatch(env, true, false, false, false, false, { marker: m, hold: h }));
  expect(d(marker, [], [])).toBe("backlog_ops_marker_held");
  expect(d(hold, [], [])).toBe("backlog_ops_hold_held");
  expect(d(marker, PAIR, PAIR)).toBeNull();
  expect(d(hold, PAIR, PAIR)).toBeNull();
  expect(d(marker, [H(A)], [H(A)])).toBe("backlog_ops_marker_held"); // malformed (one hash)
  expect(d(marker, PAIR, [])).toBe("backlog_ops_marker_held"); // the lists disagree
  // The legacy switches no longer enable anything: all on, lists empty: still held.
  expect(code(() => backlogOpsDispatch(marker, true, true, true, true, true, { marker: [], hold: [] }))).toBe("backlog_ops_marker_held");
  expect(code(() => backlogOpsDispatch(hold, true, true, true, true, true, { marker: [], hold: [] }))).toBe("backlog_ops_hold_held");
  // hold-dry and marker-dry need no enable.
  expect(code(() => backlogOpsDispatch({ BACKLOG_OPS_MODE: "hold-dry", BACKLOG_OPS_SHA: SHA }))).toBeNull();
});

test("the credential step: a hold or marker execute outside the enabled pair is refused BEFORE any credential; hold-dry records enable_scope", async () => {
  const temp = mkdtempSync(join(tmpdir(), "wave1-enable-"));
  cleanups.push(() => rmSync(temp, { recursive: true, force: true }));
  const target = {
    BACKLOG_OPS_BUCKET: "96bulls-aws-sessions-prod-transcripts-000000000000", BACKLOG_OPS_REGION: "us-east-1",
    BACKLOG_OPS_KMS_KEY_ARN: "arn:aws:kms:us-east-1:000000000000:key/00000000-0000-4000-8000-000000000000",
    BACKLOG_OPS_DRY_ROLE_ARN: "arn:aws:iam::000000000000:role/dry", BACKLOG_OPS_EXECUTE_ROLE_ARN: "arn:aws:iam::000000000000:role/exec",
  };
  let fetched = 0;
  const fetcher = (async (url: string) => {
    fetched++;
    if (url.startsWith("https://token.example/")) return new Response(JSON.stringify({ value: "a.b.c" }));
    return new Response(`<AssumeRoleWithWebIdentityResponse><Credentials><AccessKeyId>ASIAAAAAAAAAAAAAAAAA</AccessKeyId><SecretAccessKey>s</SecretAccessKey>
      <SessionToken>t</SessionToken><Expiration>${new Date(Date.now() + 6 * 3600_000).toISOString()}</Expiration></Credentials></AssumeRoleWithWebIdentityResponse>`);
  }) as unknown as typeof fetch;
  const { SCHEMA_RELEASE: P } = await import("../../hoopdb/schema-release-contract.ts");
  const ownerUrl = `postgresql://${P.owner}.${P.project}:p%40ss@${P.host}:${P.port}/${P.database}?sslmode=verify-full`;
  const base = { ...target, BACKLOG_OPS_SHA: SHA, GITHUB_SHA: SHA, GITHUB_RUN_ID: "900", RUNNER_TEMP: temp, SUPABASE_MIGRATION_OWNER_URL: ownerUrl,
    ACTIONS_ID_TOKEN_REQUEST_URL: "https://token.example/x", ACTIONS_ID_TOKEN_REQUEST_TOKEN: "bearer" };
  // hold-dry over A+B: the child's summary is irrelevant here; the receipt records how the scope stands.
  const lines: string[] = [];
  const log = console.log;
  console.log = (...a: unknown[]) => { lines.push(a.join(" ")); };
  try {
    await backlogOpsRun({ ...base, BACKLOG_OPS_MODE: "hold-dry", BACKLOG_OPS_ORGS: [A, B].join(",") }, { fetcher, runChild: async () => ({ code: 1, stdout: JSON.stringify({ refused: "internal_error" }) }) });
  } finally { console.log = log; }
  const receipt = JSON.parse(lines.find((l) => l.startsWith("backlog_ops_receipt "))!.slice("backlog_ops_receipt ".length));
  const expected = enabledScope([A, B].map(H), BACKLOG_OPS_HOLD_ENABLED_ORG_HASHES);
  expect(receipt.enable_scope).toBe(expected);
  expect(JSON.stringify(receipt)).not.toContain(A);
  // A hold execute: with the shipped (empty) lists the dispatch already refuses; the credential step's own check is the
  // shared requireScopeEnabled, exercised above. Either way no credential was read for it.
  const before = fetched;
  await expect(backlogOpsRun({ ...base, BACKLOG_OPS_MODE: "hold-install-execute", BACKLOG_OPS_ORGS: [A, C].join(","), BACKLOG_OPS_DRY_RUN: "801",
    BACKLOG_OPS_CONFIRMATION: `HOLD-INSTALL-EXECUTE ${SHA} DRY 801` }, { fetcher })).rejects.toMatchObject({ code: expect.stringMatching(/^backlog_ops_hold_(held|scope_not_enabled)$/u) });
  expect(fetched).toBe(before);
});

const SYNTH = { marker: PAIR, hold: PAIR }; // a SYNTHETIC reviewed pair (A, B), tests only

test("both real CLIs with a populated (synthetic) pair: the exact pair passes the enable and moves on; any other --org set is held, naming no org", async () => {
  const marker = await import("../../../scripts/backlog-marker.ts");
  const hold = await import("../../../scripts/backlog-hold.ts");
  const margs = (orgs: string[]) => ["execute", "--bucket", "transcripts", "--region", "us-east-1", "--sha", SHA, "--authority-run", "1", "--backfill-run", "2",
    "--full-run", "3", "--reshard-run", "4", "--kms-key-arn", "arn:aws:kms:us-east-1:000000000000:key/00000000-0000-4000-8000-000000000000", ...orgs.flatMap((o) => ["--org", o])];
  const hargs = (orgs: string[]) => ["install", "--sha", SHA, "--dry-run", "801", ...orgs.flatMap((o) => ["--org", o])];
  const mcode = async (orgs: string[], lists = SYNTH) => { try { await marker.main(margs(orgs), {}, { stdin: () => "not json", enabledOrgHashes: lists }); return null; } catch (e) { return (e as { code?: string }).code; } };
  const hcode = async (orgs: string[], lists = SYNTH) => { try { await hold.main(hargs(orgs), { stdin: () => "not json", enabledOrgHashes: lists }); return null; } catch (e) { return (e as { code?: string }).code; } };
  // The exact pair passes the enable; the next refusal is the stdin parse (nothing past the enable was reached before).
  expect(await mcode([A, B])).toBe("stdin_invalid");
  expect(await hcode([A, B])).toBe("stdin_invalid");
  for (const orgs of [[A], [A, C], [A, B, C]]) {
    expect([orgs.length, await mcode(orgs)]).toEqual([orgs.length, "marker_execute_held"]);
    expect([orgs.length, await hcode(orgs)]).toEqual([orgs.length, "hold_execute_held"]);
  }
  // Production lists (empty): held for the pair too.
  expect(await mcode([A, B], { marker: [], hold: [] })).toBe("marker_execute_held");
  expect(await hcode([A, B], { marker: [], hold: [] })).toBe("hold_execute_held");
});

test("the scope-check step: given only the inputs and the scope secret, it refuses a mismatched scope and passes the exact pair; other modes pass through", async () => {
  const { backlogOpsScopeCheck } = await import("../../../scripts/backlog-ops.ts");
  const hold = { BACKLOG_OPS_MODE: "hold-install-execute", BACKLOG_OPS_SHA: SHA, BACKLOG_OPS_DRY_RUN: "801", BACKLOG_OPS_CONFIRMATION: `HOLD-INSTALL-EXECUTE ${SHA} DRY 801` };
  expect(backlogOpsScopeCheck({ ...hold, BACKLOG_OPS_ORGS: [A, B].join(",") }, SYNTH)).toBe("checked");
  for (const orgs of ["", A, [A, C].join(","), [A, B, C].join(",")])
    expect([orgs.length, code(() => backlogOpsScopeCheck({ ...hold, BACKLOG_OPS_ORGS: orgs }, SYNTH))]).toEqual([orgs.length, "backlog_ops_hold_scope_not_enabled"]);
  expect(code(() => backlogOpsScopeCheck({ ...hold, BACKLOG_OPS_ORGS: [A, B].join(",") }))).toBe("backlog_ops_hold_held"); // shipped lists empty
  expect(backlogOpsScopeCheck({ BACKLOG_OPS_MODE: "hold-dry", BACKLOG_OPS_SHA: SHA, BACKLOG_OPS_ORGS: A })).toBe("not_applicable");
  // The workflow runs it before every step that holds another secret, with the scope secret alone.
  const yml = readFileSync(new URL("../../../.github/workflows/backlog-ops.yml", import.meta.url), "utf8");
  const at = yml.indexOf("- name: Check the wave-1 enabled scope"), recheck = yml.indexOf("- name: Recheck live gates"), tool = yml.indexOf("- name: Run the backlog tool");
  expect(at > 0 && at < recheck && recheck < tool).toBe(true);
  const block = yml.slice(at, recheck);
  expect(block).toContain("run: bun scripts/backlog-ops-scope-check.ts");
  expect(block.match(/secrets\.[A-Z_]+/gu)).toEqual(["secrets.BACKLOG_OPS_ORGS"]);
});

test("hold-dry with a populated (synthetic) pair records enable_scope match for the exact scope and mismatch otherwise (marker-dry: marker-publication.test.ts)", async () => {
  const temp = mkdtempSync(join(tmpdir(), "wave1-mdry-"));
  cleanups.push(() => rmSync(temp, { recursive: true, force: true }));
  const { backlogOpsRun: run } = await import("../../../scripts/backlog-ops.ts");
  const target = { BACKLOG_OPS_BUCKET: "96bulls-aws-sessions-prod-transcripts-000000000000", BACKLOG_OPS_REGION: "us-east-1",
    BACKLOG_OPS_KMS_KEY_ARN: "arn:aws:kms:us-east-1:000000000000:key/00000000-0000-4000-8000-000000000000",
    BACKLOG_OPS_DRY_ROLE_ARN: "arn:aws:iam::000000000000:role/dry", BACKLOG_OPS_EXECUTE_ROLE_ARN: "arn:aws:iam::000000000000:role/exec" };
  const { SCHEMA_RELEASE: P } = await import("../../hoopdb/schema-release-contract.ts");
  const fetcher = (async (url: string) => url.startsWith("https://token.example/") ? new Response(JSON.stringify({ value: "a.b.c" }))
    : new Response(`<AssumeRoleWithWebIdentityResponse><Credentials><AccessKeyId>ASIAAAAAAAAAAAAAAAAA</AccessKeyId><SecretAccessKey>s</SecretAccessKey>
      <SessionToken>t</SessionToken><Expiration>${new Date(Date.now() + 6 * 3600_000).toISOString()}</Expiration></Credentials></AssumeRoleWithWebIdentityResponse>`)) as unknown as typeof fetch;
  const runOnce = async (orgs: string[], dir: string) => {
    const lines: string[] = [];
    const log = console.log;
    console.log = (...a: unknown[]) => { lines.push(a.join(" ")); };
    try {
      // hold-dry: no bindings needed; the same receipt path as marker-dry for enable_scope.
      await run({ ...target, BACKLOG_OPS_MODE: "hold-dry", BACKLOG_OPS_SHA: SHA, BACKLOG_OPS_ORGS: orgs.join(","), GITHUB_SHA: SHA, GITHUB_RUN_ID: "901", RUNNER_TEMP: dir,
        SUPABASE_MIGRATION_OWNER_URL: `postgresql://${P.owner}.${P.project}:p%40ss@${P.host}:${P.port}/${P.database}?sslmode=verify-full`,
        ACTIONS_ID_TOKEN_REQUEST_URL: "https://token.example/x", ACTIONS_ID_TOKEN_REQUEST_TOKEN: "bearer" },
        { fetcher, runChild: async () => ({ code: 1, stdout: JSON.stringify({ refused: "internal_error" }) }), enabledOrgHashes: SYNTH });
    } finally { console.log = log; }
    return JSON.parse(lines.find((l) => l.startsWith("backlog_ops_receipt "))!.slice("backlog_ops_receipt ".length));
  };
  expect((await runOnce([A, B], join(temp, "1"))).enable_scope).toBe("match");
  expect((await runOnce([A, C], join(temp, "2"))).enable_scope).toBe("mismatch");
  expect((await runOnce([A], join(temp, "3"))).enable_scope).toBe("mismatch");
});
