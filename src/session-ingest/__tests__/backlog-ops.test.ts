/**
 * Contract, GitHub gate, credential step and workflow shape for the guarded backlog-ops workflow
 * (src/session-ingest/backlog-ops.ts, scripts/backlog-ops-github.ts, scripts/backlog-ops.ts,
 * .github/workflows/backlog-ops.yml), in the style of the 058000 prebuild gate tests. GitHub and STS are mocked;
 * nothing reaches the network or a database.
 */
import { expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { backlogOpsGithubGate } from "../../../scripts/backlog-ops-github.ts";
import {
  BACKLOG_OPS_CHILD_ENV,
  assumeRole,
  backlogOpsChildSpawn,
  checkBoundReceipts,
  toolDbUrl,
} from "../../../scripts/backlog-ops.ts";
import { SCHEMA_RELEASE as P } from "../../hoopdb/schema-release-contract.ts";
import { PRIVATE_INGEST_RELEASE } from "../private-ingest-release.ts";
import {
  BACKLOG_OPS_DRY_ENVIRONMENT,
  BACKLOG_OPS_EXECUTE_ENABLED,
  BACKLOG_OPS_EXECUTE_ENVIRONMENT,
  BACKLOG_OPS_MIN_SESSION_REMAINING_MS,
  BACKLOG_OPS_MODES,
  BACKLOG_OPS_RECEIPT_ARTIFACT,
  BACKLOG_OPS_RECEIPT_CONTRACT,
  BACKLOG_OPS_WORKFLOW,
  backlogOpsConfirmation,
  backlogOpsDispatch,
  backlogOpsEnvironment,
  backlogOpsReceipt,
  backlogOpsRole,
  backlogOpsRunName,
  backlogOpsScope,
  backlogOpsTarget,
  backlogOpsToolArgs,
  checkBoundReceipt,
  checkStsExpiration,
  countsOnly,
  parseStsCredentials,
  type BacklogOpsMode,
} from "../backlog-ops.ts";

const sha = "1".repeat(40), DRY = "111", BACKFILL = "222", EXPORT = "333";
const code = async (fn: () => unknown) => {
  try { await fn(); return null; } catch (e) { return (e as { code?: string }).code ?? String(e); }
};
const codeSync = (fn: () => unknown) => {
  try { fn(); return null; } catch (e) { return (e as { code?: string }).code ?? String(e); }
};

const target = {
  BACKLOG_OPS_BUCKET: "96bulls-aws-sessions-prod-transcripts-000000000000",
  BACKLOG_OPS_REGION: "us-east-1",
  BACKLOG_OPS_KMS_KEY_ARN: "arn:aws:kms:us-east-1:000000000000:key/00000000-0000-4000-8000-000000000000",
  BACKLOG_OPS_DRY_ROLE_ARN: "arn:aws:iam::000000000000:role/96bulls-aws-sessions-prod-backlog-ops-read",
  BACKLOG_OPS_EXECUTE_ROLE_ARN: "arn:aws:iam::000000000000:role/96bulls-aws-sessions-prod-backlog-ops-write",
};
const dispatchEnv = (mode: BacklogOpsMode, extra: Record<string, string> = {}) => {
  const tool = mode.split("-")[0];
  const exec = mode.endsWith("-execute");
  const d = {
    BACKLOG_OPS_MODE: mode, BACKLOG_OPS_SHA: sha,
    BACKLOG_OPS_DRY_RUN: exec ? DRY : "",
    BACKLOG_OPS_BACKFILL_RUN: exec && tool !== "backfill" ? BACKFILL : "",
    BACKLOG_OPS_EXPORT_RUN: exec && tool === "drain" ? EXPORT : "",
  };
  const confirmation = exec ? backlogOpsConfirmation({ mode, sha, dryRun: d.BACKLOG_OPS_DRY_RUN,
    backfillRun: d.BACKLOG_OPS_BACKFILL_RUN || null, exportRun: d.BACKLOG_OPS_EXPORT_RUN || null }) : "";
  return { ...d, BACKLOG_OPS_CONFIRMATION: confirmation, ...extra };
};

// ---- dispatch ----

test("seven modes (presence dry only); executes are dormant and refuse before anything else while disabled", () => {
  expect([...BACKLOG_OPS_MODES]).toEqual(["backfill-dry", "backfill-execute", "export-dry", "export-execute", "drain-dry", "drain-execute", "presence-dry"]);
  expect(BACKLOG_OPS_EXECUTE_ENABLED).toBe(false);
  for (const mode of BACKLOG_OPS_MODES) {
    const exec = mode.endsWith("-execute");
    expect(codeSync(() => backlogOpsDispatch(dispatchEnv(mode)))).toBe(exec ? "backlog_ops_dormant" : null);
    expect(codeSync(() => backlogOpsDispatch(dispatchEnv(mode), true))).toBeNull();
  }
  for (const mode of ["drain", "apply", "DRAIN-DRY", "drain-dry ", ""])
    expect(codeSync(() => backlogOpsDispatch({ BACKLOG_OPS_MODE: mode, BACKLOG_OPS_SHA: sha }))).toBe("backlog_ops_inputs");
  expect(codeSync(() => backlogOpsDispatch({ BACKLOG_OPS_MODE: "drain-dry", BACKLOG_OPS_SHA: "1".repeat(39) }))).toBe("backlog_ops_inputs");
});

test("typed bindings: each execute binds exactly its runs and the exact confirmation; dry modes bind nothing", () => {
  const on = (mode: BacklogOpsMode, extra: Record<string, string>) => codeSync(() => backlogOpsDispatch(dispatchEnv(mode, extra), true));
  expect(on("backfill-execute", { BACKLOG_OPS_DRY_RUN: "" })).toBe("backlog_ops_dry_run");
  expect(on("backfill-execute", { BACKLOG_OPS_BACKFILL_RUN: BACKFILL })).toBe("backlog_ops_backfill_run");
  expect(on("export-execute", { BACKLOG_OPS_BACKFILL_RUN: "" })).toBe("backlog_ops_backfill_run");
  expect(on("export-execute", { BACKLOG_OPS_EXPORT_RUN: EXPORT })).toBe("backlog_ops_export_run");
  expect(on("drain-execute", { BACKLOG_OPS_EXPORT_RUN: "" })).toBe("backlog_ops_export_run");
  expect(on("drain-execute", { BACKLOG_OPS_EXPORT_RUN: DRY })).toBe("backlog_ops_bindings");
  expect(on("drain-execute", { BACKLOG_OPS_CONFIRMATION: "" })).toBe("backlog_ops_confirmation");
  expect(on("drain-execute", { BACKLOG_OPS_CONFIRMATION: `DRAIN-EXECUTE ${sha} DRY ${DRY} BACKFILL ${BACKFILL}` })).toBe("backlog_ops_confirmation");
  expect(on("drain-execute", { BACKLOG_OPS_CONFIRMATION: `drain-execute ${sha} DRY ${DRY} BACKFILL ${BACKFILL} EXPORT ${EXPORT}` })).toBe("backlog_ops_confirmation");
  expect(backlogOpsDispatch(dispatchEnv("drain-execute"), true).confirmation).toBe(`DRAIN-EXECUTE ${sha} DRY ${DRY} BACKFILL ${BACKFILL} EXPORT ${EXPORT}`);
  expect(on("drain-dry", { BACKLOG_OPS_DRY_RUN: DRY })).toBe("backlog_ops_dry_run");
  expect(on("export-dry", { BACKLOG_OPS_CONFIRMATION: "EXPORT-DRY" })).toBe("backlog_ops_confirmation");
});

test("max_objects: drain modes only, a positive bounded integer", () => {
  const on = (mode: BacklogOpsMode, v: string) => codeSync(() => backlogOpsDispatch(dispatchEnv(mode, { BACKLOG_OPS_MAX_OBJECTS: v }), true));
  expect(on("drain-dry", "5000")).toBeNull();
  expect(on("drain-execute", "1000000")).toBeNull();
  for (const v of ["0", "-1", "1e3", "1000001", "abc", " 5"]) expect(on("drain-dry", v)).toBe("backlog_ops_max_objects");
  expect(on("backfill-dry", "10")).toBe("backlog_ops_max_objects");
  expect(on("export-execute", "10")).toBe("backlog_ops_max_objects");
});

// ---- target and argv ----

test("target variables are shape-checked: key in region, roles in the key's account and distinct", () => {
  expect(codeSync(() => backlogOpsTarget(target))).toBeNull();
  expect(codeSync(() => backlogOpsTarget({ ...target, BACKLOG_OPS_BUCKET: "s3://x" }))).toBe("backlog_ops_target_bucket");
  expect(codeSync(() => backlogOpsTarget({ ...target, BACKLOG_OPS_REGION: "east" }))).toBe("backlog_ops_target_region");
  expect(codeSync(() => backlogOpsTarget({ ...target, BACKLOG_OPS_KMS_KEY_ARN: "alias/transcripts" }))).toBe("backlog_ops_target_kms");
  expect(codeSync(() => backlogOpsTarget({ ...target, BACKLOG_OPS_REGION: "us-west-2" }))).toBe("backlog_ops_target_kms");
  expect(codeSync(() => backlogOpsTarget({ ...target, BACKLOG_OPS_DRY_ROLE_ARN: target.BACKLOG_OPS_EXECUTE_ROLE_ARN }))).toBe("backlog_ops_target_roles");
  expect(codeSync(() => backlogOpsTarget({ ...target, BACKLOG_OPS_EXECUTE_ROLE_ARN: "arn:aws:iam::111111111111:role/x" }))).toBe("backlog_ops_target_roles");
  expect(codeSync(() => backlogOpsTarget({ ...target, BACKLOG_OPS_DRY_ROLE_ARN: "" }))).toBe("backlog_ops_target_roles");
});

test("argv is fixed: no org unless scoped, never a checkpoint or start key; dry runs omit the key; execute names it; drain gets no database", () => {
  const t = backlogOpsTarget(target);
  // presence-dry's argv has its own test (session-presence.test.ts).
  for (const mode of BACKLOG_OPS_MODES.filter((m) => !m.startsWith("presence-"))) {
    const d = backlogOpsDispatch(dispatchEnv(mode, mode.startsWith("drain") ? { BACKLOG_OPS_MAX_OBJECTS: "500" } : {}), true);
    const spec = backlogOpsToolArgs(d, t);
    const exec = mode.endsWith("-execute");
    expect(spec.args[0]).toBe(exec ? "--execute" : "--dry-run");
    expect(spec.args.includes("--kms-key-arn")).toBe(exec);
    for (const flag of ["--checkpoint-file", "--start-after", "--org", "--assume-empty-fence"]) expect(spec.args).not.toContain(flag);
    expect(spec.needsDb).toBe(!mode.startsWith("drain"));
    expect(spec.args.includes("--max-objects")).toBe(mode.startsWith("drain"));
    const timeout = spec.args.indexOf("--statement-timeout-ms");
    expect(timeout >= 0).toBe(!mode.startsWith("drain"));
    if (timeout >= 0) expect(spec.args.slice(timeout, timeout + 2)).toEqual(["--statement-timeout-ms", "600000"]);
    expect(spec.args.includes("--page-size")).toBe(mode.startsWith("backfill"));
    if (mode.startsWith("backfill")) {
      expect(spec.args.slice(spec.args.indexOf("--page-size"), spec.args.indexOf("--page-size") + 2)).toEqual(["--page-size", "5000"]);
      expect(spec.args.slice(spec.args.indexOf("--row-page-size"), spec.args.indexOf("--row-page-size") + 2)).toEqual(["--row-page-size", "20000"]);
    }
    expect(backlogOpsRole(d, t)).toBe(exec ? t.executeRoleArn : t.dryRoleArn);
  }
});

const ORG1 = "11111111-2222-4333-8444-555555555555", ORG2 = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee";

test("org scope: BACKLOG_OPS_ORGS, lowercase UUIDs only; an id (never an org) for receipts; --org per org for every tool", () => {
  expect(backlogOpsScope({})).toEqual({ orgs: null, id: "all" });
  expect(backlogOpsScope({ BACKLOG_OPS_ORGS: " " })).toEqual({ orgs: null, id: "all" });
  const one = backlogOpsScope({ BACKLOG_OPS_ORGS: ORG1 });
  expect(one.orgs).toEqual([ORG1]);
  expect(one.id).toMatch(/^orgs_[0-9a-f]{32}$/);
  expect(one.id).not.toContain(ORG1.slice(0, 8));
  expect(backlogOpsScope({ BACKLOG_OPS_ORGS: `${ORG2},${ORG1}` })).toEqual(backlogOpsScope({ BACKLOG_OPS_ORGS: `${ORG1},${ORG2}` }));
  expect(backlogOpsScope({ BACKLOG_OPS_ORGS: `${ORG1},${ORG2}` }).id).not.toBe(one.id);
  for (const bad of [ORG2.toUpperCase(), `${ORG1}, ${ORG2}`, `${ORG1},`, `${ORG1},${ORG1}`, "org-a", "../x", `${ORG1};${ORG2}`,
    Array.from({ length: 65 }, (_, i) => `${String(i).padStart(8, "0")}-2222-4333-8444-555555555555`).join(",")])
    expect(codeSync(() => backlogOpsScope({ BACKLOG_OPS_ORGS: bad }))).toBe("backlog_ops_scope");
  const t = backlogOpsTarget(target);
  for (const mode of BACKLOG_OPS_MODES) {
    const d = backlogOpsDispatch(dispatchEnv(mode), true);
    const args = backlogOpsToolArgs(d, t, backlogOpsScope({ BACKLOG_OPS_ORGS: `${ORG2},${ORG1}` })).args;
    expect(args.slice(-4)).toEqual(["--org", ORG1, "--org", ORG2]);
    expect(backlogOpsToolArgs(d, t, backlogOpsScope({})).args).not.toContain("--org");
  }
});

// ---- receipts ----

test("receipts are counts only: an id, key or URL anywhere refuses", () => {
  expect(countsOnly({ mode: "execute", orgs: { incomplete: 0 }, gated: { authority_off: 3 }, stopped: "complete", ok: true })).toBeTruthy();
  for (const bad of [
    { org: "11111111-2222-4333-8444-555555555555" }, { key: "96bulls/x/v1/ws=y" }, { url: "postgresql://u:p@h/d" },
    { message: "Something Failed" }, { list: [1, 2] }, { "Org-Id": 1 }, { n: Number.NaN },
  ]) expect(codeSync(() => countsOnly(bad))).toMatch(/^backlog_ops_receipt_/);
  const d = backlogOpsDispatch(dispatchEnv("drain-dry"));
  expect(backlogOpsReceipt(d, "9", 0, "progress\n{\"mode\":\"dry_run\",\"listed\":4}\n").summary).toEqual({ mode: "dry_run", listed: 4 });
  expect(backlogOpsReceipt(d, "9", 1, "").summary).toEqual({ refused: "no_summary" });
  expect(backlogOpsReceipt(d, "9", 1, "not json").summary).toEqual({ refused: "summary_unparsed" });
  expect(codeSync(() => backlogOpsReceipt(d, "9", 0, JSON.stringify({ watermark: "96bulls/org/x" })))).toBe("backlog_ops_receipt_string");
});

const receipt = (mode: BacklogOpsMode, run: string, summary: unknown, exitCode = 0) =>
  JSON.stringify({ contract: BACKLOG_OPS_RECEIPT_CONTRACT, mode, sha, run, exitCode, summary });
const backfillOk = { mode: "execute", sections: { fences: "complete", ownership: "complete", revisions: "complete" } };
const exportOk = { mode: "execute", failed: 0, fence_late: 0, orgs: { complete: 3, would_complete: 0, already_complete: 0, incomplete: 0 } };

test("bound receipts: mode, sha, run, exit 0; backfill complete; export failed = fence_late = incomplete = 0", () => {
  const ok = (raw: string, mode: BacklogOpsMode, run: string) => codeSync(() => checkBoundReceipt(raw, { mode, sha, run }));
  expect(ok(receipt("backfill-execute", BACKFILL, backfillOk), "backfill-execute", BACKFILL)).toBeNull();
  expect(ok(receipt("export-execute", EXPORT, exportOk), "export-execute", EXPORT)).toBeNull();
  expect(ok(receipt("drain-dry", DRY, { mode: "dry_run", listed: 10 }), "drain-dry", DRY)).toBeNull();
  expect(ok(receipt("backfill-execute", BACKFILL, { ...backfillOk, sections: { ...backfillOk.sections, revisions: "incomplete" } }), "backfill-execute", BACKFILL)).toBe("backlog_ops_backfill_incomplete");
  for (const bad of [{ failed: 1 }, { fence_late: 2 }, { orgs: { ...exportOk.orgs, incomplete: 1 } }])
    expect(ok(receipt("export-execute", EXPORT, { ...exportOk, ...bad }), "export-execute", EXPORT)).toBe("backlog_ops_export_not_ready");
  expect(ok(receipt("export-execute", EXPORT, exportOk, 1), "export-execute", EXPORT)).toBe("backlog_ops_bound_export_receipt");
  expect(ok(receipt("export-execute", "999", exportOk), "export-execute", EXPORT)).toBe("backlog_ops_bound_export_receipt");
  expect(ok(receipt("export-dry", EXPORT, { ...exportOk, mode: "dry_run" }), "export-execute", EXPORT)).toBe("backlog_ops_bound_export_receipt");
  expect(ok(receipt("export-execute", EXPORT, { ...exportOk, mode: "dry_run" }), "export-execute", EXPORT)).toBe("backlog_ops_bound_export_receipt");
  expect(ok(receipt("export-execute", EXPORT, { refused: "export_claim_sessions_gate_off" }), "export-execute", EXPORT)).toBe("backlog_ops_bound_export_refused");
  expect(ok("{", "export-execute", EXPORT)).toBe("backlog_ops_bound_export_receipt");
});

test("bound receipts carry the execute's scope: the dry run exactly; a backfill or export the same or unscoped", () => {
  const scoped = backlogOpsScope({ BACKLOG_OPS_ORGS: ORG1 }).id, other = backlogOpsScope({ BACKLOG_OPS_ORGS: ORG2 }).id;
  const withScope = (raw: string, scope?: string) => JSON.stringify({ ...JSON.parse(raw), ...(scope === undefined ? {} : { scope }) });
  const ok = (raw: string, mode: BacklogOpsMode, run: string, scope: string, kind: "dry" | "backfill" | "export") =>
    codeSync(() => checkBoundReceipt(raw, { mode, sha, run, scope, kind }));
  const dry = receipt("drain-dry", DRY, { mode: "dry_run", listed: 1 });
  expect(ok(withScope(dry, scoped), "drain-dry", DRY, scoped, "dry")).toBeNull();
  expect(ok(withScope(dry, "all"), "drain-dry", DRY, scoped, "dry")).toBe("backlog_ops_bound_drain_scope");
  expect(ok(withScope(dry), "drain-dry", DRY, scoped, "dry")).toBe("backlog_ops_bound_drain_scope");
  expect(ok(withScope(dry, other), "drain-dry", DRY, scoped, "dry")).toBe("backlog_ops_bound_drain_scope");
  expect(ok(withScope(dry, scoped), "drain-dry", DRY, "all", "dry")).toBe("backlog_ops_bound_drain_scope");
  expect(ok(withScope(dry), "drain-dry", DRY, "all", "dry")).toBeNull(); // a receipt from before scoping is unscoped
  const bf = receipt("backfill-execute", BACKFILL, backfillOk);
  expect(ok(withScope(bf, "all"), "backfill-execute", BACKFILL, scoped, "backfill")).toBeNull();
  expect(ok(withScope(bf, scoped), "backfill-execute", BACKFILL, scoped, "backfill")).toBeNull();
  expect(ok(withScope(bf, other), "backfill-execute", BACKFILL, scoped, "backfill")).toBe("backlog_ops_bound_backfill_scope");
  expect(ok(withScope(bf, scoped), "backfill-execute", BACKFILL, "all", "backfill")).toBe("backlog_ops_bound_backfill_scope");
  expect(ok(withScope(bf, ORG1), "backfill-execute", BACKFILL, scoped, "backfill")).toBe("backlog_ops_bound_backfill_scope");
  const ex = receipt("export-execute", EXPORT, exportOk);
  expect(ok(withScope(ex, other), "export-execute", EXPORT, scoped, "export")).toBe("backlog_ops_bound_export_scope");
  const d = backlogOpsDispatch(dispatchEnv("drain-dry"));
  expect(backlogOpsReceipt(d, "9", 0, "{\"mode\":\"dry_run\",\"listed\":1}", scoped).scope).toBe(scoped);
  expect(backlogOpsReceipt(d, "9", 0, "{}").scope).toBe("all");
  expect(codeSync(() => backlogOpsReceipt(d, "9", 0, "{}", ORG1))).toBe("backlog_ops_scope");
});

test("the credential step binds every receipt to BACKLOG_OPS_ORGS's scope", () => {
  const scoped = backlogOpsScope({ BACKLOG_OPS_ORGS: ORG1 }).id;
  const env = { ...dispatchEnv("drain-execute"), RUNNER_TEMP: "/tmp/rt", BACKLOG_OPS_ORGS: ORG1 };
  const put = (raw: string, scope: string) => JSON.stringify({ ...JSON.parse(raw), scope });
  const files: Record<string, string> = {
    "/tmp/rt/backlog-ops-bound/dry/backlog-ops-receipt.json": put(receipt("drain-dry", DRY, { mode: "dry_run", listed: 1 }), scoped),
    "/tmp/rt/backlog-ops-bound/backfill/backlog-ops-receipt.json": put(receipt("backfill-execute", BACKFILL, backfillOk), "all"),
    "/tmp/rt/backlog-ops-bound/export/backlog-ops-receipt.json": put(receipt("export-execute", EXPORT, exportOk), scoped),
  };
  const read = (p: string) => files[p]!;
  const d = backlogOpsDispatch(env, true);
  expect(codeSync(() => checkBoundReceipts(env, d, read))).toBeNull();
  expect(codeSync(() => checkBoundReceipts({ ...env, BACKLOG_OPS_ORGS: "" }, d, read))).toBe("backlog_ops_bound_drain_scope");
  expect(codeSync(() => checkBoundReceipts({ ...env, BACKLOG_OPS_ORGS: ORG2 }, d, read))).toBe("backlog_ops_bound_drain_scope");
});

test("the credential step checks every bound receipt before any credential, from the downloaded paths", () => {
  const env = { ...dispatchEnv("drain-execute"), RUNNER_TEMP: "/tmp/rt" };
  const files: Record<string, string> = {
    "/tmp/rt/backlog-ops-bound/dry/backlog-ops-receipt.json": receipt("drain-dry", DRY, { mode: "dry_run", listed: 1 }),
    "/tmp/rt/backlog-ops-bound/backfill/backlog-ops-receipt.json": receipt("backfill-execute", BACKFILL, backfillOk),
    "/tmp/rt/backlog-ops-bound/export/backlog-ops-receipt.json": receipt("export-execute", EXPORT, exportOk),
  };
  const read = (p: string) => { if (!(p in files)) throw new Error("missing"); return files[p]!; };
  const d = backlogOpsDispatch(env, true);
  expect(codeSync(() => checkBoundReceipts(env, d, read))).toBeNull();
  delete files["/tmp/rt/backlog-ops-bound/export/backlog-ops-receipt.json"];
  expect(codeSync(() => checkBoundReceipts(env, d, read))).toBe("backlog_ops_bound_export_missing");
});

// ---- credentials ----

test("database URL: only the pinned release login, re-encoded with sslmode=verify-full alone; never for the drain", () => {
  const url = `postgresql://${P.owner}.${P.project}:p%40ss%2Fw0rd@${P.host}:${P.port}/${P.database}?sslmode=verify-full&channel_binding=require`;
  const out = new URL(toolDbUrl(url));
  expect([...out.searchParams.entries()]).toEqual([["sslmode", "verify-full"]]);
  expect(decodeURIComponent(out.password)).toBe("p@ss/w0rd");
  expect(out.hostname).toBe(P.host);
  expect(codeSync(() => toolDbUrl(url.replace(P.host, "db.example.supabase.co")))).toBe("release_connection");
  expect(codeSync(() => toolDbUrl(undefined))).toBe("release_connection");
  const env = { ...target, PATH: "/usr/bin", HOME: "/h", GITHUB_TOKEN: "ghs_x", SUPABASE_MIGRATION_OWNER_URL: url };
  const aws = { accessKeyId: "ASIAEXAMPLEEXAMPLE", secretAccessKey: "s", sessionToken: "t" };
  const drain = backlogOpsDispatch(dispatchEnv("drain-dry"));
  expect(codeSync(() => backlogOpsChildSpawn(env, drain, aws, "postgresql://x"))).toBe("backlog_ops_db_binding");
  const spawnDrain = backlogOpsChildSpawn(env, drain, aws, null);
  expect(spawnDrain.input).toBeNull();
  const backfill = backlogOpsDispatch(dispatchEnv("backfill-dry"));
  expect(codeSync(() => backlogOpsChildSpawn(env, backfill, aws, null))).toBe("backlog_ops_db_binding");
  const spawnBackfill = backlogOpsChildSpawn(env, backfill, aws, toolDbUrl(url));
  expect(Object.keys(JSON.parse(spawnBackfill.input!))).toEqual(["dbUrl"]);
  // Fixed argv, allowlisted env plus the session: never the GitHub token, the owner URL or a target variable.
  expect(Object.keys(spawnBackfill.env).sort()).toEqual(["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "HOME", "PATH"]);
  expect([...BACKLOG_OPS_CHILD_ENV]).toEqual(["PATH", "HOME", "LANG", "TMPDIR"]);
  expect(spawnBackfill.args.join(" ")).not.toContain("postgresql");
  expect(spawnBackfill.args[0]).toEndWith("scripts/upload-fence-backfill.ts");
});

test("STS: the OIDC token is exchanged for the mode's role in memory; a bad response refuses", async () => {
  const calls: Array<{ url: string; body?: string }> = [];
  const xml = "<AssumeRoleWithWebIdentityResponse><Credentials><AccessKeyId>ASIAAAAAAAAAAAAAAAAA</AccessKeyId>" +
    `<SecretAccessKey>secret</SecretAccessKey><SessionToken>token</SessionToken><Expiration>${new Date(Date.now() + 6 * 3600_000).toISOString()}</Expiration></Credentials></AssumeRoleWithWebIdentityResponse>`;
  const fetcher = (async (url: string, init?: RequestInit) => {
    calls.push({ url, body: init?.body?.toString() });
    if (url.startsWith("https://token.example/")) return new Response(JSON.stringify({ value: "a.b.c" }));
    return new Response(xml);
  }) as unknown as typeof fetch;
  const env = { ACTIONS_ID_TOKEN_REQUEST_URL: "https://token.example/x?api=1", ACTIONS_ID_TOKEN_REQUEST_TOKEN: "req" };
  const creds = await assumeRole(env, target.BACKLOG_OPS_DRY_ROLE_ARN, "us-east-1", "backlog-ops-drain-dry-9", fetcher);
  expect(creds.accessKeyId).toBe("ASIAAAAAAAAAAAAAAAAA");
  expect(calls[0]!.url).toBe("https://token.example/x?api=1&audience=sts.amazonaws.com");
  expect(calls[1]!.url).toBe("https://sts.us-east-1.amazonaws.com/");
  const body = new URLSearchParams(calls[1]!.body);
  expect(body.get("Action")).toBe("AssumeRoleWithWebIdentity");
  expect(body.get("RoleArn")).toBe(target.BACKLOG_OPS_DRY_ROLE_ARN);
  expect(body.get("DurationSeconds")).toBe("21600");
  expect(await code(() => assumeRole({}, target.BACKLOG_OPS_DRY_ROLE_ARN, "us-east-1", "s", fetcher))).toBe("backlog_ops_oidc_unavailable");
  expect(codeSync(() => parseStsCredentials("<Error><Code>AccessDenied</Code></Error>"))).toBe("backlog_ops_sts_response");
});

test("STS: assumeRole refuses a session with too little time left (the expiry check is wired in)", async () => {
  const xml = (expiresAt: number) => "<AssumeRoleWithWebIdentityResponse><Credentials><AccessKeyId>ASIAAAAAAAAAAAAAAAAA</AccessKeyId>" +
    `<SecretAccessKey>secret</SecretAccessKey><SessionToken>token</SessionToken><Expiration>${new Date(expiresAt).toISOString()}</Expiration></Credentials></AssumeRoleWithWebIdentityResponse>`;
  const env = { ACTIONS_ID_TOKEN_REQUEST_URL: "https://token.example/x?api=1", ACTIONS_ID_TOKEN_REQUEST_TOKEN: "req" };
  const fetcherFor = (expiresAt: number) => (async (url: string) =>
    url.startsWith("https://token.example/") ? new Response(JSON.stringify({ value: "a.b.c" })) : new Response(xml(expiresAt))) as unknown as typeof fetch;
  // Ten minutes left: assumeRole itself must refuse, not hand back credentials.
  expect(await code(() => assumeRole(env, target.BACKLOG_OPS_DRY_ROLE_ARN, "us-east-1", "s", fetcherFor(Date.now() + 10 * 60_000))))
    .toBe("backlog_ops_sts_expiration");
  const ok = await assumeRole(env, target.BACKLOG_OPS_DRY_ROLE_ARN, "us-east-1", "s", fetcherFor(Date.now() + 6 * 3600_000));
  expect(ok.accessKeyId).toBe("ASIAAAAAAAAAAAAAAAAA");
});

// ---- GitHub gate ----

function githubFixture(mode: BacklogOpsMode) {
  const actor = { id: P.dispatcher.id, login: P.dispatcher.login, type: "User" };
  const now = Date.now();
  const bound = (id: string, m: BacklogOpsMode, ageMs: number) => ({
    id: Number(id), head_sha: sha, head_branch: "main", event: "workflow_dispatch", path: BACKLOG_OPS_WORKFLOW, run_attempt: 1,
    status: "completed", conclusion: "success", actor: { ...actor }, triggering_actor: { ...actor },
    display_title: backlogOpsRunName(m, sha), created_at: new Date(now - ageMs).toISOString(),
  });
  const tool = mode.split("-")[0] as "backfill" | "export" | "drain";
  return {
    ref: { object: { sha } },
    run: { id: 100, head_sha: sha, head_branch: "main", event: "workflow_dispatch", path: BACKLOG_OPS_WORKFLOW, run_attempt: 1,
      actor, triggering_actor: { ...actor }, display_title: backlogOpsRunName(mode, sha) },
    environment: { id: 7, name: backlogOpsEnvironment(mode), protection_rules: [{ type: "branch_policy" }] as any[] | undefined,
      deployment_branch_policy: { protected_branches: false, custom_branch_policies: true } },
    branches: { total_count: 1, branch_policies: [{ name: "main", type: "branch" }] },
    runs: { workflow_runs: [{ id: 10, head_sha: sha, head_branch: "main", event: "push", status: "completed", conclusion: "success", run_attempt: 1 }] },
    jobs: { total_count: 5, jobs: ["checks", "session-read-worker / worker", "real PostgreSQL 15", "real PostgreSQL 16", "real PostgreSQL 17"]
      .map((name) => ({ name, head_sha: sha, status: "completed", conclusion: "success" })) },
    approvals: [] as any[],
    bound: {
      [DRY]: bound(DRY, `${tool}-dry` as BacklogOpsMode, 3 * 3600_000),
      [BACKFILL]: bound(BACKFILL, "backfill-execute", 2 * 3600_000),
      [EXPORT]: bound(EXPORT, "export-execute", 3600_000),
    } as Record<string, any>,
    artifacts: { [DRY]: 1, [BACKFILL]: 1, [EXPORT]: 1 } as Record<string, number>,
    expired: false,
    artifactsTotalDelta: 0,
  };
}
type Fixture = ReturnType<typeof githubFixture>;
const githubGet = (f: Fixture, seen: string[] = []) => async (path: string): Promise<any> => {
  seen.push(path);
  if (path.endsWith("/git/ref/heads/main")) return f.ref;
  if (path.endsWith("/actions/runs/100")) return f.run;
  if (path.endsWith("/actions/runs/100/approvals")) return f.approvals;
  for (const id of [DRY, BACKFILL, EXPORT]) {
    if (path.endsWith(`/actions/runs/${id}`)) return f.bound[id];
    if (path.endsWith(`/actions/runs/${id}/artifacts?per_page=100`))
      return { total_count: f.artifacts[id]! + f.artifactsTotalDelta,
        artifacts: Array.from({ length: f.artifacts[id]! }, () => ({ name: BACKLOG_OPS_RECEIPT_ARTIFACT, expired: f.expired })) };
  }
  if (path.endsWith("/environments/" + f.environment.name)) return f.environment;
  if (path.includes("deployment-branch-policies")) return f.branches;
  if (path.includes("/actions/workflows/")) return f.runs;
  if (path.includes("/runs/10/")) return f.jobs;
  throw new Error("unexpected " + path);
};
const context = {
  GITHUB_REPOSITORY: P.repository, GITHUB_REPOSITORY_ID: PRIVATE_INGEST_RELEASE.repositoryId, GITHUB_EVENT_NAME: "workflow_dispatch",
  GITHUB_REF: "refs/heads/main", GITHUB_SHA: sha, GITHUB_RUN_ATTEMPT: "1", GITHUB_RUN_ID: "100",
};
const gate = (mode: BacklogOpsMode, mutate: (f: Fixture) => void = () => {}, extra: Record<string, string> = {}, enabled = true, seen?: string[]) => {
  const f = githubFixture(mode);
  mutate(f);
  return code(() => backlogOpsGithubGate({ ...context, ...dispatchEnv(mode), ...extra }, githubGet(f, seen), true, { enabled }));
};

test("gate: every mode passes when enabled; executes refuse before any GitHub read while dormant", async () => {
  for (const mode of BACKLOG_OPS_MODES) expect(await gate(mode)).toBeNull();
  for (const mode of BACKLOG_OPS_MODES.filter((m) => m.endsWith("-execute"))) {
    const seen: string[] = [];
    expect(await gate(mode, () => {}, {}, false, seen)).toBe("backlog_ops_dormant");
    expect(seen).toEqual([]);
  }
  expect(await gate("drain-dry", () => {}, {}, false)).toBeNull();
});

test("gate: context, main, run, environment, CI and approval refuse", async () => {
  const cases: Array<[BacklogOpsMode, (f: Fixture) => void, Record<string, string>, string]> = [
    ["drain-dry", (f) => { f.ref.object.sha = "b".repeat(40); }, {}, "backlog_ops_gate_main_moved"],
    ["drain-dry", () => {}, { GITHUB_SHA: "b".repeat(40) }, "backlog_ops_gate_context"],
    ["drain-dry", () => {}, { GITHUB_RUN_ATTEMPT: "2" }, "backlog_ops_gate_context"],
    ["drain-dry", () => {}, { GITHUB_REF: "refs/heads/dev" }, "backlog_ops_gate_context"],
    ["drain-dry", () => {}, { GITHUB_REPOSITORY_ID: "1" }, "backlog_ops_gate_context"],
    ["drain-dry", (f) => { f.run.actor.id = 42; }, {}, "backlog_ops_gate_run"],
    ["drain-dry", (f) => { f.run.triggering_actor.type = "Bot"; }, {}, "backlog_ops_gate_run"],
    ["drain-dry", (f) => { f.run.path = ".github/workflows/apply-session-schema.yml"; }, {}, "backlog_ops_gate_run"],
    ["drain-dry", (f) => { f.run.display_title = backlogOpsRunName("drain-execute", sha); }, {}, "backlog_ops_gate_run"],
    ["drain-dry", (f) => { f.environment.deployment_branch_policy.protected_branches = true; }, {}, "backlog_ops_gate_environment"],
    ["drain-dry", (f) => { f.branches.branch_policies.push({ name: "dev", type: "branch" }); f.branches.total_count = 2; }, {}, "backlog_ops_gate_environment_branch"],
    ["drain-dry", (f) => { f.runs.workflow_runs[0]!.conclusion = "failure"; }, {}, "backlog_ops_gate_ci"],
    ["drain-dry", (f) => { f.jobs.jobs[0]!.conclusion = "failure"; }, {}, "backlog_ops_gate_ci_jobs"],
    ["drain-dry", (f) => { f.environment.protection_rules!.push({ type: "required_reviewers", reviewers: [{ type: "User", reviewer: { type: "User", id: 5 } }] }); }, {}, "backlog_ops_gate_human_approval"],
    ["drain-dry", (f) => { f.environment.protection_rules = undefined; }, {}, "backlog_ops_gate_environment"],
    // A rejection recorded for this environment refuses even beside an approval.
    ["drain-dry", (f) => {
      f.environment.protection_rules!.push({ type: "required_reviewers", reviewers: [{ type: "User", reviewer: { type: "User", id: 5 } }] });
      const envs = [{ id: 7, name: BACKLOG_OPS_DRY_ENVIRONMENT }];
      f.approvals.push({ state: "approved", user: { type: "User", id: 5 }, environments: envs }, { state: "rejected", user: { type: "User", id: 6 }, environments: envs });
    }, {}, "backlog_ops_gate_human_rejected"],
  ];
  for (const [mode, mutate, extra, want] of cases) expect(await gate(mode, mutate, extra)).toBe(want);
});

test("gate: bound runs must be this workflow, this sha, success, attempt 1, the dispatcher, the right mode, fresh, one receipt", async () => {
  const cases: Array<[BacklogOpsMode, (f: Fixture) => void, string]> = [
    ["backfill-execute", (f) => { f.bound[DRY].conclusion = "failure"; }, "backlog_ops_gate_dry_run"],
    ["backfill-execute", (f) => { f.bound[DRY].status = "in_progress"; }, "backlog_ops_gate_dry_run"],
    ["backfill-execute", (f) => { f.bound[DRY].head_sha = "b".repeat(40); }, "backlog_ops_gate_dry_run"],
    ["backfill-execute", (f) => { f.bound[DRY].run_attempt = 2; }, "backlog_ops_gate_dry_run"],
    ["backfill-execute", (f) => { f.bound[DRY].path = ".github/workflows/index-prebuild-058000.yml"; }, "backlog_ops_gate_dry_run"],
    ["backfill-execute", (f) => { f.bound[DRY].actor.login = "relay-bot"; }, "backlog_ops_gate_dry_run"],
    ["backfill-execute", (f) => { f.bound[DRY].display_title = backlogOpsRunName("drain-dry", sha); }, "backlog_ops_gate_dry_run"],
    ["backfill-execute", (f) => { f.bound[DRY].created_at = new Date(Date.now() - 25 * 3600_000).toISOString(); }, "backlog_ops_gate_dry_run"],
    ["backfill-execute", (f) => { f.bound[DRY].created_at = new Date(Date.now() + 3600_000).toISOString(); }, "backlog_ops_gate_dry_run"],
    ["backfill-execute", (f) => { f.artifacts[DRY] = 0; }, "backlog_ops_gate_dry_artifact"],
    ["backfill-execute", (f) => { f.artifacts[DRY] = 2; }, "backlog_ops_gate_dry_artifact"],
    ["backfill-execute", (f) => { f.expired = true; }, "backlog_ops_gate_dry_artifact"],
    // A truncated artifact listing (total_count above the page) refuses.
    ["backfill-execute", (f) => { f.artifactsTotalDelta = 1; }, "backlog_ops_gate_dry_artifact"],
    ["export-execute", (f) => { f.bound[BACKFILL].display_title = backlogOpsRunName("backfill-dry", sha); }, "backlog_ops_gate_backfill_run"],
    ["drain-execute", (f) => { f.bound[EXPORT].display_title = backlogOpsRunName("export-dry", sha); }, "backlog_ops_gate_export_run"],
    // The drain's backfill must be fresh (3 h), and its export after it.
    ["drain-execute", (f) => { f.bound[BACKFILL].created_at = new Date(Date.now() - 4 * 3600_000).toISOString(); }, "backlog_ops_gate_backfill_run"],
    ["drain-execute", (f) => { f.bound[EXPORT].created_at = new Date(Date.now() - 2.5 * 3600_000).toISOString(); }, "backlog_ops_gate_order"],
  ];
  for (const [mode, mutate, want] of cases) expect(await gate(mode, mutate)).toBe(want);
  // The same 4 h old backfill is fine for an export (24 h window).
  expect(await gate("export-execute", (f) => { f.bound[BACKFILL].created_at = new Date(Date.now() - 4 * 3600_000).toISOString(); })).toBeNull();
});

// ---- workflow shape ----

test("workflow: dispatch inputs, gates, one credential step, OIDC, protected vars, counts-only receipt, one op at a time", () => {
  const yml = readFileSync(fileURLToPath(new URL("../../../" + BACKLOG_OPS_WORKFLOW, import.meta.url)), "utf8");
  const code_ = yml.split("\n").filter((l) => !l.trimStart().startsWith("#")).join("\n");
  expect(yml).toContain("options: [backfill-dry, backfill-execute, export-dry, export-execute, drain-dry, drain-execute, presence-dry]");
  expect(yml).toContain("run-name: ${{ github.event_name == 'workflow_dispatch' && format('backlog ops {0} {1}', inputs.mode, inputs.sha)");
  expect(backlogOpsRunName("drain-dry", sha)).toBe(`backlog ops drain-dry ${sha}`);
  // Dispatches serialise with every guarded production operator on the owner database.
  expect(yml).toContain("group: ${{ github.event_name == 'workflow_dispatch' && 'guarded-session-schema-production'");
  expect(yml).not.toContain("guarded-backlog-ops-production");
  expect(yml).toContain("cancel-in-progress: false");
  expect(yml).toContain("github.repository_id == '1437714333' && github.ref == 'refs/heads/main'");
  expect(yml).toContain("bun scripts/backlog-ops-github.ts --approved");
  // The environment comes from the gate (derived from the mode), never a literal or an input.
  expect(yml).not.toContain('echo "environment=');
  expect(yml).toContain("name: ${{ needs.prerequisite-gate.outputs.environment }}");
  // Only the operate job may mint an OIDC token.
  expect(code_.match(/id-token: write/g)?.length).toBe(1);
  expect(yml).toContain("id-token: write");
  // Three secrets, all only in the one credential step: the owner URL (database tools only), the org scope (a
  // secret, never a variable, so this public repository's logs mask the org ids) and presence-dry's session id.
  expect(yml.match(/secrets\./g)?.length).toBe(3);
  expect(yml).toContain("BACKLOG_OPS_ORGS: ${{ secrets.BACKLOG_OPS_ORGS }}");
  expect(yml).not.toContain("vars.BACKLOG_OPS_ORGS");
  expect(yml).toContain("SUPABASE_MIGRATION_OWNER_URL: ${{ (startsWith(inputs.mode, 'backfill-') || startsWith(inputs.mode, 'export-') || inputs.mode == 'presence-dry') && secrets.SUPABASE_MIGRATION_OWNER_URL || '' }}");
  expect(yml).toContain("SESSION_PRESENCE_SESSION: ${{ inputs.mode == 'presence-dry' && secrets.SESSION_PRESENCE_SESSION || '' }}");
  for (const v of ["BACKLOG_OPS_BUCKET", "BACKLOG_OPS_REGION", "BACKLOG_OPS_KMS_KEY_ARN", "BACKLOG_OPS_DRY_ROLE_ARN", "BACKLOG_OPS_EXECUTE_ROLE_ARN"])
    expect(yml).toContain(`${v}: \${{ vars.${v} }}`);
  expect(yml).not.toContain("configure-aws-credentials");
  expect(code_).not.toMatch(/checkpoint|start-after|--org/);
  expect(yml).toContain("name: backlog-ops-receipt");
  expect(yml).toContain("path: ${{ runner.temp }}/backlog-ops-out/backlog-ops-receipt.json");
  for (const kind of ["dry", "backfill", "export"]) expect(yml).toContain(`path: \${{ runner.temp }}/backlog-ops-bound/${kind}`);
  // The proof job has no environment and no secret.
  const proof = yml.slice(yml.indexOf("  proof:"), yml.indexOf("  prerequisite-gate:"));
  expect(proof).not.toMatch(/^\s+environment:/m);
  expect(proof).not.toContain("secrets.");
});

test("environments: dry modes use the schema environment, every execute its own; the gate checks the mode's", async () => {
  for (const mode of BACKLOG_OPS_MODES)
    expect(backlogOpsEnvironment(mode)).toBe(mode.endsWith("-execute") ? BACKLOG_OPS_EXECUTE_ENVIRONMENT : BACKLOG_OPS_DRY_ENVIRONMENT);
  expect(BACKLOG_OPS_DRY_ENVIRONMENT).toBe(P.environment);
  expect(BACKLOG_OPS_EXECUTE_ENVIRONMENT).not.toBe(P.environment);
  for (const mode of BACKLOG_OPS_MODES) {
    const seen: string[] = [];
    expect(await gate(mode, () => {}, {}, true, seen)).toBeNull();
    expect(seen.some((p) => p.endsWith("/environments/" + backlogOpsEnvironment(mode)))).toBe(true);
    expect(seen.some((p) => p.endsWith("/environments/" + (mode.endsWith("-execute") ? BACKLOG_OPS_DRY_ENVIRONMENT : BACKLOG_OPS_EXECUTE_ENVIRONMENT)))).toBe(false);
  }
  // An execute whose protected environment is the schema one is refused (the fake only knows the wrong name).
  expect(await gate("drain-execute", (f) => { f.environment.name = BACKLOG_OPS_DRY_ENVIRONMENT; })).not.toBeNull();
  // The gate returns the environment the workflow's operate job must use.
  const f = githubFixture("export-execute");
  const r = await backlogOpsGithubGate({ ...context, ...dispatchEnv("export-execute") }, githubGet(f), false, { enabled: true });
  expect(r.environment).toBe(BACKLOG_OPS_EXECUTE_ENVIRONMENT);
});

test("STS expiration: the session must have at least the job's lifetime left", () => {
  const now = Date.parse("2026-11-13T18:00:00Z");
  expect(codeSync(() => checkStsExpiration("2026-11-14T00:00:00Z", now))).toBeNull();
  expect(codeSync(() => checkStsExpiration(new Date(now + BACKLOG_OPS_MIN_SESSION_REMAINING_MS - 1).toISOString(), now))).toBe("backlog_ops_sts_expiration");
  expect(codeSync(() => checkStsExpiration("2026-11-13T17:00:00Z", now))).toBe("backlog_ops_sts_expiration");
  expect(codeSync(() => checkStsExpiration("tomorrow", now))).toBe("backlog_ops_sts_expiration");
});

test("target: multi-region (mrk-) KMS key ids are accepted, other key-id shapes still refuse", () => {
  const mrk = { ...target, BACKLOG_OPS_KMS_KEY_ARN: "arn:aws:kms:us-east-1:000000000000:key/mrk-" + "0".repeat(32) };
  expect(codeSync(() => backlogOpsTarget(mrk))).toBeNull();
  expect(codeSync(() => backlogOpsTarget({ ...target, BACKLOG_OPS_KMS_KEY_ARN: "arn:aws:kms:us-east-1:000000000000:key/mrk-xyz" }))).toBe("backlog_ops_target_kms");
});
