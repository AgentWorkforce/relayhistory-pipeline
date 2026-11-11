/**
 * The pure parts of the schema release rehearsal (scripts/pg-proof/schema-rehearsal-core.ts):
 * input validation, the seed set, the pending window, the legacy forecast, result records,
 * and drift checks that tie the rehearsal to the production apply
 * (scripts/schema-release-database.ts) and keep the rehearsal files free of production
 * identifiers and secrets. The database stages run only in
 * .github/workflows/rehearse-session-schema.yml against disposable PostgreSQL.
 */
import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import {
  CONNECTION_ENV,
  disposableEnvRefusal,
  expectedWindow,
  errorCode,
  flipHex,
  INJECTION_REFUSAL,
  INJECTIONS,
  legacyForecast,
  parseExpect,
  PRODUCTION_APPLY_REFUSALS,
  rehearsalPassed,
  resultRecord,
  sameNameOid,
  seedOrder,
  seedSet,
  validateInputs,
} from "../../../scripts/pg-proof/schema-rehearsal-core.ts";
import { SCHEMA_RELEASE } from "../schema-release-contract.ts";

const ROOT = join(import.meta.dir, "../../..");
const read = (p: string) => readFileSync(join(ROOT, p), "utf8");
const code = (f: () => unknown) => {
  try {
    f();
    return null;
  } catch (e) {
    return (e as { code?: string }).code ?? "no_code";
  }
};
const SHA = "a".repeat(40);
const GEN = "b".repeat(64);
const BASE = "upstream:20261105045030_session_consumer_project_text_grant.sql";
const TARGET = "upstream:20261105047000_coordination_fact_readers.sql";
const ok = { sha: SHA, baseline: BASE, target: TARGET, generation: GEN, inject: "none" };

describe("validateInputs", () => {
  test("accepts the exact dispatch inputs, every injection", () => {
    for (const inject of INJECTIONS) expect(validateInputs({ ...ok, inject }).inject).toBe(inject);
  });
  test("refuses each malformed input with its own code", () => {
    expect(code(() => validateInputs({ ...ok, sha: SHA.slice(1) }))).toBe("rehearsal_input_sha");
    expect(code(() => validateInputs({ ...ok, sha: SHA.toUpperCase() }))).toBe("rehearsal_input_sha");
    expect(code(() => validateInputs({ ...ok, baseline: "20261105045030_x.sql" }))).toBe("rehearsal_input_baseline");
    expect(code(() => validateInputs({ ...ok, target: "upstream:../x.sql" }))).toBe("rehearsal_input_target");
    expect(code(() => validateInputs({ ...ok, target: BASE }))).toBe("rehearsal_target_not_after_baseline");
    expect(code(() => validateInputs({ ...ok, generation: "B".repeat(64) }))).toBe("rehearsal_input_generation");
    expect(code(() => validateInputs({ ...ok, inject: "drop-everything" }))).toBe("rehearsal_input_inject");
    expect(code(() => validateInputs({}))).toBe("rehearsal_input_sha");
  });
  test("expect_refusal: empty is none, <stage>:<code> parses, anything else refuses", () => {
    expect(validateInputs(ok).expect).toBeNull();
    expect(validateInputs({ ...ok, expect: "" }).expect).toBeNull();
    expect(validateInputs({ ...ok, expect: "R1:rehearsal_target_anchor_missing" }).expect).toEqual({
      stage: "R1",
      code: "rehearsal_target_anchor_missing",
    });
    expect(parseExpect("R4-stale-plan:release_ledger_changed")).toEqual({ stage: "R4-stale-plan", code: "release_ledger_changed" });
    for (const bad of ["R1", "R9:x", "R1:", ":x", "R1:x y", "R1:x;rm", "r1:x", "R1:x:y", "R1-:x"])
      expect(code(() => parseExpect(bad))).toBe("rehearsal_input_expect");
  });
  test("rehearsalPassed: no FAIL without expect; exactly the expected last FAIL with expect", () => {
    const P = (stage: string) => ({ stage, status: "PASS" as const, evidence: {} });
    const F = (stage: string, c: string) => ({ stage, status: "FAIL" as const, code: c, evidence: {} });
    const want = { stage: "R1", code: "rehearsal_target_anchor_missing" };
    expect(rehearsalPassed([], null)).toBe(false);
    expect(rehearsalPassed([P("R0"), P("R1")], null)).toBe(true);
    expect(rehearsalPassed([P("R0"), F("R1", want.code)], null)).toBe(false);
    expect(rehearsalPassed([P("R0"), P("R7-pre"), F("R1", want.code)], want)).toBe(true);
    // A clean pass, another code, another stage, or a later stage is not the expected refusal.
    expect(rehearsalPassed([P("R0"), P("R1")], want)).toBe(false);
    expect(rehearsalPassed([P("R0"), F("R1", "rehearsal_generation_mismatch")], want)).toBe(false);
    expect(rehearsalPassed([F("R0", want.code)], want)).toBe(false);
    expect(rehearsalPassed([P("R0"), F("R1", want.code), P("R2")], want)).toBe(false);
    expect(rehearsalPassed([], want)).toBe(false);
  });
  test("every injection but none names its exact refusal", () => {
    expect(Object.keys(INJECTION_REFUSAL).sort()).toEqual(INJECTIONS.filter((i) => i !== "none").sort());
  });
});

const u = (n: string) => `upstream:2026110504${n}_s${n}.sql`;
const o = (n: string) => `96bulls:${n}_o${n}.sql`;
const TREE = [u("5000"), u("5030"), u("6000"), u("7000"), o("0001"), o("0002"), o("0003")];

describe("seedSet", () => {
  test("the landing commit's steps are the seed; the rest is unseeded, both in tree order", () => {
    const r = seedSet({ landing: [u("5000"), u("5030"), o("0001")], tree: TREE, baseline: u("5030"), target: u("7000") });
    expect(r.seed).toEqual([u("5000"), u("5030"), o("0001")]);
    expect(r.unseeded).toEqual([u("6000"), u("7000"), o("0002"), o("0003")]);
    expect(r.lastSeed).toBe(o("0001"));
    expect(seedOrder(r.seed, r.unseeded)).toEqual([...r.seed, ...r.unseeded]);
  });
  test("refusals: unknown baseline/target, baseline not the landing tail, gaps, target already seeded", () => {
    const s = (landing: string[], baseline = u("5030"), target = u("7000")) =>
      code(() => seedSet({ landing, tree: TREE, baseline, target }));
    expect(s([u("5000")], u("9999"))).toBe("rehearsal_baseline_unknown");
    expect(s([u("5000"), u("5030")], u("5030"), u("9999"))).toBe("rehearsal_target_unknown");
    expect(s([u("5000"), u("5030"), u("6000")])).toBe("rehearsal_baseline_landing");
    expect(s([u("5030")])).toBe("rehearsal_seed_noncontiguous");
    expect(s([u("5000"), u("5030"), o("0002")])).toBe("rehearsal_seed_noncontiguous");
    expect(s([u("5000"), u("5030"), "upstream:20200101000000_gone.sql"])).toBe("rehearsal_seed_noncontiguous");
    expect(s([u("5000"), u("5030"), o("0001")], u("5030"), o("0001"))).toBe("rehearsal_target_not_after_baseline");
  });
});

describe("expectedWindow and legacyForecast", () => {
  test("pending up to and including the target, the unledgered rest held", () => {
    const ledgered = new Set([u("5000"), u("5030"), o("0001")]);
    expect(expectedWindow(TREE, ledgered, u("7000"))).toEqual({
      expectedPending: [u("6000"), u("7000")],
      held: [o("0002"), o("0003")],
    });
    expect(expectedWindow(TREE, ledgered, o("0003"))).toEqual({
      expectedPending: [u("6000"), u("7000"), o("0002"), o("0003")],
      held: [],
    });
    expect(code(() => expectedWindow(TREE, ledgered, u("9999")))).toBe("rehearsal_target_unknown");
  });
  test("a legacy full apply takes everything unledgered, including steps past the target", () => {
    const f = legacyForecast(TREE, new Set([u("5000"), u("5030")]), u("6000"));
    expect(f.fullApplyPending).toEqual([u("6000"), u("7000"), o("0001"), o("0002"), o("0003")]);
    expect(f.beyondTarget).toEqual([u("7000"), o("0001"), o("0002"), o("0003")]);
  });
});

describe("disposableEnvRefusal", () => {
  const ci = { GITHUB_ACTIONS: "true", REHEARSAL_DISPOSABLE_PG: "1", RUNNER_ENVIRONMENT: "github-hosted" };
  test("only a GitHub-hosted disposable CI job with no connection env passes", () => {
    expect(disposableEnvRefusal(ci)).toBeNull();
    expect(disposableEnvRefusal({ ...ci, POSTGRES_USER: "x", PATH: "/bin" })).toBeNull();
    expect(disposableEnvRefusal({ ...ci, GITHUB_ACTIONS: undefined })).toBe("rehearsal_not_disposable_ci");
    expect(disposableEnvRefusal({ ...ci, REHEARSAL_DISPOSABLE_PG: "true" })).toBe("rehearsal_not_disposable_ci");
    expect(disposableEnvRefusal({ ...ci, RUNNER_ENVIRONMENT: "self-hosted" })).toBe("rehearsal_not_github_hosted");
    expect(disposableEnvRefusal({ ...ci, RUNNER_ENVIRONMENT: undefined })).toBe("rehearsal_not_github_hosted");
  });
  test("any libpq connection variable, DATABASE_URL or SUPABASE_*URL* refuses", () => {
    for (const k of CONNECTION_ENV) expect(disposableEnvRefusal({ ...ci, [k]: "x" })).toBe("rehearsal_connection_env_set");
    expect(CONNECTION_ENV).toContain("PGHOST");
    expect(CONNECTION_ENV).toContain("DATABASE_URL");
    expect(CONNECTION_ENV).toContain(["PG", "PASS", "WORD"].join(""));
    expect(disposableEnvRefusal({ ...ci, ["SUPABASE_" + "DB_URL"]: "x" })).toBe("rehearsal_connection_env_set");
    expect(disposableEnvRefusal({ ...ci, ["SUPABASE_MIGRATION_" + "OWNER_URL"]: "x" })).toBe("rehearsal_connection_env_set");
    expect(disposableEnvRefusal({ ...ci, PGHOST: "" })).toBeNull();
    expect(disposableEnvRefusal({ ...ci, PGAPPNAME: "x" })).toBe("rehearsal_connection_env_set");
    expect(disposableEnvRefusal({ ...ci, PGCONNECT_TIMEOUT: "5" })).toBe("rehearsal_connection_env_set");
  });
});

describe("small helpers", () => {
  test("sameNameOid compares both directions", () => {
    const a = new Map([["x", "1"], ["y", "2"]]);
    expect(sameNameOid(a, new Map([["y", "2"], ["x", "1"]]))).toBe(true);
    expect(sameNameOid(a, new Map([["x", "1"]]))).toBe(false);
    expect(sameNameOid(a, new Map([["x", "1"], ["y", "3"]]))).toBe(false);
  });
  test("flipHex changes exactly the last digit and stays hex64", () => {
    const g = flipHex(GEN);
    expect(g).toMatch(/^[0-9a-f]{64}$/);
    expect(g).not.toBe(GEN);
    expect(g.slice(0, 63)).toBe(GEN.slice(0, 63));
    expect(flipHex("0".repeat(64)).at(-1)).toBe("1");
  });
  test("resultRecord keeps short codes only and refuses unsafe evidence", () => {
    expect(resultRecord("R4", "FAIL", {}, "release_ledger_changed")).toEqual({
      stage: "R4",
      status: "FAIL",
      code: "release_ledger_changed",
      evidence: {},
    });
    expect(resultRecord("R4", "FAIL", {}, "a message with: values").code).toBe("rehearsal_failed");
    expect(code(() => resultRecord("R9", "PASS"))).toBe("rehearsal_stage_name");
    expect(code(() => resultRecord("R1", "PASS", { url: `${"postgres"}://postgres.x:pw@db.example.com:5432/postgres` }))).toBe(
      "rehearsal_evidence_unsafe",
    );
    expect(code(() => resultRecord("R1", "PASS", { host: `aws-0.${"pool"}er.example` }))).toBe("rehearsal_evidence_unsafe");
    expect(resultRecord("R0", "PASS", { url: "postgres://postgres@127.0.0.1:5432/postgres" }).status).toBe("PASS");
    // Account ids (12 digits standing alone) and ARNs, built from fragments.
    expect(code(() => resultRecord("R6", "PASS", { id: "1234" + "56789012" }))).toBe("rehearsal_evidence_unsafe");
    expect(code(() => resultRecord("R6", "PASS", { a: "x/" + "1234" + "56789012" + "/y" }))).toBe("rehearsal_evidence_unsafe");
    expect(code(() => resultRecord("R6", "PASS", { r: "ar" + "n:partition:svc" }))).toBe("rehearsal_evidence_unsafe");
    // Migration stamps (14 digits), longer digit runs and digits inside hex are not ids.
    expect(resultRecord("R5", "PASS", { step: "upstream:20261105047000_x.sql", n: 6939 }).status).toBe("PASS");
    expect(resultRecord("R5", "PASS", { n: "1234567890123" }).status).toBe("PASS");
    expect(resultRecord("R5", "PASS", { h: "ab123456789012cd" }).status).toBe("PASS");
    expect(errorCode(Object.assign(new Error("x"), { code: "ledger_gap" }))).toBe("ledger_gap");
    expect(errorCode(new Error("secret value"))).toBe("rehearsal_failed");
  });
});

describe("drift against production and the rehearsal files", () => {
  test("the rehearsal replays the production apply's refusals in the same order", () => {
    const found = [...read("scripts/schema-release-database.ts").matchAll(/refuse\("([a-z_]+)"\)/g)].map((m) => m[1]);
    expect(found).toEqual([...PRODUCTION_APPLY_REFUSALS]);
  });
  const FILES = [
    ".github/workflows/rehearse-session-schema.yml",
    "scripts/pg-proof/schema-rehearsal.ts",
    "scripts/pg-proof/schema-rehearsal-core.ts",
    "runbooks/schema-release-rehearsal.md",
    "src/hoopdb/__tests__/schema-rehearsal.test.ts",
  ];
  // Built from fragments so this file does not match its own scan.
  const HOSTED = new RegExp(["supabase\\.(co|com)\\b", "pool" + "er\\.", "amazon" + "aws\\.com", "ar" + "n:aws"].join("|"), "i");
  const CREDENTIAL = new RegExp(
    ["DB_READ" + "ONLY_URL", "SUPABASE_MIGRATION_" + "OWNER_URL", "PGPASS" + "WORD", "pass" + "word\\s*[:=]"].join("|"),
    "i",
  );
  test("no production identifier, hosted endpoint, account id or credential in any rehearsal file", () => {
    const pins = SCHEMA_RELEASE as unknown as Record<string, string>;
    for (const f of FILES) {
      const text = read(f);
      // Compared in memory only; never printed.
      expect(text.includes(pins.project!)).toBe(false);
      expect(text.includes(pins.host!)).toBe(false);
      expect(/[0-9a-f]{32}/.test(text.replace(/[0-9a-f]{40,}/g, ""))).toBe(false);
      expect(/(?<![0-9a-zA-Z])[0-9]{12}(?![0-9a-zA-Z])/.test(text)).toBe(false);
      expect(HOSTED.test(text)).toBe(false);
      expect(CREDENTIAL.test(text)).toBe(false);
      for (const m of text.matchAll(/postgres(?:ql)?:\/\/[^\s"'`]+/g))
        expect(m[0]).toMatch(/^postgres:\/\/postgres@127\.0\.0\.1:5432\//);
    }
  });
  test("the workflow is read-only, secretless, environment-free, bounded and dispatch/PR only", () => {
    const wf = read(".github/workflows/rehearse-session-schema.yml");
    expect(wf).toMatch(/^permissions:\n {2}contents: read\n/m);
    expect(wf).not.toMatch(/secrets\./);
    expect(wf).not.toMatch(/^\s+environment:/m);
    expect(wf).not.toMatch(/^\s+push:/m);
    expect(wf).toMatch(/workflow_dispatch:/);
    expect(wf).toMatch(/fetch-depth: 0/);
    for (const m of wf.matchAll(/timeout-minutes: (\d+)/g)) expect(Number(m[1])).toBeLessThanOrEqual(60);
    expect([...wf.matchAll(/timeout-minutes:/g)].length).toBeGreaterThanOrEqual(2);
    for (const f of ["ledger-gap", "boundary-drift", "late-step", "wrong-generation", "stale-plan"]) expect(wf).toContain(f);
    // Known-negative rows reach the driver through env only; the tooling checkout carries
    // the contract module the core imports and lives outside .96bulls/.
    expect(wf).toMatch(/^ {6}expect_refusal:\n/m);
    expect(wf).toContain("REHEARSAL_EXPECT_REFUSAL: ${{ inputs.expect_refusal }}");
    expect([...wf.matchAll(/inputs\.expect_refusal/g)].length).toBe(1);
    expect(wf).toContain("/src/hoopdb/schema-release-contract.ts");
    expect(wf).toContain("path: .rehearsal-tooling");
    expect(wf).not.toContain(".96bulls");
    expect(read(".gitignore")).toMatch(/^\.rehearsal-tooling\/$/m);
    expect(wf).toMatch(/cancel-in-progress: \$\{\{ github\.event_name == 'pull_request' \}\}/);
  });
  test("the driver uses production's anchor helpers and imports no test module", () => {
    const drv = read("scripts/pg-proof/schema-rehearsal.ts");
    const prod = read("scripts/schema-release-database.ts");
    expect(prod).toMatch(/^export async function targetAnchor\(/m);
    expect(prod).toMatch(/^export function sameAnchor\(/m);
    expect(drv).toContain("D.targetAnchor(");
    expect(drv).toContain("D.sameAnchor(");
    expect(drv).not.toMatch(/a\.manifestDigest === b\.manifestDigest/);
    expect(drv).not.toMatch(/(?:mod\(|import\(|from )["'`][^"'`]*__tests__/);
    expect(drv).toContain("--no-patch");
  });
  test("the runbook says it authorizes nothing", () => {
    expect(read("runbooks/schema-release-rehearsal.md")).toContain("prerequisite proof only and authorizes nothing");
  });
});
