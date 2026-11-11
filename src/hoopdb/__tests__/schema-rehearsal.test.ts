/**
 * The pure parts of the schema release rehearsal (scripts/pg-proof/schema-rehearsal-core.ts):
 * input validation, the seed set, the pending window, the legacy forecast, result records,
 * and drift checks that tie the rehearsal to the production apply
 * (scripts/schema-release-database.ts) and keep the rehearsal files free of production
 * identifiers and secrets. The database stages run only in
 * .github/workflows/rehearse-session-schema.yml against disposable PostgreSQL.
 */
import { describe, expect, test } from "bun:test";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { accessSync, constants, existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  BOOTSTRAP_NEW_ROLES,
  bootstrapFiles,
  bootstrapRefreshEvidence,
  checkLandingSeed,
  CONNECTION_ENV,
  disposableEnvRefusal,
  expectedWindow,
  errorCode,
  flipHex,
  INJECTION_REFUSAL,
  INJECTIONS,
  LANDING_MIGRATOR_PROFILE,
  landingGeneration,
  landingMigratorEnv,
  landingTreeAtBaseline,
  legacyForecast,
  materializeLanding,
  parseExpect,
  PRODUCTION_APPLY_REFUSALS,
  redactOutput,
  rehearsalPassed,
  resultRecord,
  runLandingMigrator,
  sameNameOid,
  seedSet,
  seedStepBytes,
  validateInputs,
  type RoleEdge,
  type RoleRow,
  type Runner,
} from "../../../scripts/pg-proof/schema-rehearsal-core.ts";
import { landingCommit } from "../../../scripts/pg-proof/schema-rehearsal.ts";
import { manifestGeneration, migrationProfile, RELEASE_TREE } from "../migrate.ts";
import { treeSchemaSteps, treeVerifiers } from "../release-target.ts";
import { SCHEMA_RELEASE } from "../schema-release-contract.ts";
import { releaseProfile } from "../schema-release-inspect.ts";

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

describe("seedStepBytes (the seed's bytes are the baseline landing commit's)", () => {
  const sha = (t: string) => createHash("sha256").update(t).digest("hex");
  const rel = (n: string) => `m/${n.slice(n.indexOf(":") + 1)}`;
  const landingFiles: Record<string, string> = { [rel(u("5000"))]: "a;", [rel(u("5030"))]: "b;" };
  const releaseFiles: Record<string, string> = { ...landingFiles };
  const input = (over: Partial<Parameters<typeof seedStepBytes>[0]> = {}) => ({
    seed: [u("5000"), u("5030")],
    stepRel: rel,
    landing: (p: string) => landingFiles[p],
    release: (p: string) => releaseFiles[p],
    landingDigests: [u("5000"), u("5030")].map((n) => [n, sha(landingFiles[rel(n)]!)] as [string, string]),
    ...over,
  });
  test("each seed step carries the landing tree's bytes and digest", () => {
    expect(seedStepBytes(input()).map((f) => [f.name, f.rel, f.sha256])).toEqual([
      [u("5000"), rel(u("5000")), sha("a;")],
      [u("5030"), rel(u("5030")), sha("b;")],
    ]);
  });
  test("refuses drift, missing bytes and a seed that is not the landing tree", () => {
    // A landed migration whose release-tree bytes differ (landed migrations are frozen).
    const drifted = { ...releaseFiles, [rel(u("5030"))]: "b; -- edited" };
    expect(code(() => seedStepBytes(input({ release: (p) => drifted[p] })))).toBe("rehearsal_seed_bytes_drift");
    expect(code(() => seedStepBytes(input({ release: (p) => (p === rel(u("5000")) ? undefined : releaseFiles[p]) })))).toBe(
      "rehearsal_seed_bytes_drift",
    );
    // Bytes read that are not the landing tree digest.
    expect(code(() => seedStepBytes(input({ landingDigests: [[u("5000"), sha("a;")], [u("5030"), sha("x")]] })))).toBe(
      "rehearsal_seed_bytes_drift",
    );
    expect(code(() => seedStepBytes(input({ landing: (p) => (p === rel(u("5030")) ? undefined : landingFiles[p]) })))).toBe(
      "rehearsal_seed_bytes_missing",
    );
    expect(code(() => seedStepBytes(input({ landingDigests: [[u("5000"), sha("a;")]] })))).toBe("rehearsal_seed_step");
    expect(code(() => seedStepBytes(input({ seed: [u("5030"), u("5000")] })))).toBe("rehearsal_seed_step");
  });

  // The smoke row's baseline (045030), landed by 55511978. Needs full history (the
  // rehearsal workflow checks out with fetch-depth 0); skipped in a shallow checkout.
  const LANDING = "55511978bf1b1cd03f6338c4082a1d2558c8401b";
  const git = (args: string[]) => spawnSync("git", args, { cwd: ROOT, maxBuffer: 256 * 1024 * 1024 });
  const haveLanding = git(["cat-file", "-e", `${LANDING}^{commit}`]).status === 0;
  const at = (c: string) => (p: string) => {
    const r = git(["cat-file", "blob", `${c}:${p}`]);
    return r.status === 0 ? r.stdout.toString("utf8") : undefined;
  };
  test.skipIf(!haveLanding)("baseline 045030: 55511978's tree ends at it, its bytes are the release's, generation 7db65adb", () => {
    expect(landingCommit("HEAD", BASE, ROOT)).toBe(LANDING);
    const landingSteps = treeSchemaSteps(ROOT, LANDING);
    const seed = landingSteps.map(([n]) => n);
    expect(seed.filter((n) => n.startsWith("upstream:")).at(-1)).toBe(BASE);
    expect(code(() => landingTreeAtBaseline(seed, BASE))).toBeNull();
    const lanes = new Map(RELEASE_TREE.lanes.map((l) => [l.prefix, l.dir]));
    const stepRel = (n: string) => `${lanes.get(n.slice(0, n.indexOf(":") + 1))}/${n.slice(n.indexOf(":") + 1)}`;
    const files = seedStepBytes({ seed, stepRel, landing: at(LANDING), release: at("HEAD"), landingDigests: landingSteps });
    for (const f of files) expect(f.sha256).toBe(sha(at(LANDING)(f.rel)!));
    // The landing migrator's verifiers are not all the release working tree's: one
    // reason R0 runs the landing commit's own migrator.
    const verifiers = treeVerifiers(ROOT, LANDING);
    const differs = verifiers.filter((v, i) => v.sha256 !== sha(read(RELEASE_TREE.verifiers[i]!.path))).map((v) => v.name);
    expect(differs).toContain("verify:session_ingest");
    // The release catalog lists relations 046000 creates; the landing commit's does not
    // (the 42P01 the release migrator raises on the baseline database).
    expect(read("src/hoopdb/migrate-guards.ts")).toContain("sessions.session_analysis_jobs_v2");
    expect(at(LANDING)("src/hoopdb/migrate-guards.ts")).not.toContain("sessions.session_analysis_jobs_v2");
    // The seed generation is production's canonical generation at the baseline.
    const gen = manifestGeneration({
      steps: files.map((f) => [f.name, f.sha256]),
      verifiers: verifiers.map((v) => [v.name, v.sha256]),
    });
    expect(gen.slice(0, 8)).toBe("7db65adb");
  });
});

describe("R0 landing migrator (offline: fixture repo and injected runner)", () => {
  const BASE_ENV = { PATH: process.env.PATH ?? "", HOME: process.env.HOME ?? "" };
  const real: Runner = (cmd, args, opts) => {
    const r = spawnSync(cmd, [...args], { cwd: opts.cwd, env: opts.env, encoding: "utf8", timeout: opts.timeoutMs });
    return { status: r.error ? null : r.status, signal: r.signal, stdout: r.stdout ?? "", stderr: r.stderr ?? "" };
  };
  const ok = (stdout = ""): ReturnType<Runner> => ({ status: 0, signal: null, stdout, stderr: "" });
  const scratch = () => mkdtempSync(join(tmpdir(), "rehearsal-test-"));
  const cleanup = (d: string) => {
    spawnSync("chmod", ["-R", "u+w", d]);
    rmSync(d, { recursive: true, force: true });
  };
  /** A two-commit repo: the first adds 045000 (and a migrator), the second 045030. */
  const fixture = (withMigrator = true) => {
    const repo = scratch();
    const g = (...args: string[]) => {
      const r = spawnSync("git", args, {
        cwd: repo,
        encoding: "utf8",
        env: { ...BASE_ENV, GIT_AUTHOR_NAME: "t", GIT_AUTHOR_EMAIL: "t@x", GIT_COMMITTER_NAME: "t", GIT_COMMITTER_EMAIL: "t@x" },
      });
      if (r.status !== 0) throw new Error(r.stderr);
      return r.stdout.trim();
    };
    const put = (p: string, text: string) => {
      mkdirSync(join(repo, p, ".."), { recursive: true });
      writeFileSync(join(repo, p), text);
    };
    g("init", "-q", "-b", "main");
    put("supabase/migrations/20261105045000_a.sql", "select 1;");
    put("supabase/bootstrap/b.sql", "select 2;");
    put("bun.lock", "{}");
    put("package.json", "{}");
    put("tsconfig.json", "{}");
    if (withMigrator) put("src/hoopdb/migrate.ts", "export {};");
    g("add", "-A");
    g("commit", "-q", "-m", "one");
    const first = g("rev-parse", "HEAD");
    put("supabase/migrations/20261105045030_b.sql", "select 3;");
    g("add", "-A");
    g("commit", "-q", "-m", "two");
    return { repo, first, second: g("rev-parse", "HEAD") };
  };

  test("landingCommit: the first-parent commit that added the step, or rehearsal_landing_missing", () => {
    const f = fixture();
    try {
      expect(landingCommit(f.second, "upstream:20261105045030_b.sql", f.repo)).toBe(f.second);
      expect(landingCommit(f.second, "upstream:20261105045000_a.sql", f.repo)).toBe(f.first);
      expect(code(() => landingCommit(f.second, "upstream:20261105046000_never.sql", f.repo))).toBe("rehearsal_landing_missing");
      expect(code(() => landingCommit(f.first, "upstream:20261105045030_b.sql", f.repo))).toBe("rehearsal_landing_missing");
    } finally {
      cleanup(f.repo);
    }
  });

  test("landingTreeAtBaseline: a landing tree with a later step in the baseline lane refuses", () => {
    expect(code(() => landingTreeAtBaseline([u("5000"), u("5030"), o("0001")], u("5030")))).toBeNull();
    expect(code(() => landingTreeAtBaseline([u("5000"), u("5030"), u("6000"), o("0001")], u("5030")))).toBe(
      "rehearsal_landing_tree_above_baseline",
    );
  });

  test("materializeLanding: git object database tree, frozen install, read-only; each failure its own code", () => {
    const f = fixture();
    const calls: string[][] = [];
    const bunOk: Runner = (cmd, args, opts) => {
      calls.push([cmd, ...args]);
      return cmd === "bun-stub" ? ok() : real(cmd, args, opts);
    };
    const dirs: string[] = [];
    const fresh = () => {
      const d = scratch();
      dirs.push(d);
      return d;
    };
    const m = (over: Partial<Parameters<typeof materializeLanding>[0]>) =>
      materializeLanding({ repo: f.repo, landing: f.first, dir: fresh(), bun: "bun-stub", run: bunOk, exists: existsSync, base: BASE_ENV, ...over });
    try {
      const dir = fresh();
      // The working tree is dirty with a file the landing commit never had: not materialized.
      writeFileSync(join(f.repo, "supabase/migrations/20261105099999_worktree.sql"), "select 9;");
      m({ dir });
      expect(existsSync(join(dir, "supabase/migrations/20261105045000_a.sql"))).toBe(true);
      expect(existsSync(join(dir, "supabase/migrations/20261105045030_b.sql"))).toBe(false);
      expect(existsSync(join(dir, "supabase/migrations/20261105099999_worktree.sql"))).toBe(false);
      expect(existsSync(`${dir}.tar`)).toBe(false);
      expect(calls).toContainEqual(["bun-stub", "install", "--frozen-lockfile"]);
      expect(calls.find((c) => c[0] === "git")).toEqual(["git", "archive", "--format=tar", "-o", `${dir}.tar`, f.first]);
      if (process.getuid?.() !== 0)
        expect(code(() => accessSync(join(dir, "supabase/migrations/20261105045000_a.sql"), constants.W_OK))).not.toBeNull();
      // Refusals: not a full commit id; unknown commit; no migrator in the tree; install fails.
      expect(code(() => m({ landing: "main" }))).toBe("rehearsal_landing_missing");
      expect(code(() => m({ landing: "f".repeat(40) }))).toBe("rehearsal_landing_materialize");
      expect(code(() => m({ exists: () => false }))).toBe("rehearsal_landing_materialize");
      const bunFails: Runner = (cmd, args, opts) => (cmd === "bun-stub" ? { status: 1, signal: null, stdout: "", stderr: "lockfile" } : real(cmd, args, opts));
      expect(code(() => m({ run: bunFails }))).toBe("rehearsal_landing_install");
    } finally {
      for (const d of dirs) cleanup(d);
      cleanup(f.repo);
    }
    const g = fixture(false);
    const d = scratch();
    try {
      expect(code(() => materializeLanding({ repo: g.repo, landing: g.first, dir: d, bun: "bun-stub", run: bunOk, exists: existsSync, base: BASE_ENV }))).toBe(
        "rehearsal_landing_materialize",
      );
    } finally {
      cleanup(d);
      cleanup(g.repo);
    }
  });

  test("landingGeneration: the landing CLI's --generation must be the release code's A", () => {
    const A = "7".repeat(64);
    const gen = (r: ReturnType<Runner>, expected = A) => code(() => landingGeneration({ dir: "/x", bun: "bun", run: () => r, env: BASE_ENV, expected }));
    expect(gen(ok(A + "\n"))).toBeNull();
    expect(gen(ok("6".repeat(64) + "\n"))).toBe("rehearsal_landing_generation_mismatch");
    expect(gen(ok("not a generation"))).toBe("rehearsal_landing_generation");
    expect(gen({ status: 1, signal: null, stdout: A, stderr: "" })).toBe("rehearsal_landing_generation");
    let argv: readonly string[] = [];
    landingGeneration({ dir: "/x", bun: "bun", run: (_c, a) => ((argv = a), ok(A)), env: BASE_ENV, expected: A });
    expect(argv).toEqual(["/x/src/hoopdb/migrate.ts", "--generation"]);
  });

  test("runLandingMigrator: non-zero exit refuses; output is recorded redacted either way", () => {
    const logs: { exit: number | null; stdout: string; stderr: string }[] = [];
    const url = ["postgres", "//postgres@127.0.0.1:5432/postgres"].join(":");
    const run =
      (status: number | null, signal: string | null = null): Runner =>
      () => ({ status, signal, stdout: `applied ${url}\n`, stderr: "error_code 42P01 host aws-0.x." + "pool" + "er.example\n" });
    const go = (r: Runner) =>
      code(() => runLandingMigrator({ dir: "/x", bun: "bun", run: r, env: BASE_ENV, timeoutMs: 1000, record: (l) => logs.push(l) }));
    expect(go(run(0))).toBeNull();
    expect(go(run(1))).toBe("rehearsal_landing_migrator_failed");
    expect(go(run(null, "SIGKILL"))).toBe("rehearsal_landing_migrator_failed");
    expect(logs.map((l) => l.exit)).toEqual([0, 1, null]);
    for (const l of logs) {
      expect(l.stdout).not.toContain("127.0.0.1");
      expect(l.stdout).toContain("[redacted-url]");
      expect(l.stderr).toContain("42P01");
      expect(l.stderr).not.toMatch(/pool[e]r/i);
    }
    expect(redactOutput(Array.from({ length: 200 }, (_, i) => `l${i}`).join("\n")).split("\n")).toHaveLength(80);
  });

  test("checkLandingSeed: ledger = landing names and digests, marker pass at A", () => {
    const A = "7".repeat(64);
    const landingDigests: [string, string][] = [
      [u("5000"), "1".repeat(64)],
      [u("5030"), "2".repeat(64)],
    ];
    const ledger = new Map(landingDigests);
    const pass = { generation: A, status: "pass" };
    const chk = (over: Partial<Parameters<typeof checkLandingSeed>[0]>) =>
      code(() => checkLandingSeed({ landingDigests, ledger, marker: pass, generation: A, ...over }));
    expect(chk({})).toBeNull();
    expect(chk({ ledger: new Map([...landingDigests, [u("6000"), "3".repeat(64)]]) })).toBe("rehearsal_seed_ledger");
    expect(chk({ ledger: new Map(landingDigests.slice(0, 1)) })).toBe("rehearsal_seed_ledger");
    expect(chk({ ledger: new Map([landingDigests[0]!, [u("5030"), "9".repeat(64)]]) })).toBe("rehearsal_seed_ledger_digest");
    expect(chk({ marker: undefined })).toBe("rehearsal_seed_marker");
    expect(chk({ marker: { generation: A, status: "pending" } })).toBe("rehearsal_seed_marker");
    expect(chk({ marker: { generation: "6".repeat(64), status: "pass" } })).toBe("rehearsal_seed_marker");
  });

  test("the landing migrator's env is exactly the production supabase profile, nothing inherited", () => {
    const env = landingMigratorEnv({ PATH: "/bin", HOME: "/h" }, SCHEMA_RELEASE.runtime, "u");
    expect(Object.keys(env).sort()).toEqual(
      [
        "HOME",
        "HOOPDB_DB_MODE",
        "HOOPDB_FUNCTION_BOUNDARY_MODE",
        "HOOPDB_ROLE_BOOTSTRAP",
        "HOOPDB_ROLE_VERIFY",
        "HOOPDB_RUNTIME_ROLE",
        "HOOPDB_RUNTIME_ROLE_LOGIN",
        "HOOPDB_SUPABASE_SHIM",
        "HOOPDB_URL",
        "PATH",
      ].sort(),
    );
    expect(migrationProfile({ ...LANDING_MIGRATOR_PROFILE, HOOPDB_RUNTIME_ROLE: SCHEMA_RELEASE.runtime })).toEqual(
      releaseProfile("global-default-revoke"),
    );
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

describe("R2-bootstrap (the release tree's bootstrap chain before the apply)", () => {
  const h = (t: string) => createHash("sha256").update(t).digest("hex");
  const bare = (rolname: string, over: Partial<RoleRow> = {}): RoleRow => ({
    rolname,
    rolsuper: false,
    rolinherit: false,
    rolcreaterole: false,
    rolcreatedb: false,
    rolcanlogin: false,
    rolreplication: false,
    rolbypassrls: false,
    rolconnlimit: -1,
    ...over,
  });
  const BEFORE = [bare("bulls96_read_api"), bare("postgres", { rolcanlogin: true, rolcreaterole: true, rolinherit: true }), bare("supabase_admin", { rolsuper: true })];
  const ADMIT = BOOTSTRAP_NEW_ROLES[0];
  const changed = bootstrapFiles([
    { file: "b/a.sql", release: "a", landing: "a" },
    { file: "b/r.sql", release: "r2", landing: "r1" },
  ]).files;
  const same = bootstrapFiles([{ file: "b/a.sql", release: "a", landing: "a" }]).files;

  test("bootstrapFiles: release bytes and digests, changed flags, landing absent is null, release missing refuses", () => {
    const f = bootstrapFiles([
      { file: "b/a.sql", release: "a", landing: "a" },
      { file: "b/r.sql", release: "r2", landing: "r1" },
      { file: "b/n.sql", release: "n", landing: undefined },
    ]);
    expect(f.texts).toEqual(["a", "r2", "n"]);
    expect(f.files).toEqual([
      { file: "b/a.sql", release_sha256: h("a"), landing_sha256: h("a"), changed: false },
      { file: "b/r.sql", release_sha256: h("r2"), landing_sha256: h("r1"), changed: true },
      { file: "b/n.sql", release_sha256: h("n"), landing_sha256: null, changed: true },
    ]);
    expect(code(() => bootstrapFiles([{ file: "b/x.sql", release: undefined, landing: "x" }]))).toBe("rehearsal_bootstrap_bytes_missing");
  });
  test("a changed chain creating the admission role: sorted names, flags, no edges, nothing else", () => {
    const e = bootstrapRefreshEvidence({ files: changed, before: BEFORE, after: [...BEFORE, bare(ADMIT)], edges: [], applied: 2 });
    expect(e.roles_created).toEqual([ADMIT]);
    expect(e.applied).toBe(2);
    expect(e.files.map((f) => f.changed)).toEqual([false, true]);
    expect(e.role_count).toEqual({ before: 3, after: 4 });
    const { rolname: _name, ...flags } = bare(ADMIT);
    expect(e.roles).toEqual([{ name: ADMIT, present: true, preexisting: false, attributes: flags, members_in: [], member_of: [] }]);
    expect(JSON.stringify(e)).not.toMatch(/passw|rolconfig|rolvaliduntil/i);
    // roles_created is sorted whatever the snapshot order.
    const two = bootstrapRefreshEvidence({
      files: changed,
      before: BEFORE,
      after: [bare("zz_new"), ...BEFORE, bare("aa_new")],
      edges: [],
      applied: 1,
      expected: ["zz_new", "aa_new"],
    });
    expect(two.roles_created).toEqual(["aa_new", "zz_new"]);
  });
  test("pre-existing exact role: preexisting true, nothing created; absent role is reported, not refused", () => {
    const e = bootstrapRefreshEvidence({ files: same, before: [...BEFORE, bare(ADMIT)], after: [...BEFORE, bare(ADMIT)], edges: [], applied: 1 });
    expect(e.roles_created).toEqual([]);
    expect(e.roles[0]).toMatchObject({ name: ADMIT, present: true, preexisting: true });
    const none = bootstrapRefreshEvidence({ files: same, before: BEFORE, after: BEFORE, edges: [], applied: 1 });
    expect(none.roles).toEqual([{ name: ADMIT, present: false }]);
  });
  test("unexpected role: any other new role, or any new role from an unchanged chain", () => {
    expect(code(() => bootstrapRefreshEvidence({ files: changed, before: BEFORE, after: [...BEFORE, bare(ADMIT), bare("other")], edges: [], applied: 2 }))).toBe(
      "rehearsal_bootstrap_unexpected_role",
    );
    expect(code(() => bootstrapRefreshEvidence({ files: same, before: BEFORE, after: [...BEFORE, bare(ADMIT)], edges: [], applied: 1 }))).toBe(
      "rehearsal_bootstrap_unexpected_role",
    );
  });
  test("side effect: a role removed, a pre-existing role altered, or a row count mismatch", () => {
    const base = { files: changed, edges: [] as RoleEdge[], applied: 2 };
    expect(code(() => bootstrapRefreshEvidence({ ...base, before: BEFORE, after: [BEFORE[0]!, BEFORE[2]!, bare(ADMIT)] }))).toBe(
      "rehearsal_bootstrap_side_effect",
    );
    expect(
      code(() => bootstrapRefreshEvidence({ ...base, before: BEFORE, after: [BEFORE[0]!, { ...BEFORE[1]!, rolsuper: true }, BEFORE[2]!, bare(ADMIT)] })),
    ).toBe("rehearsal_bootstrap_side_effect");
    expect(code(() => bootstrapRefreshEvidence({ ...base, before: BEFORE, after: [...BEFORE, bare(ADMIT), bare(ADMIT)] }))).toBe(
      "rehearsal_bootstrap_side_effect",
    );
  });
  test("role contract: any elevated flag, LOGIN, or any inbound or outbound edge refuses (also when pre-existing)", () => {
    for (const flag of ["rolsuper", "rolinherit", "rolcreaterole", "rolcreatedb", "rolcanlogin", "rolreplication", "rolbypassrls"] as const)
      expect(
        code(() => bootstrapRefreshEvidence({ files: changed, before: BEFORE, after: [...BEFORE, bare(ADMIT, { [flag]: true })], edges: [], applied: 2 })),
      ).toBe("rehearsal_bootstrap_role_contract");
    const edge = (member: string, role: string): RoleEdge => ({ member, role, admin_option: true, inherit_option: false, set_option: false });
    for (const e of [edge("postgres", ADMIT), edge(ADMIT, "bulls96_read_api")])
      expect(code(() => bootstrapRefreshEvidence({ files: changed, before: BEFORE, after: [...BEFORE, bare(ADMIT)], edges: [e], applied: 2 }))).toBe(
        "rehearsal_bootstrap_role_contract",
      );
    expect(
      code(() => bootstrapRefreshEvidence({ files: same, before: [...BEFORE, bare(ADMIT, { rolcanlogin: true })], after: [...BEFORE, bare(ADMIT, { rolcanlogin: true })], edges: [], applied: 1 })),
    ).toBe("rehearsal_bootstrap_role_contract");
    // Edges between other roles are not the checked role's.
    expect(
      bootstrapRefreshEvidence({ files: changed, before: BEFORE, after: [...BEFORE, bare(ADMIT)], edges: [edge("postgres", "bulls96_read_api")], applied: 2 }).roles[0],
    ).toMatchObject({ members_in: [], member_of: [] });
  });
  test("missing-bootstrap expects 42704 and the R2-bootstrap evidence passes the evidence screen", () => {
    expect(INJECTION_REFUSAL["missing-bootstrap"]).toBe("42704");
    const e = bootstrapRefreshEvidence({ files: changed, before: BEFORE, after: [...BEFORE, bare(ADMIT)], edges: [], applied: 2 });
    expect(resultRecord("R2-bootstrap", "PASS", e).status).toBe("PASS");
    expect(resultRecord("R2-bootstrap-rerun", "PASS", e).status).toBe("PASS");
    expect(resultRecord("R4-missing-bootstrap", "PASS", {}).stage).toBe("R4-missing-bootstrap");
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
    for (const f of ["ledger-gap", "boundary-drift", "late-step", "wrong-generation", "stale-plan", "missing-bootstrap"]) expect(wf).toContain(f);
    // The dispatch choice list is exactly the driver's injections, in order.
    const options = /^ {6}inject_failure:\n(?: {8}.*\n)*? {8}options:\n((?: {10}- .*\n)+)/m.exec(wf);
    expect(options).not.toBeNull();
    expect(options![1]!.trim().split("\n").map((l) => l.trim().slice(2))).toEqual([...INJECTIONS]);
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
