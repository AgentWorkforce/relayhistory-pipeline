import { describe, expect, test } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { parseCliLogin, ownerPlanInputs, ownerPlanBootstrap, withOwnerPlanClient } from "../../../scripts/schema-release-owner-operator-plan.ts";
import { bootstrapApproval, bootstrapChainDigest, bootstrapChainRoles, type BootstrapState } from "../schema-release-bootstrap.ts";
import { SESSION_BOOTSTRAP_FILES } from "../session-bootstrap-chain.ts";
const good = ["export PGHOST='db.pyamkrhoyszgzirqqffq.supabase.co'", "export PGPORT='5432'", "export PGUSER='cli_login_postgres'", "export PGPASSWORD='fake-unit-only-credential-value'", "export PGDATABASE='postgres'"].join("\n");
describe("existing approved project owner plan", () => {
  test("reads CLI credential in memory without interpreting unrelated shell commands", () => { const p = parseCliLogin(good + "\nrm -rf /never-run-this-unit-fixture"); expect(p.password).toBe("fake-unit-only-credential-value"); });
  for (const [from, to] of [["pyamkrhoyszgzirqqffq", "anotherproject"], ["5432", "6543"], ["cli_login_postgres", "postgres"], ["PGDATABASE='postgres'", "PGDATABASE='other'"], ["fake-unit-only-credential-value", "[REDACTED]"], ["fake-unit-only-credential-value", "x"]]) {
    test(`refuses altered credential target ${from}`, () => expect(() => parseCliLogin(good.replace(from, to))).toThrow());
  }
  test("rejects duplicate fields instead of accepting a last-value override", () => expect(() => parseCliLogin(good + "\nexport PGUSER='postgres'")).toThrow());
  test("does not execute unquoted command substitution", () => expect(() => parseCliLogin(good.replace("PGUSER='cli_login_postgres'", "PGUSER=$(touch /never-unit-fixture)"))).toThrow());
  test("refuses oversized CLI output", () => expect(() => parseCliLogin(good + "x".repeat(65536))).toThrow());
  test("refuses missing credential fields", () => expect(() => parseCliLogin(good.split("\n").filter(s => !s.startsWith("export PGPASSWORD=")).join("\n"))).toThrow());
  const env = { OPERATOR_MODE: "plan", OPERATOR_SOURCE_SHA: "a".repeat(40), OPERATOR_GENERATION: "b".repeat(64) };
  test("accepts a pinned read-only plan invocation", () => expect(ownerPlanInputs(env, ["node", "script"])).toEqual({ sha: "a".repeat(40), digest: "b".repeat(64) }));
  for (const change of [{ OPERATOR_MODE: "apply" }, { OPERATOR_MODE: undefined }, { OPERATOR_SOURCE_SHA: "main" }, { OPERATOR_GENERATION: "latest" }]) test(`refuses unsafe mode or unpinned source ${JSON.stringify(change)}`, () => expect(() => ownerPlanInputs({ ...env, ...change }, ["node", "script"])).toThrow());
  test("rejects an apply argument even in plan mode", () => expect(() => ownerPlanInputs(env, ["node", "script", "apply"])).toThrow());
});

describe("operator connection cleanup without network", () => {
  for (const failure of ["connect", "begin", "work", "rollback", "none"] as const) {
    test(`closes when ${failure} fails; rolls back only a begun transaction`, async () => {
      const calls: string[] = [];
      const client = {
        async connect() { calls.push("connect"); if (failure === "connect") throw new Error("connect"); },
        async query(sql: string) {
          const kind = sql.startsWith("BEGIN") ? "begin" : "rollback";
          calls.push(kind);
          if (failure === kind) throw new Error(kind);
          return {};
        },
        async end() { calls.push("end"); },
      };
      const result = withOwnerPlanClient(client, async () => { calls.push("work"); if (failure === "work") throw new Error("work"); return 42; });
      if (["connect", "begin", "work"].includes(failure)) await expect(result).rejects.toThrow(failure);
      else expect(await result).toBe(42);
      expect(calls).toEqual(failure === "connect" ? ["connect", "end"] : failure === "begin" ? ["connect", "begin", "end"] : ["connect", "begin", "work", "rollback", "end"]);
    });
  }
});

describe("owner operator plan bootstrap section (pure, no network)", () => {
  const names = bootstrapChainRoles().map(r => r.name);
  const role = (rolname: string, o: object = {}) => ({ rolname, rolsuper: false, rolinherit: false, rolcreaterole: false, rolcreatedb: false, rolcanlogin: false, rolreplication: false, rolbypassrls: false, rolconnlimit: -1, ...o });
  const state = (absent: string[], o: Partial<BootstrapState> = {}): BootstrapState => ({ serverVersionNum: 160004, roles: names.filter(n => !absent.includes(n)).map(n => role(n)), edges: [], settings: [{ role: "session_directory_search_worker", global: true, kind: "statement_timeout=2s" }], ...o });
  const repo = new URL("../../../", import.meta.url);
  // Release = target = the source sha: HEAD chain bytes on both sides; steps: the source tree's upstream steps.
  const files = SESSION_BOOTSTRAP_FILES.map(file => { const b = readFileSync(new URL(file, repo)); return { file, release: b, target: b }; });
  const steps = readdirSync(new URL("supabase/migrations/", repo)).filter(f => f.endsWith(".sql")).sort().map(f => `upstream:${f}`);
  const sha = "a".repeat(40);
  const plan = (st: BootstrapState) => ownerPlanBootstrap(sha, steps, files, st);
  const creator = (r: string, o: object = {}) => ({ role: r, member: "postgres", member_inherit: true, grantor: "supabase_admin", grantor_super: true, admin_option: true, inherit_option: false, set_option: false, ...o });
  const code = (f: () => unknown) => { try { f(); } catch (e) { return (e as { code?: string }).code; } return "none"; };
  const err = console.error;
  test("absent admission role: required, the plan says bootstrap stage required before apply", () => {
    const s = plan(state(["bulls96_analysis_admit"]));
    expect(s).toMatchObject({ required: true, will_create: ["bulls96_analysis_admit"], next: "bootstrap stage required before apply" });
    expect(s.chain_digest).toBe(bootstrapChainDigest(s));
    // Target = the source tree's last step, target_sha = the source sha: every chain role required.
    expect(s.target).toBe(steps[steps.length - 1]);
    expect(s.target_sha).toBe(sha);
    expect(s.required_roles).toEqual([...names].sort());
    expect([s.excluded, s.mixed, s.release_drift]).toEqual([[], [], []]);
    expect(s.files.map(f => f.differs)).toEqual(SESSION_BOOTSTRAP_FILES.map(() => false));
    expect(bootstrapApproval({ project: "<project>", sha: "a".repeat(40), target: "t", targetSha: "a".repeat(40), chainDigest: s.chain_digest, willCreate: s.will_create, planRun: "7" })).toBe(`BOOTSTRAP <project> ${"a".repeat(40)} TARGET t ${"a".repeat(40)} CHAIN ${s.chain_digest} ROLES bulls96_analysis_admit PLAN 7`);
  });
  test("an empty source tree (no target step) refuses operator_source_pin", () => {
    expect(code(() => ownerPlanBootstrap(sha, [], files, state([])))).toBe("operator_source_pin");
  });
  test("every role exact: skip (required false, ROLES none)", () => {
    const s = plan(state([]));
    expect(s).toMatchObject({ required: false, will_create: [], next: "apply" });
    expect(bootstrapApproval({ project: "p", sha: "s", target: "t", targetSha: "u", chainDigest: s.chain_digest, willCreate: s.will_create, planRun: "1" })).toEndWith("ROLES none PLAN 1");
  });
  test("PG16 creator edge accepted (supabase mode); PG15 inbound and non-creator edges refuse as drift", () => {
    expect(plan(state([], { edges: [creator("bulls96_read_api")] })).required).toBe(false);
    console.error = () => {};
    try {
      expect(code(() => plan(state([], { serverVersionNum: 150010, edges: [creator("bulls96_read_api", { inherit_option: null, set_option: null })] })))).toBe("release_bootstrap_role_drift");
      expect(code(() => plan(state([], { edges: [creator("bulls96_read_api", { member: "anon" })] })))).toBe("release_bootstrap_role_drift");
      expect(code(() => plan(state([], { edges: [{ ...creator("authenticated"), member: "bulls96_read_api" }] })))).toBe("release_bootstrap_role_drift");
    } finally { console.error = err; }
  });
  test("owner plan reads the section inside its READ ONLY transaction, release = target = source sha", () => {
    const src = readFileSync(new URL("../../../scripts/schema-release-owner-operator-plan.ts", import.meta.url), "utf8");
    expect(src).toContain("const steps = (await schemaStepDigests([])).map(([name]) => name);");
    expect(src).toContain("ownerPlanBootstrap(input.sha, steps, releaseBootstrapFiles(process.cwd(), input.sha, input.sha), await readBootstrapState(client))");
    expect(src.indexOf("pg_advisory_xact_lock(961996,1)")).toBeLessThan(src.indexOf("await readBootstrapState(client)"));
    expect(src).toContain("...report, bootstrap }");
  });
});
