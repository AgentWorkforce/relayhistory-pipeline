import { test, expect } from "bun:test";
import {
  OWNER_APPLY as A,
  digest,
  roleAudit,
  wrapperSource,
  ownerCapture,
  validateOwnerApply,
  validateReadonlyPlan,
  withOwnerSession,
  ownerFencedClient,
  auditCanonicalOwnerSql,
  applyOwnerRelease,
} from "../owner-operator-apply.ts";
import { ownerApplyArgs, ownerApplySourceFiles } from "../../../scripts/schema-release-owner-apply.ts";
const pin = { sha: "a".repeat(40), generation: "b".repeat(64) },
  now = Date.now();
function boundary() {
  return {
    present: true,
    event: "ddl_command_end",
    tags_exact: true,
    enabled_ok: true,
    owner_is_migration_owner: true,
    function_is_reviewed: true,
    function_owner_is_migration_owner: true,
    schema_owner_is_migration_owner: true,
    prosrc_sha256: "c".repeat(64),
    prosrc_exact: false,
    proconfig_exact: true,
    security_definer: false,
    volatility: "v",
    language: "plpgsql",
    returns: "event_trigger",
    function_public_execute: false,
    runtime_role_setting_matches: true,
    schema_private: true,
    verified: false,
  };
}
async function evidence() {
  const wrapper = await wrapperSource(),
    report = {
      generation: pin.generation,
      ledgerSha256: A.ledger,
      inventory: [{ name: "fixture", sha256: "d".repeat(64), pending: true }],
      acl: { function_acl_drift_extra: [], function_acl_missing_refused: [] },
    };
  const planText = JSON.stringify({
    version: 1,
    status: "planned_readonly_not_apply_approval",
    at: new Date(now).toISOString(),
    project: A.project,
    sourceSha: pin.sha,
    boundary: "global-default-revoke",
    ...report,
  });
  const plan = validateReadonlyPlan(planText, pin, now),
    capture = ownerCapture(
      planText,
      plan,
      pin,
      { ...report, existingBoundary: boundary() },
      wrapper,
      new Date(now),
    ),
    captureText = JSON.stringify(capture);
  const approval = {
    version: 1,
    decision: "approved",
    by: "Khaliq Gant",
    execution: A.execution,
    quote: "Synthetic offline fixture only; not authorization",
    at: new Date(now).toISOString(),
    project: A.project,
    sourceSha: pin.sha,
    generation: pin.generation,
    ledgerSha256: A.ledger,
    planSha256: digest(planText),
    captureSha256: digest(captureText),
    captureGeneration: capture.captureGeneration,
    wrapperSha256: wrapper.sha256,
  };
  return { wrapper, planText, capture, captureText, approval };
}
test("current ledger/source/gen/plan/capture/wrapper/manual exception are exact and one-hour bounded", async () => {
  const x = await evidence();
  expect(
    validateOwnerApply(
      x.planText,
      x.captureText,
      JSON.stringify(x.approval),
      pin,
      x.wrapper,
      now,
    ).capture.captureGeneration,
  ).toBe(x.capture.captureGeneration);
  for (const changed of [
    { execution: "future-github" },
    { sourceSha: "e".repeat(40) },
    { ledgerSha256: "f".repeat(64) },
    { planSha256: "f".repeat(64) },
    { captureSha256: "f".repeat(64) },
    { wrapperSha256: "f".repeat(64) },
    { decision: "pending" },
  ])
    expect(() =>
      validateOwnerApply(
        x.planText,
        x.captureText,
        JSON.stringify({ ...x.approval, ...changed }),
        pin,
        x.wrapper,
        now,
      ),
    ).toThrow();
  expect(() =>
    validateOwnerApply(
      x.planText,
      x.captureText,
      JSON.stringify(x.approval),
      pin,
      x.wrapper,
      now + 3600000,
    ),
  ).toThrow("owner_plan_stale");
  expect(() =>
    validateOwnerApply(
      x.planText,
      x.captureText,
      JSON.stringify(x.approval),
      pin,
      x.wrapper,
      now - 31000,
    ),
  ).toThrow("owner_plan_stale");
});
test("generated refresh is a single existing-function replacement using the exact committed body", async () => {
  const x = await wrapperSource();
  expect(digest(x.sql)).toBe(x.sha256);
  expect(
    x.sql.startsWith(
      "CREATE OR REPLACE FUNCTION hoopdb_boundary.sessions_function_boundary()",
    ),
  ).toBe(true);
  expect(x.sql).not.toContain("CREATE EVENT TRIGGER");
  expect(x.sql).not.toContain("ALTER EVENT TRIGGER");
  expect(x.sql).toContain("v_runtime text := 'bulls96_runtime';");
});
test("role-control audit refuses explicit and dynamic role resets but permits comments and normal settings", () => {
  for (const sql of [
    'SET "role" TO NONE',
    "SELECT set_config($$role$$,'none',false)",
    "RESET ROLE;CREATE TABLE bad(x int)",
    "SET SESSION ROLE postgres",
    "DO $$ BEGIN RESET ROLE; END $$",
    "SELECT set_config('role','none',false)",
    "SET SESSION AUTHORIZATION another",
    "SELECT '--'; RESET ROLE",
  ])
    expect(() => roleAudit(sql)).toThrow("owner_sql_role_control");
  expect(() =>
    roleAudit("-- RESET ROLE\n SELECT 1 /* SET ROLE bad */;"),
  ).not.toThrow();
  expect(() =>
    roleAudit("SELECT set_config('hoopdb.db_mode','supabase',true)"),
  ).not.toThrow();
});
test("canonical actual source role audit is read-only and passes unchanged", async () => {
  await auditCanonicalOwnerSql();
});
function fake(failure = "", resetOwner = false) {
  const calls: string[] = [];
  let role = "postgres",
    inTx = false;
  return {
    calls,
    client: {
      async connect() {
        calls.push("connect");
        if (failure === "connect") throw Error(failure);
      },
      async query(sql: string) {
        calls.push(sql);
        if (sql.startsWith("BEGIN") && failure === "begin")
          throw Error(failure);
        if (sql.startsWith("BEGIN")) inTx = true;
        if (sql === "ROLLBACK") { inTx = false; role = failure === "rollback_role" ? "cli_login_postgres" : "postgres"; }
        if (sql === "SET SESSION ROLE postgres" || (sql === "SET LOCAL ROLE postgres" && failure !== "local_role")) role = "postgres";
        if (sql === "RESET ROLE")
          role = resetOwner ? "postgres" : "cli_login_postgres";
        if (sql.includes("current_user AS owner"))
          return {
            rows: [
              {
                owner: role,
                login: A.login,
                db: A.database,
                schemaOwner: A.owner,
                role: role === "cli_login_postgres" ? "none" : role,
                pid: 42,
                ro: inTx ? "on" : "off",
              },
            ],
          };
        if (sql === "ROLLBACK" && failure === "rollback") throw Error(failure);
        return { rows: [] };
      },
      async end() {
        calls.push("end");
      },
    },
  };
}
test.each(["connect", "begin", "rollback"])(
  "cleanup always closes after %s failure without queueing rollback on failed connect",
  async (failure) => {
    const x = fake(failure);
    await expect(
      withOwnerSession(x.client as any, async () => 1),
    ).rejects.toThrow();
    expect(x.calls.at(-1)).toBe("end");
    if (failure === "connect") expect(x.calls).toEqual(["connect", "end"]);
  },
);
test("an unexpected owner reset baseline refuses before work", async () => {
  const x = fake("", true);
  let work = false;
  await expect(
    withOwnerSession(x.client as any, async () => {
      work = true;
    }),
  ).rejects.toThrow("owner_reset_baseline");
  expect(work).toBe(false);
  expect(x.calls.at(-1)).toBe("end");
});
test("temporary-login reset is restored locally and rollback preserves the explicit session owner", async () => {
  const x = fake(); let work = false;
  await withOwnerSession(x.client as any, async () => { work = true; });
  expect(work).toBe(true);
  expect(x.calls).toContain("RESET ROLE");
  expect(x.calls).toContain("SET LOCAL ROLE postgres");
});
test.each(["local_role", "rollback_role"])("failed explicit owner restoration (%s) refuses before work", async failure => {
  const x = fake(failure); let work = false;
  await expect(withOwnerSession(x.client as any, async () => {work = true;})).rejects.toThrow("owner_session_identity");
  expect(work).toBe(false); expect(x.calls.at(-1)).toBe("end");
});
test("fenced canonical client rejects role change before submitting SQL; rollback stays available", async () => {
  const x = fake();
  const fenced = ownerFencedClient(x.client as any, 42);
  await expect(
    fenced.query("RESET ROLE; CREATE TABLE bad(x int)"),
  ).rejects.toThrow("owner_sql_role_control");
  expect(x.calls).toEqual([]);
  await fenced.query("ROLLBACK");
  expect(x.calls).toEqual(["ROLLBACK"]);
});
test("apply argv cannot request another endpoint/principal or omit exact capture and approval hashes", () => {
  expect(
    ownerApplyArgs([
      "capture",
      "--source-sha",
      pin.sha,
      "--generation",
      pin.generation,
    ]).mode,
  ).toBe("capture");
  for (const argv of [
    ["apply", "--source-sha", pin.sha, "--generation", pin.generation],
    [
      "capture",
      "--source-sha",
      pin.sha,
      "--generation",
      pin.generation,
      "--url",
      "anything",
    ],
  ])
    expect(() => ownerApplyArgs(argv)).toThrow();
});

test("programmatic apply cannot substitute another SQL wrapper before any query", async () => {
  const x = await evidence();
  let queries = 0;
  const client = {
    query: async () => {
      queries++;
      return { rows: [] };
    },
  };
  await expect(
    applyOwnerRelease(client, 42, {
      pin,
      planText: x.planText,
      captureText: x.captureText,
      approvalText: JSON.stringify(x.approval),
      wrapper: { ...x.wrapper, sql: x.wrapper.sql + " SELECT 1" },
      recheckSource: async () => {},
    }),
  ).rejects.toThrow("owner_wrapper_generation");
  expect(queries).toBe(0);
});

test("readonly plan rejects a changed current ledger/source/generation before capture", async () => {
  const x = await evidence(),
    p = JSON.parse(x.planText);
  for (const changed of [
    { ledgerSha256: "e".repeat(64) },
    { ledgerSha256: "de0e2fa02ba662bbc1e0ec23e1f619100d3b7cd824e8d055b04c44076d452729" },
    { sourceSha: "e".repeat(40) },
    { generation: "e".repeat(64) },
    { project: "another" },
    { status: "applied" },
    { inventory: [] },
  ])
    expect(() =>
      validateReadonlyPlan(JSON.stringify({ ...p, ...changed }), pin, now),
    ).toThrow("owner_plan_binding");
});
test("backend identity drift refuses the next canonical SQL before submission", async () => {
  const x = fake(),
    fenced = ownerFencedClient(x.client as any, 43);
  await expect(
    fenced.query("CREATE TABLE sessions.not_allowed(id int)"),
  ).rejects.toThrow("owner_session_identity");
  expect(x.calls.some((q) => q.includes("CREATE TABLE"))).toBe(false);
});

test("Window #2 requires the verified post-Window #1 ledger", () => {
  expect(A.ledger).toBe(
    "3173f2df7f749fd79c340286f3284d91d8f5a8b988b7f1b1a11e01c3a3307641",
  );
});

test("approved CLI version-cache churn cannot hide dirty source or arbitrary artifacts", () => {
  expect(() => ownerApplySourceFiles(["supabase/.temp/cli-latest"], "v3.0.0")).not.toThrow();
  expect(() => ownerApplySourceFiles(["docs/plans/supabase-owner-apply.capture.json", "supabase/.temp/cli-latest"], "v3.0.0\n")).not.toThrow();
  for (const path of ["supabase/migrations/20261104055000_lazy_current_custody.sql", "scripts/schema-release-owner-apply.ts", "supabase/.temp/project-ref", ".96bulls/unreviewed.sql"])
    expect(() => ownerApplySourceFiles(["supabase/.temp/cli-latest",path], "v3.0.0")).toThrow("owner_dirty_checkout");
  for (const cache of [undefined, "v3.0.0\nextra", "$(command)", "v"+"3".repeat(65)+".0.0"])
    expect(() => ownerApplySourceFiles(["supabase/.temp/cli-latest"], cache)).toThrow("owner_cli_cache_invalid");
});
