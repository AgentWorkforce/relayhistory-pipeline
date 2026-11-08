import { expect, spyOn, test } from "bun:test";
import {
  OWNER_APPLY as A, digest, ownerCapture, prepareOwnerWrite, validateReadonlyPlan,
  wrapperSource, applyOwnerRelease, type OwnerApplyIntegration, type OwnerReleaseInputs,
} from "../owner-operator-apply.ts";
import { ownerApplyArgs, ownerApplyMain } from "../../../scripts/schema-release-owner-apply.ts";

async function fixture(integration?: OwnerApplyIntegration) {
  const at = new Date().toISOString();
  const pin = { sha: "a".repeat(40), generation: "b".repeat(64) };
  const wrapper = await wrapperSource();
  const report = {
    generation: pin.generation, ledgerSha256: A.ledger,
    inventory: [{ name: "fixture", sha256: "d".repeat(64), pending: true }],
    acl: { function_acl_drift_extra: [], function_acl_missing_refused: [] },
  };
  const boundary = {
    present: true, event: "ddl_command_end", tags_exact: true, enabled_ok: true,
    owner_is_migration_owner: true, function_is_reviewed: true,
    function_owner_is_migration_owner: true, schema_owner_is_migration_owner: true,
    prosrc_sha256: "c".repeat(64), proconfig_exact: true, security_definer: false,
    volatility: "v", language: "plpgsql", returns: "event_trigger",
    function_public_execute: false, runtime_role_setting_matches: true, schema_private: true,
  };
  const planText = JSON.stringify({ version: 1, status: "planned_readonly_not_apply_approval",
    at, project: A.project, sourceSha: pin.sha, boundary: "global-default-revoke", ...report });
  const capture = ownerCapture(planText, validateReadonlyPlan(planText, pin), pin,
    { ...report, existingBoundary: boundary }, wrapper, new Date(at));
  const captureText = JSON.stringify(capture);
  const approvalText = JSON.stringify({ version: 1, decision: "approved", by: "Khaliq Gant",
    execution: A.execution, quote: "Synthetic offline fixture only; not authorization", at,
    project: A.project, sourceSha: pin.sha, generation: pin.generation, ledgerSha256: A.ledger,
    planSha256: digest(planText), captureSha256: digest(captureText),
    captureGeneration: capture.captureGeneration, wrapperSha256: wrapper.sha256 });
  const calls: string[] = [];
  const identity = { owner: A.owner, login: A.login, db: A.database, schemaOwner: A.owner,
    role: "postgres", pid: 42, ro: "off" };
  const client = { query: async (sql: string) => {
    calls.push("identity");
    // No write or transaction is allowed in the preparation boundary itself.
    expect(sql.startsWith("SELECT current_user AS owner")).toBe(true);
    return { rows: [{ ...identity }] };
  } };
  const args: OwnerReleaseInputs = { pin, wrapper, planText, captureText, approvalText,
    recheckSource: async () => { calls.push("source"); }, integration };
  return { args, client: client as any, identity, calls };
}

test("prepared callback sees only frozen derived hashes after source and identity, exactly once", async () => {
  let seen: unknown;
  const x = await fixture({ beforeWrite: async context => {
    x.calls.push("prepared"); seen = context;
    expect(Object.isFrozen(context)).toBe(true);
    expect(context).toEqual({ sourceSha: x.args.pin.sha, generation: x.args.pin.generation,
      expectedLedgerSha256: A.ledger, planSha256: digest(x.args.planText),
      captureSha256: digest(x.args.captureText), approvalSha256: digest(x.args.approvalText),
      wrapperSha256: x.args.wrapper.sha256 });
  } });
  await prepareOwnerWrite(x.client, 42, x.args);
  expect(seen).toBeDefined();
  expect(x.calls).toEqual(["source", "identity", "prepared", "source", "identity"]);
});

test("without integration the preparation boundary retains the original single source/identity check", async () => {
  const x = await fixture();
  await prepareOwnerWrite(x.client, 42, x.args);
  expect(x.calls).toEqual(["source", "identity"]);
});

test("a refused callback propagates before any post-release or write work", async () => {
  const refused = new Error("release_refused");
  const x = await fixture({ beforeWrite: async () => { x.calls.push("prepared"); throw refused; } });
  await expect(prepareOwnerWrite(x.client, 42, x.args)).rejects.toBe(refused);
  expect(x.calls).toEqual(["source", "identity", "prepared"]);
});

test("invalid native inputs and identity never announce preparation", async () => {
  let invoked = 0;
  const x = await fixture({ beforeWrite: async () => { invoked++; } });
  await expect(prepareOwnerWrite(x.client, 42, { ...x.args, planText: "{}" })).rejects.toThrow("owner_plan_binding");
  expect(x.calls).toEqual([]);
  x.identity.role = "none";
  await expect(prepareOwnerWrite(x.client, 42, x.args)).rejects.toThrow("owner_session_identity");
  expect(invoked).toBe(0);
});

test("source drift after the prepared wait refuses before the boundary returns", async () => {
  const x = await fixture({ beforeWrite: async () => { x.calls.push("prepared"); } });
  let checked = 0;
  await expect(prepareOwnerWrite(x.client, 42, { ...x.args, recheckSource: async () => {
    x.calls.push("source"); if (++checked === 2) throw Error("source_drift");
  } })).rejects.toThrow("source_drift");
  expect(x.calls).toEqual(["source", "identity", "prepared", "source"]);
});

test("session identity is checked again on the same PID after release", async () => {
  const x = await fixture({ beforeWrite: async () => { x.identity.pid = 99; } });
  await expect(prepareOwnerWrite(x.client, 42, x.args)).rejects.toThrow("owner_session_identity");
  expect(x.calls).toEqual(["source", "identity", "source", "identity"]);
});

test("approval/capture expiry during the wait cannot be extended by callback success", async () => {
  let time = Date.now();
  const clock = spyOn(Date, "now").mockImplementation(() => time);
  try {
    const x = await fixture({ beforeWrite: async () => { time = Date.parse(JSON.parse(x.args.planText).at) + A.maxAgeMs; } });
    await expect(prepareOwnerWrite(x.client, 42, x.args)).rejects.toThrow("owner_plan_stale");
    expect(x.calls).toEqual(["source", "identity"]);
  } finally { clock.mockRestore(); }
});

test("a callback cannot mutate the prepared context or supply substitute hashes", async () => {
  const x = await fixture({ beforeWrite: async context => { (context as any).sourceSha = "f".repeat(40); } });
  await expect(prepareOwnerWrite(x.client, 42, x.args)).rejects.toThrow();
  const forged = { beforeWrite: async () => {}, planSha256: "f".repeat(64) } as any;
  x.calls.length = 0;
  await expect(prepareOwnerWrite(x.client, 42, { ...x.args, integration: forged })).rejects.toThrow("owner_integration_shape");
  expect(x.calls).toEqual([]);
});

test("expiry during post-release source/identity checks still refuses before write preparation returns", async () => {
  let time = Date.now();
  const clock = spyOn(Date, "now").mockImplementation(() => time);
  try {
    const x = await fixture({ beforeWrite: async () => {} });
    let checked = 0;
    await expect(prepareOwnerWrite(x.client, 42, { ...x.args, recheckSource: async () => {
      if (++checked === 2) time = Date.parse(JSON.parse(x.args.planText).at) + A.maxAgeMs;
    } })).rejects.toThrow("owner_plan_stale");
    expect(checked).toBe(2);
  } finally { clock.mockRestore(); }
});

test("outer apply validation failure never reaches the callback or a query", async () => {
  let invoked = 0;
  const x = await fixture({ beforeWrite: async () => { invoked++; } });
  await expect(applyOwnerRelease(x.client, 42, { ...x.args,
    wrapper: { ...x.args.wrapper, sql: x.args.wrapper.sql + " SELECT 1" },
  })).rejects.toThrow("owner_wrapper_generation");
  expect(invoked).toBe(0); expect(x.calls).toEqual([]);
});

const captureArgs = ["capture", "--source-sha", "a".repeat(40), "--generation", "b".repeat(64)];
test("stock CLI accepts no integration selector and an internal integration is apply-only", async () => {
  expect(() => ownerApplyArgs([...captureArgs, "--prepared-fd", "3"])).toThrow("owner_apply_args");
  await expect(ownerApplyMain(captureArgs, { beforeWrite: async () => {} })).rejects.toThrow("owner_integration_apply_only");
  await expect(ownerApplyMain(["apply", ...captureArgs.slice(1), "--capture-sha", "c".repeat(64), "--approval-sha", "d".repeat(64)], {} as any)).rejects.toThrow("owner_integration_shape");
});

test("stock main without an integration still reaches the unchanged pre-auth source gate", async () => {
  await expect(ownerApplyMain(captureArgs)).rejects.toThrow("owner_checkout_generation");
  await expect(ownerApplyMain(captureArgs, undefined)).rejects.toThrow("owner_checkout_generation");
});
