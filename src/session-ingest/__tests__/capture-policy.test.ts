/**
 * The capture policy's delete gate (capture-policy.ts): only a present
 * `user_retroactive_delete: false` withdraws a caller's own delete, owners
 * and admins are exempt, and an identity without a policy (the hosted
 * signed identity, or an older relaycontrol) is unchanged. Synthetic ids only.
 */
import { describe, expect, test } from "bun:test";
import { createIngestApp, type IngestIdentity } from "../app.ts";
import { assertUserDeleteAllowed, uploadPolicyError, uploadPolicyViolation, userDeleteForbidden } from "../capture-policy.ts";
import { SELFHOST_CAPTURE_POLICY_UPLOADS_VAR, selfhostCapturePolicyUploads } from "../selfhost-config.ts";
import type { DeliverDeps } from "../deliver.ts";
import { deliveryAccount } from "../vendor/delivery/prepare.ts";
import { TENANT } from "./support.ts";

const policy = (settings: Record<string, unknown>) => ({ revision: 3, settings });

describe("userDeleteForbidden", () => {
  test("only a present false withdraws a member's delete", () => {
    expect(userDeleteForbidden({ role: "member", capturePolicy: policy({ user_retroactive_delete: false }) })).toBe(true);
    expect(userDeleteForbidden({ capturePolicy: policy({ user_retroactive_delete: false }) })).toBe(true);
    expect(userDeleteForbidden({ role: "member", capturePolicy: policy({ user_retroactive_delete: true }) })).toBe(false);
    expect(userDeleteForbidden({ role: "member", capturePolicy: policy({}) })).toBe(false);
    expect(userDeleteForbidden({ role: "member" })).toBe(false);
    expect(userDeleteForbidden({})).toBe(false);
  });

  test("owners and admins are exempt; an unknown role is not", () => {
    const forbids = policy({ user_retroactive_delete: false });
    expect(userDeleteForbidden({ role: "owner", capturePolicy: forbids })).toBe(false);
    expect(userDeleteForbidden({ role: "admin", capturePolicy: forbids })).toBe(false);
    expect(userDeleteForbidden({ role: "auditor", capturePolicy: forbids })).toBe(true);
  });

  test("assertUserDeleteAllowed throws the typed 403", () => {
    expect(() => assertUserDeleteAllowed({ role: "member", capturePolicy: policy({ user_retroactive_delete: false }) })).toThrow(
      expect.objectContaining({ code: "policy_forbids_user_delete", status: 403 }),
    );
    expect(() => assertUserDeleteAllowed({ role: "owner", capturePolicy: policy({ user_retroactive_delete: false }) })).not.toThrow();
  });
});

describe("reset under a policy that withdraws delete", () => {
  /** A ledger that fails the test if the refused reset ever reaches it. */
  const untouched = new Proxy({}, { get: () => () => { throw new Error("the ledger must not be called"); } });
  const app = (identity: IngestIdentity) =>
    createIngestApp({ identify: () => identity, deliver: { ledger: untouched, acceptanceMode: "canonical", target: {} } as unknown as DeliverDeps, log: () => {} });
  const reset = async (identity: IngestIdentity) =>
    app(identity).request("/v1/delivery/sessions/reset", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ account_id: await deliveryAccount(TENANT), request_id: "reset-1", confirmation: "DELETE ALL SESSIONS" }),
    });

  test("a member's reset is refused 403 policy_forbids_user_delete before the ledger", async () => {
    const res = await reset({ ...TENANT, scopes: ["rth:sync"], role: "member", capturePolicy: policy({ user_retroactive_delete: false }) });
    expect(res.status).toBe(403);
    expect(((await res.json()) as { error: { code: string } }).error.code).toBe("policy_forbids_user_delete");
  });
});

describe("uploadPolicyViolation", () => {
  const SINCE = Date.UTC(2026, 10, 1);
  const session = (first_activity_ms: unknown, over: Record<string, unknown> = {}) =>
    ({ kind: "session", operation: "upsert", payload: { first_activity_ms }, ...over }) as never;
  const batch = (...records: never[]) => ({ records });
  const subject = (settings: Record<string, unknown>, sharingModeSinceMs?: number) =>
    ({ role: "owner", capturePolicy: { revision: 4, settings, ...(sharingModeSinceMs !== undefined ? { sharingModeSinceMs } : {}) } });

  test("no policy, or one that sets no key, needs no revision", () => {
    expect(uploadPolicyViolation({}, undefined, batch())).toBeNull();
    expect(uploadPolicyViolation(subject({}), undefined, batch())).toBeNull();
  });

  test("a set policy needs the current revision (or newer); owners are not exempt", () => {
    const s = subject({ upload_control: "operator" });
    expect(uploadPolicyViolation(s, undefined, batch())).toBe("missing");
    expect(uploadPolicyViolation(s, "", batch())).toBe("missing");
    expect(uploadPolicyViolation(s, "04", batch())).toBe("missing");
    expect(uploadPolicyViolation(s, "-4", batch())).toBe("missing");
    expect(uploadPolicyViolation(s, "3", batch())).toBe("stale");
    expect(uploadPolicyViolation(s, "4", batch())).toBeNull();
    expect(uploadPolicyViolation(s, " 5 ", batch())).toBeNull();
  });

  test("sharing_mode new: a session record whose first activity predates the policy", () => {
    const s = subject({ sharing_mode: "new" }, SINCE);
    expect(uploadPolicyViolation(s, "4", batch(session(SINCE + 1)))).toBeNull();
    expect(uploadPolicyViolation(s, "4", batch(session(SINCE)))).toBeNull();
    expect(uploadPolicyViolation(s, "4", batch(session(SINCE + 1), session(SINCE - 1)))).toBe("session_predates");
    // Only upserted session records with a numeric start are read.
    expect(uploadPolicyViolation(s, "4", batch(session("2020-01-01"), session(SINCE - 1, { kind: "history" }), session(null),
      ({ kind: "session", operation: "delete", payload: null }) as never))).toBeNull();
    // Another sharing mode, or no recorded time, reads no records.
    expect(uploadPolicyViolation(subject({ sharing_mode: "all" }, SINCE), "4", batch(session(0)))).toBeNull();
    expect(uploadPolicyViolation(subject({ sharing_mode: "new" }), "4", batch(session(0)))).toBeNull();
  });

  test("enforce mode's refusals", () => {
    expect(uploadPolicyError("stale", 4)).toMatchObject({ code: "capture_policy_stale", status: 409 });
    expect(uploadPolicyError("missing", 4)).toMatchObject({ code: "capture_policy_stale", status: 409 });
    expect(uploadPolicyError("session_predates", 4)).toMatchObject({ code: "capture_policy_session_predates_policy", status: 422 });
  });
});

describe(SELFHOST_CAPTURE_POLICY_UPLOADS_VAR, () => {
  test("report by default; enforce only when spelled exactly; anything else refuses startup", () => {
    for (const raw of [undefined, "", "report"]) expect(selfhostCapturePolicyUploads({ [SELFHOST_CAPTURE_POLICY_UPLOADS_VAR]: raw })).toBe("report");
    expect(selfhostCapturePolicyUploads({ [SELFHOST_CAPTURE_POLICY_UPLOADS_VAR]: "enforce" })).toBe("enforce");
    for (const raw of ["Enforce", "on", "true"])
      expect(() => selfhostCapturePolicyUploads({ [SELFHOST_CAPTURE_POLICY_UPLOADS_VAR]: raw })).toThrow(SELFHOST_CAPTURE_POLICY_UPLOADS_VAR);
  });
});
