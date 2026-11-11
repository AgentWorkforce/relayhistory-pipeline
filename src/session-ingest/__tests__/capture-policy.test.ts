/**
 * The capture policy's delete gate (capture-policy.ts): only a present
 * `user_retroactive_delete: false` withdraws a caller's own delete, owners
 * and admins are exempt, and an identity without a policy (the hosted
 * signed identity, or an older relaycontrol) is unchanged. Synthetic ids only.
 */
import { describe, expect, test } from "bun:test";
import { createIngestApp, type IngestIdentity } from "../app.ts";
import { assertUserDeleteAllowed, userDeleteForbidden } from "../capture-policy.ts";
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
