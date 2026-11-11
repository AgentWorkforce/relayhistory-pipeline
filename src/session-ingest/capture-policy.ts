/**
 * Server-side enforcement of the operator capture policy (relay-control
 * docs/capture-policy.md) on the self-host ingest edge. The policy is never
 * stored here: relaycontrol introspection answers it with every verified
 * bearer (`capture_policy`, v1.5), next to the caller's live organization
 * `role` (selfhost-auth.ts puts both on the IngestIdentity). The hosted
 * Worker's signed identity carries neither, so nothing here applies to it.
 *
 * Delete: with `user_retroactive_delete: false` a member's own Cloud delete
 * (selection `enabled: false`, reset) is refused 403
 * `policy_forbids_user_delete`. Organization owners and admins still delete,
 * and the operator hard delete (session-erase.ts) never passes through here.
 * No policy on the identity (an older relaycontrol) leaves deletes as before.
 *
 * Uploads (POST /v1/delivery/batches and /v1/turns/batches): Agent Relay
 * Desktop names the policy revision it applies in
 * X-Agent-Relay-Capture-Policy-Revision. When the policy sets any key, a
 * missing or older revision is a violation when the policy restricts uploads,
 * and so is (under `sharing_mode:
 * new`) a session record whose first activity is before `sharing_mode_since`.
 * An All-only policy (with an optional delete setting) does not require the
 * header: its absence on an older client does not violate that upload policy.
 * RELAYHISTORY_SELFHOST_CAPTURE_POLICY_UPLOADS=report (default) logs it
 * (`capture_policy_upload_violation`) and accepts; `enforce` refuses: 409
 * `capture_policy_stale` (Desktop retries; its next policy read sends the
 * current revision) or 422 `capture_policy_session_predates_policy` (the
 * batch is set aside). Only identities that carry a policy are checked.
 */
import type { RelaycontrolCapturePolicy } from "@rhc/auth/relaycontrol.ts";
import { DeliveryError, type HistoryExportBatch } from "./vendor/delivery/delivery-contracts.ts";

export const USER_RETROACTIVE_DELETE = "user_retroactive_delete";
export const POLICY_FORBIDS_USER_DELETE = "policy_forbids_user_delete";

/** What the policy checks read from a verified identity. */
export interface CapturePolicySubject {
  role?: string;
  capturePolicy?: RelaycontrolCapturePolicy;
}

/** Organization owners and admins manage the policy, so it never limits them. */
export function policyExempt(subject: CapturePolicySubject): boolean {
  return subject.role === "owner" || subject.role === "admin";
}

/** Whether the policy withdraws this caller's own Cloud delete (only a present `false` does). */
export function userDeleteForbidden(subject: CapturePolicySubject): boolean {
  return subject.capturePolicy?.settings[USER_RETROACTIVE_DELETE] === false && !policyExempt(subject);
}

/** Throws 403 policy_forbids_user_delete when the policy withdraws the caller's own delete. */
export function assertUserDeleteAllowed(subject: CapturePolicySubject): void {
  if (userDeleteForbidden(subject))
    throw new DeliveryError(POLICY_FORBIDS_USER_DELETE, 403, "Your organization's capture policy does not allow deleting uploaded sessions");
}

export const CAPTURE_POLICY_REVISION_HEADER = "x-agent-relay-capture-policy-revision";
export const CAPTURE_POLICY_STALE = "capture_policy_stale";
export const CAPTURE_POLICY_SESSION_PREDATES = "capture_policy_session_predates_policy";
export type CapturePolicyUploadsMode = "report" | "enforce";

export type UploadViolation = "missing" | "stale" | "session_predates";

/**
 * The first way this upload breaks the caller's capture policy, or null. A
 * policy that sets no key needs no revision; records are read only under
 * `sharing_mode: new` with a known start (`session` records' `first_activity_ms`).
 */
export function uploadPolicyViolation(subject: CapturePolicySubject, header: string | undefined, batch: Pick<HistoryExportBatch, "records">): UploadViolation | null {
  const policy = subject.capturePolicy;
  if (!policy || Object.keys(policy.settings).length === 0) return null;
  // All alone places no restriction on uploaded content. A revision header
  // is not consent, and older clients do not send one. Delete policy is
  // enforced separately on delete routes and must not stop ordinary uploads.
  if (policy.settings.sharing_mode === "all" &&
      Object.keys(policy.settings).every(key => key === "sharing_mode" || key === USER_RETROACTIVE_DELETE)) return null;
  const raw = (header ?? "").trim();
  // A revision is a non-negative integer; 0 is one (always older than a set policy's).
  if (!/^(?:0|[1-9][0-9]{0,9})$/.test(raw)) return "missing";
  if (Number(raw) < policy.revision) return "stale";
  const since = policy.sharingModeSinceMs;
  if (policy.settings.sharing_mode === "new" && since !== undefined) {
    for (const record of batch.records) {
      if (record.kind !== "session" || record.operation !== "upsert" || !record.payload) continue;
      const started = record.payload.first_activity_ms;
      if (typeof started === "number" && Number.isFinite(started) && started < since) return "session_predates";
    }
  }
  return null;
}

/** Enforce mode's refusal for a violation. */
export function uploadPolicyError(violation: UploadViolation, revision: number): DeliveryError {
  return violation === "session_predates"
    ? new DeliveryError(CAPTURE_POLICY_SESSION_PREDATES, 422, "Your organization's capture policy shares only sessions started after it was set")
    : new DeliveryError(CAPTURE_POLICY_STALE, 409, `Uploads need capture policy revision ${revision}; read the policy again and retry`);
}
