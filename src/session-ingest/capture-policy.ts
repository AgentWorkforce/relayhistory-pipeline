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
 */
import type { RelaycontrolCapturePolicy } from "@rhc/auth/relaycontrol.ts";
import { DeliveryError } from "./vendor/delivery/delivery-contracts.ts";

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
