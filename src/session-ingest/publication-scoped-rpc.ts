/** Host adapter for the scope-bound v1 database contract. Scope is issued by
 * the admission owner; the database rechecks it under lock at claim and final
 * publication. A missing grant, admission or wrapper must fail the operation. */
import { canonicalPublicationTick, type PublicationJobDeps, type PublicationJobOptions } from "./publication-job.ts";
import type { SemanticBoundedSql } from "../search-features/semantic-copy-postgres.ts";
import type { PublicationRpc, PublicationTarget, PublicationTicket } from "./publication-protocol.ts";
import { parsePublicationTicket, publicationRpc, publicationRpcCaller } from "./publication-rpc.ts";

export interface PublicationScopeBinding {
  planSha256: string;
  admissionId: string;
  frontierSha256: string;
  tenant: {orgId: string; workspaceId: string; userId: string; originId: string};
  target: PublicationTarget;
}

const UUID = /^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/u;
const SHA = /^[a-f0-9]{64}$/u;
const identifier = (s: unknown): s is string =>
  typeof s === "string" && s.length > 0 && s.length <= 255 && !/[\x00-\x1f\x7f/\\]/u.test(s);
const refuse = (code: string): never => {
  throw Object.assign(new Error("publication_scoped_" + code), { code: "publication_scoped_" + code });
};

function bindScope(binding: PublicationScopeBinding, expected: PublicationTarget): PublicationScopeBinding {
  if (!binding || !expected) refuse("binding");
  const { planSha256, admissionId, frontierSha256, tenant, target } = binding;
  if (typeof planSha256 !== "string" || typeof frontierSha256 !== "string" || typeof admissionId !== "string" || !SHA.test(planSha256) || !SHA.test(frontierSha256) || !UUID.test(admissionId) || !tenant || !target) refuse("binding");
  const { orgId, workspaceId, userId, originId } = tenant;
  if (![orgId, workspaceId, userId, originId].every(identifier)) refuse("tenant");
  const { bucket, region, keyRoot, generation, provider } = target;
  if (provider !== "s3" || provider !== expected.provider || typeof generation !== "string" || !SHA.test(generation) || generation !== expected.generation ||
      ![bucket, region, keyRoot].every(identifier) || bucket !== expected.bucket || region !== expected.region || keyRoot !== expected.keyRoot)
    refuse("target");
  // Copy only contract fields. Changes to the caller's object cannot broaden a running cycle.
  return { planSha256, admissionId, frontierSha256, tenant: { orgId, workspaceId, userId, originId },
    target: {bucket, region, keyRoot, generation, provider} };
}

export function scopedPublicationRpc(db: SemanticBoundedSql, expected: PublicationTarget, input: PublicationScopeBinding): PublicationRpc {
  const binding = bindScope(input, expected);
  const { target, tenant } = binding;
  const ordinary = publicationRpc(db, target);
  const call = publicationRpcCaller(db, target.generation);
  const address = { keyRoot: target.keyRoot, bucket: target.bucket, region: target.region };
  let candidate: string | undefined;
  const claims = new WeakMap<PublicationTicket, {stageToken: string; attemptToken: string; leaseToken: string}>();
  function checkScope(ticket: PublicationTicket) {
    if (ticket.schemaGeneration !== target.generation || ticket.tenant.orgId !== tenant.orgId ||
        ticket.tenant.workspaceId !== tenant.workspaceId || ticket.tenant.userId !== tenant.userId ||
        ticket.originId !== tenant.originId) refuse("foreign_ticket");
  }
  function checked(ticket: PublicationTicket) {
    checkScope(ticket);
    const pin = claims.get(ticket);
    if (!pin || pin.stageToken !== ticket.stageToken || pin.attemptToken !== ticket.attemptToken || pin.leaseToken !== ticket.leaseToken)
      return refuse("unbound_ticket");
    return pin;
  }
  return {
    async candidates(limit, signal) {
      candidate = undefined;
      if (limit !== 1) refuse("candidate_limit");
      const { stageTokens } = await call("scoped_candidates_v1", { ...address, binding, limit }, signal);
      if (!Array.isArray(stageTokens) || stageTokens.length > 1 ||
          stageTokens.some(v => typeof v !== "string" || !UUID.test(v))) refuse("candidate_result");
      candidate = stageTokens[0];
      return [...stageTokens];
    },
    async claim(stageToken, signal) {
      if (!candidate || stageToken !== candidate) refuse("unselected_stage");
      candidate = undefined;
      const ticket = parsePublicationTicket(await call("scoped_claim_v1", { ...address, binding, stageToken }, signal), target);
      checkScope(ticket);
      if (ticket.stageToken !== stageToken) refuse("foreign_ticket");
      claims.set(ticket, {stageToken, attemptToken: ticket.attemptToken, leaseToken: ticket.leaseToken});
      return ticket;
    },
    // Defer(stageToken) has no lease or scope argument. Keep the error visible
    // until the scoped scheduler can persist a retry; never fall back to it.
    async defer() { return refuse("lease_free_defer"); },
    heartbeat(ticket, signal) { checked(ticket); return ordinary.heartbeat(ticket, signal); },
    prepare(ticket, images, signal) { checked(ticket); return ordinary.prepare(ticket, images, signal); },
    register(ticket, objects, signal) { checked(ticket); return ordinary.register(ticket, objects, signal); },
    abandon(ticket, signal) { checked(ticket); return ordinary.abandon(ticket, signal); },
    async publish(ticket, messages, signal) {
      const pin = checked(ticket);
      const result = await call("scoped_publish_v1", { binding, ...pin, messages }, signal);
      if (result.published !== true || typeof result.replay !== "boolean" ||
          ![result.images, result.records, result.eligibleSessions].every(n => Number.isSafeInteger(n) && n >= 0))
        refuse("publish_result");
      return result;
    },
  };
}

/** One supervised cycle. The process owner supplies an independently verified
 * target and admission, and rechecks the enable gate before each invocation. */
export function scopedPublicationCycle(
  db: SemanticBoundedSql, binding: PublicationScopeBinding, deps: Omit<PublicationJobDeps, "rpc">,
  options: Omit<PublicationJobOptions, "candidateLimit"> = {},
) {
  if (options.enabled !== true) {
    return Promise.resolve({ enabled: false as const, examined: 0, published: 0, deferred: 0, eligibleSessions: 0 });
  }
  options.signal?.throwIfAborted();
  const rpc = scopedPublicationRpc(db, deps.target, binding);
  return canonicalPublicationTick({ ...deps, rpc }, { ...options, candidateLimit: 1 });
}
