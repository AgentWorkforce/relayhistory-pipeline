import { readFileSync } from "node:fs";
import { join } from "node:path";
import {
  admissionVisible,
  BufferedFenced,
  parseControlHeadResponse,
  type Admission,
  type AuthorityHead,
  type BufferedScope,
} from "../owner-buffered.ts";
import {
  bufferedParityVisible,
  type OwnerBufferedParityProof,
  type ParityCensusInput,
} from "../../migration/owner-buffered-parity.ts";

export type Json = null | boolean | number | string | Json[] | { [key: string]: Json };
export interface CausalCase {
  id: string;
  candidate: Record<string, any> & {
    immutableAdmissionControlEpoch: string | null;
    immutableAdmissionControlVersion: string | null;
    scope: BufferedScope & { originId: string; source: string; sessionId: string };
  };
  events: Array<Record<string, any> & { action: string }>;
  initial: Record<string, any>;
  required: { finalBufferedContentEmitted: boolean; [key: string]: Json | undefined };
}
export interface ParityCase { id: string; emitBuffered: boolean; patch: Record<string, Json> }

export const root = join(import.meta.dir, "../../..");
export const causalPath = "docs/handoffs/neon-census-role-20261107/fixtures/option-c-authority-gate-causal-fixtures-v3-20261108.json";
export const parityPath = "docs/handoffs/neon-census-role-20261107/fixtures/option-c-parity-gate-fixture-addendum-v1-20261108.json";
export const causal = JSON.parse(readFileSync(join(root, causalPath), "utf8")) as { cases: CausalCase[] };
export const parity = JSON.parse(readFileSync(join(root, parityPath), "utf8")) as { base: Record<string, unknown>; cases: ParityCase[] };

const EPOCH = "1".repeat(64);
const OTHER_EPOCH = "2".repeat(64);
const BASELINE = "2".repeat(64);
const DIGEST = "3".repeat(64);

export function finalEvent(c: CausalCase) {
  return c.events.find((e) => e.action === "final-read" || e.action === "authoritative-service-binding-read-after-fetch")!;
}

/** Exact frozen 12-key relayhistory-cloud #328 response derived from one release vector. */
export function controlHeadBody(c: CausalCase): Record<string, unknown> {
  const event = finalEvent(c);
  const scope = c.candidate.scope;
  const outcome = String(event.outcome ?? "");
  if (outcome === "invalid-response") return { invalid: true };
  const observed = event.observes && typeof event.observes === "object" ? event.observes as Record<string, any> : {};
  const explicit = event.head as null | { epoch: string; version: string; digest: string; pending: boolean } | undefined;
  const noHead = explicit === null;
  const epoch = String(event.activeCutoverEpoch ?? explicit?.epoch ?? EPOCH);
  const version = noHead ? "0" : String(explicit?.version ?? event.currentControlVersion ?? observed.neonControlVersion ?? c.initial.neonControlVersion ?? c.candidate.immutableAdmissionControlVersion ?? "0");
  const pending = Boolean(explicit?.pending ?? event.pending ?? observed.neonPending ?? false);
  return {
    protocol: "relay-session-read-control-head-v1",
    version: 1,
    issuer: outcome === "wrong-origin" ? "foreign-authority" : "relayhistory-cloud",
    tenant: {
      orgId: scope.orgId,
      workspaceId: outcome === "wrong-workspace" ? "foreign-workspace" : scope.workspaceId,
      userId: outcome === "wrong-user" ? "foreign-user" : scope.userId,
    },
    controlEpoch: epoch,
    cutover: { epoch, status: "active", mode: "ingest" },
    privacyBaselineSha256: BASELINE,
    sourceControlEpoch: noHead ? null : outcome === "wrong-epoch" ? OTHER_EPOCH : epoch,
    sourceControlVersion: version,
    sourceControlDigest: noHead ? null : explicit?.digest ?? DIGEST,
    sourceControlKind: noHead ? null : c.id.includes("reset") ? "reset" : "selection",
    pending,
  };
}

export function finalAuthority(c: CausalCase): AuthorityHead | BufferedFenced {
  const outcome = String(finalEvent(c).outcome ?? "");
  if (["timeout", "non2xx", "unbound-service"].includes(outcome)) return new BufferedFenced(`fixture_${outcome}`);
  try {
    return parseControlHeadResponse(controlHeadBody(c), c.candidate.scope);
  } catch (error) {
    return error instanceof BufferedFenced ? error : new BufferedFenced("fixture_invalid");
  }
}

/** A valid prefetch witness; final failures are injected only after the object read. */
export function firstAuthority(c: CausalCase): AuthorityHead {
  const final = finalAuthority(c);
  if (!(final instanceof BufferedFenced)) return final;
  const version = String(c.initial.neonControlVersion ?? c.candidate.immutableAdmissionControlVersion ?? "7");
  return {
    cutoverEpoch: EPOCH,
    privacyBaselineSha256: BASELINE,
    head: { epoch: EPOCH, version, digest: DIGEST, kind: c.id.includes("reset") ? "reset" : "selection", pending: false },
  };
}

export function admission(c: CausalCase): Admission {
  return {
    admission_epoch: c.candidate.immutableAdmissionControlEpoch,
    admission_version: c.candidate.gcKeptOriginalAdmissionVersion ?? c.candidate.immutableAdmissionControlVersion,
  };
}

export function targetEligible(c: CausalCase): boolean {
  return [c.candidate.accepted, c.candidate.allS3ReadsFinished, c.candidate.bodyAccepted,
    c.candidate.currentDigestMatches, c.candidate.currentNativeOwnerMatches,
    c.candidate.currentRevisionMatches].every((value) => value === true);
}

/** Conditions evaluated inside 043700 before a row can become an API candidate. */
export function sqlEligible(c: CausalCase): boolean {
  return targetEligible(c) && c.initial.parityAllowlisted === true && c.id !== "head-none-without-proven-current-epoch";
}

/** Production consumer decision once the target RPC has returned a candidate. */
export function consumerVisible(c: CausalCase): boolean {
  if (!sqlEligible(c)) return false;
  const authority = finalAuthority(c);
  return !(authority instanceof BufferedFenced) && admissionVisible(authority, admission(c));
}

export function patched(base: Record<string, unknown>, patch: Record<string, Json>): Record<string, any> {
  const out = structuredClone(base) as Record<string, any>;
  for (const [path, value] of Object.entries(patch)) {
    const parts = path.split(".");
    let parent = out;
    for (const part of parts.slice(0, -1)) parent = parent[part] as Record<string, any>;
    parent[parts.at(-1)!] = value;
  }
  return out;
}

/** Materialize the v5.3-only baseline and head-kind bindings around the frozen v4 addendum. */
export function parityState(c: ParityCase): Record<string, any> {
  const base = structuredClone(parity.base) as Record<string, any>;
  base.privacyBaselineSha256 = BASELINE;
  base.proof.privacyBaselineSha256 = BASELINE;
  if (base.head) base.head.kind = "selection";
  if (base.proof.head) base.proof.head.kind = "selection";
  if (base.targetLatestReceipt) base.targetLatestReceipt.kind = "selection";
  return patched(base, c.patch);
}

/** Adapt the frozen addendum into the G1/G2 owner's schema-independent v5.3 predicate. */
export function parityVisible(c: ParityCase): boolean {
  const s = parityState(c);
  const tenant = { orgId: s.org, workspaceId: s.workspace, userId: s.user };
  const head = s.head as ParityCensusInput["head"];
  const current: ParityCensusInput = {
    tenant,
    controlEpoch: s.cutoverEpoch,
    privacyBaselineSha256: s.privacyBaselineSha256,
    head,
    sourceDeletionSetSha256: s.sourceDeletionSetSha256,
    sourceResetSetSha256: s.sourceResetSetSha256,
    targetControlEpoch: s.cutoverEpoch,
    targetPrivacyBaselineSha256: s.privacyBaselineSha256,
    targetGeneration: s.targetGeneration,
    targetLatestReceipt: s.targetLatestReceipt,
    targetReceiptVersionsContiguousFromOne: s.targetReceiptVersionsContiguousFromOne,
    targetDeletionSetSha256: s.targetDeletionSetSha256,
    targetResetSetSha256: s.targetResetSetSha256,
    targetLineageDigest: s.targetOrderedLineageSha256,
  };
  const raw = s.proof as Record<string, any> | null;
  const proof: OwnerBufferedParityProof | null = raw === null ? null : {
    contract: "owner-buffered-parity/v1",
    tenant: { orgId: raw.org, workspaceId: raw.workspace, userId: raw.user },
    controlEpoch: raw.epoch,
    privacyBaselineSha256: raw.privacyBaselineSha256,
    explicitNoHead: raw.explicitNoHead,
    head: raw.head,
    deletionSetSha256: raw.deletionSetSha256,
    resetSetSha256: raw.resetSetSha256,
    targetLineageDigest: raw.targetOrderedLineageSha256,
    targetGeneration: raw.targetGeneration,
  };
  return bufferedParityVisible(s.admission, current, proof);
}
