import { readFileSync } from "node:fs";
import { join } from "node:path";
import type {
  BufferedControlHead,
  BufferedControlHeadResult,
  BufferedGateInput,
  BufferedParityWitness,
  BufferedScope,
} from "../buffered-privacy.ts";

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

const HEX = "1".repeat(64);
const BASELINE = "2".repeat(64);
const DIGEST = "3".repeat(64);

function finalEvent(c: CausalCase) {
  return c.events.find((e) => e.action === "final-read" || e.action === "authoritative-service-binding-read-after-fetch")!;
}

function controlHead(c: CausalCase): BufferedControlHeadResult {
  const e = finalEvent(c);
  const scope = c.candidate.scope;
  const outcome = String(e.outcome ?? "");
  if (outcome === "timeout") return { ok: false, reason: "timeout" };
  if (outcome === "non2xx") return { ok: false, reason: "non2xx" };
  if (outcome === "unbound-service") return { ok: false, reason: "unbound_service" };
  if (outcome === "invalid-response" || outcome === "wrong-origin") return { ok: false, reason: "invalid_response" };
  if (e.activeCutoverEpoch === null || e.baselineProven === false) return { ok: false, reason: "invalid_response" };

  const explicit = e.head as null | { epoch: string; version: string; digest: string; pending: boolean } | undefined;
  const observed = (e.observes && typeof e.observes === "object" ? e.observes : {}) as Record<string, any>;
  const noHead = explicit === null;
  const epoch = String(e.activeCutoverEpoch ?? explicit?.epoch ?? (outcome === "wrong-epoch" ? "f".repeat(64) : HEX));
  const version = noHead ? "0" : String(explicit?.version ?? e.currentControlVersion ?? observed.neonControlVersion ?? c.candidate.immutableAdmissionControlVersion ?? "0");
  const value: BufferedControlHead = {
    tenant: { orgId: scope.orgId, workspaceId: outcome === "wrong-workspace" ? "foreign-workspace" : scope.workspaceId,
      userId: outcome === "wrong-user" ? "foreign-user" : scope.userId },
    controlEpoch: epoch,
    privacyBaselineSha256: BASELINE,
    sourceControlVersion: version,
    sourceControlDigest: noHead ? null : explicit?.digest ?? DIGEST,
    sourceControlKind: noHead ? null : "selection",
    pending: Boolean(explicit?.pending ?? e.pending ?? observed.neonPending ?? false),
  };
  return { ok: true, value };
}

function targetEligible(c: CausalCase): boolean {
  const x = c.candidate;
  return [x.accepted, x.allS3ReadsFinished, x.bodyAccepted, x.currentDigestMatches,
    x.currentNativeOwnerMatches, x.currentRevisionMatches].every((v) => v === true);
}

export function gateInput(c: CausalCase): BufferedGateInput {
  const admissionVersion = c.candidate.gcKeptOriginalAdmissionVersion ?? c.candidate.immutableAdmissionControlVersion;
  const noHead = admissionVersion === "0";
  const proofCurrent = c.initial.parityAllowlisted && c.id !== "disable-reenable-aba-old-admission-must-stay-hidden" &&
    c.id !== "head-none-without-proven-current-epoch";
  const proof: BufferedParityWitness | null = proofCurrent ? {
    contract: "owner-buffered-parity/v1",
    tenant: { orgId: c.candidate.scope.orgId, workspaceId: c.candidate.scope.workspaceId, userId: c.candidate.scope.userId },
    controlEpoch: c.candidate.immutableAdmissionControlEpoch ?? HEX,
    privacyBaselineSha256: BASELINE,
    sourceControlVersion: admissionVersion ?? "0",
    sourceControlDigest: noHead ? null : DIGEST,
    sourceControlKind: noHead ? null : "selection",
    explicitNoHead: noHead,
    targetGeneration: "4".repeat(64),
    deletionSetSha256: "5".repeat(64),
    resetSetSha256: "6".repeat(64),
    targetLineageDigest: "7".repeat(64),
  } : null;
  return {
    scope: c.candidate.scope,
    admission: { controlEpoch: c.candidate.immutableAdmissionControlEpoch, sourceControlVersion: admissionVersion },
    targetEligible: targetEligible(c),
    parity: proof,
    controlHead: controlHead(c),
  };
}

export function patched(base: Record<string, unknown>, patch: Record<string, Json>): Record<string, unknown> {
  const out = structuredClone(base);
  for (const [path, value] of Object.entries(patch)) {
    const parts = path.split(".");
    let parent: Record<string, unknown> = out;
    for (const part of parts.slice(0, -1)) {
      const next = parent[part];
      if (!next || typeof next !== "object" || Array.isArray(next)) throw new Error(`bad fixture patch ${path}`);
      parent = next as Record<string, unknown>;
    }
    parent[parts.at(-1)!] = value;
  }
  return out;
}

/**
 * The 16-case addendum is pinned to v4 (a10f2990); v5 added baseline and kind
 * bindings without changing those 16 axes.  Materialize the v5-positive values
 * here while preserving the release-owner JSON byte-for-byte.
 */
export function parityState(c: ParityCase): Record<string, unknown> {
  const base = structuredClone(parity.base) as Record<string, any>;
  base.privacyBaselineSha256 = BASELINE;
  base.proof.privacyBaselineSha256 = BASELINE;
  if (base.head) base.head.kind = "selection";
  if (base.proof.head) base.proof.head.kind = "selection";
  return patched(base, c.patch);
}
