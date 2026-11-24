/**
 * GET /v1/session-intel/synthesis (docs/session-intel-workspace-synthesis.md, "Read route"): the stored
 * workspace-synthesis/1 artifact of one workspace and window, behind READ_API_WORKSPACE_SYNTHESIS_ENABLED (exactly
 * "true"). Off: the standard 501 envelope, reason `workspace_synthesis_disabled`, and nothing is read.
 *
 * Authority: the newest complete artifact for the exact workspace and window (session-analysis/workspace-synthesis.ts
 * latestWorkspaceSynthesis). Erasure: after the artifact is read, every session its envelope records (`considered`
 * and `related`: children and any other session its evidence belongs to) is checked against the pointer tombstones
 * (Liveness.erased); if any is erased the artifact is not served (404, reason `synthesis_invalidated`) until the job
 * recomputes it, so an erased session never stays in a drilldown or a count. Only `synthesis` is served.
 *
 * `stage=2`, behind its own flag READ_API_WORKSPACE_SYNTHESIS_V2_ENABLED (exactly "true"): the stored
 * workspace-synthesis/2 document of the same authoritative Stage 1 artifact (session-analysis/workspace-synthesis.ts
 * latestWorkspaceSynthesisV2, the strict stored-v2 reader), after the same erase check over `considered`, `related`
 * and the Stage 2 kept sessions. No v2 for that artifact is a 404 (reason `stage2_not_found`); a stored v2 that does
 * not verify is a typed 500 (reason the SynthesisArtifactError code). Flag off: `stage=2` is 501
 * `workspace_synthesis_v2_disabled` and the route is otherwise unchanged.
 */
import {
  latestWorkspaceSynthesis, latestWorkspaceSynthesisV2, SynthesisArtifactError,
  type SynthesisArtifact, type SynthesisArtifactStore, type SynthesisArtifactV2,
} from "../session-analysis/workspace-synthesis.ts";
import type { WorkspaceSynthesisV2 } from "../session-intel/synthesis/stage2.ts";
import { SYNTHESIS_BOUNDS, type SessionRef, type WorkspaceSynthesis } from "../session-intel/synthesis/synthesize.ts";
import { TenantMismatchError, type Liveness } from "./page.ts";

export interface WorkspaceSynthesisSource {
  latest(orgId: string, workspaceId: string, window: { since: string; until: string }): Promise<SynthesisArtifact | null>;
  /** The authoritative Stage 1 artifact and its stored v2 (null when absent); a v2 that does not verify throws. */
  latestStage2?(orgId: string, workspaceId: string, window: { since: string; until: string }): Promise<{ v1: SynthesisArtifact; v2: SynthesisArtifactV2 | null } | null>;
}

/** The artifact store as the route's source; `keyRoot` is the job's key root (the tenant base, e.g. PIPELINE_KEY_PREFIX). */
export function storeWorkspaceSynthesis(store: SynthesisArtifactStore, keyRoot: string): WorkspaceSynthesisSource {
  return {
    latest: (orgId, workspace, window) => latestWorkspaceSynthesis(store, { keyRoot, orgId, workspace, window }),
    latestStage2: (orgId, workspace, window) => latestWorkspaceSynthesisV2(store, { keyRoot, orgId, workspace, window }),
  };
}

/**
 * The key root both readers use: the deployment's PIPELINE_KEY_PREFIX (the tenant base the job writes under), one
 * lowercase segment. Unset or not one segment: undefined, and the route answers 501 workspace_synthesis_not_configured.
 */
export function synthesisKeyRoot(raw: string | undefined): string | undefined {
  const root = (raw ?? "").replace(/^\/+|\/+$/gu, "");
  return /^[a-z0-9][a-z0-9-]{0,62}$/u.test(root) ? root : undefined;
}

/** Keys per Liveness.erased call. */
export const SYNTHESIS_ERASE_CHECK_BATCH = 500;

const ISO = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2}(?:\.\d{1,9})?)?(?:Z|[+-]\d{2}:\d{2})$/u;

/** `since` and `until` as the artifact keys them: ISO-8601 with a zone, normalized to UTC ms; at most 30 days. */
export function synthesisWindow(since: string | undefined, until: string | undefined): { since: string; until: string } | string {
  if (since === undefined || until === undefined) return "since and until are required";
  if (!ISO.test(since) || !ISO.test(until)) return "since and until must be ISO-8601 timestamps with a zone";
  const s = Date.parse(since), u = Date.parse(until);
  if (!Number.isFinite(s) || !Number.isFinite(u) || s >= u) return "since must be before until";
  if (u - s > SYNTHESIS_BOUNDS.maxWindowMs) return "the window is longer than 30 days";
  return { since: new Date(s).toISOString(), until: new Date(u).toISOString() };
}

export type SynthesisRead =
  | { status: "ok"; synthesis: WorkspaceSynthesis }
  | { status: "not_found" }
  | { status: "invalidated"; erased: number };

/** Read, then the erase check (after the object IO, so an erase during the read still wins). */
export async function readWorkspaceSynthesis(input: {
  source: WorkspaceSynthesisSource;
  liveness: Liveness;
  orgId: string;
  workspaceId: string;
  window: { since: string; until: string };
}): Promise<SynthesisRead> {
  let artifact: SynthesisArtifact | null;
  try {
    artifact = await input.source.latest(input.orgId, input.workspaceId, input.window);
  } catch (error) {
    if (error instanceof SynthesisArtifactError && error.code === "synthesis_cross_tenant") throw new TenantMismatchError();
    throw error;
  }
  if (!artifact) return { status: "not_found" };
  if (artifact.orgId !== input.orgId || artifact.workspace !== input.workspaceId) throw new TenantMismatchError();
  const erased = await erasedCount(input, [...artifact.considered, ...artifact.related]);
  if (erased > 0) return { status: "invalidated", erased };
  return { status: "ok", synthesis: artifact.synthesis };
}

/** How many of `refs` are erased; stops at the first batch with any (the answer is then only "some"). */
async function erasedCount(input: { liveness: Liveness; orgId: string; workspaceId: string }, refs: readonly SessionRef[]): Promise<number> {
  for (let i = 0; i < refs.length; i += SYNTHESIS_ERASE_CHECK_BATCH) {
    const erased = await input.liveness.erased(input.orgId, input.workspaceId, refs.slice(i, i + SYNTHESIS_ERASE_CHECK_BATCH));
    if (erased.size > 0) return erased.size;
  }
  return 0;
}

/** Stored-v2 failures served as a typed 500: the strict reader's codes (the cross-tenant one is a TenantMismatchError). */
export const STAGE2_INVALID_CODES = ["synthesis_stage2_invalid", "synthesis_stage2_digest", "synthesis_stage2_stage1_mismatch"] as const;

export type SynthesisV2Read =
  | { status: "ok"; synthesis: WorkspaceSynthesisV2 }
  | { status: "not_found"; reason: "synthesis_not_found" | "stage2_not_found" }
  | { status: "invalidated"; erased: number }
  | { status: "invalid"; code: (typeof STAGE2_INVALID_CODES)[number] };

/**
 * The v2 read: the authoritative Stage 1 artifact's stored v2 through the strict reader, then the erase check over
 * `considered`, `related` and the Stage 2 kept sessions (every session an initiative or theme names is a kept one,
 * the strict reader's rule; the kept ones are checked here directly as well, never assumed covered).
 */
export async function readWorkspaceSynthesisV2(input: {
  source: WorkspaceSynthesisSource;
  liveness: Liveness;
  orgId: string;
  workspaceId: string;
  window: { since: string; until: string };
}): Promise<SynthesisV2Read> {
  if (!input.source.latestStage2) return { status: "not_found", reason: "stage2_not_found" };
  let found: { v1: SynthesisArtifact; v2: SynthesisArtifactV2 | null } | null;
  try {
    found = await input.source.latestStage2(input.orgId, input.workspaceId, input.window);
  } catch (error) {
    if (error instanceof SynthesisArtifactError && error.code === "synthesis_cross_tenant") throw new TenantMismatchError();
    if (error instanceof SynthesisArtifactError && (STAGE2_INVALID_CODES as readonly string[]).includes(error.code))
      return { status: "invalid", code: error.code as (typeof STAGE2_INVALID_CODES)[number] };
    throw error;
  }
  if (!found) return { status: "not_found", reason: "synthesis_not_found" };
  const { v1, v2 } = found;
  if (v1.orgId !== input.orgId || v1.workspace !== input.workspaceId) throw new TenantMismatchError();
  if (!v2) return { status: "not_found", reason: "stage2_not_found" };
  if (v2.orgId !== input.orgId || v2.workspace !== input.workspaceId) throw new TenantMismatchError();
  const stage2 = v2.synthesis.stage2;
  // The strict reader admits only a complete Stage 2; anything else here is a source that skipped it.
  if (!stage2) return { status: "invalid", code: "synthesis_stage2_invalid" };
  const refs = new Map<string, SessionRef>();
  const add = (r: SessionRef) => refs.set(`${r.source}\u0000${r.sessionId}`, { source: r.source, sessionId: r.sessionId });
  [...v2.considered, ...v2.related, ...stage2.sessions].forEach(add);
  for (const item of [...stage2.initiatives, ...stage2.themes]) { item.sessions.forEach(add); item.evidence.forEach(add); }
  const erased = await erasedCount(input, [...refs.values()]);
  if (erased > 0) return { status: "invalidated", erased };
  return { status: "ok", synthesis: v2.synthesis };
}
