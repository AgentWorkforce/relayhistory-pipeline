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
 */
import {
  latestWorkspaceSynthesis, SynthesisArtifactError, type SynthesisArtifact, type SynthesisArtifactStore,
} from "../session-analysis/workspace-synthesis.ts";
import { SYNTHESIS_BOUNDS, type WorkspaceSynthesis } from "../session-intel/synthesis/synthesize.ts";
import { TenantMismatchError, type Liveness } from "./page.ts";

export interface WorkspaceSynthesisSource {
  latest(orgId: string, workspaceId: string, window: { since: string; until: string }): Promise<SynthesisArtifact | null>;
}

/** The artifact store as the route's source; `keyRoot` is the job's key root (the tenant base, e.g. PIPELINE_KEY_PREFIX). */
export function storeWorkspaceSynthesis(store: SynthesisArtifactStore, keyRoot: string): WorkspaceSynthesisSource {
  return {
    latest: (orgId, workspace, window) => latestWorkspaceSynthesis(store, { keyRoot, orgId, workspace, window }),
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
  const all = [...artifact.considered, ...artifact.related];
  for (let i = 0; i < all.length; i += SYNTHESIS_ERASE_CHECK_BATCH) {
    const erased = await input.liveness.erased(input.orgId, input.workspaceId, all.slice(i, i + SYNTHESIS_ERASE_CHECK_BATCH));
    if (erased.size > 0) return { status: "invalidated", erased: erased.size };
  }
  return { status: "ok", synthesis: artifact.synthesis };
}
