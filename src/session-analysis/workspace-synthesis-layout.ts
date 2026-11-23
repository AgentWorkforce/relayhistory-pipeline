/**
 * The key layout of the workspace-synthesis artifacts (src/session-analysis/workspace-synthesis.ts), on its own so an
 * erase lane can name the directory without loading the job:
 *
 *   {keyRoot}/{org}/v1/ws={workspace}/normalized/session-analysis-v2/workspace-synthesis-v1/w={sinceMs}-{untilMs}/g={generatedAtMs}-{sha256}.json
 */
import { layoutComponent } from "../shared/layout-component.ts";

export const SYNTHESIS_PREFIX_SEGMENT = "normalized/session-analysis-v2/workspace-synthesis-v1/";
/** The tenant prefix rules of the bucket layout (neon-s3-sync/export-session.ts orgPrefix). */
export const KEY_ROOT_SEGMENT = /^[a-z0-9][a-z0-9-]{0,62}$/u;
export const ORG_SEGMENT = /^[A-Za-z0-9._-]{1,128}$/u;
/** The cohort's workspace id rule (session-intel/synthesis/synthesize.ts WORKSPACE_ID). */
const WORKSPACE = /^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$/u;

export class SynthesisLayoutError extends Error {
  constructor(readonly code: string) {
    super(code);
    this.name = "SynthesisLayoutError";
  }
}

/** Every synthesis artifact of one workspace, all windows: the directory a workspace's erase deletes. */
export function workspaceSynthesisDirectory(keyRoot: string, orgId: string, workspace: string): string {
  if (!KEY_ROOT_SEGMENT.test(keyRoot)) throw new SynthesisLayoutError("synthesis_root_invalid");
  if (!ORG_SEGMENT.test(orgId) || orgId.startsWith(".")) throw new SynthesisLayoutError("synthesis_org_invalid");
  if (!WORKSPACE.test(workspace)) throw new SynthesisLayoutError("synthesis_workspace_invalid");
  return `${keyRoot}/${orgId}/v1/ws=${layoutComponent(workspace)}/${SYNTHESIS_PREFIX_SEGMENT}`;
}

/** The prefix of one workspace and window. */
export function workspaceSynthesisPrefix(keyRoot: string, orgId: string, workspace: string, window: { since: string; until: string }): string {
  const since = Date.parse(window.since), until = Date.parse(window.until);
  if (!Number.isSafeInteger(since) || !Number.isSafeInteger(until) || since < 0 || since >= until) throw new SynthesisLayoutError("synthesis_window_invalid");
  return `${workspaceSynthesisDirectory(keyRoot, orgId, workspace)}w=${since}-${until}/`;
}
