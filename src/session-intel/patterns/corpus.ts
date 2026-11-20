/** Verified snapshot inputs and explicit identity joins, with no content inference. */
import { coverageGap, type Snapshot } from "../replay/snapshot.ts";
import { canonicalJson, textCompare } from "../replay/util.ts";
import { ANALYSIS_BOUNDS, type Corpus } from "./types.ts";

export class PatternError extends Error { constructor(message: string) { super(message); this.name = "PatternError"; } }

export function buildCorpus(inputs: readonly Snapshot[], options: { sameIdentitySpace?: boolean; now?: Date } = {}): Corpus {
  if (!inputs.length || inputs.length > ANALYSIS_BOUNDS.inputs) throw new PatternError("provide 1 to 16 snapshots");
  if (inputs.length > 1 && options.sameIdentitySpace !== true) throw new PatternError("multiple snapshots need an explicit shared identity-space assertion");
  const now = (options.now ?? new Date()).toISOString();
  const window = inputs[0]!.manifest.window;
  const rows = new Map<string, Corpus["sessions"][number]>();
  let tenant: string | undefined, duplicates = 0, facts = 0;
  if (inputs.reduce((n, s) => n + s.sessions.length, 0) > ANALYSIS_BOUNDS.sessions) throw new PatternError("analysis input row budget exceeded");
  for (const input of inputs) {
    if (input.manifest.deleteBy <= now) throw new PatternError("input past deleteBy; delete it rather than analyse it");
    if (input.manifest.window.since !== window.since || input.manifest.window.until !== window.until)
      throw new PatternError("joined snapshots must have exactly the same window");
    for (const s of input.sessions) {
      facts += s.writes.length + s.commits.length + s.outcomes.length + s.claims.length + s.usage.length + s.summaryTimeline.length;
      if (facts > ANALYSIS_BOUNDS.facts) throw new PatternError("analysis input fact budget exceeded; use a smaller explicit frame");
      const ownTenant = JSON.stringify([s.orgId, s.workspaceId]);
      tenant ??= ownTenant;
      if (tenant !== ownTenant) throw new PatternError("analysis must contain one tenant and workspace");
      const key = JSON.stringify([s.harness, s.sessionId]);
      const prior = rows.get(key);
      if (prior) {
        if (canonicalJson(prior) !== canonicalJson(s)) throw new PatternError("conflicting duplicate session; do not silently merge facts");
        duplicates++;
        continue;
      }
      rows.set(key, s);
    }
  }
  return {
    inputs: inputs.map((s) => ({ manifestSha256: s.manifestSha256, sessions: s.sessions.length,
      coverage: s.extraFiles?.["coverage.json"] === undefined ? "not_declared" as const : coverageGap(s) ? "declared_gap" as const : "declared_verified" as const,
    })).sort((a, b) => textCompare(a.manifestSha256, b.manifestSha256)),
    sessions: [...rows.entries()].sort(([a], [b]) => textCompare(a, b)).map(([, s]) => s),
    window: { ...window }, deleteBy: inputs.map((s) => s.manifest.deleteBy).sort(textCompare)[0]!,
    identity: inputs.length === 1 ? "single_snapshot" : "operator_asserted_shared_namespace", duplicateSessions: duplicates,
  };
}
