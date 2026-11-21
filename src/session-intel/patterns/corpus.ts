/** Verified snapshot inputs and explicit identity joins, with no content inference. */
import { coverageGap, type Snapshot } from "../replay/snapshot.ts";
import { canonicalJson, textCompare } from "../replay/util.ts";
import { ANALYSIS_BOUNDS, type Corpus } from "./types.ts";
import { readActionScope } from "./action-scope.ts";

export class PatternError extends Error { constructor(message: string) { super(message); this.name = "PatternError"; } }

export function buildCorpus(inputs: readonly Snapshot[], options: { sameIdentitySpace?: boolean; now?: Date; actionScope?: boolean } = {}): Corpus {
  if (!inputs.length || inputs.length > ANALYSIS_BOUNDS.inputs) throw new PatternError("provide 1 to 16 snapshots");
  if (inputs.length > 1 && options.sameIdentitySpace !== true) throw new PatternError("multiple snapshots need an explicit shared identity-space assertion");
  const now = (options.now ?? new Date()).toISOString();
  const window = inputs[0]!.manifest.window;
  const rows = new Map<string, Corpus["sessions"][number]>();
  const actionScope: Corpus["actionScope"] = options.actionScope ? new Map() : undefined;
  const scopeRows = new Map<string, string>();
  let tenant: string | undefined, duplicates = 0, facts = 0;
  if (inputs.reduce((n, s) => n + s.sessions.length, 0) > ANALYSIS_BOUNDS.sessions) throw new PatternError("analysis input row budget exceeded");
  const charge = (s: Corpus["sessions"][number]) => {
    facts += s.writes.length + s.commits.length + s.outcomes.length + s.claims.length + s.usage.length + s.summaryTimeline.length;
    if (facts > ANALYSIS_BOUNDS.facts) throw new PatternError("analysis input fact budget exceeded; use a smaller explicit frame");
  };
  // Action mode needs a frame bound before indexing. Legacy keeps its original validation order.
  if (actionScope) for (const input of inputs) for (const s of input.sessions) charge(s);
  const manifests = new Set<string>();
  for (const input of inputs) {
    if (manifests.has(input.manifestSha256)) throw new PatternError("repeated input manifest");
    manifests.add(input.manifestSha256);
    if (input.manifest.deleteBy <= now) throw new PatternError("input past deleteBy; delete it rather than analyse it");
    if (input.manifest.window.since !== window.since || input.manifest.window.until !== window.until)
      throw new PatternError("joined snapshots must have exactly the same window");
    let actions: Corpus["actionScope"];
    try { actions = actionScope ? readActionScope(input, options.now ?? new Date()) : undefined; }
    catch { throw new PatternError("invalid or missing manifest-bound action provenance; values withheld"); }
    const perSession = new Map<string, Array<[string, unknown]>>();
    for (const [key, r] of actions ?? []) {
      const sk = JSON.stringify([r.harness, r.sessionId]);
      if (!perSession.has(sk)) perSession.set(sk, []);
      perSession.get(sk)!.push([key, r]);
    }
    for (const s of input.sessions) {
      if (!actionScope) charge(s);
      const ownTenant = JSON.stringify([s.orgId, s.workspaceId]);
      tenant ??= ownTenant;
      if (tenant !== ownTenant) throw new PatternError("analysis must contain one tenant and workspace");
      const key = JSON.stringify([s.harness, s.sessionId]);
      const ownScope = actions ? canonicalJson((perSession.get(key) ?? []).sort(([a], [b]) => textCompare(a, b))) : "";
      const prior = rows.get(key);
      if (prior) {
        if (canonicalJson(prior) !== canonicalJson(s)) throw new PatternError("conflicting duplicate session; do not silently merge facts");
        if (actions && scopeRows.get(key) !== ownScope) throw new PatternError("conflicting duplicate action provenance; do not silently merge scope");
        duplicates++;
        continue;
      }
      rows.set(key, s);
      if (actions) scopeRows.set(key, ownScope);
    }
    for (const [key, r] of actions ?? []) actionScope!.set(key, r);
  }
  return {
    ...(actionScope ? { actionScope } : {}),
    inputs: inputs.map((s) => ({ manifestSha256: s.manifestSha256, sessions: s.sessions.length,
      coverage: s.extraFiles?.["coverage.json"] === undefined ? "not_declared" as const : coverageGap(s) ? "declared_gap" as const : "declared_verified" as const,
    })).sort((a, b) => textCompare(a.manifestSha256, b.manifestSha256)),
    sessions: [...rows.entries()].sort(([a], [b]) => textCompare(a, b)).map(([, s]) => s),
    window: { ...window }, deleteBy: inputs.map((s) => s.manifest.deleteBy).sort(textCompare)[0]!,
    identity: inputs.length === 1 ? "single_snapshot" : "operator_asserted_shared_namespace", duplicateSessions: duplicates,
  };
}
