/**
 * The 610 transaction port client: canonical_feature_changed runs INSIDE the
 * 620 final-CAS transaction — it is implemented by the owner-only SQL invoker
 * (sessions.canonical_feature_changed in the 040610 migration), never by a
 * TS-side direct journal write. Rollback of the enclosing transaction emits
 * nothing; a stale source CAS refuses. OWNER-ONLY invoker: inputs are
 * server-constructed, never app-provided.
 */
import type { SqlQuery } from "./canonical-journal.ts";
import type { CanonicalFeatureChangedPort } from "./canonical-version.ts";

export class CanonicalFeatureChanged implements CanonicalFeatureChangedPort {
  constructor(readonly db: SqlQuery) {}
  async canonicalFeatureChanged(input: {
    scope: { orgId: string; workspaceId: string; harness: string;
             sessionId: string; generation: string };
    source: { authorityId: string; epoch: string; fingerprint: string };
    priorSource: { authorityId: string; epoch: string; fingerprint: string } | null;
    pointerSet: { authorityId: string; hash: string; count: string };
    /** Required when priorSource is set: the PRIOR image's inventory. */
    priorPointerSet?: { authorityId: string; hash: string; count: string };
  }): Promise<string> {
    if (!input.scope?.orgId || !input.scope.sessionId ||
        !input.source?.epoch || !input.source.fingerprint ||
        !input.pointerSet?.hash)
      throw new Error("canonical_feature_changed: invalid input");
    // priorSource and priorPointerSet must be both NULL or both set.
    if ((input.priorSource === null) !== (input.priorPointerSet === undefined))
      throw new Error("prior_incomplete");
    // Self-retire refused: a replacement's prior authority differs from new.
    if (input.priorSource && input.priorPointerSet &&
        input.priorSource.fingerprint === input.source.fingerprint)
      throw new Error("self_retire");
    // Sole live writer: the owner-only SQL invoker inside the 620 install CAS.
    const { rows } = await this.db.query<{ rev: string }>(
      "SELECT sessions.canonical_feature_changed($1,$2,$3,$4,$5)::text AS rev",
      [JSON.stringify(input.scope), JSON.stringify(input.source),
        input.priorSource ? JSON.stringify(input.priorSource) : null,
        JSON.stringify(input.pointerSet),
        input.priorPointerSet ? JSON.stringify(input.priorPointerSet) : null]);
    return rows[0]!.rev;
  }
}
