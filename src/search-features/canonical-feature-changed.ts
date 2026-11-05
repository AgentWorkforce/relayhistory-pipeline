/**
 * The 610 transaction port implementation: canonical_feature_changed runs
 * INSIDE the 620 final CAS transaction — it validates the captured source
 * authority + pointer set, allocates a managed revision above the last legacy
 * clock, and durably appends the publish/retire journal entry. Rollback of
 * the enclosing transaction emits nothing; a stale source CAS refuses.
 * OWNER-ONLY invoker: inputs are server-constructed, never app-provided.
 */
import {
  CanonicalJournalStore,
  CanonicalVersionStore,
  type SqlQuery,
} from "./canonical-journal.ts";
import type {
  CanonicalFeatureChangedPort,
  CanonicalJournalEntry,
} from "./canonical-version.ts";

export class CanonicalFeatureChanged implements CanonicalFeatureChangedPort {
  constructor(readonly db: SqlQuery) {}
  async canonicalFeatureChanged(input: {
    scope: { orgId: string; workspaceId: string; harness: string;
             sessionId: string };
    source: { epoch: string; fingerprint: string };
    priorSource: { epoch: string; fingerprint: string } | null;
    pointerSet: { hash: string; count: number };
  }): Promise<string> {
    if (!input.scope?.orgId || !input.scope.sessionId ||
        !input.source?.epoch || !input.source.fingerprint ||
        !input.pointerSet?.hash)
      throw new Error("canonical_feature_changed: invalid input");
    const versions = new CanonicalVersionStore(this.db);
    const journal = new CanonicalJournalStore(this.db);
    const revision = await versions.next();
    const entry: CanonicalJournalEntry = input.priorSource === null
      ? {
          kind: "publish", revision,
          imageKey: `${input.scope.orgId}:${input.scope.sessionId}`,
          schemaGeneration: input.source.epoch,
          sourceHash: input.source.fingerprint,
          scope: input.scope, sourceAuthority: input.source,
          pointerSet: input.pointerSet,
        }
      : {
          kind: "retire", revision,
          imageKey: `${input.scope.orgId}:${input.scope.sessionId}`,
          schemaGeneration: input.source.epoch,
          priorKeysetHash: input.priorSource.fingerprint,
          scope: input.scope, priorSourceAuthority: input.priorSource,
          pointerSet: input.pointerSet,
        };
    await journal.append(entry, String(BigInt(revision) - 1n));
    return revision;
  }
}
