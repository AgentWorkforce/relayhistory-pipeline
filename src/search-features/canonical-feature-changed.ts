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
    source: { authorityId?: string; epoch: string; fingerprint: string };
    priorSource: { authorityId?: string; epoch: string; fingerprint: string } | null;
    pointerSet: { hash: string; count: number; pointers: unknown[] };
    /** Required when priorSource is set: the PRIOR image's inventory. */
    priorPointerSet?: { hash: string; count: number; pointers: unknown[] };
  }): Promise<string> {
    if (!input.scope?.orgId || !input.scope.sessionId ||
        !input.source?.epoch || !input.source.fingerprint ||
        !input.pointerSet?.hash)
      throw new Error("canonical_feature_changed: invalid input");
    // Pointer sets carry only hash+count — the caller validates the hash
    // covers the exact inventory; nothing raw is persisted in the journal.
    // priorSource and priorPointerSet must be both NULL or both set.
    if ((input.priorSource === null) !== (input.priorPointerSet === undefined))
      throw new Error("prior_incomplete");
    // Self-retire refused: a replacement's prior authority differs from new.
    if (input.priorSource && input.priorPointerSet &&
        input.priorSource.fingerprint === input.source.fingerprint)
      throw new Error("self_retire");
    const versions = new CanonicalVersionStore(this.db);
    const journal = new CanonicalJournalStore(this.db);
    // Immutable authority versions: imageKey binds the authority identity,
    // not just org:session — a replacement is a distinct authority version.
    const imageKey = `${input.scope.orgId}:${input.scope.sessionId}@${input.source.authorityId ?? input.source.fingerprint}`;
    if (input.priorSource !== null) {
      // Replacement: TWO ordered rows — retire the PRIOR image's exact
      // inventory, then publish the NEW image. The producer consumes the pair
      // so it can tombstone the exact prior keys and land the new image.
      if (!input.priorPointerSet?.hash)
        throw new Error("canonical_feature_changed: prior pointer set required");
      const retireRev = await versions.next();
      const retire: CanonicalJournalEntry = {
        kind: "retire", revision: retireRev, imageKey,
        schemaGeneration: input.priorSource.epoch,
        // retire row: source_authority=prior, source_hash=prior fingerprint,
        // pointer_set=PRIOR set, prior_keyset_hash=prior SET hash.
        sourceHash: input.priorSource.fingerprint,
        priorKeysetHash: input.priorPointerSet.hash,
        scope: input.scope,
        priorSourceAuthority: input.priorSource,
        pointerSet: input.priorPointerSet,
      };
      await journal.append(retire, String(BigInt(retireRev) - 1n));
      const publishRev = await versions.next();
      const publish: CanonicalJournalEntry = {
        kind: "publish", revision: publishRev, imageKey,
        schemaGeneration: input.source.epoch,
        sourceHash: input.source.fingerprint,
        // publish row: source_authority=new, prior_source_authority=prior,
        // pointer_set=NEW set, prior_keyset_hash=prior SET hash.
        priorKeysetHash: input.priorPointerSet.hash,
        scope: input.scope, sourceAuthority: input.source,
        priorSourceAuthority: input.priorSource,
        pointerSet: input.pointerSet,
      };
      await journal.append(publish, retireRev);
      return publishRev;
    }
    const revision = await versions.next();
    const entry: CanonicalJournalEntry = {
      kind: "publish", revision, imageKey,
      schemaGeneration: input.source.epoch,
      sourceHash: input.source.fingerprint,
      scope: input.scope, sourceAuthority: input.source,
      pointerSet: input.pointerSet,
    };
    await journal.append(entry, String(BigInt(revision) - 1n));
    return revision;
  }
}
