/**
 * Canonical historical backfill driver: turns the exported org→keys manifest
 * index (S3 v1 corpus, ~129k groups + catch-up) into publish journal entries
 * the producer drains. One image → one managed-revision publish entry; the
 * version counter is seeded ABOVE the last legacy feature clock before the
 * first entry so historical fills never collide with the legacy series.
 * Bounded: per-image enumeration, no bulk payload reads.
 */
import {
  CanonicalJournalStore,
  CanonicalVersionStore,
  type SqlQuery,
} from "./canonical-journal.ts";
import type { CanonicalJournalEntry } from "./canonical-version.ts";

/** One image descriptor from the org→keys manifest index. */
export interface CanonicalBackfillImage {
  orgId: string; workspaceId: string; harness: string; sessionId: string;
  imageKey: string; sourceHash: string;
  schemaGeneration: string; epoch: string; fingerprint: string;
  pointerSetHash: string; pointerSetCount: number;
}

/** The index enumeration port — a stream of image descriptors (JSONL or RPC). */
export type CanonicalBackfillIndex =
  AsyncGenerator<CanonicalBackfillImage, void, void>;

export type CanonicalBackfillResult =
  | { kind: "copied"; orgId: string; entries: number; lastRevision: string }
  | { kind: "refused"; reason: string };

/**
 * Write publish journal entries for one image enumeration. The caller seeds
 * the version counter above the last legacy clock BEFORE the first call —
 * seed() refuses to move backwards, so a re-run of the same index idempotently
 * lands only new images.
 */
export async function backfillFromIndex(
  db: SqlQuery,
  images: CanonicalBackfillIndex,
  seedAboveLegacy: string,
): Promise<CanonicalBackfillResult> {
  const versions = new CanonicalVersionStore(db);
  const journal = new CanonicalJournalStore(db);
  await versions.seed(seedAboveLegacy);
  let entries = 0, lastRevision = "", orgId = "";
  for await (const img of images) {
    orgId = img.orgId;
    const revision = await versions.next();
    const entry: CanonicalJournalEntry = {
      kind: "publish", revision, imageKey: img.imageKey,
      schemaGeneration: img.schemaGeneration, sourceHash: img.sourceHash,
      scope: {
        orgId: img.orgId, workspaceId: img.workspaceId,
        harness: img.harness, sessionId: img.sessionId,
      },
      sourceAuthority: { epoch: img.epoch, fingerprint: img.fingerprint },
      pointerSet: { hash: img.pointerSetHash, count: img.pointerSetCount },
    };
    await journal.append(entry, String(BigInt(revision) - 1n));
    entries += 1;
    lastRevision = revision;
  }
  return { kind: "copied", orgId, entries, lastRevision };
}
