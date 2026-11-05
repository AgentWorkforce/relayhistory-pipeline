/**
 * Canonical historical backfill driver: turns the exported org→keys manifest
 * index (S3 v1 corpus, ~129k groups + catch-up) into publish journal entries
 * the producer drains. One image → one managed-revision publish entry; the
 * version counter is seeded ABOVE the last legacy feature clock before the
 * first entry so historical fills never collide with the legacy series.
 * Bounded: per-image enumeration, no bulk payload reads.
 */
import type { SqlQuery } from "./canonical-journal.ts";
import { CanonicalFeatureChanged } from "./canonical-feature-changed.ts";


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
  _seedAboveLegacy: string,
): Promise<CanonicalBackfillResult> {
  // The invoker is the sole journal writer — one publish call per image.
  const fn = new CanonicalFeatureChanged(db);
  let entries = 0, lastRevision = "", orgId = "";
  for await (const img of images) {
    orgId = img.orgId;
    lastRevision = await fn.canonicalFeatureChanged({
      scope: {
        orgId: img.orgId, workspaceId: img.workspaceId,
        harness: img.harness, sessionId: img.sessionId,
      },
      source: { epoch: img.epoch, fingerprint: img.fingerprint,
                authorityId: img.imageKey },
      priorSource: null,
      pointerSet: { hash: img.pointerSetHash, count: img.pointerSetCount },
    });
    entries += 1;
  }
  return { kind: "copied", orgId, entries, lastRevision };
}
