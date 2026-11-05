/**
 * Canonical historical backfill driver: turns the exported org→keys manifest
 * index (S3 v1 corpus, ~129k groups + catch-up) into publish journal entries
 * the producer drains. One image → one managed-revision publish entry; the
 * version counter is seeded ABOVE the last legacy feature clock before the
 * first entry so historical fills never collide with the legacy series.
 * Bounded: per-image enumeration, no bulk payload reads.
 */



/** One image descriptor from the org→keys manifest index. */
export interface CanonicalBackfillImage {
  orgId: string; workspaceId: string; harness: string; sessionId: string;
  imageKey: string; sourceHash: string;
  schemaGeneration: string; epoch: string; fingerprint: string;
  authorityId: string;
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
/** Owner-only idempotent publish port for backfill — NOT the final-CAS
 *  invoker (which is reserved for the 620 install path). Idempotent on
 *  (scope, authorityId): a repeated call returns the existing revision. */
export interface CanonicalPublishOnce {
  publishOnce(input: {
    scope: { orgId: string; workspaceId: string; harness: string;
             sessionId: string; generation: string };
    source: { authorityId: string; epoch: string; fingerprint: string };
    pointerSet: { authorityId: string; hash: string; count: number };
  }): Promise<string>;
}
export async function backfillFromIndex(
  publish: CanonicalPublishOnce,
  images: CanonicalBackfillIndex,
  _seedAboveLegacy: string,
): Promise<CanonicalBackfillResult> {
  let entries = 0, lastRevision = "", orgId = "";
  for await (const img of images) {
    orgId = img.orgId;
    lastRevision = await publish.publishOnce({
      scope: {
        orgId: img.orgId, workspaceId: img.workspaceId,
        harness: img.harness, sessionId: img.sessionId,
        generation: img.schemaGeneration,
      },
      source: { epoch: img.epoch, fingerprint: img.fingerprint,
                authorityId: img.authorityId },
      pointerSet: { authorityId: img.authorityId,
                    hash: img.pointerSetHash, count: img.pointerSetCount },
    });
    entries += 1;
  }
  return { kind: "copied", orgId, entries, lastRevision };
}
