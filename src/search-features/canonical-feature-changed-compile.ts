/**
 * Compile-only ABI proof: an incomplete prior pair is a TYPE ERROR on the
 * concrete class call, not only on the union alias. This file is compiled by
 * the repository typecheck (it lives under src/, not __tests__/). It is never
 * imported at runtime — the @ts-expect-error directives are the proof.
 */
import type { SqlQuery } from "./canonical-journal.ts";
import { CanonicalFeatureChanged } from "./canonical-feature-changed.ts";

declare const db: SqlQuery;
const fn = new CanonicalFeatureChanged(db);
const base = {
  scope: { orgId: "o", workspaceId: "w", harness: "h", sessionId: "s",
           generation: "a".repeat(64) },
  source: { authorityId: "u", epoch: "1", fingerprint: "a".repeat(64) },
  pointerSet: { authorityId: "u", hash: "c".repeat(64), count: "1" },
};
// A valid replacement compiles.
void fn.canonicalFeatureChanged({
  ...base,
  priorSource: { authorityId: "v", epoch: "2", fingerprint: "b".repeat(64) },
  priorPointerSet: { authorityId: "v", hash: "d".repeat(64), count: "1" },
});
// @ts-expect-error incomplete prior pair: priorSource set, priorPointerSet missing
void fn.canonicalFeatureChanged({
  ...base,
  priorSource: { authorityId: "v", epoch: "2", fingerprint: "b".repeat(64) },
});
// @ts-expect-error stray priorPointerSet when priorSource is null
void fn.canonicalFeatureChanged({
  ...base, priorSource: null,
  priorPointerSet: { authorityId: "v", hash: "d".repeat(64), count: "1" },
});
export {};
