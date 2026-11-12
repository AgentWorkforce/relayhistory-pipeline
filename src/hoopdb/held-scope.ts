/**
 * Provenance registry for release-target held scopes (ExpectedSchema.held).
 *
 * Only heldScope() in src/hoopdb/migrate.ts creates a scope: it derives each held
 * step's `creates` from the tree's own SQL bytes, deep-freezes the result and marks
 * the array and every entry here. schemaVerdict (src/shared/db.ts) refuses any held
 * value this registry does not own, so a hand-built, copied or proxied map can never
 * widen the live function gate. This module has no imports, so db.ts can use it
 * without depending on migrate.ts.
 */
export type HeldScopeEntry = { readonly name: string; readonly creates: readonly string[] };
export type HeldScope = readonly HeldScopeEntry[];

const owned = new WeakSet<object>();

/** Deep-freezes a scope built by heldScope() and registers the array and each entry. */
export function markHeldScope(entries: readonly { name: string; creates: readonly string[] }[]): HeldScope {
  const out = Object.freeze(entries.map((e) => Object.freeze({ name: e.name, creates: Object.freeze([...e.creates]) })));
  for (const e of out) owned.add(e);
  owned.add(out);
  return out;
}

/** True only for a scope array returned by markHeldScope whose entries are all its own. */
export function isHeldScope(x: unknown): x is HeldScope {
  return (
    Array.isArray(x) &&
    owned.has(x) &&
    Object.isFrozen(x) &&
    x.every((e) => typeof e === "object" && e !== null && owned.has(e) && Object.isFrozen(e) && Object.isFrozen(e.creates))
  );
}
