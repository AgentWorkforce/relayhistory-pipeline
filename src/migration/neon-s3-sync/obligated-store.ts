/**
 * The exporter's writer side of the shared unresolved-write obligation (write-obligation.ts v3), over
 * SyncState (the erasure job's own tables, same file). Every PUT gets a durable invocation id BEFORE it
 * is sent; its OWN confirmed response records the exact version it created ("null" on an unversioned
 * store); a definite refusal of that very request (an identical object already there: no write) clears
 * it; anything else (timeout, abort, lost response, crash) leaves it dispatched: unresolved, so the
 * erasure job never reports the prefix erased while it remains. Nothing else may clear it.
 */
import type { ObjectStore } from "../../shared/vendor.ts";
import { isObjectStoreError } from "../../shared/vendor.ts";
import type { SyncState } from "./state.ts";

export function obligatedStore(store: ObjectStore, state: SyncState, holder: string, acked: Map<string, string>): ObjectStore {
  return {
    ...store,
    head: (k) => store.head(k),
    get: (k, r) => store.get(k, r),
    getObject: (k, r) => store.getObject(k, r),
    list: (p, c) => store.list(p, c),
    presignGet: (k, t) => store.presignGet(k, t),
    delete: (k) => store.delete(k),
    put: async (key, body, opts) => {
      const id = state.dispatchWrite(String(key), holder);
      let ack;
      try {
        ack = await store.put(key, body, opts);
      } catch (error) {
        // A definite refusal of THIS request (different content already there): no write happened.
        if (isObjectStoreError(error, "already_exists") || isObjectStoreError(error, "precondition_failed")) state.refuseWrite(id);
        throw error; // otherwise the outcome is unknown: the invocation stays dispatched
      }
      if (!ack.created) {
        // The create was refused (412) and identical content is already stored: this PUT wrote nothing.
        state.refuseWrite(id);
        return ack;
      }
      const versionId = ack.version.version ?? "null";
      state.ackWrite(id, versionId);
      acked.set(String(key), versionId);
      return ack;
    },
  };
}

/**
 * The exporter's manifest claim (exportUnit `onPublish`), run BEFORE the manifest becomes visible: every object the
 * manifest references (new and reused chunks, and the manifest key) is claimed under the erasure job's reservation
 * lock: this run's acked versions exactly, anything else (reused / unchanged, version id unknown here) at the key
 * level. Throws `publish_reserved` (and the manifest is NOT written) if any of them is reserved for erasure; once it
 * succeeds, an erasure reserve of any claimed version is refused. No network await happens inside the transaction.
 */
export function manifestClaim(state: SyncState, acked: ReadonlyMap<string, string>, holder: string, gens: Readonly<Record<string, string>>) {
  // The lease proof: the manifest's session prefix, still held by this exporter at the generation it acquired.
  return (manifestKey: string, refs: readonly string[]) => {
    const p = manifestKey.slice(0, -"manifest.json".length);
    state.publishWrites(refs.map((key) => ({ key, versionId: acked.get(key) ?? "*" })), { holder, gens: gens[p] ? { [p]: gens[p]! } : {} });
  };
}
