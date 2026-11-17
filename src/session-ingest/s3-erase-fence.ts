/**
 * INGEST_S3_ERASE_FENCE (issue #613): the operator per-session erase, honoured by S3 upload ledger admission.
 *
 * With INGEST_LEDGER=s3, admission reads only S3 (ledger-s3.ts); the operator erase fence lives in SQL
 * (sessions.session_erasure_fences, src/session-erase/runtime.ts). This module carries that erase into S3:
 *
 *   - write: after the SQL erase begin has recorded the fence, the erase route writes one create-only tombstone
 *     per session pair (the canonical pair and its layoutComponent physical pair) BEFORE it answers. A store
 *     failure is a 503 `unavailable` (Control retries; the SQL begin is idempotent and a tombstone that already
 *     exists counts as written), so the route never reports an erase whose tombstone is missing;
 *   - read: a SessionWriteGuard on deliver (`withSessionWrites`) HEADs the tombstones of every kept session of a
 *     batch, after the claim and before any canonical write, and refuses with the SQL guard's own answer, 409
 *     `operator_session_erased`. Every S3-ledger admission path goes through deliver: the synchronous POST, the
 *     async consumer (a 409 is terminal there) and upload-inbox promotion. A store error other than not-found
 *     fails closed (503, retried).
 *
 * Tombstones are never deleted by this code (an operator un-erase is out of scope), and they live in the Locker
 * bucket: `<keyRoot>/.ledger/session-erase/...` is not a ledger key (ledger-store.ts ledgerKeyMatcher).
 *
 * Scope: the operator PER-SESSION erase only. Whole-user erase, session selection, upload consent and the erase
 * bytes worker stay refused on INGEST_LEDGER=s3 (selfhost-config.ts). Alias session ids that only SQL knows
 * (ingest_operator_runtime_ids) are fenced by the SQL guard in the erase process, not by a tombstone.
 *
 * Residual race: a claim in another process that passed its tombstone HEAD just before the tombstone write can
 * still land that batch's bytes. It is the same kind of obligation as the SQL guard's cross-process one (the SQL
 * fence and an in-flight writer in another process), and it is closed by the operator's bytes erase, not here.
 */
import { createHash } from "node:crypto";
import { isObjectStoreError, objectKey, type ObjectStore } from "../shared/vendor.ts";
import { layoutComponent } from "../shared/layout-component.ts";
import type { SessionWriteGuard } from "../session-erase/runtime.ts";
import { SessionEraseServiceError } from "../session-erase/receipt.ts";
import type { SessionEraseKey, SessionEraseService } from "./session-erase.ts";
import { DeliveryError } from "./vendor/delivery/delivery-contracts.ts";

export const S3_ERASE_FENCE_VAR = "INGEST_S3_ERASE_FENCE";

/** off (unset, "" or "off", the default: no guard, no write) | exactly "on". Anything else refuses startup. */
export function s3EraseFenceMode(raw: string | undefined): "off" | "on" {
  if (raw === undefined || raw === "" || raw === "off") return "off";
  if (raw === "on") return "on";
  throw new Error(`96bulls: ${S3_ERASE_FENCE_VAR} must be off or on`);
}

const h = (value: unknown) => createHash("sha256").update(JSON.stringify(value)).digest("hex").slice(0, 32);

/** The tenant and session are hashed: no separator in an id can move the key. The document names them. */
export const sessionEraseTombstoneKey = (keyRoot: string, t: { orgId: string; workspaceId: string }, source: string, sessionId: string) =>
  `${keyRoot}/.ledger/session-erase/v1/t=${h([t.orgId, t.workspaceId])}/s=${h([source, sessionId])}.json`;

/** The canonical pair and its physical layout pair (the SQL guard fences both, runtime.ts sqlSessionWriteGuard). */
function pairs(sessions: readonly (readonly [string, string])[]): [string, string][] {
  const out = new Map<string, [string, string]>();
  for (const [source, session] of sessions)
    for (const pair of [[source, session], [layoutComponent(source), layoutComponent(session)]] as [string, string][])
      out.set(JSON.stringify(pair), pair);
  return [...out.values()];
}

/** Create-only tombstones for one erased session. Throws SessionEraseServiceError("unavailable") on any store failure. */
export async function writeSessionEraseTombstones(store: ObjectStore, keyRoot: string, key: SessionEraseKey, atMs: number): Promise<void> {
  for (const [source, sessionId] of pairs([[key.source, key.sessionId]])) {
    const doc = { v: 1, orgId: key.orgId, workspaceId: key.workspaceId, source, sessionId, atMs };
    try {
      await store.put(objectKey(sessionEraseTombstoneKey(keyRoot, key, source, sessionId)), new TextEncoder().encode(JSON.stringify(doc)),
        { ifNoneMatch: true, contentType: "application/json" });
    } catch (error) {
      if (isObjectStoreError(error, "precondition_failed") || isObjectStoreError(error, "already_exists")) continue;
      throw new SessionEraseServiceError("unavailable");
    }
  }
}

/** The SQL erase service, plus the tombstones before the receipt is returned. `receipt` is unchanged. */
export function withS3EraseTombstones(service: SessionEraseService, store: ObjectStore, keyRoot: string, nowMs: () => number = Date.now): SessionEraseService {
  return {
    ...service,
    async erase(key, request) {
      const receipt = await service.erase(key, request);
      await writeSessionEraseTombstones(store, keyRoot, key, nowMs());
      return receipt;
    },
  };
}

/** Refuses a batch with any tombstoned session (409 operator_session_erased); a store error fails closed (503). */
export function s3EraseFenceGuard(store: ObjectStore, keyRoot: string): SessionWriteGuard {
  return async (tenant, sessions, work) => {
    if (sessions.length === 0) return work();
    const found = await Promise.all(pairs(sessions).map(async ([source, sessionId]) => {
      try { return (await store.head(objectKey(sessionEraseTombstoneKey(keyRoot, tenant, source, sessionId)))) !== undefined; }
      catch (error) {
        if (isObjectStoreError(error, "not_found")) return false;
        throw new DeliveryError("delivery_unavailable", 503, "Durable delivery is temporarily unavailable");
      }
    }));
    if (found.some(Boolean)) throw new DeliveryError("operator_session_erased", 409, "The session object namespace is permanently reserved by an operator erase");
    return work();
  };
}

/** Both guards: the S3 tombstone check first, then the inner (SQL) guard around the writes. */
export function composeSessionWriteGuards(outer: SessionWriteGuard, inner: SessionWriteGuard | undefined): SessionWriteGuard {
  return inner ? (tenant, sessions, work) => outer(tenant, sessions, () => inner(tenant, sessions, work)) : outer;
}
