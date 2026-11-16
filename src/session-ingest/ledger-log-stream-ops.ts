/**
 * Operator steps for one stream (docs/design/s3-ledger-log-commit.md): status, switch to log mode, close (rollback).
 * Every mutating step is a dry run unless `apply` is set; the CLI is scripts/ledger-log-stream.ts.
 *
 *   status   legacy | log | closed, the log head, the materialized cursor, legacy batches still pending.
 *   switch   refuses while any legacy batch of the stream is pending (drain it first) or the stream is already in
 *            log mode; writes entry 0 with legacyCutover = "committed=<n legacy batches committed>", and registers
 *            the stream for the sweeper.
 *   close    refuses until every entry is applied (cursor = head); writes the close marker, after which the stream
 *            reads as legacy again (the admission consumer and the logModeGuard both read it). One way: a closed
 *            stream is not re-opened. A writer that had the stream cached may still append an entry it had in
 *            flight; the materializer keeps applying the log, so keep it running until the cursor equals the head
 *            again.
 */
import { isObjectStoreError, objectKey, readAll, type ObjectStore } from "../shared/vendor.ts";
import type { LedgerTenant } from "./ledger.ts";
import { enableLogMode, logKeys, openStream } from "./ledger-log.ts";
import { materializedThrough, registerLogStream } from "./ledger-log-materialize.ts";
import { s3LedgerKeys } from "./ledger-s3.ts";

export interface StreamStatus {
  mode: "legacy" | "log" | "closed";
  head: number | null;
  materializedThrough: number;
  legacyPending: number;
  legacyCommitted: number;
}

async function exists(store: ObjectStore, key: string) { return (await store.head(objectKey(key))) !== undefined; }

async function legacyBatches(store: ObjectStore, keyRoot: string, tenant: LedgerTenant, originId: string) {
  const one = s3LedgerKeys(keyRoot, tenant).batch(originId, "x");
  const prefix = one.slice(0, one.lastIndexOf("/") + 1);
  let pending = 0, committed = 0;
  let cursor: string | undefined;
  do {
    const page = await store.list(objectKey(prefix.slice(0, -1)), cursor);
    for (const item of page.items) {
      const key = String(item.key);
      if (!key.startsWith(prefix)) continue;
      try {
        const doc = JSON.parse(new TextDecoder().decode(await readAll((await store.getObject(objectKey(key))).body))) as { state?: unknown };
        if (doc.state === "pending") pending++; else if (doc.state === "committed") committed++;
      } catch (error) { if (!(isObjectStoreError(error) && error.code === "not_found")) throw error; }
    }
    cursor = page.nextCursor;
  } while (cursor);
  return { pending, committed };
}

export async function streamStatus(store: ObjectStore, keyRoot: string, tenant: LedgerTenant, originId: string): Promise<StreamStatus> {
  const keys = logKeys(keyRoot, tenant, originId);
  const [state, closed, through, legacy] = await Promise.all([openStream(store, keys), exists(store, keys.closed), materializedThrough(store, keys),
    legacyBatches(store, keyRoot, tenant, originId)]);
  return { mode: closed ? "closed" : state ? "log" : "legacy", head: state ? state.head : null, materializedThrough: through,
    legacyPending: legacy.pending, legacyCommitted: legacy.committed };
}

export interface StepResult { applied: boolean; action: string; refused?: string; status: StreamStatus }

export async function switchToLogMode(store: ObjectStore, keyRoot: string, tenant: LedgerTenant, originId: string, opts: { apply: boolean; writer: string }): Promise<StepResult> {
  const status = await streamStatus(store, keyRoot, tenant, originId);
  if (status.mode !== "legacy") return { applied: false, action: "switch", refused: `stream_is_${status.mode}`, status };
  if (status.legacyPending > 0) return { applied: false, action: "switch", refused: "legacy_batches_pending", status };
  if (!opts.apply) return { applied: false, action: "switch (dry run)", status };
  await enableLogMode(store, logKeys(keyRoot, tenant, originId), `committed=${status.legacyCommitted}`, { writer: opts.writer });
  await registerLogStream(store, keyRoot, tenant, originId);
  return { applied: true, action: "switch", status: await streamStatus(store, keyRoot, tenant, originId) };
}

export async function closeLogStream(store: ObjectStore, keyRoot: string, tenant: LedgerTenant, originId: string, opts: { apply: boolean }): Promise<StepResult> {
  const status = await streamStatus(store, keyRoot, tenant, originId);
  if (status.mode !== "log") return { applied: false, action: "close", refused: `stream_is_${status.mode}`, status };
  if (status.materializedThrough !== status.head) return { applied: false, action: "close", refused: "entries_not_applied", status };
  if (!opts.apply) return { applied: false, action: "close (dry run)", status };
  const keys = logKeys(keyRoot, tenant, originId);
  try {
    await store.put(objectKey(keys.closed), new TextEncoder().encode(JSON.stringify({ v: 1, closedAtHead: status.head })), { ifNoneMatch: true, contentType: "application/json" });
  } catch (error) { if (!(isObjectStoreError(error) && error.code === "already_exists")) throw error; }
  return { applied: true, action: "close", status: await streamStatus(store, keyRoot, tenant, originId) };
}
