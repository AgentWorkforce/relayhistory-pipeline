/**
 * Erasing log payloads (docs/design/s3-ledger-log-commit.md, Remaining 3). A log-mode stream keeps each admitted
 * batch's raw records in a payload object (`<stream>/p/`) until the materializer applies the batch's entry. Before
 * this module, a privacy reset left a user's unapplied payloads in place until the stream's next apply.
 *
 * Two erasures, both scoped to ONE (org, workspace, user) by listing that user's own log directory
 * (`logUserRoot`), never the deployment-wide stream registry: a stream whose registry pointer or identity doc is
 * missing or corrupt is still found, and no other user's stream is ever read or touched.
 *
 *   eraseLogPayloads             every payload of the user, unconditionally (a whole-user erase). The caller must
 *                                already hold the user's admissions (see "Ordering" below), or a racing admission can
 *                                write a new payload after the listing; the verification listing then finds it and
 *                                the erase fails (retryable), it never reports success over it.
 *   eraseSupersededLogPayloads   the payloads of every unapplied batch the materializer will skip as superseded by a
 *                                reset: its admission snapshot's `resets` is below the fence's `resets` (exactly the
 *                                materializer's rule, ledger-log-materialize.ts `fence.resets > b.admitted.resets`).
 *                                A payload a live batch also names is kept. Used by the S3 ledger's reset
 *                                (ledger-s3.ts `resetLogErase`, INGEST_LEDGER_LOG_ERASE=on) BEFORE the reset is
 *                                completed or answered, on every answer (applied, replay, stale), so a retry of a
 *                                reset whose first erase failed erases again.
 *
 * Fail closed: any store fault, an unreadable stream (LogCorrupt, LogUnavailable) or a payload still present after its delete throws
 * `LedgerUnavailableError` (`ledger_log_erase_failed` / `ledger_log_erase_unverified`): a retry, never success.
 *
 * Ordering (what a reset relies on): log admission refuses a user while a reset is begun and not completed
 * (ledger-log-admit.ts: `ledger_reset_in_flight`, `ledger_control_pending`), and the reset's erase runs after its
 * begin CAS. So every batch appended by an admission that read the fence after the begin carries the new `resets`
 * and is not touched. An admission that read the fence BEFORE the begin and appends AFTER this erase listed its
 * stream carries the old `resets`: its payload is not deleted by the reset, but the materializer never writes it
 * (the same snapshot rule) and deletes it when it reaches the entry (or, if the append never happens, orphan
 * collection takes it after ORPHAN_GRACE_MS). That window is bounded by one admission's fence-read -> append time.
 *
 * Deletes remove the current object version only, as the materializer's own payload drop does: on a versioned
 * bucket, noncurrent versions are the bucket lifecycle's (a separate retention concern, Remaining 5).
 */
import { isObjectStoreError, objectKey, type ObjectStore } from "../shared/vendor.ts";
import { LedgerUnavailableError, type LedgerTenant } from "./ledger.ts";
import { LogCorrupt, LogUnavailable, logKeysAt, logUserRoot, readEntries, type LogKeys } from "./ledger-log.ts";
import { materializedThrough } from "./ledger-log-materialize.ts";

export const INGEST_LEDGER_LOG_ERASE_VAR = "INGEST_LEDGER_LOG_ERASE";

/** Unset, "" or "off": off (today, byte for byte). Exactly "on" otherwise; anything else throws. */
export function ledgerLogEraseMode(raw: string | undefined): "off" | "on" {
  const v = raw?.trim();
  if (v === undefined || v === "" || v === "off") return "off";
  if (v === "on") return "on";
  throw new Error(`session ingest: ${INGEST_LEDGER_LOG_ERASE_VAR} must be off or on`);
}

const STREAM_ROOT = /^(.*\/ledger\/log\/o=[0-9a-f]{32})\//;

/** Store faults and unreadable streams become one retryable code; an erase never half-succeeds silently. */
async function failClosed<T>(work: () => Promise<T>): Promise<T> {
  try { return await work(); }
  catch (error) {
    if (error instanceof LedgerUnavailableError) throw error;
    if (error instanceof LogCorrupt || error instanceof LogUnavailable || isObjectStoreError(error)) throw new LedgerUnavailableError("ledger_log_erase_failed");
    throw error;
  }
}

async function listUnder(store: ObjectStore, prefix: string): Promise<string[]> {
  const out: string[] = [];
  let cursor: string | undefined;
  do {
    const page = await store.list(objectKey(prefix.slice(0, -1)), cursor);
    for (const item of page.items) { const k = String(item.key); if (k.startsWith(prefix)) out.push(k); }
    cursor = page.nextCursor;
  } while (cursor);
  return out;
}

/** Every log stream of the user, found by listing the user's own log directory (no registry read). */
export async function userLogStreams(store: ObjectStore, keyRoot: string, tenant: LedgerTenant): Promise<LogKeys[]> {
  const prefix = logUserRoot(keyRoot, tenant);
  const roots = new Set<string>();
  for (const k of await listUnder(store, prefix)) {
    const m = STREAM_ROOT.exec(k);
    if (m && m[1]!.startsWith(prefix)) roots.add(m[1]!);
  }
  return [...roots].sort().map(logKeysAt);
}

async function deleteVerified(store: ObjectStore, keys: readonly string[]): Promise<void> {
  for (const k of keys) await store.delete(objectKey(k));
  for (const k of keys) if ((await store.head(objectKey(k))) !== undefined) throw new LedgerUnavailableError("ledger_log_erase_unverified");
}

/**
 * Delete every payload of the user's log streams. Returns how many were deleted. Throws (retryable) on any fault or if
 * a fresh listing still finds a payload (a write that raced the erase): the caller must not report the erase done.
 * A batch whose payload is gone is skipped by the materializer (`payload_erased`), never written.
 */
export async function eraseLogPayloads(store: ObjectStore, keyRoot: string, tenant: LedgerTenant): Promise<number> {
  return failClosed(async () => {
    let n = 0;
    for (const keys of await userLogStreams(store, keyRoot, tenant)) {
      const found = await listUnder(store, keys.payloads);
      for (const k of found) await store.delete(objectKey(k));
      n += found.length;
      // A fresh listing, not the deletes' answers: a payload PUT that raced the erase fails it.
      if ((await listUnder(store, keys.payloads)).length > 0) throw new LedgerUnavailableError("ledger_log_erase_unverified");
    }
    return n;
  });
}

/**
 * Delete the payloads of every unapplied batch of the user whose admission snapshot predates `resets` (the fence's
 * reset count, this reset included). Batches without a snapshot (library tests only; admission always writes one)
 * are left to the materializer's journal re-check. Returns how many payloads were deleted.
 */
export async function eraseSupersededLogPayloads(store: ObjectStore, keyRoot: string, tenant: LedgerTenant, resets: number): Promise<number> {
  if (!Number.isSafeInteger(resets) || resets < 0) throw new LedgerUnavailableError("ledger_input_refused");
  if (resets === 0) return 0;
  return failClosed(async () => {
    let n = 0;
    for (const keys of await userLogStreams(store, keyRoot, tenant)) {
      const through = await materializedThrough(store, keys);
      const doomed = new Set<string>(), live = new Set<string>();
      for await (const e of readEntries(store, keys, Math.max(through, 0))) {
        if (e.seq <= through || e.kind !== "batches") continue;
        for (const b of e.batches) {
          const superseded = b.admitted !== undefined && b.admitted.resets < resets;
          for (const p of b.payloads) {
            // An entry naming a key outside its own stream's payload directory is not something admission writes.
            if (!p.key.startsWith(keys.payloads)) throw new LedgerUnavailableError("ledger_log_erase_failed");
            (superseded ? doomed : live).add(p.key);
          }
        }
      }
      const targets = [...doomed].filter((k) => !live.has(k));
      await deleteVerified(store, targets);
      n += targets.length;
    }
    return n;
  });
}
