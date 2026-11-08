/**
 * Self-host async admission consumer (SELFHOST_INGEST_ADMISSION=async|drain, selfhost-config.ts).
 * The hosted design is docs/design/s3-first-async-ingest.md; the self-host differences are in
 * docs/selfhost-async-admission.md. The hosted Worker (worker.ts) never imports this file.
 *
 *   request  (selfhost-server.ts + app.ts, no SQL): relaycontrol introspection -> journal witness (one
 *            store GET) -> parse + prepare + scrub (CPU) -> ONE create-only inbox PUT -> 200 queued receipt
 *   consumer (this file, same Node process): ListBucket scan of `<keyRoot>/.admission-inbox/` -> for each
 *            inbox object that still holds content: the UNCHANGED hosted `admitMessage`
 *            (admission-consumer.ts: claim -> privacy gate -> writes -> commit -> outcome marker ->
 *            inbox tombstone), with bounded concurrency and per-object backoff
 *
 * There is no queue: the inbox itself is the authority. A scan finds every acknowledged upload whose
 * content has not been retired by a terminal outcome, so a crash at any point (before the claim,
 * between the claim and the commit, between the commit and the outcome marker) is resumed by the next
 * scan, in this replica or any other, through the claim's own idempotency (resume / replay). The
 * request path also hands its fresh key to this process (`notify`) so an upload is admitted without
 * waiting for the next scan; that is only a latency hint, never the record.
 *
 * Dead letters: an object that fails `maxAttempts` times (default 20, the hosted queue's max retries)
 * gets a small marker at `<keyRoot>/.admission-dlq/<same path>.json` ({code, attempts, atMs}; never
 * content) and is skipped while the marker is younger than DEAD_LETTER_RETENTION_MS (14 days, the hosted
 * DLQ's message retention). After that the marker is expired and the object is attempted again (a
 * redrive re-runs every check, so a redrive after a reset still refuses a pre-reset upload). An
 * operator redrives at once by deleting the marker. The inbox bytes are NEVER expired or deleted by this
 * file: only a terminal outcome retires them (tombstone), exactly as hosted.
 *
 * Logs: one `ingest.admission` line per attempt (the hosted fields), one `ingest.admission_consumer`
 * line per scan (backlog, oldest age, dead letters, in flight). Never a key, body, token or header value.
 */
import { zstdDecompressSync } from "node:zlib";
import { log as sharedLog } from "../shared/log.ts";
import { objectKey, type ObjectStore } from "../shared/vendor.ts";
import { admitMessage, type AdmissionAction } from "./admission-consumer.ts";
import { INBOX_NAMESPACE, pendingAdmissions } from "./async-admission.ts";
import { lockAlertFields } from "./app.ts";
import type { DeliverDeps } from "./deliver.ts";

export const DEAD_LETTER_NAMESPACE = ".admission-dlq";
export const DEAD_LETTER_RETENTION_MS = 14 * 24 * 3600 * 1000;
const INBOX_SUFFIX = ".ndjson.zst";
const ATTEMPT = /\/a=(\d{15})-[0-9a-f]{16}\.ndjson\.zst$/;

/** The dead-letter marker for one inbox object: the same path under the DLQ namespace. */
export function deadLetterKey(keyRoot: string, inboxKey: string): string {
  const prefix = `${keyRoot}/${INBOX_NAMESPACE}/`;
  if (!inboxKey.startsWith(prefix) || !ATTEMPT.test(inboxKey)) throw new Error("admission_key_invalid");
  return `${keyRoot}/${DEAD_LETTER_NAMESPACE}/${inboxKey.slice(prefix.length, -INBOX_SUFFIX.length)}.json`;
}

function inboxKeyOfDeadLetter(keyRoot: string, deadKey: string): string | undefined {
  const prefix = `${keyRoot}/${DEAD_LETTER_NAMESPACE}/`;
  if (!deadKey.startsWith(prefix) || !deadKey.endsWith(".json")) return undefined;
  const key = `${keyRoot}/${INBOX_NAMESPACE}/${deadKey.slice(prefix.length, -".json".length)}${INBOX_SUFFIX}`;
  return ATTEMPT.test(key) ? key : undefined;
}

const receivedAtOf = (key: string) => Number(ATTEMPT.exec(key)?.[1] ?? NaN);

type Logger = Pick<typeof sharedLog, "info" | "warn" | "error">;

export interface SelfhostAdmissionOptions {
  /** "on" for SELFHOST_INGEST_ADMISSION=async, "drain" for drain: both admit; neither is "off". */
  mode: "on" | "drain";
  /** Canonical deliver deps (the same ledger, slots and target as the synchronous path). */
  deliver: DeliverDeps;
  concurrency: number;
  scanIntervalMs: number;
  maxAttempts: number;
  nowMs?: () => number;
  log?: Logger;
}

export interface SelfhostAdmissionStatus {
  /** starting: no scan has completed yet; ok: the last scan is recent; stale: it is not; stopped. */
  state: "starting" | "ok" | "stale" | "stopped";
  healthy: boolean;
  /** Inbox objects with content this process knows of and has not finished (dead letters excluded). */
  backlog: number;
  oldestAgeMs: number | null;
  /** Live (unexpired) dead-letter markers whose inbox object still holds content, as of the last scan. */
  deadLetters: number;
  inFlight: number;
  lastScanAgeMs: number | null;
}

export interface SelfhostAdmissionConsumer {
  start(): void;
  stop(): Promise<void>;
  /** The request path's latency hint for a fresh inbox key. Never throws. */
  notify(key: string): void;
  /** One ListBucket pass (tests and the interval). */
  scan(): Promise<void>;
  /** Start every due object (bounded), then wait until nothing is in flight. Tests. */
  settle(): Promise<void>;
  status(): SelfhostAdmissionStatus;
}

interface Entry { attempts: number; notBefore: number; addedAt: number; receivedAtMs: number }

export function selfhostAdmissionConsumer(opts: SelfhostAdmissionOptions): SelfhostAdmissionConsumer {
  const { store, keyRoot } = opts.deliver.target as { store: ObjectStore; keyRoot: string };
  const now = opts.nowMs ?? Date.now;
  const logger = opts.log ?? sharedLog;
  const decompress = (bytes: Uint8Array) => new Uint8Array(zstdDecompressSync(bytes));
  const entries = new Map<string, Entry>();
  const inflight = new Set<string>();
  const running = new Set<Promise<void>>();
  /** Inbox keys with a dead-letter marker seen by this process (to clear it after a later terminal outcome). */
  const marked = new Set<string>();
  let deadLetters = 0;
  let lastScanOkAt: number | undefined;
  let scanning: Promise<void> | undefined;
  let started = false, stopped = false;
  let interval: ReturnType<typeof setInterval> | undefined;
  let wake: ReturnType<typeof setTimeout> | undefined;
  const staleAfterMs = Math.max(3 * opts.scanIntervalMs, 60_000);

  async function listDeadLetters(at: number): Promise<Map<string, number>> {
    const out = new Map<string, number>();
    const prefix = `${keyRoot}/${DEAD_LETTER_NAMESPACE}`;
    let cursor: string | undefined;
    do {
      const page = await store.list(objectKey(prefix), cursor);
      for (const item of page.items) {
        const inbox = inboxKeyOfDeadLetter(keyRoot, String(item.key));
        if (inbox && at - item.lastModified.getTime() < DEAD_LETTER_RETENTION_MS) out.set(inbox, item.lastModified.getTime());
      }
      cursor = page.nextCursor;
    } while (cursor);
    return out;
  }

  async function scanOnce(): Promise<void> {
    const scanAt = now();
    const t0 = Date.now();
    try {
      const dead = await listDeadLetters(scanAt);
      // No age threshold (and so no dependence on this pod's clock agreeing with the store's): every pending object.
      const seen = new Set<string>();
      let live = 0;
      for await (const m of pendingAdmissions(store, keyRoot, { nowMs: scanAt, olderThanMs: Number.NEGATIVE_INFINITY })) {
        if (dead.has(m.key)) { live++; marked.add(m.key); if (!inflight.has(m.key)) entries.delete(m.key); continue; }
        seen.add(m.key);
        if (!entries.has(m.key)) entries.set(m.key, { attempts: 0, notBefore: 0, addedAt: scanAt, receivedAtMs: receivedAtOf(m.key) });
      }
      // Terminal (tombstoned) or gone: forget it, unless it is in flight or was notified during this scan.
      for (const [key, e] of entries) if (!seen.has(key) && !inflight.has(key) && e.addedAt < scanAt) entries.delete(key);
      deadLetters = live;
      lastScanOkAt = now();
      const s = status();
      logger.info("session ingest admission consumer", { route: "ingest.admission_consumer", outcome: "ok", admission_backlog: s.backlog,
        ...(s.oldestAgeMs !== null ? { admission_oldest_age_ms: s.oldestAgeMs } : {}), admission_dead_letters: s.deadLetters, admission_in_flight: s.inFlight, duration_ms: Date.now() - t0 });
    } catch (error) {
      const code = (error as { code?: unknown })?.code;
      logger.warn("session ingest admission consumer", { route: "ingest.admission_consumer", outcome: "scan_failed", error_code: typeof code === "string" && /^[A-Za-z0-9_]{1,64}$/.test(code) ? code : "scan_failed", duration_ms: Date.now() - t0 });
    }
  }

  function scan(): Promise<void> {
    scanning ??= scanOnce().finally(() => { scanning = undefined; });
    return scanning.then(() => pump());
  }

  async function deadLetter(key: string, code: string, attempts: number): Promise<boolean> {
    try {
      await store.put(objectKey(deadLetterKey(keyRoot, key)), new TextEncoder().encode(JSON.stringify({ v: 1, kind: "ingest-admission-dead-letter", code, attempts, atMs: now() })), { contentType: "application/json" });
      marked.add(key);
      return true;
    } catch {
      return false;
    }
  }

  async function admitOne(key: string, entry: Entry): Promise<void> {
    entry.attempts += 1;
    const t0 = Date.now();
    const r: AdmissionAction = await admitMessage({ mode: opts.mode, deliver: opts.deliver, custodySource: "relayhistory", decompress, nowMs: now }, { v: 1, kind: "ingest-admission", key }, entry.attempts)
      .catch((error: unknown): AdmissionAction => ({ action: "retry", delaySeconds: 300, code: "admission_failed", error }));
    if (r.action === "ack") {
      logger.info("session ingest admission", { route: "ingest.admission", outcome: r.outcome, error_code: r.code, duration_ms: Date.now() - t0, ...(r.lagMs !== undefined ? { offset: r.lagMs } : {}), attempt: entry.attempts });
      entries.delete(key);
      if (marked.delete(key)) await store.delete(objectKey(deadLetterKey(keyRoot, key))).catch(() => {});
      return;
    }
    const fields: Record<string, unknown> = { route: "ingest.admission", outcome: "retry", error_code: r.code, ...(r.stage ? { stage: r.stage } : {}), duration_ms: Date.now() - t0, attempt: entry.attempts };
    const db = (r.error as { db?: { db_error_class: string; db_error_code: string; db_error_basis: string } } | undefined)?.db;
    if (db) Object.assign(fields, { db_error_class: db.db_error_class, db_error_code: db.db_error_code, db_error_basis: db.db_error_basis });
    logger.warn("session ingest admission", fields);
    const lock = lockAlertFields(r.error, opts.deliver.acceptanceMode);
    if (lock) logger.warn("session ingest admission", { ...lock, route: "ingest.admission_lock_alert" });
    if (entry.attempts >= opts.maxAttempts) {
      if (await deadLetter(key, r.code, entry.attempts)) {
        logger.error("session ingest admission", { route: "ingest.admission", outcome: "dead_lettered", error_code: r.code, attempt: entry.attempts });
        entries.delete(key);
        return;
      }
      // The marker could not be written: keep the object, try again later (never drop it).
      entry.notBefore = now() + 300_000;
      return;
    }
    entry.notBefore = now() + r.delaySeconds * 1000;
  }

  function pump(): void {
    if (stopped) return;
    if (wake) { clearTimeout(wake); wake = undefined; }
    const at = now();
    const waiting = [...entries].filter(([key]) => !inflight.has(key)).sort((a, b) => a[1].receivedAtMs - b[1].receivedAtMs);
    let next = Infinity;
    for (const [key, entry] of waiting) {
      if (entry.notBefore > at) { next = Math.min(next, entry.notBefore); continue; }
      if (inflight.size >= opts.concurrency) break;
      inflight.add(key);
      const p: Promise<void> = admitOne(key, entry)
        .catch(() => { entry.notBefore = now() + 300_000; })
        .finally(() => { inflight.delete(key); running.delete(p); pump(); });
      running.add(p);
    }
    if (started && next !== Infinity) {
      wake = setTimeout(pump, Math.max(0, next - at));
      wake.unref?.();
    }
  }

  function status(): SelfhostAdmissionStatus {
    const at = now();
    let oldest = Infinity;
    for (const e of entries.values()) if (Number.isFinite(e.receivedAtMs)) oldest = Math.min(oldest, e.receivedAtMs);
    const state = stopped ? "stopped" : lastScanOkAt === undefined ? "starting" : at - lastScanOkAt > staleAfterMs ? "stale" : "ok";
    return {
      state,
      healthy: state === "ok",
      backlog: entries.size,
      oldestAgeMs: oldest === Infinity ? null : Math.max(0, at - oldest),
      deadLetters,
      inFlight: inflight.size,
      lastScanAgeMs: lastScanOkAt === undefined ? null : Math.max(0, at - lastScanOkAt),
    };
  }

  return {
    start() {
      if (started || stopped) return;
      started = true;
      void scan();
      interval = setInterval(() => void scan(), opts.scanIntervalMs);
      interval.unref?.();
    },
    async stop() {
      stopped = true;
      if (interval) clearInterval(interval);
      if (wake) clearTimeout(wake);
      await scanning?.catch(() => {});
      await Promise.allSettled([...running]);
    },
    notify(key) {
      try {
        if (stopped || entries.has(key) || !ATTEMPT.test(key) || !key.startsWith(`${keyRoot}/${INBOX_NAMESPACE}/`)) return;
        entries.set(key, { attempts: 0, notBefore: 0, addedAt: now(), receivedAtMs: receivedAtOf(key) });
        if (started) pump();
      } catch {
        // A hint only: the next scan finds the object anyway.
      }
    },
    scan,
    async settle() {
      pump();
      while (running.size > 0) await Promise.allSettled([...running]);
    },
    status,
  };
}
