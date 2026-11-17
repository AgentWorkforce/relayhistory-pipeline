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
 * Upload inbox (HISTORY_UPLOAD_DIRECT allowlist|on only, `uploadInbox`): after the admission pass, the same
 * scan lists `<keyRoot>/.upload-inbox/` and runs promoteUpload (upload-admission.ts) on at most `batchSize`
 * due objects, chosen fairly across uploading origins (oldest first) and run with bounded concurrency
 * (SELFHOST_UPLOAD_PROMOTE_CONCURRENCY), with per-object backoff (backoffSeconds) after a retry. A hand-off writes an
 * admission-inbox object and notifies this consumer, which admits it like any other. Off: the prefix is
 * never listed.
 *
 * Logs: one `ingest.admission` line per attempt (the hosted fields), one `ingest.admission_consumer`
 * line per scan (backlog, oldest age, dead letters, in flight). Never a key, body, token or header value.
 */
import type { UploadConsentConfig } from "./upload-consent.ts";
import { zstdDecompressSync } from "node:zlib";
import { ADMISSION_PHASE_FIELDS, log as sharedLog } from "../shared/log.ts";
import { objectKey, type ObjectStore } from "../shared/vendor.ts";
import { admitMessage, backoffSeconds, type AdmissionAction } from "./admission-consumer.ts";
import { INBOX_NAMESPACE, pendingAdmissions, type AdmissionTenantHint } from "./async-admission.ts";
import { lockAlertFields } from "./app.ts";
import type { LogStreams } from "./ledger-log-admit.ts";
import type { LogMaterializer } from "./ledger-log-materialize.ts";
import type { DeliverDeps } from "./deliver.ts";
import { uploadInboxRoot, type PromoteResult } from "./upload-admission.ts";
import { DeliveryBusyError } from "./vendor/delivery/delivery-contracts.ts";

/**
 * Ledger codes that mean "this org may not be admitted YET" (ledger-s3.ts requireFenceAuthority): no per-org or
 * deployment-wide fence-authority marker, or the deployment-wide stop. They are operator states, not failures of
 * the object: an org whose desktops already upload direct waits here for its marker (decision 0005 rollout step 4).
 * Such an attempt is a DEFERRAL: it does not count toward `maxAttempts` (so the object is never dead-lettered for
 * it) and the whole org waits AUTHORITY_WAIT_MS before its next start, so a large waiting backlog costs one claim
 * per org per window, not one per object. An invalid or mismatched marker is NOT here: it stays an ordinary retry.
 */
export const AWAIT_AUTHORITY_CODES: ReadonlySet<string> = new Set(["ledger_fence_authority_missing", "ledger_fence_authority_stopped"]);
export const AUTHORITY_WAIT_MS = 60_000;

export const DEAD_LETTER_NAMESPACE = ".admission-dlq";
export const DEAD_LETTER_RETENTION_MS = 14 * 24 * 3600 * 1000;
const INBOX_SUFFIX = ".ndjson.zst";
const ATTEMPT = /\/a=(\d{15})-[0-9a-f]{16}\.ndjson\.zst$/;
/** Most inbox keys whose writer's user this process remembers for the tenant-cap hint (oldest dropped first). */
export const USER_HINT_MAX = 100_000;

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
  /** HISTORY_UPLOAD_DIRECT allowlist|on: promote up to `batchSize` upload-inbox objects per scan. Absent: never listed. */
  uploadInbox?: { promote: (key: string) => Promise<PromoteResult>; batchSize: number; concurrency?: number };
  /** INGEST_UPLOAD_CONSENT (upload-consent.ts): re-checked at admission, at the current policy. */
  uploadConsent?: UploadConsentConfig;
  /**
   * INGEST_LEDGER=s3 (tenant-limiter.ts): a refusal by the per-replica tenant limit (429 `delivery_busy`, lock
   * `tenant_slots`) is a DEFERRAL: the object stays in the inbox, is retried after the limit's Retry-After, and the
   * attempt does not count toward `maxAttempts`, so a busy tenant is never dead-lettered. Absent (the SQL ledger):
   * a slot refusal is an ordinary retry, unchanged.
   */
  deferTenantLimit?: boolean;
  /**
   * How many inbox keys' writer users the tenant-cap hint remembers (default USER_HINT_MAX). 0 keeps none: every
   * entry falls back to the per-directory hint, as for a key found only by a scan.
   */
  userHintMax?: number;
  /** INGEST_LEDGER_LOG_COMMIT=on (ledger-log-admit.ts): streams in log mode are admitted through their log. */
  logStreams?: LogStreams;
  /** With logStreams: the log materializer (kicked after each append; every registered stream kicked each scan). */
  logMaterializer?: LogMaterializer;
}

export interface SelfhostAdmissionStatus {
  /**
   * starting: no scan has completed yet; ok: the last scan is recent, or the admission scan now running or the
   * pass the last one started is still making progress (a long listing or promotion pass is work, not a hang);
   * stale: neither; stopped.
   */
  state: "starting" | "ok" | "stale" | "stopped";
  healthy: boolean;
  /** Inbox objects with content this process knows of and has not finished (dead letters excluded). */
  backlog: number;
  oldestAgeMs: number | null;
  /** Live (unexpired) dead-letter markers whose inbox object still holds content, as of the last scan. */
  deadLetters: number;
  inFlight: number;
  lastScanAgeMs: number | null;
  /**
   * Age of the last progress: a listed page of the admission scan now running (while the previous admission
   * scan succeeded), or a listed page or finished promotion of the pass after a successful admission scan. Null
   * when neither runs. No progress for the stale window reads stale, so a real hang stays detectable.
   */
  lastProgressAgeMs: number | null;
}

export interface SelfhostAdmissionConsumer {
  start(): void;
  stop(): Promise<void>;
  /** The request path's latency hint for a fresh inbox key. Never throws. */
  notify(key: string, hint?: AdmissionTenantHint): void;
  /** One ListBucket pass (tests and the interval). */
  scan(): Promise<void>;
  /** Start every due object (bounded), then wait until nothing is in flight. Tests. */
  settle(): Promise<void>;
  status(): SelfhostAdmissionStatus;
  /** How many inbox keys currently carry a writer-user hint (bounded by userHintMax). Tests. */
  userHints(): number;
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
  // Tenant-cap HINT (deferTenantLimit only): skip, without starting, an entry whose org or user is already at the
  // consumer's tenant cap, or whose user was just deferred by it. The authoritative guard is still deliver.ts's
  // post-claim acquire (a race still defers safely); this only stops a deferred attempt from spending a consumer
  // slot and its full S3 claim first. The per-user GROUP is the user the key was written for when this process
  // wrote it (notify's hint: the request path and upload promotion both pass it), so one user's uploads from many
  // machines count together, as the limiter counts them. A key found only by a scan (written before a restart, or
  // by another replica) falls back to its machine directory (`…/m=<hash of ws, origin, user>`), which belongs to
  // exactly one user: its count never exceeds that user's, so the fallback can only start more than the cap
  // allows (which the limiter then defers, as before), never skip a start the limiter would have allowed. The org
  // count is exact: the consumer's limiter pool is its own (selfhost-server.ts asyncLimiter).
  const caps = opts.deferTenantLimit ? opts.deliver.slots : undefined;
  const inboxPrefix = `${keyRoot}/${INBOX_NAMESPACE}/`;
  /** Inbox key -> `<org>\n<userId>` from notify's hint (bounded; forgotten with the entry). In-process only. */
  const userOf = new Map<string, string>();
  // 0: no hints are kept, so every entry uses the per-directory fallback (exactly a scan-only process).
  const userHintMax = Math.max(0, Math.floor(opts.userHintMax ?? USER_HINT_MAX));
  const tenantOf = (key: string): { org: string; group: string } | undefined => {
    if (!key.startsWith(inboxPrefix)) return undefined;
    const r = key.indexOf("/r=", inboxPrefix.length), slash = key.indexOf("/", inboxPrefix.length);
    if (r < 0 || slash < 0 || slash > r) return undefined;
    const org = key.slice(inboxPrefix.length, slash);
    const user = userOf.get(key);
    return { org, group: user !== undefined ? `u:${user}` : `d:${key.slice(0, r)}` };
  };
  /** Drop an entry and its user hint (a terminal outcome, a dead letter, or gone from the inbox). */
  const forget = (key: string) => { entries.delete(key); userOf.delete(key); };
  const inflightOrg = new Map<string, number>(), inflightGroup = new Map<string, number>();
  /** User group -> no start before (ms): set by a tenant-limit deferral, cleared when one of its admissions ends. */
  const busyUntil = new Map<string, number>();
  /** Org -> no start before (ms): set when a claim answered an AWAIT_AUTHORITY_CODES code (no marker yet). */
  const authorityUntil = new Map<string, number>();
  const orgOf = (key: string): string | undefined => {
    if (!key.startsWith(inboxPrefix)) return undefined;
    const slash = key.indexOf("/", inboxPrefix.length);
    return slash > inboxPrefix.length ? key.slice(inboxPrefix.length, slash) : undefined;
  };
  const bump = (m: Map<string, number>, k: string, d: number) => { const n = (m.get(k) ?? 0) + d; if (n > 0) m.set(k, n); else m.delete(k); };
  /** Inbox keys with a dead-letter marker seen by this process (to clear it after a later terminal outcome). */
  const marked = new Set<string>();
  let deadLetters = 0;
  let lastScanOkAt: number | undefined;
  /** Last progress of the running pass whose admission scan succeeded (undefined when no such pass runs). */
  let lastProgressAt: number | undefined;
  const progress = () => { if (lastProgressAt !== undefined) lastProgressAt = now(); };
  /**
   * Last listed page of the admission scan now running (undefined when none runs). With a large inbox the
   * scan's own listing outlasts the stale window, so a page is progress too, but only while the previous
   * admission scan succeeded: after a failed scan the next one's pages do not count until one succeeds, so a
   * scan that keeps failing goes stale and stays stale.
   */
  let scanPageAt: number | undefined;
  let lastScanFailed = false;
  /**
   * Startup: before the FIRST successful admission scan, a large inbox makes that first scan outlast the startup
   * probe (16k+ objects, 60-90 s), so a pod that is listing pages would be killed before it is ever ready. Until
   * the first success, the running scan's pages count (a failed first scan withdraws them and the retry starts
   * fresh), and lastScanAgeMs reports the age of that running first scan. After the first success, unchanged.
   */
  let firstScanStartedAt: number | undefined;
  const scanPage = () => { if (!lastScanFailed || lastScanOkAt === undefined) scanPageAt = now(); };
  let scanning: Promise<void> | undefined;
  let started = false, stopped = false;
  let interval: ReturnType<typeof setInterval> | undefined;
  let wake: ReturnType<typeof setTimeout> | undefined;
  const staleAfterMs = Math.max(3 * opts.scanIntervalMs, 60_000);
  /** Upload keys that returned `retry` (or `invalid`): attempts and the earliest next attempt. */
  const uploadBackoff = new Map<string, { attempts: number; notBefore: number }>();

  async function promoteUploads(at: number): Promise<void> {
    const upload = opts.uploadInbox;
    if (!upload) return;
    const t0 = Date.now();
    const counts: Record<string, number> = {};
    const seen = new Set<string>();
    const due: UploadCandidate[] = [];
    let cursor: string | undefined;
    const root = uploadInboxRoot(keyRoot);
    try {
      // List the whole prefix first, so the choice below sees every due object, not only the first pages.
      do {
        // objectKey refuses a trailing slash: list the bare prefix and keep only slash-bounded keys
        // (as pendingAdmissions does), so a sibling `.upload-inbox-x/` is never promoted.
        const page = await store.list(objectKey(root.slice(0, -1)), cursor);
        progress();
        for (const item of page.items) {
          const key = String(item.key);
          if (!key.startsWith(root)) continue;
          seen.add(key);
          const back = uploadBackoff.get(key);
          if (back && back.notBefore > at) continue;
          due.push({ key, lastModifiedMs: item.lastModified.getTime(), origin: uploadOriginOf(key) });
        }
        cursor = page.nextCursor;
      } while (cursor && !stopped);
      for (const key of uploadBackoff.keys()) if (!seen.has(key)) uploadBackoff.delete(key);
      const chosen = selectUploadsFairly(due, upload.batchSize);
      const promoteOne = async (key: string): Promise<void> => {
        const back = uploadBackoff.get(key);
        const r = await upload.promote(key).catch((error: unknown): PromoteResult => ({ action: "retry", code: "promote_failed", error }));
        progress();
        const label = r.action === "ack" ? (r.code ?? r.outcome) : r.code;
        counts[label] = (counts[label] ?? 0) + 1;
        if (r.action === "ack") { uploadBackoff.delete(key); return; }
        const attempts = (back?.attempts ?? 0) + 1;
        // invalid: not an upload key of this root; it is never promoted, so wait the longest.
        uploadBackoff.set(key, { attempts, notBefore: now() + (r.action === "invalid" ? 300 : backoffSeconds(attempts)) * 1000 });
      };
      // Bounded concurrency: `concurrency` workers share one cursor over the chosen keys.
      let next = 0, promoted = 0;
      const workerCount = Math.min(Math.max(1, Math.floor(upload.concurrency ?? 1)), chosen.length);
      await Promise.all(Array.from({ length: workerCount }, async () => {
        // `promoted` counts only attempts actually started: stop() can cut a pass short.
        while (!stopped && next < chosen.length) { promoted++; await promoteOne(chosen[next++]!); }
      }));
      if (promoted > 0) logger.info("session ingest upload admission", { route: "ingest.upload_admission", outcome: "ok", promoted, waiting: uploadBackoff.size,
        upload_due: due.length, upload_origins: new Set(due.map((c) => c.origin)).size, upload_results: uploadResultsField(counts), duration_ms: Date.now() - t0, ...counts });
    } catch (error) {
      const code = (error as { code?: unknown })?.code;
      logger.warn("session ingest upload admission", { route: "ingest.upload_admission", outcome: "scan_failed", error_code: typeof code === "string" && /^[A-Za-z0-9_]{1,64}$/.test(code) ? code : "scan_failed", duration_ms: Date.now() - t0 });
    }
  }

  async function listDeadLetters(at: number): Promise<Map<string, number>> {
    const out = new Map<string, number>();
    const prefix = `${keyRoot}/${DEAD_LETTER_NAMESPACE}`;
    let cursor: string | undefined;
    do {
      const page = await store.list(objectKey(prefix), cursor);
      scanPage();
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
    if (lastScanOkAt === undefined) firstScanStartedAt = scanAt;
    try {
      const dead = await listDeadLetters(scanAt);
      // No age threshold (and so no dependence on this pod's clock agreeing with the store's): every pending object.
      const seen = new Set<string>();
      let live = 0;
      for await (const m of pendingAdmissions(store, keyRoot, { nowMs: scanAt, olderThanMs: Number.NEGATIVE_INFINITY, onPage: scanPage })) {
        if (dead.has(m.key)) { live++; marked.add(m.key); if (!inflight.has(m.key)) forget(m.key); continue; }
        seen.add(m.key);
        if (!entries.has(m.key)) entries.set(m.key, { attempts: 0, notBefore: 0, addedAt: scanAt, receivedAtMs: receivedAtOf(m.key) });
      }
      // Terminal (tombstoned) or gone: forget it, unless it is in flight or was notified during this scan.
      for (const [key, e] of entries) if (!seen.has(key) && !inflight.has(key) && e.addedAt < scanAt) forget(key);
      deadLetters = live;
      lastScanOkAt = now();
      lastScanFailed = false;
      scanPageAt = undefined;
      // The promotion pass below runs inside this scan (the next cannot start until it ends): its progress keeps
      // the consumer healthy. Only after a successful admission scan, so failing scans still go stale.
      lastProgressAt = lastScanOkAt;
      const s = status();
      logger.info("session ingest admission consumer", { route: "ingest.admission_consumer", outcome: "ok", admission_backlog: s.backlog,
        ...(s.oldestAgeMs !== null ? { admission_oldest_age_ms: s.oldestAgeMs } : {}), admission_dead_letters: s.deadLetters, admission_in_flight: s.inFlight, duration_ms: Date.now() - t0 });
    } catch (error) {
      // A failed scan never counts: withdraw its pages, and do not count the next scan's until one succeeds.
      lastScanFailed = true;
      scanPageAt = undefined;
      const code = (error as { code?: unknown })?.code;
      logger.warn("session ingest admission consumer", { route: "ingest.admission_consumer", outcome: "scan_failed", error_code: typeof code === "string" && /^[A-Za-z0-9_]{1,64}$/.test(code) ? code : "scan_failed", duration_ms: Date.now() - t0 });
    }
    try {
      await promoteUploads(scanAt);
    } finally {
      lastProgressAt = undefined;
      // Log mode: sweep the registered streams even when promotion failed (the sweep returns once the registry is
      // listed; the materializing runs in the background; a failure retries next scan).
      if (opts.logMaterializer) await opts.logMaterializer.sweep().catch(() => {});
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

  async function admitOne(key: string, entry: Entry): Promise<"deferred" | void> {
    entry.attempts += 1;
    const t0 = Date.now();
    const r: AdmissionAction = await admitMessage({ mode: opts.mode, deliver: opts.deliver, custodySource: "relayhistory", decompress, nowMs: now, ...(opts.uploadConsent ? { uploadConsent: opts.uploadConsent, log: (fields: Record<string, unknown>) => logger.warn("session ingest admission", fields) } : {}),
      ...(opts.logStreams ? { logStreams: opts.logStreams } : {}), ...(opts.logMaterializer ? { logMaterializer: opts.logMaterializer } : {}) }, { v: 1, kind: "ingest-admission", key }, entry.attempts)
      .catch((error: unknown): AdmissionAction => ({ action: "retry", delaySeconds: 300, code: "admission_failed", error }));
    if (r.action === "ack") {
      const timing: Record<string, number> = {};
      for (const [phase, ms] of Object.entries(r.timingMs ?? {})) { const field = ADMISSION_PHASE_FIELDS.get(phase); if (field) timing[field] = ms; }
      logger.info("session ingest admission", { route: "ingest.admission", outcome: r.outcome, error_code: r.code, duration_ms: Date.now() - t0, ...(r.lagMs !== undefined ? { offset: r.lagMs } : {}), attempt: entry.attempts, ...timing });
      forget(key);
      if (marked.delete(key)) await store.delete(objectKey(deadLetterKey(keyRoot, key))).catch(() => {});
      return;
    }
    if (opts.deferTenantLimit && r.error instanceof DeliveryBusyError && r.error.lock === "tenant_slots") {
      // At the tenant cap: wait for a slot. Nothing was written past the pending claim, which the retry resumes.
      entry.attempts -= 1;
      entry.notBefore = now() + r.delaySeconds * 1000;
      // Its siblings in the same machine directory (the same user) wait as long, unless one of them ends first.
      const tenant = caps ? tenantOf(key) : undefined;
      if (tenant) busyUntil.set(tenant.group, Math.max(busyUntil.get(tenant.group) ?? 0, entry.notBefore));
      logger.info("session ingest admission", { route: "ingest.admission", outcome: "deferred", error_code: "tenant_limit", duration_ms: Date.now() - t0, attempt: entry.attempts + 1 });
      return "deferred";
    }
    // Matched on the retry's code: the claim's LedgerUnavailableError reaches here wrapped by the delivery layer,
    // which keeps its code (the same field the retry log line reports).
    if (AWAIT_AUTHORITY_CODES.has(r.code)) {
      // The org has no fence-authority marker yet (or the deployment-wide stop is up): wait for the operator, never
      // dead-letter. The bytes stay in the inbox; the org's other entries wait the same window (pump skips them).
      entry.attempts -= 1;
      entry.notBefore = now() + AUTHORITY_WAIT_MS;
      const org = orgOf(key);
      if (org !== undefined) authorityUntil.set(org, Math.max(authorityUntil.get(org) ?? 0, entry.notBefore));
      logger.info("session ingest admission", { route: "ingest.admission", outcome: "deferred", error_code: r.code, duration_ms: Date.now() - t0, attempt: entry.attempts + 1 });
      return "deferred";
    }
    const storeStatus = (r.error as { storeStatus?: unknown } | undefined)?.storeStatus;
    const storeCause = (r.error as { storeCause?: unknown } | undefined)?.storeCause;
    const fields: Record<string, unknown> = { route: "ingest.admission", outcome: "retry", error_code: r.code, ...(r.stage ? { stage: r.stage } : {}), ...(typeof storeStatus === "number" ? { store_status: storeStatus } : {}), ...(typeof storeCause === "string" ? { store_cause: storeCause } : {}), duration_ms: Date.now() - t0, attempt: entry.attempts };
    const db = (r.error as { db?: { db_error_class: string; db_error_code: string; db_error_basis: string } } | undefined)?.db;
    if (db) Object.assign(fields, { db_error_class: db.db_error_class, db_error_code: db.db_error_code, db_error_basis: db.db_error_basis });
    logger.warn("session ingest admission", fields);
    const lock = lockAlertFields(r.error, opts.deliver.acceptanceMode);
    if (lock) logger.warn("session ingest admission", { ...lock, route: "ingest.admission_lock_alert" });
    if (entry.attempts >= opts.maxAttempts) {
      if (await deadLetter(key, r.code, entry.attempts)) {
        logger.error("session ingest admission", { route: "ingest.admission", outcome: "dead_lettered", error_code: r.code, attempt: entry.attempts });
        forget(key);
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
    // Sweep expired deferral memos (a directory whose deferred entry left another way would otherwise linger).
    for (const [dir, until] of busyUntil) if (until <= at) busyUntil.delete(dir);
    for (const [org, until] of authorityUntil) if (until <= at) authorityUntil.delete(org);
    const waiting = [...entries].filter(([key]) => !inflight.has(key)).sort((a, b) => a[1].receivedAtMs - b[1].receivedAtMs);
    let next = Infinity;
    for (const [key, entry] of waiting) {
      if (entry.notBefore > at) { next = Math.min(next, entry.notBefore); continue; }
      if (inflight.size >= opts.concurrency) break;
      // Its org is waiting for a fence-authority marker: skip without starting until the window ends.
      const waitOrg = authorityUntil.size ? orgOf(key) : undefined;
      const authorityWait = waitOrg !== undefined ? authorityUntil.get(waitOrg) : undefined;
      if (authorityWait !== undefined && authorityWait > at) { next = Math.min(next, authorityWait); continue; }
      const tenant = caps ? tenantOf(key) : undefined;
      if (tenant && caps) {
        // At (or just deferred by) the tenant cap: leave it due and go on to other tenants. An ending admission
        // re-pumps (finally), and a deferral's window is woken below.
        const busy = busyUntil.get(tenant.group);
        if (busy !== undefined && busy > at) { next = Math.min(next, busy); continue; }
        if (busy !== undefined) busyUntil.delete(tenant.group);
        // Only a positive cap is a hint (a 0 cap is left to the limiter, as before, so this never parks an entry
        // with nothing in flight to re-pump it). The counts can only be stricter than the limiter's (a lease past its
        // TTL frees the limiter slot before the admission ends here), so the hint never over-admits.
        if ((caps.maxPerOrg >= 1 && (inflightOrg.get(tenant.org) ?? 0) >= caps.maxPerOrg)
          || (caps.maxPerUser >= 1 && (inflightGroup.get(tenant.group) ?? 0) >= caps.maxPerUser)) continue;
        bump(inflightOrg, tenant.org, 1); bump(inflightGroup, tenant.group, 1);
      }
      inflight.add(key);
      let deferred = false;
      const p: Promise<void> = admitOne(key, entry)
        .then((outcome) => { deferred = outcome === "deferred"; })
        .catch(() => { entry.notBefore = now() + 300_000; })
        .finally(() => {
          inflight.delete(key);
          if (tenant) {
            bump(inflightOrg, tenant.org, -1); bump(inflightGroup, tenant.group, -1);
            // A finished (non-deferred) admission freed this user's slot: its siblings may try again now.
            if (!deferred) busyUntil.delete(tenant.group);
          }
          running.delete(p); pump();
        });
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
    const freshest = Math.max(lastScanOkAt ?? -Infinity, lastProgressAt ?? -Infinity, scanPageAt ?? -Infinity);
    // Before the first success: ok only while the running first scan lists pages (startup grace); else starting.
    const state = stopped ? "stopped"
      : lastScanOkAt === undefined ? (scanPageAt !== undefined && at - scanPageAt <= staleAfterMs ? "ok" : "starting")
      : at - freshest > staleAfterMs ? "stale" : "ok";
    return {
      state,
      healthy: state === "ok",
      backlog: entries.size,
      oldestAgeMs: oldest === Infinity ? null : Math.max(0, at - oldest),
      deadLetters,
      inFlight: inflight.size,
      lastScanAgeMs: lastScanOkAt !== undefined ? Math.max(0, at - lastScanOkAt)
        : state === "ok" && firstScanStartedAt !== undefined ? Math.max(0, at - firstScanStartedAt) : null,
      lastProgressAgeMs: lastProgressAt === undefined && scanPageAt === undefined ? null
        : Math.max(0, at - Math.max(lastProgressAt ?? -Infinity, scanPageAt ?? -Infinity)),
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
    notify(key, hint) {
      try {
        if (stopped || !ATTEMPT.test(key) || !key.startsWith(`${keyRoot}/${INBOX_NAMESPACE}/`)) return;
        // The writer's tenant, for the per-user cap hint: only for the key's own org, and only while the
        // entry is not in flight (a running admission keeps the group it started with). Bounded: oldest first out.
        if (caps && userHintMax > 0 && hint && typeof hint.orgId === "string" && typeof hint.userId === "string" && hint.userId !== ""
          && key.startsWith(`${inboxPrefix}${hint.orgId}/`) && !inflight.has(key) && !userOf.has(key)) {
          if (userOf.size >= userHintMax) { const oldest = userOf.keys().next().value; if (oldest !== undefined) userOf.delete(oldest); }
          userOf.set(key, `${hint.orgId}\n${hint.userId}`);
        }
        if (entries.has(key)) return;
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
      await opts.logMaterializer?.settle();
    },
    status,
    userHints: () => userOf.size,
  };
}

/** A due upload-inbox object, as the scan's listing saw it. */
export interface UploadCandidate { key: string; lastModifiedMs: number; origin: string }

/** The `o=<originId>` segment of an upload key (the uploading machine), or "" when the key has none. */
export function uploadOriginOf(key: string): string {
  const m = /\/o=([^/]+)\//u.exec(key);
  return m ? m[1]! : "";
}

/**
 * Up to `limit` candidates, fair across origins and oldest first: each origin's objects in LastModified
 * order (key order on a tie), then one from each origin in turn (origins ordered by their oldest object),
 * so one origin with thousands of objects cannot starve the others.
 */
export function selectUploadsFairly(candidates: readonly UploadCandidate[], limit: number): string[] {
  const byOrigin = new Map<string, UploadCandidate[]>();
  for (const c of candidates) {
    const list = byOrigin.get(c.origin);
    if (list) list.push(c); else byOrigin.set(c.origin, [c]);
  }
  const order = (a: UploadCandidate, b: UploadCandidate) => a.lastModifiedMs - b.lastModifiedMs || (a.key < b.key ? -1 : a.key > b.key ? 1 : 0);
  const queues = [...byOrigin.values()].map((q) => q.sort(order)).sort((a, b) => order(a[0]!, b[0]!));
  const out: string[] = [];
  for (let round = 0; out.length < limit; round++) {
    let took = false;
    for (const q of queues) {
      if (out.length >= limit) break;
      const c = q[round];
      if (c) { out.push(c.key); took = true; }
    }
    if (!took) break;
  }
  return out;
}

/** Per-result counts as one `code:n,...` string. Codes are fixed identifiers (never keys or ids); anything else is dropped. */
export function uploadResultsField(counts: Record<string, number>): string {
  return Object.entries(counts).filter(([k]) => /^[A-Za-z0-9_]{1,64}$/u.test(k)).sort(([a], [b]) => (a < b ? -1 : 1)).map(([k, n]) => `${k}:${n}`).join(",");
}
