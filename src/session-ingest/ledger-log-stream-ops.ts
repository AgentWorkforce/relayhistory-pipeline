/**
 * Operator steps for one stream (docs/design/s3-ledger-log-commit.md): status, switch to log mode, close (rollback).
 * Every mutating step is a dry run unless `apply` is set; the CLI is scripts/ledger-log-stream.ts.
 *
 *   status   legacy | log | closed, the log head, the materialized cursor, legacy batches still pending.
 *   switch   refuses unless the bucket is declared non-production (logModeNonProdKey, an allowlist checked here, not
 *            only in the CLI) or the step carries a verified production context (below), and the deployment-wide
 *            proof exists (LOG_COMMIT_PROOF_KEY: every replica runs with INGEST_LEDGER_LOG_COMMIT=on, so every legacy
 *            claim and commit honours the marker); refuses while any legacy batch of the stream is pending (drain it
 *            first) or the stream is already in log mode. Right before entry 0 it re-lists the stream's legacy batch
 *            docs (reading only new or changed ones) and refuses if one is pending; it then writes entry 0 with
 *            legacyCutover = "committed=<n legacy batches committed>", waits past the longest legacy claim (settleMs)
 *            and re-lists: a legacy batch doc that is new or changed since that last listing, or pending, and is NOT a
 *            batch of the log (the materializer writes the batch docs of logged batches through the legacy core) is
 *            a legacy writer that raced entry 0: `legacy_raced_switch` (entry 0 stays: close the stream after the log
 *            drains, then investigate). Registers the stream last.
 *   close    refuses until every entry is applied (cursor = head); writes the close marker, after which the stream
 *            reads as legacy again (the admission consumer and the logModeGuard both read it). One way: a closed
 *            stream is not re-opened. A writer that had the stream cached may still append an entry it had in
 *            flight; the materializer keeps applying the log, so keep it running until the cursor equals the head
 *            again.
 *   preflight / production   the deliberate production path (docs/design/s3-ledger-log-commit.md, "Production
 *            pilot"; contract in ledger-log-stream-production.ts). With `production` set, switch and close run the
 *            production preflight (stream known, fence authority and user fence sane, the deployment-wide proof, no
 *            legacy batch in flight; close: every entry applied) and, on apply, rebuild the typed confirmation from
 *            that fresh preflight and compare it byte for byte. That replaces the non-production marker check; nothing
 *            else is relaxed. A production close waits `settleMs` after the marker and reports the stream again, so
 *            an in-flight append by a writer that had the stream cached is visible in the receipt.
 *   measure  READ ONLY: a stream's (or every origin of a user's) batch arrival rate over a window, from the legacy
 *            batch docs' lastModified and the log entries' atMs. Lists only; GETs only log entries inside the window.
 */
import { createHash } from "node:crypto";
import { isObjectStoreError, objectKey, readAll, type ObjectStore } from "../shared/vendor.ts";
import type { LedgerTenant } from "./ledger.ts";
import { enableLogMode, LogCorrupt, logKeys, openStream, readEntries, type LogEntry } from "./ledger-log.ts";
import { logStreamRegistryKey, materializedThrough, registerLogStream } from "./ledger-log-materialize.ts";
import {
  fenceAuthorityKey, fenceAuthorityStopKey, hasPendingReset, orgFenceAuthorityKey, s3LedgerKeys, validFenceAuthority,
  validOrgFenceAuthority, type FenceDoc,
} from "./ledger-s3.ts";
import {
  bucketBindingSha256, confirmationExpiry, confirmationRefusal, LEDGER_LOG_PRODUCTION_PLAN_TTL_MS, LOG_STREAM_VISIBLE_BATCHES_PER_S,
  preflightStateSha256, productionConfirmation, productionContextRefusal, streamIdentitySha256, type ProductionAction,
  type ProductionContext,
} from "./ledger-log-stream-production.ts";

export interface StreamStatus {
  mode: "legacy" | "log" | "closed";
  head: number | null;
  materializedThrough: number;
  legacyPending: number;
  legacyCommitted: number;
}

async function exists(store: ObjectStore, key: string) { return (await store.head(objectKey(key))) !== undefined; }

/** One legacy batch doc as last seen: its listing fingerprint and its state. */
interface LegacyDoc { seen: string; state: "pending" | "committed" | "other" }

/**
 * The stream's legacy batch docs, by key. With `prev`, only docs that are new or changed since `prev` (by their listing
 * fingerprint: eTag and lastModified) are read; the rest keep their known state. A doc deleted since is dropped.
 */
async function legacyScan(store: ObjectStore, keyRoot: string, tenant: LedgerTenant, originId: string,
  prev?: Map<string, LegacyDoc>): Promise<Map<string, LegacyDoc>> {
  const one = s3LedgerKeys(keyRoot, tenant).batch(originId, "x");
  const prefix = one.slice(0, one.lastIndexOf("/") + 1);
  const out = new Map<string, LegacyDoc>();
  let cursor: string | undefined;
  do {
    const page = await store.list(objectKey(prefix.slice(0, -1)), cursor);
    for (const item of page.items) {
      const key = String(item.key);
      if (!key.startsWith(prefix)) continue;
      const seen = `${item.version.eTag ?? item.version.version ?? ""}|${item.lastModified.getTime()}`;
      const known = prev?.get(key);
      if (known && known.seen === seen) { out.set(key, known); continue; }
      try {
        const doc = JSON.parse(new TextDecoder().decode(await readAll((await store.getObject(objectKey(key))).body))) as { state?: unknown };
        out.set(key, { seen, state: doc.state === "pending" ? "pending" : doc.state === "committed" ? "committed" : "other" });
      } catch (error) { if (!(isObjectStoreError(error) && error.code === "not_found")) throw error; }
    }
    cursor = page.nextCursor;
  } while (cursor);
  return out;
}
const count = (docs: Map<string, LegacyDoc>, state: LegacyDoc["state"]) => [...docs.values()].filter((d) => d.state === state).length;

/** The status and the legacy docs it counted (the switch re-lists against them instead of reading every doc again). */
async function statusAndDocs(store: ObjectStore, keyRoot: string, tenant: LedgerTenant, originId: string, prev?: Map<string, LegacyDoc>) {
  const keys = logKeys(keyRoot, tenant, originId);
  const [state, closed, through, docs] = await Promise.all([openStream(store, keys), exists(store, keys.closed), materializedThrough(store, keys),
    legacyScan(store, keyRoot, tenant, originId, prev)]);
  const status: StreamStatus = { mode: closed ? "closed" : state ? "log" : "legacy", head: state ? state.head : null, materializedThrough: through,
    legacyPending: count(docs, "pending"), legacyCommitted: count(docs, "committed") };
  return { status, docs };
}

export async function streamStatus(store: ObjectStore, keyRoot: string, tenant: LedgerTenant, originId: string): Promise<StreamStatus> {
  return (await statusAndDocs(store, keyRoot, tenant, originId)).status;
}

export interface StepResult {
  applied: boolean;
  action: string;
  refused?: string;
  status: StreamStatus;
  /** Production only: the preflight the step ran (counts and verdicts, no ids). */
  preflight?: PreflightReport;
  /** Production only: the digests the step is bound to (a plan also carries its confirmation). */
  production?: ProductionReceipt;
  /** A production close only: the stream after the settle wait (a cached writer's in-flight append shows here). */
  after?: StreamStatus;
}

/**
 * The deployment-wide proof the switch requires: written by the operator only once every replica of the deployment
 * runs with INGEST_LEDGER_LOG_COMMIT=on (nothing in the service writes it). Without it a replica still on `off`
 * would claim and commit a log-mode stream behind its log.
 */
export const logCommitProofKey = (keyRoot: string) => `${keyRoot}/.ledger/log-commit-on.json`;
/**
 * The allowlist the switch requires, inside the operation itself (not only in the CLI): an operator-written marker
 * declaring THIS bucket and key root a non-production test target. Nothing in the service writes it. Log mode is not
 * cleared for production (docs/design/s3-ledger-log-commit.md, Remaining), so a production bucket never carries it;
 * a production step instead carries a production context whose typed confirmation the step verifies itself.
 */
export const logModeNonProdKey = (keyRoot: string) => `${keyRoot}/.ledger/log-mode-nonprod.json`;
/** Longer than a legacy claim lives (claim -> commit, with its retries): the switch's settle wait. */
export const SWITCH_SETTLE_MS = 5 * 60_000;

/** The legacy batch-doc keys of every batch in the stream's log: the materializer writes those through the legacy core. */
async function logBatchDocKeys(store: ObjectStore, keyRoot: string, tenant: LedgerTenant, originId: string): Promise<Set<string>> {
  const keys = new Set<string>(), batchKey = s3LedgerKeys(keyRoot, tenant).batch;
  for await (const e of readEntries(store, logKeys(keyRoot, tenant, originId)))
    if (e.kind === "batches") for (const b of e.batches) keys.add(batchKey(originId, b.batchId));
  return keys;
}

export async function switchToLogMode(store: ObjectStore, keyRoot: string, tenant: LedgerTenant, originId: string,
  opts: { apply: boolean; writer: string; settleMs?: number; sleep?: (ms: number) => Promise<void>; production?: ProductionContext }): Promise<StepResult> {
  const { status, docs } = await statusAndDocs(store, keyRoot, tenant, originId);
  if (status.mode !== "legacy") return { applied: false, action: "switch", refused: `stream_is_${status.mode}`, status };
  let gate: ProductionGate | undefined;
  if (opts.production) {
    // Replaces the non-production marker check (and re-runs the proof and drain checks inside the preflight).
    gate = await productionGate(store, keyRoot, tenant, originId, "switch", opts.production, opts.apply);
    if (gate.refused) return { applied: false, action: "switch", refused: gate.refused, status, ...gate.out };
  } else if (!(await exists(store, logModeNonProdKey(keyRoot)))) return { applied: false, action: "switch", refused: "bucket_not_declared_nonprod", status };
  if (!(await exists(store, logCommitProofKey(keyRoot)))) return { applied: false, action: "switch", refused: "log_commit_not_proven_on", status, ...gate?.out };
  if (status.legacyPending > 0) return { applied: false, action: "switch", refused: "legacy_batches_pending", status, ...gate?.out };
  if (!opts.apply) return { applied: false, action: "switch (dry run)", status, ...gate?.out };
  // The last look before entry 0, as close to it as a listing allows: only new or changed docs are read.
  const before = await legacyScan(store, keyRoot, tenant, originId, docs);
  if (count(before, "pending") > 0)
    return { applied: false, action: "switch", refused: "legacy_batches_pending", status: { ...status, legacyPending: count(before, "pending"),
      legacyCommitted: count(before, "committed") }, ...gate?.out };
  await enableLogMode(store, logKeys(keyRoot, tenant, originId), `committed=${count(before, "committed")}`, { writer: opts.writer });
  await (opts.sleep ?? sleep)(opts.settleMs ?? SWITCH_SETTLE_MS);
  // Batch docs the materializer wrote for logged batches are expected; any other new, changed or pending doc raced.
  const logged = await logBatchDocKeys(store, keyRoot, tenant, originId);
  const afterDocs = await legacyScan(store, keyRoot, tenant, originId, before);
  const raced = [...afterDocs].some(([k, d]) => !logged.has(k) && (d.state === "pending" || before.get(k)?.seen !== d.seen));
  if (raced) return { applied: true, action: "switch", refused: "legacy_raced_switch", status: (await statusAndDocs(store, keyRoot, tenant, originId, afterDocs)).status, ...gate?.out };
  await registerLogStream(store, keyRoot, tenant, originId);
  return { applied: true, action: "switch", status: (await statusAndDocs(store, keyRoot, tenant, originId, afterDocs)).status, ...gate?.out };
}

export async function closeLogStream(store: ObjectStore, keyRoot: string, tenant: LedgerTenant, originId: string,
  opts: { apply: boolean; production?: ProductionContext; settleMs?: number; sleep?: (ms: number) => Promise<void> }): Promise<StepResult> {
  const status = await streamStatus(store, keyRoot, tenant, originId);
  if (status.mode !== "log") return { applied: false, action: "close", refused: `stream_is_${status.mode}`, status };
  let gate: ProductionGate | undefined;
  if (opts.production) {
    gate = await productionGate(store, keyRoot, tenant, originId, "close", opts.production, opts.apply);
    if (gate.refused) return { applied: false, action: "close", refused: gate.refused, status, ...gate.out };
  }
  if (status.materializedThrough !== status.head) return { applied: false, action: "close", refused: "entries_not_applied", status, ...gate?.out };
  if (!opts.apply) return { applied: false, action: "close (dry run)", status, ...gate?.out };
  const keys = logKeys(keyRoot, tenant, originId);
  try {
    await store.put(objectKey(keys.closed), new TextEncoder().encode(JSON.stringify({ v: 1, closedAtHead: status.head })), { ifNoneMatch: true, contentType: "application/json" });
  } catch (error) { if (!(isObjectStoreError(error) && error.code === "already_exists")) throw error; }
  const closed = await streamStatus(store, keyRoot, tenant, originId);
  if (!opts.production) return { applied: true, action: "close", status: closed };
  // Production: a writer that had the stream cached may still append the entry it had in flight; the materializer
  // applies it. Report the stream again after the settle wait, so the receipt shows whether the head moved.
  await (opts.sleep ?? sleep)(opts.settleMs ?? SWITCH_SETTLE_MS);
  return { applied: true, action: "close", status: closed, after: await streamStatus(store, keyRoot, tenant, originId), ...gate?.out };
}

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));
const sha256 = (v: string) => createHash("sha256").update(v).digest("hex");

// ---- production preflight ----

export interface PreflightReport {
  action: ProductionAction;
  mode: StreamStatus["mode"] | "corrupt";
  head: number | null;
  materializedThrough: number;
  legacyPending: number;
  legacyCommitted: number;
  /** The log's current epoch (log mode), or null. */
  epoch: number | null;
  /** The deployment-wide proof that every replica runs INGEST_LEDGER_LOG_COMMIT=on (logCommitProofKey). */
  logCommitProven: boolean;
  /** The stream's registry pointer exists (the materializer's sweep finds it). */
  registered: boolean;
  /** Which fence-authority marker puts this org on the S3 ledger (the claim's own rules), or why none does. */
  fenceAuthority: "org" | "deployment" | "absent" | "invalid" | "stopped";
  /** The user's fence: a reset begun and not finished (or one pending) blocks a switch. */
  userFence: "absent" | "clear" | "reset_in_flight" | "unreadable";
  refused?: string;
}

async function readJsonMaybe(store: ObjectStore, key: string): Promise<{ present: false } | { present: true; value: unknown }> {
  let body: string;
  try { body = new TextDecoder().decode(await readAll((await store.getObject(objectKey(key))).body)); }
  catch (error) {
    if (isObjectStoreError(error) && error.code === "not_found") return { present: false };
    throw error;
  }
  try { return { present: true, value: JSON.parse(body) }; } catch { return { present: true, value: undefined }; }
}

async function fenceAuthorityOf(store: ObjectStore, keyRoot: string, orgId: string): Promise<PreflightReport["fenceAuthority"]> {
  // The claim's order (ledger-s3.ts requireFenceAuthority): the STOP first (present at all, even invalid, refuses),
  // then the org's marker (present but invalid refuses), then the deployment-wide one.
  if ((await readJsonMaybe(store, fenceAuthorityStopKey(keyRoot))).present) return "stopped";
  const org = await readJsonMaybe(store, orgFenceAuthorityKey(keyRoot, orgId));
  if (org.present) return validOrgFenceAuthority(org.value) && org.value.orgId === orgId ? "org" : "invalid";
  const all = await readJsonMaybe(store, fenceAuthorityKey(keyRoot));
  if (!all.present) return "absent";
  return validFenceAuthority(all.value) ? "deployment" : "invalid";
}

async function userFenceOf(store: ObjectStore, keyRoot: string, tenant: LedgerTenant): Promise<PreflightReport["userFence"]> {
  const f = await readJsonMaybe(store, s3LedgerKeys(keyRoot, tenant).fence);
  if (!f.present) return "absent";
  const fence = f.value as FenceDoc | undefined;
  if (!fence || typeof fence !== "object" || !Number.isSafeInteger(fence.resets) || !Number.isSafeInteger(fence.resetsCompleted)) return "unreadable";
  return hasPendingReset(fence) || fence.resets !== fence.resetsCompleted ? "reset_in_flight" : "clear";
}

/**
 * Every check a production step needs, read fresh. READ ONLY. `refused` is the first failing check, in the order the
 * report lists them; the report itself is counts and verdicts only.
 */
export async function streamPreflight(store: ObjectStore, keyRoot: string, tenant: LedgerTenant, originId: string,
  action: ProductionAction): Promise<{ report: PreflightReport; legacyCutover: string | null }> {
  const keys = logKeys(keyRoot, tenant, originId);
  let status: StreamStatus, legacyCutover: string | null = null, epoch: number | null = null, corrupt = false;
  try {
    status = await streamStatus(store, keyRoot, tenant, originId);
    if (status.mode !== "legacy") {
      const state = await openStream(store, keys);
      if (state) { legacyCutover = state.legacyCutover; epoch = state.epoch; }
    }
  } catch (error) {
    if (!(error instanceof LogCorrupt)) throw error;
    corrupt = true;
    status = { mode: "legacy", head: null, materializedThrough: -1, legacyPending: 0, legacyCommitted: 0 };
  }
  const [logCommitProven, registered, fenceAuthority, userFence] = await Promise.all([
    exists(store, logCommitProofKey(keyRoot)), exists(store, logStreamRegistryKey(keyRoot, tenant, originId)),
    fenceAuthorityOf(store, keyRoot, tenant.orgId), userFenceOf(store, keyRoot, tenant)]);
  const report: PreflightReport = { action, mode: corrupt ? "corrupt" : status.mode, head: status.head, materializedThrough: status.materializedThrough,
    legacyPending: status.legacyPending, legacyCommitted: status.legacyCommitted, epoch, logCommitProven, registered, fenceAuthority, userFence };
  const refused = corrupt ? "stream_log_corrupt"
    : action === "switch" ? (
      status.mode !== "legacy" ? `stream_is_${status.mode}`
      // No committed legacy batch: a mistyped id, or a stream that never uploaded. Never switch a stream nobody uses.
      : status.legacyCommitted === 0 ? "stream_not_found"
      : fenceAuthority !== "org" && fenceAuthority !== "deployment" ? `fence_authority_${fenceAuthority}`
      : userFence === "reset_in_flight" || userFence === "unreadable" ? `user_fence_${userFence}`
      : !logCommitProven ? "log_commit_not_proven_on"
      : status.legacyPending > 0 ? "legacy_batches_pending"
      : undefined)
    : (
      // Close needs no fence authority: rolling back must stay possible while admission is stopped.
      status.mode !== "log" ? `stream_is_${status.mode}`
      : epoch === null || epoch < 1 ? "stream_log_unreadable"
      : status.materializedThrough !== status.head ? "entries_not_applied"
      : undefined);
  if (refused) report.refused = refused;
  return { report, legacyCutover };
}

export interface ProductionReceipt {
  bucketSha256: string;
  streamSha256: string;
  codeSha: string;
  stateSha256: string;
  planRef: string;
  /** Plan: when the confirmation stops being accepted. Apply: the expiry the confirmation carried. */
  expiresAt: string | null;
  /** Plan only: the exact phrase to type for the apply. */
  confirmation?: string;
  /** Apply only: sha256 of the confirmation that authorized it. */
  approvalSha256?: string;
}
interface ProductionGate { refused?: string; out: { preflight?: PreflightReport; production?: ProductionReceipt } }

async function productionGate(store: ObjectStore, keyRoot: string, tenant: LedgerTenant, originId: string, action: ProductionAction,
  ctx: ProductionContext, apply: boolean): Promise<ProductionGate> {
  const shape = productionContextRefusal(ctx, apply);
  if (shape) return { refused: shape, out: {} };
  const { report, legacyCutover } = await streamPreflight(store, keyRoot, tenant, originId, action);
  const fields = { action, bucketSha256: bucketBindingSha256(ctx.bucket, keyRoot), streamSha256: streamIdentitySha256(keyRoot, tenant, originId),
    codeSha: ctx.codeSha, stateSha256: preflightStateSha256(action, report.mode, legacyCutover), planRef: ctx.planRef };
  if (!apply) {
    const expiresAt = new Date((ctx.nowMs ?? Date.now()) + LEDGER_LOG_PRODUCTION_PLAN_TTL_MS).toISOString();
    // A refused preflight prints no phrase: there is nothing to confirm.
    const production: ProductionReceipt = { ...fields, expiresAt, ...(report.refused ? {} : { confirmation: productionConfirmation({ ...fields, expiresAt }) }) };
    return { refused: report.refused, out: { preflight: report, production } };
  }
  const production: ProductionReceipt = { ...fields, expiresAt: confirmationExpiry(ctx.confirmation ?? "") ?? null,
    approvalSha256: sha256(ctx.confirmation ?? "") };
  if (report.refused) return { refused: report.refused, out: { preflight: report, production } };
  return { refused: confirmationRefusal(ctx, fields), out: { preflight: report, production } };
}

// ---- measure (read only) ----

export interface OriginRate {
  /** The first 12 hex of the origin's key hash (the `o=` segment): never the origin id. */
  originKey: string;
  legacyBatches: number;
  logEntries: number;
  logBatches: number;
  batches: number;
  batchesPerSecond: number;
  /** The busiest bin in the window, as batches per second. */
  peakBinBatchesPerSecond: number;
  aboveVisibleRate: boolean;
}
export interface ArrivalRateReport {
  windowSeconds: number;
  binSeconds: number;
  visibleBatchesPerSecond: number;
  scannedObjects: number;
  /** The listing budget ran out (or the entry-read budget did): every rate is a lower bound. */
  truncated: boolean;
  origins: OriginRate[];
}

/** The `o=` key segment of an origin (what every per-origin key under the user's directory carries). */
export const originKeySegment = (keyRoot: string, tenant: LedgerTenant, originId: string) => logKeys(keyRoot, tenant, originId).root.split("/").pop()!;

/**
 * Batch arrival rate over the last `windowMs`, per origin. With `originId`, that stream only; without, every origin of
 * the user (to find the hottest stream). Legacy batch docs count by their lastModified (the claim writes them and the
 * commit rewrites them, so it is the commit time); log entries by their own atMs, each counting its batches. Lists the
 * user's batch docs and log objects (bounded by `maxPages`), GETs only log entries listed inside the window (bounded
 * by `maxEntryReads`). Writes nothing. A logged batch is counted once: from its entry; the batch doc its materializer
 * wrote is left out of `legacyBatches` (when the entry is inside the window).
 */
export async function measureArrivalRate(store: ObjectStore, keyRoot: string, tenant: LedgerTenant, originId: string | undefined,
  opts: { windowMs: number; nowMs?: number; binMs?: number; maxPages?: number; maxEntryReads?: number }): Promise<ArrivalRateReport> {
  if (!(Number.isSafeInteger(opts.windowMs) && opts.windowMs > 0)) throw Object.assign(new Error("measure_window"), { code: "measure_window" });
  const now = opts.nowMs ?? Date.now(), from = now - opts.windowMs, binMs = opts.binMs ?? 60_000;
  let pagesLeft = opts.maxPages ?? 500, readsLeft = opts.maxEntryReads ?? 5000, scanned = 0, truncated = false;
  const dir = s3LedgerKeys(keyRoot, tenant).dir;
  const one = originId === undefined ? "" : `/${originKeySegment(keyRoot, tenant, originId)}`;
  const per = new Map<string, { legacy: number; entries: number; batches: number; bins: Map<number, number> }>();
  const slot = (o: string) => {
    let v = per.get(o);
    if (!v) per.set(o, v = { legacy: 0, entries: 0, batches: 0, bins: new Map() });
    return v;
  };
  const bin = (bins: Map<number, number>, atMs: number, n: number) => {
    const b = Math.floor((atMs - from) / binMs);
    bins.set(b, (bins.get(b) ?? 0) + n);
  };
  async function* listed(prefix: string) {
    let cursor: string | undefined;
    do {
      if (pagesLeft <= 0) { truncated = true; return; }
      pagesLeft--;
      const page = await store.list(objectKey(prefix), cursor);
      for (const item of page.items) { scanned++; yield item; }
      cursor = page.nextCursor;
    } while (cursor);
  }
  // The log first: the batch docs its materializer wrote for logged batches (`o=<origin>/b=<h(batchId)>`) are then
  // left out of the legacy count, so a batch is counted once on a stream that switched inside the window.
  const logged = new Set<string>();
  for await (const item of listed(`${dir}/ledger/log${one}`)) {
    const m = /\/ledger\/log\/o=([0-9a-f]{32})\/e\/[0-9]+\/[0-9]{20}\.json$/.exec(String(item.key));
    // An entry is written once, so lastModified is its write time; its atMs is read only for entries in the window.
    if (!m || item.lastModified.getTime() < from) continue;
    if (readsLeft <= 0) { truncated = true; break; }
    readsLeft--;
    let e: LogEntry;
    try { e = JSON.parse(new TextDecoder().decode(await readAll((await store.getObject(objectKey(String(item.key)))).body))) as LogEntry; }
    catch (error) {
      if ((isObjectStoreError(error) && error.code === "not_found") || error instanceof SyntaxError) continue;
      throw error;
    }
    if (typeof e?.atMs !== "number" || e.atMs < from || e.atMs > now) continue;
    const v = slot(m[1]!.slice(0, 12));
    v.entries++;
    const list = e.kind === "batches" && Array.isArray(e.batches) ? e.batches : [];
    for (const b of list) if (typeof b?.batchId === "string") logged.add(`o=${m[1]}/b=${sha256(b.batchId).slice(0, 32)}`);
    v.batches += list.length;
    if (list.length) bin(v.bins, e.atMs, list.length);
  }
  for await (const item of listed(`${dir}/ledger/batches${one}`)) {
    const m = /\/ledger\/batches\/(o=([0-9a-f]{32})\/b=[0-9a-f]+)\.json$/.exec(String(item.key));
    const at = item.lastModified.getTime();
    if (!m || at < from || at > now || logged.has(m[1]!)) continue;
    const v = slot(m[2]!.slice(0, 12));
    v.legacy++;
    bin(v.bins, at, 1);
  }
  const seconds = opts.windowMs / 1000, binSeconds = binMs / 1000;
  const round = (x: number) => Math.round(x * 1000) / 1000;
  const origins = [...per.entries()].map(([originKey, v]) => {
    const batches = v.legacy + v.batches;
    const peak = round(Math.max(0, ...v.bins.values()) / binSeconds);
    return { originKey, legacyBatches: v.legacy, logEntries: v.entries, logBatches: v.batches, batches,
      batchesPerSecond: round(batches / seconds), peakBinBatchesPerSecond: peak, aboveVisibleRate: peak >= LOG_STREAM_VISIBLE_BATCHES_PER_S };
  }).sort((a, b) => b.batchesPerSecond - a.batchesPerSecond || (a.originKey < b.originKey ? -1 : 1));
  return { windowSeconds: seconds, binSeconds, visibleBatchesPerSecond: LOG_STREAM_VISIBLE_BATCHES_PER_S, scannedObjects: scanned, truncated, origins };
}
