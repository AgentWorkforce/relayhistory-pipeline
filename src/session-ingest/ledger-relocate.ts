/**
 * Moves the S3 upload ledger between the Locker bucket and a separate ledger bucket (INGEST_LEDGER_BUCKET,
 * ledger-store.ts), offline, for a whole deployment (key root) at once. Runbook: docs/ledger-relocation.md.
 *
 *   bun src/session-ingest/ledger-relocate.ts <plan|copy|verify|switch> [--direction forward|back] [--confirm]
 *       [--concurrency 16] [--min-stop-age-ms 300000] [--replace-stale] [--side both|source|destination]
 *
 * Environment, exactly as the ingest edge reads it (so it runs as a Job with the edge's own ServiceAccount, and no
 * credential is ever on disk): LOCKER_BUCKET / LOCKER_REGION / LOCKER_ENDPOINT / LOCKER_PROVIDER and the Locker
 * credentials (Pod Identity or LOCKER_ACCESS_KEY_ID/SECRET), INGEST_LEDGER_BUCKET / INGEST_LEDGER_REGION,
 * INGEST_KEY_ROOT (default 96bulls).
 *
 * `forward` copies from the Locker bucket to the ledger bucket; `back` the reverse (a rollback after the new
 * location has admitted). The source and destination of a direction:
 *
 *   plan     read-only: ledger object counts and bytes on both sides, the stop marker and both home markers.
 *   copy     --confirm: every ledger object of the source, byte for byte, into the destination, create-only. An object
 *            already there with the same bytes is skipped (reruns are no-ops); different bytes is `diverged` and fails
 *            the run, unless --replace-stale (for `back`, whose destination holds the pre-move copies) replaces it
 *            with a compare-and-swap on the version just read.
 *   verify   read-only: the destination's ledger objects are exactly the source's (same keys, same SHA-256), and
 *            reports the set's digest.
 *   switch   --confirm: verify again, then write the ledger-home marker naming the DESTINATION bucket, first into the
 *            source (from then on a process still configured for it refuses every claim: `ledger_home_moved`) and
 *            then into the destination (the process configured for it needs this one: `ledger_home_missing`).
 *            `--side destination`: write only the destination's, and only when the source's marker already names the
 *            destination (an operator wrote it with a principal that may write the source; the Job's may not).
 *            `--side source`: write only the source's "moved" marker (the operator then writes the destination's), for a
 *            direction whose destination the Job may not write (`back`).
 *
 * copy and switch refuse unless admission is stopped: the deployment-wide stop marker
 * (`<keyRoot>/.ledger/fence-authority/.stop.json`, ledger-s3.ts FenceAuthorityStopDoc) is in the Locker bucket and
 * older than --min-stop-age-ms (default 5 min: past the ledger's 60 s marker cache and a pod's 90 s grace), and the
 * source is the current home (its marker absent or naming it). The stop does not end a claimed batch's commit: the
 * runbook also scales the edge to zero first. Output: one JSON line of counts and codes only, never a key.
 * Exit 0 ok, 1 a check failed, 2 refused or misconfigured.
 */
import { createHash } from "node:crypto";
import { isObjectStoreError, objectKey, readAll, type ObjectStore, type ObjectVersion } from "../shared/vendor.ts";
import { buildLockerStore } from "../shared/locker.ts";
import { INBOX_NAMESPACE } from "./async-admission.ts";
import { fenceAuthorityStopKey, validFenceAuthorityStop } from "./ledger-s3.ts";
import { ledgerHomeKey, ledgerKeyMatcher, readLedgerHome, type LedgerHomeDoc } from "./ledger-store.ts";
import { selfhostLedgerStore } from "./selfhost-config.ts";

export type RelocateMode = "plan" | "copy" | "verify" | "switch";
export interface RelocateArgs {
  mode: RelocateMode;
  direction: "forward" | "back";
  confirm: boolean;
  concurrency: number;
  minStopAgeMs: number;
  replaceStale: boolean;
  side: "both" | "source" | "destination";
}
export class RelocateRefused extends Error {
  constructor(readonly code: string) { super(`ledger_relocate_refused: ${code}`); }
}
const refuse = (code: string): never => { throw new RelocateRefused(code); };

export function parseRelocateArgs(argv: readonly string[]): RelocateArgs {
  const [mode, ...rest] = argv;
  if (mode !== "plan" && mode !== "copy" && mode !== "verify" && mode !== "switch") return refuse("usage");
  const out: RelocateArgs = { mode, direction: "forward", confirm: false, concurrency: 16, minStopAgeMs: 300_000, replaceStale: false, side: "both" };
  for (let i = 0; i < rest.length; i++) {
    const a = rest[i]!;
    const value = () => { const v = rest[++i]; if (v === undefined || v.startsWith("--")) refuse("usage"); return v!; };
    const int = (v: string, min: number, max: number) => { if (!/^\d+$/.test(v) || Number(v) < min || Number(v) > max) refuse("usage"); return Number(v); };
    if (a === "--direction") { const v = value(); if (v !== "forward" && v !== "back") refuse("usage"); out.direction = v as "forward" | "back"; }
    else if (a === "--confirm") out.confirm = true;
    else if (a === "--replace-stale") out.replaceStale = true;
    else if (a === "--side") { const v = value(); if (v !== "both" && v !== "source" && v !== "destination") refuse("usage"); out.side = v as RelocateArgs["side"]; }
    else if (a === "--concurrency") out.concurrency = int(value(), 1, 64);
    else if (a === "--min-stop-age-ms") out.minStopAgeMs = int(value(), 0, 86_400_000);
    else refuse("usage");
  }
  if ((out.mode === "copy" || out.mode === "switch") && !out.confirm) refuse("confirm_required");
  if (out.replaceStale && out.mode !== "copy") refuse("usage");
  if (out.side !== "both" && out.mode !== "switch") refuse("usage");
  return out;
}

export interface RelocateSide { store: ObjectStore; bucket: string }
export interface RelocateDeps {
  /** The Locker bucket (the stop marker is read here). */
  main: RelocateSide;
  /** The separate ledger bucket. */
  ledger: RelocateSide;
  keyRoot: string;
  nowMs?: () => number;
}

/**
 * The Content-Type every ledger writer sets: `application/json` on the `.json` documents (ledger-s3, ledger-log,
 * rearm-sweeper all pass it); anything else (log payloads, `.json.zst`) was written without one, so the store infers it
 * from the key exactly as it did then. ObjectMeta carries no Content-Type to copy, so this reproduces the writers' rule.
 */
const contentTypeOf = (key: string): { contentType?: string } => (key.endsWith(".json") ? { contentType: "application/json" } : {});
const sha = (b: Uint8Array) => createHash("sha256").update(b).digest("hex");
const enc = new TextEncoder();

async function ledgerKeys(store: ObjectStore, keyRoot: string): Promise<string[]> {
  const isLedger = ledgerKeyMatcher(keyRoot), home = ledgerHomeKey(keyRoot), out: string[] = [];
  let cursor: string | undefined;
  do {
    const page = await store.list(objectKey(`${keyRoot}/${INBOX_NAMESPACE}`), cursor);
    for (const i of page.items) { const k = String(i.key); if (k !== home && isLedger(k)) out.push(k); }
    cursor = page.nextCursor;
  } while (cursor);
  return out;
}
async function read(store: ObjectStore, key: string): Promise<{ bytes: Uint8Array; version: ObjectVersion } | null> {
  try {
    const got = await store.getObject(objectKey(key));
    return { bytes: await readAll(got.body), version: got.meta.version };
  } catch (error) {
    if (isObjectStoreError(error, "not_found")) return null;
    throw error;
  }
}
async function pool<T>(items: readonly T[], n: number, f: (t: T) => Promise<void>): Promise<void> {
  let next = 0;
  await Promise.all(Array.from({ length: Math.min(n, items.length) }, async () => { while (next < items.length) await f(items[next++]!); }));
}

async function homeOf(store: ObjectStore, keyRoot: string): Promise<LedgerHomeDoc | null | "invalid"> {
  try { return await readLedgerHome(store, keyRoot); } catch (error) {
    if (error instanceof Error && error.message === "ledger_home_invalid") return "invalid";
    throw error;
  }
}

/** Admission is stopped (long enough) and the source is the ledger's current home. */
async function preconditions(d: RelocateDeps, src: RelocateSide, args: RelocateArgs, skipHome = false): Promise<void> {
  const stop = await read(d.main.store, fenceAuthorityStopKey(d.keyRoot));
  if (!stop) refuse("admission_not_stopped");
  let doc: unknown;
  try { doc = JSON.parse(new TextDecoder().decode(stop!.bytes)); } catch { refuse("stop_marker_invalid"); }
  if (!validFenceAuthorityStop(doc)) refuse("stop_marker_invalid");
  if ((d.nowMs ?? Date.now)() - (doc as { writtenAtMs: number }).writtenAtMs < args.minStopAgeMs) refuse("stop_too_recent");
  if (skipHome) return;
  const home = await homeOf(src.store, d.keyRoot);
  if (home === "invalid") refuse("source_home_invalid");
  if (home !== null && (home as LedgerHomeDoc).home !== src.bucket) refuse("source_not_home");
}

export interface RelocateReport {
  mode: RelocateMode; direction: "forward" | "back"; ok: boolean;
  source: { objects: number; bytes: number };
  destination: { objects: number };
  copied?: number; skipped?: number; replaced?: number; diverged?: number;
  missing?: number; extra?: number; mismatched?: number; digest?: string;
  stop?: "present" | "absent"; sourceHome?: string; destinationHome?: string;
}

const homeLabel = (h: LedgerHomeDoc | null | "invalid", side: RelocateSide, other: RelocateSide) =>
  h === null ? "absent" : h === "invalid" ? "invalid" : h.home === side.bucket ? "self" : h.home === other.bucket ? "other" : "unknown";

export async function relocateLedger(d: RelocateDeps, args: RelocateArgs): Promise<RelocateReport> {
  const [src, dst] = args.direction === "forward" ? [d.main, d.ledger] : [d.ledger, d.main];
  const base = { mode: args.mode, direction: args.direction };

  async function verify(): Promise<Omit<RelocateReport, "mode" | "direction">> {
    const [sk, dk] = await Promise.all([ledgerKeys(src.store, d.keyRoot), ledgerKeys(dst.store, d.keyRoot)]);
    const dset = new Set(dk);
    let missing = 0, mismatched = 0, bytes = 0;
    const hashes = new Map<string, string>();
    await pool(sk, args.concurrency, async (k) => {
      const [a, b] = await Promise.all([read(src.store, k), dset.has(k) ? read(dst.store, k) : Promise.resolve(null)]);
      if (!a) return; // gone since the listing: not a ledger object any more (log payload GC); the extra check catches the rest
      bytes += a.bytes.byteLength;
      const h = sha(a.bytes);
      hashes.set(k, h);
      if (!b) missing++; else if (sha(b.bytes) !== h) mismatched++;
    });
    const sset = new Set(sk);
    const extra = dk.filter((k) => !sset.has(k)).length;
    const digest = sha(enc.encode([...hashes].sort(([x], [y]) => (x < y ? -1 : 1)).map(([k, h]) => `${k}\t${h}`).join("\n")));
    return { ok: missing === 0 && mismatched === 0 && extra === 0, source: { objects: hashes.size, bytes }, destination: { objects: dk.length }, missing, extra, mismatched, digest };
  }

  if (args.mode === "plan") {
    const [sk, dk, stop, sh, dh] = await Promise.all([ledgerKeys(src.store, d.keyRoot), ledgerKeys(dst.store, d.keyRoot),
      read(d.main.store, fenceAuthorityStopKey(d.keyRoot)), homeOf(src.store, d.keyRoot), homeOf(dst.store, d.keyRoot)]);
    let bytes = 0;
    await pool(sk, args.concurrency, async (k) => { const m = await src.store.head(objectKey(k)); bytes += m?.size ?? 0; });
    return { ...base, ok: true, source: { objects: sk.length, bytes }, destination: { objects: dk.length }, stop: stop ? "present" : "absent",
      sourceHome: homeLabel(sh, src, dst), destinationHome: homeLabel(dh, dst, src) };
  }
  if (args.mode === "verify") return { ...base, ...(await verify()) };

  // switch --side destination: the source already names the destination (written first, by the operator), so the
  // source-is-home precondition cannot hold; it is replaced by exactly that check.
  if (args.mode === "switch" && args.side === "destination") {
    const sh = await homeOf(src.store, d.keyRoot);
    if (sh === null || sh === "invalid" || sh.home !== dst.bucket) refuse("source_not_moved");
    await preconditions(d, src, args, true);
  } else await preconditions(d, src, args);
  if (args.mode === "copy") {
    const keys = await ledgerKeys(src.store, d.keyRoot);
    let copied = 0, skipped = 0, replaced = 0, diverged = 0, bytes = 0;
    await pool(keys, args.concurrency, async (k) => {
      const a = await read(src.store, k);
      if (!a) return;
      bytes += a.bytes.byteLength;
      try {
        const ack = await dst.store.put(objectKey(k), a.bytes, { ifNoneMatch: true, ...contentTypeOf(k) });
        if (ack.created) copied++; else skipped++;
        return;
      } catch (error) {
        if (!isObjectStoreError(error, "already_exists") && !isObjectStoreError(error, "precondition_failed")) throw error;
      }
      const b = await read(dst.store, k);
      if (b && sha(b.bytes) === sha(a.bytes)) { skipped++; return; }
      if (!b || !args.replaceStale) { diverged++; return; }
      await dst.store.put(objectKey(k), a.bytes, { ifMatch: b.version, ...contentTypeOf(k) });
      replaced++;
    });
    return { ...base, ok: diverged === 0, source: { objects: keys.length, bytes }, destination: { objects: copied + skipped + replaced }, copied, skipped, replaced, diverged };
  }
  // switch
  const v = await verify();
  if (!v.ok) return { ...base, ...v };
  const doc: LedgerHomeDoc = { v: 1, home: dst.bucket, writtenAtMs: (d.nowMs ?? Date.now)() };
  const body = enc.encode(JSON.stringify(doc));
  // The source first: from here a process left on it refuses; the destination's marker then lets the new one admit.
  if (args.side !== "destination") await src.store.put(objectKey(ledgerHomeKey(d.keyRoot)), body, { contentType: "application/json" });
  if (args.side !== "source") await dst.store.put(objectKey(ledgerHomeKey(d.keyRoot)), body, { contentType: "application/json" });
  return { ...base, ...v, sourceHome: "other", ...(args.side === "source" ? {} : { destinationHome: "self" }) };
}

/** The two stores from the ingest edge's own environment. */
export function relocateDepsFromEnv(env: NodeJS.ProcessEnv): RelocateDeps {
  if (!env.LOCKER_BUCKET) refuse("config_invalid");
  let sep;
  try { sep = selfhostLedgerStore(env, "s3"); } catch { return refuse("config_invalid"); }
  if (!sep) return refuse("config_invalid");
  const keyRoot = env.INGEST_KEY_ROOT?.trim() || "96bulls";
  if (!/^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$/.test(keyRoot)) refuse("config_invalid");
  const main = buildLockerStore(env).store as unknown as ObjectStore;
  const ledgerEnv: NodeJS.ProcessEnv = { ...env, LOCKER_BUCKET: sep.bucket, LOCKER_REGION: sep.region };
  if (sep.endpoint) ledgerEnv.LOCKER_ENDPOINT = sep.endpoint; else delete ledgerEnv.LOCKER_ENDPOINT;
  const ledger = buildLockerStore(ledgerEnv).store as unknown as ObjectStore;
  return { main: { store: main, bucket: env.LOCKER_BUCKET! }, ledger: { store: ledger, bucket: sep.bucket }, keyRoot };
}

if (import.meta.main) {
  let code = 0;
  try {
    const args = parseRelocateArgs(process.argv.slice(2));
    const report = await relocateLedger(relocateDepsFromEnv(process.env), args);
    console.log(JSON.stringify(report));
    code = report.ok ? 0 : 1;
  } catch (error) {
    if (error instanceof RelocateRefused) { console.log(JSON.stringify({ ok: false, refused: error.code })); code = 2; }
    else { console.log(JSON.stringify({ ok: false, error: isObjectStoreError(error) ? (error as { code?: string }).code ?? "store_error" : "failed" })); code = 2; }
  }
  process.exit(code);
}
