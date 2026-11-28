/**
 * `neon-s3-erasure` CLI: the transcript bucket's only deleter.
 *
 *   <credentials JSON> | bun run migration:neon-s3-erasure [--execute] [--once]
 *
 * Credentials come ONLY through stdin, held in RAM (zero-Neon checklist E3; AGENTS.md: nothing credential-like
 * on disk). One JSON object:
 *   {"databaseUrl": "<read-only-capable URL of the privacy authority>",
 *    "eraser": {"accessKeyId": "...", "secretAccessKey": "...", "sessionToken": "...", "expiration": "..."}}   // --execute only
 * The eraser key may instead be the broker's AWS credential_process v1 object pasted in as is
 * ({"Version":1,"AccessKeyId","SecretAccessKey","SessionToken","Expiration"}; ../short-lived-credentials.ts). With an
 * expiration the job refuses to start, and stops taking new work, once less than S3X_CREDS_MIN_REMAINING_S (600,
 * 120-3000) is left, then exits 75 for the supervisor to mint a new key and restart. A dry run needs no eraser key
 * at all: only the authority's database login, supplied through a private input of the operator's choosing. The former
 * NEON_URL_FILE / NEON_URL / S3X_ERASER_CREDS_FILE (and any AWS_* credential variable) now refuse to start.
 * A terminal or a regular file on stdin refuses too.
 *
 * Without `--execute` it sweeps and reports what it would erase, deleting nothing. Shares the exporter's
 * watermark (`S3X_STATE`) on the same host.
 *
 *   S3X_PRIVACY_AUTHORITY  "neon" (default): databaseUrl is a direct Neon URL; reads deleted_upload_sessions.
 *                          "supabase" (C3): databaseUrl is a Supabase session/direct URL for the
 *                          bulls96_privacy_eraser login (owner/superuser/BYPASSRLS refused at startup);
 *                          reads the union of frozen-source and target deletions through the eraser RPCs,
 *                          and erases nothing unless the Supabase privacy authority is active. Neon is not
 *                          connected at all.
 *   S3X_STATE, S3X_BUCKET, S3X_EXPECT_BUCKET, S3X_REGION, S3X_ENDPOINT, S3X_KEY_ROOT,
 *   S3X_ERASER_RECOGNIZES_LEGACY ("true" keeps legacy-only groups; default false; neon only)
 *   S3X_LEGACY_EXPORT_ERASURE_ENABLED (exactly "true" enables the 062030 legacy fence feed; default false)
 *
 * Phase: Babysitter evidence retention (./babysitter-phase.ts), BABYSITTER_RETENTION_SWEEP
 * off (default) | dry_run | on. It shares this process's eraser, --execute gate and stop
 * signal; its registry login (the least-privilege bulls96_babysitter_retention role, never
 * the ingest login) arrives as the optional "retentionDatabaseUrl" field of the same stdin
 * credential JSON, required only when the phase is not off.
 *
 *   <credentials JSON> | bun run migration:neon-s3-erasure --stale-prefixes-only --only-identity <sha256> [...] [--execute] [--max <n>]
 *
 * Stale-prefixes-only mode (runbooks/neon-s3-sync.md): ONE pass over the queued session prefixes the exporter retired
 * after a date move (kind prefix, reason stale_prefix), erasing one only when the old prefix holds no current key and a
 * current, date-moved copy of the same session has a live manifest. No sweep, no babysitter phase, nothing else in the
 * queue touched. Only entries whose session identity (job.ts staleprefixIdentityHash) is named by a required, repeatable
 * --only-identity <sha256> (1..100 distinct values) are considered; every other entry is skipped untouched. Without
 * --execute it reads only and reports. Refused with BABYSITTER_RETENTION_SWEEP set to anything but off, with
 * S3X_OBLIGATIONS on, with any flag other than --execute, --once (implied), --max <n> and --only-identity <sha256>;
 * outside the mode --max, --only-identity and any argument resembling the switch, --max or --only-identity (a
 * misspelled switch) are refused. --execute refuses to start while another eraser process holds an unexpired lease;
 * the mode never releases leases.
 *
 *   {"reader": <S3 key>} | bun run migration:neon-s3-erasure --stale-prefixes-only --verify-only --only-identity <sha256> [...] [--receipt-out <path>]
 *
 * Verify-only (READ-ONLY, stale-prefix-verify.ts): no database and no eraser key; a reader key (ListBucketVersions +
 * GetObjectVersion) on stdin and the state file. Compares every version under each allowlisted old prefix, record by
 * record, with the survivor's current pinned versions, and writes the receipt (hashes and counts only). Exit 0 PASS,
 * 1 FAIL. --receipt-out is created, never overwritten.
 *
 * --execute in the mode REQUIRES --verified-receipt <path> (that receipt) and --protection-attestation <path> (the
 * infra attestation, stale-prefix-verify.ts ProtectionAttestation). The protection preflight (lifecycle, versioning,
 * attestation) and the per-item receipt fence (job.ts #receiptItem: exact old set, survivor pins current before every
 * delete, the survivor-prefix lease) refuse before the first delete when anything is unproven.
 */
import type { Queryable } from "../relayhistory-export.ts";
import { sweepMode } from "../../session-ingest/babysitter-retention.ts";
import { sessionReadOnly } from "../neon-s3-sync/neon-reader.ts";
import { SyncState, obligationsFlag } from "../neon-s3-sync/state.ts";
import { errorCode } from "../neon-s3-sync/sync.ts";
import { eraserDatabaseUrl, pinnedPgClient, readStdinCredentials, refuseCredentialEnv, s3Credentials, type S3Credentials } from "../stdin-credentials.ts";
import { EXIT_CREDENTIALS_RENEW, expiringSoon, minRemainingMs } from "../short-lived-credentials.ts";
import { assertEraserIdentity, neonAuthority, supabaseAuthority, type ErasureAuthorityKind } from "./authority.ts";
import { createS3Eraser, createS3VersionReader, isLiveObjectProbe, isReceiptFence, type Eraser, type PagedEraser } from "./eraser.ts";
import { parseReceipt, sha256Hex, type ProtectionAttestation } from "./stale-prefix-verify.ts";
import type { ErasureAuthority } from "./authority.ts";
import { closeSync, fstatSync, openSync, readSync, realpathSync, statSync, writeFileSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { DEFAULT_ERASURE_CONFIG, ErasureJob, STALE_PREFIX_ALLOWLIST_MAX } from "./job.ts";
import { phaseSettings, poolConfig, startBabysitterPhase, type BabysitterPhase } from "./babysitter-phase.ts";

/**
 * S3X_OBLIGATIONS (default OFF) gates the new exporter-obligation protocol, its schema and genesis bootstrap. The
 * strict canonical paged eraser runs in BOTH modes (safety hardening, unflagged); off it runs without obligations.
 */
export function obligationsEnabled(e: (k: string) => string | undefined = (k) => process.env[k]): boolean {
  return obligationsFlag(e("S3X_OBLIGATIONS"));
}

export function legacyExportErasureEnabled(raw: string | undefined): boolean {
  if (raw === undefined || raw.trim() === "") return false;
  if (raw === "true") return true;
  throw Object.assign(new Error("S3X_LEGACY_EXPORT_ERASURE_ENABLED must be exactly true or unset"), {
    code: "config_flag_invalid",
  });
}

const optionRefusal = (code: string, message: string) => Object.assign(new Error(message), { code });

/** Default path only: an argument that could be a mistyped `--stale-prefixes-only`, `--max` or `--only-identity`. */
function nearMissArgument(a: string): boolean {
  const lower = a.toLowerCase();
  const bare = lower.replace(/^-+/, "");
  return lower.includes("stale") || lower.includes("prefix") || lower.includes("identit") || lower.includes("receipt") || lower.includes("verif")
    || lower.includes("attest") || bare.startsWith("max") || bare.startsWith("only");
}

export interface StalePrefixOptions {
  execute: boolean;
  max?: number;
  onlyIdentities: string[];
  /** --verify-only: the read-only verifier (stale-prefix-verify.ts); never with --execute. */
  verifyOnly: boolean;
  receiptOut?: string;
  /** --execute: all required (the receipt fence and the owner-approved values). */
  verifiedReceipt?: string;
  protectionAttestation?: string;
  receiptSha256?: string;
  attestationSha256?: string;
  approvedAllowlistSha256?: string;
  approvedMax?: number;
}

/**
 * The stale-prefixes-only switch and its options, refused before any credential is read. `args` is argv after the
 * script. Undefined when the switch is absent (the default job, whose other flags are not changed by this parser).
 * The default job ignores unknown flags, so a misspelled switch next to --execute would run the FULL job (sweeps
 * included): without the exact switch, any argument that looks like the switch, --max or --only-identity is refused
 * here (its lower-cased text contains "stale", "prefix" or "identit", or after its leading dashes it starts with "max"
 * or "only" in any case). Every other argument is left to the default job, which keeps ignoring what it does not use.
 * In the mode, `--only-identity <sha256>` (64 lowercase hex, repeatable, distinct, 1..100) is required, dry or not.
 */
export function staleprefixOptions(args: readonly string[], e: (k: string) => string | undefined = (k) => process.env[k]): StalePrefixOptions | undefined {
  if (!args.includes("--stale-prefixes-only")) {
    if (args.some((a) => a === "--max" || a.startsWith("--max=")))
      throw optionRefusal("stale_prefix_max_without_mode", "--max applies only with --stale-prefixes-only");
    if (args.some((a) => a === "--only-identity" || a.startsWith("--only-identity=")))
      throw optionRefusal("stale_prefix_only_identity_without_mode", "--only-identity applies only with --stale-prefixes-only");
    if (args.some(nearMissArgument))
      throw optionRefusal("stale_prefix_mode_flag_refused", "an argument resembles the stale-prefix switch or --max but is not exactly --stale-prefixes-only: refused before the full job could run");
    return undefined;
  }
  let execute = false, max: number | undefined, seen = 0, verifyOnly = false;
  let receiptOut: string | undefined, verifiedReceipt: string | undefined, protectionAttestation: string | undefined;
  let receiptSha256: string | undefined, attestationSha256: string | undefined, approvedAllowlistSha256: string | undefined, approvedMax: number | undefined;
  const hexArg = (i: number, name: string, prior: string | undefined): string => {
    if (prior !== undefined) throw optionRefusal("stale_prefix_sha256_invalid", `${name} given more than once`);
    const v = args[i];
    if (v === undefined || !/^[0-9a-f]{64}$/.test(v)) throw optionRefusal("stale_prefix_sha256_invalid", `${name} takes exactly 64 lowercase hex characters`);
    return v;
  };
  const onlyIdentities: string[] = [];
  const pathArg = (i: number, name: string, prior: string | undefined): string => {
    if (prior !== undefined) throw optionRefusal("stale_prefix_path_invalid", `${name} given more than once`);
    const v = args[i];
    if (v === undefined || v === "" || v.startsWith("-")) throw optionRefusal("stale_prefix_path_invalid", `${name} takes one file path`);
    return v;
  };
  for (let i = 0; i < args.length; i++) {
    const a = args[i]!;
    if (a === "--stale-prefixes-only") seen += 1;
    else if (a === "--execute") execute = true;
    else if (a === "--once") continue; // implied: the mode is always one pass
    else if (a === "--max") {
      if (max !== undefined) throw optionRefusal("stale_prefix_max_invalid", "--max given more than once");
      const v = args[++i];
      if (v === undefined || !/^[1-9][0-9]*$/.test(v) || !Number.isSafeInteger(Number(v)))
        throw optionRefusal("stale_prefix_max_invalid", "--max takes a positive decimal integer");
      max = Number(v);
    } else if (a === "--only-identity") {
      const v = args[++i];
      if (v === undefined || !/^[0-9a-f]{64}$/.test(v))
        throw optionRefusal("stale_prefix_only_identity_invalid", "--only-identity takes one session identity hash: exactly 64 lowercase hex characters");
      if (onlyIdentities.includes(v)) throw optionRefusal("stale_prefix_only_identity_duplicate", "--only-identity given the same identity more than once");
      onlyIdentities.push(v);
    } else if (a === "--verify-only") {
      if (verifyOnly) throw optionRefusal("stale_prefix_mode_flag_refused", "--verify-only given more than once");
      verifyOnly = true;
    } else if (a === "--receipt-out") receiptOut = pathArg(++i, "--receipt-out", receiptOut);
    else if (a === "--verified-receipt") verifiedReceipt = pathArg(++i, "--verified-receipt", verifiedReceipt);
    else if (a === "--protection-attestation") protectionAttestation = pathArg(++i, "--protection-attestation", protectionAttestation);
    else if (a === "--receipt-sha256") receiptSha256 = hexArg(++i, "--receipt-sha256", receiptSha256);
    else if (a === "--attestation-sha256") attestationSha256 = hexArg(++i, "--attestation-sha256", attestationSha256);
    else if (a === "--approved-allowlist-sha256") approvedAllowlistSha256 = hexArg(++i, "--approved-allowlist-sha256", approvedAllowlistSha256);
    else if (a === "--approved-max") {
      if (approvedMax !== undefined) throw optionRefusal("stale_prefix_max_invalid", "--approved-max given more than once");
      const v = args[++i];
      if (v === undefined || !/^[1-9][0-9]*$/.test(v) || !Number.isSafeInteger(Number(v))) throw optionRefusal("stale_prefix_max_invalid", "--approved-max takes a positive decimal integer");
      approvedMax = Number(v);
    } else throw optionRefusal("stale_prefix_mode_flag_refused", "--stale-prefixes-only accepts only --execute, --once, --max <n>, --only-identity <sha256>, --verify-only, --receipt-out <path>, --verified-receipt <path>, --protection-attestation <path>, --receipt-sha256 <hex>, --attestation-sha256 <hex>, --approved-allowlist-sha256 <hex> and --approved-max <n>");
  }
  if (seen !== 1) throw optionRefusal("stale_prefix_mode_flag_refused", "--stale-prefixes-only given more than once");
  if (onlyIdentities.length === 0)
    throw optionRefusal("stale_prefix_only_identity_required", "--stale-prefixes-only requires at least one --only-identity <sha256>, dry run or not");
  if (onlyIdentities.length > STALE_PREFIX_ALLOWLIST_MAX)
    throw optionRefusal("stale_prefix_only_identity_too_many", `--only-identity given more than ${STALE_PREFIX_ALLOWLIST_MAX} times`);
  let retention: string;
  try {
    retention = sweepMode(e("BABYSITTER_RETENTION_SWEEP"));
  } catch {
    retention = "invalid";
  }
  if (retention !== "off") throw optionRefusal("stale_prefix_mode_retention_refused", "--stale-prefixes-only runs no babysitter phase: BABYSITTER_RETENTION_SWEEP must be off or unset");
  if (obligationsEnabled(e)) throw optionRefusal("stale_prefix_mode_requires_obligations_off", "--stale-prefixes-only runs only with S3X_OBLIGATIONS off");
  if (verifyOnly && (execute || max !== undefined || verifiedReceipt !== undefined || protectionAttestation !== undefined))
    throw optionRefusal("stale_prefix_verify_only_flag_refused", "--verify-only is read-only: it takes no --execute, --max, --verified-receipt or --protection-attestation");
  if (receiptOut !== undefined && !verifyOnly) throw optionRefusal("stale_prefix_receipt_out_without_verify", "--receipt-out applies only with --verify-only");
  const executeOnly = [verifiedReceipt, protectionAttestation, receiptSha256, attestationSha256, approvedAllowlistSha256, approvedMax];
  if (!execute && executeOnly.some((x) => x !== undefined))
    throw optionRefusal("stale_prefix_receipt_without_execute", "--verified-receipt, --protection-attestation, their --*-sha256 and the --approved-* values apply only with --execute");
  if (execute && verifiedReceipt === undefined) throw optionRefusal("stale_prefix_execute_requires_verified_receipt", "--execute requires --verified-receipt <path>");
  if (execute && protectionAttestation === undefined) throw optionRefusal("stale_prefix_execute_requires_attestation", "--execute requires --protection-attestation <path>");
  // The owner-approved decision entry pins the exact files and the batch: all four are required, and --max with them.
  if (execute && (receiptSha256 === undefined || attestationSha256 === undefined || approvedAllowlistSha256 === undefined || approvedMax === undefined || max === undefined))
    throw optionRefusal("stale_prefix_execute_requires_approval", "--execute requires --receipt-sha256, --attestation-sha256, --approved-allowlist-sha256, --approved-max and --max");
  return {
    execute, onlyIdentities, verifyOnly, ...(max !== undefined ? { max } : {}), ...(receiptOut !== undefined ? { receiptOut } : {}),
    ...(verifiedReceipt !== undefined ? { verifiedReceipt } : {}), ...(protectionAttestation !== undefined ? { protectionAttestation } : {}),
    ...(receiptSha256 !== undefined ? { receiptSha256 } : {}), ...(attestationSha256 !== undefined ? { attestationSha256 } : {}),
    ...(approvedAllowlistSha256 !== undefined ? { approvedAllowlistSha256 } : {}), ...(approvedMax !== undefined ? { approvedMax } : {}),
  };
}

const env = (k: string, d?: string) => process.env[k]?.trim() || d;
const flag = (name: string) => process.argv.includes(`--${name}`);
function log(fields: Record<string, unknown>) {
  process.stdout.write(`${JSON.stringify({ at: new Date().toISOString(), service: "neon-s3-erasure", ...fields })}\n`);
}

export function eraserFor(execute: boolean, creds: S3Credentials | undefined, e: (k: string, d?: string) => string | undefined = env): Eraser & PagedEraser {
  if (!execute)
    return {
      eraseObject: async () => 0,
      erasePrefix: async () => 0,
      eraseSessionAnyDate: async () => ({ deleted: 0, kept: 0 }),
      eraseObjectBefore: async () => ({ deleted: 0, kept: 0 }),
      listKeys: async () => [],
      // A dry run never reaches a delete path (the sweep returns before erasing); refuse loudly if it ever did.
      eraseStep: async () => { throw Object.assign(new Error("dry run: no erasure"), { code: "dry_run" }); },
      verifyErased: async () => { throw Object.assign(new Error("dry run: no erasure"), { code: "dry_run" }); },
    };
  const bucket = e("S3X_BUCKET");
  if (!bucket || e("S3X_EXPECT_BUCKET") !== bucket) throw new Error("S3X_BUCKET must equal S3X_EXPECT_BUCKET (write guard)");
  if (!creds) throw new Error("stdin eraser credentials are required with --execute");
  const endpoint = e("S3X_ENDPOINT");
  return createS3Eraser({ bucket, region: e("S3X_REGION", "us-east-1")!, ...(endpoint ? { endpoint } : {}), credentials: creds });
}

export function authorityKind(raw: string | undefined): ErasureAuthorityKind {
  const v = raw?.trim() || "neon";
  if (v !== "neon" && v !== "supabase") throw new Error("S3X_PRIVACY_AUTHORITY must be neon or supabase");
  return v;
}

/** The authority database URL, refused when it cannot hold this job's session-level read-only settings. */
export function authorityUrl(kind: ErasureAuthorityKind, raw: unknown): string {
  // Session-level read-only settings must stick: the shared guard refuses known transaction poolers pre-connect.
  // No startup-parameter overrides (options=-c session_authorization=... would forge the eraser identity).
  return eraserDatabaseUrl(raw, "databaseUrl", kind);
}

/**
 * The stale-prefix pass's exit status: 0 when it completed, EXIT_CREDENTIALS_RENEW when it stopped because the
 * eraser key is about to expire (a partial pass to rerun after renewal, never reported as success). Throws the
 * existing refusals for a held lease, or for a failed erase that was not an expiry stop.
 */
export function stalePassStatus(r: { ok: boolean; refused?: string; stoppedBy?: string }, credStop: boolean): number {
  if (r.refused) throw optionRefusal(r.refused, "the stale-prefix pass refused to start (nothing erased)");
  // A genuine failed erase (ok=false, stoppedBy=<outcome>) always fails, even if the expiry timer also fired.
  // An expiry stop (the job's stop signal) between items is a partial batch to rerun after renewal: 75, never success.
  if (!r.ok && credStop && r.stoppedBy === "signal") return EXIT_CREDENTIALS_RENEW;
  if (!r.ok) throw optionRefusal("stale_prefix_pass_stopped", "the stale-prefix pass stopped at a failed erase");
  // An expiry stop (the job's stop signal; ok stays true) is a partial pass: renew and rerun, never success.
  if (credStop) return EXIT_CREDENTIALS_RENEW;
  return 0;
}

/** The exit status for an error that escaped main(): 75 for a short-lived credential refusal (renew), else 1. */
export function exitStatusForError(error: unknown): number {
  return errorCode(error).startsWith("short_lived_") ? EXIT_CREDENTIALS_RENEW : 1;
}

/** The process exit status after a run: EXIT_CREDENTIALS_RENEW whenever the run stopped on an expiring key. */
export function runStatus(status: number, credStop: boolean): number {
  return credStop ? EXIT_CREDENTIALS_RENEW : status;
}

/** A small operator file (receipt or attestation), bounded; any read failure is the given refusal code. */
/**
 * An operator file bound to the owner-approved decision entry: its exact bytes must hash to `sha256` (copied from
 * that entry), else `<code>_hash_mismatch`. Returns the text.
 */
export function boundOperatorFile(path: string, sha256: string, code: string): string {
  // ONE read: the bytes hashed are exactly the bytes parsed.
  const bytes = readBoundedBytes(path, code);
  if (createHash("sha256").update(bytes).digest("hex") !== sha256) throw optionRefusal(`${code}_hash_mismatch`, "an operator file does not match its approved sha256");
  return utf8OrRefuse(bytes, code);
}

/** The verifier's source commit: HEAD of a CLEAN checkout (a dirty tree is refused: the commit would not name the code). */
export function verifierCommit(dir = dirname(fileURLToPath(import.meta.url))): string {
  try {
    const head = execFileSync("git", ["-C", dir, "rev-parse", "HEAD"], { encoding: "utf8", stdio: ["ignore", "pipe", "ignore"] }).trim();
    const dirty = execFileSync("git", ["-C", dir, "status", "--porcelain", "--untracked-files=no"], { encoding: "utf8", stdio: ["ignore", "pipe", "ignore"] }).trim();
    if (!/^[0-9a-f]{40}$/.test(head) || dirty) throw new Error("unclean");
    return head;
  } catch {
    throw optionRefusal("stale_prefix_verifier_commit_unknown", "the verifier commit cannot be established (a clean git checkout is required)");
  }
}

/** The state file's identity: sha256 over its real path, device and inode (stable across runs, distinct per file). */
export function stateIdentity(path: string): string {
  const real = realpathSync(path), st = statSync(real);
  return createHash("sha256").update(`${real}\n${st.dev}\n${st.ino}`).digest("hex");
}

function readBoundedBytes(path: string, code: string, max = 32 * 1024 * 1024): Uint8Array {
  let fd: number | undefined;
  try {
    fd = openSync(path, "r");
    const st = fstatSync(fd);
    if (!st.isFile() || st.size > max) throw new Error("not a bounded regular file");
    const buf = Buffer.alloc(st.size);
    let at = 0;
    while (at < st.size) { const n = readSync(fd, buf, at, st.size - at, at); if (n <= 0) break; at += n; }
    return buf.subarray(0, at);
  } catch {
    throw optionRefusal(code, "an operator file could not be read");
  } finally {
    if (fd !== undefined) closeSync(fd);
  }
}

export function readBounded(path: string, code: string): string {
  return utf8OrRefuse(readBoundedBytes(path, code), code);
}
const utf8OrRefuse = (b: Uint8Array, code: string) => {
  try { return new TextDecoder("utf-8", { fatal: true, ignoreBOM: false }).decode(b); } catch { throw optionRefusal(code, "an operator file is not UTF-8"); }
};

/** The attestation as JSON only; its facts are checked by the job's preflight (stale-prefix-verify.ts attestationProblem). */
export function parseAttestationFile(text: string): ProtectionAttestation {
  try { return JSON.parse(text) as ProtectionAttestation; } catch { throw optionRefusal("stale_prefix_protection_unproven", "the protection attestation is not JSON"); }
}

/** The read-only verifier client for the configured bucket (verify-only, and execute's inline re-verification). */
function readerFor(key: S3Credentials) {
  const bucket = env("S3X_BUCKET");
  if (!bucket || env("S3X_EXPECT_BUCKET") !== bucket) throw new Error("S3X_BUCKET must equal S3X_EXPECT_BUCKET (bucket guard)");
  const endpoint = env("S3X_ENDPOINT");
  return createS3VersionReader({ bucket, region: env("S3X_REGION", "us-east-1")!, ...(endpoint ? { endpoint } : {}), credentials: key });
}

/** The verifier never touches the privacy authority: any call is a bug, refused loudly. */
const NO_AUTHORITY = new Proxy({ kind: "neon" }, {
  get: (t, k) => (k === "kind" ? t.kind : k === "then" ? undefined : () => { throw optionRefusal("verify_only_no_authority", "the verifier reads no authority"); }),
}) as unknown as ErasureAuthority;

/**
 * `--stale-prefixes-only --verify-only`: READ-ONLY. stdin carries exactly {"reader": <S3 key, the eraser key's
 * shapes>} (ListBucketVersions + GetObjectVersion; no database, no eraser key). Reads the state file, lists and GETs
 * by version id, writes the receipt to --receipt-out (created, never overwritten, mode 0600) or else as one stdout
 * line. Exit 0 when the receipt is PASS, 1 when FAIL. Logs: counts and hashes only.
 */
async function verifyOnlyMain(stale: StalePrefixOptions): Promise<number> {
  const bucket = env("S3X_BUCKET");
  if (!bucket || env("S3X_EXPECT_BUCKET") !== bucket) throw new Error("S3X_BUCKET must equal S3X_EXPECT_BUCKET (bucket guard)");
  const creds = await readStdinCredentials(["reader"], ["reader"]);
  const key = s3Credentials(creds.reader, "reader");
  delete creds.reader;
  const reader = readerFor(key);
  const statePath = env("S3X_STATE", `${process.env.HOME}/.agent-tmp/s3x/state.sqlite`)!;
  const commit = verifierCommit();
  const state = new SyncState(statePath, { obligations: obligationsEnabled() });
  try {
    const job = new ErasureJob(NO_AUTHORITY, state, eraserFor(false, undefined), log, { ...DEFAULT_ERASURE_CONFIG, keyRoot: env("S3X_KEY_ROOT", "96bulls")! });
    log({ message: "start", mode: "stale_prefixes_verify_only", allowlist_size: stale.onlyIdentities.length });
    const receipt = await job.staleprefixVerify({ onlyIdentities: stale.onlyIdentities, reader, bucketSha256: sha256Hex(bucket), target: { stateIdentity: stateIdentity(statePath), verifierCommit: commit } });
    if (stale.receiptOut !== undefined) writeFileSync(stale.receiptOut, `${JSON.stringify(receipt, null, 2)}\n`, { flag: "wx", mode: 0o600 });
    else log({ message: "stale prefix receipt", receipt });
    return receipt.status === "PASS" ? 0 : 1;
  } finally {
    state.close();
  }
}

/** Runs the job and returns its exit status (0, or EXIT_CREDENTIALS_RENEW); a refusal or failure throws. */
export async function main(): Promise<number> {
  const execute = flag("execute");
  const stale = staleprefixOptions(process.argv.slice(2));
  refuseCredentialEnv();
  if (stale?.verifyOnly) return verifyOnlyMain(stale);
  const kind = authorityKind(env("S3X_PRIVACY_AUTHORITY"));
  // Babysitter retention phase (additive): off without --execute; when on, its registry login is one
  // more field of the same stdin credential JSON (never a file, env or argv).
  // The stale-prefixes-only mode never runs the phase (refused above unless BABYSITTER_RETENTION_SWEEP is off).
  const retention = stale ? undefined : phaseSettings(env, execute, env("S3X_KEY_ROOT", "96bulls")!);
  // The stale-prefix execute also needs the read-only reader key for its mandatory inline re-verification.
  const required = [...(execute ? ["databaseUrl", "eraser"] : ["databaseUrl"]), ...(stale && execute ? ["reader"] : []), ...(retention && retention.mode !== "off" ? ["retentionDatabaseUrl"] : [])];
  const creds = await readStdinCredentials(required, ["databaseUrl", "eraser", "retentionDatabaseUrl", ...(stale && execute ? ["reader"] : [])]);
  const readerKey = creds.reader === undefined ? undefined : s3Credentials(creds.reader, "reader");
  delete creds.reader;
  const retentionUrl = creds.retentionDatabaseUrl;
  delete creds.retentionDatabaseUrl;
  const url = authorityUrl(kind, creds.databaseUrl);
  const eraserKey = creds.eraser === undefined ? undefined : s3Credentials(creds.eraser, "eraser");
  const minMs = minRemainingMs(env("S3X_CREDS_MIN_REMAINING_S"));
  if (execute && eraserKey && expiringSoon(eraserKey, minMs, Date.now())) {
    log({ message: "credentials expiring", reason: "credentials_expiring_at_start" });
    process.exit(EXIT_CREDENTIALS_RENEW);
  }
  const eraser = eraserFor(execute, eraserKey);
  delete creds.databaseUrl;
  delete creds.eraser;
  const obligations = obligationsEnabled();
  const statePath = env("S3X_STATE", `${process.env.HOME}/.agent-tmp/s3x/state.sqlite`)!;
  const state = new SyncState(statePath, { obligations });
  const { default: pg } = await import("pg");
  // The password is pinned from the stdin URL: pg can never fall back to ~/.pgpass (a credential on disk).
  const client = pinnedPgClient(pg, url, "databaseUrl", kind, { application_name: `96bulls-${kind}-s3-erasure` });
  await client.connect();
  let stop = false;
  for (const sig of ["SIGINT", "SIGTERM"] as const) process.on(sig, () => void (stop = true));
  // A short-lived eraser key: stop taking new work before it expires (the same stop signal as SIGTERM), then exit 75.
  let credStop = false;
  const credTimer = execute && eraserKey?.expiresAt
    ? setInterval(() => {
        if (!credStop && expiringSoon(eraserKey, minMs, Date.now())) { credStop = true; stop = true; log({ message: "stopping", reason: "credentials_expiring" }); }
      }, 15_000)
    : undefined;
  credTimer?.unref();
  let phase: BabysitterPhase | undefined;
  if (retention && retention.mode !== "off") {
    const pool = new pg.Pool(poolConfig(retentionUrl, env("BABYSITTER_RETENTION_ALLOW_LOOPBACK") === "true"));
    const stoppable = async (ms: number) => { for (let left = ms; left > 0 && !stop; left -= 1000) await new Promise((r) => setTimeout(r, Math.min(1000, left))); };
    phase = await startBabysitterPhase(retention, pool, eraser, log, stoppable);
  }
  try {
    const db = await sessionReadOnly(client as unknown as Queryable);
    const cfg = {
      ...DEFAULT_ERASURE_CONFIG,
      keyRoot: env("S3X_KEY_ROOT", "96bulls")!,
      recognizeLegacy: env("S3X_ERASER_RECOGNIZES_LEGACY", "false") === "true",
      legacyExportErasure: legacyExportErasureEnabled(process.env.S3X_LEGACY_EXPORT_ERASURE_ENABLED),
    };
    // Supabase mode: least-privilege identity first (bulls96_privacy_eraser only; never owner/superuser/BYPASSRLS).
    if (kind === "supabase") await assertEraserIdentity(db);
    const authority = kind === "supabase" ? supabaseAuthority(db) : neonAuthority(db);
    const job = new ErasureJob(authority, state, eraser, log, cfg, Date.now, undefined, () => stop);
    if (stale) {
      // One pass, no sweep, no phase. A dry run's eraser has no existence check: it reports sibling_checked false.
      log({ message: "start", mode: "stale_prefixes_only", execute, authority: kind, obligations, allowlist_size: stale.onlyIdentities.length, ...(stale.max !== undefined ? { max: stale.max } : {}) });
      // Execute: only through the receipt fence (the receipt and the attestation were required by the option parser).
      const fenced = execute
        ? {
            receipt: parseReceipt(boundOperatorFile(stale.verifiedReceipt!, stale.receiptSha256!, "stale_prefix_receipt")),
            attestation: parseAttestationFile(boundOperatorFile(stale.protectionAttestation!, stale.attestationSha256!, "stale_prefix_attestation")),
            bucketSha256: sha256Hex(env("S3X_BUCKET")!),
            reader: readerFor(readerKey!),
            target: { stateIdentity: stateIdentity(statePath), verifierCommit: verifierCommit() },
            approvedAllowlistSha256: stale.approvedAllowlistSha256!,
            approvedMax: stale.approvedMax!,
            ...(isReceiptFence(eraser) ? { fence: eraser } : {}),
          }
        : {};
      const r = await job.staleprefixPass({ execute, onlyIdentities: stale.onlyIdentities, ...(stale.max !== undefined ? { max: stale.max } : {}), ...(isLiveObjectProbe(eraser) ? { probe: eraser } : {}), ...fenced });
      // Returned through the finally below, which cleans up first; an expiry stop exits 75, never 0.
      return stalePassStatus(r, credStop);
    }
    log({ message: "start", execute, authority: kind, recognize_legacy: cfg.recognizeLegacy, obligations });
    // A dry run deletes nothing and marks nothing done: it sweeps and reports what it would erase.
    if (!execute) {
      await job.sweep({ dryRun: true });
      job.progress(true);
      return 0;
    }
    // Babysitter retention phase (additive): runs beside the job until the same stop signal.
    const phaseRun = phase?.loop(() => stop, flag("once"));
    await job.run({ once: flag("once") });
    await phaseRun;
    return runStatus(0, credStop);
  } finally {
    if (credTimer) clearInterval(credTimer);
    await phase?.close();
    state.close();
    await client.end().catch(() => undefined);
  }
}

if (import.meta.main)
  main().then((status) => { if (status !== 0) process.exit(status); }).catch((error) => {
    const code = errorCode(error);
    log({ message: "exit on error", error_code: code });
    // A short-lived eraser key refused before signing (expired): renew and restart, not a crash.
    process.exit(exitStatusForError(error));
  });
