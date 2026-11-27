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
 */
import type { Queryable } from "../relayhistory-export.ts";
import { sweepMode } from "../../session-ingest/babysitter-retention.ts";
import { sessionReadOnly } from "../neon-s3-sync/neon-reader.ts";
import { SyncState, obligationsFlag } from "../neon-s3-sync/state.ts";
import { errorCode } from "../neon-s3-sync/sync.ts";
import { eraserDatabaseUrl, pinnedPgClient, readStdinCredentials, refuseCredentialEnv, s3Credentials, type S3Credentials } from "../stdin-credentials.ts";
import { EXIT_CREDENTIALS_RENEW, expiringSoon, minRemainingMs } from "../short-lived-credentials.ts";
import { assertEraserIdentity, neonAuthority, supabaseAuthority, type ErasureAuthorityKind } from "./authority.ts";
import { createS3Eraser, isLiveObjectProbe, type Eraser, type PagedEraser } from "./eraser.ts";
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
  return lower.includes("stale") || lower.includes("prefix") || lower.includes("identit") || bare.startsWith("max") || bare.startsWith("only");
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
export function staleprefixOptions(args: readonly string[], e: (k: string) => string | undefined = (k) => process.env[k]): { execute: boolean; max?: number; onlyIdentities: string[] } | undefined {
  if (!args.includes("--stale-prefixes-only")) {
    if (args.some((a) => a === "--max" || a.startsWith("--max=")))
      throw optionRefusal("stale_prefix_max_without_mode", "--max applies only with --stale-prefixes-only");
    if (args.some((a) => a === "--only-identity" || a.startsWith("--only-identity=")))
      throw optionRefusal("stale_prefix_only_identity_without_mode", "--only-identity applies only with --stale-prefixes-only");
    if (args.some(nearMissArgument))
      throw optionRefusal("stale_prefix_mode_flag_refused", "an argument resembles the stale-prefix switch or --max but is not exactly --stale-prefixes-only: refused before the full job could run");
    return undefined;
  }
  let execute = false, max: number | undefined, seen = 0;
  const onlyIdentities: string[] = [];
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
    } else throw optionRefusal("stale_prefix_mode_flag_refused", "--stale-prefixes-only accepts only --execute, --once, --max <n> and --only-identity <sha256>");
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
  return max === undefined ? { execute, onlyIdentities } : { execute, max, onlyIdentities };
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
export function stalePassStatus(r: { ok: boolean; refused?: string }, credStop: boolean): number {
  if (r.refused) throw optionRefusal(r.refused, "another eraser holds an unexpired lease: the stale-prefix pass did not start");
  if (credStop) return EXIT_CREDENTIALS_RENEW;
  if (!r.ok) throw optionRefusal("stale_prefix_pass_stopped", "the stale-prefix pass stopped at a failed erase");
  return 0;
}

/** The process exit status after a run: EXIT_CREDENTIALS_RENEW whenever the run stopped on an expiring key. */
export function runStatus(status: number, credStop: boolean): number {
  return credStop ? EXIT_CREDENTIALS_RENEW : status;
}

/** Runs the job and returns its exit status (0, or EXIT_CREDENTIALS_RENEW); a refusal or failure throws. */
export async function main(): Promise<number> {
  const execute = flag("execute");
  const stale = staleprefixOptions(process.argv.slice(2));
  refuseCredentialEnv();
  const kind = authorityKind(env("S3X_PRIVACY_AUTHORITY"));
  // Babysitter retention phase (additive): off without --execute; when on, its registry login is one
  // more field of the same stdin credential JSON (never a file, env or argv).
  // The stale-prefixes-only mode never runs the phase (refused above unless BABYSITTER_RETENTION_SWEEP is off).
  const retention = stale ? undefined : phaseSettings(env, execute, env("S3X_KEY_ROOT", "96bulls")!);
  const required = [...(execute ? ["databaseUrl", "eraser"] : ["databaseUrl"]), ...(retention && retention.mode !== "off" ? ["retentionDatabaseUrl"] : [])];
  const creds = await readStdinCredentials(required, ["databaseUrl", "eraser", "retentionDatabaseUrl"]);
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
  const state = new SyncState(env("S3X_STATE", `${process.env.HOME}/.agent-tmp/s3x/state.sqlite`)!, { obligations });
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
      const r = await job.staleprefixPass({ execute, onlyIdentities: stale.onlyIdentities, ...(stale.max !== undefined ? { max: stale.max } : {}), ...(isLiveObjectProbe(eraser) ? { probe: eraser } : {}) });
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
    log({ message: "exit on error", error_code: errorCode(error) });
    process.exit(1);
  });
