/**
 * `neon-s3-erasure` CLI: the transcript bucket's only deleter.
 *
 *   <credentials JSON> | bun run migration:neon-s3-erasure [--execute] [--once]
 *
 * Credentials come ONLY through stdin, held in RAM (zero-Neon checklist E3; AGENTS.md: nothing credential-like
 * on disk). One JSON object:
 *   {"databaseUrl": "<read-only-capable URL of the privacy authority>",
 *    "eraser": {"accessKeyId": "...", "secretAccessKey": "...", "sessionToken": "..."}}   // eraser: --execute only
 * from 1Password items "sessions-prod transcript eraser" and the authority's database login. The former
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
 */
import type { Queryable } from "../relayhistory-export.ts";
import { sessionReadOnly } from "../neon-s3-sync/neon-reader.ts";
import { SyncState } from "../neon-s3-sync/state.ts";
import { errorCode } from "../neon-s3-sync/sync.ts";
import { eraserDatabaseUrl, pinnedPgClient, readStdinCredentials, refuseCredentialEnv, s3Credentials, type S3Credentials } from "../stdin-credentials.ts";
import { assertEraserIdentity, neonAuthority, supabaseAuthority, type ErasureAuthorityKind } from "./authority.ts";
import { createS3Eraser, type Eraser, type PagedEraser } from "./eraser.ts";
import { DEFAULT_ERASURE_CONFIG, ErasureJob } from "./job.ts";

/**
 * S3X_OBLIGATIONS (default OFF) gates the new exporter-obligation protocol, its schema and genesis bootstrap. The
 * strict canonical paged eraser runs in BOTH modes (safety hardening, unflagged); off it runs without obligations.
 */
export function obligationsEnabled(e: (k: string) => string | undefined = (k) => process.env[k]): boolean {
  return e("S3X_OBLIGATIONS") === "on";
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

export async function main() {
  const execute = flag("execute");
  refuseCredentialEnv();
  const kind = authorityKind(env("S3X_PRIVACY_AUTHORITY"));
  const creds = await readStdinCredentials(execute ? ["databaseUrl", "eraser"] : ["databaseUrl"], ["databaseUrl", "eraser"]);
  const url = authorityUrl(kind, creds.databaseUrl);
  const eraser = eraserFor(execute, creds.eraser === undefined ? undefined : s3Credentials(creds.eraser, "eraser"));
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
  try {
    const db = await sessionReadOnly(client as unknown as Queryable);
    const cfg = {
      ...DEFAULT_ERASURE_CONFIG,
      keyRoot: env("S3X_KEY_ROOT", "96bulls")!,
      recognizeLegacy: env("S3X_ERASER_RECOGNIZES_LEGACY", "false") === "true",
    };
    // Supabase mode: least-privilege identity first (bulls96_privacy_eraser only; never owner/superuser/BYPASSRLS).
    if (kind === "supabase") await assertEraserIdentity(db);
    const authority = kind === "supabase" ? supabaseAuthority(db) : neonAuthority(db);
    const job = new ErasureJob(authority, state, eraser, log, cfg, Date.now, undefined, () => stop);
    log({ message: "start", execute, authority: kind, recognize_legacy: cfg.recognizeLegacy, obligations });
    // A dry run changes nothing: it sweeps and reports what it would erase.
    if (!execute) {
      await job.sweep({ dryRun: true });
      job.progress(true);
      return;
    }
    await job.run({ once: flag("once") });
  } finally {
    state.close();
    await client.end().catch(() => undefined);
  }
}

if (import.meta.main)
  main().catch((error) => {
    log({ message: "exit on error", error_code: errorCode(error) });
    process.exit(1);
  });
