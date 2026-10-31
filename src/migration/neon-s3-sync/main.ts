/**
 * Continuous Neon to S3 transcript export (96Bulls layout), CLI.
 *
 *   bun run migration:neon-s3-sync [--execute] [--once] [--max-groups=N]
 *
 * Without `--execute` it is a dry run: it reads Neon and builds every chunk and
 * manifest but stores nothing (a discarding store). With `--execute` it
 * writes, and only to `S3X_BUCKET` when that equals `S3X_EXPECT_BUCKET`.
 *
 * Configuration (environment; secrets only through files, never argv):
 *   NEON_URL_FILE        file holding the read-only Neon URL (e.g. a <(...) fd)
 *   S3X_CREDS_FILE       writer/exporter key: {"accessKeyId","secretAccessKey","sessionToken"?}
 *                        (else AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_SESSION_TOKEN)
 *   S3X_ERASER_CREDS_FILE  the eraser key, same shape: the only principal allowed to delete
 *                        (required with --execute; erasure is part of the contract)
 *   S3X_BUCKET, S3X_EXPECT_BUCKET, S3X_REGION (us-east-1), S3X_ENDPOINT (MinIO)
 *   S3X_SSE              unset for sessions-prod (bucket default SSE-KMS; its policy refuses
 *                        `aws:kms` without the key id). aws:kms | AES256 for other stores.
 *   S3X_KEY_ROOT         96bulls
 *   S3X_GRANT_ID         neon-s3-sync
 *   S3X_STATE            SQLite watermark path
 *   S3X_ROWS_PER_SEC     1500      S3X_BYTES_PER_SEC  4000000
 *   S3X_SLOW_WINDOWS     2026-10-31T15:00Z/2026-11-01T00:00Z   S3X_SLOW_FACTOR 0.5
 *   S3X_RECENT_DAYS      14
 *   S3X_LANES            2  (parallel Neon connections; Neon is ~80 ms away, so latency, not load, bounds a lane)
 *
 * Logs are JSON lines of counts and codes only: never content, ids of
 * sessions, keys or credentials.
 */
import { readFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { createS3Store, objectKey, ObjectStoreError, type ObjectStore } from "../../shared/vendor.ts";
import type { Queryable } from "../relayhistory-export.ts";
import { splitStore } from "./export-session.ts";
import { sessionReadOnly } from "./neon-reader.ts";
import { SyncState } from "./state.ts";
import { DEFAULT_SYNC_CONFIG, Syncer, errorCode } from "./sync.ts";
import { Throttle, parseWindows } from "./throttle.ts";

const env = (k: string, d?: string) => process.env[k]?.trim() || d;
const flag = (name: string) => process.argv.includes(`--${name}`);
const arg = (name: string) => process.argv.find((a) => a.startsWith(`--${name}=`))?.slice(name.length + 3);

function log(fields: Record<string, unknown>) {
  process.stdout.write(`${JSON.stringify({ at: new Date().toISOString(), service: "neon-s3-sync", ...fields })}\n`);
}

/** A store that keeps nothing: a dry run computes every object and writes none. */
export function discardingStore(): ObjectStore {
  const notFound = (key: string) => new ObjectStoreError("not_found", "dry-run", `${key} not stored`);
  return {
    backend: "dry-run",
    head: async () => undefined,
    get: async (key) => {
      throw notFound(key);
    },
    getObject: async (key) => {
      throw notFound(key);
    },
    list: async () => ({ items: [] }),
    presignGet: async () => {
      throw new ObjectStoreError("unsupported", "dry-run", "no presign in a dry run");
    },
    put: async (key, body) => ({
      key: objectKey(key),
      version: {},
      contentSha256: createHash("sha256").update(body).digest("hex"),
      size: body.byteLength,
      created: true,
    }),
    delete: async () => undefined,
  };
}

function credentialsFrom(file: string | undefined, fallbackEnv: boolean) {
  if (file) {
    const c = JSON.parse(readFileSync(file, "utf8")) as Record<string, string>;
    if (!c.accessKeyId || !c.secretAccessKey) throw new Error("a credentials file lacks accessKeyId/secretAccessKey");
    return { accessKeyId: c.accessKeyId, secretAccessKey: c.secretAccessKey, ...(c.sessionToken ? { sessionToken: c.sessionToken } : {}) };
  }
  if (!fallbackEnv) throw new Error("S3X_ERASER_CREDS_FILE is required with --execute");
  const accessKeyId = env("AWS_ACCESS_KEY_ID");
  const secretAccessKey = env("AWS_SECRET_ACCESS_KEY");
  if (!accessKeyId || !secretAccessKey) throw new Error("no S3 credentials (S3X_CREDS_FILE or AWS_* env)");
  const sessionToken = env("AWS_SESSION_TOKEN");
  return { accessKeyId, secretAccessKey, ...(sessionToken ? { sessionToken } : {}) };
}

function storeFromEnv(execute: boolean): ObjectStore {
  if (!execute) return discardingStore();
  const bucket = env("S3X_BUCKET");
  if (!bucket) throw new Error("S3X_BUCKET is required with --execute");
  if (env("S3X_EXPECT_BUCKET") !== bucket) throw new Error("S3X_EXPECT_BUCKET must equal S3X_BUCKET (write guard)");
  const sse = env("S3X_SSE");
  if (sse && sse !== "aws:kms" && sse !== "AES256") throw new Error("S3X_SSE must be aws:kms or AES256");
  const endpoint = env("S3X_ENDPOINT");
  const store = (credentials: ReturnType<typeof credentialsFrom>) =>
    createS3Store({
      bucket,
      region: env("S3X_REGION", "us-east-1")!,
      ...(endpoint ? { endpoint } : {}),
      credentials,
      ...(sse ? { serverSideEncryption: sse as "aws:kms" | "AES256" } : {}),
    });
  return splitStore(
    store(credentialsFrom(env("S3X_CREDS_FILE"), true)),
    store(credentialsFrom(env("S3X_ERASER_CREDS_FILE"), false)),
  );
}

export async function main() {
  const execute = flag("execute");
  const urlFile = env("NEON_URL_FILE");
  const url = urlFile ? readFileSync(urlFile, "utf8").trim() : env("NEON_URL");
  if (!url) throw new Error("NEON_URL_FILE (or NEON_URL) is required");
  const store = storeFromEnv(execute);
  const statePath = env("S3X_STATE", `${process.env.HOME}/.agent-tmp/s3x/state${execute ? "" : "-dry"}.sqlite`)!;
  const state = new SyncState(statePath);
  const throttle = new Throttle({
    rowsPerSec: Number(env("S3X_ROWS_PER_SEC", "1500")),
    bytesPerSec: Number(env("S3X_BYTES_PER_SEC", "4000000")),
    slowWindows: parseWindows(env("S3X_SLOW_WINDOWS")),
    slowFactor: Number(env("S3X_SLOW_FACTOR", "0.5")),
  });
  if (/-pooler\./.test(new URL(url).hostname))
    throw new Error("use a direct (unpooled) Neon URL: session settings must not leak through a pooler");
  const lanes = Number(env("S3X_LANES", "2"));
  if (!Number.isInteger(lanes) || lanes < 1 || lanes > 4) throw new Error("S3X_LANES must be 1-4");
  const { default: pg } = await import("pg");
  const clients = Array.from({ length: lanes }, () => new pg.Client({ connectionString: url, application_name: "96bulls-neon-s3-sync" }));
  for (const c of clients) await c.connect();
  let stop = false;
  for (const sig of ["SIGINT", "SIGTERM"] as const)
    process.on(sig, () => {
      stop = true;
      log({ message: "stopping", signal: sig });
    });
  try {
    const dbs: Queryable[] = [];
    for (const c of clients) dbs.push(await sessionReadOnly(c as unknown as Queryable));
    const [db, ...extra] = dbs as [Queryable, ...Queryable[]];
    const syncer = new Syncer(
      db,
      state,
      {
        store,
        keyRoot: env("S3X_KEY_ROOT", "96bulls")!,
        grantId: env("S3X_GRANT_ID", "neon-s3-sync")!,
      },
      throttle,
      log,
      { ...DEFAULT_SYNC_CONFIG, recentMs: Number(env("S3X_RECENT_DAYS", "14")) * 86_400_000 },
      () => stop,
      undefined,
      extra,
    );
    log({ message: "start", execute, backend: store.backend, lanes, rates: throttle.rates() });
    const maxGroups = arg("max-groups");
    await syncer.run({ once: flag("once"), ...(maxGroups ? { maxGroups: Number(maxGroups) } : {}) });
  } finally {
    state.close();
    for (const c of clients) await c.end().catch(() => undefined);
  }
}

if (import.meta.main)
  main().catch((error) => {
    log({ message: "exit on error", error_code: errorCode(error) });
    process.exit(1);
  });
