/**
 * One-shot export of the Neon-only legacy tables (conversation_turns,
 * session_links, session_outcomes) to canonical S3 objects.
 *
 *   <secrets on stdin> | bun run migration:legacy-export --plan   [--org=ORG ...|--all-orgs] [--since=ISO] [--until=ISO]
 *   <secrets on stdin> | bun run migration:legacy-export --export [same scoping] [--refresh] [--force] [--max-scopes=N]
 *   <secrets on stdin> | bun run migration:legacy-export --verify [--org=ORG]
 *
 * Secrets come ONLY on stdin, one per line, never argv, env or files:
 *   line 1  the Neon URL of a SELECT-only role (direct, not the pooler)
 *   line 2  S3 access key id       (--export / --verify)
 *   line 3  S3 secret access key   (--export / --verify)
 *   line 4  S3 session token       (optional)
 * Non-secret configuration is flags:
 *   --bucket= --expect-bucket= (must be equal) --region=us-east-1 --sse=aws:kms --kms-key=ARN
 *   --key-root=96bulls --grant-id=neon-legacy-export --state=~/.agent-tmp/legacy-export/state.sqlite
 *   --rows-per-sec=1000 --bytes-per-sec=3000000
 *
 * --plan reads discovery and fingerprints only and writes nothing anywhere.
 * --export writes objects (write-only store; no deletes) and the checkpoint.
 * --verify re-reads Neon and S3 and exits 2 on any mismatch. Output is JSON
 * lines of counts and codes; the verify report names scopes (ids, not content).
 */
import { homedir } from "node:os";
import { mkdirSync } from "node:fs";
import { dirname } from "node:path";
import { zstdDecompressSync } from "node:zlib";
import { createS3Store } from "../../shared/vendor.ts";
import type { Queryable } from "../relayhistory-export.ts";
import { writeOnly } from "../neon-s3-sync/export-session.ts";
import { sessionReadOnly } from "../neon-s3-sync/neon-reader.ts";
import { Throttle } from "../neon-s3-sync/throttle.ts";
import { LEGACY_TABLES, legacyOrgs } from "../neon-s3-sync/legacy-source.ts";
import { Checkpoint, newCounters, runOrg, verifyExported } from "./runner.ts";

const args = process.argv.slice(2);
const flag = (n: string) => args.includes(`--${n}`);
const arg = (n: string) => args.find((a) => a.startsWith(`--${n}=`))?.slice(n.length + 3);
const all = (n: string) => args.filter((a) => a.startsWith(`--${n}=`)).map((a) => a.slice(n.length + 3));
const log = (f: Record<string, unknown>) =>
  process.stdout.write(`${JSON.stringify({ at: new Date().toISOString(), service: "legacy-export", ...f })}\n`);

/** Secrets from stdin only; refuses when stdin is a terminal (no echo, no history). */
export async function readSecrets(text?: string): Promise<{ neonUrl: string; s3?: { accessKeyId: string; secretAccessKey: string; sessionToken?: string } }> {
  if (text === undefined) {
    if (process.stdin.isTTY) throw new Error("pipe the secrets on stdin (see the runbook); a terminal is refused");
    const chunks: Buffer[] = [];
    for await (const c of process.stdin) chunks.push(Buffer.from(c as Uint8Array));
    text = Buffer.concat(chunks).toString("utf8");
  }
  const [neonUrl, accessKeyId, secretAccessKey, sessionToken] = text.split(/\r?\n/u).map((l) => l.trim());
  if (!neonUrl || !/^postgres(ql)?:\/\//u.test(neonUrl)) throw new Error("stdin line 1 must be the Neon URL");
  return {
    neonUrl,
    ...(accessKeyId && secretAccessKey ? { s3: { accessKeyId, secretAccessKey, ...(sessionToken ? { sessionToken } : {}) } } : {}),
  };
}

/** The Neon role must not be able to write any legacy table. */
export async function assertSelectOnly(db: Queryable) {
  for (const s of LEGACY_TABLES) {
    const { rows } = await db.query<{ w: boolean }>(
      `SELECT has_table_privilege(current_user,'sessions.${s.table}','INSERT,UPDATE,DELETE,TRUNCATE') AS w`,
    );
    if (rows[0]?.w !== false) throw Object.assign(new Error("the Neon role can write legacy tables; use the SELECT-only role"), { code: "neon_role_not_read_only" });
  }
}

const iso = (v: string | undefined) => {
  if (v === undefined) return undefined;
  const ms = Date.parse(v);
  if (!Number.isFinite(ms)) throw new Error(`invalid time ${v}`);
  return ms;
};

export async function main() {
  const mode = (["plan", "export", "verify"] as const).filter((m) => flag(m));
  if (mode.length !== 1) throw new Error("choose exactly one of --plan, --export, --verify");
  const secrets = await readSecrets();
  const url = new URL(secrets.neonUrl);
  if (/-pooler\./u.test(url.hostname)) throw new Error("use a direct (unpooled) Neon URL");
  const window = { sinceMs: iso(arg("since")), untilMs: iso(arg("until")) };
  const statePath = arg("state") ?? `${homedir()}/.agent-tmp/legacy-export/state.sqlite`;
  mkdirSync(dirname(statePath), { recursive: true, mode: 0o700 });
  const { default: pg } = await import("pg");
  const client = new pg.Client({ connectionString: secrets.neonUrl, application_name: "96bulls-legacy-export" });
  await client.connect();
  let stop = false;
  for (const sig of ["SIGINT", "SIGTERM"] as const) process.on(sig, () => (stop = true));
  const checkpoint = new Checkpoint(statePath);
  try {
    const db = await sessionReadOnly(client as unknown as Queryable);
    await assertSelectOnly(db);
    const needsS3 = !flag("plan");
    let store;
    if (needsS3) {
      const bucket = arg("bucket");
      if (!bucket || arg("expect-bucket") !== bucket) throw new Error("--bucket and --expect-bucket must be given and equal");
      if (!secrets.s3) throw new Error("stdin lines 2-3 (S3 key) are required for --export/--verify");
      const sse = arg("sse");
      if (sse && sse !== "aws:kms" && sse !== "AES256") throw new Error("--sse must be aws:kms or AES256");
      const kms = arg("kms-key");
      store = writeOnly(
        createS3Store({
          bucket,
          region: arg("region") ?? "us-east-1",
          credentials: secrets.s3,
          ...(sse ? { serverSideEncryption: sse as "aws:kms" | "AES256" } : {}),
          ...(kms ? { sseKmsKeyId: kms } : {}),
        }),
      );
    }
    const decompress = (b: Uint8Array) => new Uint8Array(zstdDecompressSync(b, { maxOutputLength: 256 * 1024 * 1024 }));
    if (flag("verify")) {
      const report = await verifyExported(db, store!, checkpoint, decompress, arg("org"));
      process.stdout.write(`${JSON.stringify({ service: "legacy-export", mode: "verify", ...report })}\n`);
      if (!report.ok) process.exitCode = 2;
      return;
    }
    let orgs = all("org");
    if (flag("all-orgs")) orgs = [...new Set((await Promise.all(LEGACY_TABLES.map((s) => legacyOrgs(db, s.table)))).flat())].sort();
    if (!orgs.length) throw new Error("--org=ORG (repeatable) or --all-orgs is required");
    const throttle = new Throttle({
      rowsPerSec: Number(arg("rows-per-sec") ?? 1000),
      bytesPerSec: Number(arg("bytes-per-sec") ?? 3_000_000),
      slowWindows: [],
      slowFactor: 1,
    });
    const hook = async (rows: number, bytes: number) => {
      if (stop) throw Object.assign(new Error("stopping"), { code: "interrupted" });
      await throttle.take(rows, bytes);
    };
    const exporting = flag("export");
    for (const org of orgs) {
      const counters = newCounters();
      await runOrg(
        {
          db,
          target: { store: store!, keyRoot: arg("key-root") ?? "96bulls", grantId: arg("grant-id") ?? "neon-legacy-export" },
          checkpoint,
          hook,
          log,
          stopped: () => stop,
        },
        org,
        { export: exporting, window, refresh: flag("refresh"), force: flag("force"), ...(arg("max-scopes") ? { maxScopes: Number(arg("max-scopes")) } : {}) },
        counters,
      );
      log({ message: exporting ? "org exported" : "org plan", org, ...counters });
      if (stop) break;
    }
  } finally {
    checkpoint.close();
    await client.end().catch(() => undefined);
  }
}

if (import.meta.main)
  main().catch((error) => {
    log({ message: "exit on error", error_code: String((error as { code?: unknown })?.code ?? (error as Error)?.message ?? "error").slice(0, 120) });
    process.exit(1);
  });
