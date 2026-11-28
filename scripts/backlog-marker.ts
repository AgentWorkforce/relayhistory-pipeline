/**
 * The backlog-ops marker tool (contract: src/session-ingest/marker-publication.ts; run only by scripts/backlog-ops.ts
 * with fixed argv, an allowlisted environment and its secrets on stdin):
 *
 *   bun scripts/backlog-marker.ts dry --bucket <locker> --region <r> --sha <sha> --authority-run <id> --backfill-run <id>
 *       --full-run <id> --reshard-run <id> --org <org> ...
 *   bun scripts/backlog-marker.ts execute ... (the same) --kms-key-arn <arn>
 *       stdin {"dbUrl", "cloudflareToken", "baseline", "ledgerBucket", "bound"} (+ "wave" for execute).
 *       execute is refused unless its --org set is EXACTLY the reviewed enabled pair (backlog-ops.ts requireScopeEnabled; the
 *       lists are empty in this tree, so it is always refused).
 *
 * AWS_* credentials from the environment only. The evidence reads run each statement in its own REPEATABLE READ READ
 * ONLY transaction (scripts/backlog-authority.ts readOnlySql) on their own session; each org's hold reads, marker
 * attempt and guarded publication run on one of up to `--concurrency` sessions (READ COMMITTED; only the marker
 * attempt is READ WRITE). A dry run's sessions are read-only (default_transaction_read_only=on). Output: one JSON line,
 * counts, codes and hashes only; refusals as {"refused": <code>}. Nothing is written to stderr but fixed codes.
 */
import { readFileSync } from "node:fs";
import { parseArgs } from "node:util";
import pg from "pg";
import { createS3Store, type ObjectStore } from "../src/shared/vendor.ts";
import { ownerClientConfig } from "../src/shared/owner-pg-config.ts";
import { AUTHORITY_EVIDENCE, type AuthorityEvidenceSources } from "../src/session-ingest/authority-evidence.ts";
import { BACKLOG_OPS_ENABLED_ORG_HASHES, backlogOpsScope, requireScopeEnabled, type EnabledOrgHashes } from "../src/session-ingest/backlog-ops.ts";
import { evidenceHash } from "../src/session-ingest/authority-evidence.ts";
import { BACKLOG_AUTHORITY_KEY_ROOT, CLOUDFLARE_TOKEN, cloudflareGet } from "../src/session-ingest/backlog-authority.ts";
import {
  MARKER_LOCK_TIMEOUT_MS,
  MARKER_STATEMENT_TIMEOUT_MS,
  MARKER_SQL,
  MARKER_TOOL_REFUSALS,
  MarkerRefusal,
  markerOk,
  markerRun,
  validMarkerBound,
  type MarkerSqlSession,
} from "../src/session-ingest/marker-publication.ts";
import { WAVE_DEFAULT_CONCURRENCY, waveConcurrency, waveOrgList } from "../src/session-ingest/org-wave.ts";
import { readOnlySql } from "./backlog-authority.ts";

const fail = (code: string): never => { throw Object.assign(new Error(code), { code }); };
const RUN_ID = /^[1-9][0-9]{0,19}$/u;

export interface MarkerArgs { command: "dry" | "execute"; bucket: string; region: string; kmsKeyArn?: string; sha: string; orgs: string[];
  runs: { authority: string; backfill: string; full: string; reshard: string }; concurrency: number }
export function parseMarkerArgs(argv: readonly string[]): MarkerArgs {
  const [command, ...rest] = argv;
  if (command !== "dry" && command !== "execute") return fail("usage");
  let values;
  try {
    ({ values } = parseArgs({ args: [...rest], options: { bucket: { type: "string" }, region: { type: "string" }, "kms-key-arn": { type: "string" },
      sha: { type: "string" }, org: { type: "string", multiple: true }, "authority-run": { type: "string" }, "backfill-run": { type: "string" },
      "full-run": { type: "string" }, "reshard-run": { type: "string" }, concurrency: { type: "string" } }, strict: true, allowPositionals: false }));
  } catch { return fail("usage"); }
  if (!/^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/u.test(values.bucket ?? "") || !/^[a-z]{2}(-gov)?-[a-z]+-[0-9]$/u.test(values.region ?? "") ||
      !/^[0-9a-f]{40}$/u.test(values.sha ?? "")) fail("usage");
  if ((command === "execute") !== (values["kms-key-arn"] !== undefined)) fail("usage");
  if (command === "execute" && !/^arn:aws:kms:[a-z0-9-]+:\d{12}:key\/[0-9a-z-]{36,40}$/u.test(values["kms-key-arn"]!)) fail("usage");
  const runs = { authority: values["authority-run"] ?? "", backfill: values["backfill-run"] ?? "", full: values["full-run"] ?? "", reshard: values["reshard-run"] ?? "" };
  if (!Object.values(runs).every((r) => RUN_ID.test(r)) || new Set(Object.values(runs)).size !== 4) fail("usage");
  const c = values.concurrency;
  if (c !== undefined && !/^[1-9][0-9]?$/u.test(c)) fail("usage");
  let orgs: string[];
  try { orgs = waveOrgList(values.org ?? []); } catch (e) { return fail((e as { code: string }).code); }
  return { command, bucket: values.bucket!, region: values.region!, ...(values["kms-key-arn"] ? { kmsKeyArn: values["kms-key-arn"] } : {}), sha: values.sha!,
    orgs, runs, concurrency: waveConcurrency(c === undefined ? WAVE_DEFAULT_CONCURRENCY : Number(c), orgs.length) };
}

export interface MarkerStdin { dbUrl: string; cloudflareToken: string; baseline: unknown; ledgerBucket: string | null; bound: unknown;
  wave?: { id: string; orgs: { orgHash: string; epochHash: string }[] } }
export function markerStdin(line: string, command: MarkerArgs["command"]): MarkerStdin {
  if (line.length > 4 * 1024 * 1024) fail("stdin_too_large");
  let p: unknown;
  try { p = JSON.parse(line); } catch { return fail("stdin_invalid"); }
  const keys = ["dbUrl", "cloudflareToken", "baseline", "ledgerBucket", "bound", ...(command === "execute" ? ["wave"] : [])];
  if (p === null || typeof p !== "object" || Array.isArray(p) || Object.keys(p).length !== keys.length || !keys.every((k) => Object.hasOwn(p as object, k))) fail("stdin_invalid");
  const o = p as Record<string, any>;
  if (typeof o.dbUrl !== "string" || o.dbUrl === "") fail("db_url_required");
  if (typeof o.cloudflareToken !== "string" || !CLOUDFLARE_TOKEN.test(o.cloudflareToken)) fail("cloudflare_token_invalid");
  if (o.ledgerBucket !== null && (typeof o.ledgerBucket !== "string" || !/^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/u.test(o.ledgerBucket))) fail("stdin_invalid");
  if (!validMarkerBound(o.bound)) fail("marker_bound_invalid");
  if (command === "execute") {
    const w = o.wave;
    if (!w || typeof w !== "object" || Array.isArray(w) || Object.keys(w).length !== 2 || typeof w.id !== "string" || !Array.isArray(w.orgs) ||
        !w.orgs.every((x: unknown) => !!x && typeof x === "object" && Object.keys(x as object).length === 2 &&
          typeof (x as Record<string, unknown>).orgHash === "string" && typeof (x as Record<string, unknown>).epochHash === "string"))
      fail("marker_wave_invalid");
  }
  return o as MarkerStdin;
}

/** One pg client as a MarkerSqlSession (header). */
export function pgMarkerSession(client: { query(text: string, params?: unknown[]): Promise<{ rows: Record<string, unknown>[] }> }): MarkerSqlSession {
  const one = async (access: "READ ONLY" | "READ WRITE", text: string, params: readonly unknown[]) => {
    await client.query(`BEGIN ISOLATION LEVEL READ COMMITTED ${access}`);
    try {
      await client.query(`SET LOCAL statement_timeout = ${MARKER_STATEMENT_TIMEOUT_MS}`);
      await client.query(`SET LOCAL lock_timeout = ${MARKER_LOCK_TIMEOUT_MS}`);
      const rows = (await client.query(text, [...params])).rows;
      await client.query("COMMIT");
      return rows;
    } catch (error) {
      await client.query("ROLLBACK").catch(() => {});
      throw error;
    }
  };
  return {
    read: (text, params) => one("READ ONLY", text, params),
    write: (text, params) => one("READ WRITE", text, params),
    async guarded(org, body) {
      await client.query("BEGIN ISOLATION LEVEL READ COMMITTED READ ONLY");
      let result;
      try {
        await client.query(`SET LOCAL statement_timeout = ${MARKER_STATEMENT_TIMEOUT_MS}`);
        await client.query(`SET LOCAL lock_timeout = ${MARKER_LOCK_TIMEOUT_MS}`);
        const lock = MARKER_SQL.lockShared(org);
        await client.query(lock.text, [...lock.params]);
        result = await body(async (text, params) => (await client.query(text, [...params])).rows);
      } catch (error) {
        await client.query("ROLLBACK").catch(() => {});
        throw error;
      }
      try { await client.query("COMMIT"); } catch { throw new MarkerRefusal("marker_guard_lost"); }
      return result;
    },
  };
}

/** A bounded pool: at most `size` sessions, each used by one org at a time. */
export function sessionPool(open: () => Promise<{ session: MarkerSqlSession; close(): Promise<void> }>, size: number) {
  const idle: { session: MarkerSqlSession; close(): Promise<void> }[] = [], all: typeof idle = [];
  const waiting: ((s: (typeof idle)[number]) => void)[] = [];
  // Slots are reserved synchronously, before the open awaits, so concurrent acquires never open more than `size`.
  let reserved = 0;
  const acquire = async () => {
    const s = idle.pop();
    if (s) return s;
    if (reserved < size) {
      reserved++;
      try { const n = await open(); all.push(n); return n; } catch (error) { reserved--; throw error; }
    }
    return new Promise<(typeof idle)[number]>((r) => waiting.push(r));
  };
  const release = (s: (typeof idle)[number]) => { const w = waiting.shift(); if (w) w(s); else idle.push(s); };
  return {
    async withSession<T>(fn: (s: MarkerSqlSession) => Promise<T>): Promise<T> {
      const s = await acquire();
      try { return await fn(s.session); } finally { release(s); }
    },
    async close() { await Promise.all(all.map((s) => s.close().catch(() => {}))); },
  };
}

function awsCredentials(env: NodeJS.ProcessEnv) {
  const accessKeyId = env.AWS_ACCESS_KEY_ID, secretAccessKey = env.AWS_SECRET_ACCESS_KEY;
  if (!accessKeyId || !secretAccessKey) fail("aws_credentials_required");
  return { accessKeyId: accessKeyId!, secretAccessKey: secretAccessKey!, ...(env.AWS_SESSION_TOKEN ? { sessionToken: env.AWS_SESSION_TOKEN } : {}) };
}

export async function main(argv: readonly string[], env: NodeJS.ProcessEnv, deps: { stdin?: () => string; /** TESTS ONLY */ enabledOrgHashes?: EnabledOrgHashes } = {}) {
  const args = parseMarkerArgs(argv);
  // The switch, or every --org inside the reviewed two-hash allow-list (backlog-ops.ts); a refusal names no org.
  if (args.command === "execute") {
    try { requireScopeEnabled(args.orgs.map((o) => evidenceHash("org", o)), deps.enabledOrgHashes ?? BACKLOG_OPS_ENABLED_ORG_HASHES, "marker", "marker_execute_held"); }
    catch { fail("marker_execute_held"); }
  }
  const input = markerStdin((deps.stdin ?? (() => readFileSync(0, "utf8")))().split("\n")[0] ?? "", args.command);
  const dbUrl = input.dbUrl;
  input.dbUrl = "";
  const config = (name: string, readOnly: boolean) => {
    const b = ownerClientConfig(dbUrl, name);
    return { ...b, ...(readOnly ? { options: `${b.options ?? ""} -c default_transaction_read_only=on`.trim() } : {}), keepAlive: true };
  };
  const evidenceClient = new pg.Client(config("96bulls-backlog-marker-evidence", true));
  evidenceClient.on("error", () => {});
  await evidenceClient.connect();
  const pool = sessionPool(async () => {
    const c = new pg.Client(config("96bulls-backlog-marker", args.command === "dry"));
    c.on("error", () => {});
    await c.connect();
    return { session: pgMarkerSession(c), close: () => c.end() };
  }, args.concurrency);
  try {
    const sources: AuthorityEvidenceSources = {
      cfGet: cloudflareGet(AUTHORITY_EVIDENCE.accountId, input.cloudflareToken), sql: readOnlySql(evidenceClient),
      nowMs: () => Date.now(), sleep: (ms) => new Promise((r) => setTimeout(r, ms)),
    };
    const store = createS3Store({ bucket: args.bucket, region: args.region, credentials: awsCredentials(env),
      ...(args.kmsKeyArn ? { serverSideEncryption: "aws:kms" as const, sseKmsKeyId: args.kmsKeyArn } : {}) }) as unknown as ObjectStore;
    const summary = await markerRun({
      mode: args.command === "execute" ? "execute" : "dry_run", orgs: args.orgs, sha: args.sha,
      scopeId: backlogOpsScope({ BACKLOG_OPS_ORGS: args.orgs.join(",") }).id, runs: args.runs, sources, baseline: input.baseline as never,
      bound: input.bound as never, ...(input.wave ? { wave: input.wave } : {}), store, keyRoot: BACKLOG_AUTHORITY_KEY_ROOT, bucket: args.bucket,
      ledgerBucket: input.ledgerBucket, withSession: pool.withSession, now: () => Date.now(), concurrency: args.concurrency,
    });
    return { summary, ok: markerOk(summary) };
  } finally {
    await pool.close();
    await evidenceClient.end().catch(() => {});
  }
}

if (import.meta.main) {
  main(process.argv.slice(2), process.env)
    .then(({ summary, ok }) => { process.stdout.write(JSON.stringify(summary) + "\n"); process.exitCode = ok ? 0 : 1; })
    .catch((error) => {
      // A closed set only: never a message (a store, pg or fetch error's text can name an org or a key).
      const c = (error as { code?: unknown })?.code ?? (error as Error)?.message;
      process.stdout.write(JSON.stringify({ refused: typeof c === "string" && (MARKER_TOOL_REFUSALS as readonly string[]).includes(c) ? c : "internal_error" }) + "\n");
      process.exitCode = 1;
    });
}
