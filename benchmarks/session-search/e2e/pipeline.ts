/**
 * The real path from courier to pipeline jobs, in process: runCourierOnce, buildIngestApp
 * (scrubbing chunk hop included), pollPostgresStage. Only the database (PGlite with every
 * migration) and the raw store (a temp fs store) are substituted, as in ingest-auth.test.ts.
 */
import { createSign, generateKeyPairSync } from "node:crypto";
import { migrationProfile, runMigrations } from "../../../src/hoopdb/migrate.ts";
import { mkdtemp, readdir, readFile, rm, writeFile } from "node:fs/promises";
import { DatabaseSync } from "node:sqlite";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import { runCourierOnce } from "../../../src/alleyoop/courier.ts";
import { buildIngestApp } from "../../../src/alleyoop/ingest.ts";
import { sqlPipelineEventSink } from "../../../src/hoopdb/sink.ts";
import {
  objectKey, pipelineQueueDeps, pollPostgresStage, postgresDeadLetterStore, postgresPipelineQueue, readAll,
  type Bindings, type ManifestV1, type SqlExecutor,
} from "../../../src/shared/vendor.ts";

const ROOT = join(import.meta.dir, "../../..");
const ORIGIN = "https://history.example.test";
const ISSUER = "https://relayauth.example.test";
const JWKS_URL = "https://relayauth.example.test/.well-known/jwks.json";
const KID = "search-proof";
const INGEST = "http://ingest.test";

export interface SessionInput { id: string; lines: string[] }
export interface Pointer { orgId: string; manifest: ManifestV1; key: string; keyPrefix: string }

export async function startPipeline(orgIdForStack: string, onIndexed: (p: Pointer) => Promise<void>) {
  const { privateKey, publicKey } = generateKeyPairSync("rsa", { modulusLength: 2048 });
  const jwks = { keys: [{ ...publicKey.export({ format: "jwk" }), kid: KID, alg: "RS256" }] };
  const sign = (claims: Record<string, unknown>) => {
    const b64 = (v: unknown) => Buffer.from(JSON.stringify(v)).toString("base64url");
    const head = b64({ alg: "RS256", kid: KID, typ: "JWT" });
    const body = b64(claims);
    return `${head}.${body}.${createSign("RSA-SHA256").update(`${head}.${body}`).end().sign(privateKey, "base64url")}`;
  };
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  // The real migrator, with the explicit database-mode contract: PGlite is a local
  // plain Postgres whose superuser runs everything (shim and Worker bootstrap included,
  // owner-as-runtime, which only local mode allows).
  await runMigrations(
    {
      async query<R>(text: string, params?: unknown[]) {
        if (params?.length) return (await db.query<R>(text, params)) as { rows: R[] };
        return { rows: ((await db.exec(text)).at(-1)?.rows ?? []) as R[] };
      },
    },
    { profile: migrationProfile({ HOOPDB_DB_MODE: "local", HOOPDB_SUPABASE_SHIM: "true", HOOPDB_RUNTIME_ROLE: "postgres" }) },
  );
  const wrap = (q: { query: PGlite["query"] }): SqlExecutor => ({
    query: async (text, params) => ({ rows: (await q.query(text, params)).rows as never[] }),
  });
  const sql = { ...wrap(db), transaction: <T>(work: (tx: SqlExecutor) => Promise<T>) => db.transaction((tx) => work(wrap(tx))) };
  const root = await mkdtemp(join(tmpdir(), "96bulls-search-proof-"));
  Object.assign(process.env, {
    LOCKER_BUCKET: "raw", TENANT_ORG_ID: orgIdForStack, TENANT_ORG_IDS: `${orgIdForStack},org-e2e-b`, PIPELINE_ORG_FROM_PREFIX: "true", AUTH_ISSUER: ISSUER, RELAYAUTH_JWKS_URL: JWKS_URL,
    RELAYHISTORY_ORIGIN: ORIGIN, ALLOW_INTERIM_RELAYAUTH_BEARER: "true",
  });
  const store = createFsStore({ root });
  const { app, env } = await buildIngestApp({ db: sql, raw: { store, provider: "fs", bucket: "raw" } as never });
  const refusals: string[] = [];
  const realFetch = globalThis.fetch;
  globalThis.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = String(input);
    if (url === JWKS_URL) return Response.json(jwks);
    if (url.startsWith(INGEST)) {
      const u = new URL(url);
      const res = await app.request(`${u.pathname}${u.search}`, init, env);
      if (res.status >= 400) refusals.push(((await res.clone().json().catch(() => ({}))) as { error?: { code?: string } }).error?.code ?? `http_${res.status}`);
      return res;
    }
    return realFetch(input, init);
  }) as typeof fetch;

  const deps = pipelineQueueDeps({ PIPELINE_ORG_FROM_PREFIX: "true" } as Bindings, {
    queue: postgresPipelineQueue(sql), sink: sqlPipelineEventSink(sql), deadLetters: postgresDeadLetterStore(sql),
    index: {
      upsertRawPointer: async (pointer: { manifestKey: string }, ctx: { orgId: string }) => {
        const manifest = JSON.parse(new TextDecoder().decode(await readAll(await store.get(objectKey(pointer.manifestKey))))) as ManifestV1;
        await onIndexed({ orgId: ctx.orgId, manifest, key: pointer.manifestKey, keyPrefix: pointer.manifestKey.slice(0, pointer.manifestKey.indexOf("v1/")) });
      },
    } as never,
  });

  async function courier(orgId: string, workspaceId: string, sessions: SessionInput[], dir?: string, claim: { machineId?: string } | null = {}) {
    const work = dir ?? await mkdtemp(join(tmpdir(), "96bulls-courier-"));
    const dbPath = join(work, "history.db");
    const hist = new DatabaseSync(dbPath);
    hist.exec("CREATE TABLE IF NOT EXISTS sessions (session_id TEXT, source TEXT, raw_path TEXT, first_activity_ms INTEGER, last_activity_ms INTEGER, cwd TEXT, agent_version TEXT)");
    hist.exec("DELETE FROM sessions");
    const started = Date.parse("2026-10-30T10:00:00.000Z");
    for (const s of sessions) {
      const rawPath = join(work, `${s.id}.jsonl`);
      await writeFile(rawPath, s.lines.map((l) => `${l}\n`).join(""));
      hist.prepare("INSERT INTO sessions VALUES (?, ?, ?, ?, ?, ?, ?)").run(s.id, "claude", rawPath, started, started + 1000, null, "1.0.0");
    }
    hist.close();
    const now = () => Math.floor(Date.now() / 1000);
    const machineId = `${workspaceId}-m1`;
    // A signed machine claim is required for uploads; null omits it for the refusal test.
    const signedMachine = claim === null ? {} : { machineId: claim.machineId ?? machineId };
    const token = () => sign({
      sub: "agent_relayhistory_assertion", org: orgId, wks: workspaceId, sponsorId: "u1", scopes: ["rth:sync"], token_type: "access",
      iss: ISSUER, aud: [ORIGIN], iat: now(), exp: now() + 600, meta: { ...signedMachine, tokenClass: "relayhistory_assertion" },
    });
    const result = await runCourierOnce({
      dbPath, statePath: join(work, "state.json"), ingestUrl: INGEST, token: { get: async () => token(), invalidate: () => false },
      orgId, workspaceId, userId: "u1", machineId, harnesses: ["claude"], closeAfterMinutes: 1_000_000, startMode: "all",
    });
    const landed = await pollPostgresStage({ sql, stage: "object-landed", deps });
    const indexed = await pollPostgresStage({ sql, stage: "session-indexed", deps });
    return { result, landed, indexed, dir: work };
  }

  return {
    courier, store, db, refusals, env,
    readToken: (orgId: string, workspaceId: string) => {
      const now = Math.floor(Date.now() / 1000);
      // This ingest has no read-token class, so the proof borrows the assertion shape.
      return sign({ sub: "agent_relayhistory_assertion", org: orgId, wks: workspaceId, sponsorId: "u1", scopes: ["rth:sync"], token_type: "access", iss: ISSUER, aud: [ORIGIN], iat: now, exp: now + 600, meta: { machineId: `${workspaceId}-m1`, tokenClass: "relayhistory_assertion" } });
    },
    stop: async () => { globalThis.fetch = realFetch; await db.close(); await rm(root, { recursive: true, force: true }); },
  };
}
