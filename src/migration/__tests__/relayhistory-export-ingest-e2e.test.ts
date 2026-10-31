/**
 * The export courier end to end against main's REAL ingest routes (merged #43,
 * c6ebf872): AlleyOop's `createCourierManifestRoutes` (owner fence, signed
 * upload identity, provenance, signed work key) and the pipeline chunk hop
 * (`createPipelineRoutes`: plan-entry framing, chunk ledger), on a real fs
 * object store and the real SQL ledger on PGlite with every supabase
 * migration. Only the token verifier is replaced: a test issuer hands out
 * opaque tokens whose verified auth context is what `auth/relayauth.ts`
 * would produce. No network, no real RelayAuth, no credentials.
 */
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import { randomBytes } from "node:crypto";
import { mkdtemp, readdir, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Hono } from "hono";
import { PGlite } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/index.js";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import {
  migrationPairAuthorized,
  migrationRouteAllowed,
  type TransactionalSqlExecutor,
} from "@rhc/lib/pipeline/migration-assertions.ts";
import {
  computeContentSha256,
  createPipelineRoutes,
  objectKey,
  sealRawChunk,
  type ManifestV1,
  objectStoreManifestReader,
  type AppEnv,
  type SqlExecutor,
} from "../../shared/vendor.ts";
import { courierMigrationLedger, createCourierManifestRoutes } from "../../alleyoop/manifests.ts";
import type { DeliveryRecord } from "../relayhistory-export.ts";
import {
  planContract,
  plannedManifestFromRecords,
  runExportUpload,
  type AssertionSource,
  type ExportPlan,
  type UploadIdentity,
} from "../relayhistory-export-upload.ts";

setDefaultTimeout(60_000);

const ORG = "org_test";
const WORKSPACE = "ws_test";
const USER = "user_a";
const ORIGIN = "https://history.example";
const GRANT = `mgr_${"0".repeat(32)}`;
const SUPABASE = join(import.meta.dir, "../../../supabase");

let db: PGlite;
let root: string;

beforeAll(async () => {
  db = new PGlite();
  await db.exec(await readFile(join(SUPABASE, "tests/supabase_shim.sql"), "utf8"));
  for (const file of (await readdir(join(SUPABASE, "migrations"))).filter((f) => f.endsWith(".sql")).sort())
    await db.exec(await readFile(join(SUPABASE, "migrations", file), "utf8"));
  await db.exec("SET ROLE service_role");
});
afterAll(async () => {
  await db?.close();
});
beforeEach(async () => {
  root = await mkdtemp(join(tmpdir(), "96bulls-export-e2e-"));
  await db.exec("TRUNCATE sessions.migration_assertion_uses; TRUNCATE sessions.migration_assertion_chunks; TRUNCATE sessions.pipeline_jobs");
});
afterEach(async () => {
  await rm(root, { recursive: true, force: true });
});

function executor(): TransactionalSqlExecutor {
  const wrap = (q: { query: PGlite["query"] }): SqlExecutor => ({
    query: async (text, params) => ({ rows: (await q.query(text, params)).rows as never[] }),
  });
  return { ...wrap(db), transaction: (work) => db.transaction((tx) => work(wrap(tx))) };
}
const count = async (table: string) => (await db.query<{ n: number }>(`SELECT count(*)::int AS n FROM ${table}`)).rows[0]!.n;

const record = (i: number, session: string): DeliveryRecord => ({
  kind: "session_event",
  origin_id: "feed-epoch-1",
  record_id: `${session}-r${i}`,
  revision: String(10 + i),
  operation: "upsert",
  received_at: "2026-10-01T00:00:00Z",
  payload: { ts_ms: Date.UTC(2026, 9, 1) + i * 1000, text: `line ${i} `.repeat(20) },
});

function exportPlan(): ExportPlan {
  return {
    exportedAt: "2026-10-30T12:00:00.000Z",
    manifests: ["sess-a", "sess-b"].map((session) =>
      plannedManifestFromRecords({
        orgId: ORG, workspaceId: WORKSPACE, source: "claude", sessionId: session, machine: "machine-a",
        sourceUserId: USER, records: [0, 1, 2, 3].map((i) => record(i, session)), maxChunkBytes: 600,
      })!),
  };
}

/** The real routes with a test verifier in front (see the file comment). */
async function realIngest(plan: ExportPlan) {
  const store = createFsStore({ root });
  const raw = { store, provider: "fs" as const, bucket: "raw" };
  const reader = objectStoreManifestReader(store, { provider: "fs", bucket: "raw" });
  const ledger = courierMigrationLedger(executor(), reader);
  const contract = planContract(plan, GRANT);
  const env = {
    PIPELINE_ORG_ID: ORG,
    RELAYHISTORY_ORIGIN: ORIGIN,
    RELAYHISTORY_MIGRATION_GRANTS: `${GRANT}=${contract.planDigest}`,
  };
  const issued = new Map<string, Record<string, unknown>>();
  const app = new Hono<AppEnv>();
  app.use("*", async (c, next) => {
    c.set("correlationId", "e2e");
    const token = (c.req.header("authorization") ?? "").replace(/^Bearer /, "");
    const auth = issued.get(token);
    if (!auth) return c.json({ error: { code: "invalid_token" } }, 401);
    // As requireAuth does after verifying the token.
    if (!migrationRouteAllowed(c.req.method, c.req.path)) return c.json({ error: { code: "migration_assertion_route" } }, 403);
    if (!migrationPairAuthorized(auth as never)) return c.json({ error: { code: "migration_workspace_forbidden" } }, 403);
    c.set("auth", auth as never);
    await next();
  });
  app.route("/v1", createCourierManifestRoutes({ manifests: reader, raw, send: async () => {}, migrationLedger: ledger }));
  app.route("/v1", createPipelineRoutes({
    manifests: () => reader,
    rawStore: () => raw,
    sink: () => ({ record: async () => {}, list: async () => [] }) as never,
    deadLetters: () => ({}) as never,
    send: async () => {},
    migrationLedger: () => ledger,
  }));
  /** The issuer: signs the stored plan's identity for the requested key. */
  const assertions: AssertionSource = {
    async mint(sourceUserId, workspaceId, { key }) {
      const upload = contract.uploads.find((u) => u.key === key) as UploadIdentity;
      if (!upload || upload.source_user_id !== sourceUserId || upload.workspace_id !== workspaceId) throw Object.assign(new Error("not in plan"), { code: "relayauth_http_403" });
      const meta = { tokenClass: "migration_assertion", grantId: GRANT, planDigest: contract.planDigest, sourceUserId, workspaceId, upload, nonce: randomBytes(24).toString("base64url") };
      const token = `e2e.${Buffer.from(JSON.stringify({ meta })).toString("base64url")}.${randomBytes(8).toString("hex")}`;
      issued.set(token, {
        userId: USER, orgId: ORG, workspaceId: WORKSPACE, tokenSubject: "agent_relayhistory_migration", scopes: ["rth:sync"], historyOrigin: ORIGIN,
        claims: { sub: "agent_relayhistory_migration", org: ORG, wks: WORKSPACE, sponsorId: USER, meta: { tokenClass: "migration_assertion", grantId: GRANT, planDigest: contract.planDigest } },
        migration: { jti: `tok_${randomBytes(16).toString("hex")}`, nonce: meta.nonce, grantId: GRANT, planDigest: contract.planDigest, sourceUserId, workspaceId, expiresAt: Math.floor(Date.now() / 1000) + 65, upload },
      });
      return { token, expiresAt: new Date(Date.now() + 60_000), nonce: meta.nonce };
    },
  };
  const control = { loseCommit: 0, commits: 0 };
  const fetcher = (async (input: string, init: RequestInit) => {
    const url = new URL(input);
    const res = await app.request(`${url.pathname}${url.search}`, init, env);
    if (url.pathname === "/v1/pipeline/manifests" && init.method === "POST" && res.status === 202) {
      control.commits += 1;
      // The real route committed; the response never reaches the courier.
      if (control.commits === control.loseCommit) throw new Error("response lost");
    }
    return res;
  }) as unknown as typeof fetch;
  return { store, contract, assertions, fetcher, reader, control };
}

describe("export courier against main's real ingest routes", () => {
  test("a cold export commits: owner = writer = migration:<grant>, signed work key, one job per manifest", async () => {
    const plan = exportPlan();
    const ingest = await realIngest(plan);
    const dir = await mkdtemp(join(tmpdir(), "export-e2e-cp-"));
    const result = await runExportUpload(plan, {
      execute: true, ingestUrl: "https://ingest.example", grantId: GRANT, expectedPlanDigest: ingest.contract.planDigest,
      assertions: ingest.assertions, checkpointPath: join(dir, "cp.json"), fetcher: ingest.fetcher, backoffMs: [1], sleep: async () => {},
    });
    await rm(dir, { recursive: true, force: true });
    expect(result.failed).toEqual([]);
    expect(result.stopped).toBeUndefined();
    expect(result.committed.map((c) => c.workKey)).toEqual(ingest.contract.uploads.map((u) => `${u.key}@${u.entry_sha256}`));
    expect(await count("sessions.migration_assertion_uses")).toBe(2);
    expect(await count("sessions.pipeline_jobs")).toBe(2);
    for (const u of ingest.contract.uploads) {
      const read = await ingest.reader.read(u.key);
      expect(read!.manifest.raw.owner).toBe(`migration:${GRANT}`);
      expect(read!.manifest.writer.delivery_instance_id).toBe(`migration:${GRANT}`);
      expect(read!.manifest.raw.epochs[0]!.opened_by).toBe(`migration:${GRANT}`);
    }
  });

  const run = async (plan: ExportPlan, ingest: Awaited<ReturnType<typeof realIngest>>) => {
    const dir = await mkdtemp(join(tmpdir(), "export-e2e-cp-"));
    try {
      return await runExportUpload(plan, {
        execute: true, ingestUrl: "https://ingest.example", grantId: GRANT, expectedPlanDigest: ingest.contract.planDigest,
        assertions: ingest.assertions, checkpointPath: join(dir, "cp.json"), fetcher: ingest.fetcher, backoffMs: [1, 1, 1], sleep: async () => {},
      });
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  };

  test("the real route accepts the bytes-derived watermark and #51 resume marks", async () => {
    const plan = exportPlan();
    const ingest = await realIngest(plan);
    await run(plan, ingest);
    const read = await ingest.reader.read(ingest.contract.uploads[0]!.key);
    const marker = (read!.manifest.extensions as Record<string, Record<string, unknown>>)["client.relayhistory_export"]!;
    expect(marker.origin_revisions).toEqual({ "feed-epoch-1": 13 });
    const tail = read!.manifest.raw.chunks.at(-1) as unknown as { source_origin: string; source_end: number };
    expect(tail).toMatchObject({ source_origin: "feed-epoch-1", source_end: 13 });
  });

  test("a lost commit response converges: the fresh-assertion retry replays, one manifest, the same work key", async () => {
    const plan = exportPlan();
    const ingest = await realIngest(plan);
    ingest.control.loseCommit = 1;
    const result = await run(plan, ingest);
    expect(result.failed).toEqual([]);
    expect(result.committed.map((c) => c.workKey)).toEqual(ingest.contract.uploads.map((u) => `${u.key}@${u.entry_sha256}`));
    expect(await count("sessions.migration_assertion_uses")).toBe(3); // two assertions spent on the first manifest
    expect(await count("sessions.pipeline_jobs")).toBe(3); // the same work key enqueued twice, idempotent downstream
  });

  test("a live session's manifest at the key: deferred as straddling_session, nothing written", async () => {
    const plan = exportPlan();
    const ingest = await realIngest(plan);
    const key = ingest.contract.uploads[0]!.key;
    // A valid live manifest, written by the session's own client (owner = its machine).
    const m = plan.manifests[0]!;
    const sealed = await sealRawChunk(ingest.store, {
      location: m.location, format: "jsonl", seq: 0, epoch: 0, startOffset: 0,
      source: new TextEncoder().encode('{"type":"user","text":"live"}\n'), traceId: m.traceId,
    });
    const at = m.startedAt;
    const live: ManifestV1 = {
      schema_v: 1, layout: "v1", trace_id: m.traceId, revision: 1, created_at: at, updated_at: at,
      session: { workspace_id: WORKSPACE, machine_id: m.location.machineId, harness: m.location.harness, session_id: m.location.sessionId, started_at: at },
      writer: { delivery_instance_id: "machine-a", plugin_version: "live" },
      raw: {
        owner: "machine-a", state: "open", updated_at: at, format: "jsonl", compression: "zstd", chunk_count: 1,
        raw_bytes: sealed.entry.bytes, stored_bytes: sealed.entry.stored_bytes, content_sha256: await computeContentSha256([sealed.entry]),
        epoch: 0, epochs: [{ epoch: 0, opened_at: at, reason: "initial", first_seq: 0, opened_by: "machine-a" }],
        next_offset: sealed.entry.bytes, chunks: [sealed.entry], scrub: sealed.scrub,
      },
      extensions: { "96bulls": { user_id: USER } },
    } as ManifestV1;
    await ingest.store.put(objectKey(key), new TextEncoder().encode(JSON.stringify(live)), { contentType: "application/json" });
    const result = await run(plan, ingest);
    expect(result.deferred.map((d) => [d.key, d.code])).toEqual([[key, "straddling_session"]]);
    expect(result.committed.map((c) => c.key)).toEqual([ingest.contract.uploads[1]!.key]);
    expect(await count("sessions.pipeline_jobs")).toBe(1);
  });
});
