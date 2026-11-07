/**
 * The DEPLOYED turns composition (packages/session-read-worker turnsDeps -> timingDeps ->
 * canonicalSnapshot over the raw S3 store) with only the database runner and the S3 HTTP
 * endpoint faked. Proves the never-in-Neon objects half survives the Worker composition and
 * that the pointer's manifest_etag binds the read to the exact manifest version.
 */
import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import type { PGlite } from "@electric-sql/pglite";
import { database, read } from "../../session-brief/__tests__/harness.ts";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createHash } from "node:crypto";
import { zstdCompressSync } from "node:zlib";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import { sha256Hex } from "../../../packages/object-store/src/crypto.ts";
import { computeContentSha256, manifestKey, objectKey, rawChunkKey, readAll, type ChunkEntry, type ManifestV1 } from "../../../packages/object-store/src/index.ts";
import { turnsDeps } from "../../../packages/session-read-worker/src/index.ts";
import { createReadApi, type VerifiedIdentity } from "../app.ts";
import { TENANT_SETTINGS_SQL, type TenantRunner } from "../liveness.ts";

const AT = "2026-11-07T10:00:00.000Z";
const location = { prefix: "96bulls/org_1", workspaceId: "ws_1", machineId: "srv_bb", harness: "claude", sessionId: "sess_1", startDate: "2026-11-07" };
const identity: VerifiedIdentity = { userId: "user_1", orgId: "org_1", workspaceId: "ws_1", scopes: ["rth:read"] };
let root: string;
let store: ReturnType<typeof createFsStore>;
let manifest: ManifestV1;
let pointerRow: Record<string, unknown>;
const etagOf = (bytes: Uint8Array) => `"${createHash("md5").update(bytes).digest("hex")}"`;

async function writeManifest(m: ManifestV1) {
  const bytes = new TextEncoder().encode(JSON.stringify(m));
  await store.put(objectKey(manifestKey(location)), bytes, {});
  // session_index stores the normalized (unquoted) ETag, as the reader compares it.
  return etagOf(bytes).replace(/"/g, "");
}

beforeEach(async () => {
  root = await mkdtemp(join(tmpdir(), "turns-composition-"));
  store = createFsStore({ root });
  const raw = new TextEncoder().encode(JSON.stringify({
    kind: "session_event", origin_id: "origin", record_id: "r1", revision: "1", operation: "upsert", received_at: AT,
    payload: { ts_ms: Date.parse(AT), text: "hello", role: "user", message_id: "m1" },
  }) + "\n");
  const stored = new Uint8Array(zstdCompressSync(raw)), sha = await sha256Hex(raw);
  const chunk: ChunkEntry = {
    seq: 0, epoch: 0, start_offset: 0, bytes: raw.byteLength, stored_bytes: stored.byteLength, sha256: sha, sha256_8: sha.slice(0, 8),
    stored_sha256: await sha256Hex(stored), stored_raw_bytes: raw.byteLength, raw_sha256: sha, compression: "zstd", records: 1,
  };
  manifest = {
    schema_v: 1, layout: "v1", trace_id: "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18", revision: 1, created_at: AT, updated_at: AT,
    session: { workspace_id: "ws_1", machine_id: "srv_bb", harness: "claude", session_id: "sess_1", started_at: AT },
    writer: { delivery_instance_id: "srv_bb", plugin_version: "test" },
    raw: {
      owner: "srv_bb", state: "open", updated_at: AT, format: "ndjson", compression: "zstd", chunk_count: 1, raw_bytes: raw.byteLength,
      stored_bytes: stored.byteLength, content_sha256: await computeContentSha256([chunk]), epoch: 0,
      epochs: [{ epoch: 0, opened_at: AT, reason: "initial", first_seq: 0, opened_by: "srv_bb" }],
      next_offset: raw.byteLength, chunks: [chunk], scrub: { ruleset: "relayhistory-scrub", version: 1 },
    },
    extensions: { "96bulls": { user_id: "user_1", canonical_session_id: "sess_1" } },
  } as ManifestV1;
  await store.put(rawChunkKey(location, 0, 0, sha, "ndjson"), stored, {});
  const etag = await writeManifest(manifest);
  pointerRow = {
    org_id: "org_1", workspace_id: "ws_1", harness: "claude", session_id: "sess_1", machine_id: "srv_bb", user_id: "user_1",
    storage_provider: "s3", bucket: "bucket", manifest_key: manifestKey(location), manifest_etag: etag,
    content_sha256: "\\x" + manifest.raw.content_sha256, raw_format: "ndjson", revision: 1, deleted_at: null,
  };
});
afterEach(async () => {
  await rm(root, { recursive: true, force: true });
});

/** S3 path-style GET/HEAD over the fs store, with an MD5 ETag of the current bytes. */
const s3: typeof fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
  const url = new URL(typeof input === "string" ? input : input instanceof URL ? input.href : input.url);
  const method = (init?.method ?? (input instanceof Request ? input.method : "GET")).toUpperCase();
  const key = decodeURIComponent(url.pathname.replace(/^\/bucket\//, ""));
  let bytes: Uint8Array;
  try {
    bytes = await readAll(await store.get(objectKey(key)));
  } catch {
    return new Response("", { status: 404 });
  }
  const etag = etagOf(bytes), modified = new Date(AT).toUTCString();
  const range = new Headers(init?.headers ?? (input instanceof Request ? input.headers : undefined)).get("range");
  const m = range && /^bytes=(\d+)-(\d*)$/.exec(range);
  if (m && method !== "HEAD") {
    const start = Number(m[1]), end = m[2] ? Math.min(Number(m[2]), bytes.byteLength - 1) : bytes.byteLength - 1;
    const part = bytes.slice(start, end + 1);
    return new Response(part, { status: 206, headers: { etag, "content-length": String(part.byteLength), "content-range": `bytes ${start}-${end}/${bytes.byteLength}`, "last-modified": modified } });
  }
  const headers = { etag, "content-length": String(bytes.byteLength), "last-modified": modified };
  return new Response(method === "HEAD" ? null : bytes, { status: 200, headers });
}) as never;

/** The database runner: answers only with the columns the SQL actually selects. */
function runner(neonFree: boolean): TenantRunner {
  return (async (_org: string, _ws: string, work: (q: { query: (t: string, p?: unknown[]) => Promise<{ rows: unknown[] }> }) => Promise<unknown>) =>
    work({
      async query(text: string) {
        if (text.includes("session_neon_free_for")) return { rows: [{ free: neonFree }] };
        if (text.includes("GROUP BY p.harness")) return { rows: [{ harness: "claude", pointers: 1, live: 1 }] };
        if (text.includes("p.manifest_key")) {
          const row = Object.fromEntries(Object.entries(pointerRow).filter(([k]) => new RegExp(`\\bp\\.${k}\\b`).test(text)));
          return { rows: [row] };
        }
        return { rows: [] };
      },
    })) as never;
}

function api(neonFree: boolean) {
  const env = {
    PIPELINE_RAW_S3_BUCKET: "bucket", PIPELINE_RAW_S3_ENDPOINT: "https://s3.fake.test", PIPELINE_RAW_S3_REGION: "us-east-1",
    PIPELINE_RAW_S3_ACCESS_KEY_ID: "AKIDEXAMPLE", PIPELINE_RAW_S3_SECRET_ACCESS_KEY: "secret",
    PIPELINE_KEY_PREFIX: "96bulls", PIPELINE_ORG_FROM_PREFIX: "true",
  } as never;
  const turns = turnsDeps(env, runner(neonFree), s3);
  if (!turns) throw new Error("turnsDeps not configured");
  const app = createReadApi({
    verify: async () => identity,
    ch: { database: "x", async command() {}, async insert() {}, async select() { throw new Error("no ClickHouse"); } },
    liveness: { async erased() { return new Set(); }, async livePointerSessions() { return new Set(); }, async sessionStart() { return { pointers: 0, live: 0, createdAtMs: null }; } },
    completeSinceMs: 0,
    directoryWorkspaces: new Set(["ws_1"]),
    turns,
  });
  return () => app.request("/v1/sessions/sess_1/turns?workspace=ws_1", { headers: { authorization: "Bearer t" } });
}
const reason = async (r: Response) => ((await r.json()) as any).error?.reason;

test("control: a live-only post-seal session with the database proof is served through the real composition", async () => {
  const r = await api(true)();
  expect([r.status, r.status === 200 ? null : await reason(r)]).toEqual([200, null]);
});

test("(1) a post-seal session whose manifest has a Neon backfill epoch, database proof true -> 501 turns_unknown", async () => {
  // A real Neon export manifest: the signed export marker and the backfill epoch its grant opened.
  manifest.raw.chunks = manifest.raw.chunks.map((c) => ({ ...c, epoch: 1 }));
  manifest.raw.epoch = 1;
  manifest.raw.epochs = [
    { epoch: 0, opened_at: AT, reason: "initial", first_seq: 0, opened_by: "migration:g1" },
    { epoch: 1, opened_at: AT, reason: "backfill", first_seq: 0, opened_by: "migration:g1" },
  ] as never;
  manifest.raw.content_sha256 = await computeContentSha256(manifest.raw.chunks);
  pointerRow.content_sha256 = "\\x" + manifest.raw.content_sha256;
  (manifest.extensions as Record<string, unknown>)["client.relayhistory_export"] = {
    version: 1, source: "neon.delivery_records", grant_id: "g1", plan_digest: "e".repeat(64), source_user_id: "user_1",
  };
  pointerRow.manifest_etag = await writeManifest(manifest);
  const r = await api(true)();
  expect([r.status, await reason(r)]).toEqual([501, "turns_unknown"]);
});

test("(2) the manifest replaced at the same key while the pointer names the old etag -> refused", async () => {
  // Same raw hash and revision, different manifest bytes (metadata): a new ETag.
  await writeManifest({ ...manifest, updated_at: "2026-11-07T11:00:00.000Z" } as ManifestV1);
  const r = await api(true)();
  expect(r.status).toBe(501);
  expect(await reason(r)).toBe("turns_canonical_manifest_mismatch");
});

/**
 * The same deployed composition over a REAL database: every migration (046000's
 * session_neon_free_for / session_analysis_neon_free, the census and the frozen seal), the
 * read role's bootstrap, and the worker's own SQL (SESSION_POINTERS_SQL, NEON_FREE_FOR_READ_SQL)
 * executed as bulls96_read_api under the tenant settings, exactly as poolTenantRunner does.
 * Only the S3 HTTP endpoint is a fake.
 */
describe("real database: the worker composition with the SQL proof", () => {
  let db: PGlite;
  const runnerOver = (pg: PGlite): TenantRunner =>
    (async (orgId: string, workspaceId: string, work: (q: { query: (t: string, p?: unknown[]) => Promise<{ rows: unknown[] }> }) => Promise<unknown>) => {
      await pg.exec("BEGIN READ ONLY");
      try {
        await pg.query(TENANT_SETTINGS_SQL, [orgId, workspaceId]);
        await pg.exec("SET LOCAL ROLE bulls96_read_api");
        // PGlite is one connection: the worker's parallel reads are serialized here.
        let chain: Promise<unknown> = Promise.resolve();
        const result = await work({
          query: (text: string, params?: unknown[]) => {
            const next = chain.then(() => pg.query(text, params as never));
            chain = next.catch(() => {});
            return next.then((r) => ({ rows: r.rows }));
          },
        });
        await pg.exec("COMMIT");
        return result;
      } catch (error) {
        await pg.exec("ROLLBACK");
        throw error;
      }
    }) as never;
  const insertPointer = (etag: string, createdAgo = "0 seconds") =>
    db.query(
      `INSERT INTO sessions.session_index
         (org_id, workspace_id, harness, session_id, machine_id, user_id, created_at, last_activity_at,
          storage_provider, bucket, manifest_key, manifest_etag, content_sha256, raw_format, trace_id, revision, title)
       VALUES ('org_1', 'ws_1', 'claude', 'sess_1', 'srv_bb', 'user_1', now() - $3::interval, now(), 's3', 'bucket', $1, $2,
               decode($4, 'hex'), 'ndjson', '019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18', 1, 't')`,
      [manifestKey(location), etag, createdAgo, manifest.raw.content_sha256],
    );
  const seal = async () => {
    await db.query(
      `INSERT INTO sessions.ingest_frozen_source_completeness
         (source_identity_sha256, control_epoch, privacy_baseline_sha256, org_count, record_rows, manifest_sha256, neon_snapshot, sealed_at)
       VALUES (repeat('a',64), repeat('b',64), repeat('c',64), 1, 0, repeat('d',64), '{}'::jsonb, now() - interval '1 hour')`,
    );
    await db.query("INSERT INTO sessions.session_neon_census_seal (census_rows, census_sha256, neon_snapshot) VALUES (0, repeat('e',64), '{}'::jsonb)");
  };
  const realApi = () => {
    const env = {
      PIPELINE_RAW_S3_BUCKET: "bucket", PIPELINE_RAW_S3_ENDPOINT: "https://s3.fake.test", PIPELINE_RAW_S3_REGION: "us-east-1",
      PIPELINE_RAW_S3_ACCESS_KEY_ID: "AKIDEXAMPLE", PIPELINE_RAW_S3_SECRET_ACCESS_KEY: "secret",
      PIPELINE_KEY_PREFIX: "96bulls", PIPELINE_ORG_FROM_PREFIX: "true",
    } as never;
    const turns = turnsDeps(env, runnerOver(db), s3);
    if (!turns) throw new Error("turnsDeps not configured");
    const app = createReadApi({
      verify: async () => identity,
      ch: { database: "x", async command() {}, async insert() {}, async select() { throw new Error("no ClickHouse"); } },
      liveness: { async erased() { return new Set(); }, async livePointerSessions() { return new Set(); }, async sessionStart() { return { pointers: 0, live: 0, createdAtMs: null }; } },
      completeSinceMs: 0,
      directoryWorkspaces: new Set(["ws_1"]),
      turns,
    });
    return app.request("/v1/sessions/sess_1/turns?workspace=ws_1", { headers: { authorization: "Bearer t" } });
  };

  beforeEach(async () => {
    db = await database();
    await db.exec(read("supabase/bootstrap/session_read_api_role.sql").split("\n").filter((l) => !l.startsWith("\\")).join("\n"));
  });
  afterEach(async () => {
    await db.close();
  });

  test("a fresh never-in-Neon session (sealed copy + census, no marker, current etag) is admitted", async () => {
    await seal();
    await insertPointer(String(pointerRow.manifest_etag));
    const r = await realApi();
    expect([r.status, r.status === 200 ? null : await reason(r)]).toEqual([200, null]);
  });

  test("without the census seal the SQL proof is false: turns_unknown", async () => {
    await db.query(
      `INSERT INTO sessions.ingest_frozen_source_completeness
         (source_identity_sha256, control_epoch, privacy_baseline_sha256, org_count, record_rows, manifest_sha256, neon_snapshot, sealed_at)
       VALUES (repeat('a',64), repeat('b',64), repeat('c',64), 1, 0, repeat('d',64), '{}'::jsonb, now() - interval '1 hour')`,
    );
    await insertPointer(String(pointerRow.manifest_etag));
    const r = await realApi();
    expect([r.status, await reason(r)]).toEqual([501, "turns_unknown"]);
  });

  test("#301 (1): an old turns-only Neon session (census row) whose first SP pointer is after the seal: turns_unknown", async () => {
    await seal();
    await db.query("INSERT INTO sessions.session_neon_census (org_id, workspace_id, session_id) VALUES ('org_1', 'ws_old', 'sess_1')");
    await insertPointer(String(pointerRow.manifest_etag));
    const r = await realApi();
    expect([r.status, await reason(r)]).toEqual([501, "turns_unknown"]);
  });

  test("an export marker (backfill epoch) refuses even with the SQL proof true", async () => {
    await seal();
    manifest.raw.chunks = manifest.raw.chunks.map((c) => ({ ...c, epoch: 1 }));
    manifest.raw.epoch = 1;
    manifest.raw.epochs = [
      { epoch: 0, opened_at: AT, reason: "initial", first_seq: 0, opened_by: "migration:g1" },
      { epoch: 1, opened_at: AT, reason: "backfill", first_seq: 0, opened_by: "migration:g1" },
    ] as never;
    manifest.raw.content_sha256 = await computeContentSha256(manifest.raw.chunks);
    (manifest.extensions as Record<string, unknown>)["client.relayhistory_export"] = {
      version: 1, source: "neon.delivery_records", grant_id: "g1", plan_digest: "e".repeat(64), source_user_id: "user_1",
    };
    await insertPointer(await writeManifest(manifest));
    const r = await realApi();
    expect([r.status, await reason(r)]).toEqual([501, "turns_unknown"]);
  });

  test("a stale etag (manifest replaced at the same key) refuses", async () => {
    await seal();
    await insertPointer(String(pointerRow.manifest_etag));
    await writeManifest({ ...manifest, updated_at: "2026-11-07T11:00:00.000Z" } as ManifestV1);
    const r = await realApi();
    expect([r.status, await reason(r)]).toEqual([501, "turns_canonical_manifest_mismatch"]);
  });
});
