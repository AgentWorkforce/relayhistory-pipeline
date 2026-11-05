/** The actual ingest entry inside workerd, with its restricted PostgreSQL
 * ledger through Hyperdrive and the real S3/SigV4 client against an in-memory
 * HTTP boundary. This proves runtime composition; it is not live AWS/KMS proof. */
import { env } from "cloudflare:workers";
import { createExecutionContext, waitOnExecutionContext } from "cloudflare:test";
import { createHash } from "node:crypto";
import { afterEach, describe, expect, test, vi } from "vitest";
import worker, { type IngestWorkerEnv } from "../../../src/session-ingest/worker.ts";
import { encodeIdentity, signRequest, IDENTITY_HEADER, SIGNATURE_HEADER } from "../../../src/session-ingest/app.ts";
import { connectPg } from "../src/index.ts";

const E = env as unknown as { TEST_INGEST_PG: Hyperdrive; TEST_PG_ADMIN: Hyperdrive };
const KEY = "workerd-test-signing-key";
const BUCKET = "ingest-workerd-test";
const HOST = `${BUCKET}.s3.us-east-1.amazonaws.com`;
const KMS = "arn:aws:kms:us-east-1:000000000000:key/workerd-test";
const encoder = new TextEncoder();
type Stored = { bytes: Uint8Array; headers: Headers; etag: string };

function s3Boundary() {
  const objects = new Map<string, Stored>();
  const writes: string[] = [];
  let unavailable = false;
  let seq = 0;
  vi.stubGlobal("fetch", async (input: RequestInfo | URL, init?: RequestInit) => {
    const request = new Request(input, init);
    const url = new URL(request.url);
    if (url.hostname !== HOST) throw new Error("Unexpected external HTTP request");
    expect(request.headers.get("authorization")).toMatch(/^AWS4-HMAC-SHA256 /);
    const key = decodeURIComponent(url.pathname.slice(1));
    const current = objects.get(key);
    if (unavailable) return new Response("Unavailable", { status: 503 });
    if (request.method === "PUT") {
      expect(request.headers.get("x-amz-server-side-encryption")).toBe("aws:kms");
      expect(request.headers.get("x-amz-server-side-encryption-aws-kms-key-id")).toBe(KMS);
      if (request.headers.get("if-none-match") === "*" && current)
        return new Response("Already exists", { status: 412 });
      const match = request.headers.get("if-match");
      if (match && current?.etag !== match) return new Response("Precondition failed", { status: 412 });
      const bytes = new Uint8Array(await request.arrayBuffer());
      const etag = `"v${++seq}"`;
      const headers = new Headers({ etag, "content-length": String(bytes.length), "last-modified": new Date().toUTCString() });
      request.headers.forEach((value, name) => { if (name.startsWith("x-amz-meta-") || name === "content-type") headers.set(name, value); });
      objects.set(key, { bytes, headers, etag }); writes.push(key);
      return new Response(null, { status: 200, headers: { etag } });
    }
    if (request.method === "GET" || request.method === "HEAD") {
      if (!current) return new Response("<Error><Code>NoSuchKey</Code></Error>", { status: 404 });
      return new Response(request.method === "HEAD" ? null : current.bytes.slice(), { status: 200, headers: current.headers });
    }
    throw new Error(`Unexpected S3 method ${request.method}`);
  });
  return { objects, writes, outage: (value: boolean) => { unavailable = value; } };
}

function tenant() {
  const id = crypto.randomUUID();
  return { orgId: `ingest-org-${id}`, workspaceId: `ingest-ws-${id}`, userId: `ingest-user-${id}`, scopes: ["rth:sync"] };
}
function batch(t: ReturnType<typeof tenant>, sessions = ["s-one"], batchId = crypto.randomUUID()) {
  return { schema_version: 1, origin_id: "workerd-origin", batch_id: batchId, job_id: "workerd-job", generation: 1,
    destination_id: "history", instance_id: "workerd", mapping_version: "relayhistory-delivery-v1",
    account_id: "relayhistory:" + createHash("sha256").update(JSON.stringify([t.orgId, t.workspaceId])).digest("hex"),
    records: sessions.map((session, i) => ({ schema_version: 1, origin_id: "workerd-origin", record_id: `event-${i}`,
      revision_id: `event-${i}:r1`, revision: 1, kind: "session_event", source: "codex", session_id: session,
      operation: "upsert", payload: { role: "user", text: `workerd prompt ${i}`, ts_ms: 1761955200000 } })) };
}
function settings(over: Partial<IngestWorkerEnv> = {}): IngestWorkerEnv {
  return { HYPERDRIVE: E.TEST_INGEST_PG, INGEST_FANOUT: { send: async () => {}, sendBatch: async () => {} },
    S3_ACCESS_KEY_ID: "workerd-test-key", S3_SECRET_ACCESS_KEY: "workerd-test-secret", INGEST_SIGNING_KEY: KEY,
    INGEST_S3_BUCKET: BUCKET, INGEST_S3_REGION: "us-east-1", INGEST_KEY_ROOT: "96bulls", INGEST_KMS_KEY_ARN: KMS,
    INGEST_REPLICA_MODE: "off", ...over };
}
async function send(t: ReturnType<typeof tenant>, b: ReturnType<typeof batch>, e = settings(), signed = true) {
  const bytes = encoder.encode(JSON.stringify({ protocolVersion: 1, batch: b }));
  const identity = encodeIdentity(t);
  const ctx = createExecutionContext();
  try {
    const response = await worker.fetch(new Request("https://ingest.test/v1/delivery/batches", { method: "POST", body: bytes,
      headers: { "content-type": "application/json", [IDENTITY_HEADER]: identity,
        ...(signed ? { [SIGNATURE_HEADER]: signRequest(KEY, Math.floor(Date.now() / 1000), "POST", "/v1/delivery/batches", identity, bytes) } : {}) } }), e, ctx);
    return { status: response.status, body: await response.json() as Record<string, unknown> };
  } finally { await waitOnExecutionContext(ctx); }
}
async function revisions(t: ReturnType<typeof tenant>) {
  const db = await connectPg(E.TEST_PG_ADMIN.connectionString);
  try {
    return Number((await db.query("SELECT count(*)::int AS n FROM sessions.ingest_record_revisions WHERE org_id=$1 AND workspace_id=$2", [t.orgId, t.workspaceId])).rows[0].n);
  } finally { await db.end(); }
}

afterEach(() => { vi.unstubAllGlobals(); });
describe("session ingest entry inside workerd", () => {
  test("is dormant without bindings and rejects unsigned identities before ledger/S3 access", async () => {
    const s3 = s3Boundary(); const t = tenant();
    expect((await send(t, batch(t), {})).status).toBe(503);
    expect((await send(t, batch(t), settings(), false)).status).toBe(401);
    expect(s3.writes).toHaveLength(0); expect(await revisions(t)).toBe(0);
  });
  test("acknowledges signed batches only after durable S3 writes and committed ledger state, then replays without writes", async () => {
    const s3 = s3Boundary(); const t = tenant(); const b = batch(t);
    const first = await send(t, b);
    expect(first.status, JSON.stringify(first.body)).toBe(200); expect(await revisions(t)).toBe(1);
    const manifests = [...s3.objects.entries()].filter(([key]) => key.endsWith("/manifest.json"));
    expect(manifests).toHaveLength(1);
    expect(JSON.parse(new TextDecoder().decode(manifests[0][1].bytes)).writer.plugin_version).toBe("96bulls-session-ingest/1");
    const writes = s3.writes.length;
    const replay = await send(t, b);
    expect(replay).toEqual(first); expect(s3.writes).toHaveLength(writes);
  });
  test("an S3 outage never acknowledges or commits a revision; retry writes it", async () => {
    const s3 = s3Boundary(); const t = tenant(); const b = batch(t);
    s3.outage(true);
    expect((await send(t, b)).status).toBe(503); expect(await revisions(t)).toBe(0);
    s3.outage(false);
    expect((await send(t, b)).status).toBe(200); expect(await revisions(t)).toBe(1);
  });
  test("staged batches are durable before acknowledgement and become committed revisions only after fan-out", async () => {
    const s3 = s3Boundary(); const t = tenant(); const b = batch(t, ["one", "two"]);
    const queued: unknown[] = [];
    const e = settings({ INGEST_INLINE_MAX_SESSIONS: "1", INGEST_FANOUT: {
      send: async body => { queued.push(body); }, sendBatch: async messages => { queued.push(...messages.map(m => m.body)); },
    } });
    expect((await send(t, b, e)).status).toBe(200);
    expect([...s3.objects.keys()].some(key => key.endsWith(".staged.ndjson.zst"))).toBe(true);
    expect(queued.length).toBeGreaterThan(0); expect(await revisions(t)).toBe(0);
    const ack = vi.fn(); const retry = vi.fn();
    await worker.queue({ messages: queued.map(body => ({ body, ack, retry })) }, e);
    expect(retry).not.toHaveBeenCalled(); expect(ack).toHaveBeenCalledTimes(queued.length);
    expect(await revisions(t)).toBe(2);
  });
});
