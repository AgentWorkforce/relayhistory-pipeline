import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Hono } from "hono";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import {
  computeContentSha256,
  manifestKey,
  objectKey,
  objectStoreManifestReader,
  sealRawChunk,
  type AppEnv,
  type ManifestV1,
  type SessionLocation,
  type StageMessage,
} from "../../shared/vendor.ts";
import { createCourierManifestRoutes } from "../manifests.ts";

const ORG = "org_test";
const WORKSPACE = "ws_test";
const MACHINE = "machine-a";
const SESSION = "session-a";
const TRACE = "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18";
const UUID = "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c19";
const AT = "2026-10-29T10:00:00.000Z";
const TELEMETRY_KEY = "test-telemetry-key-at-least-32-bytes-long";

let root: string;

beforeEach(async () => {
  root = await mkdtemp(join(tmpdir(), "96bulls-manifests-"));
});

afterEach(async () => {
  await rm(root, { recursive: true, force: true });
});

async function fixture() {
  const store = createFsStore({ root });
  const raw = { store, provider: "fs" as const, bucket: "raw" };
  const reader = objectStoreManifestReader(store, { provider: "fs", bucket: "raw" });
  const location: SessionLocation = {
    workspaceId: WORKSPACE,
    machineId: MACHINE,
    harness: "claude",
    sessionId: SESSION,
    startDate: "2026-10-29",
  };
  const sealed = await sealRawChunk(store, {
    location,
    format: "jsonl",
    seq: 0,
    epoch: 0,
    startOffset: 0,
    source: new TextEncoder().encode('{"type":"user","text":"hello"}\n'),
    traceId: TRACE,
  });
  const build = async (revision: number, writer = MACHINE): Promise<ManifestV1> => ({
    schema_v: 1,
    layout: "v1",
    trace_id: TRACE,
    revision,
    created_at: AT,
    updated_at: new Date(Date.parse(AT) + revision).toISOString(),
    session: {
      workspace_id: WORKSPACE,
      machine_id: MACHINE,
      harness: "claude",
      session_id: SESSION,
      started_at: AT,
      ...(revision > 1 ? { ended_at: AT } : {}),
    },
    writer: { delivery_instance_id: writer, plugin_version: "test" },
    raw: {
      owner: MACHINE,
      state: revision > 1 ? "closed" : "open",
      updated_at: AT,
      format: "jsonl",
      compression: "zstd",
      chunk_count: 1,
      raw_bytes: sealed.entry.bytes,
      stored_bytes: sealed.entry.stored_bytes,
      content_sha256: await computeContentSha256([sealed.entry]),
      epoch: 0,
      epochs: [{ epoch: 0, opened_at: AT, reason: "initial", first_seq: 0, opened_by: MACHINE }],
      next_offset: sealed.entry.bytes,
      chunks: [sealed.entry],
      scrub: sealed.scrub,
    },
    extensions: { "96bulls": { session_uuid: UUID, session_uuid_source: "minted" } },
  });
  const sent: StageMessage[] = [];
  const routes = createCourierManifestRoutes({
    manifests: reader,
    raw,
    send: async (_env, _stage, message) => void sent.push(message),
  });
  const app = new Hono<AppEnv>();
  app.use("*", async (c, next) => {
    c.set("correlationId", "test");
    c.set("auth", {
      userId: "user",
      orgId: ORG,
      workspaceId: WORKSPACE,
      tokenSubject: "agent_relayhistory_assertion",
      scopes: ["rth:sync"],
      claims: {},
      historyOrigin: "https://history.example",
    });
    await next();
  });
  app.route("/v1", routes);
  const env = {
    PIPELINE_ORG_ID: ORG,
    RELAYHISTORY_ORIGIN: "https://history.example",
    PIPELINE_TELEMETRY_ID_KEY: TELEMETRY_KEY,
  };
  const post = (body: unknown) => app.request("/v1/pipeline/manifests", {
    method: "POST",
    headers: { "content-type": "application/json", "x-machine-id": MACHINE, "x-session-id": SESSION },
    body: JSON.stringify(body),
  }, env);
  return { store, reader, location, build, sent, app, env, post };
}

describe("AlleyOop courier manifest routes", () => {
  test("creates a server-validated manifest with identity and keyed telemetry metadata", async () => {
    const f = await fixture();
    const key = manifestKey(f.location);
    const manifest = await f.build(1);
    const response = await f.post({ key, manifest });
    expect(response.status).toBe(202);
    const body = await response.json() as { version: { eTag?: string } };
    expect(body.version.eTag).toBeTruthy();
    const head = await f.store.head(objectKey(key));
    expect(head?.metadata).toMatchObject({
      "session-uuid": UUID,
      "trace-id": TRACE,
      revision: "1",
    });
    expect(head?.metadata["machine-token"]).toMatch(/^h1_[0-9a-f]{32}$/);
    expect(head?.metadata["session-token"]).toMatch(/^h1_[0-9a-f]{32}$/);
    expect(f.sent).toHaveLength(1);

    const again = await f.post({ key, manifest });
    expect(again.status).toBe(202);
    expect(f.sent).toHaveLength(2);
  });

  test("uses compare-and-swap and writer continuity for later revisions", async () => {
    const f = await fixture();
    const key = manifestKey(f.location);
    const first = await f.post({ key, manifest: await f.build(1) });
    const firstBody = await first.json() as { version: { eTag?: string } };

    expect((await f.post({ key, manifest: await f.build(2) })).status).toBe(409);
    expect((await f.post({ key, manifest: await f.build(2, "machine-b"), previousVersion: firstBody.version })).status).toBe(409);
    const update = await f.post({ key, manifest: await f.build(2), previousVersion: firstBody.version });
    expect(update.status).toBe(202);
    const stored = await f.reader.read(key);
    expect(stored?.manifest.revision).toBe(2);
  });

  test("checks ownership and server-sealed chunks before writing", async () => {
    const f = await fixture();
    const manifest = await f.build(1);
    const foreign = manifestKey({ ...f.location, workspaceId: "ws_other" });
    expect((await f.post({ key: foreign, manifest: { ...manifest, session: { ...manifest.session, workspace_id: "ws_other" } } })).status).toBe(403);
    expect(await f.store.head(objectKey(foreign))).toBeUndefined();

    const key = manifestKey(f.location);
    const unsealed = { ...manifest, raw: { ...manifest.raw, scrub: undefined } };
    expect((await f.post({ key, manifest: unsealed })).status).toBe(409);
    expect(await f.store.head(objectKey(key))).toBeUndefined();
  });

  test("returns a version only to the matching machine and writer", async () => {
    const f = await fixture();
    const key = manifestKey(f.location);
    await f.post({ key, manifest: await f.build(1) });
    const found = await f.app.request(`/v1/pipeline/manifests/status?key=${encodeURIComponent(key)}&machine=${MACHINE}&writer=${MACHINE}`, {}, f.env);
    expect(found.status).toBe(200);
    expect((await found.json() as { version: { eTag?: string } }).version.eTag).toBeTruthy();
    const wrong = await f.app.request(`/v1/pipeline/manifests/status?key=${encodeURIComponent(key)}&machine=machine-b&writer=machine-b`, {}, f.env);
    expect(wrong.status).toBe(403);
  });
});
