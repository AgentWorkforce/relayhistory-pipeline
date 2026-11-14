/** READ_API_CANONICAL_FETCH_CONCURRENCY (service-canonical.ts): self-host canonical chunk-fetch concurrency. */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { zstdCompressSync } from "node:zlib";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import { sha256Hex } from "../../../packages/object-store/src/crypto.ts";
import { computeContentSha256, manifestKey, objectKey, rawChunkKey, type ChunkEntry, type ObjectStore } from "../../../packages/object-store/src/index.ts";
import { CANONICAL_FETCH_CONCURRENCY_MAX, CANONICAL_FETCH_CONCURRENCY_MIN, canonicalFetchConcurrency, serviceCanonicalDeps } from "../service-canonical.ts";

const concurrency = (v: string | undefined) => canonicalFetchConcurrency(v === undefined ? {} : { READ_API_CANONICAL_FETCH_CONCURRENCY: v });
const MESSAGE = "session read api: READ_API_CANONICAL_FETCH_CONCURRENCY must be a whole number in 1..16";

describe("canonicalFetchConcurrency", () => {
  test("unset or empty: 1, the sequential read", () => {
    expect(CANONICAL_FETCH_CONCURRENCY_MIN).toBe(1);
    expect(CANONICAL_FETCH_CONCURRENCY_MAX).toBe(16);
    expect(concurrency(undefined)).toBe(1);
    expect(concurrency("")).toBe(1);
  });

  test.each([["1", 1], ["8", 8], ["16", 16]])("%p is accepted as %p", (raw, want) => {
    expect(concurrency(raw)).toBe(want);
  });

  test.each(["0", "17", "abc", "8.5", "-1", "08", "+8", " 8", "8 ", "1e1", "0x8", "100", " ", "Infinity", "NaN"])(
    "%j fails startup, naming the variable and range without echoing the value",
    (raw) => {
      let message = "";
      try { concurrency(raw); } catch (e) { message = (e as Error).message; }
      expect(message).toBe(MESSAGE);
    },
  );
});

const AT = "2026-11-01T10:00:00.000Z";
const identity = { orgId: "org_1", workspaceId: "ws_1", source: "claude", sessionId: "sess_1" };
const location = { prefix: "96bulls/org_1", workspaceId: "ws_1", machineId: "srv_bb", harness: "claude", sessionId: "sess_1", startDate: "2026-11-01" };
const env = {
  LOCKER_BUCKET: "bucket",
  LOCKER_ENDPOINT: "http://127.0.0.1:9",
  LOCKER_REGION: "us-east-1",
  LOCKER_ACCESS_KEY_ID: "synthetic-access-key",
  LOCKER_SECRET_ACCESS_KEY: "synthetic-secret-key",
  PIPELINE_KEY_PREFIX: "96bulls",
  PIPELINE_ORG_FROM_PREFIX: "true",
};

describe("serviceCanonicalDeps wires the concurrency into every per-request canonical read", () => {
  let root: string, store: ObjectStore, pointerRow: Record<string, unknown>;
  const CHUNKS = 24;
  beforeAll(async () => {
    root = await mkdtemp(join(tmpdir(), "canonical-fetch-concurrency-"));
    store = createFsStore({ root });
    const chunks: ChunkEntry[] = [];
    let offset = 0;
    for (let seq = 0; seq < CHUNKS; seq++) {
      const record = { kind: "session_event", origin_id: "origin", record_id: `r${seq}`, revision: "1", operation: "upsert", received_at: AT,
        payload: { ts_ms: Date.parse(AT) + seq, text: `turn ${seq}`, role: "assistant", message_id: `msg-${seq}` } };
      const raw = new TextEncoder().encode(JSON.stringify(record) + "\n"), stored = new Uint8Array(zstdCompressSync(raw)), sha = await sha256Hex(raw);
      chunks.push({ seq, epoch: 0, start_offset: offset, bytes: raw.byteLength, stored_bytes: stored.byteLength, sha256: sha, sha256_8: sha.slice(0, 8),
        stored_sha256: await sha256Hex(stored), stored_raw_bytes: raw.byteLength, raw_sha256: sha, compression: "zstd", records: 1 });
      await store.put(rawChunkKey(location, seq, offset, sha, "ndjson"), stored, {});
      offset += raw.byteLength;
    }
    const manifest = {
      schema_v: 1, layout: "v1", trace_id: "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18", revision: 1, created_at: AT, updated_at: AT,
      session: { workspace_id: "ws_1", machine_id: "srv_bb", harness: "claude", session_id: "sess_1", started_at: AT },
      writer: { delivery_instance_id: "srv_bb", plugin_version: "test" },
      raw: { owner: "srv_bb", state: "open", updated_at: AT, format: "ndjson", compression: "zstd", chunk_count: CHUNKS, raw_bytes: offset,
        stored_bytes: chunks.reduce((n, c) => n + c.stored_bytes, 0), content_sha256: await computeContentSha256(chunks), epoch: 0,
        epochs: [{ epoch: 0, opened_at: AT, reason: "initial", first_seq: 0, opened_by: "srv_bb" }], next_offset: offset, chunks,
        scrub: { ruleset: "relayhistory-scrub", version: 1 } },
      extensions: { "96bulls": { user_id: "user_1" } },
    };
    await store.put(objectKey(manifestKey(location)), new TextEncoder().encode(JSON.stringify(manifest)), {});
    pointerRow = { org_id: "org_1", workspace_id: "ws_1", harness: "claude", session_id: "sess_1", machine_id: "srv_bb", user_id: "user_1",
      storage_provider: "s3", bucket: "bucket", manifest_key: manifestKey(location), content_sha256: manifest.raw.content_sha256, raw_format: "ndjson", revision: 1 };
  });
  afterAll(async () => { await rm(root, { recursive: true, force: true }); });

  /** A path-style S3 endpoint over the fs store: chunk GETs take a few ms and are counted. */
  function endpoint() {
    const state = { inFlight: 0, peak: 0, chunks: 0 };
    const fetcher: typeof fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = new URL(input instanceof Request ? input.url : String(input));
      const key = url.pathname.split("/").slice(2).map(decodeURIComponent).join("/");
      const chunk = key.endsWith(".zst");
      if (chunk) { state.chunks++; state.peak = Math.max(state.peak, ++state.inFlight); }
      try {
        if (chunk) await new Promise((r) => setTimeout(r, 3));
        init?.signal?.throwIfAborted();
        const object = await store.getObject(key as never).catch(() => undefined);
        if (!object) return new Response("<Error><Code>NoSuchKey</Code></Error>", { status: 404 });
        const body = new Uint8Array(await new Response(object.body).arrayBuffer());
        return new Response(body, { headers: { "content-length": String(body.byteLength), etag: '"e"', "last-modified": new Date(0).toUTCString() } });
      } finally { if (chunk) state.inFlight--; }
    }) as typeof fetch;
    return { state, fetcher };
  }
  const run = (async (_org: string, _ws: string, work: (q: unknown) => Promise<unknown>) =>
    work({ query: async () => ({ rows: [pointerRow] }) })) as never;

  test.each([[undefined, 1], ["", 1], ["1", 1], ["8", 8]])("READ_API_CANONICAL_FETCH_CONCURRENCY=%p: peak chunk GETs in flight is %p, same rows", async (raw, want) => {
    const { state, fetcher } = endpoint();
    const deps = (await serviceCanonicalDeps(raw === undefined ? env : { ...env, READ_API_CANONICAL_FETCH_CONCURRENCY: raw }, run, fetcher))!;
    const snapshot = await deps.turns.source(identity, new AbortController().signal);
    expect(snapshot.exists).toBe(true);
    expect(snapshot.rows.filter((r) => r.table === "convergence_events")).toHaveLength(CHUNKS);
    expect(state.chunks).toBe(CHUNKS);
    if (want === 1) expect(state.peak).toBe(1);
    else { expect(state.peak).toBeGreaterThan(1); expect(state.peak).toBeLessThanOrEqual(want); }
  });

  test.each(["0", "17", "abc", "8.5"])("%p refuses startup", async (raw) => {
    await expect(serviceCanonicalDeps({ ...env, READ_API_CANONICAL_FETCH_CONCURRENCY: raw }, run)).rejects.toThrow(MESSAGE);
  });

  test("LOCKER_BUCKET unset: nothing is wired, so the concurrency is not read (as the other canonical knobs)", async () => {
    expect(await serviceCanonicalDeps({ READ_API_CANONICAL_FETCH_CONCURRENCY: "bad" }, run)).toBeUndefined();
  });
});
