import { expect, test } from "bun:test";
import { setImmediate } from "node:timers/promises";
import {
  boundedCanonicalReads,
  buildCanonicalReportExport,
} from "../source.ts";
import type {
  CanonicalDeps,
  CanonicalPointer,
} from "../../session-read-api/canonical.ts";
import { zstdCompressSync, zstdDecompressSync } from "node:zlib";
import { createHash } from "node:crypto";
import {
  computeContentSha256,
  manifestKey,
  objectKey,
  rawChunkKey,
  type ChunkEntry,
  type ManifestV1,
} from "../../../packages/object-store/src/index.ts";
const encode = (s: string) => new TextEncoder().encode(s);
const digest = (b: Uint8Array) => createHash("sha256").update(b).digest("hex");
const at = "2026-11-01T10:00:00.000Z";
async function fixture(count = 2, text?: string) {
  const objects = new Map<string, Uint8Array>(),
    pointers = new Map<string, CanonicalPointer>();
  const accesses: string[] = [];
  for (const sessionId of ["s1", "s2"]) {
    const loc = {
      prefix: "96bulls/org",
      workspaceId: "ws",
      machineId: "machine",
      harness: "claude",
      sessionId,
      startDate: "2026-11-01",
    };
    const records = Array.from({ length: count }, (_, n) => n + 1).map((i) => ({
      kind: "session_event",
      origin_id: "origin",
      record_id: `r${i}`,
      revision: "1",
      operation: "upsert",
      received_at: at,
      payload: {
        ts_ms: Date.parse(at),
        text: text ?? `message ${i}`,
        role: i === 1 ? "user" : "assistant",
        project_key: "github.com/team/project",
        message_id: `msg-${i}`,
      },
    }));
    const raw = encode(records.map((r) => JSON.stringify(r)).join("\n") + "\n"),
      stored = new Uint8Array(zstdCompressSync(raw)),
      sha = digest(raw);
    const chunk: ChunkEntry = {
      seq: 0,
      epoch: 0,
      start_offset: 0,
      bytes: raw.length,
      stored_bytes: stored.length,
      sha256: sha,
      sha256_8: sha.slice(0, 8),
      stored_sha256: digest(stored),
      stored_raw_bytes: raw.length,
      raw_sha256: sha,
      compression: "zstd",
      records: count,
    };
    const manifest = {
      schema_v: 1,
      layout: "v1",
      trace_id: "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18",
      revision: 1,
      created_at: at,
      updated_at: at,
      session: {
        workspace_id: "ws",
        machine_id: "machine",
        harness: "claude",
        session_id: sessionId,
        started_at: at,
      },
      writer: { delivery_instance_id: "machine", plugin_version: "test" },
      raw: {
        owner: "machine",
        state: "open",
        updated_at: at,
        format: "ndjson",
        compression: "zstd",
        chunk_count: 1,
        raw_bytes: raw.length,
        stored_bytes: stored.length,
        content_sha256: await computeContentSha256([chunk]),
        epoch: 0,
        epochs: [
          {
            epoch: 0,
            opened_at: at,
            reason: "initial",
            first_seq: 0,
            opened_by: "machine",
          },
        ],
        next_offset: raw.length,
        chunks: [chunk],
        scrub: { ruleset: "relayhistory-scrub", version: 1 },
      },
      extensions: {
        "96bulls": { user_id: "user", canonical_session_id: sessionId },
      },
    } as ManifestV1;
    objects.set(rawChunkKey(loc, 0, 0, sha, "ndjson"), stored);
    objects.set(manifestKey(loc), encode(JSON.stringify(manifest)));
    pointers.set(sessionId, {
      org_id: "org",
      workspace_id: "ws",
      harness: "claude",
      session_id: sessionId,
      machine_id: "machine",
      user_id: "user",
      storage_provider: "s3",
      bucket: "bucket",
      manifest_key: manifestKey(loc),
      content_sha256: manifest.raw.content_sha256,
      raw_format: "ndjson",
      revision: 1,
      deleted_at: null,
    });
  }
  const stream = (bytes: Uint8Array) =>
    new ReadableStream<Uint8Array>({
      start(c) {
        c.enqueue(bytes);
        c.close();
      },
    });
  const canonical = {
    provider: "s3",
    bucket: "bucket",
    prefix: {
      PIPELINE_KEY_PREFIX: "96bulls",
      PIPELINE_ORG_FROM_PREFIX: "true",
    },
    pointers: async (identity) => {
      accesses.push(identity.sessionId);
      return [pointers.get(identity.sessionId)!];
    },
    decompress: (bytes, max) =>
      new Uint8Array(zstdDecompressSync(bytes, { maxOutputLength: max })),
    store: {
      backend: "fs",
      get: async (key) => stream(objects.get(key)!),
      getObject: async (key) => ({
        body: stream(objects.get(key)!),
        meta: {
          key,
          size: objects.get(key)!.length,
          lastModified: new Date(at),
          version: {},
          metadata: {},
        },
      }),
    },
  } as CanonicalDeps;
  return { canonical, accesses };
}
test("rejects event cap while processing first session before reading a second", async () => {
  const { canonical, accesses } = await fixture();
  const scope = { organizationId: "org", workspaceId: "ws" };
  await expect(
    buildCanonicalReportExport(
      {
        canonical,
        trustedScope: scope,
        now: () => "2026-11-03T00:00:00.000Z",
        listSessions: async () => ({
          generation: "1",
          complete: true,
          sessions: ["s1", "s2"].map((sessionId) => ({
            orgId: "org",
            workspaceId: "ws",
            source: "claude",
            sessionId,
          })),
        }),
      },
      {
        scope,
        window: {
          since: "2026-11-01T00:00:00.000Z",
          until: "2026-11-02T00:00:00.000Z",
        },
        maxSessions: 10,
        maxEvents: 1,
      },
    ),
  ).rejects.toThrow("event_budget_exceeded");
  expect(accesses).toEqual(["s1", "s1"]);
});
test("generation validation does not reread full objects", async () => {
  const { canonical, accesses } = await fixture();
  let reads = 0;
  const get = canonical.store.get;
  const getObject = canonical.store.getObject;
  canonical.store.get = async (...args) => {
    reads++;
    return get(...args);
  };
  canonical.store.getObject = async (...args) => {
    reads++;
    return getObject(...args);
  };
  const scope = { organizationId: "org", workspaceId: "ws" };
  const result = await buildCanonicalReportExport(
    {
      canonical,
      trustedScope: scope,
      now: () => "2026-11-03T00:00:00.000Z",
      listSessions: async () => ({
        generation: "1",
        complete: true,
        sessions: [
          {
            orgId: "org",
            workspaceId: "ws",
            source: "claude",
            sessionId: "s1",
          },
        ],
      }),
    },
    {
      scope,
      window: {
        since: "2026-11-01T00:00:00.000Z",
        until: "2026-11-02T00:00:00.000Z",
      },
      maxSessions: 10,
      maxEvents: 10,
    },
  );
  expect(result.export.events).toHaveLength(2);
  expect(
    result.export.events.find((e) => e.text.includes("message 1"))!.text,
  ).toContain(
    "Recorded role: user; human or automated authorship not established",
  );
  expect(
    result.export.events.find((e) => e.text.includes("message 2"))!.text,
  ).toContain("Recorded role: assistant");
  expect(result.export.events[0]!.kind).toBe("session_event");
  expect(result.export.events.every((e) => e.availableAt === null)).toBe(true);
  expect(
    result.sourceSnapshot.eventMetadata.every((e) => e.ingestedAt === at),
  ).toBe(true);
  expect(result.export.coverage.notes).toContain(
    "Historical authorized read availability is unknown",
  );
  expect(result.export.sessions[0]!.projectKey).toBe("team/project");
  expect(
    result.sourceSnapshot.eventMetadata.map((m) => m.actorRole).sort(),
  ).toEqual(["assistant", "user"]);
  expect(accesses).toHaveLength(3);
  expect(reads).toBe(2);
});
test("shared decoded scan budget stops a later session before projection", () => {
  const canonical = {
    store: { backend: "fs" },
    decompress: (bytes: Uint8Array) => bytes,
  } as CanonicalDeps;
  const bounded = boundedCanonicalReads(canonical, undefined, 10);
  expect(bounded.decompress(new Uint8Array(6), 100)).toHaveLength(6);
  expect(() => bounded.decompress(new Uint8Array(6), 100)).toThrow(
    "scan_budget_exceeded",
  );
});
test("deadline rejects a stalled stream and cancels its reader", async () => {
  let cancelled = false;
  const controller = new AbortController();
  const canonical = {
    store: {
      backend: "fs",
      get: async () =>
        new ReadableStream<Uint8Array>({
          cancel() {
            cancelled = true;
          },
        }),
    },
  } as CanonicalDeps;
  const bounded = boundedCanonicalReads(canonical, controller.signal);
  const stream = await bounded.store.get(objectKey("x"));
  const pending = stream.getReader().read();
  controller.abort(new Error("deadline"));
  await expect(pending).rejects.toThrow("deadline");
  expect(cancelled).toBe(true);
  await expect(bounded.store.get(objectKey("next"))).rejects.toThrow(
    "deadline",
  );
});
test("late read responses are cancelled after deadline", async () => {
  let finish!: (value: ReadableStream<Uint8Array>) => void;
  let cancelled = false;
  const controller = new AbortController();
  const canonical = {
    store: {
      backend: "fs",
      get: async () =>
        new Promise<ReadableStream<Uint8Array>>((resolve) => {
          finish = resolve;
        }),
    },
  } as CanonicalDeps;
  const bounded = boundedCanonicalReads(canonical, controller.signal);
  const pending = bounded.store.get(objectKey("x"));
  await Promise.resolve();
  controller.abort(new Error("deadline"));
  await expect(pending).rejects.toThrow("deadline");
  finish(
    new ReadableStream<Uint8Array>({
      cancel() {
        cancelled = true;
      },
    }),
  );
  await setImmediate();
  expect(cancelled).toBe(true);
});

test("serialized output byte cap fails before scanning the next session", async () => {
  const { canonical, accesses } = await fixture(750, "x".repeat(12000));
  const scope = { organizationId: "org", workspaceId: "ws" };
  await expect(
    buildCanonicalReportExport(
      {
        canonical,
        trustedScope: scope,
        now: () => "2026-11-03T00:00:00.000Z",
        listSessions: async () => ({
          generation: "1",
          complete: true,
          sessions: ["s1", "s2"].map((sessionId) => ({
            orgId: "org",
            workspaceId: "ws",
            source: "claude",
            sessionId,
          })),
        }),
      },
      {
        scope,
        window: {
          since: "2026-11-01T00:00:00.000Z",
          until: "2026-11-02T00:00:00.000Z",
        },
        maxSessions: 10,
        maxEvents: 10000,
      },
    ),
  ).rejects.toThrow("export_budget_exceeded");
  expect(accesses).toEqual(["s1", "s1"]);
});
test("stored stream budget is cumulative and cancels an over-budget body", async () => {
  let cancelled = false;
  const canonical = {
    store: {
      backend: "fs",
      get: async () =>
        new ReadableStream<Uint8Array>({
          start(c) {
            c.enqueue(new Uint8Array(6));
          },
          cancel() {
            cancelled = true;
          },
        }),
    },
  } as CanonicalDeps;
  const bounded = boundedCanonicalReads(canonical, undefined, 10);
  const first = (await bounded.store.get(objectKey("first"))).getReader();
  await first.read();
  await first.cancel();
  cancelled = false;
  const second = (await bounded.store.get(objectKey("second"))).getReader();
  await expect(second.read()).rejects.toThrow("scan_budget_exceeded");
  expect(cancelled).toBe(true);
  await expect(bounded.store.delete(objectKey("never"))).rejects.toThrow(
    "store_operation_denied",
  );
});
