import { zstdCompressSync, zstdDecompressSync } from "node:zlib";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { createFsStore } from "../../object-store/src/fs.js";
import { sha256Hex } from "../../object-store/src/crypto.js";
import {
  computeContentSha256,
  manifestKey,
  objectKey,
  rawChunkKey,
  readAll,
  type ChunkEntry,
  type ManifestV1,
  type SessionLocation,
} from "../src/lib/pipeline/object-layout.js";
import { objectStoreManifestReader } from "../src/lib/pipeline/adapter.js";
import {
  RelayhistoryExportTransform,
  normalizeRelayhistoryRecords,
  parseRelayhistoryRecord,
  readRelayhistoryExport,
  type RelayhistoryStreamRecord,
} from "../src/lib/pipeline/relayhistory-transform.js";

const AT = "2026-10-30T12:00:00.000Z";
const TRACE = "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18";
const loc: SessionLocation = {
  workspaceId: "workspace",
  machineId: "machine",
  harness: "claude",
  sessionId: "session",
  startDate: "2026-10-30",
};
const record = (
  record_id: string,
  revision: string,
  ts_ms: number,
  overrides: Partial<RelayhistoryStreamRecord> = {},
): RelayhistoryStreamRecord => ({
  kind: "session_event",
  origin_id: "origin",
  record_id,
  revision,
  operation: "upsert",
  received_at: AT,
  payload: { ts_ms, text: `${record_id}-${revision}` },
  ...overrides,
});
const ndjson = (records: readonly RelayhistoryStreamRecord[]) =>
  new TextEncoder().encode(`${records.map((value) => JSON.stringify(value)).join("\n")}\n`);

let root: string;

beforeEach(async () => {
  root = await mkdtemp(join(tmpdir(), "96bulls-relayhistory-transform-"));
});

afterEach(async () => {
  await rm(root, { recursive: true, force: true });
});

async function fixture() {
  const store = createFsStore({ root });
  const live = ndjson([
    record("changed", "2", 3_000),
    record("deleted", "3", 4_000, { operation: "delete", payload: null }),
    record("live", "1", 5_000),
  ]);
  const backfill = ndjson([
    record("changed", "1", 1_000),
    record("deleted", "1", 2_000),
    record("historical", "1", 500, {
      payload: { ts_ms: 500, "😀": "astral", "\uE000": "private-use" },
    }),
  ]);
  const chunks: ChunkEntry[] = [];
  for (const [seq, epoch, source] of [
    [0, 0, live],
    [1, 1, backfill],
  ] as const) {
    const stored = new Uint8Array(zstdCompressSync(source));
    const sourceSha = await sha256Hex(source);
    const entry: ChunkEntry = {
      seq,
      epoch,
      start_offset: 0,
      bytes: source.byteLength,
      stored_bytes: stored.byteLength,
      sha256: sourceSha,
      sha256_8: sourceSha.slice(0, 8),
      stored_sha256: await sha256Hex(stored),
      stored_raw_bytes: source.byteLength,
      raw_sha256: sourceSha,
      compression: "zstd",
      records: 3,
    };
    chunks.push(entry);
    await store.put(
      rawChunkKey(loc, seq, 0, sourceSha, "ndjson"),
      stored,
      { ifNoneMatch: true },
    );
  }
  const manifest: ManifestV1 = {
    schema_v: 1,
    layout: "v1",
    trace_id: TRACE,
    revision: 2,
    created_at: AT,
    updated_at: AT,
    session: {
      workspace_id: loc.workspaceId,
      machine_id: loc.machineId,
      harness: loc.harness,
      session_id: loc.sessionId,
      started_at: AT,
    },
    writer: { delivery_instance_id: "live-writer", plugin_version: "test" },
    raw: {
      owner: "live-writer",
      state: "open",
      updated_at: AT,
      format: "ndjson",
      compression: "zstd",
      chunk_count: chunks.length,
      raw_bytes: chunks.reduce((sum, chunk) => sum + chunk.bytes, 0),
      stored_bytes: chunks.reduce((sum, chunk) => sum + chunk.stored_bytes, 0),
      content_sha256: await computeContentSha256(chunks),
      epoch: 1,
      epochs: [
        { epoch: 0, opened_at: AT, reason: "initial", first_seq: 0, opened_by: "live-writer" },
        { epoch: 1, opened_at: AT, reason: "backfill", first_seq: 1, opened_by: "migration:mgr_test" },
      ],
      next_offset: backfill.byteLength,
      chunks,
      scrub: { ruleset: "relayhistory-scrub", version: 1 },
    },
    extensions: {
      "client.relayhistory_export": {
        version: 1,
        source: "neon.delivery_records",
      },
    },
  };
  const key = manifestKey(loc);
  await store.put(objectKey(key), new TextEncoder().encode(JSON.stringify(manifest)), {
    ifNoneMatch: true,
    contentType: "application/json",
  });
  const reader = objectStoreManifestReader(store, { provider: "fs" });
  return { store, reader, manifest, key };
}

describe("RelayHistory export transform", () => {
  it("merges live and pre-T0 epochs by revision, applies tombstones, and event-orders", async () => {
    const f = await fixture();
    const records = await readRelayhistoryExport(
      f.manifest,
      f.store,
      (stored) => new Uint8Array(zstdDecompressSync(stored)),
    );
    expect(records.map((value) => [value.record_id, value.revision])).toEqual([
      ["historical", "1"],
      ["changed", "2"],
      ["live", "1"],
    ]);
  });

  it("refuses equal revisions whose content differs", () => {
    expect(() =>
      normalizeRelayhistoryRecords([
        record("same", "7", 1, { payload: { ts_ms: 1, text: "a" } }),
        record("same", "7", 1, { payload: { ts_ms: 1, text: "b" } }),
      ]),
    ).toThrow(/equal RelayHistory revisions/);
  });

  it("folds one revision that the export and the live client stored at different times", () => {
    const exported = record("same", "7", 1, { received_at: "2026-10-29T00:00:00.000Z" });
    const live = record("same", "7", 1, { received_at: "2026-10-30T00:00:00.000Z" });
    for (const records of [[exported, live], [live, exported]])
      expect(normalizeRelayhistoryRecords(records).map((value) => value.record_id)).toEqual(["same"]);
    expect(() =>
      normalizeRelayhistoryRecords([
        record("same", "7", 1),
        record("same", "7", 1, { kind: "tool_call" }),
      ]),
    ).toThrow(/equal RelayHistory revisions/);
  });

  it("bounds record identities and signed-bigint revisions", () => {
    expect(() =>
      parseRelayhistoryRecord(JSON.stringify(record("id", "9223372036854775808", 1))),
    ).toThrow(/revision is not numeric/);
    expect(() =>
      parseRelayhistoryRecord(JSON.stringify(record("x".repeat(1025), "1", 1))),
    ).toThrow(/record_id is missing/);
  });

  it("writes an idempotent content-addressed normalized object", async () => {
    const f = await fixture();
    const transform = new RelayhistoryExportTransform({
      manifests: f.reader,
      store: f.store,
      decompress: (stored) => new Uint8Array(zstdDecompressSync(stored)),
    });
    const pointer = {
      traceId: TRACE,
      workspaceId: loc.workspaceId,
      manifestKey: f.key,
    };
    const first = await transform.normalize(pointer);
    const second = await transform.normalize(pointer);
    expect(second).toEqual(first);
    expect(first.outputKey).toContain(f.manifest.raw.content_sha256);
    const body = new TextDecoder().decode(
      await readAll(await f.store.get(objectKey(first.outputKey!))),
    );
    const lines = body.trim().split("\n").map((line) => JSON.parse(line));
    expect(lines.map((line) => line.record_id)).toEqual([
      "historical",
      "changed",
      "live",
    ]);
    expect(Object.keys(lines[0]!.payload)).toEqual(["ts_ms", "\uE000", "😀"]);
  });

  it("refuses a chunk whose stored digest does not match the manifest", async () => {
    const f = await fixture();
    f.manifest.raw.chunks[0]!.stored_sha256 = "f".repeat(64);
    await expect(
      readRelayhistoryExport(
        f.manifest,
        f.store,
        (stored) => new Uint8Array(zstdDecompressSync(stored)),
      ),
    ).rejects.toMatchObject({ code: "chunk_digest_mismatch", retryable: false });
  });

  it("requires every chunk to declare its record count", async () => {
    const f = await fixture();
    delete f.manifest.raw.chunks[0]!.records;
    await expect(
      readRelayhistoryExport(
        f.manifest,
        f.store,
        (stored) => new Uint8Array(zstdDecompressSync(stored)),
      ),
    ).rejects.toMatchObject({ code: "relayhistory_record_count", retryable: false });
  });
});
