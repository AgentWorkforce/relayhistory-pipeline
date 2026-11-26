/**
 * The pointer stage for move tests: processBatch (pointer stage only) on a PGlite with every migration (the
 * revision fence, the re-key and the directory projection are the real SQL) and an fs object store. Every
 * object-store call and SQL statement is recorded, with whether it ran inside the scope lock (the lock is the
 * Lambda's shape, recorded on a per-scope mutex, as PGlite has one connection).
 *
 * The manifest writer can write the exporter's cold-export shape (closed, owner `migration:<grant>`, an export
 * marker with `origin_revisions`), with chunks sealed the way `sealRawChunk` stores them (the `raw-sha256` and
 * `raw-bytes` metadata), so a date move can be staged as it happens in production.
 *
 * Synthetic, secret-free data only.
 */
import { zstdCompressSync } from "node:zlib";
import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { readdirSync, readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import { sha256Hex } from "../../../packages/object-store/src/crypto.ts";
import {
  computeContentSha256,
  manifestKey,
  objectKey,
  objectStoreManifestReader,
  prefixAllowed,
  prefixOrgResolver,
  rawChunkKey,
  type ChunkEntry,
  type ManifestReader,
  type ManifestV1,
  type ObjectStore,
} from "../../shared/vendor.ts";
import type { HoopDb } from "../../shared/db.ts";
import { processBatch, type ConsumerDeps } from "../consumer.ts";
import { droppedFanout, scopeLockKey, type PointerDeps, type ScopeLock } from "../pointer.ts";

const repo = join(import.meta.dir, "../../..");
const readF = (p: string) => readFileSync(join(repo, p), "utf8");
export const AT = "2026-11-01T10:00:00.000Z";
export const NOW = new Date("2026-11-02T00:00:00.000Z");
export const loc = { prefix: "96bulls/org_1", workspaceId: "ws_1", machineId: "srv_bb", harness: "claude", sessionId: "sess_1", startDate: "2026-11-01" };
export type Loc = typeof loc;
export const at = (startDate: string): Loc => ({ ...loc, startDate });
export const GRANT_OWNER = "migration:grant_synthetic";

export type Call = { at: "lock" | "free"; kind: string; detail: unknown };

export interface ManifestSpec {
  revision?: number;
  /** Manifest `updated_at` (and `raw.updated_at`). */
  updatedAt?: string;
  state?: "open" | "closed";
  owner?: string;
  /** One chunk per entry; each entry is the record ids of that chunk. */
  chunks?: string[][];
  /** The exporter marker's `origin_revisions`; absent: no marker. */
  watermarks?: Record<string, number>;
  /** Override `session.started_at` (default: the key's date at 10:00 UTC). */
  startedAt?: string;
  /** Epoch records (default one `initial` epoch opened by the owner). */
  epochs?: ManifestV1["raw"]["epochs"];
  /** Seal chunks without the `raw-sha256` metadata. */
  noChunkMetadata?: boolean;
  /** Write the manifest but none of its chunks. */
  skipChunks?: boolean;
}

export async function pointerRig() {
  const base = join(homedir(), ".agent-tmp");
  await mkdir(base, { recursive: true });
  const root = await mkdtemp(join(base, "96bulls-pointer-move-"));
  const store: ObjectStore = createFsStore({ root });
  const pg = new PGlite({ extensions: { btree_gin, pg_trgm } });
  await pg.exec(readF("supabase/tests/supabase_shim.sql"));
  await pg.exec(readF("supabase/bootstrap/session_directory_search_worker.sql"));
  for (const f of readdirSync(join(repo, "supabase/migrations")).sort()) await pg.exec(readF(`supabase/migrations/${f}`));
  for (const f of readdirSync(join(repo, "src/hoopdb/migrations")).filter((x) => x.endsWith(".sql")).sort()) await pg.exec(readF(`src/hoopdb/migrations/${f}`));

  const calls: Call[] = [];
  let depth = 0;
  const where = () => (depth > 0 ? "lock" : "free") as Call["at"];
  const plain = (v: unknown): unknown => JSON.parse(JSON.stringify(v ?? null));
  const reader0 = objectStoreManifestReader(store, { provider: "s3", bucket: "bucket" });
  /** Hooks a test sets. `afterRead(key, n)` runs after the n-th read of `key` (1-based) returns. */
  const hooks: { afterRead?: (key: string, n: number) => Promise<void>; beforeUpsert?: () => Promise<void> } = {};
  const reads = new Map<string, number>();
  const reader: ManifestReader = {
    ...reader0,
    read: async (key) => {
      calls.push({ at: where(), kind: "s3.read", detail: key });
      const r = await reader0.read(key);
      const n = (reads.get(key) ?? 0) + 1;
      reads.set(key, n);
      if (hooks.afterRead) await hooks.afterRead(key, n);
      return r;
    },
  };
  const exists = async (key: string) => {
    calls.push({ at: where(), kind: "s3.head", detail: key });
    return (await store.head(objectKey(key))) !== undefined;
  };
  const head = async (key: string) => {
    calls.push({ at: where(), kind: "s3.head_meta", detail: key });
    const meta = await store.head(objectKey(key));
    return meta ? { size: meta.size, metadata: { ...meta.metadata } } : undefined;
  };
  const db = {
    pool: undefined,
    end: async () => {},
    async query(text: string, params?: unknown[]) {
      calls.push({ at: where(), kind: "sql", detail: { sql: text.trim().replace(/\s+/g, " "), params: plain(params ?? []) } });
      if (hooks.beforeUpsert && text.includes("upsert_session_index")) {
        const h = hooks.beforeUpsert;
        hooks.beforeUpsert = undefined;
        await h();
      }
      const r = await pg.query(text, params as unknown[]);
      return { rows: r.rows };
    },
  } as unknown as HoopDb;
  const held = new Map<string, Promise<void>>();
  const lock: ScopeLock = async (scope, work) => {
    const key = scopeLockKey(scope);
    while (held.has(key)) await held.get(key);
    let release!: () => void;
    held.set(key, new Promise<void>((r) => (release = r)));
    calls.push({ at: "lock", kind: "lock.sql", detail: ["SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", key] });
    depth += 1;
    try {
      return await work();
    } finally {
      depth -= 1;
      calls.push({ at: "lock", kind: "lock.sql", detail: "COMMIT" });
      held.delete(key);
      release();
    }
  };
  const b = { PIPELINE_ORG_FROM_PREFIX: "true", PIPELINE_KEY_PREFIX: "96bulls" };
  const pointer = (extra: Partial<PointerDeps> = {}): PointerDeps => ({
    db, lock, reader, exists, orgs: prefixOrgResolver, allowPrefix: (p, o) => prefixAllowed(b, p, o), fanout: droppedFanout, now: () => NOW, ...extra,
  });
  const deps = (extra: Partial<PointerDeps> = {}): ConsumerDeps => ({ pointer: pointer(extra) });

  /** The legacy single-chunk manifest the existing pointer tests write (open, owner `srv_bb`, no marker). */
  async function writeManifest(revision: number, l: Loc = loc) {
    return writeExport(l, { revision, state: "open", owner: "srv_bb", chunks: [[`r${revision}`]] });
  }

  async function sealChunk(l: Loc, seq: number, startOffset: number, records: string[], noMetadata: boolean, skip: boolean) {
    const source = new TextEncoder().encode(
      records.map((id) => JSON.stringify({ kind: "session_event", origin_id: "o", record_id: id, revision: "1", operation: "upsert", received_at: AT, payload: { ts_ms: 1, text: "x" } })).join("\n") + "\n",
    );
    const stored = new Uint8Array(zstdCompressSync(source));
    const sha = await sha256Hex(source);
    const chunk: ChunkEntry = { seq, epoch: 0, start_offset: startOffset, bytes: source.byteLength, stored_bytes: stored.byteLength, sha256: sha, sha256_8: sha.slice(0, 8), stored_sha256: await sha256Hex(stored), stored_raw_bytes: source.byteLength, raw_sha256: sha, compression: "zstd", records: records.length };
    if (!skip)
      await store.put(rawChunkKey(l, seq, startOffset, sha, "ndjson"), stored, noMetadata ? {} : { metadata: { "raw-sha256": sha, "raw-bytes": String(source.byteLength) } });
    return chunk;
  }

  /** A manifest at `l` (default: the exporter's cold-export shape, closed, one chunk). Returns its key. */
  async function writeExport(l: Loc, spec: ManifestSpec = {}) {
    const owner = spec.owner ?? GRANT_OWNER;
    const updatedAt = spec.updatedAt ?? AT;
    const chunks: ChunkEntry[] = [];
    let offset = 0;
    for (const [seq, records] of (spec.chunks ?? [["r1"]]).entries()) {
      const c = await sealChunk(l, seq, offset, records, spec.noChunkMetadata === true, spec.skipChunks === true);
      chunks.push(c);
      offset += c.bytes;
    }
    const startedAt = spec.startedAt ?? `${l.startDate}T10:00:00.000Z`;
    const epochs = spec.epochs ?? [{ epoch: 0, opened_at: AT, reason: "initial" as const, first_seq: 0, opened_by: owner }];
    const manifest = {
      schema_v: 1, layout: "v1", trace_id: "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18", revision: spec.revision ?? 1, created_at: AT, updated_at: updatedAt,
      session: { workspace_id: l.workspaceId, machine_id: l.machineId, harness: l.harness, session_id: l.sessionId, started_at: startedAt },
      writer: { delivery_instance_id: owner, plugin_version: "t" },
      raw: {
        owner, state: spec.state ?? "closed", updated_at: updatedAt, format: "ndjson", compression: "zstd", chunk_count: chunks.length,
        raw_bytes: chunks.reduce((n, c) => n + c.bytes, 0), stored_bytes: chunks.reduce((n, c) => n + c.stored_bytes, 0),
        content_sha256: await computeContentSha256(chunks), epoch: epochs.length - 1, epochs,
        // Every chunk is in epoch 0, so a later (empty) current epoch starts at offset 0.
        next_offset: epochs.length === 1 ? offset : 0, chunks, scrub: { ruleset: "relayhistory-scrub", version: 1 },
      },
      extensions: {
        "96bulls": { user_id: "user_1", session_uuid: "0193a1f3-8e2b-7c41-9d3a-5b6f0e2a7c18" },
        ...(spec.watermarks ? { "client.relayhistory_export": { version: 1, exported_at: updatedAt, origin_revisions: spec.watermarks } } : {}),
      },
    } as unknown as ManifestV1;
    const key = manifestKey(l);
    await store.put(objectKey(key), new TextEncoder().encode(JSON.stringify(manifest)), { contentType: "application/json" });
    return key;
  }

  const body = (key: string, name: string, seq: string, eTag = "e") =>
    JSON.stringify({ Records: [{ eventSource: "aws:s3", eventName: name, eventTime: AT, s3: { bucket: { name: "bucket" }, object: { key, eTag, sequencer: seq } } }] });
  const run = (id: string, b: string, extra: Partial<PointerDeps> = {}) => processBatch([{ messageId: id, body: b }], deps(extra));
  const failures = async (id: string, b: string, extra: Partial<PointerDeps> = {}) => (await run(id, b, extra)).batchItemFailures.map((f) => f.itemIdentifier);
  const row = async () =>
    (await pg.query<{ revision: string; deleted_at: string | null; manifest_key: string }>(
      "SELECT revision::text, deleted_at, manifest_key FROM sessions.session_index WHERE session_id='sess_1'")).rows;
  const close = async () => {
    await pg.close();
    await rm(root, { recursive: true, force: true });
  };
  return { pg, store, calls, hooks, head, pointer, deps, run, failures, writeManifest, writeExport, body, row, close };
}

export type Rig = Awaited<ReturnType<typeof pointerRig>>;

/** Capture the JSON log lines of the pointer stage written while `act` runs. */
export async function captureLogs<T>(act: () => Promise<T>): Promise<{ result: T; lines: Record<string, unknown>[] }> {
  const lines: Record<string, unknown>[] = [];
  const stdout = process.stdout.write, stderr = process.stderr.write;
  const tap = (w: typeof process.stdout.write, stream: NodeJS.WriteStream) =>
    ((chunk: unknown, ...rest: unknown[]) => {
      for (const line of String(chunk).split("\n"))
        if (line.startsWith("{"))
          try {
            // Wall-clock fields (the log time, durations and the latency ages against Date.now) are dropped.
            const { ts: _ts, duration_ms: _d, event_age_ms: _e, s3_event_age_ms: _s, ...entry } = JSON.parse(line) as Record<string, unknown>;
            if (entry.stage === "session-indexed" || entry.stage === "object-landed") lines.push(entry);
          } catch {}
      return (w as (...a: unknown[]) => boolean).call(stream, chunk, ...rest);
    }) as typeof process.stdout.write;
  process.stdout.write = tap(stdout, process.stdout);
  process.stderr.write = tap(stderr, process.stderr);
  try {
    return { result: await act(), lines };
  } finally {
    process.stdout.write = stdout;
    process.stderr.write = stderr;
  }
}
