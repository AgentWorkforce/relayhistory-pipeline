/**
 * The pointer stage for lock-hold tests: processBatch (pointer stage only) on a PGlite with every migration, an
 * fs object store, and the Lambda's scope lock shape (lambda.ts: one transaction, SET LOCAL lock_timeout, the
 * transaction-scoped advisory lock, then the work) recorded instead of held on a second connection (PGlite has
 * one): a per-scope in-memory mutex plus an "inside the lock" marker. Every object-store call and every SQL
 * statement is recorded with that marker, so a test can assert what runs while the lock transaction is open.
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
import { computeContentSha256, manifestKey, objectKey, objectStoreManifestReader, prefixAllowed, prefixOrgResolver, rawChunkKey, type ChunkEntry, type ManifestV1, type ObjectStore, type ManifestReader } from "../../shared/vendor.ts";
import type { HoopDb } from "../../shared/db.ts";
import { processBatch, type ConsumerDeps } from "../consumer.ts";
import { droppedFanout, scopeLockKey, type PointerDeps, type ScopeLock } from "../pointer.ts";
import { SESSION_BOOTSTRAP_FILES } from "../../hoopdb/session-bootstrap-chain.ts";

const repo = join(import.meta.dir, "../../..");
const readF = (p: string) => readFileSync(join(repo, p), "utf8");
export const AT = "2026-11-01T10:00:00.000Z";
export const NOW = new Date("2026-11-02T00:00:00.000Z");
export const loc = { prefix: "96bulls/org_1", workspaceId: "ws_1", machineId: "srv_bb", harness: "claude", sessionId: "sess_1", startDate: "2026-11-01" };
export type Loc = typeof loc;

export type Call = { at: "lock" | "free"; kind: string; detail: unknown };

export async function pointerRig() {
  const base = join(homedir(), ".agent-tmp");
  await mkdir(base, { recursive: true });
  const root = await mkdtemp(join(base, "96bulls-pointer-lock-"));
  const store: ObjectStore = createFsStore({ root });
  const pg = new PGlite({ extensions: { btree_gin, pg_trgm } });
  await pg.exec(readF("supabase/tests/supabase_shim.sql"));
  for (const f of SESSION_BOOTSTRAP_FILES) await pg.exec(readF(f));
  for (const f of readdirSync(join(repo, "supabase/migrations")).sort()) await pg.exec(readF(`supabase/migrations/${f}`));
  for (const f of readdirSync(join(repo, "src/hoopdb/migrations")).filter((x) => x.endsWith(".sql")).sort()) await pg.exec(readF(`src/hoopdb/migrations/${f}`));

  const calls: Call[] = [];
  let depth = 0;
  const at = () => (depth > 0 ? "lock" : "free") as Call["at"];
  const plain = (v: unknown): unknown => JSON.parse(JSON.stringify(v ?? null));
  const reader0 = objectStoreManifestReader(store, { provider: "s3", bucket: "bucket" });
  const reader: ManifestReader = {
    ...reader0,
    read: async (key) => {
      calls.push({ at: at(), kind: "s3.read", detail: key });
      return reader0.read(key);
    },
  };
  const exists = async (key: string) => {
    calls.push({ at: at(), kind: "s3.head", detail: key });
    return (await store.head(objectKey(key))) !== undefined;
  };
  /** Hooks a test sets: runs right before the first pool statement inside the lock, once. */
  const hooks: { beforeLock?: () => Promise<void>; atLock?: () => Promise<void>; beforeUpsert?: () => Promise<void> } = {};
  const db = {
    pool: undefined,
    end: async () => {},
    async query(text: string, params?: unknown[]) {
      calls.push({ at: at(), kind: "sql", detail: { sql: text.trim().replace(/\s+/g, " "), params: plain(params ?? []) } });
      if (hooks.beforeUpsert && text.includes("upsert_session_index")) {
        const h = hooks.beforeUpsert;
        hooks.beforeUpsert = undefined;
        await h();
      }
      const r = await pg.query(text, params as unknown[]);
      return { rows: r.rows };
    },
  } as unknown as HoopDb;
  // The Lambda's lock shape (lambda.ts runtimeFromSecret), recorded: its statements are the lock connection's.
  const held = new Map<string, Promise<void>>();
  const lock: ScopeLock = async (scope, work) => {
    if (hooks.beforeLock) {
      const h = hooks.beforeLock;
      hooks.beforeLock = undefined;
      await h();
    }
    const key = scopeLockKey(scope);
    while (held.has(key)) await held.get(key);
    let release!: () => void;
    held.set(key, new Promise<void>((r) => (release = r)));
    calls.push({ at: "lock", kind: "lock.sql", detail: "BEGIN" });
    calls.push({ at: "lock", kind: "lock.sql", detail: "SET LOCAL lock_timeout = '30s'" });
    calls.push({ at: "lock", kind: "lock.sql", detail: ["SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", key] });
    depth += 1;
    try {
      if (hooks.atLock) {
        const h = hooks.atLock;
        hooks.atLock = undefined;
        await h();
      }
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

  async function writeManifest(revision: number, l: Loc = loc) {
    const source = new TextEncoder().encode(JSON.stringify({ kind: "session_event", origin_id: "o", record_id: `r${revision}`, revision: "1", operation: "upsert", received_at: AT, payload: { ts_ms: 1, text: "x" } }) + "\n");
    const stored = new Uint8Array(zstdCompressSync(source));
    const sha = await sha256Hex(source);
    const chunk: ChunkEntry = { seq: 0, epoch: 0, start_offset: 0, bytes: source.byteLength, stored_bytes: stored.byteLength, sha256: sha, sha256_8: sha.slice(0, 8), stored_sha256: await sha256Hex(stored), stored_raw_bytes: source.byteLength, raw_sha256: sha, compression: "zstd", records: 1 };
    await store.put(rawChunkKey(l, chunk.seq, 0, sha, "ndjson"), stored, {});
    const manifest = { schema_v: 1, layout: "v1", trace_id: "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18", revision, created_at: AT, updated_at: AT,
      session: { workspace_id: "ws_1", machine_id: "srv_bb", harness: "claude", session_id: "sess_1", started_at: `${l.startDate}T10:00:00.000Z` },
      writer: { delivery_instance_id: "srv_bb", plugin_version: "t" },
      raw: { owner: "srv_bb", state: "open", updated_at: AT, format: "ndjson", compression: "zstd", chunk_count: 1, raw_bytes: chunk.bytes, stored_bytes: chunk.stored_bytes, content_sha256: await computeContentSha256([chunk]), epoch: 0, epochs: [{ epoch: 0, opened_at: AT, reason: "initial", first_seq: chunk.seq, opened_by: "srv_bb" }], next_offset: source.byteLength, chunks: [chunk], scrub: { ruleset: "relayhistory-scrub", version: 1 } },
      extensions: { "96bulls": { user_id: "user_1", session_uuid: "0193a1f3-8e2b-7c41-9d3a-5b6f0e2a7c18" } } } as unknown as ManifestV1;
    const key = manifestKey(l);
    await store.put(objectKey(key), new TextEncoder().encode(JSON.stringify(manifest)), { contentType: "application/json" });
    return key;
  }
  const body = (key: string, name: string, seq: string, eTag = "e") =>
    JSON.stringify({ Records: [{ eventSource: "aws:s3", eventName: name, eventTime: AT, s3: { bucket: { name: "bucket" }, object: { key, eTag, sequencer: seq } } }] });
  const run = (id: string, b: string, extra: Partial<PointerDeps> = {}) => processBatch([{ messageId: id, body: b }], deps(extra));
  const row = async () =>
    (await pg.query<{ revision: string; deleted_at: string | null; manifest_key: string }>(
      "SELECT revision::text, deleted_at, manifest_key FROM sessions.session_index WHERE session_id='sess_1'")).rows;
  const close = async () => {
    await pg.close();
    await rm(root, { recursive: true, force: true });
  };
  return { pg, store, calls, hooks, pointer, deps, run, writeManifest, body, row, close };
}
