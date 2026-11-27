/**
 * The publication authority's generation guard, on PGlite with this tree's real migrations and bootstrap (every
 * session_index trigger installed). A pointer write through hoopDbSessionIndex with the publication transaction
 * calls sessions.session_publication_lock -> ingest_publication_scope -> ingest_require_generation with the
 * evidence's schemaGeneration. Only a generation equal to the passing role-validation marker (or an admitted
 * overlap) is accepted; any other one is refused with SQLSTATE 55000 "session ingest: generation not ready" before
 * anything is written. This is why a Lambda image whose derived generation is ahead of the database's passing
 * marker fails every pointer write, and why the image's derived generation must equal the live marker.
 */
import { afterAll, beforeAll, expect, setDefaultTimeout, test } from "bun:test";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { SESSION_BOOTSTRAP_FILES } from "../../hoopdb/session-bootstrap-chain.ts";
import { hoopDbSessionIndex } from "../../hoopdb/index.ts";
import { generation } from "../../hoopdb/migrate.ts";
import { ROLE_VALIDATION_TABLE, type HoopDb, type Transact } from "../../shared/db.ts";
import type { ManifestV1, RawPointer, TraceContext } from "../../shared/vendor.ts";
setDefaultTimeout(180_000);

const root = join(import.meta.dir, "../../..");
const read = (p: string) => readFileSync(join(root, p), "utf8");
/** The generation of the tree one schema step ahead (the same tree plus 20261105062300_search_index_stage): what a
 * main image derives while the database's passing marker is still this tree's. */
const AHEAD = "f31d73e2a286e712175fa8d4417f07895702012f80c1b3ff88a210381418e44a";
const traceId = "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18";

let pg: PGlite;
let live: string;
const db: HoopDb = { query: async <R>(text: string, params?: unknown[]) => ({ rows: (await pg.query(text, params as never)).rows as R[] }) } as HoopDb;
const transact: Transact = (work) => pg.transaction(async (tx) => work({ query: async <R>(t: string, p?: unknown[]) => ({ rows: (await tx.query(t, p as never)).rows as R[] }) }));

beforeAll(async () => {
  pg = new PGlite({ extensions: { btree_gin, pg_trgm } });
  await pg.exec(read("supabase/tests/supabase_shim.sql"));
  for (const f of SESSION_BOOTSTRAP_FILES) await pg.exec(read(f));
  for (const f of readdirSync(join(root, "supabase/migrations")).filter((f) => f.endsWith(".sql")).sort()) await pg.exec(read("supabase/migrations/" + f));
  for (const f of readdirSync(join(root, "src/hoopdb/migrations")).filter((f) => f.endsWith(".sql")).sort()) await pg.exec(read("src/hoopdb/migrations/" + f));
  live = await generation();
  // The marker table is the migrator's (migrate.ts runMigrations), not a migration file's: the same DDL here.
  await pg.exec(`CREATE TABLE IF NOT EXISTS ${ROLE_VALIDATION_TABLE} (name text PRIMARY KEY, generation text NOT NULL,
    status text NOT NULL CHECK (status IN ('pending', 'pass')), updated_at timestamptz NOT NULL DEFAULT now())`);
  // The database's passing marker is this tree's own generation, as after a migrate + verify at this tree.
  await pg.query(`INSERT INTO ${ROLE_VALIDATION_TABLE} (name, generation, status) VALUES ('session_directory_search_worker', $1, 'pass')
    ON CONFLICT (name) DO UPDATE SET generation = EXCLUDED.generation, status = 'pass'`, [live]);
});
afterAll(async () => { await pg?.close(); });

function manifest(session: string): ManifestV1 {
  return {
    schema_v: 1, layout: "v1", trace_id: traceId, revision: 1,
    created_at: "2026-11-27T04:00:00.000Z", updated_at: "2026-11-27T04:00:01.000Z",
    session: { workspace_id: "ws-test", machine_id: "srv_" + "a".repeat(32), harness: "codex", session_id: session, started_at: "2026-11-27T04:00:00.000Z" },
    writer: { delivery_instance_id: "srv_" + "a".repeat(32), plugin_version: "test" },
    raw: {
      owner: "srv_" + "a".repeat(32), state: "open", updated_at: "2026-11-27T04:00:01.000Z", format: "jsonl", compression: "zstd",
      chunk_count: 1, raw_bytes: 12, stored_bytes: 10, content_sha256: "1".repeat(64), epoch: 0,
      epochs: [{ epoch: 0, opened_at: "2026-11-27T04:00:00.000Z", reason: "initial", first_seq: 0, opened_by: "srv_" + "a".repeat(32) }],
      next_offset: 12,
      chunks: [{ seq: 0, epoch: 0, start_offset: 0, bytes: 12, stored_bytes: 10, sha256: "2".repeat(64), sha256_8: "22222222", stored_sha256: "3".repeat(64), compression: "zstd" }],
      scrub: { ruleset: "relayhistory-scrub", version: 1 },
    },
    extensions: { "96bulls": { session_uuid: "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c19", session_uuid_source: "minted" } },
  } as ManifestV1;
}

async function write(session: string, schemaGeneration: string) {
  const machineId = "srv_" + "a".repeat(32);
  const manifestKey = `96bulls/org-test/v1/ws=ws-test/m=${machineId}/h=codex/d=2026/11/27/s=${session}/manifest.json`;
  const m = manifest(session);
  const index = hoopDbSessionIndex({
    db, reader: { provider: "s3", bucket: "locker", read: async () => ({ manifest: m, etag: "etag" }) },
    queue: { backend: "memory", send: async () => {}, sendBatch: async () => {} }, current: () => undefined,
    publication: { schemaGeneration, transact },
  });
  const pointer: RawPointer = { traceId, workspaceId: "ws-test", machineId, harness: "codex", sessionId: session, manifestKey, provider: "s3", bucket: "locker" };
  const ctx: TraceContext = { traceId, orgId: "org-test", workspaceId: "ws-test", machineId, harness: "codex", sessionId: session };
  return index.upsertRawPointer(pointer, ctx);
}
const rows = async (session: string) =>
  (await pg.query<{ n: number }>("SELECT count(*)::int AS n FROM sessions.session_index WHERE org_id='org-test' AND session_id=$1", [session])).rows[0]!.n;

test("a generation other than the passing marker is refused 55000 'generation not ready' and writes nothing", async () => {
  expect(AHEAD).not.toBe(live);
  const err = await write("session-ahead", AHEAD).then(() => null, (e: unknown) => e as { code?: string; message?: string });
  expect(err?.code).toBe("55000");
  expect(err?.message).toBe("session ingest: generation not ready");
  expect(await rows("session-ahead")).toBe(0);
});

test("the passing marker's own generation indexes the pointer, and the row reads back", async () => {
  // upsertRawPointer resolves void on a write and { status: "skipped" } only when it handled the event without one.
  const result = await write("session-live", live);
  expect(result).toBeUndefined();
  expect(await rows("session-live")).toBe(1);
  const back = (await pg.query<{ manifest_key: string; revision: string; deleted: boolean }>(
    "SELECT manifest_key, revision::text, deleted_at IS NOT NULL AS deleted FROM sessions.session_index WHERE org_id='org-test' AND session_id='session-live'")).rows[0]!;
  expect(back.manifest_key).toContain("/s=session-live/manifest.json");
  expect(back.revision).toBe("1");
  expect(back.deleted).toBe(false);
});
