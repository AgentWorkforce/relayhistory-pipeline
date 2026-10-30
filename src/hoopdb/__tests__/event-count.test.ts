/**
 * event_count against the real `sessions.upsert_session_index`, and against the
 * Rust writer's backfill manifest.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { PGlite } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/index.js";
import { eventCount, hoopDbSessionIndex } from "../index.ts";
import { parseManifest, type ManifestV1, type RawPointer, type TraceContext } from "../../shared/vendor.ts";
import type { HoopDb } from "../../shared/db.ts";

const SUPABASE = join(import.meta.dir, "../../../supabase");
const FIXTURE = JSON.parse(
  readFileSync(join(import.meta.dir, "../../../packages/object-store/test/fixtures/rust-manifest-v1-backfill.json"), "utf8"),
) as { key: string; manifest: ManifestV1 };

let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.exec(readFileSync(join(SUPABASE, "tests/supabase_shim.sql"), "utf8"));
  for (const file of readdirSync(join(SUPABASE, "migrations")).sort())
    await pg.exec(readFileSync(join(SUPABASE, "migrations", file), "utf8"));
  for (const file of ["0002_session_ext_and_summaries.sql", "0003_fanout_ledger.sql"])
    await pg.exec(readFileSync(join(import.meta.dir, "../migrations", file), "utf8"));
}, 60_000);
afterAll(async () => {
  await pg.close();
});

/** The Rust fixture, as a plain live manifest or with its backfill epochs kept. */
function manifest(sessionId: string, revision: number, backfilled: boolean): ManifestV1 {
  const m = structuredClone(FIXTURE.manifest);
  m.session.session_id = sessionId;
  m.revision = revision;
  if (!backfilled) {
    m.raw.epochs = m.raw.epochs.filter((epoch) => epoch.reason === "initial");
    m.raw.chunks = m.raw.chunks.filter((chunk) => chunk.epoch === 0);
  }
  return m;
}

async function index(m: ManifestV1) {
  const query = async (text: string, params?: unknown[]) => ({ rows: (await pg.query(text, params as never)).rows });
  const s = m.session;
  const pointer = { traceId: m.trace_id, workspaceId: s.workspace_id, machineId: s.machine_id, harness: s.harness, sessionId: s.session_id, manifestKey: FIXTURE.key, provider: "s3", bucket: "b" } as RawPointer;
  const ctx = { traceId: m.trace_id, orgId: "org", workspaceId: s.workspace_id, machineId: s.machine_id, harness: s.harness, sessionId: s.session_id } as TraceContext;
  await hoopDbSessionIndex({
    db: { query } as unknown as HoopDb,
    reader: { provider: "s3", bucket: "b", read: async () => ({ manifest: m, etag: `e${m.revision}` }) } as never,
    queue: { backend: "memory", send: async () => {}, sendBatch: async () => {} } as never,
    current: () => undefined,
  }).upsertRawPointer(pointer, ctx);
  const { rows } = await pg.query<{ event_count: number }>(
    "SELECT event_count FROM sessions.session_index WHERE session_id = $1",
    [s.session_id],
  );
  return rows[0]?.event_count;
}

describe("event_count", () => {
  const count = (m: ManifestV1) => m.raw.chunks.reduce((n, c) => n + (c.records ?? 0), 0);
  const lines = count(FIXTURE.manifest);
  const live = count(manifest("live", 1, false));

  test("the fixture's chunk records would over-count if summed", () => {
    expect(lines).toBeGreaterThan(live);
    expect(eventCount(FIXTURE.manifest)).toBeNull();
  });

  test.each([["backfill"], ["resumed"]])("sends no count for a %s epoch", (reason) => {
    const m = manifest("any", 1, false);
    m.raw.epochs.push({ epoch: 1, opened_at: m.created_at, reason: reason as "backfill", first_seq: 0, opened_by: "x" });
    expect(eventCount(m)).toBeNull();
  });

  test("still sums chunk records with neither", async () => {
    expect(await index(manifest("plain", 1, false))).toBe(live);
  });

  test("a first index of a backfilled session stores the column's 0, never the sum", async () => {
    expect(await index(manifest("cold", 1, true))).toBe(0);
  });

  test("a backfill keeps the stored count rather than adding the export's lines", async () => {
    expect(await index(manifest("straddling", 1, false))).toBe(live);
    expect(await index(manifest("straddling", 2, true))).toBe(live);
  });

  // What an un-upgraded reader does: the manifest stays schema_v 1, so a reader
  // that predates an epoch reason stops at validation and the row does not move.
  // Safe, but that session's index stalls until the reader is upgraded.
  test("stops at validation on an epoch reason it does not know", async () => {
    const future = structuredClone(FIXTURE.manifest) as { raw: { epochs: { reason: string }[] }; schema_v: number };
    future.raw.epochs.at(-1)!.reason = "compacted";
    expect(future.schema_v).toBe(1);
    await expect(parseManifest(JSON.stringify(future), FIXTURE.key)).rejects.toThrow();
  });
});
