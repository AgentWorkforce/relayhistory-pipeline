/**
 * Neon-to-S3 export core (`src/migration/relayhistory-export.ts`): the stream
 * builder and chunker, and the Neon reader on the pinned legacy schema.
 */
import { PGlite } from "@electric-sql/pglite";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { readdirSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
  buildSessionStream,
  byMachine,
  chunkRanges,
  eventTimeMs,
  originMachines,
  sessionRecords,
  sessionsNewestFirst,
  type DeliveryRecord,
} from "../../../src/migration/relayhistory-export.js";

const record = (overrides: Partial<DeliveryRecord> & { record_id: string }): DeliveryRecord => ({
  kind: "session_event",
  origin_id: "o-1",
  revision: "1",
  operation: "upsert",
  received_at: "2026-10-01T00:00:00Z",
  payload: { ts_ms: 1_000, text: "x" },
  ...overrides,
});
const lines = (bytes: Uint8Array) =>
  new TextDecoder().decode(bytes).trimEnd().split("\n").map((line) => JSON.parse(line));

describe("session stream", () => {
  it("keeps live records only, in event-time order, and is deterministic", () => {
    const records = [
      record({ record_id: "c", payload: { ts_ms: 3_000 } }),
      record({ record_id: "gone", operation: "delete", payload: null }),
      record({ record_id: "a", payload: { timestamp_ms: 1_000 } }),
      record({ record_id: "b", origin_id: "o-0", payload: { ts_ms: 1_000 } }),
      record({ record_id: "r", kind: "session", payload: { first_activity_ms: 500 } }),
    ];
    const stream = buildSessionStream(records);
    expect(lines(stream.bytes).map((line) => line.record_id)).toEqual(["r", "b", "a", "c"]);
    expect(stream).toMatchObject({ records: 4, firstMs: 500, lastMs: 3_000 });
    expect(buildSessionStream([...records].reverse()).bytes).toEqual(stream.bytes);
    expect(lines(stream.bytes)[0]).toEqual({
      kind: "session",
      origin_id: "o-1",
      record_id: "r",
      revision: "1",
      received_at: "2026-10-01T00:00:00Z",
      payload: { first_activity_ms: 500 },
    });
  });

  it("falls back to receipt time when the payload has no clock", () => {
    expect(eventTimeMs(record({ record_id: "x", payload: {}, received_at: "2026-10-02T00:00:00Z" }))).toBe(
      Date.parse("2026-10-02T00:00:00Z"),
    );
  });

  it("chunks at record boundaries within the cap, never splitting a record", () => {
    const records = Array.from({ length: 10 }, (_, i) =>
      record({ record_id: `r${i}`, payload: { ts_ms: i, pad: "y".repeat(i === 7 ? 500 : 20) } }),
    );
    const stream = buildSessionStream(records);
    const ranges = chunkRanges(stream, 200);
    expect(ranges[0]!.start).toBe(0);
    expect(ranges.at(-1)!.end).toBe(stream.bytes.byteLength);
    for (const [i, range] of ranges.entries()) {
      if (i > 0) expect(range.start).toBe(ranges[i - 1]!.end);
      expect(stream.bytes[range.end - 1]).toBe(0x0a); // ends on a newline
      if (range.records > 1) expect(range.end - range.start).toBeLessThanOrEqual(200);
    }
    expect(ranges.reduce((n, r) => n + r.records, 0)).toBe(10);
    // The oversized record is alone in its chunk.
    expect(ranges.some((r) => r.records === 1 && r.end - r.start > 200)).toBe(true);
    expect(chunkRanges(buildSessionStream([]))).toEqual([]);
  });
});

describe("Neon reader on the pinned legacy schema", () => {
  const MIGRATIONS = fileURLToPath(
    new URL("../../../vendor/relayhistory-cloud/migrations/", import.meta.url).href,
  );
  const forPglite = (sql: string) =>
    sql
      .replace(/CREATE EXTENSION IF NOT EXISTS vector[^;]*;/gi, "")
      .replace(/CREATE (UNIQUE )?INDEX[^;]*USING hnsw[^;]*;/gi, "")
      .replace(/public\.vector\(\d+\)/g, "text")
      .replace(/\bCONCURRENTLY\b/gi, "");
  let db: PGlite;
  beforeAll(async () => {
    db = new PGlite({ extensions: { pg_trgm } });
    await db.exec(`SET search_path = pg_catalog, public;
      CREATE SCHEMA IF NOT EXISTS sessions;
      CREATE TABLE IF NOT EXISTS sessions.__migrations (
        name text PRIMARY KEY, checksum text NOT NULL, applied_at timestamptz NOT NULL DEFAULT now());`);
    for (const file of readdirSync(MIGRATIONS).filter((name) => name.endsWith(".sql")).sort())
      await db.exec(forPglite(readFileSync(MIGRATIONS + file, "utf8")));
    // Seed as the backfill would see Neon: rows only, no triggers.
    await db.exec(`SET search_path = public; SET session_replication_role = replica;
      INSERT INTO sessions.delivery_records
        (org_id, workspace_id, origin_id, record_id, revision_id, revision, digest, kind, source, session_id, operation, payload, user_id, received_at)
      VALUES
        ('o', 'w', 'orig-a', 'rec-1', 'v1', 1, 'd1', 'session_event', 'claude', 's-1', 'upsert', '{"ts_ms": 2000, "text": "hi"}', 'u', '2026-10-01T00:00:00Z'),
        ('o', 'w', 'orig-a', 'rec-2', 'v2', 2, 'd2', 'tool_call', 'claude', 's-1', 'delete', NULL, 'u', '2026-10-01T00:00:00Z'),
        ('o', 'w', 'orig-b', 'rec-3', 'v1', 1, 'd3', 'session', 'claude', 's-1', 'upsert', '{"first_activity_ms": 1000, "title": "t"}', 'u', '2026-10-01T00:00:00Z'),
        ('o', 'w', 'orig-a', 'rec-9', 'v1', 1, 'd9', 'session_event', 'claude', 's-other', 'upsert', '{"ts_ms": 1}', 'u', '2026-10-01T00:00:00Z');
      INSERT INTO sessions.convergence_events
        (org_id, workspace_id, machine_id, user_id, source, session_id, event_id, kind, type, ts, record, delivery_record_id)
      VALUES ('o', 'w', 'laptop', 'u', 'claude', 's-1', 'e-1', 'session_event', 'user', '2026-10-01T00:00:02Z', '{}', 'rec-1');
      SET session_replication_role = origin;`);
    // Rollup rows: given keys and times, every other required column zeroed
    // (read from the catalog, so new NOT NULL columns don't break the seed).
    const required = (
      await db.query<{ name: string; type: string }>(
        `SELECT attname AS name, format_type(atttypid, atttypmod) AS type FROM pg_attribute
          WHERE attrelid = 'sessions.session_rollups'::regclass AND attnum > 0 AND NOT attisdropped
            AND attnotnull AND NOT atthasdef
            AND attname NOT IN ('org_id', 'session_id', 'workspace_id', 'source', 'first_ts', 'last_ts')`,
      )
    ).rows;
    const zero = (type: string) =>
      type.endsWith("[]") ? "'{}'" : type === "jsonb" ? "'[]'" : type.startsWith("timestamp") ? "now()" : type === "text" ? "''" : "0";
    await db.exec(`SET session_replication_role = replica;
      INSERT INTO sessions.session_rollups (org_id, session_id, workspace_id, source, project_id, first_ts, last_ts${required.map((c) => `, ${c.name}`).join("")})
      SELECT 'o', s, 'w', 'claude', p, ts - interval '1 day', ts${required.map((c) => `, ${zero(c.type)}`).join("")}
        FROM (VALUES ('s-1', NULL, timestamptz '2026-10-20'), ('s-1', 'proj', timestamptz '2026-10-25'),
                     ('s-old', NULL, timestamptz '2026-09-01'), ('s-other', NULL, timestamptz '2026-10-10')) AS v(s, p, ts);
      SET session_replication_role = origin;`);
  }, 120_000);
  afterAll(async () => {
    await db?.close();
  });

  it("pages sessions newest first across rollups", async () => {
    const first = await sessionsNewestFirst(db, null, 2);
    expect(first.map((s) => s.session_id)).toEqual(["s-1", "s-other"]);
    const next = await sessionsNewestFirst(db, first.at(-1)!, 2);
    expect(next.map((s) => s.session_id)).toEqual(["s-old"]);
  });

  it("reads a session's records and splits them by the machine their events name", async () => {
    const [key] = await sessionsNewestFirst(db, null, 1);
    const records = await sessionRecords(db, key!);
    expect(records.map((r) => r.record_id).sort()).toEqual(["rec-1", "rec-2", "rec-3"]);
    const machines = await originMachines(db, key!);
    expect([...machines]).toEqual([["orig-a", "laptop"]]);
    const groups = byMachine(records, machines);
    expect([...groups.keys()].sort()).toEqual(["laptop", "unknown-origin-orig-b"]);
    // The delete is read but never streamed.
    const laptop = buildSessionStream(groups.get("laptop")!);
    expect(lines(laptop.bytes).map((line) => line.record_id)).toEqual(["rec-1"]);
  });
});
