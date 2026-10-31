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
  deletionFenced,
  eventTimeMs,
  planSession,
  originMachines,
  sessionRecords,
  sessionsNewestFirst,
  undeliveredEvents,
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

  it("plans per machine stream, and skips sessions a user deleted or reset", async () => {
    const [key] = await sessionsNewestFirst(db, null, 1);
    expect(await planSession(db, key!)).toEqual([
      expect.objectContaining({ machine: "laptop", records: 1, chunks: 1 }),
      expect.objectContaining({ machine: "unknown-origin-orig-b", records: 1, chunks: 1 }),
    ]);
    await db.exec(`INSERT INTO sessions.deleted_upload_sessions VALUES ('o', 'w', 'u', 'orig-a', 'claude', 's-1')`);
    expect(await deletionFenced(db, key!)).toBe(true);
    expect(await planSession(db, key!)).toBe("fenced");
    await db.exec(`DELETE FROM sessions.deleted_upload_sessions`);
    const other = (await sessionsNewestFirst(db, null, 2))[1]!;
    expect(await deletionFenced(db, other)).toBe(false);
  });

  it("exports sessions whose content exists only as undelivered events (ingest-API harnesses)", async () => {
    await db.exec(`SET session_replication_role = replica;
      INSERT INTO sessions.convergence_events
        (org_id, workspace_id, machine_id, user_id, source, session_id, event_id, kind, type, ts, content, record)
      VALUES ('o', 'w', 'server', 'u', 'claude', 's-old', 'ev-2', 'assistant', 'message', '2026-09-01T00:00:02Z', 'answer', '{}'),
             ('o', 'w', 'server', 'u', 'claude', 's-old', 'ev-1', 'user', 'message', '2026-09-01T00:00:01Z', 'question', '{}');
      SET session_replication_role = origin;`);
    const key = (await sessionsNewestFirst(db, null, 10)).find((s) => s.session_id === "s-old")!;
    const plan = await planSession(db, key);
    expect(plan).toEqual([expect.objectContaining({ machine: "server", records: 2, chunks: 1 })]);
    const events = await undeliveredEvents(db, key);
    const stream = buildSessionStream(events);
    const streamed = lines(stream.bytes);
    expect(streamed.map((line) => line.record_id)).toEqual(["ev-1", "ev-2"]);
    expect(streamed[0]).toMatchObject({ kind: "convergence_event", payload: { content: "question", kind: "user" } });
    expect(streamed[0].payload.org_id).toBeUndefined();
  });

  describe("numbers past a double's precision", () => {
    const key = { org_id: "o", workspace_id: "w", source: "claude", session_id: "s-exact" } as never;
    const TWO_53_PLUS_1 = "9007199254740993";
    const TWO_63 = "9223372036854775808";
    const MAX = "9223372036854775807";
    beforeAll(async () => {
      await db.exec(`SET session_replication_role = replica;
        INSERT INTO sessions.delivery_records
          (org_id, workspace_id, origin_id, record_id, revision_id, revision, digest, kind, source, session_id, operation, payload, user_id, received_at)
        VALUES ('o', 'w', 'a', 'big', 'v1', 1, 'd', 'session_event', 'claude', 's-exact', 'upsert',
                '{"ts_ms": 1, "small": ${TWO_53_PLUS_1}, "large": ${TWO_63}, "fine": 0.1234567890123456789}', 'u', '2026-10-01T00:00:00Z');
        INSERT INTO sessions.convergence_events
          (org_id, workspace_id, machine_id, user_id, source, session_id, event_id, kind, type, ts, record, input_tokens, cost_usd_micros)
        VALUES ('o', 'w', 'server', 'u', 'claude', 's-exact', 'ev', 'user', 'message', '2026-09-01T00:00:01.5Z', '{}', ${MAX}, ${TWO_53_PLUS_1});
        SET session_replication_role = origin;`);
    });
    const text = (records: DeliveryRecord[]) => new TextDecoder().decode(buildSessionStream(records).bytes);

    it("carries a delivery payload's numbers to the line byte for byte", async () => {
      const line = text(await sessionRecords(db, key));
      for (const number of [TWO_53_PLUS_1, TWO_63, "0.1234567890123456789"]) expect(line).toContain(number);
    });

    it("carries a direct event's bigint columns to the line byte for byte", async () => {
      const line = text(await undeliveredEvents(db, key));
      expect(line).toContain(`"input_tokens": ${MAX}`);
      expect(line).toContain(`"cost_usd_micros": ${TWO_53_PLUS_1}`);
      expect(lines(new TextEncoder().encode(line))[0].payload.ts_ms).toBe(Date.parse("2026-09-01T00:00:01.5Z"));
    });

    it("still writes a parsed payload when there is no source text", () => {
      const stream = buildSessionStream([record({ record_id: "a", payload: { ts_ms: 1 } })]);
      expect(lines(stream.bytes)[0].payload).toEqual({ ts_ms: 1 });
    });
  });

  describe("the write the live projection keeps", () => {
    const LATER = "2026-10-01T00:00:09Z";
    const EARLIER = "2026-10-01T00:00:01Z";
    const key = { org_id: "o", workspace_id: "w", source: "claude", session_id: "s-ledger" } as never;
    beforeAll(async () => {
      const put = (origin: string, record: string, kind: string, operation: string, payload: string, at: string) =>
        `('o', 'w', '${origin}', '${record}', 'v1', 1, 'd', '${kind}', 'claude', 's-ledger', '${operation}', ${payload}, 'u', '${at}')`;
      const seat = (origin: string, record: string, seq: number) => `('o', 'w', '${origin}', '${record}', ${seq})`;
      await db.exec(`SET session_replication_role = replica;
        INSERT INTO sessions.delivery_records
          (org_id, workspace_id, origin_id, record_id, revision_id, revision, digest, kind, source, session_id, operation, payload, user_id, received_at)
        VALUES
          ${put("a", "kept", "session_event", "upsert", `'{"ts_ms": 1, "text": "first"}'`, LATER)},
          ${put("b", "kept", "session_event", "upsert", `'{"ts_ms": 1, "text": "last"}'`, EARLIER)},
          ${put("a", "gone", "session_event", "upsert", `'{"ts_ms": 1, "text": "x"}'`, LATER)},
          ${put("b", "gone", "session_event", "delete", "NULL", EARLIER)},
          ${put("a", "unseated", "session_event", "upsert", `'{"ts_ms": 1, "text": "projected"}'`, EARLIER)},
          ${put("b", "unseated", "session_event", "upsert", `'{"ts_ms": 1, "text": "later"}'`, LATER)},
          ${put("a", "cat", "session", "upsert", `'{"title": "first"}'`, LATER)},
          ${put("b", "cat", "session", "upsert", `'{"title": "last"}'`, EARLIER)};
        INSERT INTO sessions.delivery_projection_acceptance (org_id, workspace_id, origin_id, record_id, accepted_seq) VALUES
          ${seat("a", "kept", 1)}, ${seat("b", "kept", 2)}, ${seat("a", "gone", 3)}, ${seat("b", "gone", 4)},
          ${seat("a", "cat", 9)}, ${seat("b", "cat", 1)};
        INSERT INTO sessions.delivery_catalog_acceptance (org_id, workspace_id, origin_id, record_id, accepted_seq) VALUES
          ${seat("a", "cat", 1)}, ${seat("b", "cat", 2)};
        INSERT INTO sessions.convergence_events
          (org_id, workspace_id, machine_id, user_id, source, session_id, event_id, kind, type, ts, record, delivery_record_id)
        VALUES ('o', 'w', 'delivery:' || md5(jsonb_build_array('w', 'a')::text), 'u', 'claude', 's-ledger', 'e-u', 'session_event', 'user', now(), '{}', 'unseated');
        SET session_replication_role = origin;`);
    });
    const records = () => sessionRecords(db, key);
    const held = async (id: string) => (await records()).filter((record) => record.record_id === id);

    it("keeps the origin accepted last, whatever received_at says", async () => {
      expect((await held("kept")).map((r) => [r.origin_id, r.accepted_seq])).toEqual([["b", "2"]]);
    });

    it("lets a tombstone accepted last remove the other origin's copy", async () => {
      const kept = await records();
      expect((await held("gone")).map((r) => [r.origin_id, r.operation])).toEqual([["b", "delete"]]);
      const stream = buildSessionStream(kept);
      expect(lines(stream.bytes).map((line) => line.record_id)).not.toContain("gone");
    });

    it("settles a catalog record by the catalog ledger, and carries its seat", async () => {
      expect((await held("cat")).map((r) => [r.origin_id, r.accepted_seq])).toEqual([["b", "2"]]);
      const stream = buildSessionStream(await held("cat"));
      expect(lines(stream.bytes)[0]).toMatchObject({ accepted_seq: "2" });
    });

    it("falls back to the projected row for a record with no seat", async () => {
      expect((await held("unseated")).map((r) => r.origin_id)).toEqual(["a"]);
    });
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
