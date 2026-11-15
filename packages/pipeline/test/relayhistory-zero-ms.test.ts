/**
 * A `*_ms` of 0 is "not reported", never 1970-01-01.
 *
 * The relayhistory Claude parser writes ts_ms 0 for a record that carries no
 * time. The export projection (`projectRelayhistorySession`) used to take it as
 * the epoch, and `settle` then gave every timeless tool call under that event
 * the epoch too, so the session's directory event times gained a 1970 row.
 *
 * TS: the event falls back to its delivery time and its child to the parent's.
 * SQL: the pinned upstream chain still accepts 0 (asserted, so a re-pin that
 * carries the upstream fix shows up here); with the proposed upstream migration
 * (integration/relayhistory-cloud/.../0068_delivery_zero_ms_not_reported.sql)
 * applied over it, delivery_activity_ms(0) and delivery_catalog_time(0) are
 * NULL, and the live triggers write exactly what the TS projection writes.
 */
import { PGlite } from "@electric-sql/pglite";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { readdirSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
  canonicalTimestamp,
  projectRelayhistorySession,
} from "../src/lib/pipeline/relayhistory-projection.js";
import {
  normalizeRelayhistoryRecords,
  parseRelayhistoryRecord,
} from "../src/lib/pipeline/relayhistory-transform.js";

const path = (p: string) =>
  fileURLToPath(new URL(`../../../${p}`, import.meta.url).href);
const MIGRATIONS = path("vendor/relayhistory-cloud/migrations/");
const PROPOSED = path(
  "integration/relayhistory-cloud/packages/relayhistory/db/migrations/0068_delivery_zero_ms_not_reported.sql",
);
const forPglite = (sql: string) =>
  sql
    .replace(/CREATE EXTENSION IF NOT EXISTS vector[^;]*;/gi, "")
    .replace(/CREATE (UNIQUE )?INDEX[^;]*USING hnsw[^;]*;/gi, "")
    .replace(/public\.vector\(\d+\)/g, "text")
    .replace(/\bCONCURRENTLY\b/gi, "");

const WORKSPACE = "workspace-1";
const SESSION = "session-1";
const RECEIVED = "2026-10-30 12:00:00.123456+00";
const RECEIVED_TS = canonicalTimestamp(RECEIVED);
const EPOCH_TS = canonicalTimestamp("1970-01-01T00:00:00.000Z");
const T = 1_793_000_000_000;

type Line = {
  kind: string;
  origin_id: string;
  record_id: string;
  revision: string;
  received_at: string;
  payload: Record<string, unknown>;
};
const line = (
  record_id: string,
  kind: string,
  payload: Record<string, unknown>,
): Line => ({
  kind,
  origin_id: "origin-a",
  record_id,
  revision: "1",
  received_at: RECEIVED,
  payload,
});

const context = {
  workspaceId: WORKSPACE,
  source: "claude",
  sessionId: SESSION,
  machineId: "machine:1",
  userId: "u",
};

/** The TS projection's events, by record id: [ts, ingested_at]. */
function projectedTimes(lines: Line[]) {
  const { rows } = projectRelayhistorySession(
    normalizeRelayhistoryRecords(
      lines.map((l) => parseRelayhistoryRecord(JSON.stringify(l))),
    ),
    context,
  );
  return Object.fromEntries(
    rows
      .filter((r) => r.table === "convergence_events")
      .map((r) => [
        String(r.row.delivery_record_id),
        canonicalTimestamp(String(r.row.ts)),
      ]),
  );
}

/** A Claude assistant event with ts_ms 0, and a timeless tool call under it. */
const ZERO_PARENT = [
  line("parent", "session_event", {
    role: "assistant",
    text: "calling a tool",
    tool_use_id: "t1",
    ts_ms: 0,
  }),
  line("child", "tool_call", { name: "Read", tool_use_id: "t1" }),
];

describe("export projection: ts_ms 0 is not a time", () => {
  it("a ts_ms 0 event takes received_at, and its timeless tool call takes the parent's time, never the epoch", () => {
    const times = projectedTimes(ZERO_PARENT);
    expect(times).toEqual({ parent: RECEIVED_TS, child: RECEIVED_TS });
    expect(Object.values(times)).not.toContain(EPOCH_TS);
  });

  it("a ts_ms 0 child of a parent with a real time takes the parent's real time", () => {
    const times = projectedTimes([
      line("parent", "session_event", {
        role: "assistant",
        tool_use_id: "t1",
        ts_ms: T,
      }),
      line("child", "tool_call", { name: "Read", tool_use_id: "t1", ts_ms: 0 }),
    ]);
    expect(times.child).toBe(canonicalTimestamp(new Date(T).toISOString()));
  });

  it("a ts_ms 0 parent does not pull a child below a real-time sibling parent to the epoch", () => {
    const times = projectedTimes([
      ...ZERO_PARENT,
      line("parent-real", "session_event", {
        role: "assistant",
        tool_use_id: "t1",
        ts_ms: T,
      }),
    ]);
    // The minimum over real parent times: T (2026-10-26) sorts before received_at.
    expect(times.child).toBe(canonicalTimestamp(new Date(T).toISOString()));
    expect(Object.values(times)).not.toContain(EPOCH_TS);
  });

  it("a 0 in an earlier key falls through to the next reported key", () => {
    const times = projectedTimes([
      line("x", "session_event", { role: "user", updated_ms: 0, ts_ms: T }),
    ]);
    expect(times.x).toBe(canonicalTimestamp(new Date(T).toISOString()));
  });

  it("keeps 1 ms as a real time and still refuses negatives", () => {
    const times = projectedTimes([
      line("one", "session_event", { role: "user", ts_ms: 1 }),
      line("neg", "session_event", { role: "user", ts_ms: -1, timestamp_ms: T }),
    ]);
    expect(times.one).toBe(canonicalTimestamp("1970-01-01T00:00:00.001Z"));
    expect(times.neg).toBe(RECEIVED_TS);
  });

  it("a catalog *_ms of 0 is NULL, not 1970-01-01", () => {
    const { rows } = projectRelayhistorySession(
      normalizeRelayhistoryRecords([
        parseRelayhistoryRecord(
          JSON.stringify(
            line("session", "session", {
              first_activity_ms: 0,
              last_activity_ms: T,
            }),
          ),
        ),
      ]),
      context,
    );
    const catalog = rows.find((r) => r.table === "session_catalog")!.row;
    expect(catalog.first_activity_at).toBeNull();
    expect(catalog.last_activity_at).toBe(
      canonicalTimestamp(new Date(T).toISOString()),
    );
  });
});

describe("SQL twin (sessions.delivery_activity_ms, sessions.delivery_catalog_time)", () => {
  let db: PGlite;
  beforeAll(async () => {
    db = new PGlite({ extensions: { pg_trgm } });
    await db.exec(`SET search_path = pg_catalog, public;
      CREATE SCHEMA IF NOT EXISTS sessions;
      CREATE TABLE IF NOT EXISTS sessions.__migrations (
        name text PRIMARY KEY, checksum text NOT NULL, applied_at timestamptz NOT NULL DEFAULT now());`);
    for (const file of readdirSync(MIGRATIONS)
      .filter((name) => name.endsWith(".sql"))
      .sort())
      await db.exec(forPglite(readFileSync(MIGRATIONS + file, "utf8")));
    const { rows } = await db.query<{ index_name: string; definition: string }>(
      "SELECT index_name, definition FROM sessions.delivery_rollout_indexes()",
    );
    for (const index of rows)
      await db.exec(`CREATE INDEX ${index.index_name} ${index.definition}`);
    await db.exec(`SET session_replication_role = replica;
      UPDATE sessions.upload_cleanup_rollout SET phase = 5 WHERE singleton;
      SET session_replication_role = origin;`);
    await db.exec(`SELECT sessions.activate_delivery_catalog();
      SELECT sessions.activate_delivery_projection_v2();
      SET search_path = public`);
  }, 120_000);
  afterAll(async () => {
    await db?.close();
  });

  const scalar = async (sql: string) =>
    (await db.query<{ v: unknown }>(`SELECT ${sql} AS v`)).rows[0]!.v;

  it("the pinned upstream chain still accepts 0 (drop the proposal once a re-pin carries the fix)", async () => {
    expect(
      Number(await scalar(`sessions.delivery_activity_ms('{"ts_ms":0}'::jsonb)`)),
    ).toBe(0);
  });

  it("with the proposed upstream migration, 0 is NULL and the twins agree", async () => {
    await db.exec(readFileSync(PROPOSED, "utf8"));
    const activity = (payload: string) =>
      scalar(`sessions.delivery_activity_ms('${payload}'::jsonb)::text`);
    expect(await activity('{"ts_ms":0}')).toBeNull();
    expect(await activity('{"ts_ms":0.0}')).toBeNull();
    expect(await activity('{"updated_ms":0,"ts_ms":5}')).toBe("5");
    expect(await activity('{"ts_ms":1}')).toBe("1");
    // A non-number is never cast (no error inside the delivery trigger) and is skipped, as before.
    expect(await activity('{"ts_ms":"abc","timestamp_ms":5}')).toBe("5");
    expect(await activity('{"ts_ms":-1}')).toBeNull();
    expect(await activity('{"ts_ms":8640000000000001}')).toBeNull();
    expect(
      await scalar(`sessions.delivery_catalog_time('0'::jsonb)`),
    ).toBeNull();
    expect(
      await scalar(`sessions.delivery_catalog_time('1'::jsonb) IS NOT NULL`),
    ).toBe(true);

    // The live triggers, through accept-equivalent inserts, write what the TS projection writes.
    for (const l of ZERO_PARENT)
      await db.query(
        `INSERT INTO sessions.delivery_records
           (org_id, workspace_id, origin_id, record_id, revision_id, revision, digest, kind, source,
            session_id, operation, payload, user_id, received_at)
         VALUES ('org', $1, $2, $3, $3 || ':1', 1, 'digest', $4, 'claude', $5, 'upsert', $6::jsonb, 'u', $7::timestamptz)`,
        [WORKSPACE, l.origin_id, l.record_id, l.kind, SESSION, JSON.stringify(l.payload), l.received_at],
      );
    const { rows } = await db.query<{ record: string; ts: string }>(
      `SELECT delivery_record_id AS record, ts::text AS ts FROM sessions.convergence_events ORDER BY delivery_record_id`,
    );
    const live = Object.fromEntries(
      rows.map((r) => [r.record, canonicalTimestamp(r.ts)]),
    );
    expect(live).toEqual(projectedTimes(ZERO_PARENT));
    expect(Object.values(live)).not.toContain(EPOCH_TS);
  });
});
