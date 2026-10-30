/**
 * The T0 copy set (mirror) and history backfill from Neon to Supabase
 * (`src/migration/relayhistory-copy.ts`), between two in-process Postgres
 * databases standing in for Neon (source) and Supabase (target).
 */
import { PGlite } from "@electric-sql/pglite";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
  copyTable,
  planTable,
  prepareBackfillFence,
  prepareTargetSession,
  type Queryable,
} from "../../../src/migration/relayhistory-copy.js";

const DDL = `CREATE SCHEMA sessions;
CREATE TABLE sessions.tokens (
  org_id text NOT NULL,
  id bigint NOT NULL,
  secret bytea,
  seen_local timestamp without time zone,
  seen_at timestamptz,
  meta jsonb,
  tags text[],
  label text,
  label_upper text GENERATED ALWAYS AS (upper(label)) STORED,
  PRIMARY KEY (org_id, id)
);`;

let source: PGlite;
let target: PGlite;
beforeEach(async () => {
  [source, target] = [new PGlite(), new PGlite()];
  await Promise.all([source.exec(DDL), target.exec(DDL)]);
});
afterEach(async () => {
  await Promise.all([source.close(), target.close()]);
});

const BIG = "9007199254740993"; // 2^53 + 1: lost if parsed as a JS number
const insert = (db: PGlite, rows: string) =>
  db.exec(
    `INSERT INTO sessions.tokens (org_id, id, secret, seen_local, seen_at, meta, tags, label) VALUES ${rows}`,
  );
const dump = async (db: PGlite) =>
  (
    await db.query<Record<string, unknown>>(
      `SELECT org_id, id::text, encode(secret, 'hex') AS secret, seen_local::text,
              seen_at::text, meta, tags, label, label_upper
         FROM sessions.tokens ORDER BY org_id, id`,
    )
  ).rows;

describe("mirror (T0 copy set)", () => {
  it("makes the target equal to the source across pages, types intact", async () => {
    await insert(
      source,
      `('a', 1, '\\xdeadbeef', '2026-10-30 12:34:56.789', '2026-10-30 12:00:00+00', '{"k":[1,{"x":null}]}', '{x,"y z"}', 'one'),
       ('a', ${BIG}, NULL, NULL, NULL, NULL, NULL, 'big'),
       ('b', 2, '\\x00', '2026-01-01 00:00:00', '2026-01-01 00:00:00+00', '[]', '{}', 'two'),
       ('c', 3, NULL, NULL, NULL, NULL, NULL, 'three')`,
    );
    // Target has a stale copy of one row, a row the source deleted mid-range,
    // and target-only rows past the source's last key.
    await insert(
      target,
      `('a', 1, '\\x01', NULL, NULL, NULL, NULL, 'old'),
       ('a', 5, NULL, NULL, NULL, NULL, NULL, 'gone'),
       ('d', 9, NULL, NULL, NULL, NULL, NULL, 'tail-1'),
       ('e', 9, NULL, NULL, NULL, NULL, NULL, 'tail-2')`,
    );
    const plan = await planTable(source as Queryable, target as Queryable, "tokens");
    expect(plan.columns).not.toContain("label_upper");
    const result = await copyTable(source as Queryable, target as Queryable, plan, {
      mode: "mirror",
      chunkSize: 2,
    });
    expect(await dump(target)).toEqual(await dump(source));
    expect(result).toMatchObject({ complete: true, read: 4, deleted: 3 });

    // A catch-up pass with nothing changed writes nothing.
    const again = await copyTable(source as Queryable, target as Queryable, plan, {
      mode: "mirror",
      chunkSize: 2,
    });
    expect(again).toMatchObject({ written: 0, deleted: 0, read: 4 });
  });

  it("keeps rows only the target has when the table is shared", async () => {
    await insert(source, `('a', 1, NULL, NULL, NULL, NULL, NULL, 'from-neon')`);
    await insert(
      target,
      `('a', 1, NULL, NULL, NULL, NULL, NULL, 'stale'),
       ('p', 7, NULL, NULL, NULL, NULL, NULL, 'pipeline-only')`,
    );
    const plan = await planTable(source as Queryable, target as Queryable, "tokens");
    const result = await copyTable(source as Queryable, target as Queryable, plan, {
      mode: "mirror",
      chunkSize: 10,
      keepTargetOnly: true,
    });
    expect(result).toMatchObject({ written: 1, deleted: 0 });
    expect((await dump(target)).map((row) => row.label)).toEqual([
      "from-neon",
      "pipeline-only",
    ]);
  });

  it("changes nothing on a dry run", async () => {
    await insert(source, `('a', 1, NULL, NULL, NULL, NULL, NULL, 'one')`);
    await insert(target, `('z', 1, NULL, NULL, NULL, NULL, NULL, 'stale')`);
    const plan = await planTable(source as Queryable, target as Queryable, "tokens");
    const result = await copyTable(source as Queryable, target as Queryable, plan, {
      mode: "mirror",
      chunkSize: 10,
      dryRun: true,
    });
    expect(result).toMatchObject({ read: 1, written: 0, deleted: 1 });
    expect(await dump(target)).toHaveLength(1);
    expect((await dump(target))[0]).toMatchObject({ org_id: "z" });
  });
});

describe("backfill (history after T0)", () => {
  it("inserts missing rows newest first and never touches rows written after T0", async () => {
    await insert(
      source,
      `('a', 1, NULL, NULL, NULL, NULL, NULL, 'h1'),
       ('a', 2, NULL, NULL, NULL, NULL, NULL, 'h2'),
       ('a', 3, NULL, NULL, NULL, NULL, NULL, 'h3-pre-T0'),
       ('b', 1, NULL, NULL, NULL, NULL, NULL, 'h4')`,
    );
    // Written on Supabase after T0, same key as a source row: fresh data wins.
    await insert(target, `('a', 3, NULL, NULL, NULL, NULL, NULL, 'fresh-after-T0')`);
    const plan = await planTable(source as Queryable, target as Queryable, "tokens");
    const pages: unknown[] = [];
    const result = await copyTable(source as Queryable, target as Queryable, plan, {
      mode: "backfill",
      chunkSize: 2,
      onPage: async (progress) => {
        pages.push(progress.after);
      },
    });
    expect(result).toMatchObject({ complete: true, read: 4, written: 3, deleted: 0 });
    // Newest key first: the first page ends at ('a','3'), then ('a','1').
    expect(pages.slice(0, 2)).toEqual([
      ["a", "3"],
      ["a", "1"],
    ]);
    const labels = (await dump(target)).map((row) => row.label);
    expect(labels).toEqual(["h1", "h2", "fresh-after-T0", "h4"]);
  });

  it("resumes from a checkpoint without rereading finished pages", async () => {
    await insert(
      source,
      `('a', 1, NULL, NULL, NULL, NULL, NULL, 'x'), ('a', 2, NULL, NULL, NULL, NULL, NULL, 'y'),
       ('a', 3, NULL, NULL, NULL, NULL, NULL, 'z')`,
    );
    const plan = await planTable(source as Queryable, target as Queryable, "tokens");
    const resumed = await copyTable(source as Queryable, target as Queryable, plan, {
      mode: "backfill",
      chunkSize: 10,
      after: ["a", "3"],
    });
    expect(resumed).toMatchObject({ read: 2, written: 2 });
    expect((await dump(target)).map((row) => row.id)).toEqual(["1", "2"]);
  });
});

describe("check (Neon fallback removal criterion)", () => {
  it("counts source rows Supabase lacks, NULL keys included, without writing", async () => {
    const ddl = `CREATE TABLE sessions.rollups (
      org_id text NOT NULL, session_id text NOT NULL, project_id text, n int);
      CREATE UNIQUE INDEX rollups_key ON sessions.rollups (org_id, session_id, project_id) NULLS NOT DISTINCT;`;
    await Promise.all([source.exec(ddl), target.exec(ddl)]);
    await source.exec(`INSERT INTO sessions.rollups VALUES
      ('o', 's1', NULL, 1), ('o', 's2', 'p', 2), ('o', 's3', NULL, 3)`);
    // Present (a changed value is fine after T0) and one missing NULL-keyed row.
    await target.exec(`INSERT INTO sessions.rollups VALUES ('o', 's1', NULL, 99), ('o', 's2', 'p', 2)`);
    const plan = await planTable(source as Queryable, target as Queryable, "rollups");
    const result = await copyTable(source as Queryable, target as Queryable, plan, {
      mode: "check",
      chunkSize: 2,
    });
    expect(result).toMatchObject({ read: 3, missing: 1, written: 0 });
    expect((await target.query("SELECT count(*)::int AS n FROM sessions.rollups")).rows).toEqual([
      { n: 2 },
    ]);
    await copyTable(source as Queryable, target as Queryable, plan, { mode: "backfill", chunkSize: 2 });
    expect(
      await copyTable(source as Queryable, target as Queryable, plan, { mode: "check", chunkSize: 2 }),
    ).toMatchObject({ missing: 0 });
  });
});

describe("deletions made on Supabase after T0", () => {
  const FENCE_DDL = `CREATE TABLE sessions.deleted_upload_sessions (
      org_id text NOT NULL, workspace_id text NOT NULL, user_id text NOT NULL,
      origin_id text NOT NULL, source text NOT NULL, session_id text NOT NULL,
      PRIMARY KEY (org_id, workspace_id, user_id, origin_id, source, session_id));
    CREATE TABLE sessions.session_resets (
      org_id text NOT NULL, workspace_id text NOT NULL, user_id text NOT NULL,
      request_id text NOT NULL, deleted bigint NOT NULL,
      PRIMARY KEY (org_id, workspace_id, user_id, request_id));
    CREATE TABLE sessions.convergence_events (
      org_id text NOT NULL, workspace_id text NOT NULL, user_id text NOT NULL,
      source text NOT NULL, session_id text NOT NULL, event_id text NOT NULL,
      PRIMARY KEY (org_id, event_id));
    CREATE TABLE sessions.session_rollups (
      org_id text NOT NULL, session_id text NOT NULL, n int, PRIMARY KEY (org_id, session_id));`;

  it("never resurrects a deleted upload or a reset user's history", async () => {
    await Promise.all([source.exec(FENCE_DDL), target.exec(FENCE_DDL)]);
    await source.exec(`INSERT INTO sessions.convergence_events VALUES
        ('o', 'w', 'u1', 'claude', 's1', 'e1'), ('o', 'w', 'u1', 'claude', 's1b', 'e1b'),
        ('o', 'w', 'u2', 'claude', 's2', 'e2'), ('o', 'w', 'u3', 'claude', 's3', 'e3');
      INSERT INTO sessions.session_rollups VALUES ('o', 's1', 1), ('o', 's2', 2), ('o', 's3', 3);
      -- u3 reset before T0: already on both sides; their later history stays.
      INSERT INTO sessions.session_resets VALUES ('o', 'w', 'u3', 'r-pre', 0);`);
    await target.exec(`INSERT INTO sessions.session_resets VALUES ('o', 'w', 'u3', 'r-pre', 0);
      -- After T0 on Supabase: u1 deleted session s1, and u2 reset everything.
      INSERT INTO sessions.deleted_upload_sessions VALUES ('o', 'w', 'u1', 'origin', 'claude', 's1');
      INSERT INTO sessions.session_resets VALUES ('o', 'w', 'u2', 'r-post', 1);`);
    await prepareTargetSession(target as Queryable);
    await prepareBackfillFence(source as Queryable, target as Queryable);
    for (const table of ["convergence_events", "session_rollups"]) {
      const plan = await planTable(source as Queryable, target as Queryable, table);
      await copyTable(source as Queryable, target as Queryable, plan, {
        mode: "backfill",
        chunkSize: 2,
        fenced: true,
      });
    }
    const events = (
      await target.query<{ event_id: string }>(
        "SELECT event_id FROM sessions.convergence_events ORDER BY event_id",
      )
    ).rows.map((row) => row.event_id);
    expect(events).toEqual(["e1b", "e3"]); // s1 deleted, all of u2 reset
    const rollups = (
      await target.query<{ session_id: string }>(
        "SELECT session_id FROM sessions.session_rollups ORDER BY session_id",
      )
    ).rows.map((row) => row.session_id);
    // Without user columns, u2's sessions are matched through Neon's events.
    expect(rollups).toEqual(["s3"]);
  });
});

describe("target session", () => {
  it("lands rows without firing triggers, so history wakes nothing", async () => {
    await target.exec(`CREATE TABLE sessions.wakeups (id bigint);
      CREATE FUNCTION sessions.wake() RETURNS trigger LANGUAGE plpgsql AS
        $$ BEGIN INSERT INTO sessions.wakeups VALUES (NEW.id); RETURN NEW; END $$;
      CREATE TRIGGER wake AFTER INSERT ON sessions.tokens
        FOR EACH ROW EXECUTE FUNCTION sessions.wake();`);
    await insert(source, `('a', 1, NULL, NULL, NULL, NULL, NULL, 'h')`);
    await prepareTargetSession(target as Queryable);
    const plan = await planTable(source as Queryable, target as Queryable, "tokens");
    await copyTable(source as Queryable, target as Queryable, plan, {
      mode: "backfill",
      chunkSize: 10,
    });
    expect((await dump(target)).map((row) => row.label)).toEqual(["h"]);
    expect((await target.query("SELECT * FROM sessions.wakeups")).rows).toEqual([]);
    // A live session (default role) still fires them.
    await target.exec("SET session_replication_role = origin");
    await insert(target, `('b', 2, NULL, NULL, NULL, NULL, NULL, 'live')`);
    expect((await target.query("SELECT id::text FROM sessions.wakeups")).rows).toEqual([
      { id: "2" },
    ]);
  });

  it("fails closed when triggers cannot be disabled", async () => {
    const refusing: Queryable = {
      query: async (sql: string) =>
        sql.startsWith("SHOW")
          ? ({ rows: [{ session_replication_role: "origin" }] } as never)
          : ({ rows: [] } as never),
    };
    await expect(prepareTargetSession(refusing)).rejects.toThrow(
      "could not disable triggers",
    );
  });
});

describe("tables keyed by a unique index with nullable columns", () => {
  it("backfills every row, NULL keys included, and refuses to mirror", async () => {
    const ddl = `CREATE TABLE sessions.rollups (
      org_id text NOT NULL, session_id text NOT NULL, project_id text, n int);
      CREATE UNIQUE INDEX rollups_key ON sessions.rollups (org_id, session_id, project_id) NULLS NOT DISTINCT;`;
    await Promise.all([source.exec(ddl), target.exec(ddl)]);
    await source.exec(`INSERT INTO sessions.rollups VALUES
      ('o', 's1', NULL, 1), ('o', 's1', 'p', 2), ('o', 's1', '', 3),
      ('o', 's2', NULL, 4), ('o', 's3', 'p', 5), ('o', 's3', NULL, 6)`);
    // Written after T0 with a NULL in its key: must survive.
    await target.exec(`INSERT INTO sessions.rollups VALUES ('o', 's2', NULL, 40)`);
    const plan = await planTable(source as Queryable, target as Queryable, "rollups");
    expect(plan.nullableKey).toBe(true);
    const result = await copyTable(source as Queryable, target as Queryable, plan, {
      mode: "backfill",
      chunkSize: 2,
    });
    expect(result).toMatchObject({ read: 6, written: 5 });
    const rows = (
      await target.query<{ n: number }>("SELECT n FROM sessions.rollups ORDER BY n")
    ).rows.map((row) => row.n);
    expect(rows).toEqual([1, 2, 3, 5, 6, 40]);
    await expect(
      copyTable(source as Queryable, target as Queryable, plan, {
        mode: "mirror",
        chunkSize: 2,
      }),
    ).rejects.toThrow("mirror needs a key without NULLs");
  });

  it("refuses a table with no usable key", async () => {
    const ddl = `CREATE TABLE sessions.loose (a text, b text);
      CREATE UNIQUE INDEX loose_key ON sessions.loose (a, b);`;
    await Promise.all([source.exec(ddl), target.exec(ddl)]);
    await expect(
      planTable(source as Queryable, target as Queryable, "loose"),
    ).rejects.toThrow("no primary or usable unique key");
  });
});

describe("plans fail closed", () => {
  it("refuses a target missing a source column or with a different key", async () => {
    await target.exec("ALTER TABLE sessions.tokens DROP COLUMN tags");
    await expect(
      planTable(source as Queryable, target as Queryable, "tokens"),
    ).rejects.toThrow("target lacks 1 source column");
    await target.exec(`ALTER TABLE sessions.tokens ADD COLUMN tags text[];
      ALTER TABLE sessions.tokens DROP CONSTRAINT tokens_pkey;
      ALTER TABLE sessions.tokens ADD PRIMARY KEY (id, org_id);`);
    await expect(
      planTable(source as Queryable, target as Queryable, "tokens"),
    ).rejects.toThrow("keys differ");
  });
});
