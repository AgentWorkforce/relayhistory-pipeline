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
    ).rejects.toThrow("primary keys differ");
  });
});
