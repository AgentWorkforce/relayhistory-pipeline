/**
 * The V1 projector precedence contract (approved; provenance in
 * docs/session-search-index-contract.md, "Projector precedence"), driven through
 * projectSessionDirectory from crafted session_index pointer rows. In-process
 * PGlite with every supabase migration; synthetic data.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { projectSessionDirectory } from "../session-directory-projector.ts";

const root = join(import.meta.dir, "../../..");
const read = (path: string) => readFileSync(join(root, path), "utf8");
const KEY = { orgId: "o", workspaceId: "w", harness: "codex", sessionId: "s" };

const open: PGlite[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((db) => db.close()));
});

async function database(): Promise<PGlite> {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  open.push(db);
  await db.exec(read("supabase/tests/supabase_shim.sql"));
  for (const file of readdirSync(join(root, "supabase/migrations")).sort()) {
    await db.exec(read(`supabase/migrations/${file}`));
  }
  return db;
}

type Pointer = {
  machine: string;
  user?: string;
  created: string;
  last: string;
  revision?: number;
  title?: string;
  state?: string;
  events?: number;
  inputTokens?: number;
  indexed?: string;
};

async function upsertPointer(db: PGlite, p: Pointer) {
  await db.query(
    `INSERT INTO sessions.session_index
       (org_id, workspace_id, harness, session_id, machine_id, user_id, created_at, last_activity_at,
        title, state, event_count, input_tokens, storage_provider, bucket, manifest_key, trace_id,
        revision, indexed_at, deleted_at)
     VALUES ('o', 'w', 'codex', 's', $1, $2, $3::timestamptz, $4::timestamptz, $5, $6, $7, $8,
             's3', 'bucket', 'key/' || $1, '019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18', $9,
             coalesce($10::timestamptz, now()), NULL)
     ON CONFLICT (org_id, workspace_id, harness, session_id, machine_id) DO UPDATE SET
       user_id = excluded.user_id, created_at = excluded.created_at,
       last_activity_at = excluded.last_activity_at, title = excluded.title, state = excluded.state,
       event_count = excluded.event_count, input_tokens = excluded.input_tokens,
       revision = excluded.revision, indexed_at = excluded.indexed_at, deleted_at = NULL`,
    [
      p.machine,
      p.user ?? `u-${p.machine}`,
      p.created,
      p.last,
      p.title ?? `title ${p.machine}`,
      p.state ?? "active",
      p.events ?? 1,
      p.inputTokens ?? 0,
      p.revision ?? 1,
      p.indexed ?? null,
    ],
  );
}

async function tombstonePointer(db: PGlite, machine: string, at = "now()") {
  await db.query(
    `UPDATE sessions.session_index SET deleted_at = ${at}, indexed_at = ${at}
      WHERE org_id = 'o' AND workspace_id = 'w' AND harness = 'codex' AND session_id = 's' AND machine_id = $1`,
    [machine],
  );
}

type Row = Record<string, unknown>;
const row = async (db: PGlite): Promise<Row | undefined> =>
  (
    await db.query<Row>(
      "SELECT * FROM sessions.session_directory WHERE session_id = 's'",
    )
  ).rows[0];
const members = async (db: PGlite, table: "users" | "machines") =>
  (
    await db.query<{ v: string; at: Date }>(
      `SELECT ${table === "users" ? "user_id" : "machine_id"} AS v, last_activity_at AS at
       FROM sessions.session_directory_${table} WHERE session_id = 's' ORDER BY 1`,
    )
  ).rows;
const iso = (v: unknown) => (v as Date).toISOString();

describe("V1 projector precedence", () => {
  test("stable historical created_at: the earliest ever projected, kept after its pointer is tombstoned", async () => {
    const db = await database();
    await upsertPointer(db, {
      machine: "m1",
      created: "2026-10-01T10:00:00Z",
      last: "2026-10-01T12:00:00Z",
    });
    await upsertPointer(db, {
      machine: "m2",
      created: "2026-10-01T11:00:00Z",
      last: "2026-10-01T13:00:00Z",
    });
    await projectSessionDirectory(db, KEY);
    expect(iso((await row(db))!.created_at)).toBe("2026-10-01T10:00:00.000Z");
    await tombstonePointer(db, "m1");
    await projectSessionDirectory(db, KEY);
    expect(iso((await row(db))!.created_at)).toBe("2026-10-01T10:00:00.000Z");
  });

  test("latest live pointer wins every scalar and counter, without summing", async () => {
    const db = await database();
    await upsertPointer(db, {
      machine: "m1",
      created: "2026-10-01T10:00:00Z",
      last: "2026-10-01T12:00:00Z",
      title: "older",
      state: "done",
      events: 5,
      inputTokens: 500,
    });
    await upsertPointer(db, {
      machine: "m2",
      created: "2026-10-01T11:00:00Z",
      last: "2026-10-01T13:00:00Z",
      title: "newer",
      state: "active",
      events: 3,
      inputTokens: 300,
    });
    await projectSessionDirectory(db, KEY);
    let current = (await row(db))!;
    expect([
      current.title,
      current.state,
      current.event_count,
      current.input_tokens,
    ]).toEqual(["newer", "active", 3, 300]);
    expect(iso(current.last_activity_at)).toBe("2026-10-01T13:00:00.000Z");
    // The winner's pointer is tombstoned: the remaining live pointer wins, still not summed.
    await tombstonePointer(db, "m2");
    await projectSessionDirectory(db, KEY);
    current = (await row(db))!;
    expect([
      current.title,
      current.state,
      current.event_count,
      current.input_tokens,
    ]).toEqual(["older", "done", 5, 500]);
    expect(iso(current.last_activity_at)).toBe("2026-10-01T12:00:00.000Z");
  });

  test("deterministic ties: latest activity, then highest revision, then highest machine_id", async () => {
    const db = await database();
    const same = "2026-10-01T12:00:00Z";
    await upsertPointer(db, {
      machine: "m1",
      created: same,
      last: same,
      revision: 3,
      title: "m1 rev 3",
    });
    await upsertPointer(db, {
      machine: "m2",
      created: same,
      last: same,
      revision: 2,
      title: "m2 rev 2",
    });
    await projectSessionDirectory(db, KEY);
    expect((await row(db))!.title).toBe("m1 rev 3");
    await upsertPointer(db, {
      machine: "m2",
      created: same,
      last: same,
      revision: 3,
      title: "m2 rev 3",
    });
    await projectSessionDirectory(db, KEY);
    expect((await row(db))!.title).toBe("m2 rev 3");
    // Repeating the projection changes nothing.
    const before = await row(db);
    await projectSessionDirectory(db, KEY);
    const after = await row(db);
    expect({ ...after, projected_at: null }).toEqual({
      ...before,
      projected_at: null,
    });
  });

  test("memberships are the union of LIVE pointers and carry the row's last_activity_at", async () => {
    const db = await database();
    await upsertPointer(db, {
      machine: "m1",
      user: "u1",
      created: "2026-10-01T10:00:00Z",
      last: "2026-10-01T12:00:00Z",
    });
    await upsertPointer(db, {
      machine: "m2",
      user: "u2",
      created: "2026-10-01T10:00:00Z",
      last: "2026-10-01T13:00:00Z",
    });
    await projectSessionDirectory(db, KEY);
    let current = (await row(db))!;
    expect(current.user_ids).toEqual(["u1", "u2"]);
    expect(current.machine_ids).toEqual(["m1", "m2"]);
    expect(
      (await members(db, "users")).map((m) => [m.v, m.at.toISOString()]),
    ).toEqual([
      ["u1", "2026-10-01T13:00:00.000Z"],
      ["u2", "2026-10-01T13:00:00.000Z"],
    ]);
    await tombstonePointer(db, "m2");
    await projectSessionDirectory(db, KEY);
    current = (await row(db))!;
    expect(current.user_ids).toEqual(["u1"]);
    expect(current.machine_ids).toEqual(["m1"]);
    expect((await members(db, "users")).map((m) => m.v)).toEqual(["u1"]);
    expect((await members(db, "machines")).map((m) => m.v)).toEqual(["m1"]);
  });

  test("tombstone when no live pointer remains; resurrection when a later live pointer appears", async () => {
    const db = await database();
    await upsertPointer(db, {
      machine: "m1",
      user: "u1",
      created: "2026-10-01T10:00:00Z",
      last: "2026-10-01T12:00:00Z",
      indexed: "2026-10-30T10:00:00Z",
    });
    await projectSessionDirectory(db, KEY);
    await tombstonePointer(db, "m1", "'2026-10-30T11:00:00Z'::timestamptz");
    expect(await projectSessionDirectory(db, KEY)).toEqual({
      projected: false,
      tombstoned: true,
    });
    let current = (await row(db))!;
    expect(current.deleted_at).not.toBeNull();
    const tombstonedAt = (current.deleted_at as Date).getTime();
    expect([
      current.user_ids,
      current.machine_ids,
      current.search_text,
    ]).toEqual([[], [], ""]);
    expect(await members(db, "users")).toEqual([]);

    // A later live pointer (indexed after the tombstone) brings the row back, keeping
    // the historical created_at and holding no summary until one is written.
    await upsertPointer(db, {
      machine: "m3",
      user: "u3",
      created: "2026-10-31T09:00:00Z",
      last: "2026-10-31T09:30:00Z",
      title: "back again",
      indexed: new Date(tombstonedAt + 1000).toISOString(),
    });
    expect(await projectSessionDirectory(db, KEY)).toEqual({
      projected: true,
      tombstoned: false,
    });
    current = (await row(db))!;
    expect(current.deleted_at).toBeNull();
    expect(current.title).toBe("back again");
    expect(iso(current.created_at)).toBe("2026-10-01T10:00:00.000Z");
    expect(current.short_summary).toBeNull();
    expect(current.user_ids).toEqual(["u3"]);
    expect((await members(db, "users")).map((m) => m.v)).toEqual(["u3"]);
  });

  test("a live pointer indexed before the tombstone does not resurrect it", async () => {
    const db = await database();
    await upsertPointer(db, {
      machine: "m1",
      created: "2026-10-01T10:00:00Z",
      last: "2026-10-01T12:00:00Z",
      indexed: "2026-10-30T10:00:00Z",
    });
    await projectSessionDirectory(db, KEY);
    // The directory row is tombstoned directly at a time after the pointer's last write.
    await db.query(
      "UPDATE sessions.session_directory SET deleted_at = '2026-10-30T12:00:00Z' WHERE session_id = 's'",
    );
    expect(await projectSessionDirectory(db, KEY)).toEqual({
      projected: false,
      tombstoned: false,
    });
    expect(iso((await row(db))!.deleted_at)).toBe("2026-10-30T12:00:00.000Z");
    // The same pointer re-indexed later does resurrect it.
    await upsertPointer(db, {
      machine: "m1",
      created: "2026-10-01T10:00:00Z",
      last: "2026-10-01T12:30:00Z",
      indexed: "2026-10-30T13:00:00Z",
    });
    expect(await projectSessionDirectory(db, KEY)).toEqual({
      projected: true,
      tombstoned: false,
    });
    expect((await row(db))!.deleted_at).toBeNull();
  });
});
