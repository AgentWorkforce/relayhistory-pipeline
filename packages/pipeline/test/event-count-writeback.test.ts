/**
 * The event_count write-back (`docs/event-count-writeback.md`) against every
 * migration on PGlite, called as the real `session_count_writer`: one test for
 * each row of the ordering table, then what must hold around it.
 */
import { PGlite } from "@electric-sql/pglite";
import { readdirSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";

const SUPABASE = new URL("../../../supabase/", import.meta.url).href;
const supabaseFile = (path: string) =>
  fileURLToPath(new URL(path, SUPABASE).href);

const KEY = {
  org_id: "org_a",
  workspace_id: "ws_a",
  harness: "claude",
  session_id: "s-1",
  machine_id: "m-1",
};
const MANIFEST_KEY = "v1/ws=ws_a/m=m-1/h=claude/d=2026/10/30/s=s-1/manifest.json";
const hex = (byte: string) => byte.repeat(32);
const CONTENT = hex("aa");
const OBJECT = hex("bb");

let db: PGlite;

async function index(revision: number, options: { eventCount?: number; content?: string; deleted?: boolean } = {}) {
  const row = {
    ...KEY,
    user_id: "user-1",
    created_at: "2026-10-30T00:00:00Z",
    last_activity_at: "2026-10-30T00:00:00Z",
    event_count: options.eventCount ?? 7,
    storage_provider: "s3",
    bucket: "b",
    manifest_key: MANIFEST_KEY,
    content_sha256: `\\x${options.content ?? CONTENT}`,
    raw_bytes: 1,
    chunk_count: 1,
    raw_format: "jsonl",
    format_version: 1,
    revision,
    trace_id: "0192f000-0000-7000-8000-000000000001",
    scrubbed: true,
    deleted_at: options.deleted ? "2026-10-30T01:00:00Z" : null,
  };
  await db.query("SELECT * FROM sessions.upsert_session_index($1::jsonb)", [JSON.stringify([row])]);
}

interface Count {
  revision?: number;
  version?: number;
  workKey?: string;
  content?: string;
  object?: string;
  count?: number;
  key?: Partial<typeof KEY>;
}

async function record(role: string, count: Count = {}) {
  const k = { ...KEY, ...count.key };
  await db.exec(`SET ROLE ${role}`);
  try {
    const { rows } = await db.query<{ outcome: string }>(
      `SELECT sessions.record_normalized_event_count(
         $1, $2, $3, $4, $5, $6::bigint, $7::integer, $8,
         decode($9, 'hex'), decode($10, 'hex'), $11::integer) AS outcome`,
      [
        k.org_id, k.workspace_id, k.harness, k.session_id, k.machine_id,
        count.revision ?? 1, count.version ?? 2, count.workKey ?? `${MANIFEST_KEY}@etag-1`,
        count.content ?? CONTENT, count.object ?? OBJECT, count.count ?? 42,
      ],
    );
    return rows[0]!.outcome;
  } finally {
    await db.exec("RESET ROLE");
  }
}
const write = (count?: Count) => record("session_count_writer", count);

async function stored() {
  const { rows } = await db.query<{
    event_count: number;
    event_count_revision: number | null;
    event_count_normalized_version: number | null;
    event_count_work_key: string | null;
    sha: string | null;
    event_count_recorded_at: string | null;
  }>(`SELECT event_count, event_count_revision, event_count_normalized_version, event_count_work_key,
             encode(event_count_object_sha256, 'hex') AS sha, event_count_recorded_at
        FROM sessions.session_index WHERE session_id = 's-1'`);
  return rows[0]!;
}

beforeAll(async () => {
  db = new PGlite();
  await db.exec(readFileSync(supabaseFile("tests/supabase_shim.sql"), "utf8"));
  for (const file of readdirSync(supabaseFile("migrations/")).sort())
    await db.exec(readFileSync(supabaseFile(`migrations/${file}`), "utf8"));
});
afterAll(async () => {
  await db?.close();
});
beforeEach(async () => {
  await db.exec("RESET ROLE; TRUNCATE sessions.session_index CASCADE");
});

describe("the ordering table", () => {
  it("1: no row yet is retryable, and writes nothing", async () => {
    await expect(write()).rejects.toMatchObject({ code: "CN001" });
  });

  it("1: a revision ahead of the index is retryable, and writes nothing", async () => {
    await index(1);
    await expect(write({ revision: 2 })).rejects.toMatchObject({ code: "CN001" });
    expect((await stored()).event_count_revision).toBeNull();
  });

  it("2: a deleted session takes no count", async () => {
    await index(1);
    await index(2, { deleted: true });
    expect(await write({ revision: 2 })).toBe("deleted");
    expect((await stored()).event_count_revision).toBeNull();
  });

  it("3: an older revision is a no-op, and can't replace a newer count", async () => {
    await index(1);
    await index(2);
    expect(await write({ revision: 2, count: 10 })).toBe("applied");
    expect(await write({ revision: 1, count: 99 })).toBe("stale");
    expect(await stored()).toMatchObject({ event_count: 10, event_count_revision: 2 });
  });

  it("4: another manifest key, or other content, is a conflict and writes nothing", async () => {
    await index(1);
    await expect(write({ workKey: "v1/other/manifest.json@etag-1" })).rejects.toMatchObject({ code: "CN002" });
    await expect(write({ content: hex("cc") })).rejects.toMatchObject({ code: "CN002" });
    expect((await stored()).event_count_revision).toBeNull();
  });

  it("5: an older normalized version of the same revision is a no-op", async () => {
    await index(1);
    await write({ version: 3, count: 10 });
    expect(await write({ version: 2, count: 99 })).toBe("stale");
    expect(await stored()).toMatchObject({ event_count: 10, event_count_normalized_version: 3 });
  });

  it("6: a replay is a no-op and changes no timestamp", async () => {
    await index(1);
    expect(await write()).toBe("applied");
    const before = await stored();
    expect(await write({ workKey: `${MANIFEST_KEY}@etag-retry` })).toBe("replayed");
    expect(await stored()).toEqual(before);
  });

  it("7: two outputs for one revision and version are a conflict", async () => {
    await index(1);
    await write();
    await expect(write({ object: hex("cc") })).rejects.toMatchObject({ code: "CN002" });
    await expect(write({ count: 43 })).rejects.toMatchObject({ code: "CN002" });
    expect(await stored()).toMatchObject({ event_count: 42, sha: OBJECT });
  });

  it("8: applies the count with all of its provenance", async () => {
    await index(1);
    expect(await write()).toBe("applied");
    const row = await stored();
    expect(row).toMatchObject({
      event_count: 42,
      event_count_revision: 1,
      event_count_normalized_version: 2,
      event_count_work_key: `${MANIFEST_KEY}@etag-1`,
      sha: OBJECT,
    });
    expect(row.event_count_recorded_at).not.toBeNull();
  });

  it("8: a newer normalized version of the same revision applies", async () => {
    await index(1);
    await write({ version: 2, count: 10 });
    expect(await write({ version: 3, count: 11, object: hex("cc") })).toBe("applied");
    expect((await stored()).event_count).toBe(11);
  });
});

describe("around the fence", () => {
  it("lets a newer revision decrease the count", async () => {
    await index(1);
    await write({ count: 100 });
    await index(2, { content: hex("dd") });
    expect(await write({ revision: 2, content: hex("dd"), count: 3 })).toBe("applied");
    expect((await stored()).event_count).toBe(3);
  });

  it("keeps the count when the index stage sends a chunk sum, then fails the gate until counted", async () => {
    await index(1);
    await write({ count: 42 });
    await index(2, { eventCount: 999 });
    const gate = await db.query<{ authoritative: boolean }>(
      `SELECT event_count_revision = revision AND deleted_at IS NULL AS authoritative
         FROM sessions.session_index WHERE session_id = 's-1'`,
    );
    expect(await stored()).toMatchObject({ event_count: 42, event_count_revision: 1 });
    expect(gate.rows).toEqual([{ authoritative: false }]);
  });

  it("leaves the index stage's sum alone before any count has provenance", async () => {
    await index(1, { eventCount: 5 });
    await index(2, { eventCount: 9 });
    expect((await stored()).event_count).toBe(9);
  });

  it("never touches another session", async () => {
    await index(1);
    await expect(write({ key: { org_id: "org_b" } })).rejects.toMatchObject({ code: "CN001" });
    await expect(write({ key: { machine_id: "m-2" } })).rejects.toMatchObject({ code: "CN001" });
    expect((await stored()).event_count_revision).toBeNull();
  });

  it("refuses bad input", async () => {
    await index(1);
    await expect(write({ count: -1 })).rejects.toMatchObject({ code: "22023" });
    await expect(write({ object: "aa" })).rejects.toMatchObject({ code: "22023" });
    await expect(write({ workKey: "no-version" })).rejects.toMatchObject({ code: "22023" });
  });
});

describe("access", () => {
  it("the writer role has no BYPASSRLS, no login and no table access", async () => {
    const { rows } = await db.query("SELECT rolbypassrls, rolcanlogin FROM pg_roles WHERE rolname = 'session_count_writer'");
    expect(rows).toEqual([{ rolbypassrls: false, rolcanlogin: false }]);
    await db.exec("SET ROLE session_count_writer");
    try {
      await expect(db.query("SELECT 1 FROM sessions.session_index")).rejects.toMatchObject({ code: "42501" });
      await expect(db.query("UPDATE sessions.session_index SET event_count = 1")).rejects.toMatchObject({ code: "42501" });
    } finally {
      await db.exec("RESET ROLE");
    }
  });

  it("tenants and the API roles can't call it", async () => {
    await index(1);
    for (const role of ["anon", "authenticated"])
      await expect(record(role)).rejects.toMatchObject({ code: "42501" });
  });

  it("a definer RLS would filter raises CN003, not index-behind", async () => {
    await index(1);
    const fn = `sessions.record_normalized_event_count(text, text, text, text, text, bigint, integer, text, bytea, bytea, integer)`;
    await db.exec(`CREATE ROLE plain_owner NOLOGIN NOBYPASSRLS;
      GRANT USAGE ON SCHEMA sessions TO plain_owner;
      GRANT SELECT, UPDATE ON sessions.session_index TO plain_owner;
      ALTER FUNCTION ${fn} OWNER TO plain_owner`);
    try {
      await expect(write()).rejects.toMatchObject({ code: "CN003" });
    } finally {
      await db.exec(`ALTER FUNCTION ${fn} OWNER TO CURRENT_USER;
        REVOKE ALL ON sessions.session_index FROM plain_owner;
        REVOKE ALL ON SCHEMA sessions FROM plain_owner;
        DROP ROLE plain_owner`);
    }
  });

  it("accepts the new stage, on all three tables, and still refuses an unknown one", async () => {
    const insert = (stage: string) =>
      db.query(
        `INSERT INTO sessions.pipeline_jobs (stage, work_key, trace_id, org_id, workspace_id, message)
         VALUES ($1, 'k', '0192f000-0000-7000-8000-000000000001', 'o', 'w', '{}')`,
        [stage],
      );
    await insert("normalized-ready");
    await db.exec("DELETE FROM sessions.pipeline_jobs");
    await expect(insert("not-a-stage")).rejects.toMatchObject({ code: "23514" });
    await db.query(
      `INSERT INTO sessions.pipeline_events (trace_id, stage, status)
       VALUES ('0192f000-0000-7000-8000-000000000001', 'normalized-ready', 'ok')`,
    );
    await db.query(
      `INSERT INTO sessions.pipeline_dead_letters (id, stage, message) VALUES ('d', 'normalized-ready', '{}')`,
    );
    await db.exec("DELETE FROM sessions.pipeline_dead_letters; TRUNCATE sessions.pipeline_events");
  });
});
