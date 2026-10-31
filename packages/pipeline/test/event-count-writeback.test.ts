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
const TRACE = "0192f000-0000-7000-8000-000000000001";
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
    trace_id: TRACE,
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
  /** Overrides on the payload alone, so it disagrees with its job. */
  payload?: Record<string, unknown>;
}

const LEASE = "0192f000-0000-7000-8000-00000000000a";
const OTHER_LEASE = "0192f000-0000-7000-8000-00000000000b";

/** A leased normalized-ready job, as the queue would hold it. */
async function leasedJob(count: Count = {}, columns: { stage?: string; lease?: string | null; visible?: string; dlq?: boolean } = {}) {
  const k = { ...KEY, ...count.key };
  const workKey = count.workKey ?? `${MANIFEST_KEY}@etag-1`;
  const revision = count.revision ?? 1;
  const stage = columns.stage ?? "normalized-ready";
  const message = {
    v: 1,
    stage,
    workKey,
    revision,
    ctx: { traceId: TRACE, orgId: k.org_id, workspaceId: k.workspace_id, machineId: k.machine_id, sessionId: k.session_id, harness: k.harness },
    payload: {
      v: 1,
      ...k,
      manifest_key: workKey.replace(/@[^@]*$/, ""),
      manifest_revision: revision,
      work_key: workKey,
      trace_id: TRACE,
      source_content_sha256: count.content ?? CONTENT,
      normalized_version: count.version ?? 2,
      output_key: "k",
      output_sha256: count.object ?? OBJECT,
      output_bytes: 1,
      convergence_event_count: count.count ?? 42,
      unprojected_records: 0,
      output_reused: false,
      output_divergent: false,
      ...count.payload,
    },
  };
  const { rows } = await db.query<{ id: number }>(
    `INSERT INTO sessions.pipeline_jobs
       (stage, work_key, trace_id, org_id, workspace_id, session_id, machine_id, message, lease_id, visible_at, dlq)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, now() + $10::interval, $11)
     RETURNING id`,
    [stage, workKey, TRACE, k.org_id, k.workspace_id, k.session_id, k.machine_id, JSON.stringify(message),
     columns.lease === undefined ? LEASE : columns.lease, columns.visible ?? "300 seconds", columns.dlq ?? false],
  );
  return rows[0]!.id;
}

async function call(role: string, id: number, lease: string | null = LEASE) {
  await db.exec(`SET ROLE ${role}`);
  try {
    const { rows } = await db.query<{ outcome: string }>(
      "SELECT sessions.record_normalized_event_count($1::bigint, $2::uuid) AS outcome",
      [id, lease],
    );
    return rows[0]!.outcome;
  } finally {
    await db.exec("RESET ROLE");
  }
}
const record = async (role: string, count: Count = {}) => call(role, await leasedJob(count));
const write = (count?: Count) => record("session_count_writer", count);
const jobCount = async () => (await db.query<{ n: number }>("SELECT count(*)::int AS n FROM sessions.pipeline_jobs")).rows[0]!.n;

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
  await db.exec("RESET ROLE; TRUNCATE sessions.session_index CASCADE; TRUNCATE sessions.pipeline_jobs");
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

  it("moves the fence to a new revision with identical content and the same object", async () => {
    await index(1);
    await write({ count: 42 });
    await index(2);
    expect(await write({ revision: 2, workKey: `${MANIFEST_KEY}@etag-2`, count: 42 })).toBe("applied");
    expect(await stored()).toMatchObject({
      event_count: 42,
      event_count_revision: 2,
      event_count_work_key: `${MANIFEST_KEY}@etag-2`,
    });
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
    await expect(write({ workKey: "no-version" })).rejects.toMatchObject({ code: "CN002" });
  });
});

describe("the job capability", () => {
  const untouched = async () => {
    expect((await stored()).event_count_revision).toBeNull();
    expect((await stored()).event_count).toBe(7);
  };

  it("applies and acks in one call, and every outcome acks", async () => {
    await index(1);
    await index(2);
    expect(await write({ revision: 2 })).toBe("applied");
    expect(await jobCount()).toBe(0);
    expect(await write({ revision: 2 })).toBe("replayed");
    expect(await jobCount()).toBe(0);
    expect(await write({ revision: 1 })).toBe("stale");
    expect(await jobCount()).toBe(0);
  });

  it("a failure leaves the job and the count, so neither half commits alone", async () => {
    await expect(write()).rejects.toMatchObject({ code: "CN001" });
    expect(await jobCount()).toBe(1);
    await index(1);
    await db.exec("TRUNCATE sessions.pipeline_jobs");
    await expect(write({ content: hex("cc") })).rejects.toMatchObject({ code: "CN002" });
    expect(await jobCount()).toBe(1);
    await untouched();
  });

  it("a lost response can't apply twice: the retry finds no job", async () => {
    await index(1);
    const id = await leasedJob({ count: 42 });
    expect(await call("session_count_writer", id)).toBe("applied");
    await expect(call("session_count_writer", id)).rejects.toMatchObject({ code: "CN004" });
    expect((await stored()).event_count).toBe(42);
  });

  it.each([
    ["a wrong lease", {}, OTHER_LEASE],
    ["no lease", {}, null],
    ["a released lease", { lease: null }, LEASE],
    ["an expired lease", { visible: "-1 second" }, LEASE],
    ["another stage's job", { stage: "session-indexed" }, LEASE],
    ["a job in the DLQ path", { dlq: true }, LEASE],
  ])("refuses %s", async (_, columns, lease) => {
    await index(1);
    const id = await leasedJob({}, columns);
    await expect(call("session_count_writer", id, lease)).rejects.toMatchObject({ code: "CN004" });
    expect(await jobCount()).toBe(1);
    await untouched();
  });

  it("refuses another job's id with this lease", async () => {
    await index(1);
    await leasedJob();
    const other = await leasedJob({}, { lease: OTHER_LEASE });
    await expect(call("session_count_writer", other, LEASE)).rejects.toMatchObject({ code: "CN004" });
    await untouched();
  });

  it("refuses a missing job", async () => {
    await expect(call("session_count_writer", 999_999)).rejects.toMatchObject({ code: "CN004" });
  });

  it.each([
    ["org", { org_id: "org_b" }],
    ["workspace", { workspace_id: "ws_b" }],
    ["session", { session_id: "s-2" }],
    ["machine", { machine_id: "m-2" }],
    ["harness", { harness: "codex" }],
    ["work key", { work_key: "other@etag" }],
    ["manifest key", { manifest_key: "v1/other/manifest.json" }],
    ["revision", { manifest_revision: 2 }],
    ["divergent output", { output_divergent: true }],
  ])("refuses a payload substituting the %s", async (_, payload) => {
    await index(1);
    await expect(write({ payload })).rejects.toMatchObject({ code: "CN002" });
    expect(await jobCount()).toBe(1);
    await untouched();
  });

  it("a job for one tenant can't write another tenant's identical row", async () => {
    await index(1);
    await expect(
      write({ key: { org_id: "org_b" }, payload: { org_id: "org_a" } }),
    ).rejects.toMatchObject({ code: "CN002" });
    await untouched();
  });

  it.each([
    ["a non-numeric count", { convergence_event_count: "42" }],
    ["a fractional count", { convergence_event_count: 1.5 }],
    ["a missing count", { convergence_event_count: null }],
    ["a bad checksum", { output_sha256: "zz" }],
    ["no divergence flag", { output_divergent: "false" }],
  ])("refuses %s", async (_, payload) => {
    await index(1);
    await expect(write({ payload })).rejects.toMatchObject({ code: "22023" });
    await untouched();
  });

  it("the old tenant-naming form is gone, and the internal one is out of reach", async () => {
    await index(1);
    await db.exec("SET ROLE session_count_writer");
    try {
      await expect(
        db.query(
          `SELECT sessions.apply_normalized_event_count('org_a','ws_a','claude','s-1','m-1',1,2,'k@e',''::bytea,''::bytea,1)`,
        ),
      ).rejects.toMatchObject({ code: "42501" });
    } finally {
      await db.exec("RESET ROLE");
    }
    const { rows } = await db.query(
      `SELECT p.oid FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'sessions' AND p.proname = 'record_normalized_event_count' AND p.pronargs <> 2`,
    );
    expect(rows).toEqual([]);
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

  describe("provenance columns", () => {
    const PROVENANCE = [
      "event_count_revision",
      "event_count_normalized_version",
      "event_count_work_key",
      "event_count_object_sha256",
      "event_count_recorded_at",
      "revision",
      "content_sha256",
    ];
    const asService = async <T>(run: () => Promise<T>) => {
      await db.exec("SET ROLE service_role");
      try {
        return await run();
      } finally {
        await db.exec("RESET ROLE");
      }
    };

    it("no login or API role can write them, which the owner and superusers are outside of", async () => {
      const { rows } = await db.query<{ rolname: string; attname: string; priv: string }>(
        `SELECT r.rolname, a.attname, p.priv
           FROM pg_roles r
           CROSS JOIN pg_attribute a
           CROSS JOIN (VALUES ('INSERT'), ('UPDATE')) AS p (priv)
          WHERE a.attrelid = 'sessions.session_index'::regclass
            AND a.attname = ANY($1::text[])
            AND NOT r.rolsuper AND r.rolname NOT LIKE 'pg\\_%'
            AND r.rolname <> 'normalized_ready_writer'
            AND r.oid <> (SELECT relowner FROM pg_class WHERE oid = 'sessions.session_index'::regclass)
            AND has_column_privilege(r.oid, a.attrelid, a.attnum, p.priv)`,
        [PROVENANCE],
      );
      expect(rows).toEqual([]);
    });

    it("service_role can't set them by UPDATE or INSERT, or through any column of them", async () => {
      await index(1);
      for (const column of PROVENANCE) {
        const value = column === "event_count_recorded_at" ? "now()" : column === "event_count_object_sha256" || column === "content_sha256" ? "NULL::bytea" : column === "event_count_work_key" ? "'x'" : "1";
        await expect(
          asService(() => db.query(`UPDATE sessions.session_index SET ${column} = ${value}`)),
        ).rejects.toMatchObject({ code: "42501" });
      }
      await expect(
        asService(() =>
          db.query(
            `INSERT INTO sessions.session_index (org_id, workspace_id, harness, session_id, machine_id, user_id, created_at, last_activity_at, event_count_revision)
             VALUES ('o', 'w', 'claude', 's-x', 'm', 'u', now(), now(), 1)`,
          ),
        ),
      ).rejects.toMatchObject({ code: "42501" });
      expect((await stored()).event_count_revision).toBeNull();
    });

    // CI's guard: a later migration that adds a column must grant it to service_role.
    it("service_role can INSERT and UPDATE every session_index column but provenance", async () => {
      const { rows } = await db.query<{ attname: string; ins: boolean; upd: boolean }>(
        `SELECT a.attname,
                has_column_privilege('service_role', a.attrelid, a.attnum, 'INSERT') AS ins,
                has_column_privilege('service_role', a.attrelid, a.attnum, 'UPDATE') AS upd
           FROM pg_attribute a
          WHERE a.attrelid = 'sessions.session_index'::regclass AND a.attnum > 0
            AND NOT a.attisdropped AND a.attgenerated = ''`,
      );
      expect(rows.length).toBeGreaterThan(PROVENANCE.length);
      for (const { attname, ins, upd } of rows) {
        const allowed = !PROVENANCE.includes(attname);
        expect({ attname, ins, upd }).toEqual({ attname, ins: allowed, upd: allowed });
      }
    });

    it("service_role keeps every other column, and the real upsert still works as it", async () => {
      await asService(() => index(1, { eventCount: 5 }));
      await asService(() => index(2, { eventCount: 6 }));
      expect((await stored()).event_count).toBe(6);
      await asService(() => db.query("UPDATE sessions.session_index SET title = 't'"));
      expect(await write({ revision: 2 })).toBe("applied");
      await asService(() => index(3, { eventCount: 999 }));
      expect(await stored()).toMatchObject({ event_count: 42, event_count_revision: 2 });
    });
  });

  it("tenants and the API roles can't call it", async () => {
    await index(1);
    for (const role of ["anon", "authenticated"])
      await expect(record(role)).rejects.toMatchObject({ code: "42501" });
  });

  it("a definer RLS would filter raises CN003, not index-behind", async () => {
    await index(1);
    const fn = "sessions.record_normalized_event_count(bigint, uuid)";
    await db.exec(`CREATE ROLE plain_owner NOLOGIN NOBYPASSRLS;
      GRANT USAGE ON SCHEMA sessions TO plain_owner;
      GRANT SELECT, UPDATE ON sessions.session_index TO plain_owner;
      GRANT SELECT, DELETE ON sessions.pipeline_jobs TO plain_owner;
      ALTER FUNCTION ${fn} OWNER TO plain_owner`);
    try {
      await expect(write()).rejects.toMatchObject({ code: "CN003" });
    } finally {
      await db.exec(`ALTER FUNCTION ${fn} OWNER TO normalized_ready_writer;
        REVOKE ALL ON sessions.session_index FROM plain_owner;
        REVOKE ALL ON sessions.pipeline_jobs FROM plain_owner;
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
