/**
 * epoch-event-time-repair.ts on PGlite with the real migrations: plan writes nothing and counts what
 * apply would mark; apply marks exactly the live, ready directory sessions with an event_ts = 'epoch'
 * row (not ready, epoch + 1, as the pointer trigger does); the mark fences a pass that read the old
 * epoch; a fixed pass at the new epoch replaces the event times and the rerun finds nothing; reruns
 * before that are no-ops; the CLI surface refuses bad input before touching a database.
 */
import { afterAll, beforeAll, beforeEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { SESSION_BOOTSTRAP_FILES } from "../../hoopdb/session-bootstrap-chain.ts";
import { directoryEventsFromProjection, writeDirectoryEvents } from "../../hoopdb/session-directory-events.ts";
import { projectSessionDirectory } from "../../hoopdb/session-directory-projector.ts";
import {
  EpochRepairRefused,
  epochRepairArgs,
  epochRepairDatabaseUrl,
  exitCodeForError,
  exitCodeForResult,
  repairEpochEventTimes,
  runEpochRepairMain,
  type EpochRepairDb,
} from "../epoch-event-time-repair.ts";
setDefaultTimeout(180_000);

const root = join(import.meta.dir, "../../..");
const read = (p: string) => readFileSync(join(root, p), "utf8");
const EPOCH = "1970-01-01T00:00:00.000000Z";
const REAL = "2026-10-30T12:00:00.123456Z";

let pg: PGlite;
const db: EpochRepairDb = { query: async <R>(text: string, params?: unknown[]) => ({ rows: (await pg.query(text, params as never)).rows as R[] }) };

beforeAll(async () => {
  pg = new PGlite({ extensions: { btree_gin, pg_trgm } });
  await pg.exec(read("supabase/tests/supabase_shim.sql"));
  for (const f of SESSION_BOOTSTRAP_FILES) await pg.exec(read(f));
  for (const f of readdirSync(join(root, "supabase/migrations")).filter((f) => f.endsWith(".sql")).sort()) await pg.exec(read("supabase/migrations/" + f));
  for (const f of readdirSync(join(root, "src/hoopdb/migrations")).filter((f) => f.endsWith(".sql")).sort()) await pg.exec(read("src/hoopdb/migrations/" + f));
});
afterAll(async () => { await pg?.close(); });

const keyOf = (workspaceId: string, sessionId: string) => ({ orgId: "org-a", workspaceId, harness: "claude", sessionId });
const manifest = (ws: string, s: string) => `96bulls/org-a/v1/ws=${ws}/m=srv_a/h=claude/d=2026/11/01/s=${s}/manifest.json`;
const event = (s: string, id: string, ts: string) => ({
  machine_id: "m1", event_id: id, source: "claude", session_id: s, kind: "session_event", ts,
  input_tokens: 0, output_tokens: 0, reasoning_tokens: 0, cache_read_tokens: 0, cache_create_tokens: 0,
  cache_create_5m_tokens: null, cache_create_1h_tokens: null, cost_usd_micros: null, task_ref: {}, task_title: null, task_status: null,
});
const epochOf = async (ws: string, s: string) =>
  (await pg.query<{ epoch: string }>("SELECT event_projection_epoch::text AS epoch FROM sessions.session_directory WHERE workspace_id=$1 AND session_id=$2", [ws, s])).rows[0]!.epoch;

/** A pass of the projection as it was (`times`) or as it is, at the directory's current epoch. */
async function pass(ws: string, s: string, times: string[], expectedEpoch?: string) {
  return writeDirectoryEvents(pg as never, keyOf(ws, s), {
    sourceSessionId: s,
    expectedEpoch: expectedEpoch ?? (await epochOf(ws, s)),
    sources: [{ manifestKey: manifest(ws, s), revision: "1" }],
    events: directoryEventsFromProjection(times.map((ts, i) => event(s, `e${i}`, ts))),
  });
}

async function seed(ws: string, s: string, times: string[]) {
  await pg.query(
    `INSERT INTO sessions.session_index(org_id,workspace_id,harness,session_id,machine_id,user_id,created_at,last_activity_at,storage_provider,bucket,manifest_key,raw_format,trace_id,revision,event_count)
     VALUES('org-a',$1,'claude',$2,'m1','u1','2026-10-01','2026-10-02','s3','b',$3,'ndjson','019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18',1,2)`,
    [ws, s, manifest(ws, s)],
  );
  await projectSessionDirectory(pg as never, keyOf(ws, s));
  expect(await pass(ws, s, times)).toEqual({ written: true });
}

const directory = async () =>
  (await pg.query<{ workspace_id: string; session_id: string; ready: boolean; epoch: string }>(
    `SELECT workspace_id, session_id, event_projection_ready AS ready, event_projection_epoch::text AS epoch
       FROM sessions.session_directory ORDER BY workspace_id, session_id`)).rows;
const epochRows = async () =>
  Number((await pg.query<{ n: string }>("SELECT count(*)::text AS n FROM sessions.session_directory_event_times WHERE event_ts='epoch'")).rows[0]!.n);

beforeEach(async () => {
  await pg.exec("DELETE FROM sessions.session_directory; DELETE FROM sessions.session_index; DELETE FROM sessions.session_directory_event_pending;");
  // ws-a: s-bug (the bug: an epoch event and its child), s-clean, s-pending (already owes a pass), s-gone (tombstoned).
  await seed("ws-a", "s-bug", [EPOCH, EPOCH, REAL]);
  await seed("ws-a", "s-clean", [REAL]);
  await seed("ws-a", "s-pending", [EPOCH, REAL]);
  await pg.exec("UPDATE sessions.session_directory SET event_projection_ready=false WHERE session_id='s-pending'");
  await pg.exec(`INSERT INTO sessions.session_directory(org_id,workspace_id,harness,session_id,created_at,last_activity_at,deleted_at)
    VALUES('org-a','ws-a','claude','s-gone',now(),now(),now());
    INSERT INTO sessions.session_directory_event_times VALUES('org-a','ws-a','claude','s-gone','epoch')`);
  // ws-b: one more affected session in another workspace.
  await seed("ws-b", "s-bug-b", [EPOCH]);
});

describe("repairEpochEventTimes (real migrations)", () => {
  test("plan writes nothing and counts what apply would mark", async () => {
    const before = await directory();
    const r = await repairEpochEventTimes(db, { apply: false, pageSize: 200 });
    expect(r).toEqual({ mode: "plan", workspaces: 2, affected: 4, markable: 2, marked: 0, already_pending: 1, not_live: 1, raced: 0, refused: 0, refused_codes: {}, complete: true });
    expect(exitCodeForResult(r)).toBe(0);
    expect(await directory()).toEqual(before);
  });

  test("apply marks only live, ready, affected sessions (not ready, epoch + 1); a rerun is a no-op", async () => {
    const before = new Map((await directory()).map((d) => [d.session_id, d]));
    const r = await repairEpochEventTimes(db, { apply: true, pageSize: 1 });
    expect(r).toMatchObject({ mode: "apply", workspaces: 2, affected: 4, markable: 2, marked: 2, already_pending: 1, not_live: 1, raced: 0, refused: 0, complete: true });
    expect(exitCodeForResult(r)).toBe(0);
    for (const d of await directory()) {
      const was = before.get(d.session_id)!;
      if (d.session_id === "s-bug" || d.session_id === "s-bug-b") expect(d).toEqual({ ...was, ready: false, epoch: String(Number(was.epoch) + 1) });
      else expect(d).toEqual(was);
    }
    // Nothing is deleted here: the consumer's next pass replaces the event times.
    expect(await epochRows()).toBe(4);
    const after = await directory();
    const again = await repairEpochEventTimes(db, { apply: true, pageSize: 200 });
    expect(again).toMatchObject({ affected: 4, markable: 0, marked: 0, already_pending: 3, not_live: 1 });
    expect(await directory()).toEqual(after);
  });

  test("the mark fences a pass that read the old epoch; a fixed pass at the new epoch clears the session and a rerun finds nothing", async () => {
    const stale = await epochOf("ws-a", "s-bug");
    await repairEpochEventTimes(db, { apply: true, pageSize: 200 });
    // An old-code pass that read the epoch before the mark cannot make the session ready over the epoch rows.
    expect(await pass("ws-a", "s-bug", [EPOCH, REAL], stale)).toEqual({ written: false });
    // The fixed projection: the ts_ms 0 event and its child take the delivery time.
    expect(await pass("ws-a", "s-bug", [REAL, REAL, REAL])).toEqual({ written: true });
    expect(await pass("ws-b", "s-bug-b", [REAL])).toEqual({ written: true });
    await pg.exec("UPDATE sessions.session_directory SET event_projection_ready=true WHERE session_id='s-pending'");
    await pass("ws-a", "s-pending", [REAL]);
    const { rows } = await pg.query<{ session_id: string; ready: boolean; first: string }>(
      "SELECT session_id, event_projection_ready AS ready, event_first_ts::text AS first FROM sessions.session_directory WHERE session_id IN ('s-bug','s-bug-b') ORDER BY session_id");
    for (const row of rows) { expect(row.ready).toBe(true); expect(row.first.startsWith("1970")).toBe(false); }
    expect(await epochRows()).toBe(1); // only the tombstone's, which has nothing to re-project
    const r = await repairEpochEventTimes(db, { apply: true, pageSize: 200 });
    expect(r).toMatchObject({ affected: 1, markable: 0, marked: 0, not_live: 1, complete: true });
  });

  test("a session re-projected between selection and UPDATE is raced, not marked", async () => {
    const racing: EpochRepairDb = {
      query: async <R>(text: string, params?: unknown[]) => {
        if (text.startsWith("UPDATE sessions.session_directory d") && params?.[3] === "s-bug") await pass("ws-a", "s-bug", [REAL]);
        return db.query<R>(text, params);
      },
    };
    const before = await epochOf("ws-a", "s-bug");
    const r = await repairEpochEventTimes(racing, { apply: true, pageSize: 200 });
    expect(r).toMatchObject({ marked: 1, raced: 1 });
    expect(await epochOf("ws-a", "s-bug")).toBe(before);
  });

  test("a 1 ms time is a reported time and is left alone", async () => {
    await seed("ws-a", "s-one-ms", ["1970-01-01T00:00:00.001000Z"]);
    const r = await repairEpochEventTimes(db, { apply: false, pageSize: 200 });
    expect(r).toMatchObject({ affected: 4, markable: 2 });
  });

  test("--max-pages stops early and reports incomplete (exit 3); resuming finishes", async () => {
    const r = await repairEpochEventTimes(db, { apply: true, pageSize: 1, maxPages: 2 });
    expect(r).toMatchObject({ complete: false, marked: 1 });
    expect(exitCodeForResult(r)).toBe(3);
    const rest = await repairEpochEventTimes(db, { apply: true, pageSize: 1 });
    expect(rest).toMatchObject({ complete: true, marked: 1 });
  });

  test("a refused UPDATE is counted by SQLSTATE and exits 3", async () => {
    const refusing: EpochRepairDb = {
      query: async <R>(text: string, params?: unknown[]) => {
        if (text.startsWith("UPDATE sessions.session_directory d")) throw Object.assign(new Error("lock"), { code: "55P03" });
        return db.query<R>(text, params);
      },
    };
    const r = await repairEpochEventTimes(refusing, { apply: true, pageSize: 200 });
    expect(r).toMatchObject({ marked: 0, refused: 2, refused_codes: { "55P03": 2 }, complete: true });
    expect(exitCodeForResult(r)).toBe(3);
  });
});

describe("CLI surface", () => {
  test("args: plan by default, strict flags", () => {
    expect(epochRepairArgs([])).toEqual({ apply: false, pageSize: 200 });
    expect(epochRepairArgs(["--apply", "--page", "50", "--max-pages", "3"])).toEqual({ apply: true, pageSize: 50, maxPages: 3 });
    for (const bad of [["--apply", "--plan"], ["--page", "0"], ["--page", "1001"], ["--page"], ["--max-pages", "0"], ["--org", "x"], ["apply"]])
      expect(() => epochRepairArgs(bad)).toThrow(EpochRepairRefused);
  });

  test("database url: exactly one source, an absolute non-empty file", () => {
    expect(epochRepairDatabaseUrl({ EPOCH_EVENT_TIME_REPAIR_DATABASE_URL: "postgres://x" })).toBe("postgres://x");
    for (const env of [{}, { EPOCH_EVENT_TIME_REPAIR_DATABASE_URL: "" }, { EPOCH_EVENT_TIME_REPAIR_DATABASE_URL: "a", EPOCH_EVENT_TIME_REPAIR_DATABASE_URL_FILE: "/x" }, { EPOCH_EVENT_TIME_REPAIR_DATABASE_URL_FILE: "relative" }, { EPOCH_EVENT_TIME_REPAIR_DATABASE_URL_FILE: "/nonexistent/x" }])
      expect(() => epochRepairDatabaseUrl(env)).toThrow(EpochRepairRefused);
  });

  test("main: bad args refuse (exit 2) before any query; plan runs on the injected database", async () => {
    let queried = 0;
    const counting: EpochRepairDb = { query: async <R>(t: string, p?: unknown[]) => { queried++; return db.query<R>(t, p); } };
    const refused = await runEpochRepairMain({}, ["--bogus"], { db: counting }).catch((e) => e);
    expect(exitCodeForError(refused)).toBe(2);
    expect(queried).toBe(0);
    expect(exitCodeForError(new Error("boom"))).toBe(4);
    const r = await runEpochRepairMain({}, [], { db: counting, log: () => {} });
    expect(r).toMatchObject({ mode: "plan", affected: 4 });
  });
});
