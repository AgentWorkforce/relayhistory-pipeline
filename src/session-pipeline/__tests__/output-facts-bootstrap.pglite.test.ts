/**
 * The output facts bootstrap's SQL on PGlite with every migration (045040 included, applied
 * the way src/hoopdb/__tests__/selfhost-output-facts.pglite.test.ts applies it) plus the
 * consumer bootstrap's service tables: the Phase A enqueue into the consumer's pending queue,
 * the progress counts, the Phase B watermark (written only for complete workspaces, never
 * lowered, tombstoned sessions ignored, idempotent), and the owner and ledger gates.
 *
 * State rows are seeded directly as the superuser: the 045040 writers are the consumer's
 * (runtime role only) and are covered by selfhost-output-facts.pglite.test.ts; this job
 * never calls them.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import { SESSION_BOOTSTRAP_FILES } from "../../hoopdb/session-bootstrap-chain.ts";
import { expectedSchema } from "../../hoopdb/migrate.ts";
import { ensureServiceTables } from "../consumer-bootstrap.ts";
import {
  BootstrapRefused,
  EXIT,
  OUTPUT_FACTS_MIGRATION,
  PENDING_MARKER,
  assertBootstrapOwner,
  assertOutputFactsLedger,
  bootstrapProgress,
  enqueueOwingScopes,
  markCompleteWorkspaces,
  runOutputFactsBootstrap,
  watermarkLock,
} from "../output-facts-bootstrap.ts";

const root = join(import.meta.dir, "../../..");
const read = (path: string) => readFileSync(join(root, path), "utf8");
const BACKEND = "selfhost-test";
const PREFIX = "96bulls";

const open: PGlite[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((db) => db.close()));
});

async function database(): Promise<PGlite> {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  open.push(db);
  await db.exec(read("supabase/tests/supabase_shim.sql"));
  for (const f of SESSION_BOOTSTRAP_FILES) await db.exec(read(f));
  for (const file of readdirSync(join(root, "supabase/migrations")).filter((f) => f.endsWith(".sql")).sort())
    await db.exec(read(`supabase/migrations/${file}`));
  await ensureServiceTables(db as unknown as SqlExecutor);
  return db;
}

const sql = (db: PGlite) => db as unknown as SqlExecutor;

/** One logical session: its directory row, then one live pointer (the trigger bumps the epoch). */
async function session(db: PGlite, org: string, ws: string, id: string, opts: { prefix?: string; machine?: string } = {}): Promise<string> {
  const machine = opts.machine ?? "machine_a";
  const key = `${opts.prefix ?? PREFIX}/${org}/v1/ws=${ws}/m=${machine}/h=codex/d=2026/11/08/s=${id}/manifest.json`;
  await db.query(
    `INSERT INTO sessions.session_directory(org_id,workspace_id,harness,session_id,created_at,last_activity_at)
     VALUES($1,$2,'codex',$3,now(),now()) ON CONFLICT DO NOTHING`, [org, ws, id]);
  await db.query(
    `INSERT INTO sessions.session_index(org_id,workspace_id,harness,session_id,machine_id,user_id,title,created_at,last_activity_at,storage_provider,bucket,manifest_key,trace_id)
     VALUES($1,$2,'codex',$3,$4,'user_a','fixture',now(),now(),'s3','fixture-bucket',$5,'01984624-2222-7000-8000-111111111111')`,
    [org, ws, id, machine, key]);
  return key;
}

const epoch = async (db: PGlite, org: string, ws: string, id: string) =>
  Number((await db.query<{ e: number | string }>(
    "SELECT event_projection_epoch AS e FROM sessions.session_directory WHERE org_id=$1 AND workspace_id=$2 AND harness='codex' AND session_id=$3",
    [org, ws, id])).rows[0]!.e);

/** What a consumer pass leaves behind (seeded as the superuser; see the header). */
async function state(db: PGlite, org: string, ws: string, id: string, opts: { complete?: boolean; minTs?: string | null; version?: number } = {}) {
  const version = opts.version ?? (await epoch(db, org, ws, id));
  const minTs = opts.minTs === undefined ? "2026-11-01T00:00:00Z" : opts.minTs;
  await db.query(
    `INSERT INTO sessions.output_facts_session_state(org_id,workspace_id,source,session_id,complete,synced_at,min_ts,max_ts,source_version)
     VALUES($1,$2,'codex',$3,$4,now(),$5,$5,$6)
     ON CONFLICT(org_id,workspace_id,source,session_id) DO UPDATE SET complete=excluded.complete,min_ts=excluded.min_ts,max_ts=excluded.max_ts,source_version=excluded.source_version`,
    [org, ws, id, opts.complete ?? true, minTs, version]);
}

async function mark(db: PGlite) {
  await db.exec("BEGIN");
  try {
    await watermarkLock(sql(db));
    const out = await markCompleteWorkspaces(sql(db));
    await db.exec("COMMIT");
    return out;
  } catch (e) {
    await db.exec("ROLLBACK");
    throw e;
  }
}

const watermarks = async (db: PGlite) =>
  (await db.query<{ org_id: string; workspace_id: string; complete_since: Date; marked_by: string }>(
    "SELECT org_id, workspace_id, complete_since, marked_by FROM sessions.output_facts_workspace_state ORDER BY org_id, workspace_id")).rows
    .map((r) => [r.org_id, r.workspace_id, new Date(r.complete_since).toISOString(), r.marked_by]);

describe("output facts bootstrap SQL", () => {
  test("the watermark is written only when every live session is complete and current", async () => {
    const db = await database();
    await session(db, "org_a", "ws_done", "s1");
    await session(db, "org_a", "ws_done", "s2");
    await state(db, "org_a", "ws_done", "s1", { minTs: "2026-10-20T00:00:00Z" });
    await state(db, "org_a", "ws_done", "s2", { minTs: "2026-10-05T00:00:00Z" });
    // Three ways to owe: no state row, an incomplete one, one older than the directory epoch.
    await session(db, "org_a", "ws_missing", "s3");
    await session(db, "org_a", "ws_incomplete", "s4");
    await state(db, "org_a", "ws_incomplete", "s4", { complete: false });
    await session(db, "org_a", "ws_stale", "s5");
    await state(db, "org_a", "ws_stale", "s5", { version: (await epoch(db, "org_a", "ws_stale", "s5")) - 1 });

    const summary = await mark(db);
    expect(summary).toEqual({ workspaces: 4, complete: 1, incomplete: 3, marked: 1, alreadyMarked: 0, markedIncomplete: 0 });
    // complete_since is the earliest min_ts of the workspace's live sessions.
    expect(await watermarks(db)).toEqual([["org_a", "ws_done", "2026-10-05T00:00:00.000Z", "bootstrap"]]);
  }, 180_000);

  test("a rerun never lowers (or rewrites) an existing watermark and is idempotent", async () => {
    const db = await database();
    await session(db, "org_a", "ws_1", "s1");
    await state(db, "org_a", "ws_1", "s1", { minTs: "2026-10-20T00:00:00Z" });
    expect((await mark(db)).marked).toBe(1);
    const first = await watermarks(db);
    // An older session completes later: the earliest min_ts drops, the watermark does not.
    await session(db, "org_a", "ws_1", "s0");
    await state(db, "org_a", "ws_1", "s0", { minTs: "2026-01-01T00:00:00Z" });
    expect(await mark(db)).toEqual({ workspaces: 1, complete: 1, incomplete: 0, marked: 0, alreadyMarked: 1, markedIncomplete: 0 });
    expect(await watermarks(db)).toEqual(first);
    expect(first[0]![2]).toBe("2026-10-20T00:00:00.000Z");
    // A workspace that became incomplete keeps its earlier mark untouched and is reported.
    await session(db, "org_a", "ws_1", "s9");
    expect(await mark(db)).toEqual({ workspaces: 1, complete: 0, incomplete: 1, marked: 0, alreadyMarked: 0, markedIncomplete: 1 });
    expect(await watermarks(db)).toEqual(first);
  }, 180_000);

  test("tombstoned sessions are ignored; a workspace with no live session is marked at now()", async () => {
    const db = await database();
    await session(db, "org_a", "ws_1", "live");
    await state(db, "org_a", "ws_1", "live", { minTs: "2026-10-20T00:00:00Z" });
    await session(db, "org_a", "ws_1", "gone"); // owes facts, but erased
    await session(db, "org_a", "ws_empty", "gone2");
    await db.exec("UPDATE sessions.session_directory SET deleted_at = now() WHERE session_id IN ('gone','gone2')");
    const before = Date.now();
    expect(await mark(db)).toEqual({ workspaces: 2, complete: 2, incomplete: 0, marked: 2, alreadyMarked: 0, markedIncomplete: 0 });
    const rows = await watermarks(db);
    expect(rows[0]).toEqual(["org_a", "ws_1", "2026-10-20T00:00:00.000Z", "bootstrap"]);
    expect(rows[1]![1]).toBe("ws_empty");
    expect(Date.parse(rows[1]![2]!)).toBeGreaterThanOrEqual(before - 60_000);
    expect((await bootstrapProgress(sql(db), BACKEND)).owing).toBe(0);
  }, 180_000);

  test("Phase A queues each live manifest of each owing session once, under our prefix only, never disturbing an existing row", async () => {
    const db = await database();
    const done = await session(db, "org_a", "ws_1", "done");
    await state(db, "org_a", "ws_1", "done");
    const owing = await session(db, "org_a", "ws_1", "owing");
    const second = await session(db, "org_a", "ws_1", "owing", { machine: "machine_b" });
    const dead = await session(db, "org_a", "ws_1", "deadlettered");
    await session(db, "org_a", "ws_1", "other", { prefix: "elsewhere" });
    await session(db, "org_a", "ws_1", "erased");
    await db.exec("UPDATE sessions.session_directory SET deleted_at = now() WHERE session_id = 'erased'");
    await db.query(
      `INSERT INTO sessions.search_consumer_pending(backend_id,manifest_key,attempts,next_at,last_error_code,dead,updated_at)
       VALUES($1,$2,9,now(),'boom',true,now())`, [BACKEND, dead]);

    // Batch size 1 forces the keyset loop through every page.
    expect(await enqueueOwingScopes(sql(db), BACKEND, PREFIX, 1)).toEqual({ keys: 3, enqueued: 2, batches: 3 });
    const { rows } = await db.query<{ manifest_key: string; attempts: number; dead: boolean; last_error_code: string }>(
      "SELECT manifest_key, attempts, dead, last_error_code FROM sessions.search_consumer_pending ORDER BY manifest_key");
    expect(rows).toEqual([
      { manifest_key: dead, attempts: 9, dead: true, last_error_code: "boom" },
      { manifest_key: owing, attempts: 0, dead: false, last_error_code: PENDING_MARKER },
      { manifest_key: second, attempts: 0, dead: false, last_error_code: PENDING_MARKER },
    ].sort((a, b) => a.manifest_key.localeCompare(b.manifest_key)));
    expect(rows.some((r) => r.manifest_key === done)).toBe(false);
    expect(await bootstrapProgress(sql(db), BACKEND)).toEqual({ owing: 3, inFlight: 2, dead: 1 });
    // Idempotent: a rerun inserts nothing new.
    expect(await enqueueOwingScopes(sql(db), BACKEND, PREFIX)).toEqual({ keys: 3, enqueued: 0, batches: 1 });
    // A LIKE metacharacter in the prefix is literal.
    expect(await enqueueOwingScopes(sql(db), BACKEND, "96bull_")).toEqual({ keys: 0, enqueued: 0, batches: 0 });
  }, 180_000);

  test("end to end with a fake consumer: partial gives exit 3 and no watermark for the incomplete workspace; the rerun completes it", async () => {
    const db = await database();
    await session(db, "org_a", "ws_ok", "s1");
    await session(db, "org_a", "ws_stuck", "s2");
    const io = (consume: () => Promise<void>) => ({
      enqueue: () => enqueueOwingScopes(sql(db), BACKEND, PREFIX),
      progress: () => bootstrapProgress(sql(db), BACKEND),
      mark: () => mark(db),
      now: () => 0,
      sleep: consume,
    });
    // The fake consumer projects ws_ok's session and drains the whole queue; ws_stuck stays owing.
    const partial = await runOutputFactsBootstrap(io(async () => {
      await state(db, "org_a", "ws_ok", "s1");
      await db.exec("DELETE FROM sessions.search_consumer_pending");
    }), { deadlineMs: 60_000, allowPartial: false, pollMs: 1 });
    expect(partial.code).toBe(EXIT.incomplete);
    expect(partial.mark).toMatchObject({ complete: 1, incomplete: 1, marked: 1 });
    expect((await watermarks(db)).map((r) => r[1])).toEqual(["ws_ok"]);

    const rerun = await runOutputFactsBootstrap(io(async () => {
      await state(db, "org_a", "ws_stuck", "s2");
      await db.exec("DELETE FROM sessions.search_consumer_pending");
    }), { deadlineMs: 60_000, allowPartial: false, pollMs: 1 });
    expect(rerun.code).toBe(EXIT.ok);
    expect(rerun.enqueue).toEqual({ keys: 1, enqueued: 1, batches: 1 });
    expect(rerun.mark).toMatchObject({ complete: 2, incomplete: 0, marked: 1, alreadyMarked: 1 });
    expect((await watermarks(db)).map((r) => r[1])).toEqual(["ws_ok", "ws_stuck"]);
  }, 180_000);
});

describe("gates", () => {
  test("owner check: the owner passes; the runtime and reader roles are refused; a missing service table is refused", async () => {
    const db = await database();
    await assertBootstrapOwner(sql(db));
    for (const role of ["search_consumer_runtime", "bulls96_read_api"]) {
      await db.exec(`SET ROLE ${role}`);
      try {
        await expect(assertBootstrapOwner(sql(db))).rejects.toBeInstanceOf(BootstrapRefused);
      } finally {
        await db.exec("RESET ROLE");
      }
    }
    await db.exec("DROP TABLE sessions.search_consumer_pending");
    await expect(assertBootstrapOwner(sql(db))).rejects.toThrow("missing");
  }, 180_000);

  test("ledger check against this image's planned checksum", async () => {
    const db = await database();
    const expected = await expectedSchema();
    await expect(assertOutputFactsLedger(sql(db), expected)).rejects.toThrow("ledger missing");
    await db.exec("CREATE TABLE sessions.__96bulls_migrations(name text PRIMARY KEY, sha256 text NOT NULL)");
    await expect(assertOutputFactsLedger(sql(db), expected)).rejects.toThrow("not recorded");
    await db.query("INSERT INTO sessions.__96bulls_migrations VALUES($1,$2)", [OUTPUT_FACTS_MIGRATION, "0".repeat(64)]);
    await expect(assertOutputFactsLedger(sql(db), expected)).rejects.toThrow("not recorded");
    await db.query("UPDATE sessions.__96bulls_migrations SET sha256=$2 WHERE name=$1", [OUTPUT_FACTS_MIGRATION, expected.shas![OUTPUT_FACTS_MIGRATION]]);
    await assertOutputFactsLedger(sql(db), expected);
  }, 180_000);
});
