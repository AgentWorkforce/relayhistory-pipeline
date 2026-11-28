/**
 * 20261105062350: sessions.buffered_search_feed_page(jsonb) with a sargable keyset and force_custom_plan, through the
 * real migrator, against 043870's exact body (installed under a scratch name in the test).
 *
 * Equivalence: for a synthetic backlog (stages before `since`, exactly at `since`, created_at ties, inactive and
 * not-acknowledged stages) and cursors NULL, mid-backlog, at a created_at tie, at the last row and past the end, the
 * new function returns exactly the old body's page, for several limits; a full walk returns every eligible stage once,
 * in (created_at, stage_token) order. Plans: auto_explain (nested, NOTICE) shows the page query as Limit <- Index Scan
 * on ingest_stage_intents_cleanup_idx in index order, the row comparison in its Index Cond and the outbox check a
 * per-row SubPlan probe, under both a forced generic and a custom plan; on the same data the 043870 body reproduces
 * the production shape (hash join estimated at ~1 row, Seq Scan of the outbox, Sort, keyset only a Filter). Rollback: the lane-generated down restores 043870's exact body and proconfig.
 * Synthetic, secret-free data only.
 */
import { afterAll, beforeAll, describe, expect, setDefaultTimeout, test } from "bun:test";
import { createHash, randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { auto_explain } from "@electric-sql/pglite/contrib/auto_explain";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { MIGRATION_SENTINELS } from "../../hoopdb/migration-sentinels.ts";
import { runMigrations, type MigrationClient, type MigrationProfile } from "../../hoopdb/migrate.ts";
import { BUFFERED_SEARCH_FEED_RUNTIME_ROLE as FEED_ROLE } from "../buffered-feed-role.ts";

setDefaultTimeout(300_000);
const root = join(import.meta.dir, "../../..");
const read = (path: string) => readFileSync(join(root, path), "utf8");
const FILE = "20261105062350_buffered_search_feed_page_keyset.sql";
const STEP = `upstream:${FILE}`;
const UP = read(`supabase/migrations/${FILE}`);
const DOWN = read("supabase/rollbacks/20261105062350_buffered_search_feed_page_keyset.down.sql");
// 20261105062360 (the session erasure replay manifest) sits above 062350: its down runs first.
const MANIFEST_DOWN = read("supabase/rollbacks/20261105062360_session_erasure_replay_manifest.down.sql");
const ORIGINAL = read("supabase/migrations/20261105043870_buffered_search_feed_cursor.sql");
const LOCAL: MigrationProfile = { mode: "local", shim: true, bootstrap: true, verify: false, runtimeRole: "postgres" };
const PAGE = "sessions.buffered_search_feed_page(jsonb)";
const OLD = "sessions.zz_page_043870(jsonb)";
const PRIOR_CONFIG = ["search_path=pg_catalog, sessions, pg_temp", "lock_timeout=2s"];
const SINCE = "2026-11-05T00:00:00Z";
const ORG = "org-pk", WS = "ws-pk", ORIGIN = "origin-pk";
const sha = (s: string) => createHash("sha256").update(s).digest("hex");
const bodyOf = (src: string) => {
  const m = /CREATE(?: OR REPLACE)? FUNCTION sessions\.buffered_search_feed_page\(p jsonb\)[\s\S]*?END \$page\$;/u.exec(src)![0];
  const open = m.indexOf("$page$") + 6;
  return { def: m, body: m.slice(open, m.indexOf("$page$", open)) };
};

function client(db: PGlite): MigrationClient {
  return { async query<R>(text: string, params?: unknown[]) {
    if (params?.length) return (await db.query<R>(text, params)) as { rows: R[] };
    const results = await db.exec(text);
    return { rows: (results.at(-1)?.rows ?? []) as R[] };
  } };
}

type Stage = { stageToken: string; createdAt: string };
type Page = { cursor: string; since: string; after: unknown; stages: Stage[] };
let db: PGlite;
/** Every stage the page may return, in (created_at, stage_token) order (computed in SQL: uuid ordering is PG's). */
let eligible: Stage[] = [];

/** A backlog of `n` eligible stages from 2026-11-04 (before since) on, with ties, plus inactive/preparing/retired noise. */
async function seed(n: number) {
  await db.exec("SET session_replication_role = replica; BEGIN");
  let k = 0;
  const intent = async (createdAt: string, opts: { active?: boolean; receipt?: string; cleanup?: string } = {}) => {
    const batch = "b-" + ++k, token = randomUUID(), claim = randomUUID();
    await db.query("INSERT INTO sessions.ingest_batches(org_id,workspace_id,origin_id,batch_id,user_id,digest,receipt_id,state,committed_at,publication_state,buffer_receipt_kind) VALUES($1,$2,$3,$4,'user-pk',$5,'rhr_'||repeat('a',64),'committed',clock_timestamp(),'buffered','staged')", [ORG, WS, ORIGIN, batch, sha(batch)]);
    await db.query("INSERT INTO sessions.ingest_claim_attempts(claim_token,org_id,workspace_id,user_id,origin_id,batch_id,digest,epochs,records,reset_epoch,schema_generation) VALUES($1,$2,$3,'user-pk',$4,$5,$6,'[]','[]',0,repeat('a',64))", [claim, ORG, WS, ORIGIN, batch, sha(batch)]);
    await db.query("INSERT INTO sessions.ingest_stage_intents(stage_token,claim_token,org_id,workspace_id,user_id,origin_id,batch_id,digest,stage_key,stage_sha256,stage_bytes,group_ordinals,reset_epoch,receipt_state,cleanup_state,expires_at,created_at) VALUES($1,$2,$3,$4,'user-pk',$5,$6,$7,$8,$9,10,'[[0]]',0,$10,$11,$12::timestamptz+interval '5 minutes',$12)",
      [token, claim, ORG, WS, ORIGIN, batch, sha(batch), "k/" + token, sha(token), opts.receipt ?? "acknowledged", opts.cleanup ?? "live", createdAt]);
    if (opts.active !== false)
      await db.query("INSERT INTO sessions.ingest_buffer_outbox(org_id,workspace_id,origin_id,batch_id,user_id,digest,active_stage_token) VALUES($1,$2,$3,$4,'user-pk',$5,$6)", [ORG, WS, ORIGIN, batch, sha(batch), token]);
  };
  const base = Date.parse("2026-11-04T23:59:00Z");
  for (let i = 0; i < n; i++) {
    // Every 10th group of three shares one created_at (ties broken by stage_token); a few land exactly at since.
    const at = i % 10 < 3 ? base + Math.floor(i / 10) * 997_000 : base + i * 331_000 + (i % 7);
    await intent(new Date(at).toISOString());
    if (i % 50 === 0) await intent(new Date(at + 1).toISOString(), { active: false });
    if (i % 50 === 1) await intent(new Date(at + 2).toISOString(), { receipt: "preparing" });
    if (i % 50 === 2) await intent(new Date(at + 3).toISOString(), { cleanup: "retired" });
  }
  for (let i = 0; i < 3; i++) await intent(SINCE);
  await db.exec("COMMIT; SET session_replication_role = origin; ANALYZE sessions.ingest_stage_intents; ANALYZE sessions.ingest_buffer_outbox");
  eligible = (await db.query<Stage>(`SELECT s.stage_token::text "stageToken", to_char(s.created_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"') "createdAt"
    FROM sessions.ingest_stage_intents s JOIN sessions.ingest_buffer_outbox o ON o.active_stage_token=s.stage_token AND o.state='buffered'
   WHERE s.cleanup_state='live' AND s.receipt_state='acknowledged' AND s.created_at>=$1 ORDER BY s.created_at,s.stage_token`, [SINCE])).rows;
}

/** One call as the genuine feed login; `notices` collects auto_explain plans of nested statements when given. */
async function call(fn: string, cursor: string, limit: number, notices?: string[]) {
  await db.exec("SET SESSION AUTHORIZATION " + FEED_ROLE);
  try {
    const r = await db.query<{ r: Page }>(`SELECT ${fn.replace("(jsonb)", "")}($1::jsonb) r`, [JSON.stringify({ cursor, since: SINCE, limit })],
      notices ? { onNotice: (m) => { if (String(m.message).includes("Query Text:")) notices.push(String(m.message)); } } : undefined);
    return r.rows[0]!.r;
  } finally {
    await db.exec("SET SESSION AUTHORIZATION postgres");
  }
}
const setCursor = (name: string, after: Stage | null) =>
  db.query("UPDATE sessions.buffered_search_feed_cursors SET after_created_at=$2::timestamptz, after_stage_token=$3::uuid WHERE name=$1",
    [name, after?.createdAt ?? null, after?.stageToken ?? null]);
const tokens = (p: Page) => p.stages.map((s) => s.stageToken);

beforeAll(async () => {
  db = new PGlite({ extensions: { auto_explain, btree_gin, pg_trgm } });
  await runMigrations(client(db), { profile: LOCAL });
  // 043870's exact page function under a scratch name, same owner and grant, for the equivalence comparison.
  await db.exec(bodyOf(ORIGINAL).def.replace("CREATE FUNCTION sessions.buffered_search_feed_page(", "CREATE FUNCTION sessions.zz_page_043870("));
  await db.exec(`REVOKE ALL ON FUNCTION ${OLD} FROM PUBLIC; GRANT EXECUTE ON FUNCTION ${OLD} TO ${FEED_ROLE}`);
  await seed(3000);
});
afterAll(async () => { await db.close(); });

describe("20261105062350 buffered search feed page keyset", () => {
  test("installed: the 043870 body except the keyset, force_custom_plan, same signature, SECURITY DEFINER, owner and ACL", async () => {
    const live = (await db.query<{ s: string; c: string[]; d: boolean; v: string; acl: string; own: string }>(
      "SELECT prosrc s, proconfig c, prosecdef d, provolatile v, proacl::text acl, pg_get_userbyid(proowner) own FROM pg_proc WHERE oid=$1::regprocedure", [PAGE])).rows[0]!;
    expect(live.c).toEqual([...PRIOR_CONFIG, "plan_cache_mode=force_custom_plan"]);
    expect([live.d, live.v]).toEqual([true, "v"]);
    expect(live.s).toBe(bodyOf(UP).body);
    const old = (await db.query<{ acl: string }>("SELECT proacl::text acl FROM pg_proc WHERE oid=$1::regprocedure", [OLD])).rows[0]!;
    expect(live.acl).toBe(old.acl); // owner EXECUTE + buffered_search_feed_runtime EXECUTE only, as 043870 left it
    expect(live.acl).toContain(FEED_ROLE + "=X/");
    // Exactly 043870's body with the two predicate replacements (and the comment) the migration header documents.
    const swap = (b: string, from: string, to: string) => { expect(b.split(from).length).toBe(2); return b.replace(from, to); };
    let expected = bodyOf(ORIGINAL).body;
    expected = swap(expected, " -- ingest_stage_intents_cleanup_idx(cleanup_state,created_at,stage_token) serves this keyset.\n",
      bodyOf(UP).body.slice(bodyOf(UP).body.indexOf(" -- ingest_stage_intents_cleanup_idx"), bodyOf(UP).body.indexOf(" SELECT coalesce(jsonb_agg(")));
    expected = swap(expected, "      AND (c.after_created_at IS NULL OR (s.created_at,s.stage_token)>(c.after_created_at,c.after_stage_token))\n",
      "      AND (s.created_at,s.stage_token)>(coalesce(c.after_created_at,c.since-interval '1 microsecond'),\n       coalesce(c.after_stage_token,'00000000-0000-0000-0000-000000000000'::uuid))\n");
    expected = swap(expected, "      AND EXISTS(SELECT 1 FROM sessions.ingest_buffer_outbox o WHERE o.org_id=s.org_id AND o.workspace_id=s.workspace_id\n       AND o.origin_id=s.origin_id AND o.batch_id=s.batch_id AND o.state='buffered' AND o.active_stage_token=s.stage_token)\n",
      "      AND (SELECT true FROM sessions.ingest_buffer_outbox o WHERE o.org_id=s.org_id AND o.workspace_id=s.workspace_id\n       AND o.origin_id=s.origin_id AND o.batch_id=s.batch_id AND o.state='buffered' AND o.active_stage_token=s.stage_token\n       LIMIT 1) IS TRUE\n");
    expect(live.s).toBe(expected);
    expect((await db.query<{ applied: boolean }>(MIGRATION_SENTINELS[STEP]!.sql)).rows[0]!.applied).toBe(true);
    expect((await db.query<{ sha256: string }>("SELECT sha256 FROM sessions.__96bulls_migrations WHERE name=$1", [STEP])).rows[0]!.sha256).toBe(sha(UP));
    expect(UP.split("\n").filter((l) => !l.startsWith("--")).join("\n")).not.toMatch(/\b(?:GRANT|REVOKE)\s/u);
  });

  test("equivalence: the same page as 043870's body for NULL, mid, tie, last and past-the-end cursors and several limits", async () => {
    expect(eligible.length).toBeGreaterThan(2900);
    expect(eligible.filter((s) => s.createdAt === "2026-11-05T00:00:00.000000Z").length).toBeGreaterThanOrEqual(3);
    const tie = eligible.findIndex((s, i) => i > 0 && i < eligible.length - 1 && eligible[i + 1]!.createdAt === s.createdAt && eligible[i - 1]!.createdAt === s.createdAt);
    expect(tie).toBeGreaterThan(0);
    const cursors: [string, Stage | null][] = [
      ["null", null], ["first", eligible[0]!], ["mid", eligible[Math.floor(eligible.length / 2)]!], ["tie", eligible[tie]!],
      ["before-last", eligible.at(-2)!], ["last", eligible.at(-1)!],
      ["past-end", { createdAt: "2027-01-01T00:00:00.000000Z", stageToken: "ffffffff-ffff-ffff-ffff-ffffffffffff" }],
      ["before-since", { createdAt: "2026-11-04T00:00:00.000000Z", stageToken: "00000000-0000-0000-0000-000000000000" }],
    ];
    for (const [name, after] of cursors) for (const limit of [1, 3, 7, 200]) {
      const cursor = "eq-" + name;
      await call(PAGE, cursor, 1);
      await setCursor(cursor, after);
      const neu = await call(PAGE, cursor, limit), old = await call(OLD, cursor, limit);
      expect([name, limit, neu]).toEqual([name, limit, old]);
      const from = after === null ? 0 : eligible.findIndex((s) => s.createdAt === after.createdAt && s.stageToken === after.stageToken) + 1;
      const expected = after === null || from > 0 ? eligible.slice(from, from + limit)
        : eligible.filter((s) => s.createdAt > after.createdAt).slice(0, limit);
      expect([name, limit, tokens(neu)]).toEqual([name, limit, expected.map((s) => s.stageToken)]);
    }
  });

  test("a full walk (advance the cursor to each page's last row) returns every eligible stage once, in order, as the old body does", async () => {
    for (const [fn, cursor] of [[PAGE, "walk-new"], [OLD, "walk-old"]] as const) {
      const seen: string[] = [];
      await call(fn, cursor, 1);
      for (;;) {
        const p = await call(fn, cursor, 200);
        if (!p.stages.length) break;
        seen.push(...tokens(p));
        await setCursor(cursor, p.stages.at(-1)!);
      }
      expect([fn, seen]).toEqual([fn, eligible.map((s) => s.stageToken)]);
    }
  });

  test("plans: an ordered cleanup_idx walk to the limit with the row comparison as Index Cond and a per-row outbox probe, under custom and forced generic plans (the old body: hash join + sort)", async () => {
    const after = eligible[Math.floor(eligible.length * 0.9)]!;
    const pagePlan = async (fn: string, cursor: string, mode: string) => {
      await call(fn, cursor, 1);
      await setCursor(cursor, after);
      await db.exec(`ALTER FUNCTION ${fn} SET plan_cache_mode = ${mode}`); // test-only; restored below
      await db.exec(`LOAD 'auto_explain'; SET auto_explain.log_min_duration = 0; SET auto_explain.log_nested_statements = on;
        SET auto_explain.log_level = notice; SET auto_explain.log_format = text`);
      try {
        const plans: string[][] = [];
        for (let i = 0; i < 8; i++) {
          const notices: string[] = [];
          const p = await call(fn, cursor, 50, notices);
          expect(tokens(p)[0]).toBe(eligible[Math.floor(eligible.length * 0.9) + 1]!.stageToken);
          plans.push(notices.filter((n) => n.includes("FROM sessions.ingest_stage_intents s")));
        }
        return plans.map((p) => { expect(p).toHaveLength(1); return p[0]!; }).at(-1)!;
      } finally {
        await db.exec("RESET auto_explain.log_min_duration");
      }
    };
    const rowCond = /Index Cond: \(.*ROW\(created_at, stage_token\) > ROW\(/u;
    // The page plan: Limit <- Index Scan on cleanup_idx in index order (no Sort), the row comparison in the Index Cond,
    // the outbox probe a per-row SubPlan (an index probe); no hash, no seq scan of either table.
    const ordered = (plan: string) => {
      expect(plan).toMatch(/\bLimit\b/u);
      expect(plan).toContain("Index Scan using ingest_stage_intents_cleanup_idx on ingest_stage_intents s");
      expect(plan).toMatch(rowCond);
      expect(plan).toMatch(/SubPlan/u);
      expect(plan).toMatch(/SubPlan 1\n\s+->  Limit[^\n]*\n\s+->  Index Scan using ingest_buffer_outbox_\w+ on ingest_buffer_outbox o/u);
      expect(plan).not.toMatch(/Hash|Seq Scan|\bSort\b|Gather/u);
    };
    const generic = await pagePlan(PAGE, "plan-generic", "force_generic_plan");
    const custom = await pagePlan(PAGE, "plan-custom", "force_custom_plan");
    ordered(generic);
    ordered(custom);
    expect(generic.slice(generic.indexOf("\n", generic.indexOf("Query Text:")))).toMatch(/\$\d/u); // really generic
    expect(custom.slice(custom.indexOf("\n", custom.indexOf("Query Text:")))).not.toMatch(/\$\d/u); // really custom
    // Contrast, the 043870 body on the same data: its keyset is never an Index Cond.
    const oldGeneric = await pagePlan(OLD, "plan-old-generic", "force_generic_plan");
    const oldCustom = await pagePlan(OLD, "plan-old-custom", "force_custom_plan");
    if (process.env.PAGE_KEYSET_PLANS) console.log([generic, custom, oldGeneric, oldCustom].join("\n-----\n"));
    expect(oldGeneric).not.toMatch(rowCond);
    // ... and it reproduces the production shape: the EXISTS becomes a hash join estimated at ~1 row, so neither plan
    // walks the index in order to stop at the limit (a Sort over the join instead), in both plan modes.
    for (const old of [oldGeneric, oldCustom]) {
      expect(old).toMatch(/Hash Join[^\n]*rows=1 /u);
      expect(old).toMatch(/\bSort\b/u);
      expect(old).toMatch(/Seq Scan on ingest_buffer_outbox o/u);
    }
    // Restore the migrated setting (the ALTERs above are test-only).
    await db.exec(`ALTER FUNCTION ${PAGE} SET plan_cache_mode = force_custom_plan`);
  });

  test("rollback: 043870's exact page function and proconfig come back, the ledger row goes, a rerun refuses; re-migrate re-applies", async () => {
    await db.exec(MANIFEST_DOWN);
    await db.exec(DOWN);
    const live = (await db.query<{ s: string; c: string[]; d: boolean }>("SELECT prosrc s, proconfig c, prosecdef d FROM pg_proc WHERE oid=$1::regprocedure", [PAGE])).rows[0]!;
    expect(live).toEqual({ s: bodyOf(ORIGINAL).body, c: PRIOR_CONFIG, d: true });
    expect((await db.query<{ applied: boolean }>(MIGRATION_SENTINELS[STEP]!.sql)).rows[0]!.applied).toBe(false);
    expect((await db.query("SELECT 1 FROM sessions.__96bulls_migrations WHERE name=$1", [STEP])).rows).toEqual([]);
    const err = await db.exec(DOWN).then(() => null, (e: { code?: string }) => e.code);
    await db.exec("ROLLBACK").catch(() => {});
    expect(err).toBe("55000");
    await runMigrations(client(db), { profile: LOCAL });
    expect((await db.query<{ applied: boolean }>(MIGRATION_SENTINELS[STEP]!.sql)).rows[0]!.applied).toBe(true);
    expect((await db.query<{ s: string }>("SELECT prosrc s FROM pg_proc WHERE oid=$1::regprocedure", [PAGE])).rows[0]!.s).toBe(bodyOf(UP).body);
  });
});
