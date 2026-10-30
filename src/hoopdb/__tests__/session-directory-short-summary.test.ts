/**
 * G6: short_summary on sessions.session_directory. In-process PGlite only, with
 * every supabase/migrations file applied in name order after the Supabase role
 * shim; no hosted database is involved. Synthetic ids and text only.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { SCRUB_RULESET, SCRUB_RULESET_VERSION } from "../../shared/vendor.ts";
import {
  SESSION_DIRECTORY_QUERIES,
  SESSION_DIRECTORY_SEARCH_BOUNDARY,
  summaryContainsPattern,
} from "../search-index-contract.ts";
import {
  SHORT_SUMMARY_MAX_CHARS,
  ShortSummaryRejected,
  clearShortSummary,
  scrubShortSummary,
  writeShortSummary,
} from "../session-directory-summary.ts";

const root = join(import.meta.dir, "../../..");
const read = (path: string) => readFileSync(join(root, path), "utf8");
const MIGRATION = "20261030020000_session_directory_short_summary.sql";
const DOWN = read(`supabase/rollbacks/${MIGRATION.replace(/\.sql$/, ".down.sql")}`);
const UP = read(`supabase/migrations/${MIGRATION}`);

// Synthetic secret-shaped values, assembled at runtime so no literal sits on disk.
const FAKE_RELAY_TOKEN = ["relay", "ag", "x".repeat(24)].join("_");
const FAKE_HOME = ["", "Users", "alice", "proj"].join("/");

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

const KEY = { orgId: "org-a", workspaceId: "ws-a", harness: "codex", sessionId: "s-1" };
const AT = new Date("2026-10-30T12:00:00Z");

async function insertRow(db: PGlite, org = "org-a", workspace = "ws-a", session = "s-1") {
  await db.query(
    `INSERT INTO sessions.session_directory
       (org_id, workspace_id, harness, session_id, created_at, last_activity_at,
        title, agent_name, agent_name_normalized, project_id, search_text)
     VALUES ($1, $2, 'codex', $3, $4, $4, 'Fix flaky queue test', 'Helm', 'helm', 'proj-1',
             sessions.session_directory_search_text('Fix flaky queue test', NULL, 'Helm', 'proj-1',
               NULL, NULL, NULL, NULL))`,
    [org, workspace, session, AT.toISOString()],
  );
}

type Row = Record<string, unknown>;
async function row(db: PGlite, session = "s-1"): Promise<Row | undefined> {
  return (await db.query<Row>(
    "SELECT * FROM sessions.session_directory WHERE session_id = $1", [session],
  )).rows[0];
}

describe("short_summary writer", () => {
  test("stores only scrubbed text and records the scrub ruleset and version", async () => {
    const db = await database();
    await insertRow(db);
    const summary = `Rotated ${FAKE_RELAY_TOKEN} and edited ${FAKE_HOME}/queue.ts to fix retries.`;
    const result = await writeShortSummary(db, {
      ...KEY, summary, source: "bob-costas/extractive/v1", generatedAt: AT,
    });
    expect(result).toEqual({ written: true, scrubVersion: SCRUB_RULESET_VERSION });
    const stored = await row(db);
    expect(stored?.short_summary).toBe("Rotated [REDACTED] and edited ~/proj/queue.ts to fix retries.");
    expect(String(stored?.short_summary)).not.toContain(FAKE_RELAY_TOKEN);
    expect(String(stored?.search_text)).not.toContain(FAKE_RELAY_TOKEN);
    expect(stored?.short_summary_scrub_ruleset).toBe(SCRUB_RULESET);
    expect(stored?.short_summary_scrub_version).toBe(SCRUB_RULESET_VERSION);
    expect(stored?.short_summary_source).toBe("bob-costas/extractive/v1");
    expect((stored?.short_summary_generated_at as Date).toISOString()).toBe(AT.toISOString());
  });

  test("refuses over-length text in the writer and in the database", async () => {
    const db = await database();
    await insertRow(db);
    const exact = "a".repeat(SHORT_SUMMARY_MAX_CHARS);
    expect(scrubShortSummary(exact).text).toBe(exact);
    // Counted in code points, as PostgreSQL char_length does: 1000 emoji fit.
    expect(scrubShortSummary("\u{1F402}".repeat(SHORT_SUMMARY_MAX_CHARS)).text).toHaveLength(2000);

    const over = "a".repeat(SHORT_SUMMARY_MAX_CHARS + 1);
    expect(() => scrubShortSummary(over)).toThrow(new ShortSummaryRejected("over_length"));
    await expect(writeShortSummary(db, { ...KEY, summary: over, source: "bob-costas/v1", generatedAt: AT }))
      .rejects.toThrow("short_summary rejected: over_length");
    expect((await row(db))?.short_summary).toBeNull();

    // A writer that bypasses the TypeScript path still hits the CHECK.
    await expect(db.query(
      `UPDATE sessions.session_directory SET short_summary = $1, short_summary_scrub_ruleset = 'relayhistory-scrub',
         short_summary_scrub_version = 1, short_summary_source = 'x', short_summary_generated_at = now()`,
      [over],
    )).rejects.toThrow(/session_directory_short_summary_length/);
  });

  test("refuses when the scrub fails or is not stable, and writes nothing", async () => {
    const db = await database();
    await insertRow(db);
    const input = { ...KEY, summary: "a normal summary", source: "bob-costas/v1", generatedAt: AT };
    await expect(writeShortSummary(db, input, () => { throw new Error("boom"); }))
      .rejects.toThrow("short_summary rejected: scrub_failed");
    await expect(writeShortSummary(db, input, (value) => `${value}!`))
      .rejects.toThrow("short_summary rejected: scrub_unstable");
    await expect(writeShortSummary(db, input, () => undefined as unknown as string))
      .rejects.toThrow("short_summary rejected: scrub_failed");
    await expect(writeShortSummary(db, { ...input, summary: "   " }))
      .rejects.toThrow("short_summary rejected: empty");
    await expect(writeShortSummary(db, { ...input, source: "Not A Source" }))
      .rejects.toThrow("short_summary rejected: invalid_source");
    expect((await row(db))?.short_summary).toBeNull();
  });

  test("the database rejects summary text that lacks scrub provenance", async () => {
    const db = await database();
    await insertRow(db);
    await expect(db.query(
      "UPDATE sessions.session_directory SET short_summary = 'unscrubbed text'",
    )).rejects.toThrow(/session_directory_short_summary_provenance/);
    await expect(db.query(
      `UPDATE sessions.session_directory SET short_summary = 'x', short_summary_scrub_ruleset = 'something-else',
         short_summary_scrub_version = 1, short_summary_source = 'x', short_summary_generated_at = now()`,
    )).rejects.toThrow(/session_directory_short_summary_scrub_ruleset/);
    await expect(db.query(
      `UPDATE sessions.session_directory SET short_summary = 'x', short_summary_scrub_ruleset = 'relayhistory-scrub',
         short_summary_scrub_version = 0, short_summary_source = 'x', short_summary_generated_at = now()`,
    )).rejects.toThrow(/session_directory_short_summary_scrub_version/);
  });

  test("an older summary never replaces a newer one", async () => {
    const db = await database();
    await insertRow(db);
    await writeShortSummary(db, { ...KEY, summary: "newer", source: "bob-costas/v1", generatedAt: AT });
    const older = await writeShortSummary(db, {
      ...KEY, summary: "older", source: "bob-costas/v1", generatedAt: new Date(AT.getTime() - 1000),
    });
    expect(older.written).toBe(false);
    expect((await row(db))?.short_summary).toBe("newer");
  });
});

describe("short_summary in the search document", () => {
  test("is part of the declared boundary and of search_text", async () => {
    expect(SESSION_DIRECTORY_SEARCH_BOUNDARY.fields).toContain("short_summary");
    const db = await database();
    await insertRow(db);
    await writeShortSummary(db, {
      ...KEY, summary: "Migrated the zebra-crossing scheduler", source: "bob-costas/v1", generatedAt: AT,
    });
    expect((await row(db))?.search_text).toBe("Fix flaky queue test Helm proj-1 Migrated the zebra-crossing scheduler");
    const hits = await db.query(SESSION_DIRECTORY_QUERIES.metadataContains, ["org-a", "ws-a", "zebra-crossing", 50]);
    expect(hits.rows).toHaveLength(1);
    const summaryHits = await db.query(SESSION_DIRECTORY_QUERIES.summaryContains, [
      "org-a", "ws-a", summaryContainsPattern("ZEBRA-cross"), 50,
    ]);
    expect(summaryHits.rows).toHaveLength(1);

    expect(await clearShortSummary(db, KEY)).toBe(true);
    expect((await row(db))?.search_text).toBe("Fix flaky queue test Helm proj-1");
  });

  test("summary search escapes LIKE metacharacters and needs three characters", () => {
    expect(summaryContainsPattern(" 50%_off ")).toBe("%50\\%\\_off%");
    expect(() => summaryContainsPattern("ab")).toThrow(RangeError);
  });
});

type Plan = { [key: string]: unknown; Plans?: Plan[]; "Node Type"?: string; "Index Name"?: string };
const walk = (plan: Plan): Plan[] => [plan, ...(plan.Plans ?? []).flatMap(walk)];

async function explain(db: PGlite, sql: string, params: unknown[]): Promise<Plan[]> {
  const result = await db.query<Record<string, unknown>>(`EXPLAIN (ANALYZE, FORMAT JSON) ${sql}`, params);
  const envelope = result.rows[0]?.["QUERY PLAN"] as Array<{ Plan: Plan }> | undefined;
  if (!envelope?.[0]?.Plan) throw new Error("PostgreSQL returned no JSON plan");
  return walk(envelope[0].Plan);
}

describe("short_summary query plan", () => {
  test("tenant-scoped summary search uses the trigram index, never a seq scan", async () => {
    const db = await database();
    // 30,000 rows: a large tenant (org-0/ws-0, 22,500 rows) beside 7,500 rows of other
    // tenants, so the tenant's own primary-key range is no longer a cheap scan.
    // 1 in 1,000 summaries holds the needle, in every tenant.
    await db.exec(`
      INSERT INTO sessions.session_directory
        (org_id, workspace_id, harness, session_id, created_at, last_activity_at,
         short_summary, short_summary_scrub_ruleset, short_summary_scrub_version,
         short_summary_source, short_summary_generated_at, search_text)
      SELECT CASE WHEN n % 4 = 0 THEN 'org-' || (n % 7 + 1) ELSE 'org-0' END,
             CASE WHEN n % 4 = 0 THEN 'ws-' || (n % 7 + 1) ELSE 'ws-0' END, 'codex', 's-' || n,
             '2026-10-01T00:00:00Z'::timestamptz + n * interval '1 second',
             '2026-10-01T00:00:00Z'::timestamptz + n * interval '1 second',
             CASE WHEN n % 1000 = 0 OR n % 1000 = 1 THEN 'Rebuilt the quokka ledger reconciler, session ' || n
                  ELSE 'Refactored module ' || (n % 997) || ' and updated tests for case ' || n END,
             'relayhistory-scrub', 1, 'bob-costas/v1', now(), 'metadata ' || n
        FROM generate_series(1, 30000) AS n;
      ANALYZE sessions.session_directory;
    `);
    const nodes = await explain(db, SESSION_DIRECTORY_QUERIES.summaryContains, [
      "org-0", "ws-0", summaryContainsPattern("quokka ledger"), 50,
    ]);
    expect(nodes.some((node) => node["Index Name"] === "session_directory_summary_trgm_idx")).toBe(true);
    expect(nodes.some((node) => node["Node Type"] === "Seq Scan")).toBe(false);
    const hits = await db.query<{ org_id: string; workspace_id: string }>(SESSION_DIRECTORY_QUERIES.summaryContains, [
      "org-0", "ws-0", summaryContainsPattern("quokka ledger"), 50,
    ]);
    expect(hits.rows).toHaveLength(30);
    const everywhere = await db.query("SELECT 1 FROM sessions.session_directory WHERE short_summary LIKE '%quokka ledger%'");
    expect(everywhere.rows).toHaveLength(60);
    expect(hits.rows.every((hit) => hit.org_id === "org-0" && hit.workspace_id === "ws-0")).toBe(true);
  });
});

describe("short_summary tenancy", () => {
  test("RLS hides other tenants' summaries from authenticated readers", async () => {
    const db = await database();
    await db.exec("SET ROLE service_role");
    await insertRow(db, "org-a", "ws-a", "s-a");
    await insertRow(db, "org-b", "ws-b", "s-b");
    await insertRow(db, "org-a", "ws-hidden", "s-hidden");
    for (const [org, workspace, session] of [["org-a", "ws-a", "s-a"], ["org-b", "ws-b", "s-b"], ["org-a", "ws-hidden", "s-hidden"]]) {
      await writeShortSummary(db, {
        orgId: org!, workspaceId: workspace!, harness: "codex", sessionId: session!,
        summary: `walrus summary for ${session}`, source: "bob-costas/v1", generatedAt: AT,
      });
    }
    await db.exec(`RESET ROLE; SET ROLE authenticated;
      SELECT set_config('request.jwt.claims', '{"app_metadata":{"org_id":"org-a","workspace_ids":["ws-a"]}}', false);`);
    try {
      const all = await db.query<{ session_id: string; short_summary: string }>(
        "SELECT session_id, short_summary FROM sessions.session_directory ORDER BY session_id",
      );
      expect(all.rows).toEqual([{ session_id: "s-a", short_summary: "walrus summary for s-a" }]);
      // Asking for another tenant explicitly returns nothing.
      for (const [org, workspace] of [["org-b", "ws-b"], ["org-a", "ws-hidden"]]) {
        const other = await db.query(SESSION_DIRECTORY_QUERIES.summaryContains, [
          org, workspace, summaryContainsPattern("walrus"), 50,
        ]);
        expect(other.rows).toEqual([]);
      }
      // And the authenticated role cannot write a summary at all.
      await expect(db.query("UPDATE sessions.session_directory SET short_summary = NULL")).rejects.toThrow();
    } finally {
      await db.exec("RESET ROLE");
    }
  });
});

describe("short_summary deletion propagation", () => {
  test("a tombstone clears the summary, its provenance and the search document", async () => {
    const db = await database();
    await insertRow(db);
    await writeShortSummary(db, { ...KEY, summary: "heron migration notes", source: "bob-costas/v1", generatedAt: AT });
    await db.query("UPDATE sessions.session_directory SET deleted_at = now() WHERE session_id = 's-1'");
    const tombstone = await row(db);
    expect(tombstone?.deleted_at).not.toBeNull();
    for (const column of [
      "short_summary", "short_summary_scrub_ruleset", "short_summary_scrub_version",
      "short_summary_source", "short_summary_generated_at",
    ]) expect(tombstone?.[column]).toBeNull();
    expect(tombstone?.search_text).toBe("");

    // The writer never puts a summary back on a tombstone.
    const rewrite = await writeShortSummary(db, {
      ...KEY, summary: "heron again", source: "bob-costas/v1", generatedAt: new Date(AT.getTime() + 1000),
    });
    expect(rewrite.written).toBe(false);
    // Nor does a direct insert of a tombstone that carries one.
    await db.query(
      `INSERT INTO sessions.session_directory
         (org_id, workspace_id, harness, session_id, created_at, last_activity_at, deleted_at,
          short_summary, short_summary_scrub_ruleset, short_summary_scrub_version,
          short_summary_source, short_summary_generated_at, search_text)
       VALUES ('org-a', 'ws-a', 'codex', 's-2', now(), now(), now(),
               'heron', 'relayhistory-scrub', 1, 'bob-costas/v1', now(), 'heron')`,
    );
    expect((await row(db, "s-2"))?.short_summary).toBeNull();
    const leaks = await db.query(
      "SELECT 1 FROM sessions.session_directory WHERE short_summary ILIKE '%heron%' OR search_text ILIKE '%heron%'",
    );
    expect(leaks.rows).toEqual([]);
  });

  test("erasure deletes the row, the summary and its memberships", async () => {
    const db = await database();
    await insertRow(db, "org-a", "ws-a", "s-1");
    await insertRow(db, "org-a", "ws-a", "s-2");
    await insertRow(db, "org-a", "ws-keep", "s-3");
    for (const [workspace, session] of [["ws-a", "s-1"], ["ws-a", "s-2"], ["ws-keep", "s-3"]]) {
      await writeShortSummary(db, {
        orgId: "org-a", workspaceId: workspace!, harness: "codex", sessionId: session!,
        summary: `ibis summary ${session}`, source: "bob-costas/v1", generatedAt: AT,
      });
    }
    await db.exec(`INSERT INTO sessions.session_directory_users VALUES ('org-a', 'ws-a', 'codex', 's-1', 'u1', now())`);
    // Session erasure, then workspace erasure.
    await db.query("DELETE FROM sessions.session_directory WHERE org_id = 'org-a' AND workspace_id = 'ws-a' AND harness = 'codex' AND session_id = 's-1'");
    expect(await row(db, "s-1")).toBeUndefined();
    expect((await db.query("SELECT 1 FROM sessions.session_directory_users")).rows).toEqual([]);
    await db.query("DELETE FROM sessions.session_directory WHERE org_id = 'org-a' AND workspace_id = 'ws-a'");
    const left = await db.query<{ session_id: string }>(
      "SELECT session_id FROM sessions.session_directory WHERE short_summary ILIKE '%ibis%' OR search_text ILIKE '%ibis%'",
    );
    expect(left.rows).toEqual([{ session_id: "s-3" }]);
  });
});

describe("short_summary rollback", () => {
  test("down removes every summary column and copy, and up applies again", async () => {
    const db = await database();
    await insertRow(db);
    await writeShortSummary(db, { ...KEY, summary: "pelican rollback check", source: "bob-costas/v1", generatedAt: AT });
    await db.exec(DOWN);
    const columns = await db.query<{ column_name: string }>(
      `SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'sessions' AND table_name = 'session_directory' AND column_name LIKE 'short_summary%'`,
    );
    expect(columns.rows).toEqual([]);
    const objects = await db.query(
      `SELECT 1 FROM pg_class WHERE relname = 'session_directory_summary_trgm_idx'
       UNION ALL SELECT 1 FROM pg_proc WHERE proname IN
         ('session_directory_search_text', 'session_directory_tombstone_clears_summary')
       UNION ALL SELECT 1 FROM pg_trigger WHERE tgname = 'session_directory_tombstone_clears_summary'`,
    );
    expect(objects.rows).toEqual([]);
    expect((await row(db))?.search_text).toBe("Fix flaky queue test Helm proj-1");

    await db.exec(UP);
    await writeShortSummary(db, { ...KEY, summary: "pelican again", source: "bob-costas/v1", generatedAt: AT });
    expect((await row(db))?.short_summary).toBe("pelican again");
  });
});
