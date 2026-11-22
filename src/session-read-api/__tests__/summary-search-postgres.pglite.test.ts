/**
 * GET /v1/sessions/summaries/search with summarySearchBackend "postgres" against PGlite with the full
 * migration plan, through the real directory query (sessions.query_session_directory_for) as
 * bulls96_read_api: a title match and a short-summary match are served; a row whose search_text matches
 * only on another field, an erased (tombstoned) session, a stale search_text row whose title no longer
 * matches, and another workspace's row are not; the keyset walks past a full page. No migration of its own.
 */
import { afterAll, beforeAll, describe, expect, setDefaultTimeout, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { PGlite } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/index.js";
import { btree_gin } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/contrib/btree_gin.js";
import { pg_trgm } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/contrib/pg_trgm.js";
import { runMigrations, type MigrationClient, type MigrationProfile } from "../../hoopdb/migrate.ts";
import { createReadApi } from "../app.ts";
import { DIRECTORY_LIST_MAX_LIMIT, sqlDirectory } from "../directory.ts";
import { poolTenantRunner } from "../liveness.ts";
import { memoryLiveness } from "./support.ts";

setDefaultTimeout(300_000);
const root = join(import.meta.dir, "../../..");
const sqlFile = (path: string) => readFileSync(join(root, path), "utf8").split("\n").filter((l) => !l.startsWith("\\")).join("\n");
const LOCAL: MigrationProfile = { mode: "local", shim: true, bootstrap: true, verify: false, runtimeRole: "postgres" };

function client(db: PGlite): MigrationClient {
  return { async query<R>(text: string, params?: unknown[]) {
    if (params?.length) return (await db.query<R>(text, params)) as { rows: R[] };
    const results = await db.exec(text);
    return { rows: (results.at(-1)?.rows ?? []) as R[] };
  } };
}

type Seed = { id: string; minute: number; title?: string; branch?: string; summary?: string; workspace?: string; searchText?: string; harness?: string };
async function seed(db: PGlite, rows: Seed[]) {
  for (const s of rows) {
    const at = new Date(Date.UTC(2026, 9, 20) + s.minute * 60_000).toISOString();
    await db.query(
      `INSERT INTO sessions.session_directory
         (org_id, workspace_id, harness, session_id, created_at, last_activity_at, title, git_branch, search_text,
          short_summary, short_summary_scrub_ruleset, short_summary_scrub_version, short_summary_source, short_summary_generated_at,
          catalog_projected_at)
       VALUES ('org-a', $1, $2, $3, $4, $4, $5::text, $6::text,
               coalesce($8::text, sessions.session_directory_search_text($5::text, NULL, NULL, NULL, NULL, $6::text, NULL, $7::text)),
               $7::text, CASE WHEN $7::text IS NOT NULL THEN 'relayhistory-scrub' END, CASE WHEN $7::text IS NOT NULL THEN 1 END,
               CASE WHEN $7::text IS NOT NULL THEN 'test/v1' END, CASE WHEN $7::text IS NOT NULL THEN $4::timestamptz END, now())`,
      [s.workspace ?? "ws-a", s.harness ?? "codex", s.id, at, s.title ?? null, s.branch ?? null, s.summary ?? null, s.searchText ?? null],
    );
  }
}

describe("summary search, Postgres backend, real directory query", () => {
  let db: PGlite;
  let get: (q: string, extra?: string) => Promise<{ status: number; body: Record<string, any> }>;
  const logs: Array<Record<string, unknown>> = [];

  beforeAll(async () => {
    db = new PGlite({ extensions: { btree_gin, pg_trgm } });
    await runMigrations(client(db), { profile: LOCAL });
    await db.exec(sqlFile("supabase/bootstrap/session_read_api_role.sql"));
    await db.exec("UPDATE sessions.session_directory_catalog_rollout SET completed_at = now()");
    const pool = {
      async connect() {
        await db.exec("SET ROLE bulls96_read_api");
        return {
          query: (text: string, p?: unknown[]) => db.query(text, p as unknown[]) as Promise<{ rows: unknown[] }>,
          release: () => void db.exec("RESET ROLE"),
        };
      },
    };
    const app = createReadApi({
      verify: async () => ({ userId: "u-1", orgId: "org-a", workspaceId: "ws-a", scopes: ["rth:read"] }),
      adapter: {} as never,
      liveness: memoryLiveness().liveness,
      completeSinceMs: 0,
      log: (entry) => { logs.push(entry as Record<string, unknown>); },
      directory: sqlDirectory(poolTenantRunner(pool)),
      directoryWorkspaces: new Set(["ws-a"]),
      summarySearch: true,
      summarySearchBackend: "postgres",
    });
    get = async (q: string, extra = "") => {
      const r = await app.request(`/v1/sessions/summaries/search?workspace=ws-a&q=${encodeURIComponent(q)}${extra}`, { headers: { authorization: "Bearer t" } });
      return { status: r.status, body: (await r.json()) as Record<string, any> };
    };
  });
  afterAll(async () => { await db?.close(); });

  const served = () => logs.filter((l) => l.reason === "summary_search_served").at(-1);
  const ids = (body: Record<string, any>) => body.sessions.map((s: { sessionId: string }) => s.sessionId);

  test("title and short-summary matches are served; other search_text fields, erased, stale and foreign rows are not", async () => {
    await seed(db, [
      { id: "s-title", minute: 50, title: "Erasure follow-up" },
      { id: "s-summary", minute: 40, title: "Other work", summary: "Fixed the ERASURE race in the reader" },
      { id: "s-branch", minute: 30, title: "Other work", branch: "fix/erasure-race" },
      { id: "s-erased", minute: 20, title: "Erasure was here", summary: "erasure summary" },
      // The title was renamed but search_text still holds the old one: the SQL term matches, the re-check does not.
      { id: "s-stale", minute: 10, title: "Renamed session", searchText: "Old erasure title" },
      { id: "s-foreign", minute: 60, title: "Erasure elsewhere", workspace: "ws-b" },
    ]);
    // Erase: the tombstone trigger blanks summary and search_text; the directory query serves live rows only.
    await db.exec("UPDATE sessions.session_directory SET deleted_at = now() WHERE session_id = 's-erased'");
    // The SQL candidates are a superset (the stale and branch rows), proving the re-check is what drops them.
    const sqlRows = await db.query<{ session_id: string }>(
      "SELECT session_id FROM sessions.session_directory WHERE workspace_id = 'ws-a' AND deleted_at IS NULL AND search_text ILIKE '%erasure%' ORDER BY 1");
    expect(sqlRows.rows.map((r) => r.session_id)).toEqual(["s-branch", "s-stale", "s-summary", "s-title"]);

    const r = await get("erasure");
    expect(r.status).toBe(200);
    expect(Object.keys(r.body).sort()).toEqual(["correlationId", "sessions", "truncated"]);
    expect(ids(r.body)).toEqual(["s-title", "s-summary"]);
    expect(r.body.truncated).toBe(false);
    expect(r.body.sessions[0]).toMatchObject({ source: "codex", sessionId: "s-title", title: "Erasure follow-up" });
    expect(r.body.sessions[1]).toMatchObject({ sessionId: "s-summary", shortSummary: "Fixed the ERASURE race in the reader" });
    expect(served()).toMatchObject({ reason: "summary_search_served", kind: "postgres", listed: 4, count: 2, status: 200 });
    // Case folds both ways; limit 1 is truncated.
    const one = await get("ERASURE", "&limit=1");
    expect([ids(one.body), one.body.truncated]).toEqual([["s-title"], true]);
    // A multi-word q is one phrase (word order and adjacency matter); whitespace runs normalise to one space.
    expect(ids((await get("the  ERASURE race")).body)).toEqual(["s-summary"]);
    expect(ids((await get("race erasure")).body)).toEqual([]);
    // LIKE wildcards in q are literal.
    expect(ids((await get("eras%re")).body)).toEqual([]);
    expect(ids((await get("_rasure")).body)).toEqual([]);
  });

  test("the keyset walks past a full directory page with the real cursor", async () => {
    await db.exec("DELETE FROM sessions.session_directory");
    const filler = Array.from({ length: DIRECTORY_LIST_MAX_LIMIT + 5 }, (_, i) => ({ id: `f${String(i).padStart(3, "0")}`, minute: 1000 - i, title: "Other", branch: "walk-gamma" }));
    await seed(db, [...filler, { id: "s-old", minute: 1, title: "Walk-gamma notes" }]);
    const r = await get("walk-gamma");
    expect(r.status).toBe(200);
    expect([ids(r.body), r.body.truncated]).toEqual([["s-old"], false]);
    expect(served()).toMatchObject({ listed: DIRECTORY_LIST_MAX_LIMIT + 6, count: 1 });
  });
});
