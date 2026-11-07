/**
 * Opt-in OWNED PostgreSQL 17 rehearsal of the F2 brief-title backfill (no cloud, no real
 * credential): the full registered migration chain through 20261105043000 on one fixture
 * database, a fixture "Neon" (the relayhistory session_analysis_requests/jobs columns Cloud
 * reads) on another, and the real CLI (src/migration/neon-brief-titles/main.ts) as a child
 * process: dry run (nothing kept), execute (guarded), verify, and an idempotent rerun.
 * Parity oracle: Cloud's exact Neon SQL (packages/web/app/dashboard/sessions/
 * session-brief-titles.ts at cloud trunk f877e58e1), compared per scope with what
 * sessions.session_brief_titles_for serves AS bulls96_read_api.
 *
 *   BRIEF_TITLES_TEST_PG_URL=postgresql://postgres@127.0.0.1:<port>/postgres bun test <this file>
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import pg from "pg";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { runMigrations, type MigrationProfile } from "../../hoopdb/migrate.ts";
import { derivedSchemaAssets } from "../../session-pipeline/derived-schema.ts";
import { schemaVerdict, type Transact } from "../../shared/db.ts";
import { TENANT_SETTINGS_SQL } from "../../session-read-api/liveness.ts";

const url = process.env.BRIEF_TITLES_TEST_PG_URL;
if (url) {
  const u = new URL(url);
  if (!["postgresql:", "postgres:"].includes(u.protocol) || !["127.0.0.1", "localhost"].includes(u.hostname) || u.username !== "postgres" ||
      u.password || u.pathname !== "/postgres" || u.search || u.hash) throw Error("owned_loopback_fixture_required");
}
const root = join(import.meta.dir, "../../..");
const text = (path: string) => readFileSync(join(root, path), "utf8").split("\n").filter((l) => !l.startsWith("\\")).join("\n");
const profile: MigrationProfile = { mode: "local", shim: true, bootstrap: true, verify: true, runtimeRole: "postgres" };

/** Cloud trunk f877e58e1's query, verbatim apart from the parameter style. */
const CLOUD_SQL = `SELECT wanted.session_id, wanted.source, cached.title
    FROM jsonb_to_recordset($1::jsonb)
      AS wanted(session_id text, source text)
    CROSS JOIN LATERAL (
      SELECT job.result->>'title' AS title
      FROM sessions.session_analysis_requests request
      JOIN sessions.session_analysis_jobs job ON job.id = request.brief_job_id
        AND job.org_id = request.org_id AND job.session_id = request.session_id
      WHERE request.org_id = $2 AND request.session_id = wanted.session_id
        AND request.status = 'resolved' AND job.status = 'completed' AND job.kind = 'brief'
        AND job.workspace_id = $3
        AND job.source_provider = wanted.source
        AND job.source->'evidenceWorkspaceIds' = $4::jsonb
      ORDER BY request.created_at DESC, request.id DESC LIMIT 1
    ) cached`;

/** The relayhistory columns the selection reads (0015/0016/0024), nothing else. */
const NEON_DDL = `CREATE SCHEMA sessions;
CREATE TABLE sessions.session_analysis_jobs (
  id uuid PRIMARY KEY, org_id text NOT NULL, workspace_id text, session_id text NOT NULL,
  source_provider text NOT NULL, summary_version text NOT NULL, summary_model_version text NOT NULL,
  source jsonb NOT NULL, status text NOT NULL, kind text NOT NULL, result jsonb,
  completed_at timestamptz);
CREATE TABLE sessions.session_analysis_requests (
  id uuid PRIMARY KEY, org_id text NOT NULL, workspace_id text NOT NULL, session_id text NOT NULL,
  status text NOT NULL, brief_job_id uuid, created_at timestamptz NOT NULL);`;

type Job = { org: string; ws: string; src: string; session: string; title: unknown; status?: string; reqStatus?: string; evidence?: string[]; at: string; kind?: string; state?: string | null; prompt?: string };
const JOBS: Job[] = [
  { org: "org-a", ws: "ws-a", src: "claude", session: "s-1", title: "  Older title ", at: "2026-10-01T00:00:00Z" },
  { org: "org-a", ws: "ws-a", src: "claude", session: "s-1", title: " Latest title ", at: "2026-10-02T00:00:00Z" },
  { org: "org-a", ws: "ws-a", src: "claude", session: "s-1", title: "Pending newer", status: "running", at: "2026-10-03T00:00:00Z" },
  { org: "org-a", ws: "ws-a", src: "claude", session: "s-1", title: "Failed request", reqStatus: "failed", at: "2026-10-04T00:00:00Z" },
  { org: "org-a", ws: "ws-a", src: "codex", session: "s-1", title: "Codex title", at: "2026-10-02T00:00:00Z" },
  { org: "org-a", ws: "ws-a", src: "claude", session: "s-2", title: "Mixed evidence", evidence: ["ws-a", "ws-b"], at: "2026-10-02T00:00:00Z" },
  { org: "org-a", ws: "ws-a", src: "claude", session: "s-3", title: "Analysis kind", kind: "analysis", at: "2026-10-02T00:00:00Z" },
  { org: "org-a", ws: "ws-a", src: "claude", session: "s-4", title: "Legacy without state", state: null, prompt: "session-summary-v1", at: "2026-10-02T00:00:00Z" },
  { org: "org-a", ws: "ws-a", src: "claude", session: "s-5", title: "   ", at: "2026-10-02T00:00:00Z" },
  { org: "org-a", ws: "ws-a", src: "claude", session: "s-6", title: "No pointer in SP", at: "2026-10-02T00:00:00Z" },
  { org: "org-a", ws: "ws-a", src: "claude", session: "s-7", title: "Native wins", at: "2026-10-02T00:00:00Z" },
  { org: "org-a", ws: "ws-b", src: "claude", session: "s-1", title: "Other workspace", evidence: ["ws-b"], at: "2026-10-02T00:00:00Z" },
  { org: "org-b", ws: "ws-a", src: "claude", session: "s-1", title: "Other org", at: "2026-10-02T00:00:00Z" },
];
/** SP sessions with a live pointer (s-6 has none). */
const POINTERS: Array<[string, string, string, string]> = [
  ["org-a", "ws-a", "claude", "s-1"], ["org-a", "ws-a", "codex", "s-1"], ["org-a", "ws-a", "claude", "s-2"],
  ["org-a", "ws-a", "claude", "s-3"], ["org-a", "ws-a", "claude", "s-4"], ["org-a", "ws-a", "claude", "s-5"],
  ["org-a", "ws-a", "claude", "s-7"], ["org-a", "ws-b", "claude", "s-1"], ["org-b", "ws-a", "claude", "s-1"],
];

describe.skipIf(!url)("owned PG17: F2 brief-title backfill rehearsal", () => {
  let admin: pg.Client, target: pg.Pool, neon: pg.Pool, targetUrl: string, neonUrl: string;
  const names: string[] = [];
  const transact: Transact = async (work) => {
    const c = await target.connect();
    try { await c.query("BEGIN"); const r = await work(c as never); await c.query("COMMIT"); return r; }
    catch (e) { await c.query("ROLLBACK").catch(() => {}); throw e; } finally { c.release(); }
  };
  const cli = async (args: string[]) => {
    const p = Bun.spawn(["bun", "src/migration/neon-brief-titles/main.ts", ...args], {
      cwd: root, stdout: "pipe", stderr: "pipe",
      env: { ...process.env, PATH: `${process.execPath.replace(/\/bun$/, "")}:${process.env.PATH}`, BULLS_TARGET_URL: targetUrl, NEON_BRIEF_TITLES_SOURCE_URL: neonUrl },
    });
    const [out, err, exit] = await Promise.all([new Response(p.stdout).text(), new Response(p.stderr).text(), p.exited]);
    return { out: out.trim() ? JSON.parse(out) : null, err, exit };
  };
  const servedTitles = async (org: string, ws: string, keys: Array<[string, string]>) => {
    const c = await target.connect();
    try {
      await c.query("BEGIN READ ONLY");
      await c.query("SET LOCAL ROLE bulls96_read_api");
      await c.query(TENANT_SETTINGS_SQL, [org, ws]);
      const r = await c.query<{ harness: string; session_id: string; title: string }>(
        "SELECT harness, session_id, title FROM sessions.session_brief_titles_for($1, $2::text[], $3::text[])",
        [ws, keys.map((k) => k[0]), keys.map((k) => k[1])]);
      await c.query("COMMIT");
      return new Map(r.rows.map((x) => [JSON.stringify([x.harness, x.session_id]), x.title]));
    } finally { await c.query("ROLLBACK").catch(() => {}); c.release(); }
  };

  beforeAll(async () => {
    admin = new pg.Client({ connectionString: url }); await admin.connect();
    const version = Number((await admin.query("SHOW server_version_num")).rows[0].server_version_num);
    expect(version).toBeGreaterThanOrEqual(170000); expect(version).toBeLessThan(180000);
    const suffix = crypto.randomUUID().replaceAll("-", "");
    for (const n of [`f2_target_${suffix}`, `f2_neon_${suffix}`]) { await admin.query(`CREATE DATABASE ${n}`); names.push(n); }
    // Different host spellings, so the write guard's "same host" refusal is not what is tested.
    const t = new URL(url!); t.hostname = "127.0.0.1"; t.pathname = `/${names[0]}`; targetUrl = t.href;
    const s = new URL(url!); s.hostname = "localhost"; s.pathname = `/${names[1]}`; neonUrl = s.href;
    target = new pg.Pool({ connectionString: targetUrl, max: 4 });
    neon = new pg.Pool({ connectionString: neonUrl, max: 2 });
    await target.query("CREATE EXTENSION pg_trgm; CREATE EXTENSION btree_gin");
    const c = await target.connect();
    try { await runMigrations(c as never, { profile }); } finally { c.release(); }
    await target.query(text("supabase/bootstrap/session_read_api_role.sql"));
    await target.query(text("supabase/bootstrap/session_read_api_role_verify.sql"));
    const assets = await derivedSchemaAssets();
    expect(assets.expected.names).toContain("upstream:20261105043000_brief_titles.sql");
    expect(await schemaVerdict({ ...assets.expected, dbMode: "local" }, transact)).toEqual({ ok: true });
    for (const [org, ws, harness, session] of POINTERS)
      await target.query(
        `INSERT INTO sessions.session_index
           (org_id, workspace_id, harness, session_id, machine_id, user_id, created_at, last_activity_at,
            storage_provider, bucket, manifest_key, content_sha256, raw_format, trace_id, revision, title)
         VALUES ($1, $2, $3, $4, 'm-1', 'u-1', now(), now(), 'fs', 'bucket', $5,
                 decode(repeat('ab', 32), 'hex'), 'jsonl', '019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18', 1, 'pointer title')`,
        [org, ws, harness, session, `manifest/${org}/${ws}/${harness}/${session}`]);
    // s-7 already has a native brief: the backfill must not replace it.
    await target.query(
      `INSERT INTO sessions.session_briefs (org_id, workspace_id, harness, session_id, status, revision, source_revisions,
         title, description, state, brief_revision, brief_source_revisions, brief_fingerprint, scrub_ruleset, scrub_version,
         prompt_version, engine_version, model, generated_at)
       VALUES ('org-a','ws-a','claude','s-7','ready',1,'{"m-1":1}','Native title','Native description.','completed',1,'{"m-1":1}',
               repeat('e',64),'relayhistory-scrub',1,'session-summary-v3','session-brief-v3','gpt-5.6-luna',now())`);
    await neon.query(NEON_DDL);
    for (const [i, j] of JOBS.entries()) {
      const id = `00000000-0000-4000-8000-${String(i).padStart(12, "0")}`;
      await neon.query(
        `INSERT INTO sessions.session_analysis_jobs VALUES ($1,$2,$3,$4,$5,'session-summary-v3','gpt-5.6-luna',$6,$7,$8,$9,$10)`,
        [id, j.org, j.ws, j.session, j.src, JSON.stringify({ evidenceWorkspaceIds: j.evidence ?? [j.ws] }), j.status ?? "completed", j.kind ?? "brief",
         JSON.stringify({ title: j.title, description: "A description.", ...(j.state === null ? {} : { state: j.state ?? "completed" }), promptVersion: j.prompt ?? "session-summary-v3", generatedAt: j.at, backend: { model: "gpt-5.6-luna", inputTokens: 10, outputTokens: 2 } }),
         j.at]);
      await neon.query("INSERT INTO sessions.session_analysis_requests VALUES ($1,$2,$3,$4,$5,$1,$6)", [id, j.org, j.ws, j.session, j.reqStatus ?? "resolved", j.at]);
    }
  }, 180000);

  afterAll(async () => {
    await target?.end(); await neon?.end();
    if (admin) { for (const n of names) await admin.query(`DROP DATABASE IF EXISTS ${n} WITH (FORCE)`); await admin.end(); }
  }, 30000);

  test("dry run, guarded execute, exact parity with Cloud's Neon SQL, idempotent rerun", async () => {
    const dry = await cli([]);
    expect(dry.exit).toBe(0);
    expect(dry.out.backfill).toMatchObject({
      mode: "dry-run", candidates: 8, skipped: { title_blank: 1 },
      outcomes: { inserted: 5, updated: 0, skipped_native: 1, skipped_quarantined: 0, skipped_no_pointer: 1 },
    });
    expect((await target.query("SELECT count(*)::int AS n FROM sessions.session_briefs")).rows[0].n).toBe(1);
    // Nothing printed is content.
    for (const j of JOBS) if (typeof j.title === "string" && j.title.trim()) expect(dry.out ? JSON.stringify(dry.out) : "").not.toContain(j.title.trim());

    expect((await cli(["--execute"])).exit).toBe(1);
    expect((await cli(["--execute", "--expect-target-host=db.example.com"])).exit).toBe(1);
    const exec = await cli(["--execute", "--expect-target-host=127.0.0.1"]);
    expect(exec.exit).toBe(0);
    expect(exec.out.backfill.outcomes).toEqual(dry.out.backfill.outcomes);

    // Parity: for every scope, the title Cloud's Neon SQL shows equals the one SP serves.
    const scopes = new Map<string, Array<[string, string]>>();
    for (const j of JOBS) {
      const k = JSON.stringify([j.org, j.ws]);
      const list = scopes.get(k) ?? [];
      if (!list.some((x) => x[0] === j.src && x[1] === j.session)) list.push([j.src, j.session]);
      scopes.set(k, list);
    }
    let compared = 0;
    for (const [k, keys] of scopes) {
      const [org, ws] = JSON.parse(k) as [string, string];
      const neonRows = (await neon.query<{ session_id: string; source: string; title: string }>(CLOUD_SQL,
        [JSON.stringify(keys.map(([source, session_id]) => ({ session_id, source }))), org, ws, JSON.stringify([ws])])).rows;
      const cloud = new Map(neonRows.flatMap((r) => (typeof r.title === "string" && r.title.trim() ? [[JSON.stringify([r.source, r.session_id]), r.title.trim()] as const] : [])));
      const served = await servedTitles(org, ws, keys);
      for (const key of keys) {
        const id = JSON.stringify(key);
        if (org === "org-a" && ws === "ws-a" && key[1] === "s-7") { expect(served.get(id)).toBe("Native title"); continue; }
        if (key[1] === "s-6") { expect(served.has(id)).toBe(false); continue; }
        expect(served.get(id)).toBe(cloud.get(id));
        compared += 1;
      }
    }
    expect(compared).toBe(8);
    expect((await servedTitles("org-a", "ws-a", [["claude", "s-1"]])).get(JSON.stringify(["claude", "s-1"]))).toBe("Latest title");

    const verify = await cli(["--verify"]);
    expect(verify.exit).toBe(0);
    expect(verify.out.verify).toMatchObject({ candidates: 8, compared: 7, equal: 5, native: 1, missing: 1, different: 0 });
    expect(verify.out.verify.samples.missing).toEqual([["org-a", "ws-a", "claude", "s-6"]]);
    expect((await cli(["--verify", "--execute"])).exit).toBe(1);

    const again = await cli(["--execute", "--expect-target-host=127.0.0.1"]);
    expect(again.out.backfill.outcomes).toMatchObject({ inserted: 0, updated: 5, skipped_native: 1 });
    // The read role verifier still passes with the backfilled rows and the new function.
    await target.query(text("supabase/bootstrap/session_read_api_role_verify.sql"));
    const rows = (await target.query("SELECT origin, count(*)::int AS n FROM sessions.session_briefs GROUP BY 1 ORDER BY 1")).rows;
    expect(rows).toEqual([{ origin: "native", n: 1 }, { origin: "neon_backfill", n: 5 }]);
  }, 120000);

  test("guarded down refuses while backfilled rows remain; after deleting them, down and up are clean", async () => {
    const down = text("supabase/rollbacks/20261105043000_brief_titles.down.sql");
    const c = await target.connect();
    try {
      await c.query("BEGIN");
      await expect(c.query(down)).rejects.toMatchObject({ code: "55000" });
      await c.query("ROLLBACK");
      await c.query("BEGIN");
      await c.query("DELETE FROM sessions.session_briefs WHERE origin = 'neon_backfill'");
      await c.query(down);
      await c.query("COMMIT");
    } finally { c.release(); }
    const assets = await derivedSchemaAssets();
    expect(await schemaVerdict({ ...assets.expected, dbMode: "local" }, transact)).toMatchObject({ ok: false });
    const m = await target.connect();
    try { await runMigrations(m as never, { profile }); } finally { m.release(); }
    expect(await schemaVerdict({ ...assets.expected, dbMode: "local" }, transact)).toEqual({ ok: true });
    expect((await target.query("SELECT origin FROM sessions.session_briefs")).rows).toEqual([{ origin: "native" }]);
  }, 120000);
});
