/**
 * Feature F2 (20261105043000_brief_titles.sql): session_briefs provenance, the read API's
 * sessions.session_brief_titles_for, the Neon backfill load against fixtures, and up/down/up.
 * PGlite with every migration; the backfill is also rehearsed on owned PG17 in
 * src/migration/__tests__/neon-brief-titles.postgres.test.ts.
 */
import { afterEach, describe, expect, test } from "bun:test";
import type { PGlite } from "@electric-sql/pglite";
import { briefRpc, claims, database, hoopDb, pointer, code, read, KEY } from "./harness.ts";
import { recordBrief, type BriefText } from "../store.ts";
import { TENANT_SETTINGS_SQL, poolTenantRunner } from "../../session-read-api/liveness.ts";
import { sqlBriefTitles } from "../../session-read-api/brief-titles.ts";
import {
  BACKFILL_ENGINE_VERSION,
  backfillFingerprint,
  loadBackfill,
  verifyBackfill,
  verifyPassed,
  type NeonBriefRow,
} from "../../migration/neon-brief-titles/backfill.ts";

const sql = (path: string) => read(path).split("\n").filter((l) => !l.startsWith("\\")).join("\n");
const ROLE = sql("supabase/bootstrap/session_read_api_role.sql");
const VERIFY = sql("supabase/bootstrap/session_read_api_role_verify.sql");
const DOWN = "supabase/rollbacks/20261105043000_brief_titles.down.sql";
const UP = "supabase/migrations/20261105043000_brief_titles.sql";

const open: PGlite[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((db) => db.close()));
});
async function db(): Promise<PGlite> {
  const pg = await database();
  open.push(pg);
  await pg.exec(ROLE);
  return pg;
}

/** As the read Worker: role, BEGIN READ ONLY, tenant bound, run, COMMIT. */
async function asReadApi<T>(pg: PGlite, tenant: [string, string] | null, work: () => Promise<T>, role = "bulls96_read_api"): Promise<T> {
  await pg.exec(`SET ROLE ${role}`);
  try {
    await pg.exec("BEGIN READ ONLY");
    try {
      if (tenant) await pg.query(TENANT_SETTINGS_SQL, tenant);
      const out = await work();
      await pg.exec("COMMIT");
      return out;
    } catch (error) {
      await pg.exec("ROLLBACK");
      throw error;
    }
  } finally {
    await pg.exec("RESET ROLE");
  }
}

const titlesFor = (pg: PGlite, ws: string, keys: Array<[string, string]>) =>
  pg
    .query<Record<string, unknown>>("SELECT * FROM sessions.session_brief_titles_for($1, $2::text[], $3::text[])", [
      ws,
      keys.map((k) => k[0]),
      keys.map((k) => k[1]),
    ])
    .then((r) => r.rows);

const text = (over: Partial<BriefText> = {}): BriefText => ({
  title: "Native title",
  description: "A native description.",
  state: "completed",
  scrubRuleset: "relayhistory-scrub",
  scrubVersion: 1,
  fingerprint: "f".repeat(64),
  promptVersion: "session-summary-v3",
  engineVersion: "session-brief-v3",
  model: "gpt-5.6-luna",
  messageCount: 3,
  shortenedMessages: 0,
  inputTokens: 10,
  outputTokens: 5,
  durationMs: 20,
  generatedAt: new Date("2026-11-01T00:00:00Z"),
  ...over,
});

const neon = (over: Partial<NeonBriefRow> = {}): NeonBriefRow => ({
  org_id: KEY.orgId,
  workspace_id: KEY.workspaceId,
  source_provider: KEY.harness,
  session_id: KEY.sessionId,
  job_id: "0b6f6a39-7d5e-4c0e-9c1e-2f1d8b8f0a01",
  title: "  Neon title  ",
  description: "A Neon description.",
  state: "completed",
  prompt_version: "session-summary-v3",
  summary_version: "session-summary-v3",
  model: "gpt-5.6-luna",
  summary_model_version: "gpt-5.6-luna",
  generated_at: "2026-10-20T10:00:00.000Z",
  completed_at: "2026-10-20T10:00:01.000000Z",
  message_count: "4",
  shortened_messages: "0",
  input_tokens: "100",
  output_tokens: "20",
  ...over,
});

const row = async (pg: PGlite, key = KEY) =>
  (
    await pg.query<Record<string, unknown>>(
      "SELECT * FROM sessions.session_briefs WHERE org_id=$1 AND workspace_id=$2 AND harness=$3 AND session_id=$4",
      [key.orgId, key.workspaceId, key.harness, key.sessionId],
    )
  ).rows[0];

/** The PGlite connection as a transaction-capable target client. */
const target = (pg: PGlite) => ({ query: async (q: string, p?: unknown[]) => ({ rows: (await pg.query(q, p as never)).rows as never[] }) });

describe("session_briefs provenance", () => {
  test("origin is derived from the brief text, never from the writer", async () => {
    const pg = await db();
    await pointer(pg);
    await recordBrief(hoopDb(pg), KEY, { "m-1": 1 }, { status: "ready", text: text() });
    expect(await row(pg)).toMatchObject({ origin: "native", legacy_job_id: null });
    // A writer cannot label a native brief as backfilled.
    await pg.query("UPDATE sessions.session_briefs SET origin = 'neon_backfill', legacy_job_id = 'j-1'");
    expect(await row(pg)).toMatchObject({ origin: "native", legacy_job_id: null });
    // A backfill engine version without a legacy job id violates the CHECK.
    expect(await code(pg.query(`UPDATE sessions.session_briefs SET engine_version = '${BACKFILL_ENGINE_VERSION}'`))).toBe("23514");
  });

  test("a native brief replaces a backfilled one; a failed attempt keeps it; a quarantine clears it", async () => {
    const pg = await db();
    await pointer(pg);
    expect((await loadBackfill(target(pg), [neon()], "execute")).outcomes.inserted).toBe(1);
    expect(await row(pg)).toMatchObject({
      origin: "neon_backfill",
      legacy_job_id: neon().job_id,
      title: "Neon title",
      engine_version: BACKFILL_ENGINE_VERSION,
      brief_fingerprint: backfillFingerprint(neon().job_id),
      revision: 0,
    });
    // Contained: relayhistory-cloud's /brief (session_brief) reads a backfill-only brief as
    // missing (an absent brief), never stale or ready.
    expect((await briefRpc(pg))[0]).toMatchObject({ status: "missing", stale: false, title: null, description: null, state: null, engine_version: null });
    // A failed native attempt keeps the previous text and so its provenance.
    await recordBrief(hoopDb(pg), KEY, { "m-1": 1 }, { status: "failed", error: "model_failed" });
    expect(await row(pg)).toMatchObject({ status: "failed", origin: "neon_backfill", title: "Neon title" });
    // A native "reused" never matches the backfill fingerprint.
    await recordBrief(hoopDb(pg), KEY, { "m-1": 1 }, { status: "reused", fingerprint: "f".repeat(64) });
    expect(await row(pg)).toMatchObject({ origin: "neon_backfill" });
    await recordBrief(hoopDb(pg), KEY, { "m-1": 1 }, { status: "ready", text: text() });
    expect(await row(pg)).toMatchObject({ origin: "native", legacy_job_id: null, title: "Native title" });
    // The backfill never overwrites a native title, on a rerun either.
    expect((await loadBackfill(target(pg), [neon()], "execute")).outcomes).toMatchObject({ skipped_native: 1, inserted: 0, updated: 0 });
    expect(await row(pg)).toMatchObject({ title: "Native title" });
    await recordBrief(hoopDb(pg), KEY, { "m-1": 1 }, { status: "quarantined", error: "session_quarantined" });
    expect(await row(pg)).toMatchObject({ origin: "native", title: null, status: "quarantined" });
    // A quarantine is never refilled.
    expect((await loadBackfill(target(pg), [neon()], "execute")).outcomes.skipped_quarantined).toBe(1);
    expect(await row(pg)).toMatchObject({ title: null });
  });
});

describe("sessions.session_brief (the /brief route) is contained", () => {
  const BRIEF_BODY = /CREATE OR REPLACE FUNCTION sessions\.session_brief\([\s\S]*?\$brief\$;/;
  const body026000 = () => read("supabase/migrations/20261105026000_canonical_history_lineage.sql").match(BRIEF_BODY)![0];
  const body043000 = () => read(UP).match(BRIEF_BODY)![0];
  const fields = (r: Record<string, unknown>) => {
    const { org_id, workspace_id, harness, session_id, current_revision, ...rest } = r;
    void org_id; void workspace_id; void harness; void session_id; void current_revision;
    return rest;
  };
  const queue = (pg: PGlite, key = KEY) =>
    pg.query("INSERT INTO sessions.session_brief_wakeups (org_id, workspace_id, harness, session_id, admission, due_at) VALUES ($1,$2,$3,$4,'background',now())", [key.orgId, key.workspaceId, key.harness, key.sessionId]);

  test("in the worker's own scope a backfill-only row reads exactly as an absent native brief (ordinary and line-break)", async () => {
    for (const title of ["Neon title", "Fix login\nand logout"]) {
      const pg = await db();
      const absent = { ...KEY, sessionId: "s-absent" };
      await pointer(pg);
      await pointer(pg, { key: absent });
      await loadBackfill(target(pg), [neon({ title, description: null })], "execute");
      expect((await row(pg))!.origin).toBe("neon_backfill");
      const got = (await briefRpc(pg))[0]!;
      const none = (await briefRpc(pg, absent))[0]!;
      expect(fields(got)).toEqual(fields(none));
      expect(got).toMatchObject({ status: "missing", stale: false, title: null, description: null, state: null, engine_version: null, model: null, prompt_version: null, generated_at: null, attempted_at: null, error: null });
      expect(JSON.stringify(got)).not.toContain(title.split("\n")[0]);
      expect(JSON.stringify(got)).not.toContain("neon-backfill");
      // Queued: pending, as for an absent brief.
      await queue(pg);
      await queue(pg, absent);
      expect(fields((await briefRpc(pg))[0]!)).toEqual(fields((await briefRpc(pg, absent))[0]!));
      expect((await briefRpc(pg))[0]).toMatchObject({ status: "pending", title: null });
      // The session list's own reader still serves it, within its own (read API) scope.
      expect((await asReadApi(pg, ["org-a", "ws-a"], () => titlesFor(pg, "ws-a", [["claude", "s-1"]]))).map((r) => r.title)).toEqual([title]);
      // A native replacement becomes visible normally.
      await pg.query("DELETE FROM sessions.session_brief_wakeups");
      await recordBrief(hoopDb(pg), KEY, { "m-1": 1 }, { status: "ready", text: text() });
      expect((await briefRpc(pg))[0]).toMatchObject({ status: "ready", title: "Native title", engine_version: "session-brief-v3" });
    }
  });

  test("a native attempt after a backfill keeps its failure semantics, without the legacy text", async () => {
    const pg = await db();
    await pointer(pg);
    await loadBackfill(target(pg), [neon()], "execute");
    await recordBrief(hoopDb(pg), KEY, { "m-1": 1 }, { status: "failed", error: "model_failed" });
    expect((await row(pg))!.origin).toBe("neon_backfill");
    expect((await briefRpc(pg))[0]).toMatchObject({ status: "failed", error: "model_failed", title: null, description: null, engine_version: null, stale: false });
    expect(((await briefRpc(pg))[0]!.attempted_at)).not.toBeNull();
  });

  test("native stale, tenant isolation, erasure and the qualified-history refusal are unchanged", async () => {
    const pg = await db();
    await pointer(pg);
    await recordBrief(hoopDb(pg), KEY, { "m-1": 1 }, { status: "ready", text: text() });
    // A newer pointer revision: the native brief is returned stale.
    await pointer(pg, { revision: 2 });
    expect((await briefRpc(pg))[0]).toMatchObject({ status: "stale", stale: true, title: "Native title" });
    // Another org's claims never see it.
    expect(await briefRpc(pg, KEY, claims("org-b", [KEY.workspaceId]))).toEqual([]);
    // History gate: a live, unqualified managed scope refuses (0A000).
    await pg.exec("CREATE OR REPLACE FUNCTION sessions.ingest_history_live() RETURNS boolean LANGUAGE sql STABLE SET search_path=pg_catalog,pg_temp AS $live$ SELECT true $live$");
    await pg.exec("SET session_replication_role = replica");
    await pg.query("INSERT INTO sessions.ingest_history_scope_current (org_id, workspace_id, harness, session_id, authority_id) VALUES ($1,$2,$3,$4,gen_random_uuid())", [KEY.orgId, KEY.workspaceId, KEY.harness, KEY.sessionId]);
    await pg.exec("SET session_replication_role = origin");
    expect(await code(briefRpc(pg))).toBe("0A000");
    await pg.query("DELETE FROM sessions.ingest_history_scope_current");
    // Erasure: a tombstoned pointer returns no row.
    await pg.query("UPDATE sessions.session_index SET deleted_at = now()");
    expect(await briefRpc(pg)).toEqual([]);
  });

  test("CAUSAL: 026000's body (no provenance exclusion) returns the backfilled title; 043000's does not", async () => {
    const pg = await db();
    await pointer(pg);
    await loadBackfill(target(pg), [neon({ title: "Fix login\nand logout" })], "execute");
    await pg.exec(body026000());
    expect((await briefRpc(pg))[0]).toMatchObject({ status: "stale", title: "Fix login\nand logout", engine_version: "neon-backfill:v1" });
    await pg.exec(body043000());
    expect((await briefRpc(pg))[0]).toMatchObject({ status: "missing", title: null, engine_version: null });
    // The worker grant is the same either way (CREATE OR REPLACE keeps the ACL).
    const acl = (await pg.query<{ g: string }>("SELECT string_agg(r.rolname, ',' ORDER BY r.rolname) AS g FROM pg_proc p CROSS JOIN LATERAL aclexplode(p.proacl) a JOIN pg_roles r ON r.oid = a.grantee WHERE p.oid = 'sessions.session_brief(text,text,text)'::regprocedure AND a.grantee <> p.proowner")).rows[0]!.g;
    expect(acl).toBe("session_directory_search_worker");
  });
});

describe("sessions.session_brief_titles_for", () => {
  test("titles for the bound tenant only; physical or logical ids; live pointers only", async () => {
    const pg = await db();
    const other = { ...KEY, sessionId: "s-2" };
    const foreign = { ...KEY, orgId: "org-b" };
    await pointer(pg);
    await pointer(pg, { key: other });
    await pointer(pg, { key: foreign });
    await recordBrief(hoopDb(pg), KEY, { "m-1": 1 }, { status: "ready", text: text({ title: "First" }) });
    await recordBrief(hoopDb(pg), foreign, { "m-1": 1 }, { status: "ready", text: text({ title: "Foreign" }) });
    // s-2 has a pointer and no brief: no row (a known null).
    const got = await asReadApi(pg, ["org-a", "ws-a"], () => titlesFor(pg, "ws-a", [["claude", "s-1"], ["claude", "s-2"], ["claude", "nope"], ["claude", "s-1"]]));
    expect(got).toEqual([{ org_id: "org-a", workspace_id: "ws-a", harness: "claude", session_id: "s-1", title: "First", origin: "native", generated_at: expect.anything() }]);
    // org-b's brief is invisible to org-a's binding, whatever is asked.
    expect(await asReadApi(pg, ["org-a", "ws-a"], () => titlesFor(pg, "ws-a", [["claude", "s-1"]]))).toHaveLength(1);
    expect((await asReadApi(pg, ["org-b", "ws-a"], () => titlesFor(pg, "ws-a", [["claude", "s-1"]])))[0]).toMatchObject({ title: "Foreign", org_id: "org-b" });
    // A logical (canonical) id resolves to its physical session's brief.
    await pg.query("UPDATE sessions.session_index SET canonical_session_id = 'logical-1' WHERE org_id='org-a' AND session_id='s-1'");
    expect((await asReadApi(pg, ["org-a", "ws-a"], () => titlesFor(pg, "ws-a", [["claude", "logical-1"]])))[0]).toMatchObject({ session_id: "logical-1", title: "First" });
    // The physical id of an encoded pointer is not a RelayHistory id: it resolves to nothing.
    expect(await asReadApi(pg, ["org-a", "ws-a"], () => titlesFor(pg, "ws-a", [["claude", "s-1"]]))).toEqual([]);
    // A tombstoned pointer removes the brief (forget trigger) and so the title.
    await pg.query("UPDATE sessions.session_index SET deleted_at = now() WHERE org_id='org-a' AND session_id='s-1'");
    expect(await asReadApi(pg, ["org-a", "ws-a"], () => titlesFor(pg, "ws-a", [["claude", "s-1"], ["claude", "logical-1"]]))).toEqual([]);
  });

  test("refuses every other caller, binding, workspace and malformed key list", async () => {
    const pg = await db();
    await pointer(pg);
    expect(await code(asReadApi(pg, null, () => titlesFor(pg, "ws-a", [["claude", "s-1"]])))).toBe("42501");
    expect(await code(asReadApi(pg, ["org-a", "ws-b"], () => titlesFor(pg, "ws-a", [["claude", "s-1"]])))).toBe("42501");
    expect(await code(asReadApi(pg, ["org-a", "ws-a"], () => titlesFor(pg, "ws-a", [["claude", "s-1"]]), "session_directory_search_worker"))).toBe("42501");
    expect(await code(asReadApi(pg, ["org-a", "ws-a"], () => titlesFor(pg, "ws-a", [["claude", "s-1"]]), "service_role"))).toBe("42501");
    for (const keys of [[], Array.from({ length: 101 }, (_, i) => ["claude", `s-${i}`] as [string, string]), [["Bad Harness", "s"]], [["claude", ""]], [["claude", "x".repeat(256)]]] as Array<Array<[string, string]>>)
      expect(await code(asReadApi(pg, ["org-a", "ws-a"], () => titlesFor(pg, "ws-a", keys)))).toBe("22023");
    expect(
      await code(
        asReadApi(pg, ["org-a", "ws-a"], () =>
          pg.query("SELECT * FROM sessions.session_brief_titles_for('ws-a', ARRAY['claude','claude'], ARRAY['s-1'])"),
        ),
      ),
    ).toBe("22023");
    // The read role still cannot read the table directly.
    expect(await code(asReadApi(pg, ["org-a", "ws-a"], () => pg.query("SELECT title FROM sessions.session_briefs")))).toBe("42501");
  });

  test("the read API's runner reads it, the role verifier passes, and the ACL is exact", async () => {
    const pg = await db();
    await pointer(pg);
    await recordBrief(hoopDb(pg), KEY, { "m-1": 1 }, { status: "ready", text: text() });
    await pg.exec(VERIFY);
    const acl = (
      await pg.query<{ fn: string; grantee: string }>(
        `SELECT p.oid::regprocedure::text AS fn, coalesce(r.rolname, 'PUBLIC') AS grantee
           FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
           CROSS JOIN LATERAL aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) a
           LEFT JOIN pg_roles r ON r.oid = a.grantee
          WHERE n.nspname = 'sessions' AND p.proname IN ('session_brief_titles_for', 'session_briefs_provenance')
            AND a.grantee <> p.proowner ORDER BY 1, 2`,
      )
    ).rows;
    expect(acl).toEqual([{ fn: "sessions.session_brief_titles_for(text,text[],text[])", grantee: "bulls96_read_api" }]);
    await pg.exec("SET ROLE bulls96_read_api");
    try {
      const run = poolTenantRunner({ connect: async () => ({ query: (t: string, p?: unknown[]) => pg.query(t, p as never) as never, release() {} }) });
      const rows = await sqlBriefTitles(run).titles("org-a", "ws-a", [{ source: "claude", sessionId: "s-1" }]);
      expect(rows?.map((r) => r.title)).toEqual(["Native title"]);
    } finally {
      await pg.exec("RESET ROLE");
    }
  });
});

describe("Neon backfill load (fixtures)", () => {
  test("dry run reports and keeps nothing; execute writes; rerun is idempotent; verify is exact", async () => {
    const pg = await db();
    await pointer(pg);
    await pointer(pg, { key: { ...KEY, sessionId: "s-2" } });
    await pointer(pg, { key: { ...KEY, sessionId: "s-3" } });
    for (const id of ["s-6", "s-7", "s-8", "s-9"]) await pointer(pg, { key: { ...KEY, sessionId: id } });
    await pg.query("UPDATE sessions.session_index SET deleted_at = now() WHERE session_id = 's-3'");
    const fixture = [
      neon(),
      neon({ session_id: "s-2", job_id: "j-2", state: null, prompt_version: "session-summary-v1", title: "Legacy v1" }),
      neon({ session_id: "s-3", job_id: "j-3" }),
      neon({ session_id: "s-4", job_id: "j-4" }),
      neon({ session_id: "s-5", job_id: "j-5", title: "   " }),
      neon({ session_id: "s-6", job_id: "j-6", title: "line\nbreak" }),
      neon({ session_id: "s-7", job_id: "j-7", description: null }),
      neon({ session_id: "s-8", job_id: "j-8", state: "nope" }),
      neon({ session_id: "s-9", job_id: "j-9", generated_at: "garbage", completed_at: null }),
      neon({ session_id: "s-10", job_id: "j-10", source_provider: "Bad" }),
    ];
    const dry = await loadBackfill(target(pg), fixture, "dry-run");
    expect(dry).toMatchObject({
      mode: "dry-run",
      candidates: 10,
      outcomes: { inserted: 5, updated: 0, skipped_native: 0, skipped_quarantined: 0, skipped_no_pointer: 2 },
      // Only what Cloud would not show, or SP cannot key, is skipped (parity).
      skipped: { title_blank: 1, generated_at_invalid: 1, key_invalid: 1 },
    });
    expect((await pg.query("SELECT 1 FROM sessions.session_briefs")).rows).toHaveLength(0);
    const exec = await loadBackfill(target(pg), fixture, "execute");
    expect(exec.outcomes).toEqual(dry.outcomes);
    expect(await row(pg, { ...KEY, sessionId: "s-2" })).toMatchObject({ origin: "neon_backfill", state: null, prompt_version: "session-summary-v1", title: "Legacy v1" });
    // Titles Cloud shows survive even when the rest of the Neon brief is not representable.
    expect(await row(pg, { ...KEY, sessionId: "s-6" })).toMatchObject({ title: "line\nbreak", origin: "neon_backfill" });
    expect(await row(pg, { ...KEY, sessionId: "s-7" })).toMatchObject({ title: "Neon title", description: null, origin: "neon_backfill" });
    expect(await row(pg, { ...KEY, sessionId: "s-8" })).toMatchObject({ title: "Neon title", state: null });
    // The directory title is not touched by the backfill.
    expect((await pg.query<{ title: string }>("SELECT title FROM sessions.session_directory WHERE session_id = 's-1'")).rows[0]?.title).toBe("pointer title");
    const again = await loadBackfill(target(pg), fixture, "execute");
    expect(again.outcomes).toMatchObject({ inserted: 0, updated: 5 });
    const v = await verifyBackfill(target(pg), fixture);
    // s-9 has a live pointer but no representable brief: missing (a defect, fails the run).
    // s-3 (erased), s-4 (never copied) and s-10 (unkeyable) have no live pointer: reported
    // as no_pointer (excluded from the title comparison), not failed. s-5 is blank (not compared).
    expect(v).toMatchObject({ candidates: 10, compared: 9, equal: 5, different: 0, native: 0, missing: 1, no_pointer: 3 });
    expect(v.samples.missing).toEqual([["org-a", "ws-a", "claude", "s-9"]]);
    expect(verifyPassed(v)).toBe(false);
    // Served through the read API function exactly as Neon's Cloud-visible (trimmed) titles.
    const served = await asReadApi(pg, ["org-a", "ws-a"], () => titlesFor(pg, "ws-a", [["claude", "s-1"], ["claude", "s-2"], ["claude", "s-3"]]));
    expect(served.map((r) => [r.session_id, r.title, r.origin])).toEqual([["s-1", "Neon title", "neon_backfill"], ["s-2", "Legacy v1", "neon_backfill"]]);
    // A tampered SP title is a reported difference, never silently equal.
    await pg.query("UPDATE sessions.session_briefs SET title = 'Tampered' WHERE session_id = 's-1'");
    expect((await verifyBackfill(target(pg), fixture)).different).toBe(1);
  });

  test("an object-layout-encoded pointer gets its Neon title (resolution through canonical_session_id)", async () => {
    const pg = await db();
    // RelayHistory id sess/abc, stored on the encoded physical pointer sess_abc.
    await pointer(pg, { key: { ...KEY, sessionId: "sess_abc" } });
    await pg.query("UPDATE sessions.session_index SET canonical_session_id = 'sess/abc' WHERE session_id = 'sess_abc'");
    const fixture = [neon({ session_id: "sess/abc", title: "Fix login" })];
    expect((await loadBackfill(target(pg), fixture, "execute")).outcomes).toMatchObject({ inserted: 1, skipped_no_pointer: 0 });
    expect(await row(pg, { ...KEY, sessionId: "sess_abc" })).toMatchObject({ title: "Fix login", origin: "neon_backfill" });
    // Served after cutover under the id Cloud keys...
    const served = await asReadApi(pg, ["org-a", "ws-a"], () => titlesFor(pg, "ws-a", [["claude", "sess/abc"], ["claude", "sess_abc"]]));
    expect(served.map((r) => [r.session_id, r.title])).toEqual([["sess/abc", "Fix login"]]);
    // ...and verify agrees.
    const v = await verifyBackfill(target(pg), fixture);
    expect(v).toMatchObject({ equal: 1, missing: 0, no_pointer: 0, different: 0 });
    expect(verifyPassed(v)).toBe(true);
  });

  test("a native brief on any physical sibling wins over a newer backfilled one", async () => {
    const pg = await db();
    for (const id of ["sess_a", "sess_b"]) await pointer(pg, { key: { ...KEY, sessionId: id } });
    await pg.query("UPDATE sessions.session_index SET canonical_session_id = 'L-1' WHERE session_id IN ('sess_a', 'sess_b')");
    await recordBrief(hoopDb(pg), { ...KEY, sessionId: "sess_a" }, { "m-1": 1 }, { status: "ready", text: text({ title: "Current native", generatedAt: new Date("2026-10-01T00:00:00Z") }) });
    // The backfill writes every physical sibling it can; sess_a keeps its native title.
    expect((await loadBackfill(target(pg), [neon({ session_id: "L-1", title: "Stale Neon", generated_at: "2026-10-30T00:00:00Z" })], "execute")).outcomes.inserted).toBe(1);
    expect(await row(pg, { ...KEY, sessionId: "sess_b" })).toMatchObject({ title: "Stale Neon", origin: "neon_backfill" });
    const served = await asReadApi(pg, ["org-a", "ws-a"], () => titlesFor(pg, "ws-a", [["claude", "L-1"]]));
    expect(served.map((r) => r.title)).toEqual(["Current native"]);
    const v = await verifyBackfill(target(pg), [neon({ session_id: "L-1", title: "Stale Neon" })]);
    expect(v).toMatchObject({ native: 1, missing: 0, different: 0 });
  });

  test("a whitespace-only title is never served and does not hide a sibling's title", async () => {
    const pg = await db();
    for (const id of ["sess_a", "sess_b"]) await pointer(pg, { key: { ...KEY, sessionId: id } });
    await pg.query("UPDATE sessions.session_index SET canonical_session_id = 'L-2' WHERE session_id IN ('sess_a', 'sess_b')");
    await recordBrief(hoopDb(pg), { ...KEY, sessionId: "sess_a" }, { "m-1": 1 }, { status: "ready", text: text({ title: "   ", generatedAt: new Date("2026-11-02T00:00:00Z") }) });
    await recordBrief(hoopDb(pg), { ...KEY, sessionId: "sess_b" }, { "m-1": 1 }, { status: "ready", text: text({ title: "  Older real  ", generatedAt: new Date("2026-10-01T00:00:00Z") }) });
    const served = await asReadApi(pg, ["org-a", "ws-a"], () => titlesFor(pg, "ws-a", [["claude", "L-2"], ["claude", "sess_a"]]));
    expect(served.map((r) => [r.session_id, r.title])).toEqual([["L-2", "Older real"]]);
  });

  test("a fixture that repeats a key is refused before any write", async () => {
    const pg = await db();
    await pointer(pg);
    await expect(loadBackfill(target(pg), [neon(), neon({ job_id: "j-x" })], "execute")).rejects.toThrow(/duplicate session key/);
    expect((await pg.query("SELECT 1 FROM sessions.session_briefs")).rows).toHaveLength(0);
  });
});

describe("20261105043000 up/down/up", () => {
  test("down refuses while backfilled briefs remain (never deletes them), then restores the pre-043000 shapes incl. 026000 session_brief; up again is clean", async () => {
    const pg = await db();
    await pointer(pg);
    await loadBackfill(target(pg), [neon()], "execute");
    expect(await code(pg.exec(read(DOWN)))).toBe("55000");
    await pg.query("DELETE FROM sessions.session_briefs WHERE origin = 'neon_backfill'");
    await pg.exec(read(DOWN));
    // session_brief is back to 026000's body exactly.
    const body = (await pg.query<{ src: string }>("SELECT prosrc AS src FROM pg_proc WHERE oid = 'sessions.session_brief(text,text,text)'::regprocedure")).rows[0]!.src;
    const original = read("supabase/migrations/20261105026000_canonical_history_lineage.sql").match(/CREATE OR REPLACE FUNCTION sessions\.session_brief\([\s\S]*?AS \$brief\$([\s\S]*?)\$brief\$;/)![1];
    expect(body).toBe(original);
    const cols = (await pg.query<{ attname: string }>("SELECT attname FROM pg_attribute WHERE attrelid = 'sessions.session_briefs'::regclass AND attname IN ('origin','legacy_job_id') AND NOT attisdropped")).rows;
    expect(cols).toEqual([]);
    expect((await pg.query("SELECT to_regprocedure('sessions.session_brief_titles_for(text,text[],text[])') AS f")).rows[0]).toEqual({ f: null });
    // The restored CHECK refuses a v3 brief without a state again.
    await recordBrief(hoopDb(pg), KEY, { "m-1": 1 }, { status: "ready", text: text() });
    expect(await code(pg.query("UPDATE sessions.session_briefs SET state = NULL"))).toBe("23514");
    await pg.exec(read(UP));
    expect(await row(pg)).toMatchObject({ origin: "native", legacy_job_id: null });
    // Up again: session_brief is 043000's contained body again, with the same worker grant.
    const again = (await pg.query<{ src: string }>("SELECT prosrc AS src FROM pg_proc WHERE oid = 'sessions.session_brief(text,text,text)'::regprocedure")).rows[0]!.src;
    expect(again).toContain("neon-backfill:%");
    expect(again).toBe(read(UP).match(/CREATE OR REPLACE FUNCTION sessions\.session_brief\([\s\S]*?AS \$brief\$([\s\S]*?)\$brief\$;/)![1]);
    await pg.exec(VERIFY);
  });
});
