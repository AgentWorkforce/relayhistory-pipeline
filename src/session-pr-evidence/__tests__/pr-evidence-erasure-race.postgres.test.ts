/** Two-connection race on a real PostgreSQL 17 (opt in with PR_EVIDENCE_PG_URL, a throwaway local
 * server; this file creates and drops its own database). Connection A holds an install_v1 open
 * (it owns the replay-publication row lock); connection B erases the same session (directory
 * delete, tombstone, or the user's upload reset). B must wait for A and then remove what A wrote:
 * no PR evidence, state or wakeup may survive an erasure that commits after an in-flight install.
 * No cloud deployment, live queue or permission claim follows from this fixture.
 */
import { afterAll, beforeAll, describe, expect, setDefaultTimeout, test } from "bun:test";
import pg from "pg";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { randomUUID } from "node:crypto";
import { prEvidenceStore, type PrEvidenceClaim } from "../store.ts";
import { PR_EVIDENCE_EXTRACTOR_VERSION, type PullRequestEvidenceRow } from "../extract.ts";
setDefaultTimeout(120_000);
const url = process.env.PR_EVIDENCE_PG_URL;
const root = join(import.meta.dir, "../../.."), generation = "a".repeat(64);
const org = "c4fae7b9-43a9-44e8-94d5-455aa579d67b", ws = "1220c1dc-0ce5-45fe-8b67-e7bb57a2db63", user = "55555555-5555-5555-5555-555555555555";
const ROW: PullRequestEvidenceRow = { pull_request_url: "https://github.com/acme/repo/pull/42", owner: "acme", repo: "repo", number: 42, head_ref: "feat/a",
  tool_use_id: "t1", tool_name: "Bash", evidence_kind: "gh_pr_create", role: "creator", detected_at: "2026-10-30T12:00:00.123456Z", last_seen_at: "2026-10-30T12:00:01.000000Z" };

describe.skipIf(!url)("PR evidence: erasure serializes with an in-flight install (PG17, two connections)", () => {
  let admin: pg.Client, pool: pg.Pool, name = "", serial = 0;
  const read = (p: string) => readFileSync(join(root, p), "utf8");
  const ingest = async <T = any>(sql: string, args: unknown[] = []) => {
    const c = await pool.connect();
    try {
      await c.query("BEGIN; SET LOCAL ROLE bulls96_session_ingest");
      const r = await c.query(sql, args as never[]); await c.query("COMMIT"); return r as unknown as { rows: T[] };
    } catch (e) { await c.query("ROLLBACK").catch(() => {}); throw e; } finally { c.release(); }
  };
  const rpc = async (fn: string, p: unknown) => (await ingest<{ r: any }>("SELECT sessions." + fn + "($1::jsonb) r", [JSON.stringify(p)])).rows[0]!.r;

  beforeAll(async () => {
    admin = new pg.Client({ connectionString: url }); await admin.connect();
    name = "f3_pr_evidence_race_" + randomUUID().replaceAll("-", "");
    await admin.query("CREATE DATABASE " + name);
    const u = new URL(url!); u.pathname = "/" + name;
    pool = new pg.Pool({ connectionString: u.toString(), max: 6 });
    for (const r of ["anon", "authenticated", "service_role"])
      await pool.query(`DO $$BEGIN IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='${r}')THEN CREATE ROLE ${r} NOLOGIN;END IF;END$$`);
    await pool.query(read("supabase/tests/supabase_shim.sql"));
    for (const f of ["session_directory_search_worker", "session_token_verifier", "session_ingest_roles", "session_read_api_roles"]) await pool.query(read("supabase/bootstrap/" + f + ".sql"));
    for (const f of readdirSync(join(root, "supabase/migrations")).filter(f => f.endsWith(".sql")).sort()) await pool.query(read("supabase/migrations/" + f));
    await pool.query(read("supabase/bootstrap/canonical_project_events_verify.sql"));
    await pool.query(`CREATE TABLE sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text);
      INSERT INTO sessions.__96bulls_role_validation VALUES('session_directory_search_worker','${generation}','pass')`);
    await pool.query("SELECT sessions.canonical_feature_journal_seed(9007199254740992,$1)", [generation]);
    await pool.query("INSERT INTO sessions.canonical_project_event_scopes VALUES($1,$2,$3,true)", [org, ws, generation]);
    await pool.query("INSERT INTO sessions.session_pr_evidence_settings(enabled) VALUES(true) ON CONFLICT(singleton) DO UPDATE SET enabled=true");
  });
  afterAll(async () => {
    await pool?.end();
    if (name) await admin.query("DROP DATABASE IF EXISTS " + name + " WITH (FORCE)");
    await admin?.end();
  });

  /** A live, history-authoritative scope with a published source and an armed wakeup, claimed. */
  async function claimed(): Promise<{ sessionId: string; claim: PrEvidenceClaim }> {
    const sessionId = `race-${++serial}-${randomUUID().slice(0, 8)}`, identity = { orgId: org, workspaceId: ws, source: "claude", sessionId };
    const row = { org_id: org, workspace_id: ws, user_id: user, harness: "claude", session_id: sessionId, machine_id: "srv_origin",
      created_at: "2026-11-01T00:00:00Z", last_activity_at: "2026-11-01T00:00:00Z", storage_provider: "s3", bucket: "fixture",
      manifest_key: `96bulls/${org}/v1/ws=${ws}/m=srv_origin/h=claude/d=2026/11/01/s=${sessionId}/manifest.json`, content_sha256: "\\x" + "a".repeat(64),
      raw_format: "ndjson", revision: 1, trace_id: "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18" };
    await pool.query("SELECT sessions.upsert_session_index($1::jsonb)", [JSON.stringify([row])]);
    await pool.query("INSERT INTO sessions.session_directory(org_id,workspace_id,harness,session_id,created_at,last_activity_at) VALUES($1,$2,'claude',$3,now(),now())", [org, ws, sessionId]);
    const recordIds = ["record-" + sessionId];
    const mask = await rpc("ingest_history_mask_register", { generation, document: { protocol: "history-mask-v1", orgId: org, workspaceId: ws, prefix: "", kind: "leaf", count: "1", items: recordIds.map(recordId => ({ recordId, winner: null })) } });
    const b = await rpc("ingest_history_scope_begin", { generation, identity });
    await rpc("ingest_history_scope_build_keys", { generation, buildId: b.buildId, leaseToken: b.leaseToken, recordIds, owners: [] });
    await rpc("ingest_history_scope_capture", { generation, buildId: b.buildId, leaseToken: b.leaseToken });
    const id = randomUUID();
    await rpc("ingest_history_scope_install", { generation, buildId: b.buildId, leaseToken: b.leaseToken, maskSha256: mask.sha256, recordCount: "1",
      derivedAnchorKey: `96bulls/${org}/upload-generations/g=${id}/v1/ws=${ws}/m=history_scope/h=claude/d=2026/11/01/s=${sessionId}/publications/${id}/manifest.json`, bucket: "fixture", region: "us-east-1", keyRoot: "96bulls" });
    const claim = (await prEvidenceStore({ query: ingest } as never).claim(10)).find(c => c.scope.sessionId === sessionId);
    if (!claim) throw new Error("fixture: no claim for " + sessionId);
    expect({ exists: claim.exists, blocked: claim.blocked }).toEqual({ exists: true, blocked: false });
    return { sessionId, claim };
  }
  const survivors = async (sessionId: string) => {
    const out: Record<string, number> = {};
    for (const t of ["session_pr_evidence", "session_pr_evidence_state", "session_pr_evidence_wakeups"])
      out[t] = (await pool.query<{ n: number }>(`SELECT count(*)::int n FROM sessions.${t} WHERE org_id=$1 AND session_id=$2`, [org, sessionId])).rows[0]!.n;
    return out;
  };

  const erasures: [string, (sessionId: string) => [string, unknown[]]][] = [
    ["directory delete", s => ["DELETE FROM sessions.session_directory WHERE org_id=$1 AND workspace_id=$2 AND harness='claude' AND session_id=$3", [org, ws, s]]],
    ["directory tombstone", s => ["UPDATE sessions.session_directory SET deleted_at=now() WHERE org_id=$1 AND workspace_id=$2 AND harness='claude' AND session_id=$3", [org, ws, s]]],
    ["user upload reset", s => ["INSERT INTO sessions.upload_resets(org_id,workspace_id,user_id,request_id) VALUES($1,$2,$3,$4)", [org, ws, user, "reset-" + s]]],
  ];
  for (const [label, erase] of erasures)
    test(`${label} waits for the in-flight install, then leaves no evidence behind`, async () => {
      const { sessionId, claim } = await claimed();
      const a = await pool.connect(), b = await pool.connect();
      let bDone = false, bError: unknown = null;
      try {
        // A: the worker's install, inside an open transaction (holds the replay-publication row lock).
        await a.query("BEGIN; SET LOCAL ROLE bulls96_session_ingest");
        const installed = (await a.query<{ r: any }>("SELECT sessions.session_pr_evidence_install_v1($1::jsonb) r", [JSON.stringify({
          scope: claim.scope, requestVersion: claim.requestVersion, leaseToken: claim.leaseToken, source: claim.source,
          history: { authorityId: claim.history!.authorityId, maskSha256: claim.history!.maskSha256, maskApplied: true },
          extractorVersion: PR_EVIDENCE_EXTRACTOR_VERSION, status: "ready", reason: null, rows: [ROW] })])).rows[0]!.r;
        expect(installed).toEqual({ ok: true, status: "ready" });
        // B: the erasure, autocommit, on its own connection.
        const bPid = (await b.query<{ pid: number }>("SELECT pg_backend_pid() pid")).rows[0]!.pid;
        const [sql, args] = erase(sessionId);
        const bRun = b.query(sql, args as never[]).then(() => { bDone = true; }, (e) => { bDone = true; bError = e; });
        // B must be blocked on a lock held by A (not merely slow), and not finished.
        let blocked = false;
        for (let i = 0; i < 100 && !blocked && !bDone; i++) {
          const w = (await pool.query<{ blockers: number[] }>("SELECT pg_blocking_pids($1) blockers", [bPid])).rows[0]!.blockers;
          const aPid = (a as unknown as { processID: number }).processID;
          blocked = w.includes(aPid);
          if (!blocked) await Bun.sleep(50);
        }
        const finishedBeforeCommit = bDone;
        await a.query("COMMIT");
        await bRun;
        expect(bError).toBeNull();
        expect({ blocked, finishedBeforeCommit, survivors: await survivors(sessionId) }).toEqual({
          blocked: true, finishedBeforeCommit: false,
          survivors: { session_pr_evidence: 0, session_pr_evidence_state: 0, session_pr_evidence_wakeups: 0 },
        });
      } finally {
        await a.query("ROLLBACK").catch(() => {});
        a.release(); b.release();
      }
    });

  test("an install that waits for an in-flight erasure refuses instead of writing", async () => {
    const { sessionId, claim } = await claimed();
    const a = await pool.connect(), b = await pool.connect();
    try {
      // B erases first and holds its transaction open (it owns the publication row lock).
      await b.query("BEGIN");
      await b.query("DELETE FROM sessions.session_directory WHERE org_id=$1 AND workspace_id=$2 AND harness='claude' AND session_id=$3", [org, ws, sessionId]);
      await a.query("BEGIN; SET LOCAL ROLE bulls96_session_ingest");
      const aRun = a.query<{ r: any }>("SELECT sessions.session_pr_evidence_install_v1($1::jsonb) r", [JSON.stringify({
        scope: claim.scope, requestVersion: claim.requestVersion, leaseToken: claim.leaseToken, source: claim.source,
        history: { authorityId: claim.history!.authorityId, maskSha256: claim.history!.maskSha256, maskApplied: true },
        extractorVersion: PR_EVIDENCE_EXTRACTOR_VERSION, status: "ready", reason: null, rows: [ROW] })]);
      await Bun.sleep(200);
      await b.query("COMMIT");
      const r = (await aRun).rows[0]!.r;
      await a.query("COMMIT");
      expect(r).toEqual({ ok: false, code: "lease_lost" });
      expect(await survivors(sessionId)).toEqual({ session_pr_evidence: 0, session_pr_evidence_state: 0, session_pr_evidence_wakeups: 0 });
    } finally {
      await a.query("ROLLBACK").catch(() => {}); await b.query("ROLLBACK").catch(() => {});
      a.release(); b.release();
    }
  });
});
