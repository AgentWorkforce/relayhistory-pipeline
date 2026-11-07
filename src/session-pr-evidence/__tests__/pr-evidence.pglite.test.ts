/** Actual creator -> 610 journal -> F10 event + PR-evidence wakeup -> worker RPCs -> event-bound
 * Cloud read, on PGlite with every migration. Set PR_EVIDENCE_PG_URL to run the same file against
 * a real PostgreSQL 17 (a throwaway local database; it is dropped and recreated).
 * No cloud deployment, live queue or permission claim follows from this fixture.
 */
import { beforeAll, afterAll, expect, test, setDefaultTimeout } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import pg from "pg";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { createHash, randomUUID } from "node:crypto";
import { prEvidenceStore } from "../store.ts";
import { prEvidenceTick } from "../worker.ts";
import { PR_EVIDENCE_EXTRACTOR_VERSION, type PullRequestEvidenceRow } from "../extract.ts";
setDefaultTimeout(120_000);
const root = join(import.meta.dir, "../../.."), generation = "a".repeat(64);
const org = "c4fae7b9-43a9-44e8-94d5-455aa579d67b", ws = "1220c1dc-0ce5-45fe-8b67-e7bb57a2db63", user = "44444444-4444-4444-4444-444444444444";
const scope = { orgId: org, workspaceId: ws };
type Db = { exec(sql: string): Promise<unknown>; query<T = any>(sql: string, args?: unknown[]): Promise<{ rows: T[] }>; close(): Promise<void> };
let db: Db, serial = 0, readySession = "";

async function open(): Promise<Db> {
  const url = process.env.PR_EVIDENCE_PG_URL;
  if (!url) {
    const lite = new PGlite({ extensions: { btree_gin, pg_trgm } });
    return { exec: (s) => lite.exec(s), query: (s, a) => lite.query(s, a as never) as never, close: () => lite.close() };
  }
  const admin = new pg.Client({ connectionString: url });
  await admin.connect();
  const name = "f3_pr_evidence_" + process.pid;
  await admin.query(`DROP DATABASE IF EXISTS ${name}`); await admin.query(`CREATE DATABASE ${name}`); await admin.end();
  const u = new URL(url); u.pathname = "/" + name;
  const client = new pg.Client({ connectionString: u.toString() }); await client.connect();
  return { exec: (s) => client.query(s), query: (s, a) => client.query(s, a as unknown[]) as never, close: () => client.end() };
}

beforeAll(async () => {
  db = await open();
  const read = (p: string) => readFileSync(join(root, p), "utf8");
  if (process.env.PR_EVIDENCE_PG_URL)
    for (const r of ["anon", "authenticated", "service_role"])
      await db.exec(`DO $$BEGIN IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='${r}')THEN CREATE ROLE ${r} NOLOGIN;END IF;END$$`);
  await db.exec(read("supabase/tests/supabase_shim.sql"));
  for (const f of ["session_directory_search_worker", "session_token_verifier", "session_ingest_roles", "session_read_api_roles"]) await db.exec(read("supabase/bootstrap/" + f + ".sql"));
  for (const f of readdirSync(join(root, "supabase/migrations")).filter(f => f.endsWith(".sql")).sort()) await db.exec(read("supabase/migrations/" + f));
  await db.exec(read("supabase/bootstrap/canonical_project_events_verify.sql"));
  await db.exec(`CREATE TABLE sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text);
    INSERT INTO sessions.__96bulls_role_validation VALUES('session_directory_search_worker','${generation}','pass')`);
  await db.query("SELECT sessions.canonical_feature_journal_seed(9007199254740992,$1)", [generation]);
  await db.query("INSERT INTO sessions.canonical_project_event_scopes VALUES($1,$2,$3,true)", [org, ws, generation]);
});
afterAll(async () => { await db?.close(); });
const queryAs = (role: string) => async <T = any>(sql: string, args: unknown[] = []) => {
  await db.exec("BEGIN; SET LOCAL ROLE " + role);
  try { const r = await db.query<T>(sql, args); await db.exec("COMMIT"); return r; }
  catch (e) { await db.exec("ROLLBACK"); throw e; }
};
const ingest = queryAs("bulls96_session_ingest"), actor = queryAs("bulls96_project_events"), service = queryAs("service_role");
const rpc = async (fn: string, p: unknown) => (await ingest<{ r: any }>("SELECT sessions." + fn + "($1::jsonb) r", [JSON.stringify(p)])).rows[0]!.r;
const read = async (eventId: string, gen: string, s = scope) =>
  (await actor<{ r: any }>("SELECT sessions.session_pr_evidence_read_v1($1::jsonb) r", [JSON.stringify({ schemaGeneration: generation, scope: s, eventId, generation: gen })])).rows[0]!.r;
const setEnabled = (on: boolean) => db.query("INSERT INTO sessions.session_pr_evidence_settings(enabled) VALUES($1) ON CONFLICT(singleton) DO UPDATE SET enabled=excluded.enabled", [on]);
const store = () => prEvidenceStore({ query: ingest } as never);

async function prepare() {
  const sessionId = `f3-${++serial}`, identity = { orgId: org, workspaceId: ws, source: "claude", sessionId };
  const row = { org_id: org, workspace_id: ws, user_id: user, harness: "claude", session_id: sessionId, machine_id: "srv_origin",
    created_at: "2026-11-01T00:00:00Z", last_activity_at: "2026-11-01T00:00:00Z", storage_provider: "s3", bucket: "fixture",
    manifest_key: `96bulls/${org}/v1/ws=${ws}/m=srv_origin/h=claude/d=2026/11/01/s=${sessionId}/manifest.json`, content_sha256: "\\x" + "a".repeat(64),
    raw_format: "ndjson", revision: 1, trace_id: "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18" };
  await db.query("SELECT sessions.upsert_session_index($1::jsonb)", [JSON.stringify([row])]);
  await db.query("INSERT INTO sessions.session_directory(org_id,workspace_id,harness,session_id,created_at,last_activity_at) VALUES($1,$2,'claude',$3,now(),now())", [org, ws, sessionId]);
  const recordIds = ["record-" + sessionId].sort((a, b) => createHash("sha256").update(a).digest("hex").localeCompare(createHash("sha256").update(b).digest("hex")));
  const mask = await rpc("ingest_history_mask_register", { generation, document: { protocol: "history-mask-v1", orgId: org, workspaceId: ws, prefix: "", kind: "leaf", count: "1", items: recordIds.map(recordId => ({ recordId, winner: null })) } });
  const install = async () => {
    const b = await rpc("ingest_history_scope_begin", { generation, identity });
    await rpc("ingest_history_scope_build_keys", { generation, buildId: b.buildId, leaseToken: b.leaseToken, recordIds, owners: [] });
    await rpc("ingest_history_scope_capture", { generation, buildId: b.buildId, leaseToken: b.leaseToken });
    const id = randomUUID();
    return rpc("ingest_history_scope_install", { generation, buildId: b.buildId, leaseToken: b.leaseToken, maskSha256: mask.sha256, recordCount: "1",
      derivedAnchorKey: `96bulls/${org}/upload-generations/g=${id}/v1/ws=${ws}/m=history_scope/h=claude/d=2026/11/01/s=${sessionId}/publications/${id}/manifest.json`, bucket: "fixture", region: "us-east-1", keyRoot: "96bulls" });
  };
  const event = async () => (await db.query<any>("SELECT event_id::text,generation::text,source_epoch::text,source_fingerprint FROM sessions.canonical_project_events WHERE org_id=$1 AND workspace_id=$2 AND session_id=$3", [org, ws, sessionId])).rows[0];
  const wakeup = async () => (await db.query<any>("SELECT request_version::text,last_error,lease_token FROM sessions.session_pr_evidence_wakeups WHERE org_id=$1 AND session_id=$2", [org, sessionId])).rows[0];
  const replace = async () => {
    await db.query("SELECT sessions.upsert_session_index($1::jsonb)", [JSON.stringify([{ ...row, revision: 2, content_sha256: "\\x" + "b".repeat(64) }])]);
    return install();
  };
  return { identity, install, replace, event, wakeup, sessionId };
}
const ROW: PullRequestEvidenceRow = { pull_request_url: "https://github.com/acme/repo/pull/42", owner: "acme", repo: "repo", number: 42, head_ref: "feat/a",
  tool_use_id: "t1", tool_name: "Bash", evidence_kind: "gh_pr_create", role: "creator", detected_at: "2026-10-30T12:00:00.123456Z", last_seen_at: "2026-10-30T12:00:01.000000Z" };
const BRANCH: PullRequestEvidenceRow = { ...ROW, pull_request_url: null, number: null, head_ref: "lonely", tool_use_id: "p1", evidence_kind: "git_push", role: "contributor" };
const claimFor = async (sessionId: string) => (await store().claim(10)).find(c => c.scope.sessionId === sessionId)!;

test("default off: publishes arm no wakeup and the worker claims nothing; Cloud's read is pending, never empty", async () => {
  const f = await prepare(); await f.install();
  expect(await f.wakeup()).toBeUndefined();
  expect(await store().claim(10)).toEqual([]);
  const e = await f.event();
  expect(await read(e.event_id, e.generation)).toEqual({ status: "pending" });
});

test("enabled: claim captures the exact F10 source, install binds it, Cloud reads Neon-shaped rows", async () => {
  await setEnabled(true);
  const f = await prepare(); await f.install(); const e = await f.event();
  expect(await f.wakeup()).toMatchObject({ request_version: "1" });
  const c = await claimFor(f.sessionId);
  expect(c.source).toEqual({ epoch: e.source_epoch, fingerprint: e.source_fingerprint });
  expect({ exists: c.exists, blocked: c.blocked }).toEqual({ exists: true, blocked: false });
  expect(c.history).toMatchObject({ ready: true });
  expect(c.manifestKeys).toEqual([`96bulls/${org}/v1/ws=${ws}/m=srv_origin/h=claude/d=2026/11/01/s=${f.sessionId}/manifest.json`]);
  expect(await read(e.event_id, e.generation)).toEqual({ status: "pending" });
  expect(await store().install(c, PR_EVIDENCE_EXTRACTOR_VERSION, { status: "ready", rows: [ROW, BRANCH] })).toEqual({ ok: true });
  expect(await f.wakeup()).toBeUndefined();
  readySession = f.sessionId;
  const r = await read(e.event_id, e.generation);
  expect(r.status).toBe("ready"); expect(r.extractorVersion).toBe(PR_EVIDENCE_EXTRACTOR_VERSION);
  expect(r.evidence.map((x: any) => ({ ...x, detected_at: new Date(x.detected_at).toISOString(), last_seen_at: new Date(x.last_seen_at).toISOString() }))).toEqual(
    [ROW, BRANCH].map(x => ({ ...x, detected_at: new Date(x.detected_at).toISOString(), last_seen_at: new Date(x.last_seen_at).toISOString() })));
  expect(Object.keys(r.evidence[0]).sort()).toEqual(Object.keys(ROW).sort());
  // A stale generation is not this event: missing, never another session's or an older build's rows.
  expect(await read(e.event_id, String(BigInt(e.generation) - 1n))).toEqual({ status: "missing" });
});

test("a newer source between claim and install refuses the old build and keeps the wakeup for the new one", async () => {
  const f = await prepare(); await f.install();
  const c = await claimFor(f.sessionId);
  await f.replace(); const e = await f.event();
  expect(c.source.fingerprint).not.toBe(e.source_fingerprint);
  expect(await store().install(c, PR_EVIDENCE_EXTRACTOR_VERSION, { status: "ready", rows: [ROW] })).toEqual({ ok: false, code: "source_changed" });
  expect(await read(e.event_id, e.generation)).toEqual({ status: "pending" });
  const next = await claimFor(f.sessionId);
  expect(next.source.fingerprint).toBe(e.source_fingerprint);
  expect(await store().install(next, PR_EVIDENCE_EXTRACTOR_VERSION, { status: "ready", rows: [] })).toEqual({ ok: true });
  expect(await read(e.event_id, e.generation)).toMatchObject({ status: "ready", evidence: [] });
});

test("a publish mid-build keeps a due wakeup after the install; lost leases and released claims refuse", async () => {
  const f = await prepare(); await f.install();
  const c = await claimFor(f.sessionId);
  // Same source content, newer journal publish (request_version bump only).
  await db.query("UPDATE sessions.session_pr_evidence_wakeups SET request_version=request_version+1 WHERE session_id=$1", [f.sessionId]);
  expect(await store().install(c, PR_EVIDENCE_EXTRACTOR_VERSION, { status: "ready", rows: [] })).toEqual({ ok: true });
  expect(await f.wakeup()).toMatchObject({ request_version: "2", lease_token: null });
  const again = await claimFor(f.sessionId);
  expect(await store().release(again, "read_failed")).toBe(true);
  expect(await store().install(again, PR_EVIDENCE_EXTRACTOR_VERSION, { status: "ready", rows: [] })).toEqual({ ok: false, code: "lease_lost" });
  expect(await f.wakeup()).toMatchObject({ last_error: "read_failed" });
});

test("an expired lease without an install is not re-claimed before its backoff; a new publish makes it due", async () => {
  const f = await prepare(); await f.install();
  expect(await claimFor(f.sessionId)).toBeDefined();
  await db.query("UPDATE sessions.session_pr_evidence_wakeups SET lease_until=clock_timestamp()-interval'1 second' WHERE session_id=$1", [f.sessionId]);
  expect(await claimFor(f.sessionId)).toBeUndefined();
  await f.replace();
  expect(await claimFor(f.sessionId)).toBeDefined();
});

test("P1-b: a ready scope that later claims blocked is released by the worker, never wiped; install refuses it too", async () => {
  await setEnabled(true);
  const f = await prepare(); await f.install(); const e = await f.event();
  const c = await claimFor(f.sessionId);
  expect(await store().install(c, PR_EVIDENCE_EXTRACTOR_VERSION, { status: "ready", rows: [ROW] })).toEqual({ ok: true });
  // The directory row is tombstoned for a moment without the forget path (simulated: trigger disabled).
  await db.exec("ALTER TABLE sessions.session_directory DISABLE TRIGGER session_pr_evidence_forget");
  try {
    await db.query("UPDATE sessions.session_directory SET deleted_at=now() WHERE org_id=$1 AND session_id=$2", [org, f.sessionId]);
    await db.query("INSERT INTO sessions.session_pr_evidence_wakeups(org_id,workspace_id,harness,session_id) VALUES($1,$2,'claude',$3)", [org, ws, f.sessionId]);
    const blocked = await claimFor(f.sessionId);
    expect(blocked.blocked).toBe(true);
    const tick = await prEvidenceTick({ store: { ...store(), claim: async () => [blocked] }, reader: {} as never, objects: {} as never,
      prefixFor: o => `96bulls/${o}`, generation });
    expect(tick).toMatchObject({ deferred: 1, ready: 0, incomplete: 0 });
    expect((await f.wakeup()).last_error).toBe("session_blocked");
    // Even a forged install for the blocked source is refused and keeps the stored evidence.
    const again = { ...blocked, leaseToken: (await claimFor(f.sessionId).catch(() => null))?.leaseToken ?? blocked.leaseToken };
    await db.query("UPDATE sessions.session_pr_evidence_wakeups SET lease_token=$2,lease_until=now()+interval'60 seconds' WHERE session_id=$1", [f.sessionId, again.leaseToken]);
    expect(await store().install(again, PR_EVIDENCE_EXTRACTOR_VERSION, { status: "ready", rows: [] })).toMatchObject({ ok: false });
    expect((await db.query("SELECT row_count FROM sessions.session_pr_evidence_state WHERE session_id=$1", [f.sessionId])).rows).toEqual([{ row_count: 1 }]);
  } finally {
    await db.query("UPDATE sessions.session_directory SET deleted_at=NULL WHERE org_id=$1 AND session_id=$2", [org, f.sessionId]);
    await db.exec("ALTER TABLE sessions.session_directory ENABLE TRIGGER session_pr_evidence_forget");
  }
  expect(e).toBeDefined();
});

test("P1-c: history scopes install only with the attested live authority+mask; the worker reads the real mask", async () => {
  const f = await prepare(); await f.install();
  const c = await claimFor(f.sessionId);
  expect(c.history?.ready).toBe(true);
  // The real mask for this authority (05026000 fixture: one record) is readable by the worker actor.
  const page = await store().maskPage(c, generation, "0");
  expect(page).toEqual({ records: [{ recordId: "record-" + f.sessionId, winner: null }], next: null });
  // Unattested (no mask applied) or a wrong mask is refused; the correct attestation installs.
  const forged = (history: unknown) => ingest("SELECT sessions.session_pr_evidence_install_v1($1::jsonb) r", [JSON.stringify({
    scope: c.scope, requestVersion: c.requestVersion, leaseToken: c.leaseToken, source: c.source, history,
    extractorVersion: PR_EVIDENCE_EXTRACTOR_VERSION, status: "ready", reason: null, rows: [] })]);
  expect((await forged(null)).rows[0].r).toMatchObject({ ok: false, code: "source_not_installable" });
  await db.query("UPDATE sessions.session_pr_evidence_wakeups SET lease_token=$2,lease_until=now()+interval'60 seconds' WHERE session_id=$1", [f.sessionId, c.leaseToken]);
  expect((await forged({ authorityId: c.history!.authorityId, maskSha256: "0".repeat(64), maskApplied: true })).rows[0].r).toMatchObject({ ok: false });
  await db.query("UPDATE sessions.session_pr_evidence_wakeups SET lease_token=$2,lease_until=now()+interval'60 seconds' WHERE session_id=$1", [f.sessionId, c.leaseToken]);
  expect(await store().install(c, PR_EVIDENCE_EXTRACTOR_VERSION, { status: "ready", rows: [] })).toEqual({ ok: true });
});

test("P2: a user's upload reset forgets that user's sessions' evidence even when the directory row stays", async () => {
  const f = await prepare(); await f.install();
  const c = await claimFor(f.sessionId);
  await store().install(c, PR_EVIDENCE_EXTRACTOR_VERSION, { status: "ready", rows: [ROW] });
  await db.query("INSERT INTO sessions.upload_resets(org_id,workspace_id,user_id,request_id) VALUES($1,$2,$3,$4)", [org, ws, user, "reset-" + f.sessionId]);
  expect((await db.query("SELECT 1 FROM sessions.session_directory WHERE session_id=$1 AND deleted_at IS NULL", [f.sessionId])).rows).toHaveLength(1);
  expect((await db.query("SELECT 1 FROM sessions.session_pr_evidence_state WHERE session_id=$1", [f.sessionId])).rows).toHaveLength(0);
  expect((await db.query("SELECT 1 FROM sessions.session_pr_evidence WHERE session_id=$1", [f.sessionId])).rows).toHaveLength(0);
});

test("P2: a wakeup failure never fails the creator transaction", async () => {
  const f = await prepare();
  await db.exec("ALTER TABLE sessions.session_pr_evidence_wakeups ADD CONSTRAINT f3_probe_refuse CHECK (session_id <> 'never-matches' AND false) NOT VALID");
  try {
    await f.install(); // must not throw
    expect(await f.event()).toBeDefined();
    expect(await f.wakeup()).toBeUndefined();
  } finally { await db.exec("ALTER TABLE sessions.session_pr_evidence_wakeups DROP CONSTRAINT f3_probe_refuse"); }
});

test("P1-b: a wakeup lost to the trigger's handler is recovered by enqueue even when older ready evidence exists", async () => {
  await setEnabled(true);
  const f = await prepare(); await f.install();
  const v1 = await claimFor(f.sessionId);
  expect(await store().install(v1, PR_EVIDENCE_EXTRACTOR_VERSION, { status: "ready", rows: [ROW] })).toEqual({ ok: true });
  // V2 publishes while ONLY the wakeup insert fails (swallowed by the trigger's handler).
  await db.exec("ALTER TABLE sessions.session_pr_evidence_wakeups ADD CONSTRAINT f3_probe_lost CHECK (false) NOT VALID");
  try { await f.replace(); } finally { await db.exec("ALTER TABLE sessions.session_pr_evidence_wakeups DROP CONSTRAINT f3_probe_lost"); }
  const e2 = await f.event();
  expect(await f.wakeup()).toBeUndefined();
  expect(await read(e2.event_id, e2.generation)).toEqual({ status: "pending" });
  // Bounded recovery arms the stale scope (V1 state, V2 source) ...
  let after: unknown = null, found = false;
  for (let i = 0; i < 100 && !found; i++) {
    const page = (await db.query<{ r: any }>("SELECT sessions.session_pr_evidence_enqueue_v1($1::jsonb) r", [JSON.stringify({ limit: 5000, after })])).rows[0]!.r;
    found = !!(await f.wakeup()); after = page.next; if (after === null) break;
  }
  expect(await f.wakeup()).toBeDefined();
  // ... and the rebuild serves V2.
  const v2 = await claimFor(f.sessionId);
  expect(v2.source.fingerprint).toBe(e2.source_fingerprint);
  expect(await store().install(v2, PR_EVIDENCE_EXTRACTOR_VERSION, { status: "ready", rows: [BRANCH] })).toEqual({ ok: true });
  expect((await read(e2.event_id, e2.generation)).evidence.map((x: any) => x.head_ref)).toEqual(["lonely"]);
});

test("incomplete builds are served as incomplete, not as no PRs", async () => {
  const f = await prepare(); await f.install(); const e = await f.event();
  const c = await claimFor(f.sessionId);
  expect(await store().install(c, PR_EVIDENCE_EXTRACTOR_VERSION, { status: "incomplete", reason: "scope_too_large" })).toEqual({ ok: true });
  expect(await read(e.event_id, e.generation)).toEqual({ status: "incomplete", reason: "scope_too_large" });
});

test("worker tick: an unreadable source is released with a code and retried, never installed", async () => {
  await setEnabled(true);
  const f = await prepare(); await f.install();
  const result = await prEvidenceTick({ store: store(), reader: { read: async () => { throw Object.assign(new Error("x"), { code: "s3_down", retryable: true }); } } as never,
    objects: {} as never, prefixFor: o => `96bulls/${o}`, batch: 10, generation });
  expect(result.claimed).toBeGreaterThan(0); expect(result.failed).toBe(result.claimed);
  expect((await f.wakeup()).last_error).toBe("read_failed");
  expect((await db.query("SELECT 1 FROM sessions.session_pr_evidence_state WHERE session_id=$1", [f.sessionId])).rows).toHaveLength(0);
});

test("erasure purges evidence, state and wakeup in the deleting transaction", async () => {
  const f = await prepare(); await f.install();
  const c = await claimFor(f.sessionId);
  await store().install(c, PR_EVIDENCE_EXTRACTOR_VERSION, { status: "ready", rows: [ROW] });
  await db.query("INSERT INTO sessions.session_pr_evidence_wakeups(org_id,workspace_id,harness,session_id) VALUES($1,$2,'claude',$3)", [org, ws, f.sessionId]);
  await db.query("DELETE FROM sessions.session_directory WHERE org_id=$1 AND session_id=$2", [org, f.sessionId]);
  for (const t of ["session_pr_evidence", "session_pr_evidence_state", "session_pr_evidence_wakeups"])
    expect((await db.query(`SELECT 1 FROM sessions.${t} WHERE session_id=$1`, [f.sessionId])).rows).toHaveLength(0);
});

test("roles: Cloud cannot touch worker RPCs or tables, the worker cannot read through Cloud's RPC, disabled scopes refuse", async () => {
  const f = await prepare(); await f.install(); const e = await f.event();
  await expect(actor("SELECT sessions.session_pr_evidence_claim_v1('{\"limit\":1}')")).rejects.toThrow();
  await expect(actor("SELECT * FROM sessions.session_pr_evidence")).rejects.toThrow();
  await expect(actor("SELECT * FROM sessions.session_pr_evidence_state")).rejects.toThrow();
  await expect(service("SELECT sessions.session_pr_evidence_read_v1($1::jsonb)", [JSON.stringify({ schemaGeneration: generation, scope, eventId: e.event_id, generation: e.generation })])).rejects.toThrow();
  await expect(service("SELECT * FROM sessions.session_pr_evidence")).rejects.toThrow();
  await expect(service("SELECT sessions.session_pr_evidence_enqueue_v1('{\"limit\":1,\"after\":null}')")).rejects.toThrow();
  // P1-a: the shared service key cannot claim, install or release (no EXECUTE, and ingest_require_role).
  for (const fn of ["claim", "install", "release"])
    await expect(service(`SELECT sessions.session_pr_evidence_${fn}_v1('{}')`)).rejects.toThrow();
  await db.exec("GRANT EXECUTE ON FUNCTION sessions.session_pr_evidence_claim_v1(jsonb) TO service_role");
  try {
    await expect(service("SELECT sessions.session_pr_evidence_claim_v1('{\"limit\":1}')")).rejects.toMatchObject({ code: "42501" });
  } finally { await db.exec("REVOKE EXECUTE ON FUNCTION sessions.session_pr_evidence_claim_v1(jsonb) FROM service_role"); }
  await expect(ingest("SELECT sessions.session_pr_evidence_install_v1('{}')")).rejects.toThrow();
  await expect(read(e.event_id, e.generation, { ...scope, orgId: "99999999-9999-9999-9999-999999999999" })).rejects.toThrow("scope not enabled");
  await expect(read("not-a-uuid", e.generation)).rejects.toThrow();
  await expect(read(e.event_id, "0")).rejects.toThrow();
  await expect(store().claim(11)).rejects.toThrow();
  await db.exec(readFileSync(join(root, "supabase/bootstrap/canonical_project_events_verify.sql"), "utf8"));
});

test("install refuses malformed rows atomically", async () => {
  const f = await prepare(); await f.install(); const e = await f.event();
  const c = await claimFor(f.sessionId);
  await expect(store().install(c, PR_EVIDENCE_EXTRACTOR_VERSION, { status: "ready", rows: [ROW, { ...ROW, owner: "bad owner" }] })).rejects.toThrow();
  await expect(store().install(c, PR_EVIDENCE_EXTRACTOR_VERSION, { status: "ready", rows: [{ ...ROW, extra: 1 } as never] })).rejects.toThrow();
  expect(await read(e.event_id, e.generation)).toEqual({ status: "pending" });
});

test("operator backfill arms only scopes without state, bounded and resumable", async () => {
  // A scope with fresh evidence state (the reset test above forgot earlier ones, as it should).
  { const f = await prepare(); await f.install(); const c = await claimFor(f.sessionId);
    expect(await store().install(c, PR_EVIDENCE_EXTRACTOR_VERSION, { status: "ready", rows: [] })).toEqual({ ok: true }); readySession = f.sessionId; }
  const before = (await db.query<{ n: number }>("SELECT count(*)::int n FROM sessions.session_pr_evidence_wakeups")).rows[0]!.n;
  const page = (await db.query<{ r: any }>("SELECT sessions.session_pr_evidence_enqueue_v1('{\"limit\":2,\"after\":null}') r")).rows[0]!.r;
  expect(page.armed).toBeLessThanOrEqual(2); expect(page.next).not.toBeNull();
  const next = (await db.query<{ r: any }>("SELECT sessions.session_pr_evidence_enqueue_v1($1::jsonb) r", [JSON.stringify({ limit: 5000, after: page.next })])).rows[0]!.r;
  const after = (await db.query<{ n: number }>("SELECT count(*)::int n FROM sessions.session_pr_evidence_wakeups")).rows[0]!.n;
  // Arming is an upsert (an existing wakeup is re-dated), so new rows <= armed.
  expect(after - before).toBeLessThanOrEqual(page.armed + next.armed);
  // A scope that already has evidence state is not re-armed.
  expect((await db.query("SELECT 1 FROM sessions.session_pr_evidence_wakeups WHERE session_id=$1", [readySession])).rows).toHaveLength(0);
});

test("rollback refuses while evidence exists and leaves it intact", async () => {
  const n = (await db.query<{ n: number }>("SELECT count(*)::int n FROM sessions.session_pr_evidence_state")).rows[0]!.n;
  expect(n).toBeGreaterThan(0);
  await expect(db.exec(readFileSync(join(root, "supabase/rollbacks/20261105044000_pr_evidence.down.sql"), "utf8"))).rejects.toThrow(/retain evidence/);
  expect((await db.query<{ n: number }>("SELECT count(*)::int n FROM sessions.session_pr_evidence_state")).rows[0]!.n).toBe(n);
});
