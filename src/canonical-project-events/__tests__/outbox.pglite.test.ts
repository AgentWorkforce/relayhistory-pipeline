/** Actual creator -> 610 journal -> F10 outbox -> restricted RPCs on PGlite.
 * No cloud deployment or live queue/permission claim follows from this fixture.
 */
import { beforeAll, afterAll, expect, test, setDefaultTimeout } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { createHash, randomUUID } from "node:crypto";
import { canonicalProjectEventStore } from "../store.ts";
import { publishCanonicalProjectEvents } from "../publisher.ts";
setDefaultTimeout(60_000);
const root = join(import.meta.dir, "../../.."), generation = "a".repeat(64);
const org = "c4fae7b9-43a9-44e8-94d5-455aa579d67b", ws = "1220c1dc-0ce5-45fe-8b67-e7bb57a2db63", user = "44444444-4444-4444-4444-444444444444";
const scope = { orgId: org, workspaceId: ws };
let db: PGlite, serial = 0;
beforeAll(async () => {
  db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  const read = (p: string) => readFileSync(join(root, p), "utf8");
  await db.exec(read("supabase/tests/supabase_shim.sql"));
  for (const f of ["session_directory_search_worker", "session_token_verifier", "session_ingest_roles"]) await db.exec(read("supabase/bootstrap/" + f + ".sql"));
  for (const f of readdirSync(join(root, "supabase/migrations")).filter(f => f.endsWith(".sql")).sort()) await db.exec(read("supabase/migrations/" + f));
  await db.exec(read("supabase/bootstrap/canonical_project_events_verify.sql"));
  await db.exec(`CREATE TABLE sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text);
    INSERT INTO sessions.__96bulls_role_validation VALUES('session_directory_search_worker','${generation}','pass')`);
  await db.query("SELECT sessions.canonical_feature_journal_seed(9007199254740992,$1)", [generation]);
});
afterAll(async () => { await db?.close(); });
const queryAs = (role: "bulls96_session_ingest" | "bulls96_project_events" | "service_role") => async <T = Record<string, unknown>>(sql: string, args: unknown[] = []) => {
  await db.exec("BEGIN; SET LOCAL ROLE " + role);
  try { const r = await db.query<T>(sql, args as never); await db.exec("COMMIT"); return r; }
  catch (e) { await db.exec("ROLLBACK"); throw e; }
};
const ingest = queryAs("bulls96_session_ingest"), actor = queryAs("bulls96_project_events");
const rpc = async (fn: string, p: unknown) => (await ingest<{ r: any }>("SELECT sessions." + fn + "($1::jsonb) r", [JSON.stringify(p)])).rows[0]!.r;
const eventRpc = async (fn: "read" | "ack" | "pending", extra: Record<string, unknown>) => {
  const name = fn === "pending" ? "canonical_project_events_pending_v1" : `canonical_project_event_${fn}_v1`;
  return (await actor<{ r: any }>(`SELECT sessions.${name}($1::jsonb) r`, [JSON.stringify({ schemaGeneration: generation, scope, ...extra })])).rows[0]!.r;
};
async function prepare(enabled: boolean, selectedOrg = org) {
  const sessionId = `f10-${++serial}`, identity = { orgId: selectedOrg, workspaceId: ws, source: "claude", sessionId };
  if (enabled) await db.query("INSERT INTO sessions.canonical_project_event_scopes VALUES($1,$2,$3,true) ON CONFLICT(org_id,workspace_id) DO UPDATE SET enabled=true,schema_generation=excluded.schema_generation", [selectedOrg, ws, generation]);
  else await db.query("UPDATE sessions.canonical_project_event_scopes SET enabled=false WHERE org_id=$1 AND workspace_id=$2", [selectedOrg, ws]);
  const row = { org_id: selectedOrg, workspace_id: ws, user_id: user, harness: "claude", session_id: sessionId, machine_id: "srv_origin",
    created_at: "2026-11-01T00:00:00Z", last_activity_at: "2026-11-01T00:00:00Z", storage_provider: "s3", bucket: "fixture", manifest_key: `96bulls/${selectedOrg}/v1/ws=${ws}/m=srv_origin/h=claude/d=2026/11/01/s=${sessionId}/manifest.json`, content_sha256: "\\x" + "a".repeat(64), raw_format: "ndjson", revision: 1, trace_id: "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18" };
  await db.query("SELECT sessions.upsert_session_index($1::jsonb)", [JSON.stringify([row])]);
  await db.query("INSERT INTO sessions.session_directory(org_id,workspace_id,harness,session_id,created_at,last_activity_at) VALUES($1,$2,'claude',$3,now(),now())", [selectedOrg, ws, sessionId]);
  const recordIds = ["record-" + sessionId].sort((a, b) => createHash("sha256").update(a).digest("hex").localeCompare(createHash("sha256").update(b).digest("hex")));
  const mask = await rpc("ingest_history_mask_register", { generation, document: { protocol: "history-mask-v1", orgId: selectedOrg, workspaceId: ws, prefix: "", kind: "leaf", count: "1", items: recordIds.map(recordId => ({ recordId, winner: null })) } });
  const build = async () => {
    const b = await rpc("ingest_history_scope_begin", { generation, identity });
    await rpc("ingest_history_scope_build_keys", { generation, buildId: b.buildId, leaseToken: b.leaseToken, recordIds, owners: [] });
    await rpc("ingest_history_scope_capture", { generation, buildId: b.buildId, leaseToken: b.leaseToken });
    return b;
  };
  const installArgs = (b: { buildId: string; leaseToken: string }) => {
    const id = randomUUID(); return { generation, buildId: b.buildId, leaseToken: b.leaseToken, maskSha256: mask.sha256, recordCount: "1",
      derivedAnchorKey: `96bulls/${selectedOrg}/upload-generations/g=${id}/v1/ws=${ws}/m=history_scope/h=claude/d=2026/11/01/s=${sessionId}/publications/${id}/manifest.json`, bucket: "fixture", region: "us-east-1", keyRoot: "96bulls" };
  };
  const install = async () => rpc("ingest_history_scope_install", installArgs(await build()));
  const event = async () => (await db.query<any>("SELECT event_id::text,generation::text,authority_id::text,source_epoch::text,source_fingerprint,user_id,sent_at,acknowledged_at FROM sessions.canonical_project_events WHERE org_id=$1 AND workspace_id=$2 AND session_id=$3", [selectedOrg, ws, sessionId])).rows[0];
  const replace = async () => {
    // A replacement must represent new source content, not merely a new
    // authority UUID. Keep this proof independent of #230 fingerprint bugs.
    await db.query("SELECT sessions.upsert_session_index($1::jsonb)", [JSON.stringify([{ ...row, revision: 2, content_sha256: "\\x" + "b".repeat(64) }])]);
    return install();
  };
  return { identity, build, install, replace, installArgs, event };
}

test("dormant scope emits no event, selected creator publication commits an attributed ID-only outbox event", async () => {
  const off = await prepare(false); await off.install(); expect(await off.event()).toBeUndefined();
  const on = await prepare(true); const installed = await on.install(); const e = await on.event();
  expect(e.authority_id).toBe(installed.authorityId); expect(e.user_id).toBe(user); expect(BigInt(e.generation)).toBeGreaterThan(9007199254740992n);
  const resolved = await eventRpc("read", { eventId: e.event_id });
  expect(resolved.status).toBe("ready"); expect(resolved.event.identity).toEqual({ ...scope, source: "claude", sessionId: on.identity.sessionId, userId: user });
  expect(resolved.event.generation).toBe(e.generation);
});
test("rolling back the actual final creator transaction rolls back journal and outbox together", async () => {
  const f = await prepare(true), b = await f.build();
  await db.exec("BEGIN; SET LOCAL ROLE bulls96_session_ingest");
  await db.query("SELECT sessions.ingest_history_scope_install($1::jsonb)", [JSON.stringify(f.installArgs(b))]);
  await db.exec("ROLLBACK"); expect(await f.event()).toBeUndefined();
  expect((await db.query("SELECT count(*)::int n FROM sessions.canonical_feature_journal WHERE org_id=$1 AND scope->>'sessionId'=$2", [org, f.identity.sessionId])).rows[0]).toEqual({ n: 0 });
});
test("replacement coalesces one event identity; stale ACK and sent/release nonces cannot finish a newer generation", async () => {
  const f = await prepare(true); await f.install(); const first = await f.event();
  const store = canonicalProjectEventStore({ query: ingest } as never, generation, scope);
  const claimed = (await store.claim({ schemaGeneration: generation, scope, limit: 50 })).find(c => c.event.eventId === first.event_id)!;
  expect(claimed).toBeDefined(); await f.replace(); const next = await f.event();
  expect(next.event_id).toBe(first.event_id); expect(BigInt(next.generation)).toBeGreaterThan(BigInt(first.generation));
  expect(BigInt(next.source_epoch)).toBeGreaterThan(BigInt(first.source_epoch));
  expect(next.source_fingerprint).not.toBe(first.source_fingerprint);
  expect(await store.sent(claimed)).toBe(false); expect(await store.release(claimed)).toBe(false);
  expect(await eventRpc("ack", { eventId: first.event_id, generation: first.generation })).toEqual({ ok: false });
  expect((await f.event()).acknowledged_at).toBeNull();
  expect(await eventRpc("ack", { eventId: next.event_id, generation: next.generation })).toEqual({ ok: true });
});
test("real send failure releases its claims; retry sends the exact current ABI without acknowledging the source", async () => {
  await db.query("UPDATE sessions.canonical_project_events SET acknowledged_at=clock_timestamp(),lease_token=NULL,lease_until=NULL");
  const f = await prepare(true); await f.install(); const e = await f.event();
  const store = canonicalProjectEventStore({ query: ingest } as never, generation, scope);
  await expect(publishCanonicalProjectEvents({ schemaGeneration: generation, scope, store, queue: { send: async () => { throw new Error("fixture transport failure"); } } }))
    .rejects.toMatchObject({ code: "canonical_project_send_failed" });
  const sent: unknown[] = [];
  expect(await publishCanonicalProjectEvents({ schemaGeneration: generation, scope, store, queue: { send: async m => { sent.push(m); } } })).toMatchObject({ sent: 1 });
  expect(sent).toEqual([{ kind: "session.revised", eventId: e.event_id, generation: e.generation }]);
  expect((await f.event()).acknowledged_at).toBeNull();
});
test("foreign scopes, actor mismatch, direct table access and stale readiness all fail closed", async () => {
  const f = await prepare(true); await f.install(); const e = await f.event();
  const foreign = { ...scope, orgId: "99999999-9999-9999-9999-999999999999" };
  await expect(actor("SELECT sessions.canonical_project_event_read_v1($1::jsonb)", [JSON.stringify({ schemaGeneration: generation, scope: foreign, eventId: e.event_id })])).rejects.toThrow("scope not enabled");
  await expect(actor("SELECT * FROM sessions.canonical_project_events")).rejects.toThrow();
  await expect(actor("UPDATE sessions.canonical_project_event_scopes SET enabled=true")).rejects.toThrow();
  await expect(queryAs("service_role")("SELECT sessions.canonical_project_event_read_v1($1::jsonb)", [JSON.stringify({ schemaGeneration: generation, scope, eventId: e.event_id })])).rejects.toThrow();
  await db.query("UPDATE sessions.__96bulls_role_validation SET status='pending'");
  await expect(eventRpc("read", { eventId: e.event_id })).rejects.toThrow();
  await db.query("UPDATE sessions.__96bulls_role_validation SET status='pass'");
  await db.query("SELECT sessions.ingest_history_invalidate_scope($1,$2,$3,$4)", [org, ws, "claude", f.identity.sessionId]);
  expect(await eventRpc("read", { eventId: e.event_id })).toEqual({ status: "deferred" });
  expect(await eventRpc("ack", { eventId: e.event_id, generation: e.generation })).toEqual({ ok: false });
});

test("pure actor verifier rejects effective table rights and extra RPC grants", async () => {
 const verify=readFileSync(join(root,"supabase/bootstrap/canonical_project_events_verify.sql"),"utf8");
 await db.exec("GRANT SELECT ON sessions.canonical_project_events TO bulls96_project_events");
 await expect(db.exec(verify)).rejects.toThrow(/privilege/);
 await db.exec("REVOKE SELECT ON sessions.canonical_project_events FROM bulls96_project_events");
 await db.exec("GRANT EXECUTE ON FUNCTION sessions.canonical_project_events_claim_v1(jsonb) TO bulls96_project_events");
 await expect(db.exec(verify)).rejects.toThrow(/EXECUTE/);
 await db.exec("REVOKE EXECUTE ON FUNCTION sessions.canonical_project_events_claim_v1(jsonb) FROM bulls96_project_events");
 await db.exec(verify);
});

test("schema rollback refuses durable events and leaves the outbox intact", async () => {
 const before=(await db.query<{n:number}>("SELECT count(*)::int n FROM sessions.canonical_project_events")).rows[0]!.n;
 expect(before).toBeGreaterThan(0);
 await expect(db.exec(readFileSync(join(root,"supabase/rollbacks/20261105027000_canonical_project_events.down.sql"),"utf8"))).rejects.toThrow(/retain durable events/);
 expect((await db.query<{n:number}>("SELECT count(*)::int n FROM sessions.canonical_project_events")).rows[0]!.n).toBe(before);
});

test("bounded recovery rotates pending events even when the first remains unacknowledged", async () => {
 await db.query("UPDATE sessions.canonical_project_events SET acknowledged_at=clock_timestamp(),lease_token=NULL,lease_until=NULL");
 const ids:string[]=[];
 for(let i=0;i<3;i++){const f=await prepare(true);await f.install();ids.push((await f.event()).event_id);}
 const selected:string[]=[];
 for(let i=0;i<3;i++){
  const page=await eventRpc("pending",{limit:1});expect(page).toHaveLength(1);selected.push(page[0].eventId);
 }
 expect(new Set(selected).size).toBe(3);expect(selected.sort()).toEqual(ids.sort());
 expect((await db.query<{n:number}>("SELECT count(*)::int n FROM sessions.canonical_project_events WHERE acknowledged_at IS NULL")).rows[0]!.n).toBe(3);
});
