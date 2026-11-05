import { beforeAll, afterAll, expect, test, setDefaultTimeout } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";

// Minimal SQL-only mask builder (history-mask-v1): leaves sorted by sha256(recordId); a 16-way
// root node above 500 records. Mirrors ingest_history_mask_register's own validation.
import { createHash } from "node:crypto";
const shaHex = (s: string) => createHash("sha256").update(s).digest("hex");
async function registerMask(reg: (doc: unknown) => Promise<any>, orgId: string, workspaceId: string, recordIds: string[]) {
  const sorted = [...recordIds].sort((a, b) => (shaHex(a) < shaHex(b) ? -1 : 1));
  const leaf = (prefix: string, ids: string[]) => reg({ protocol: "history-mask-v1", orgId, workspaceId, prefix, kind: "leaf", count: String(ids.length), items: ids.map((recordId) => ({ recordId, winner: null })) });
  if (sorted.length <= 500) { const r = await leaf("", sorted); return { sha256: r.sha256 as string, count: String(sorted.length) }; }
  const items: { digit: string; sha256: string }[] = [];
  for (const d of "0123456789abcdef") { const ids = sorted.filter((x) => shaHex(x)[0] === d); if (ids.length) items.push({ digit: d, sha256: (await leaf(d, ids)).sha256 }); }
  const r = await reg({ protocol: "history-mask-v1", orgId, workspaceId, prefix: "", kind: "node", count: String(sorted.length), items });
  return { sha256: r.sha256 as string, count: String(sorted.length) };
}
setDefaultTimeout(60000);
// 620 must be invisible to the live upload ACK and control paths while dormant, and only ever touch
// MANAGED scopes (installed or with an in-flight build) once active. The writes below are exactly the
// statement-level writes ingest_lazy_ack_batch / slot initialization make on ingest_lazy_native_slots.
const root = join(import.meta.dir, "../../.."), gen = "a".repeat(64);
let db: PGlite;
beforeAll(async () => {
  db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  const read = (p: string) => readFileSync(join(root, p), "utf8");
  await db.exec(read("supabase/tests/supabase_shim.sql"));
  for (const f of ["session_directory_search_worker", "session_token_verifier", "session_ingest_roles"]) await db.exec(read("supabase/bootstrap/" + f + ".sql"));
  for (const f of readdirSync(join(root, "supabase/migrations")).filter((f) => f.endsWith(".sql")).sort()) await db.exec(read("supabase/migrations/" + f));
  await db.exec("CREATE TABLE sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text);INSERT INTO sessions.__96bulls_role_validation VALUES('session_directory_search_worker','" + gen + "','pass')");
  // Operator seed above the drained legacy feature clock (H=1000): the journal refuses until it exists.
  await db.query("SELECT sessions.canonical_feature_journal_seed(1000,$1)", [gen]);
});
afterAll(async () => { await db?.close(); });
const run: any = { query: async (sql: string, args: unknown[]) => {
  await db.exec("BEGIN; SET LOCAL ROLE bulls96_session_ingest");
  try { const r = await db.query(sql, args as never); await db.exec("COMMIT"); return r; } catch (e) { await db.exec("ROLLBACK"); throw e; }
} };
const rpc = async (fn: string, p: unknown) => (await run.query("SELECT sessions." + fn + "($1::jsonb) r", [JSON.stringify(p)])).rows[0].r;
let n = 0;
async function scope(records: string[]) {
  const i = ++n, org = "creator-org-" + i, ws = "creator-ws", identity = { orgId: org, workspaceId: ws, source: "claude", sessionId: "creator-session-" + i };
  const row = { org_id: org, workspace_id: ws, user_id: "creator-user", harness: "claude", session_id: identity.sessionId, machine_id: "srv_origin", created_at: "2026-11-01T00:00:00Z", last_activity_at: "2026-11-01T00:00:00Z", storage_provider: "s3", bucket: "creator-fixture", manifest_key: "96bulls/" + org + "/v1/ws=" + ws + "/m=srv_origin/h=claude/d=2026/11/01/s=" + identity.sessionId + "/manifest.json", content_sha256: "\\x" + "a".repeat(64), raw_format: "ndjson", revision: 1, trace_id: "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18" };
  await db.query("SELECT sessions.upsert_session_index($1::jsonb)", [JSON.stringify([row])]);
  await db.query("INSERT INTO sessions.session_directory(org_id,workspace_id,harness,session_id,created_at,last_activity_at) VALUES($1,$2,$3,$4,now(),now())", [org, ws, "claude", identity.sessionId]);
  const mask = await registerMask((doc) => rpc("ingest_history_mask_register", { generation: gen, document: doc }), org, ws, records);
  const anchor = (g: string) => "96bulls/" + org + "/upload-generations/g=" + g + "/v1/ws=" + ws + "/m=history_scope/h=claude/d=2026/11/01/s=" + identity.sessionId + "/publications/" + g + "/manifest.json";
  return { identity, mask, anchor, row };
}
const install = (b: any, s: Awaited<ReturnType<typeof scope>>, g = "12345678-1234-1234-1234-123456789abc") => rpc("ingest_history_scope_install", {
  generation: gen, buildId: b.buildId, leaseToken: b.leaseToken, maskSha256: s.mask.sha256, recordCount: s.mask.count,
  derivedAnchorKey: s.anchor(g), bucket: "creator-fixture", region: "us-east-1", keyRoot: "96bulls" });
async function build(s: Awaited<ReturnType<typeof scope>>, keys: string[]) {
  const b = await rpc("ingest_history_scope_begin", { generation: gen, identity: s.identity });
  for (let i = 0; i < keys.length; i += 500) await rpc("ingest_history_scope_build_keys", { generation: gen, buildId: b.buildId, leaseToken: b.leaseToken, recordIds: keys.slice(i, i + 500), owners: [] });
  const c = await rpc("ingest_history_scope_capture", { generation: gen, buildId: b.buildId, leaseToken: b.leaseToken });
  return { b, c };
}


const hex = (s: string) => shaHex(s);
const SUMMARY = "UPDATE sessions.session_directory SET short_summary=$5,short_summary_scrub_ruleset='relayhistory-scrub',short_summary_scrub_version=1,short_summary_source='bob-costas/extractive/v1',short_summary_generated_at=now() WHERE org_id=$1 AND workspace_id=$2 AND harness=$3 AND session_id=$4";
const summarize = (s: any, text: string) => db.query(SUMMARY, [s.identity.orgId, s.identity.workspaceId, "claude", s.identity.sessionId, text]);
const dir = async (s: any) => (await db.query<any>("SELECT state,short_summary,short_summary_scrub_ruleset r,short_summary_scrub_version v,short_summary_source src,short_summary_generated_at g FROM sessions.session_directory WHERE org_id=$1 AND session_id=$2", [s.identity.orgId, s.identity.sessionId])).rows[0];
// Initialize a native slot for record `rec`, then ACK it to the scope's session (owner u1), in one statement each.
async function ack(s: any, rec: string, revision = 1) {
  const key = hex("slot:" + s.identity.orgId + ":" + rec);
  await db.query("INSERT INTO sessions.ingest_lazy_native_slots(key_sha256,key_material,org_id,workspace_id,origin_id,record_id,source_identity_sha256,control_epoch,initialized_witness_sha256) VALUES($1,$2::jsonb,$3,$4,'srv_origin',$5,$6,$7,$8) ON CONFLICT DO NOTHING",
    [key, JSON.stringify(["global-native-custody-v1", s.identity.orgId, s.identity.workspaceId, "srv_origin", rec]), s.identity.orgId, s.identity.workspaceId, rec, hex("src"), hex("ctl"), hex("wit")]);
  await db.query("UPDATE sessions.ingest_lazy_native_slots SET lineage=lineage+1,current_owner='u1',current_source='claude',current_session=$2,current_revision=$3,current_digest=$4,current_reset_epoch=0,current_fence_epoch=0 WHERE key_sha256=$1",
    [key, s.identity.sessionId, revision, hex("digest:" + revision)]);
}
const counts = async () => (await db.query<any>("SELECT (SELECT count(*)::int FROM sessions.ingest_history_reconcile_outbox) o,(SELECT count(*)::int FROM sessions.ingest_history_required_scopes) q,(SELECT count(*)::int FROM sessions.ingest_history_privacy_outbox) p")).rows[0];

test("dormant: an ACK on a session WITH a summary succeeds and changes nothing 620-owned", async () => {
  const s = await scope(["k1"]);
  await summarize(s, "a legacy summary");
  await ack(s, "k1");
  await ack(s, "k1", 2);
  expect(await dir(s)).toMatchObject({ short_summary: "a legacy summary", r: "relayhistory-scrub", v: 1, src: "bob-costas/extractive/v1" });
  expect(await counts()).toEqual({ o: 0, q: 0, p: 0 });
  await summarize(s, "the legacy summarizer still writes");              // the BEFORE trigger is inert
  expect((await dir(s)).short_summary).toBe("the legacy summarizer still writes");
});

test("dormant: the named operational rollback runs after live ACK activity", async () => {
  const s = await scope(["k1"]);
  await ack(s, "k1");
  const down = readFileSync(join(root, "supabase/rollbacks/" + readdirSync(join(root, "supabase/rollbacks")).find((f) => f.includes("canonical_history_lineage"))!), "utf8");
  await db.exec("BEGIN");
  try { await db.exec(down); } finally { await db.exec("ROLLBACK"); }
});

test("active: an unmanaged session keeps its summary through ACKs and summarizer writes", async () => {
  const managed = await scope(["m1"]);
  await install((await build(managed, ["m1"])).b, managed);              // 620 now active somewhere
  const s = await scope(["k1"]);
  await summarize(s, "untouched");
  await ack(s, "k1");
  expect(await dir(s)).toMatchObject({ short_summary: "untouched", r: "relayhistory-scrub" });
  await summarize(s, "rewritten");
  expect((await dir(s)).short_summary).toBe("rewritten");
  expect((await db.query<any>("SELECT count(*)::int n FROM sessions.ingest_history_required_scopes WHERE org_id=$1", [s.identity.orgId])).rows[0].n).toBe(0);
});

test("managed: an ACK invalidates the installed scope and clears the summary WITH all four provenance columns", async () => {
  const s = await scope(["k1"]);
  await summarize(s, "summary before install");
  await install((await build(s, ["k1"])).b, s);
  await ack(s, "k1");
  expect(await dir(s)).toEqual({ state: "unknown", short_summary: null, r: null, v: null, src: null, g: null });
  expect((await db.query<any>("SELECT ready FROM sessions.ingest_history_scope_current WHERE org_id=$1", [s.identity.orgId])).rows[0].ready).toBe(false);
  expect((await db.query<any>("SELECT count(*)::int n FROM sessions.ingest_history_reconcile_outbox o JOIN sessions.ingest_lazy_native_slots n ON n.key_sha256=o.native_key_sha256 WHERE n.org_id=$1", [s.identity.orgId])).rows[0].n).toBe(1);
  // A later summarizer write on a managed scope is neutralized consistently (no CHECK violation).
  await summarize(s, "legacy write after management");
  expect(await dir(s)).toMatchObject({ short_summary: null, r: null, v: null, src: null, g: null });
});

test("managed: an in-flight build is fenced by an ACK that changes a native winner", async () => {
  const s = await scope(["k1"]);
  const { b } = await build(s, ["k1"]);
  await ack(s, "k1");
  await expect(install(b, s)).rejects.toThrow("scope source changed since capture");
});
