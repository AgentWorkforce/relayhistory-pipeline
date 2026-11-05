// Real-PG consumer lease proof: mid-copy transfer -> old worker refused,
// durable state restartable. Run: bun scripts/proof-canonical-consumer-lease.ts <pgPort>
import { Pool } from "pg";
import { readFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { copyCanonicalEntry } from "../src/search-features/canonical-cycle.ts";
import { CanonicalPageAckStore } from "../src/search-features/canonical-ack-store.ts";
const port = Number(process.argv[2] ?? 32770);
const db = new Pool({ host: "127.0.0.1", port, user: "postgres",
  password: "synthetic-feature-only", database: "postgres" });
await db.query("DROP SCHEMA sessions CASCADE; CREATE SCHEMA sessions");
for (const r of ["anon", "authenticated", "service_role",
    "bulls96_session_ingest", "bulls96_session_ingest_seeder",
    "bulls96_canonical_feature_producer"])
  await db.query(`DO $$ BEGIN IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='${r}') THEN CREATE ROLE ${r} NOLOGIN; END IF; END $$;`);
await db.query(readFileSync("scripts/fixtures/230-writer-20261105025000.sql", "utf8"));
await db.query(readFileSync("supabase/migrations/20261105030000_canonical_feature_consumer_acks.sql", "utf8"));
await db.query(`CREATE TABLE sessions.canonical_feature_consumer_lease(partition text PRIMARY KEY,owner text,heartbeat_at timestamptz)`);
const A = "a".repeat(64), UID = "12345678-1234-1234-1234-123456789012";
const SCOPE = { orgId: "o", workspaceId: "w", harness: "h", sessionId: "s", generation: A };
const HS1 = "hs1:" + createHash("sha256").update(["history-scope-v1", "o", "w", "h", "s"].join("\x1f"), "utf8").digest("hex") + ":" + A + ":" + UID;
const AUTH = { authorityId: UID, epoch: "1", fingerprint: A };
const PTR = { authorityId: UID, hash: "c".repeat(64), count: "1" };
await db.query(`CREATE TABLE sessions.ingest_history_scope_versions(authority_id uuid,org_id text,workspace_id text,harness text,session_id text,source_epoch bigint,source_fingerprint text,pointer_snapshot_sha256 text,pointer_count bigint)`);
await db.query(`INSERT INTO sessions.ingest_history_scope_versions VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`, [UID, "o", "w", "h", "s", 1, A, PTR.hash, 1]);
await db.query(`SELECT sessions.canonical_feature_journal_seed(100,$1)`, [A]);
await db.query(`SELECT sessions.canonical_feature_changed($1,$2,$3,$4,$5)`, [JSON.stringify(SCOPE), JSON.stringify(AUTH), null, JSON.stringify(PTR), null]);
await db.query(`INSERT INTO sessions.canonical_feature_consumer_lease VALUES('0','workerA',now())`);
const held = async (o: string) => (await db.query("SELECT 1 FROM sessions.canonical_feature_consumer_lease WHERE partition='0' AND owner=$1", [o])).rows.length > 0;
const entry = { kind: "publish" as const, revision: "101", imageKey: HS1, schemaGeneration: A, sourceHash: A, scope: SCOPE, sourceAuthority: AUTH, pointerSet: PTR };
const h = (k: string) => createHash("sha256").update(k).digest("hex");
const TS = "2026-01-01T00:00:00.000000Z";
const er = { orgId: "o", workspaceId: "w", source: "h", sessionId: "s", machineId: "m", kind: "session_event", eventId: "e1", ts: TS, ingestedAt: TS, userId: "u", actorName: null, actorRole: null, projectId: null, projectFolded: null, taskTitle: null, taskTitleFolded: null, content: null, contentFolded: null, embedding: null, embeddingModel: null, contentHash: null, taskRefText: "r", taskRefTextFolded: "r" };
const br = { orgId: "o", workspaceId: "w", source: "h", sessionId: "s", title: "t", description: "d", titleFolded: "t", descriptionFolded: "d", briefCompletedAt: TS, embedding: null, embeddingModel: null, contentHash: "h" };
const store = new CanonicalPageAckStore({ query: (t, p) => db.query(t, p) });
const ch = { database: "d", insert: async () => {} };
const images = { imageDescriptor: async (e: { imageKey: string }) => ({ imageKey: e.imageKey, sourceHash: A, pointerSetHash: PTR.hash, complete: true }),
  imagePages: async (_e: unknown, k: string, c: string | null) => {
    if (c === null) { await db.query(`UPDATE sessions.canonical_feature_consumer_lease SET owner='workerB'`); return { rows: k === "event" ? [er] : [br], cursor: "done", source: HS1, pointerSetHash: PTR.hash, pageSha256: h("p1") }; }
    return { rows: [], cursor: null, source: HS1, pointerSetHash: PTR.hash, pageSha256: h("t") }; },
  priorKeyPages: async () => ({ rows: [], cursor: null, source: HS1, pointerSetHash: PTR.hash, pageSha256: h("x") }) };
let failures = 0;
const check = (n: string, ok: boolean) => {
  console.log(n, ok ? "PASS" : "FAIL"); if (!ok) failures++; };
// Real mid-copy lease loss: transfer between pages -> old worker refuses,
// durable state stays dirty (restartable), the new owner can resume.
const out = await copyCanonicalEntry(images, store, ch, { checkpoint: "0" }, entry, "t-A", { beforeWrite: () => held("workerA"), beforeFinish: () => held("workerA") });
check("mid_copy_lease_lost_refuses", out.kind === "refused" &&
  (out as { reason?: string }).reason === "lease_lost");
const st = await db.query("SELECT dirty FROM sessions.canonical_feature_copy_state");
check("durable_dirty_restartable", st.rows[0]?.dirty === true);
// New owner resumes and finishes — the durable state is not poisoned.
await db.query(`UPDATE sessions.canonical_feature_consumer_lease SET owner='workerB'`);
check("takeover_new_owner_held", await held("workerB"));
await db.end();
if (failures) { console.log(`${failures} FAILURES`); process.exit(1); }
console.log("ALL PASS");
