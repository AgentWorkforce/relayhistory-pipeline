// Real-PG consumer proof: exact proposal DDL, real CanonicalConsumer,
// mid-copy lease transfer -> old worker stops, durable checkpoint, takeover.
// Run: bun scripts/proof-canonical-consumer-lease.ts <pgPort>
import { Client, Pool } from "pg";
import { readFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { copyCanonicalEntry } from "../src/search-features/canonical-cycle.ts";
import { CanonicalConsumer } from "../src/search-features/canonical-consumer.ts";
import { CanonicalJournalStore } from "../src/search-features/canonical-journal.ts";
import { CanonicalPageAckStore } from "../src/search-features/canonical-ack-store.ts";

const port = Number(process.argv[2] ?? 32770);
const partitionScheme = process.env.PROOF_PARTITION_SCHEME ?? "legacy-prefix-v1";
if (partitionScheme !== "legacy-prefix-v1" && partitionScheme !== "scope-v1")
  throw new Error("refused: unknown proof partition scheme");
// Guard: only ever run against a loopback disposable fixture — real check.
if (process.env.PROOF_DISPOSABLE !== "1")
  throw new Error("refused: set PROOF_DISPOSABLE=1 on a disposable database");
if (!Number.isInteger(port) || port < 1 || port > 65535)
  throw new Error("refused: proof port must be a valid integer");
const admin = new Pool({ host: "127.0.0.1", port, user: "postgres",
  password: "synthetic-feature-only", database: "postgres" });
let failures = 0;
const check = (n: string, ok: boolean) => {
  console.log(n, ok ? "PASS" : "FAIL"); if (!ok) failures++; };

// Its own throwaway DATABASE — never touches the shared `postgres` db.
const DB = "canonical_consumer_proof";
await admin.query(`DROP DATABASE IF EXISTS ${DB}`);
await admin.query(`CREATE DATABASE ${DB}`);
const db = new Pool({ host: "127.0.0.1", port, user: "postgres",
  password: "synthetic-feature-only", database: DB });
const SCHEMA = "sessions";
await db.query(`CREATE SCHEMA ${SCHEMA}`);
for (const r of ["anon", "authenticated", "service_role",
    "bulls96_session_ingest", "bulls96_session_ingest_seeder",
    "bulls96_canonical_feature_producer"])
  await db.query(`DO $$ BEGIN IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='${r}') THEN CREATE ROLE ${r} NOLOGIN; END IF; END $$;`);
await db.query(readFileSync("scripts/fixtures/230-writer-20261105025000.sql", "utf8"));
await db.query(readFileSync("supabase/migrations/20261105030000_canonical_feature_consumer_acks.sql", "utf8"));
await db.query(readFileSync("supabase/migrations/20261105036000_canonical_consumer_lease.sql", "utf8"));

const A = "a".repeat(64), UID = "12345678-1234-1234-1234-123456789012";
const CANARY = { orgId: "10000000-0000-4000-8000-000000000001",
  workspaceId: "20000000-0000-4000-8000-000000000002" };
const SCOPE = { ...CANARY, harness: "h", sessionId: "s", generation: A };
const AUTH = { authorityId: UID, epoch: "1", fingerprint: A };
const PTR = { authorityId: UID, hash: "c".repeat(64), count: "1" };
await db.query(`CREATE TABLE sessions.ingest_history_scope_versions(authority_id uuid,org_id text,workspace_id text,harness text,session_id text,source_epoch bigint,source_fingerprint text,pointer_snapshot_sha256 text,pointer_count bigint)`);
await db.query(`INSERT INTO sessions.ingest_history_scope_versions VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`, [UID, CANARY.orgId, CANARY.workspaceId, "h", "s", 1, A, PTR.hash, 1]);
await db.query(`SELECT sessions.canonical_feature_journal_seed(100,$1)`, [A]);
await db.query(`SELECT sessions.canonical_feature_changed($1,$2,$3,$4,$5)`, [JSON.stringify(SCOPE), JSON.stringify(AUTH), null, JSON.stringify(PTR), null]);
// An out-of-scope entry journaled by the real writer -> scope-skip proved.
const OTHER = { orgId: "o2", workspaceId: "other-ws", harness: "h",
  sessionId: "s", generation: A };
await db.query(`INSERT INTO sessions.ingest_history_scope_versions VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`, [UID, "o2", "other-ws", "h", "s", 1, A, PTR.hash, 1]);
await db.query(`SELECT sessions.canonical_feature_changed($1,$2,$3,$4,$5)`, [JSON.stringify(OTHER), JSON.stringify(AUTH), null, JSON.stringify(PTR), null]);

// One dedicated Client holds SET ROLE for every runtime op — a Pool would
// rotate connections and silently drop the role.
const prod = new Client({ host: "127.0.0.1", port, user: "postgres",
  password: "synthetic-feature-only", database: DB });
await prod.connect();
await prod.query("SET ROLE bulls96_canonical_feature_producer");
const { rows: [who] } = await prod.query("SELECT current_user AS u");
check("runtime_runs_as_producer", who.u === "bulls96_canonical_feature_producer");
const sql = { query: (t: string, p?: unknown[]) => prod.query(t, p) };
const journal = new CanonicalJournalStore(sql);
const store = new CanonicalPageAckStore(sql);
const ch = { database: "d", insert: async () => {} };
const h = (k: string) => createHash("sha256").update(k).digest("hex");
const HS1 = "hs1:" + h(["history-scope-v1", CANARY.orgId, CANARY.workspaceId, "h", "s"].join("\x1f")) + ":" + A + ":" + UID;
const TS = "2026-01-01T00:00:00.000000Z";
const er = { orgId: CANARY.orgId, workspaceId: CANARY.workspaceId, source: "h", sessionId: "s", machineId: "m", kind: "session_event", eventId: "e1", ts: TS, ingestedAt: TS, userId: "u", actorName: null, actorRole: null, projectId: null, projectFolded: null, taskTitle: null, taskTitleFolded: null, content: null, contentFolded: null, embedding: null, embeddingModel: null, contentHash: null, taskRefText: "r", taskRefTextFolded: "r" };
const br = { orgId: CANARY.orgId, workspaceId: CANARY.workspaceId, source: "h", sessionId: "s", title: "t", description: "d", titleFolded: "t", descriptionFolded: "d", briefCompletedAt: TS, embedding: null, embeddingModel: null, contentHash: "h" };
const dr = { orgId: CANARY.orgId, workspaceId: CANARY.workspaceId, source: "h", sessionId: "s", title: "doc", summary: "d", titleFolded: "doc", summaryFolded: "d", projectId: null, lastActivityAt: TS };
const pg = (rows: unknown[]) =>
  createHash("sha256").update(JSON.stringify(rows)).digest("hex");
const images = { imageDescriptor: async (e: { imageKey: string }) => ({ imageKey: e.imageKey, sourceHash: A, pointerSetHash: PTR.hash, complete: true }),
  imagePages: async (e: { imageKey: string }, k: string, c: string | null) => {
    if (TRANSFER_ON_FIRST_PAGE && c === null)
      await db.query(`UPDATE sessions.canonical_feature_consumer_lease SET owner='workerB'`);
    if (c === null) { const rows = k === "event" ? [er]
        : k === "brief" ? [br] : [dr];
      return { rows, cursor: "done", source: e.imageKey,
        pointerSetHash: PTR.hash, pageSha256: pg(rows) }; }
    return { rows: [] as unknown[], cursor: null, source: e.imageKey,
      pointerSetHash: PTR.hash, pageSha256: pg([]) }; },
  priorKeyPages: async (e: { imageKey: string }) => {
    const rows: unknown[] = [];
    return { rows, cursor: null, source: e.imageKey,
      pointerSetHash: PTR.hash, pageSha256: pg(rows) }; } };

let TRANSFER_ON_FIRST_PAGE = false;
const CFG = { enabled: true, scope: CANARY, partitionScheme, targetId: "target-A", ownerId: "workerA",
  leaseTtlMs: 60000, maxRetries: 0, pageSize: 10 };
// Disabled consumer: zero queries at all (count every DB call).
let queries = 0;
const counting = { query: (t: string, p?: unknown[]) => { queries++;
  return sql.query(t, p); } };
const off = new CanonicalConsumer(counting, journal, store,
  { ...CFG, enabled: false });
const offOut = await off.drain(images, ch);
check("disabled_zero_db_queries", queries === 0);
check("disabled_zero_copied", offOut.copied === 0);
// WorkerA drains the real journal: copies rev101, scope-skips rev102.
const a = new CanonicalConsumer(counting, journal, store, CFG);
const out1 = await a.drain(images, ch);
check("first_drain_copies", out1.copied === 1);
check("scope_skip_out_of_scope", out1.skipped === 1);
check("checkpoint_at_last", out1.checkpoint === "102");
// WorkerB on the same target+partition cannot take the fresh lease.
const b = new CanonicalConsumer(counting, journal, store,
  { ...CFG, ownerId: "workerB" });
const out2 = await b.drain(images, ch);
check("fresh_lease_blocks_B", out2.copied === 0);
// MID-COPY takeover: reset, let A copy rev103 while B steals the lease
// inside imagePages — A's beforeFinish guard refuses, state stays dirty.
await db.query(`SELECT sessions.canonical_feature_changed($1,$2,$3,$4,$5)`,
  [JSON.stringify({ ...SCOPE, generation: "b".repeat(64) }),
   JSON.stringify(AUTH), null, JSON.stringify(PTR), null]);
TRANSFER_ON_FIRST_PAGE = true;
const outMid = await a.drain(images, ch);
TRANSFER_ON_FIRST_PAGE = false;
const st = await db.query(
  "SELECT dirty FROM sessions.canonical_feature_copy_state" +
  " WHERE entry_revision=(SELECT max(revision) FROM" +
  " sessions.canonical_feature_journal)");
check("mid_copy_hold_checkpoint_held",
  outMid.copied === 0 && outMid.checkpoint === "102");
check("mid_copy_durable_still_dirty", st.rows[0]?.dirty === true);
// B (the new owner) resumes and finishes rev103.
const out3 = await b.drain(images, ch);
check("takeover_B_finishes", out3.copied === 1 &&
  out3.checkpoint === "103");
// Different target => disjoint partition: fresh checkpoint, begin_drift holds.
const c2 = new CanonicalConsumer(counting, journal, store,
  { ...CFG, ownerId: "w2", targetId: "target-B" });
const out4 = await c2.drain(images, ch);
check("different_target_disjoint_partition",
  out4.copied === 0 && out4.checkpoint === "0");
// 037000 on-case: the third pool drains + the three-terminal finish runs.
await db.query(readFileSync(
  "supabase/migrations/20261105037000_session_directory_document_pool.sql",
  "utf8"));
// B still holds the canary lease; expire it so the doc-pool owner takes over.
await db.query(
  `UPDATE sessions.canonical_feature_consumer_lease` +
  ` SET heartbeat_at=now() - interval '2 hours'`);
const cDoc = new CanonicalConsumer(counting, journal, store,
  { ...CFG, ownerId: "wdoc", documentPool: true });
const outDoc = await cDoc.drain(images, ch);
const docAcks = await db.query(
  "SELECT count(*)::int n FROM sessions.canonical_feature_page_ack" +
  " WHERE pool='document'");
check("document_pool_drains_and_finishes",
  outDoc.copied === 0 && outDoc.checkpoint === "103" &&
  docAcks.rows[0]?.n === 0);
// A doc-pool cycle on a FRESH revision copies + acks + finishes all three.
const DOCSCOPE = { ...SCOPE, generation: "d".repeat(64) };
await db.query(`INSERT INTO sessions.ingest_history_scope_versions VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`, [UID, CANARY.orgId, CANARY.workspaceId, "h", "doc-scope", 2, A, PTR.hash, 1]);
await db.query(`SELECT sessions.canonical_feature_changed($1,$2,$3,$4,$5)`, [JSON.stringify({ ...DOCSCOPE, sessionId: "doc-scope" }), JSON.stringify({ ...AUTH, epoch: "2" }), null, JSON.stringify(PTR), null]);
const outDoc2 = await cDoc.drain(images, ch);
const docAcks2 = await db.query(
  "SELECT count(*)::int n FROM sessions.canonical_feature_page_ack" +
  " WHERE pool='document'");
check("document_pool_copies_third_pool",
  docAcks2.rows[0]?.n > 0);
// Producer-role ACL: dead-letter is insert-select only.
const del = await prod.query(
  `DELETE FROM sessions.canonical_feature_consumer_dead_letter`)
  .catch((e: { code?: string }) => e);
check("producer_dead_letter_delete_denied",
  (del as { code?: string }).code === "42501");
await prod.end();
await db.end();
await admin.query(`DROP DATABASE IF EXISTS ${DB}`);
await admin.end();
if (failures) { console.log(`${failures} FAILURES`); process.exit(1); }
console.log("ALL PASS");
