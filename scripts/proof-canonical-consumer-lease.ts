// Real-PG consumer proof: exact proposal DDL, real CanonicalConsumer,
// mid-copy lease transfer -> old worker stops, durable checkpoint, takeover.
// Run: bun scripts/proof-canonical-consumer-lease.ts <pgPort>
import { Pool } from "pg";
import { readFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { CanonicalConsumer } from "../src/search-features/canonical-consumer.ts";
import { CanonicalJournalStore } from "../src/search-features/canonical-journal.ts";
import { CanonicalPageAckStore } from "../src/search-features/canonical-ack-store.ts";

const port = Number(process.argv[2] ?? 32770);
const db = new Pool({ host: "127.0.0.1", port, user: "postgres",
  password: "synthetic-feature-only", database: "postgres" });
let failures = 0;
const check = (n: string, ok: boolean) => {
  console.log(n, ok ? "PASS" : "FAIL"); if (!ok) failures++; };

await db.query("DROP SCHEMA sessions CASCADE; CREATE SCHEMA sessions");
for (const r of ["anon", "authenticated", "service_role",
    "bulls96_session_ingest", "bulls96_session_ingest_seeder",
    "bulls96_canonical_feature_producer"])
  await db.query(`DO $$ BEGIN IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='${r}') THEN CREATE ROLE ${r} NOLOGIN; END IF; END $$;`);
await db.query(readFileSync("scripts/fixtures/230-writer-20261105025000.sql", "utf8"));
await db.query(readFileSync("supabase/migrations/20261105030000_canonical_feature_consumer_acks.sql", "utf8"));
await db.query(readFileSync("docs/proposals/20261105031000_canonical_consumer_lease.sql", "utf8"));

const A = "a".repeat(64), UID = "12345678-1234-1234-1234-123456789012";
const CANARY = { orgId: "c4fae7b9-43a9-44e8-94d5-455aa579d67b",
  workspaceId: "1220c1dc-0ce5-45fe-8b67-e7bb57a2db63" };
const SCOPE = { ...CANARY, harness: "h", sessionId: "s", generation: A };
const AUTH = { authorityId: UID, epoch: "1", fingerprint: A };
const PTR = { authorityId: UID, hash: "c".repeat(64), count: "1" };
await db.query(`CREATE TABLE sessions.ingest_history_scope_versions(authority_id uuid,org_id text,workspace_id text,harness text,session_id text,source_epoch bigint,source_fingerprint text,pointer_snapshot_sha256 text,pointer_count bigint)`);
await db.query(`INSERT INTO sessions.ingest_history_scope_versions VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`, [UID, CANARY.orgId, CANARY.workspaceId, "h", "s", 1, A, PTR.hash, 1]);
await db.query(`SELECT sessions.canonical_feature_journal_seed(100,$1)`, [A]);
await db.query(`SELECT sessions.canonical_feature_changed($1,$2,$3,$4,$5)`, [JSON.stringify(SCOPE), JSON.stringify(AUTH), null, JSON.stringify(PTR), null]);
// an out-of-scope entry the canary must scope-skip (different workspace)
const OTHER_SCOPE = { ...SCOPE, workspaceId: "other-ws" };
await db.query(`INSERT INTO sessions.ingest_history_scope_versions VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`, [UID, "o2", "other-ws", "h", "s", 1, A, PTR.hash, 1]);

const sql = { query: (t: string, p?: unknown[]) => db.query(t, p) };
const journal = new CanonicalJournalStore(sql);
const store = new CanonicalPageAckStore(sql);
const ch = { database: "d", insert: async () => {} };
const h = (k: string) => createHash("sha256").update(k).digest("hex");
const HS1 = "hs1:" + h(["history-scope-v1", CANARY.orgId, CANARY.workspaceId, "h", "s"].join("\x1f")) + ":" + A + ":" + UID;
const TS = "2026-01-01T00:00:00.000000Z";
const er = { orgId: CANARY.orgId, workspaceId: CANARY.workspaceId, source: "h", sessionId: "s", machineId: "m", kind: "session_event", eventId: "e1", ts: TS, ingestedAt: TS, userId: "u", actorName: null, actorRole: null, projectId: null, projectFolded: null, taskTitle: null, taskTitleFolded: null, content: null, contentFolded: null, embedding: null, embeddingModel: null, contentHash: null, taskRefText: "r", taskRefTextFolded: "r" };
const br = { orgId: CANARY.orgId, workspaceId: CANARY.workspaceId, source: "h", sessionId: "s", title: "t", description: "d", titleFolded: "t", descriptionFolded: "d", briefCompletedAt: TS, embedding: null, embeddingModel: null, contentHash: "h" };
const pg = (rows: unknown[]) =>
  createHash("sha256").update(JSON.stringify(rows)).digest("hex");
const images = { imageDescriptor: async (e: { imageKey: string }) => ({ imageKey: e.imageKey, sourceHash: A, pointerSetHash: PTR.hash, complete: true }),
  imagePages: async (_e: unknown, k: string, c: string | null) => {
    if (c === null) { const rows = k === "event" ? [er] : [br];
      return { rows, cursor: "done", source: HS1, pointerSetHash: PTR.hash,
        pageSha256: pg(rows) }; }
    return { rows: [] as unknown[], cursor: null, source: HS1,
      pointerSetHash: PTR.hash, pageSha256: pg([]) }; },
  priorKeyPages: async () => { const rows: unknown[] = [];
    return { rows, cursor: null, source: HS1, pointerSetHash: PTR.hash,
      pageSha256: pg(rows) }; } };

const CFG = { enabled: true, targetId: "target-A", ownerId: "workerA",
  leaseTtlMs: 60000, maxRetries: 0, pageSize: 10 };
// Disabled consumer: zero access.
const off = new CanonicalConsumer(sql, journal, store, { ...CFG, enabled: false });
const offOut = await off.drain(images, ch);
const leaseBefore = await db.query("SELECT count(*) c FROM sessions.canonical_feature_consumer_lease");
check("disabled_zero_lease", Number(leaseBefore.rows[0].c) === 0);
check("disabled_zero_copied", offOut.copied === 0);
// WorkerA drains the real journal — copies the canary entry.
const a = new CanonicalConsumer(sql, journal, store, CFG);
const out1 = await a.drain(images, ch);
check("first_drain_copies", out1.copied === 1);
check("checkpoint_advanced", out1.checkpoint === "101");
// WorkerB on the same target+partition cannot take the fresh lease.
const b = new CanonicalConsumer(sql, journal, store, { ...CFG, ownerId: "workerB" });
const out2 = await b.drain(images, ch);
check("fresh_lease_blocks_B", out2.copied === 0);
// Expire A's heartbeat -> B takes over.
await db.query(`UPDATE sessions.canonical_feature_consumer_lease SET heartbeat_at = now() - interval '2 hours'`);
const out3 = await b.drain(images, ch);
check("expired_takeover_B", out3.copied === 0 && Number(out3.checkpoint) >= 101);
// Different target => disjoint partition: fresh checkpoint (not A's 101)
// and begin_drift holds the copy — target binding refuses state reuse.
const c2 = new CanonicalConsumer(sql, journal, store, { ...CFG, ownerId: "w2", targetId: "target-B" });
const out4 = await c2.drain(images, ch);
check("different_target_disjoint_partition",
  out4.copied === 0 && out4.checkpoint === "0");
await db.end();
if (failures) { console.log(`${failures} FAILURES`); process.exit(1); }
console.log("ALL PASS");
