// Assertion-driven real-PG proof: full composed DDL + real store +
// copyCanonicalEntry. Exits nonzero on any failure.
import { Pool } from "pg";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { CanonicalPageAckStore } from "../src/search-features/canonical-ack-store.ts";
import { copyCanonicalEntry } from "../src/search-features/canonical-cycle.ts";
const port = Number(process.argv[2] ?? 32770);
if (port !== 32770) throw new Error("owned PG17 fixture port only");
const SCHEMA = "sessions_proof";           // dedicated, never 'sessions'
const db = new Pool({ host: "127.0.0.1", port, user: "postgres",
  password: "synthetic-feature-only", database: "postgres" });
let failures = 0;
const check = (name: string, cond: boolean) => {
  console.log(name, cond ? "PASS" : "FAIL");
  if (!cond) failures++;
};
await db.query(`DROP SCHEMA IF EXISTS ${SCHEMA} CASCADE; CREATE SCHEMA ${SCHEMA}`);
// The real DDL ACL names these roles — create each idempotently so the
// grants apply UNMODIFIED on a bare PG17 (no blanket error swallowing).
for (const role of ["anon", "authenticated", "service_role",
    "bulls96_session_ingest", "bulls96_session_ingest_seeder",
    "bulls96_canonical_feature_producer"])
  await db.query(
    `DO $$ BEGIN IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='${role}')` +
    ` THEN CREATE ROLE ${role} NOLOGIN; END IF; END $$;`);
// Apply the REAL merged #230 025000 writer migration (vendored snapshot at
// 233c62b730251eb1bcad096b16f75cb8e67df6a1) + the companion state DDL.
// Self-contained: read the vendored fixture, no shell/out-of-tree paths.
const writer = readFileSync(
  "scripts/fixtures/230-writer-20261105025000.sql", "utf8")
  .replaceAll("sessions.", `${SCHEMA}.`)
  .replaceAll("SCHEMA sessions", `SCHEMA ${SCHEMA}`)
  .replaceAll("GRANT USAGE ON SCHEMA", `GRANT USAGE ON SCHEMA`);
await db.query(writer);
const ddl = readFileSync(
  "supabase/migrations/20261105030000_canonical_feature_consumer_acks.sql", "utf8")
  .replaceAll("sessions.", `${SCHEMA}.`);
// Apply only the companion state + RPC parts (the writer owns the journal).
const companion = ddl.match(
  /-- Companion producer-side state[\s\S]*$/)?.[0] ?? "";
await db.query(companion);
const A = "a".repeat(64), UID = "12345678-1234-1234-1234-123456789012";
const SCOPE = { orgId: "o", workspaceId: "w", harness: "h", sessionId: "s",
  generation: A };
import { canonicalImageKey } from "../src/search-features/canonical-image-key.ts";
const HS1 = canonicalImageKey(SCOPE, UID);
const AUTH = { authorityId: UID, epoch: "1", fingerprint: A };
const PTR = { authorityId: UID, hash: "c".repeat(64), count: "1" };
// The writer verifies each descriptor against the frozen authority — stub
// the lineage table with the matching row.
await db.query(`CREATE TABLE ${SCHEMA}.ingest_history_scope_versions(
  authority_id uuid, org_id text, workspace_id text, harness text,
  session_id text, source_epoch bigint, source_fingerprint text,
  pointer_snapshot_sha256 text, pointer_count bigint)`);
await db.query(`INSERT INTO ${SCHEMA}.ingest_history_scope_versions VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
  [UID, SCOPE.orgId, SCOPE.workspaceId, SCOPE.harness, SCOPE.sessionId,
   Number(AUTH.epoch), AUTH.fingerprint, PTR.hash, Number(PTR.count)]);
// The REAL #230 writer produces the journal row — seed then publish via the
// SQL function, never a direct INSERT. The decoder reads what it wrote.
await db.query(`SELECT ${SCHEMA}.canonical_feature_journal_seed(100,$1)`, [A]);
await db.query(`SELECT ${SCHEMA}.canonical_feature_changed($1,$2,$3,$4,$5)`,
  [JSON.stringify(SCOPE), JSON.stringify(AUTH), null,
   JSON.stringify(PTR), null]);
// All consumer operations below use the actual restricted producer identity,
// not the migration owner. Prove its positive and negative privilege boundary
// before exercising the store.
const producer = await db.connect();
await producer.query(`SET ROLE bulls96_canonical_feature_producer`);
const privileges = await producer.query(`SELECT
  has_table_privilege(current_user,$1,'SELECT') AS journal_read,
  has_table_privilege(current_user,$1,'INSERT') AS journal_write,
  has_table_privilege(current_user,$2,'INSERT,SELECT') AS ack_rw,
  has_table_privilege(current_user,$2,'UPDATE,DELETE') AS ack_mutate,
  has_table_privilege(current_user,$3,'INSERT,SELECT') AS state_rw,
  has_table_privilege(current_user,$3,'UPDATE,DELETE') AS state_mutate,
  has_function_privilege(current_user,$4,'EXECUTE') AS writer_exec,
  has_function_privilege(current_user,$5,'EXECUTE') AS finish_exec`, [
  `${SCHEMA}.canonical_feature_journal`,
  `${SCHEMA}.canonical_feature_page_ack`,
  `${SCHEMA}.canonical_feature_copy_state`,
  `${SCHEMA}.canonical_feature_changed(jsonb,jsonb,jsonb,jsonb,jsonb)`,
  `${SCHEMA}.finish_canonical_copy(bigint,jsonb,text,text,text,text)`,
]);
const pv = privileges.rows[0]!;
check("producer_acl",
  pv.journal_read === true && pv.journal_write === false &&
  pv.ack_rw === true && pv.ack_mutate === false &&
  pv.state_rw === true && pv.state_mutate === false &&
  pv.writer_exec === false && pv.finish_exec === true);
import { CanonicalJournalStore } from "../src/search-features/canonical-journal.ts";
const journal = new CanonicalJournalStore({ query: (t, p) =>
  producer.query(t.replaceAll("sessions.", `${SCHEMA}.`), p) });
const { entries } = await journal.entries("100", 10);
const entry = entries[0]!;
if (!entry) throw new Error("journal_decoder_empty");
console.log("decoded", entry.kind, entry.revision, entry.imageKey.slice(0,30));
// Real store over the composed schema (table names are sessions.* in the
// store — alias it via search_path).
const store = new CanonicalPageAckStore({ query: (t, p) =>
  producer.query(t.replaceAll("sessions.", `${SCHEMA}.`), p) });
// A crash after one nonterminal page resumes at the durable next edge. It is
// not mislabeled as a corrupt dangling chain.
await store.begin(entry, "partial-profile", "partial-target");
await store.ackPage({ entry, kind: "event", cursor: "start",
  nextCursor: "page-2", pageSha256: "1".repeat(64) });
const partial = await store.resumeCursor(entry, "event");
check("partial_chain_resumes",
  partial.state === "resume" && partial.cursor === "page-2");
await db.query(`DELETE FROM ${SCHEMA}.canonical_feature_page_ack;
  DELETE FROM ${SCHEMA}.canonical_feature_copy_state`);
// Fake CH that counts writes + a minimal image source.
let chWrites = 0;
const ch = { database: "test_ch", insert: async () => { chWrites++; },
  select: async () => [] };
const page = (rows: unknown[], cursor: string | null) => ({
  rows, cursor, source: HS1, pointerSetHash: PTR.hash,
  pageSha256: createHash("sha256").update(JSON.stringify(rows)).digest("hex"),
});
const TS = "2026-01-01T00:00:00.000000Z";
const eventRow = { orgId: "o", workspaceId: "w", source: "h",
  sessionId: "s", machineId: "m", kind: "session_event", eventId: "e1",
  ts: TS, ingestedAt: TS, userId: "u",
  actorName: null, actorRole: null, projectId: null, projectFolded: null,
  taskTitle: null, taskTitleFolded: null, content: null, contentFolded: null,
  embedding: null, embeddingModel: null, contentHash: null,
  taskRefText: "r", taskRefTextFolded: "r" };
const briefRow = { orgId: "o", workspaceId: "w", source: "h",
  sessionId: "s", title: "t", description: "d", titleFolded: "t",
  descriptionFolded: "d", briefCompletedAt: TS,
  embedding: null, embeddingModel: null, contentHash: "h" };
const images = {
  imageDescriptor: async (e: { imageKey: string }) => ({
    imageKey: e.imageKey, sourceHash: A, pointerSetHash: PTR.hash,
    complete: true }),
  imagePages: async (_e: unknown, kind: "event" | "brief",
      c: string | null) =>
    c === null ? page(kind === "event" ? [eventRow] : [briefRow], "done")
               : page([], null),
  priorKeyPages: async () => page([], null),
};
// 1. Fresh copy: begin -> pages -> finish -> dirty=false + checkpoint.
const r1 = await copyCanonicalEntry(images, store, ch,
  { checkpoint: "0" }, entry, "target-A");
console.log("R1", JSON.stringify(r1));
check("first_copy", r1.kind === "copied" && r1.checkpoint === entry.revision);
const dirty1 = await db.query(
  `SELECT dirty FROM ${SCHEMA}.canonical_feature_copy_state`)
  .then((r) => r.rows[0]?.dirty);
check("dirty_false", dirty1 === false);
// The ACK trigger takes the same state-row lock as finish and refuses once
// complete. No late page can appear after the terminal chains were validated.
let lateAckRefused = false;
try {
  await store.ackPage({ entry, kind: "event", cursor: "late",
    nextCursor: null, pageSha256: "2".repeat(64) });
} catch { lateAckRefused = true; }
check("post_finish_ack_refused", lateAckRefused);
// 2. Crash-after-finish: same entry, no checkpoint advance -> already done,
//    no CH write, no image descriptor needed.
chWrites = 0;
const imagesDown = {
  imageDescriptor: async () => { throw new Error("source_down"); },
  imagePages: async () => { throw new Error("source_down"); },
  priorKeyPages: async () => { throw new Error("source_down"); },
};
const r2 = await copyCanonicalEntry(imagesDown, store, ch,
  { checkpoint: "0" }, entry, "target-A");
check("crash_after_finish_complete",
  r2.kind === "copied" && r2.checkpoint === entry.revision && chWrites === 0);
// 3. Different target on the same database/tables -> NOT already_complete.
await db.query(`UPDATE ${SCHEMA}.canonical_feature_copy_state SET dirty=true`);
await db.query(`DELETE FROM ${SCHEMA}.canonical_feature_page_ack`);
// A DIFFERENT target on the same revision must fail closed (begin_drift) —
// never reuse ACKs or silently switch. It throws before any write.
let threw = "";
try {
  await copyCanonicalEntry(images, store, ch,
    { checkpoint: "0" }, entry, "target-B");
} catch (x) { threw = (x as Error).message; }
check("different_target_refused", threw === "begin_drift");
await producer.query("RESET ROLE");
producer.release();
// The named down removes exactly the restartable consumer-progress boundary;
// applying the same migration again recreates it with the restricted grant.
const down = readFileSync(
  "supabase/rollbacks/20261105030000_canonical_feature_consumer_acks.down.sql",
  "utf8").replaceAll("sessions.", `${SCHEMA}.`);
await db.query(down);
const removed = await db.query(`SELECT
  to_regclass($1) IS NULL AS state_gone,
  to_regprocedure($2) IS NULL AS finish_gone`, [
  `${SCHEMA}.canonical_feature_copy_state`,
  `${SCHEMA}.finish_canonical_copy(bigint,jsonb,text,text,text,text)`,
]);
await db.query(companion);
const reapplied = await db.query(`SELECT
  to_regclass($1) IS NOT NULL AS state_back,
  has_function_privilege($2,$3,'EXECUTE') AS finish_granted`, [
  `${SCHEMA}.canonical_feature_copy_state`,
  "bulls96_canonical_feature_producer",
  `${SCHEMA}.finish_canonical_copy(bigint,jsonb,text,text,text,text)`,
]);
check("down_reapply",
  removed.rows[0]?.state_gone === true &&
  removed.rows[0]?.finish_gone === true &&
  reapplied.rows[0]?.state_back === true &&
  reapplied.rows[0]?.finish_granted === true);
await db.end();
if (failures) { console.log(`${failures} FAILURES`); process.exit(1); }
console.log("ALL PASS");
