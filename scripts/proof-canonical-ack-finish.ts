// Assertion-driven real-PG proof: full composed DDL + real store +
// copyCanonicalEntry. Exits nonzero on any failure.
import { Pool } from "pg";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { CanonicalPageAckStore } from "../src/search-features/canonical-ack-store.ts";
import { copyCanonicalEntry } from "../src/search-features/canonical-cycle.ts";
const port = Number(process.argv[2] ?? 32770);
const SCHEMA = "sessions_proof";           // dedicated, never 'sessions'
const db = new Pool({ host: "127.0.0.1", port, user: "postgres",
  password: "synthetic-feature-only", database: "postgres" });
let failures = 0;
const check = (name: string, cond: boolean) => {
  console.log(name, cond ? "PASS" : "FAIL");
  if (!cond) failures++;
};
await db.query(`DROP SCHEMA IF EXISTS ${SCHEMA} CASCADE; CREATE SCHEMA ${SCHEMA}`);
// The real DDL grants the producer role — create it so grants apply unmodified.
await db.query(`CREATE ROLE bulls96_canonical_feature_producer NOLOGIN`)
  .catch(() => {});
// Apply the REAL #230 025000 writer migration (vendored snapshot at
// 226410e721beca5885082382094d1b38e3318c64) + the companion state DDL.
// Self-contained: read the vendored fixture, no shell/out-of-tree paths.
const writer = readFileSync(
  "scripts/fixtures/230-writer-20261105025000.sql", "utf8")
  .replaceAll("sessions.", `${SCHEMA}.`)
  .replaceAll("SCHEMA sessions", `SCHEMA ${SCHEMA}`)
  .replaceAll("GRANT USAGE ON SCHEMA", `GRANT USAGE ON SCHEMA`);
await db.query(writer);
const ddl = readFileSync(
  "docs/proposals/20261105025000_canonical_feature_journal.sql", "utf8")
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
import { CanonicalJournalStore } from "../src/search-features/canonical-journal.ts";
const journal = new CanonicalJournalStore({ query: (t, p) =>
  db.query(t.replaceAll("sessions.", `${SCHEMA}.`), p) });
const { entries } = await journal.entries("100", 10);
const entry = entries[0]!;
if (!entry) throw new Error("journal_decoder_empty");
console.log("decoded", entry.kind, entry.revision, entry.imageKey.slice(0,30));
// Real store over the composed schema (table names are sessions.* in the
// store — alias it via search_path).
const store = new CanonicalPageAckStore({ query: (t, p) =>
  db.query(`SET search_path TO ${SCHEMA}`).then(() =>
    db.query(t.replaceAll("sessions.", `${SCHEMA}.`), p)) });
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
await db.end();
if (failures) { console.log(`${failures} FAILURES`); process.exit(1); }
console.log("ALL PASS");
