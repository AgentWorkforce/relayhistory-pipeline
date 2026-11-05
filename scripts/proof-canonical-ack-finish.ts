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
// Apply the composed DDL under the proof schema.
const ddl = readFileSync(
  "docs/proposals/20261105025000_canonical_feature_journal.sql", "utf8")
  .replaceAll("sessions.", `${SCHEMA}.`);
await db.query(ddl);
const A = "a".repeat(64), UID = "12345678-1234-1234-1234-123456789012";
const SCOPE = { orgId: "o", workspaceId: "w", harness: "h", sessionId: "s",
  generation: A };
import { canonicalImageKey } from "../src/search-features/canonical-image-key.ts";
const HS1 = canonicalImageKey(SCOPE, UID);
const AUTH = { authorityId: UID, epoch: "1", fingerprint: A };
const PTR = { authorityId: UID, hash: "c".repeat(64), count: "1" };
await db.query(`INSERT INTO ${SCHEMA}.canonical_feature_journal(revision,kind,org_id,workspace_id,scope,schema_generation,image_key,source_hash,source_authority,pointer_set) VALUES(10,'publish','o','w',$1,$2,$3,$4,$5,$6)`,
  [JSON.stringify(SCOPE), A, HS1, A,
   JSON.stringify(AUTH), JSON.stringify(PTR)]);
const entry = { kind: "publish" as const, revision: "10", imageKey: HS1,
  schemaGeneration: A, sourceHash: A, scope: SCOPE,
  sourceAuthority: AUTH, pointerSet: PTR };
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
check("first_copy", r1.kind === "copied" && r1.checkpoint === "10");
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
  r2.kind === "copied" && r2.checkpoint === "10" && chWrites === 0);
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
