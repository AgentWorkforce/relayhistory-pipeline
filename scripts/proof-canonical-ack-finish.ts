// Real-PG integration: drives the ACTUAL CanonicalPageAckStore +
// copyCanonicalEntry against the composed DDL — no regex extraction.
// Run: bun scripts/proof-canonical-ack-finish.ts <pgPort>
import { Pool } from "pg";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { CanonicalPageAckStore } from "../src/search-features/canonical-ack-store.ts";
const port = Number(process.argv[2] ?? 32770);
const db = new Pool({ host: "127.0.0.1", port, user: "postgres",
  password: "synthetic-feature-only", database: "postgres" });
await db.query("DROP SCHEMA sessions CASCADE; CREATE SCHEMA sessions");
// Apply the FULL composed DDL — not a regex-extracted function body.
await db.query(readFileSync(
  "docs/proposals/20261105025000_canonical_feature_journal.sql", "utf8"));
const A = "a".repeat(64), UID = "12345678-1234-1234-1234-123456789012";
const AUTH = { authorityId: UID, epoch: "1", fingerprint: A };
const PTR = { authorityId: UID, hash: "c".repeat(64), count: "1" };
await db.query(`INSERT INTO sessions.canonical_feature_journal(revision,kind,org_id,workspace_id,scope,schema_generation,image_key,source_hash,source_authority,pointer_set) VALUES(10,'publish','o','w','{}',$1,'img',$4,$2,$3)`,
  [A, JSON.stringify(AUTH), JSON.stringify(PTR), A]);
const e = { kind: "publish" as const, revision: "10", imageKey: "img",
  schemaGeneration: A, sourceHash: A,
  scope: { orgId: "o", workspaceId: "w", harness: "h", sessionId: "s",
           generation: A },
  sourceAuthority: AUTH, pointerSet: PTR };
const s = new CanonicalPageAckStore({ query: (t, p) => db.query(t, p) });
const h = (k: string) => createHash("sha256").update(k).digest("hex");
const t = async (n: string, f: () => Promise<unknown>) => {
  try { console.log(n, JSON.stringify(await f()).slice(0, 70)); }
  catch (x) { console.log(n, "ERR", (x as Error).message.slice(0, 50)); }
};
// REAL store path: begin (started) -> ack -> resume(complete) -> finish -> dirty
await t("begin", () => s.begin(e, "pb", "img"));
const h1 = h("ev"), h2 = h("br");
await s.ackPage({ entry: e, kind: "event", cursor: "start",
  nextCursor: null, pageSha256: h1 });
await s.ackPage({ entry: e, kind: "brief", cursor: "start",
  nextCursor: null, pageSha256: h2 });
await t("resume_complete", () => s.resumeCursor(e, "event"));
await t("finish_ok", () => s.finish({ entry: e,
  terminalEvent: h1, terminalBrief: h2,
  sourceAuthority: AUTH, profileBinding: "pb", targetBinding: "img" }));
await t("dirty_false", () =>
  db.query("SELECT dirty FROM sessions.canonical_feature_copy_state")
    .then((r) => r.rows[0].dirty));
// crash-after-finish: begin again -> already_complete, NOT drift
await t("begin_after_finish", () => s.begin(e, "pb", "img"));
await db.end();
