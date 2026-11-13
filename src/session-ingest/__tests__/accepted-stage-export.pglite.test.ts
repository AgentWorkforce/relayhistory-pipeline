import { afterAll, afterEach, beforeAll, describe, expect, setDefaultTimeout, test } from "bun:test";
import { createHash, randomUUID } from "node:crypto";
import { zstdDecompressSync } from "node:zlib";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { objectKey, readAll, type ObjectStore } from "../../shared/vendor.ts";
import { exportAcceptedStages, type ExportOptions, type ExportSql } from "../accepted-stage-export.ts";
import { ACCEPTED_SUFFIX, acceptedCompleteKey, drainBufferedBatches, parseAcceptedComplete } from "../buffered-drain.ts";
import { orgFenceAuthorityKey, s3LedgerKeys, s3LedgerObjects } from "../ledger-s3.ts";
import { BUFFER_STAGE_MAX_BYTES, prepareBufferedStage, putBufferedStage, stagedKey } from "../staging.ts";
import { parseExportArgs } from "../../../scripts/accepted-stage-export.ts";
import { applySupabaseSchema } from "./pglite-schema.ts";
import { scratchStore } from "./support.ts";

setDefaultTimeout(60_000);
const ROOT = "96bulls";
// Synthetic tenants and times only (the repo is public).
const A = { orgId: "org-a", workspaceId: "ws-a", userId: "user-1" };
const C = { orgId: "org-b", workspaceId: "ws-b", userId: "user-3" };
type T = typeof A;
const ORIGIN = "origin-1";
const sha = (v: string | Uint8Array) => createHash("sha256").update(v).digest("hex");
const dec = new TextDecoder();
const enc = new TextEncoder();

let db: PGlite;
beforeAll(async () => {
  db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  // ingest_batches (022000), ingest_claim_attempts (030000), ingest_stage_intents + ingest_buffer_outbox (050000).
  await applySupabaseSchema(db, { until: "20261104050000_buffered_upload_receipts.sql" });
});
afterAll(async () => { await db.close(); });
let cleanup: (() => void)[] = [];
afterEach(async () => {
  cleanup.forEach((c) => c());
  cleanup = [];
  await db.exec("DELETE FROM sessions.ingest_buffer_outbox; DELETE FROM sessions.ingest_stage_intents; DELETE FROM sessions.ingest_claim_attempts; DELETE FROM sessions.ingest_batches;");
});
function scratch(): ObjectStore {
  const s = scratchStore();
  cleanup.push(s.done);
  return s.store;
}

interface Stage { t: T; batchId: string; digest: string; claim: string; token: string; key: string; sha256: string }
/** SQL rows for one buffered batch and its stage, in FK order. */
async function seed(t: T, o: { batchState?: "committed" | "pending"; receipt?: "acknowledged" | "preparing"; cleanupState?: "live" | "retired"; outbox?: "active" | "erased" | "none"; key?: string; sha256?: string; digest?: string; claim?: string } = {}): Promise<Stage> {
  const batchId = randomUUID(), digest = o.digest ?? sha(batchId), claim = o.claim ?? randomUUID(), token = randomUUID();
  const sha256 = o.sha256 ?? sha(token);
  const key = o.key ?? stagedKey(ROOT, { org_id: t.orgId, workspace_id: t.workspaceId, origin_id: ORIGIN, user_id: t.userId, batch_digest: digest, claim_token: claim }, sha256, "buffered-batches");
  const committed = (o.batchState ?? "committed") === "committed";
  await db.query(`INSERT INTO sessions.ingest_batches (org_id, workspace_id, origin_id, batch_id, user_id, digest, receipt_id, state, committed_at)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,${committed ? "now()" : "NULL"})`, [t.orgId, t.workspaceId, ORIGIN, batchId, t.userId, digest, `rhr_${"c".repeat(64)}`, committed ? "committed" : "pending"]);
  await db.query(`INSERT INTO sessions.ingest_claim_attempts (claim_token, org_id, workspace_id, user_id, origin_id, batch_id, digest, epochs) VALUES ($1,$2,$3,$4,$5,$6,$7,'[]')`,
    [claim, t.orgId, t.workspaceId, t.userId, ORIGIN, batchId, digest]);
  await db.query(`INSERT INTO sessions.ingest_stage_intents (stage_token, claim_token, org_id, workspace_id, user_id, origin_id, batch_id, digest, stage_key, stage_sha256, stage_bytes, group_ordinals, reset_epoch, receipt_state, cleanup_state, expires_at)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,100,'[]',0,$11,$12,now() + interval '1 day')`,
    [token, claim, t.orgId, t.workspaceId, t.userId, ORIGIN, batchId, digest, key, sha256, o.receipt ?? "acknowledged", o.cleanupState ?? "live"]);
  const outbox = o.outbox ?? "active";
  if (outbox !== "none")
    await db.query(`INSERT INTO sessions.ingest_buffer_outbox (org_id, workspace_id, origin_id, batch_id, user_id, digest, active_stage_token, state) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
      [t.orgId, t.workspaceId, ORIGIN, batchId, t.userId, digest, outbox === "active" ? token : null, outbox === "active" ? "buffered" : "erased"]);
  return { t, batchId, digest, claim, token, key, sha256 };
}

function recordingSql(log: string[]): ExportSql {
  return { async query(text, params) { log.push(text.trim()); return { rows: (await db.query(text, params as unknown[])).rows as Record<string, unknown>[] }; } };
}
const run = (store: ObjectStore, over: Partial<ExportOptions> = {}, log: string[] = []) =>
  exportAcceptedStages({ sql: recordingSql(log), store, keyRoot: ROOT, dryRun: false, nowMs: () => 1_893_542_400_000, ...over });
const exists = async (store: ObjectStore, key: string) => (await store.head(objectKey(key))) !== undefined;
const readText = async (store: ObjectStore, key: string) => dec.decode(await readAll(await store.get(objectKey(key))));
async function allKeys(store: ObjectStore): Promise<string[]> {
  const out: string[] = [];
  let cursor: string | undefined;
  do {
    const page = await store.list(objectKey(ROOT), cursor);
    out.push(...page.items.map((i) => `${i.key}@${JSON.stringify(i.version)}`));
    cursor = page.nextCursor;
  } while (cursor);
  return out.sort();
}

describe("accepted-stage export (0005 step 0b)", () => {
  test("only acknowledged, live, active stages of committed batches get a sidecar; the org completes", async () => {
    const store = scratch();
    const ok = await seed(A);
    const preparing = await seed(A, { receipt: "preparing" });                    // never acknowledged
    const pending = await seed(A, { batchState: "pending" });                     // batch never committed
    const erased = await seed(A, { outbox: "erased" });                           // outbox erased
    const retired = await seed(A, { cleanupState: "retired" });                   // a retired stage
    const notActive = await seed(A, { outbox: "none" });                          // e.g. a replaced GC original
    const summary = await run(store);
    expect(summary).toMatchObject({ mode: "execute", rows: 1, written: 1, invalid: 0, conflict: 0, failed: 0, orgs: { complete: 1, incomplete: 0 } });
    expect(JSON.parse(await readText(store, ok.key + ACCEPTED_SUFFIX))).toEqual({ v: 1, kind: "buffered-accepted", stage_sha256: ok.sha256 });
    for (const s of [preparing, pending, erased, retired, notActive]) expect(await exists(store, s.key + ACCEPTED_SUFFIX)).toBe(false);
    expect(parseAcceptedComplete(await readText(store, acceptedCompleteKey(ROOT, A.orgId)))).toMatchObject({ stages: 1 });
    const text = JSON.stringify(summary);
    for (const needle of ["org-a", "ws-a", "user-1", ORIGIN, ok.sha256, "96bulls"]) expect(text).not.toContain(needle);
  });

  test("SQL is one read-only snapshot, checked, and always rolled back", async () => {
    const store = scratch();
    await seed(A);
    const log: string[] = [];
    await run(store, {}, log);
    expect(log[0]).toBe("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
    expect(log[1]).toBe("SHOW transaction_read_only");
    expect(log.at(-1)).toBe("ROLLBACK");
    for (const s of log.slice(2, -1)) expect(s).toMatch(/^(SELECT|SET LOCAL statement_timeout = \d+$)/);
    const refused: string[] = [];
    const notReadOnly: ExportSql = { async query(text) { refused.push(text); return { rows: text.startsWith("SHOW") ? [{ transaction_read_only: "off" }] : [] }; } };
    await expect(exportAcceptedStages({ sql: notReadOnly, store, keyRoot: ROOT, dryRun: false })).rejects.toThrow("export_not_read_only");
    expect(refused).toEqual(["BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY", "SHOW transaction_read_only", "ROLLBACK"]);
  });

  test("a rerun is a no-op; dry run writes nothing; org scope", async () => {
    const store = scratch();
    await seed(A);
    await seed(C);
    const dry = await run(store, { dryRun: true });
    expect(dry).toMatchObject({ mode: "dry_run", would_write: 2, written: 0, orgs: { would_complete: 2, complete: 0 } });
    expect(await allKeys(store)).toEqual([]);
    const scoped = await run(store, { orgs: ["org-b"] });
    expect(scoped).toMatchObject({ rows: 1, written: 1, orgs: { complete: 1 } });
    expect(await exists(store, acceptedCompleteKey(ROOT, A.orgId))).toBe(false);
    await run(store);
    const before = await allKeys(store);
    const again = await run(store);
    expect(again).toMatchObject({ rows: 2, written: 0, already: 2, orgs: { complete: 2 } });
    expect(await allKeys(store)).toEqual(before);
    await expect(run(store, { orgs: ["../x"] })).rejects.toThrow("export_org_invalid");
  });

  test("fail closed: a row whose key does not bind, or a sidecar naming other bytes, leaves the org incomplete", async () => {
    const store = scratch();
    await run(store); // nothing yet
    await seed(A);
    await run(store);
    expect(parseAcceptedComplete(await readText(store, acceptedCompleteKey(ROOT, A.orgId)))).not.toBeNull();
    // A stage key built for another user under this org: never written.
    const other = { ...A, userId: "user-9" };
    const misfiled = stagedKey(ROOT, { org_id: A.orgId, workspace_id: A.workspaceId, origin_id: ORIGIN, user_id: other.userId, batch_digest: sha("x"), claim_token: randomUUID() }, sha("y"), "buffered-batches");
    await seed(A, { key: misfiled });
    // A sidecar already there for another stage, naming other bytes.
    const clash = await seed(A);
    await store.put(objectKey(clash.key + ACCEPTED_SUFFIX), enc.encode(JSON.stringify({ v: 1, kind: "buffered-accepted", stage_sha256: "0".repeat(64) })));
    const summary = await run(store);
    expect(summary).toMatchObject({ rows: 3, already: 1, invalid: 1, conflict: 1, orgs: { complete: 0, incomplete: 1 } });
    expect(await exists(store, misfiled + ACCEPTED_SUFFIX)).toBe(false);
    // The earlier completion object is replaced by one the drain reads as missing.
    expect(parseAcceptedComplete(await readText(store, acceptedCompleteKey(ROOT, A.orgId)))).toBeNull();
  });

  test("end to end: the drain writes the acknowledged stage and reports the refused one", async () => {
    const store = scratch();
    const line = (id: string) => JSON.stringify({ kind: "session_event", origin_id: ORIGIN, record_id: id, revision: "1", operation: "upsert", received_at: "2030-01-02T00:00:00.000Z", payload: { text: id, ts_ms: 1_893_542_400_000 } });
    const make = async (session: string) => {
      const claim = randomUUID(), digest = sha(randomUUID());
      const prepared = prepareBufferedStage(ROOT, { claim_token: claim, batch_digest: digest, received_at: "2030-01-02T00:00:00.000Z", org_id: A.orgId, workspace_id: A.workspaceId, origin_id: ORIGIN, user_id: A.userId },
        [{ source: "claude", sessionId: session, lines: [line(`${session}-r`)], meta: [[`${session}:r1`, sha(session), "claude", session]] }], [[0]]);
      await putBufferedStage(store, prepared);
      return { prepared, claim, digest };
    };
    const acked = await make("sess-acked");
    const refused = await make("sess-refused"); // staged, then the consent gate refused it before the ACK
    await seed(A, { key: acked.prepared.spec.key, sha256: acked.prepared.spec.sha256, digest: acked.digest, claim: acked.claim });
    await seed(A, { key: refused.prepared.spec.key, sha256: refused.prepared.spec.sha256, digest: refused.digest, claim: refused.claim, receipt: "preparing", outbox: "none" });
    expect((await run(store)).written).toBe(1);
    await s3LedgerObjects(store, ROOT).editFence(s3LedgerKeys(ROOT, A).fence, (f) => ({ next: f, result: undefined }));
    // --execute needs the org's fence authority, as admission does (main's OrgFenceAuthorityDoc).
    await store.put(objectKey(orgFenceAuthorityKey(ROOT, A.orgId)), enc.encode(JSON.stringify({ v: 1, fenceBackfill: "complete", revisionBackfill: "complete",
      ownershipBackfill: "complete", controlMirror: "on", rearmSweeper: "live", workerHttpAdmission: "off", writtenAtMs: 1, orgId: A.orgId })));
    const drained = await drainBufferedBatches({ store, keyRoot: ROOT, orgs: [A.orgId], dryRun: false, sleep: async () => {},
      decompress: (b) => new Uint8Array(zstdDecompressSync(b, { maxOutputLength: BUFFER_STAGE_MAX_BYTES })) });
    expect(drained.objects).toMatchObject({ drained: 1, stage_unacknowledged: 1, accepted_mismatch: 0, stage_export_missing: 0 });
    expect(drained.sessions_written).toBe(1);
  });

  test("CLI: the mode is never implied and --execute needs the KMS key", () => {
    expect(() => parseExportArgs(["--bucket", "my-bucket"])).toThrow("exactly one of");
    expect(() => parseExportArgs(["--bucket", "my-bucket", "--execute"])).toThrow("--kms-key-arn");
    expect(() => parseExportArgs(["--bucket", "my-bucket", "--dry-run", "--org", "../x"])).toThrow("--org");
    expect(parseExportArgs(["--dry-run", "--bucket", "my-bucket"])).toEqual({ mode: "dry_run", bucket: "my-bucket", region: "us-east-1", orgs: [], pageSize: 1_000, concurrency: 8, statementTimeoutMs: 30_000 });
  });
});
