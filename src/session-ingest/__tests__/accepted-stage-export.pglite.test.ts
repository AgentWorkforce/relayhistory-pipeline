import { afterAll, afterEach, beforeAll, describe, expect, setDefaultTimeout, test } from "bun:test";
import { createHash, randomUUID } from "node:crypto";
import { zstdDecompressSync } from "node:zlib";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { objectKey, readAll, type ObjectStore } from "../../shared/vendor.ts";
import { exportAcceptedStages, exportLockKey, type ExportOptions, type ExportSql } from "../accepted-stage-export.ts";
import { ACCEPTED_SUFFIX, acceptedCompleteKey, acquireDrainLock, drainBufferedBatches, parseAcceptedComplete, parseAcceptedMarker } from "../buffered-drain.ts";
import { applyFenceChange, fenceEpoch, orgFenceAuthorityKey, s3LedgerKeys, s3LedgerObjects, sessionKeyHash } from "../ledger-s3.ts";
import { BUFFER_STAGE_MAX_BYTES, prepareBufferedStage, putBufferedStage, stagedKey } from "../staging.ts";
import type { Group } from "../writer.ts";
import { parseExportArgs } from "../../../scripts/accepted-stage-export.ts";
import { applySupabaseSchema } from "./pglite-schema.ts";
import { scratchStore } from "./support.ts";

setDefaultTimeout(120_000);
const ROOT = "96bulls";
// Synthetic tenants, hashes and times only (the repo is public).
const A = { orgId: "org-a", workspaceId: "ws-a", userId: "user-1" };
const C = { orgId: "org-b", workspaceId: "ws-b", userId: "user-3" };
type T = typeof A;
const ORIGIN = "origin-1";
const hex = (c: string) => c.repeat(64);
const sha = (v: string | Uint8Array) => createHash("sha256").update(v).digest("hex");
const POLICY_IDENTITY = hex("1"), POLICY_EPOCH = hex("2"), CLOCK_EPOCH = hex("3"), CLOCK_IDENTITY = hex("4"), GEN = hex("5");
const dec = new TextDecoder();
const enc = new TextEncoder();

let db: PGlite;
beforeAll(async () => {
  db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  // Through the reader whose predicates the export transcribes (20261105047020).
  await applySupabaseSchema(db, { until: "20261105047020_buffered_session_stages_paged.sql" });
  await db.exec("SET session_replication_role = replica");
  await db.query(`INSERT INTO sessions.ingest_lazy_policy (singleton, baseline_mode, generation, source_identity_sha256, control_epoch, privacy_baseline_sha256,
      retention_contract_sha256, source_proof_sha256, issuer, valid_until) VALUES (true, 'lazy-current', $1, $2, $3, $1, $1, $1, 'relayhistory-cloud', now() + interval '1 day')`,
    [GEN, POLICY_IDENTITY, POLICY_EPOCH]);
  await db.query(`INSERT INTO sessions.ingest_receipt_seat_clock (singleton, control_epoch, source_identity_sha256, allocated_high_water, last_seat, baseline, initial_generation)
      VALUES (true, $1, $2, 0, 0, '{}', $3)`, [CLOCK_EPOCH, CLOCK_IDENTITY, GEN]);
  await db.exec("SET session_replication_role = origin");
});
afterAll(async () => { await db.close(); });
let cleanup: (() => void)[] = [];
afterEach(async () => {
  cleanup.forEach((c) => c());
  cleanup = [];
  await db.exec(`SET session_replication_role = replica;
    DELETE FROM sessions.ingest_buffer_outbox; DELETE FROM sessions.ingest_lazy_ack_decisions; DELETE FROM sessions.ingest_lazy_native_slots;
    DELETE FROM sessions.ingest_receipt_seats; DELETE FROM sessions.ingest_stage_intents; DELETE FROM sessions.ingest_claim_attempts;
    DELETE FROM sessions.ingest_batches; DELETE FROM sessions.ingest_fence_versions; DELETE FROM sessions.upload_deletions; DELETE FROM sessions.upload_resets;
    SET session_replication_role = origin;`);
});
function scratch(): ObjectStore {
  const s = scratchStore();
  cleanup.push(s.done);
  return s.store;
}

type SeedGroup = { source: string; session: string; records: { id: string; rev: number; accepted?: boolean }[] };
interface Seeded { key: string; sha256: string; groups: SeedGroup[] }
let seat = 0;
/**
 * One buffered batch, fully in SQL, as admission left it under the lazy policy: committed batch, claim (records,
 * epochs, admission version 0 = no control yet), acknowledged active stage, outbox row, and per line its receipt seat,
 * native slot and ACK decision. The ACK candidate key is computed in SQL with the reader's exact expression.
 */
async function seed(t: T, groups: SeedGroup[], o: { claimEpoch?: number; fenceVersion?: number; receipt?: "acknowledged" | "preparing"; active?: boolean;
  batchState?: "committed" | "pending"; key?: string; sha256?: string; digest?: string; claim?: string } = {}): Promise<Seeded> {
  const batchId = randomUUID(), digest = o.digest ?? sha(batchId), claim = o.claim ?? randomUUID(), token = randomUUID();
  const sha256 = o.sha256 ?? sha(token);
  const key = o.key ?? stagedKey(ROOT, { org_id: t.orgId, workspace_id: t.workspaceId, origin_id: ORIGIN, user_id: t.userId, batch_digest: digest, claim_token: claim }, sha256, "buffered-batches");
  const epoch = o.claimEpoch ?? 0;
  let ord = 0;
  const groupOrdinals = groups.map((g) => g.records.map(() => ord++));
  const lines = groups.flatMap((g, gi) => g.records.map((r, j) => ({ ord: groupOrdinals[gi]![j]!, g, r, digest: sha(`${r.id}:${r.rev}`) })));
  const records = lines.map((l) => ({ ord: l.ord, source: l.g.source, session_id: l.g.session, record_id: l.r.id, revision: l.r.rev, digest: l.digest }));
  const sessions = [...new Map(groups.map((g) => [JSON.stringify([g.source, g.session]), g])).values()];
  const epochs = sessions.map((g) => ({ source: g.source, session_id: g.session, epoch, reset_epoch: 0 }));
  const committed = (o.batchState ?? "committed") === "committed";
  await db.exec("SET session_replication_role = replica");
  await db.query(`INSERT INTO sessions.ingest_batches (org_id, workspace_id, origin_id, batch_id, user_id, digest, receipt_id, state, committed_at, publication_state)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,${committed ? "now()" : "NULL"},'buffered')`, [t.orgId, t.workspaceId, ORIGIN, batchId, t.userId, digest, `rhr_${hex("c")}`, committed ? "committed" : "pending"]);
  await db.query(`INSERT INTO sessions.ingest_claim_attempts (claim_token, org_id, workspace_id, user_id, origin_id, batch_id, digest, epochs, records, reset_epoch, source_control_version, source_identity_sha256)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9::jsonb,0,0,$10)`, [claim, t.orgId, t.workspaceId, t.userId, ORIGIN, batchId, digest, JSON.stringify(epochs), JSON.stringify(records), POLICY_IDENTITY]);
  await db.query(`INSERT INTO sessions.ingest_stage_intents (stage_token, claim_token, org_id, workspace_id, user_id, origin_id, batch_id, digest, stage_key, stage_sha256, stage_bytes, group_ordinals, reset_epoch, receipt_state, cleanup_state, expires_at)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,100,$11::jsonb,0,$12,'live',now() + interval '1 day')`,
    [token, claim, t.orgId, t.workspaceId, t.userId, ORIGIN, batchId, digest, key, sha256, JSON.stringify(groupOrdinals), o.receipt ?? "acknowledged"]);
  if (o.active !== false)
    await db.query(`INSERT INTO sessions.ingest_buffer_outbox (org_id, workspace_id, origin_id, batch_id, user_id, digest, active_stage_token, state) VALUES ($1,$2,$3,$4,$5,$6,$7,'buffered')`,
      [t.orgId, t.workspaceId, ORIGIN, batchId, t.userId, digest, token]);
  for (const l of lines) {
    const seatKey = sha(`seat:${claim}:${l.ord}`), slotKey = sha(`slot:${claim}:${l.ord}`);
    await db.query(`INSERT INTO sessions.ingest_receipt_seats (key_sha256, key_material, seat, first_digest, source, session_id, fence_epoch) VALUES ($1,'[]',$2,$3,$4,$5,$6)`,
      [seatKey, ++seat, l.digest, l.g.source, l.g.session, epoch]);
    await db.query(`INSERT INTO sessions.ingest_lazy_native_slots (key_sha256, key_material, org_id, workspace_id, origin_id, record_id, source_identity_sha256, control_epoch,
        initialized_witness_sha256, current_owner, current_source, current_session, current_revision, current_digest, current_reset_epoch, current_fence_epoch, current_seat_key)
      VALUES ($1,'[]',$2,$3,$4,$5,$6,$7,$6,$8,$9,$10,$11,$12,0,$13,$14)`,
      [slotKey, t.orgId, t.workspaceId, ORIGIN, l.r.id, POLICY_IDENTITY, POLICY_EPOCH, t.userId, l.g.source, l.g.session, l.r.rev, l.digest, epoch, seatKey]);
    await db.query(`INSERT INTO sessions.ingest_lazy_ack_decisions (claim_token, ord, native_key_sha256, native_lineage, candidate_key_sha256, candidate_digest, first_digest,
        body_accepted, seat_key_sha256, reset_epoch, fence_epoch, generation, source_control_version, source_witness_sha256)
      SELECT $1, $2, $3, 0, encode(sha256(convert_to(jsonb_build_array('buffer-first-ack-seat-v1', $4::text, $5::text, $6::text, $7::text, $8::text, $9::text,
               $10::text, $11::text, '0'::text, $12::text, $13::text, $14::text)::text, 'UTF8')), 'hex'),
             $15, $15, $16, $17, 0, $18, $19, 0, $19`,
      [claim, l.ord, slotKey, CLOCK_EPOCH, CLOCK_IDENTITY, t.orgId, t.workspaceId, t.userId, ORIGIN, l.r.id, String(l.r.rev), l.g.source, l.g.session, String(epoch),
        l.digest, l.r.accepted !== false, seatKey, epoch, GEN]);
  }
  if (o.fenceVersion !== undefined)
    for (const g of sessions)
      await db.query(`INSERT INTO sessions.ingest_fence_versions (org_id, workspace_id, user_id, origin_id, source, session_id, epoch) VALUES ($1,$2,$3,$4,$5,$6,$7)
        ON CONFLICT (org_id, workspace_id, user_id, origin_id, source, session_id) DO UPDATE SET epoch = excluded.epoch`,
        [t.orgId, t.workspaceId, t.userId, ORIGIN, g.source, g.session, o.fenceVersion]);
  await db.exec("SET session_replication_role = origin");
  return { key, sha256, groups };
}

function recordingSql(log: string[]): ExportSql {
  return { async query(text, params) { log.push(text.trim()); return { rows: (await db.query(text, params as unknown[])).rows as Record<string, unknown>[] }; } };
}
const run = (store: ObjectStore, over: Partial<ExportOptions> = {}, log: string[] = []) =>
  exportAcceptedStages({ sql: recordingSql(log), store, keyRoot: ROOT, dryRun: false, nowMs: () => 1_893_542_400_000, ...over });
const exists = async (store: ObjectStore, key: string) => (await store.head(objectKey(key))) !== undefined;
const readText = async (store: ObjectStore, key: string) => dec.decode(await readAll(await store.get(objectKey(key))));
const verdict = async (store: ObjectStore, key: string) => parseAcceptedMarker(await readText(store, key + ACCEPTED_SUFFIX));
const backfillFence = (store: ObjectStore, t: T) =>
  s3LedgerObjects(store, ROOT).editFence(s3LedgerKeys(ROOT, t).fence, (f) => ({ next: f, result: undefined }));
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

describe("accepted-stage export (0005 step 0b): SQL's own verdict per stage", () => {
  test("a live stage: SQL's kept lines per object group, and the S3 fence the export saw", async () => {
    const store = scratch();
    await backfillFence(store, A);
    const s = await seed(A, [{ source: "claude", session: "sess-1", records: [{ id: "a", rev: 1 }, { id: "b", rev: 1 }] }, { source: "claude", session: "sess-2", records: [{ id: "c", rev: 1 }] }]);
    const summary = await run(store);
    expect(summary).toMatchObject({ mode: "execute", rows: 1, live: 1, not_live: 0, written: 1, invalid: 0, failed: 0, orgs: { complete: 1 } });
    const v = (await verdict(store, s.key))!;
    expect(v).toMatchObject({ v: 2, live: true, kept: [[0, 1], [0]], stage_sha256: s.sha256 });
    const fence = (await s3LedgerObjects(store, ROOT).readFence(s3LedgerKeys(ROOT, A).fence)) as { state: "present"; fence: Parameters<typeof fenceEpoch>[0] };
    expect(v.fence).toEqual({ instance: fence.fence.instance, resets: 0, resetsCompleted: 0,
      epochs: { [sessionKeyHash("claude", "sess-1")]: 0, [sessionKeyHash("claude", "sess-2")]: 0 } });
    expect(parseAcceptedComplete(await readText(store, acceptedCompleteKey(ROOT, A.orgId)))).toMatchObject({ stages: 1 });
    const text = JSON.stringify(summary);
    for (const needle of ["org-a", "ws-a", "user-1", ORIGIN, s.sha256, "96bulls"]) expect(text).not.toContain(needle);
  });

  test("finding 1: disabled then re-enabled before the export (the fence version moved past the claim's): not live", async () => {
    const store = scratch();
    await backfillFence(store, A);
    // Claimed under epoch 0; a disable bumped ingest_fence_versions to 1, and the re-enable removed the deletion only.
    const s = await seed(A, [{ source: "claude", session: "sess-1", records: [{ id: "a", rev: 1 }] }], { claimEpoch: 0, fenceVersion: 1 });
    const summary = await run(store);
    expect(summary).toMatchObject({ live: 0, not_live: 1, written: 1 });
    expect(await verdict(store, s.key)).toMatchObject({ live: false, kept: [] });
  });

  test("finding 2: under the lazy policy only body-accepted lines are kept", async () => {
    const store = scratch();
    await backfillFence(store, A);
    const s = await seed(A, [{ source: "claude", session: "sess-1", records: [{ id: "a", rev: 1 }, { id: "b", rev: 1, accepted: false }] }, { source: "claude", session: "sess-2", records: [{ id: "c", rev: 1 }] }]);
    await run(store);
    expect(await verdict(store, s.key)).toMatchObject({ live: true, kept: [[0], [0]] });
  });

  test("a reset at or after the batch's commit, or a deletion, makes the lines not live (exact, database clock)", async () => {
    const store = scratch();
    await backfillFence(store, A);
    const reset = await seed(A, [{ source: "claude", session: "sess-1", records: [{ id: "a", rev: 1 }] }]);
    await db.query("INSERT INTO sessions.upload_resets (org_id, workspace_id, user_id, request_id, reset_at) VALUES ($1,$2,$3,'req-1', now() + interval '1 minute')", [A.orgId, A.workspaceId, A.userId]);
    await run(store);
    expect((await verdict(store, reset.key))!.live).toBe(false);
    await db.exec("DELETE FROM sessions.upload_resets");
    const deleted = await seed(A, [{ source: "claude", session: "gone", records: [{ id: "b", rev: 1 }] }]);
    await db.query("INSERT INTO sessions.upload_deletions (org_id, workspace_id, user_id, origin_id, source, session_id) VALUES ($1,$2,$3,$4,'claude','gone')", [A.orgId, A.workspaceId, A.userId, ORIGIN]);
    await run(store);
    expect((await verdict(store, deleted.key))!.live).toBe(false);
    expect((await verdict(store, reset.key))!.live).toBe(true); // the reset is gone: its verdict is rewritten (CAS)
  });

  test("candidates are active, acknowledged stages of committed batches only", async () => {
    const store = scratch();
    await backfillFence(store, A);
    const ok = await seed(A, [{ source: "claude", session: "s1", records: [{ id: "a", rev: 1 }] }]);
    const preparing = await seed(A, [{ source: "claude", session: "s2", records: [{ id: "b", rev: 1 }] }], { receipt: "preparing" });
    const inactive = await seed(A, [{ source: "claude", session: "s3", records: [{ id: "c", rev: 1 }] }], { active: false }); // e.g. a GC original
    const pending = await seed(A, [{ source: "claude", session: "s4", records: [{ id: "d", rev: 1 }] }], { batchState: "pending" });
    const summary = await run(store);
    expect(summary.rows).toBe(1);
    expect(await exists(store, ok.key + ACCEPTED_SUFFIX)).toBe(true);
    for (const s of [preparing, inactive, pending]) expect(await exists(store, s.key + ACCEPTED_SUFFIX)).toBe(false);
  });

  test("SQL: every page its own short read-only transaction, checked, always rolled back", async () => {
    const store = scratch();
    await backfillFence(store, A);
    for (let i = 0; i < 3; i++) await seed(A, [{ source: "claude", session: `s${i}`, records: [{ id: `r${i}`, rev: 1 }] }]);
    const log: string[] = [];
    await run(store, { pageSize: 1 }, log);
    const txs: string[][] = [];
    for (const s of log) {
      if (s.startsWith("BEGIN")) txs.push([]);
      txs[txs.length - 1]!.push(s);
    }
    expect(txs.length).toBeGreaterThanOrEqual(4); // orgs + one per page + the empty last page
    for (const tx of txs) {
      expect(tx[0]).toBe("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
      expect(tx[1]).toBe("SHOW transaction_read_only");
      expect(tx.at(-1)).toBe("ROLLBACK");
      for (const s of tx.slice(2, -1)) expect(s).toMatch(/^(SELECT|WITH|SET LOCAL statement_timeout = \d+$)/);
    }
    const notReadOnly: ExportSql = { async query(text) { return { rows: text.startsWith("SHOW") ? [{ transaction_read_only: "off" }] : [] }; } };
    await expect(exportAcceptedStages({ sql: notReadOnly, store, keyRoot: ROOT, dryRun: false })).rejects.toThrow("export_not_read_only");
  });

  test("a rerun is a no-op; dry run writes nothing; org scope; completion objects are create-only", async () => {
    const store = scratch();
    await backfillFence(store, A);
    await backfillFence(store, C);
    await seed(A, [{ source: "claude", session: "s1", records: [{ id: "a", rev: 1 }] }]);
    await seed(C, [{ source: "claude", session: "s9", records: [{ id: "z", rev: 1 }] }]);
    const dry = await run(store, { dryRun: true });
    expect(dry).toMatchObject({ mode: "dry_run", would_write: 2, written: 0, orgs: { would_complete: 2, complete: 0 } });
    const beforeDry = (await allKeys(store)).filter((k) => !k.includes("upload-fences"));
    expect(beforeDry).toEqual([]);
    const scoped = await run(store, { orgs: ["org-b"] });
    expect(scoped).toMatchObject({ rows: 1, written: 1, orgs: { complete: 1 } });
    await run(store);
    // Everything but the export's own lease (renewed and released by every run) is untouched by a rerun.
    const settled = async () => (await allKeys(store)).filter((k) => !k.startsWith(exportLockKey(ROOT)));
    const before = await settled();
    const again = await run(store);
    expect(again).toMatchObject({ rows: 2, written: 0, updated: 0, already: 2, orgs: { complete: 0, already_complete: 2 } });
    expect(await settled()).toEqual(before);
    await expect(run(store, { orgs: ["../x"] })).rejects.toThrow("export_org_invalid");
  });

  test("a user with no S3 fence yet: SQL's verdict is kept with no fence view (the drain then refuses until re-exported)", async () => {
    const store = scratch();
    const s = await seed(A, [{ source: "claude", session: "s1", records: [{ id: "a", rev: 1 }] }]);
    const summary = await run(store);
    expect(summary).toMatchObject({ live: 1, fence_absent: 1 });
    expect(await verdict(store, s.key)).toMatchObject({ live: true, fence: null });
  });

  test("finding 9: one export at a time (the drain's fenced lease, its own key)", async () => {
    const store = scratch();
    await backfillFence(store, A);
    await seed(A, [{ source: "claude", session: "s1", records: [{ id: "a", rev: 1 }] }]);
    const held = await acquireDrainLock(store, ROOT, { holder: "export-other", ttlMs: 60_000, now: () => 1_893_542_400_000, key: exportLockKey(ROOT) });
    await expect(run(store)).rejects.toThrow("drain_locked");
    await expect(run(store, { dryRun: true })).rejects.toThrow("drain_locked");
    await held.release();
    expect((await run(store)).written).toBe(1);
  });

  test("a row whose key does not bind is never written and leaves the org incomplete", async () => {
    const store = scratch();
    await backfillFence(store, A);
    const misfiled = stagedKey(ROOT, { org_id: A.orgId, workspace_id: A.workspaceId, origin_id: ORIGIN, user_id: "user-9", batch_digest: sha("x"), claim_token: randomUUID() }, sha("y"), "buffered-batches");
    await seed(A, [{ source: "claude", session: "s1", records: [{ id: "a", rev: 1 }] }], { key: misfiled });
    const summary = await run(store);
    expect(summary).toMatchObject({ rows: 1, invalid: 1, orgs: { complete: 0, incomplete: 1 } });
    expect(await exists(store, misfiled + ACCEPTED_SUFFIX)).toBe(false);
  });

  test("end to end: the drain writes exactly the lines SQL keeps, and nothing of a stage SQL hides", async () => {
    const store = scratch();
    await backfillFence(store, A);
    await store.put(objectKey(orgFenceAuthorityKey(ROOT, A.orgId)), enc.encode(JSON.stringify({ v: 1, fenceBackfill: "complete", revisionBackfill: "complete",
      ownershipBackfill: "complete", controlMirror: "on", rearmSweeper: "live", workerHttpAdmission: "off", writtenAtMs: 1, orgId: A.orgId })));
    const line = (id: string) => JSON.stringify({ kind: "session_event", origin_id: ORIGIN, record_id: id, revision: "1", operation: "upsert", received_at: "2030-01-02T00:00:00.000Z", payload: { text: id, ts_ms: 1_893_542_400_000 } });
    const make = async (groups: SeedGroup[], o: Parameters<typeof seed>[2] = {}) => {
      const claim = randomUUID(), digest = sha(randomUUID());
      const objectGroups: Group[] = groups.map((g) => ({ source: g.source, sessionId: g.session, lines: g.records.map((r) => line(r.id)), meta: g.records.map((r) => [`${r.id}:r${r.rev}`, sha(`${r.id}:${r.rev}`), g.source, g.session]) }));
      let ord = 0;
      const prepared = prepareBufferedStage(ROOT, { claim_token: claim, batch_digest: digest, received_at: "2030-01-02T00:00:00.000Z", org_id: A.orgId, workspace_id: A.workspaceId, origin_id: ORIGIN, user_id: A.userId },
        objectGroups, groups.map((g) => g.records.map(() => ord++)));
      await putBufferedStage(store, prepared);
      return seed(A, groups, { ...o, key: prepared.spec.key, sha256: prepared.spec.sha256, digest, claim });
    };
    await make([{ source: "claude", session: "kept", records: [{ id: "yes", rev: 1 }, { id: "no", rev: 1, accepted: false }] }]);
    await make([{ source: "claude", session: "reenabled", records: [{ id: "hidden", rev: 1 }] }], { fenceVersion: 1 });
    expect(await run(store)).toMatchObject({ live: 1, not_live: 1 });
    const drained = await drainBufferedBatches({ store, keyRoot: ROOT, orgs: [A.orgId], dryRun: false, sleep: async () => {},
      decompress: (b) => new Uint8Array(zstdDecompressSync(b, { maxOutputLength: BUFFER_STAGE_MAX_BYTES })) });
    expect(drained.objects).toMatchObject({ drained: 1, stage_not_live: 1, accepted_mismatch: 0, stage_export_missing: 0 });
    expect(drained.lines).toMatchObject({ written: 1, not_kept: 1 });
    // A control after the export moves the S3 fence: the next stage waits for a fresh export.
    const third = await make([{ source: "claude", session: "later", records: [{ id: "x", rev: 1 }] }]);
    await run(store);
    await s3LedgerObjects(store, ROOT).editFence(s3LedgerKeys(ROOT, A).fence, (f) => {
      applyFenceChange(f, { deletions: [{ originId: ORIGIN, sessions: [["claude", "later"]] }] });
      return { next: f, result: undefined };
    });
    const after = await drainBufferedBatches({ store, keyRoot: ROOT, orgs: [A.orgId], dryRun: false, sleep: async () => {},
      decompress: (b) => new Uint8Array(zstdDecompressSync(b, { maxOutputLength: BUFFER_STAGE_MAX_BYTES })) });
    expect(after.objects.fence_moved_since_export).toBe(1);
    expect(await exists(store, third.key + ".drained.json")).toBe(false);
  });

  test("CLI: the mode is never implied and --execute needs the KMS key", () => {
    expect(() => parseExportArgs(["--bucket", "my-bucket"])).toThrow("exactly one of");
    expect(() => parseExportArgs(["--bucket", "my-bucket", "--execute"])).toThrow("--kms-key-arn");
    expect(() => parseExportArgs(["--bucket", "my-bucket", "--dry-run", "--org", "../x"])).toThrow("--org");
    expect(parseExportArgs(["--dry-run", "--bucket", "my-bucket"])).toEqual({ mode: "dry_run", bucket: "my-bucket", region: "us-east-1", orgs: [], pageSize: 1_000, concurrency: 8, statementTimeoutMs: 30_000 });
  });
});
