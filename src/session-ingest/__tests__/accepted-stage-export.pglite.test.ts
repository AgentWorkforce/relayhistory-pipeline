import { afterAll, afterEach, beforeAll, describe, expect, setDefaultTimeout, test } from "bun:test";
import { createHash, randomUUID } from "node:crypto";
import { zstdDecompressSync } from "node:zlib";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { objectKey, readAll, type ObjectStore } from "../../shared/vendor.ts";
import { exportAcceptedStages, exportLockKey, type ExportOptions, type ExportSql } from "../accepted-stage-export.ts";
import { ACCEPTED_SUFFIX, SUPERSEDED_SUFFIX, acceptedCompleteKey, acquireDrainLock, drainBufferedBatches, parseAcceptedComplete, parseAcceptedMarker } from "../buffered-drain.ts";
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
  // The reader's per-user gate (owner_buffered_read_scope) holds by default: the role-validation PASS generation (made by
  // the migrator outside the migration set), an ACTIVE privacy authority (so an absent control head is proven absent),
  // and the v2 claim-sessions reader gate on.
  await db.exec(`CREATE TABLE sessions.__96bulls_role_validation(name text PRIMARY KEY, generation text, status text);
    INSERT INTO sessions.__96bulls_role_validation VALUES ('session_directory_search_worker', '${GEN}', 'pass')`);
  await db.exec("UPDATE sessions.ingest_claim_sessions_policy SET write_enabled = true, backfill_complete_at = now(), read_enabled = true");
  await db.exec("SET session_replication_role = origin");
  await setAuthority("active");
});
/** The privacy authority singleton: 'active' (default), 'fenced', or absent (Neon is the authority). */
async function setAuthority(state: "active" | "fenced" | null) {
  await db.exec("SET session_replication_role = replica");
  await db.exec("DELETE FROM sessions.ingest_privacy_authority");
  if (state === "active") await db.query("INSERT INTO sessions.ingest_privacy_authority (singleton, state, control_epoch, neon_fingerprint_sha256, activated_at) VALUES (true, 'active', $1, $2, now())", [POLICY_EPOCH, hex("6")]);
  if (state === "fenced") await db.query("INSERT INTO sessions.ingest_privacy_authority (singleton, state, control_epoch) VALUES (true, 'fenced', $1)", [POLICY_EPOCH]);
  await db.exec("SET session_replication_role = origin");
}
type Head = { version: number; digest: string; kind: "selection" | "reset"; epoch?: string; pending?: boolean };
/** The mirrored control head (ingest_source_control_heads) and, unless `receipts: false`, receipts 1..version. */
async function putHead(t: T, h: Head, o: { receipts?: { version: number; digest: string; kind: string; epoch?: string }[] } = {}) {
  await db.exec("SET session_replication_role = replica");
  await db.query(`INSERT INTO sessions.ingest_source_control_heads (org_id, workspace_id, user_id, epoch, version, digest, kind, payload, pending)
    VALUES ($1,$2,$3,$4,$5,$6,$7,'{}',$8)`, [t.orgId, t.workspaceId, t.userId, h.epoch ?? POLICY_EPOCH, h.version, h.digest, h.kind, h.pending ?? false]);
  const receipts = o.receipts ?? Array.from({ length: h.version }, (_, i) => ({ version: i + 1, digest: i + 1 === h.version ? h.digest : hex("a"), kind: i + 1 === h.version ? h.kind : "selection" }));
  for (const r of receipts)
    await db.query(`INSERT INTO sessions.ingest_control_receipts (org_id, workspace_id, user_id, epoch, version, digest, kind, result, schema_generation)
      VALUES ($1,$2,$3,$4,$5,$6,$7,'{}',$8)`, [t.orgId, t.workspaceId, t.userId, r.epoch ?? POLICY_EPOCH, r.version, r.digest, r.kind, GEN]);
  await db.exec("SET session_replication_role = origin");
}
/**
 * The owner's G2 parity proof for the user, as the writer contract computes it (owner_buffered_parity_expected), bound
 * to the user's mirrored head (or no head). Recomputed from SQL's current deletion and reset sets.
 */
async function prove(t: T) {
  await db.exec("SET session_replication_role = replica");
  await db.query("DELETE FROM sessions.owner_buffered_parity WHERE org_id = $1 AND workspace_id = $2 AND user_id = $3", [t.orgId, t.workspaceId, t.userId]);
  await db.query(`INSERT INTO sessions.owner_buffered_parity (org_id, workspace_id, user_id, cutover_epoch, head_epoch, head_version, head_digest, head_kind, no_head,
      privacy_baseline_sha256, deletion_set_digest, reset_set_digest, target_lineage_digest, target_generation)
    SELECT $1, $2, $3, $4, h.epoch, h.version, h.digest, h.kind, h.org_id IS NULL, $5, e.deletion_set_digest, e.reset_set_digest, e.target_lineage_digest, e.target_generation
      FROM (SELECT 1) AS one
      LEFT JOIN sessions.ingest_source_control_heads AS h ON h.org_id = $1 AND h.workspace_id = $2 AND h.user_id = $3
     CROSS JOIN LATERAL sessions.owner_buffered_parity_expected($1, $2, $3, $4, h.version, h.digest, h.kind) AS e`,
    [t.orgId, t.workspaceId, t.userId, POLICY_EPOCH, GEN]);
  await db.exec("SET session_replication_role = origin");
}
afterAll(async () => { await db.close(); });
let cleanup: (() => void)[] = [];
afterEach(async () => {
  cleanup.forEach((c) => c());
  cleanup = [];
  await db.exec(`SET session_replication_role = replica;
    DELETE FROM sessions.ingest_buffer_outbox; DELETE FROM sessions.ingest_lazy_ack_decisions; DELETE FROM sessions.ingest_lazy_native_slots;
    DELETE FROM sessions.ingest_receipt_seats; DELETE FROM sessions.ingest_stage_intents; DELETE FROM sessions.ingest_claim_attempts;
    DELETE FROM sessions.ingest_batches; DELETE FROM sessions.ingest_fence_versions; DELETE FROM sessions.upload_deletions; DELETE FROM sessions.upload_resets;
    DELETE FROM sessions.ingest_claim_sessions; DELETE FROM sessions.owner_buffered_parity; DELETE FROM sessions.ingest_source_control_heads;
    DELETE FROM sessions.ingest_control_receipts;
    UPDATE sessions.ingest_lazy_policy SET valid_until = now() + interval '1 day';
    UPDATE sessions.ingest_claim_sessions_policy SET write_enabled = true, backfill_complete_at = now(), read_enabled = true;
    SET session_replication_role = origin;`);
  await setAuthority("active");
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
  batchState?: "committed" | "pending"; key?: string; sha256?: string; digest?: string; claim?: string; admVersion?: number; prove?: boolean } = {}): Promise<Seeded> {
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
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9::jsonb,0,$11,$10)`, [claim, t.orgId, t.workspaceId, t.userId, ORIGIN, batchId, digest, JSON.stringify(epochs), JSON.stringify(records), POLICY_IDENTITY, o.admVersion ?? 0]);
  // The v2 claim-sessions side rows (044080), as admission writes them while its writer is on.
  for (const g of sessions)
    await db.query(`INSERT INTO sessions.ingest_claim_sessions (claim_token, source, session_id, org_id, workspace_id, user_id, origin_id, batch_id, digest, reset_epoch, accepted_at)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,0,now())`, [claim, g.source, g.session, t.orgId, t.workspaceId, t.userId, ORIGIN, batchId, digest]);
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
             $15, $15, $16, $17, 0, $18, $19, $20, $19`,
      [claim, l.ord, slotKey, CLOCK_EPOCH, CLOCK_IDENTITY, t.orgId, t.workspaceId, t.userId, ORIGIN, l.r.id, String(l.r.rev), l.g.source, l.g.session, String(epoch),
        l.digest, l.r.accepted !== false, seatKey, epoch, GEN, o.admVersion ?? 0]);
  }
  if (o.fenceVersion !== undefined)
    for (const g of sessions)
      await db.query(`INSERT INTO sessions.ingest_fence_versions (org_id, workspace_id, user_id, origin_id, source, session_id, epoch) VALUES ($1,$2,$3,$4,$5,$6,$7)
        ON CONFLICT (org_id, workspace_id, user_id, origin_id, source, session_id) DO UPDATE SET epoch = excluded.epoch`,
        [t.orgId, t.workspaceId, t.userId, ORIGIN, g.source, g.session, o.fenceVersion]);
  await db.exec("SET session_replication_role = origin");
  if (o.prove !== false) await prove(t);
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

const grantAuthority = (store: ObjectStore) =>
  store.put(objectKey(orgFenceAuthorityKey(ROOT, A.orgId)), enc.encode(JSON.stringify({ v: 1, fenceBackfill: "complete", revisionBackfill: "complete",
    ownershipBackfill: "complete", controlMirror: "on", rearmSweeper: "live", workerHttpAdmission: "off", writtenAtMs: 1, orgId: A.orgId })));
const objectLine = (id: string) => JSON.stringify({ kind: "session_event", origin_id: ORIGIN, record_id: id, revision: "1", operation: "upsert", received_at: "2030-01-02T00:00:00.000Z", payload: { text: id, ts_ms: 1_893_542_400_000 } });
/** A real staged object in S3 for user A, and the same batch in SQL (seed). */
async function stageInBoth(store: ObjectStore, groups: SeedGroup[], o: Parameters<typeof seed>[2] = {}) {
  const claim = randomUUID(), digest = sha(randomUUID());
  const objectGroups: Group[] = groups.map((g) => ({ source: g.source, sessionId: g.session, lines: g.records.map((r) => objectLine(r.id)), meta: g.records.map((r) => [`${r.id}:r${r.rev}`, sha(`${r.id}:${r.rev}`), g.source, g.session]) }));
  let ord = 0;
  const prepared = prepareBufferedStage(ROOT, { claim_token: claim, batch_digest: digest, received_at: "2030-01-02T00:00:00.000Z", org_id: A.orgId, workspace_id: A.workspaceId, origin_id: ORIGIN, user_id: A.userId },
    objectGroups, groups.map((g) => g.records.map(() => ord++)));
  await putBufferedStage(store, prepared);
  return seed(A, groups, { ...o, key: prepared.spec.key, sha256: prepared.spec.sha256, digest, claim });
}
const drain = (store: ObjectStore) => drainBufferedBatches({ store, keyRoot: ROOT, orgs: [A.orgId], dryRun: false, sleep: async () => {},
  decompress: (b) => new Uint8Array(zstdDecompressSync(b, { maxOutputLength: BUFFER_STAGE_MAX_BYTES })) });

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
    await prove(A); // the owner's proof follows SQL's reset set: the line predicates alone decide here
    await run(store);
    expect((await verdict(store, reset.key))!.live).toBe(false);
    await db.exec("DELETE FROM sessions.upload_resets");
    const deleted = await seed(A, [{ source: "claude", session: "gone", records: [{ id: "b", rev: 1 }] }]);
    await db.query("INSERT INTO sessions.upload_deletions (org_id, workspace_id, user_id, origin_id, source, session_id) VALUES ($1,$2,$3,$4,'claude','gone')", [A.orgId, A.workspaceId, A.userId, ORIGIN]);
    await prove(A);
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
    await grantAuthority(store);
    const make = (groups: SeedGroup[], o: Parameters<typeof seed>[2] = {}) => stageInBoth(store, groups, o);
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


  // ---- second review: the reader's per-user gate (3), the control head (4), the gate-off path (5), fence order (6),
  // stale sidecars (7) ----

  const one = [{ source: "claude", session: "s1", records: [{ id: "a", rev: 1 }] }];
  /** Export, then the stage's verdict and the summary. */
  const exportOne = async (store: ObjectStore, s: Seeded) => {
    const summary = await run(store);
    return { summary, v: (await verdict(store, s.key))! };
  };

  test("finding 3: a fenced privacy authority hides every user (counted with a reason)", async () => {
    const store = scratch();
    await backfillFence(store, A);
    const s = await seed(A, one);
    await setAuthority("fenced");
    const { summary, v } = await exportOne(store, s);
    expect(v.live).toBe(false);
    expect(summary).toMatchObject({ live: 0, not_live: 1, gated: { privacy_fenced: 1 } });
  });

  test("finding 3: an expired lazy policy hides the user", async () => {
    const store = scratch();
    await backfillFence(store, A);
    const s = await seed(A, one);
    await db.exec("UPDATE sessions.ingest_lazy_policy SET valid_until = now() - interval '1 second'");
    const { summary, v } = await exportOne(store, s);
    expect(v.live).toBe(false);
    expect(summary.gated).toMatchObject({ policy_invalid: 1 });
  });

  test("finding 3: no parity row, or one whose digests no longer match SQL, hides the user", async () => {
    const store = scratch();
    await backfillFence(store, A);
    const s = await seed(A, one, { prove: false });
    // With no head, a missing proof fails the absent-head proof first (head_unproven); with a head it is parity_missing.
    expect((await exportOne(store, s)).summary.gated).toMatchObject({ head_unproven: 1 });
    expect((await verdict(store, s.key))!.live).toBe(false);
    await prove(A);
    expect((await exportOne(store, s)).v.live).toBe(true);
    // A deletion of ANOTHER session after the proof: the line predicates still pass, the deletion-set digest does not.
    await db.query("INSERT INTO sessions.upload_deletions (org_id, workspace_id, user_id, origin_id, source, session_id) VALUES ($1,$2,$3,$4,'claude','other')", [A.orgId, A.workspaceId, A.userId, ORIGIN]);
    const after = await exportOne(store, s);
    expect(after.v.live).toBe(false);
    expect(after.summary.gated).toMatchObject({ parity_mismatch: 1 });
  });

  test("finding 3: a control pending at the head hides the user", async () => {
    const store = scratch();
    await backfillFence(store, A);
    await putHead(A, { version: 1, digest: hex("d"), kind: "selection", pending: true });
    const s = await seed(A, one, { admVersion: 1 });
    const { summary, v } = await exportOne(store, s);
    expect(v.live).toBe(false);
    expect(summary.gated).toMatchObject({ head_pending: 1 });
  });

  test("finding 4: the mirrored head proves receipts 1..v (tip digest and kind) and sets the admission version", async () => {
    const store = scratch();
    await backfillFence(store, A);
    await putHead(A, { version: 2, digest: hex("d"), kind: "reset" });
    const ok = await seed(A, one, { admVersion: 2 });
    const older = await seed(A, [{ source: "claude", session: "s2", records: [{ id: "b", rev: 1 }] }], { admVersion: 1 });
    const summary = await run(store);
    expect((await verdict(store, ok.key))!.live).toBe(true);
    // Admitted before the latest control: not live by design (the drain's fence recheck covers later controls).
    expect((await verdict(store, older.key))!.live).toBe(false);
    expect(summary).toMatchObject({ live: 1, not_live: 1 });
  });

  test("finding 4: never max(receipts): a head behind the receipts, a gap, a foreign epoch or a tip mismatch is not live", async () => {
    const cases: [string, (t: T) => Promise<number>][] = [
      // The head says 1; receipts run to 2 (max would say 2, the claim's version).
      ["head behind receipts", async (t) => { await putHead(t, { version: 1, digest: hex("d"), kind: "selection" }, { receipts: [{ version: 1, digest: hex("d"), kind: "selection" }, { version: 2, digest: hex("e"), kind: "selection" }] }); return 2; }],
      ["gap", async (t) => { await putHead(t, { version: 2, digest: hex("d"), kind: "selection" }, { receipts: [{ version: 2, digest: hex("d"), kind: "selection" }] }); return 2; }],
      ["tip digest", async (t) => { await putHead(t, { version: 2, digest: hex("d"), kind: "selection" }, { receipts: [{ version: 1, digest: hex("a"), kind: "selection" }, { version: 2, digest: hex("e"), kind: "selection" }] }); return 2; }],
      ["tip kind", async (t) => { await putHead(t, { version: 1, digest: hex("d"), kind: "selection" }, { receipts: [{ version: 1, digest: hex("d"), kind: "reset" }] }); return 1; }],
      ["foreign-epoch receipt", async (t) => { await putHead(t, { version: 1, digest: hex("d"), kind: "selection" }, { receipts: [{ version: 1, digest: hex("d"), kind: "selection" }, { version: 1, digest: hex("d"), kind: "selection", epoch: hex("9") }] }); return 1; }],
      ["foreign-epoch head", async (t) => { await putHead(t, { version: 1, digest: hex("d"), kind: "selection", epoch: hex("9") }, { receipts: [{ version: 1, digest: hex("d"), kind: "selection", epoch: hex("9") }] }); return 1; }],
      ["no head, receipts", async (t) => {
        await db.exec("SET session_replication_role = replica");
        await db.query(`INSERT INTO sessions.ingest_control_receipts (org_id, workspace_id, user_id, epoch, version, digest, kind, result, schema_generation) VALUES ($1,$2,$3,$4,1,$5,'selection','{}',$6)`, [t.orgId, t.workspaceId, t.userId, POLICY_EPOCH, hex("d"), GEN]);
        await db.exec("SET session_replication_role = origin");
        return 1;
      }],
    ];
    for (const [name, arrange] of cases) {
      const store = scratch();
      await backfillFence(store, A);
      const version = await arrange(A);
      const s = await seed(A, one, { admVersion: version });
      const { v, summary } = await exportOne(store, s);
      expect([name, v.live]).toEqual([name, false]);
      expect([name, summary.live]).toEqual([name, 0]);
      await db.exec(`SET session_replication_role = replica; DELETE FROM sessions.ingest_source_control_heads; DELETE FROM sessions.ingest_control_receipts;
        DELETE FROM sessions.owner_buffered_parity; DELETE FROM sessions.ingest_buffer_outbox; SET session_replication_role = origin;`);
    }
  });

  test("finding 4: an absent head counts as 0 only when proven (an active privacy authority); else not live", async () => {
    const store = scratch();
    await backfillFence(store, A);
    const s = await seed(A, one);
    expect((await exportOne(store, s)).v.live).toBe(true);
    await setAuthority(null); // Neon is the authority: the Supabase mirror may lack a head
    const { summary, v } = await exportOne(store, s);
    expect(v.live).toBe(false);
    expect(summary.gated).toMatchObject({ head_unproven: 1 });
  });

  test("finding 4 (third round): an absent head is 0 only with an active authority on the policy's epoch, no receipt anywhere, and a no-head proof", async () => {
    const cases: [string, () => Promise<void>][] = [
      ["authority on another epoch", async () => {
        await db.exec("SET session_replication_role = replica");
        await db.query("UPDATE sessions.ingest_privacy_authority SET control_epoch = $1", [hex("9")]);
        await db.exec("SET session_replication_role = origin");
      }],
      ["a receipt on another epoch", async () => {
        await db.exec("SET session_replication_role = replica");
        await db.query(`INSERT INTO sessions.ingest_control_receipts (org_id, workspace_id, user_id, epoch, version, digest, kind, result, schema_generation) VALUES ($1,$2,$3,$4,1,$5,'selection','{}',$6)`, [A.orgId, A.workspaceId, A.userId, hex("9"), hex("d"), GEN]);
        await db.exec("SET session_replication_role = origin");
        await prove(A);
      }],
      ["no parity row", async () => {
        await db.exec("SET session_replication_role = replica; DELETE FROM sessions.owner_buffered_parity; SET session_replication_role = origin;");
      }],
      ["a parity row proving a head", async () => {
        await db.exec("SET session_replication_role = replica");
        await db.query("UPDATE sessions.owner_buffered_parity SET no_head = false, head_epoch = $1, head_version = 1, head_digest = $2, head_kind = 'selection'", [POLICY_EPOCH, hex("d")]);
        await db.exec("SET session_replication_role = origin");
      }],
    ];
    for (const [name, arrange] of cases) {
      const store = scratch();
      await backfillFence(store, A);
      const s = await seed(A, one);
      expect([name, (await exportOne(store, s)).v.live]).toEqual([name, true]); // all four hold: version 0
      await arrange();
      const { summary, v } = await exportOne(store, s);
      expect([name, v.live, summary.gated]).toEqual([name, false, { head_unproven: 1 }]);
      await db.exec(`SET session_replication_role = replica; DELETE FROM sessions.ingest_control_receipts; DELETE FROM sessions.owner_buffered_parity;
        DELETE FROM sessions.ingest_buffer_outbox; SET session_replication_role = origin;`);
      await setAuthority("active");
    }
  });

  test("finding 3: with a proven head, a missing parity row is parity_missing", async () => {
    const store = scratch();
    await backfillFence(store, A);
    await putHead(A, { version: 1, digest: hex("d"), kind: "selection" });
    const s = await seed(A, one, { admVersion: 1, prove: false });
    const { summary, v } = await exportOne(store, s);
    expect(v.live).toBe(false);
    expect(summary.gated).toEqual({ parity_missing: 1 });
  });

  test("third review: the export and the drain exclude each other, in both modes, both directions", async () => {
    const store = scratch();
    await backfillFence(store, A);
    await grantAuthority(store);
    const s = await stageInBoth(store, one);
    const T0 = 1_893_542_400_000;
    const drainRun = (dryRun: boolean) => drainBufferedBatches({ store, keyRoot: ROOT, orgs: [A.orgId], dryRun, sleep: async () => {}, now: () => new Date(T0),
      decompress: (b) => new Uint8Array(zstdDecompressSync(b, { maxOutputLength: BUFFER_STAGE_MAX_BYTES })) });
    // A live drain lease: the export refuses in both modes and writes nothing.
    const drainLease = await acquireDrainLock(store, ROOT, { holder: "drain-other", ttlMs: 60_000, now: () => T0 });
    await expect(run(store, { dryRun: true })).rejects.toThrow("drain_peer_locked");
    await expect(run(store)).rejects.toThrow("drain_peer_locked");
    expect(await exists(store, s.key + ACCEPTED_SUFFIX)).toBe(false);
    // The refused export expired its own lease in place: it does not block anyone.
    expect(JSON.parse(await readText(store, exportLockKey(ROOT))).expiresAtMs).toBe(0);
    await drainLease.release();
    expect((await run(store)).written).toBe(1);
    // A live export lease: the drain refuses in both modes and writes nothing.
    const exportLease = await acquireDrainLock(store, ROOT, { holder: "export-other", ttlMs: 60_000, now: () => T0, key: exportLockKey(ROOT) });
    await expect(drainRun(true)).rejects.toThrow("drain_peer_locked");
    await expect(drainRun(false)).rejects.toThrow("drain_peer_locked");
    expect(await exists(store, s.key + ".drained.json")).toBe(false);
    await exportLease.release();
    expect((await drainRun(false)).objects.drained).toBe(1);
  });

  test("finding 5: with the claim-sessions reader gate off, the export refuses before any write", async () => {
    const store = scratch();
    await backfillFence(store, A);
    await seed(A, one);
    await db.exec("UPDATE sessions.ingest_claim_sessions_policy SET read_enabled = false");
    const before = await allKeys(store);
    await expect(run(store)).rejects.toThrow("export_claim_sessions_gate_off");
    expect((await allKeys(store)).filter((k) => !k.startsWith(exportLockKey(ROOT)))).toEqual(before.filter((k) => !k.startsWith(exportLockKey(ROOT))));
    expect(await exists(store, acceptedCompleteKey(ROOT, A.orgId))).toBe(false);
  });

  test("finding 6: the S3 fence is read BEFORE the SQL snapshot: a disable and re-enable right after it is caught", async () => {
    const store = scratch();
    await backfillFence(store, A);
    await grantAuthority(store);
    const s = await stageInBoth(store, one);
    // Right after the verdict snapshot, a disable then re-enable lands: S3 bumps the session's epoch and drops the
    // tombstone (SQL moves too, but the snapshot already said live).
    let landed = false;
    const sql: ExportSql = {
      async query(text, params) {
        const rows = (await db.query(text, params as unknown[])).rows as Record<string, unknown>[];
        if (!landed && text.includes("judged AS")) {
          landed = true;
          await s3LedgerObjects(store, ROOT).editFence(s3LedgerKeys(ROOT, A).fence, (f) => {
            applyFenceChange(f, { deletions: [{ originId: ORIGIN, sessions: [["claude", "s1"]] }] });
            f.deleted[ORIGIN] = (f.deleted[ORIGIN] ?? []).filter(([src, id]) => !(src === "claude" && id === "s1")); // the re-enable
            return { next: f, result: undefined };
          });
        }
        return { rows };
      },
    };
    await exportAcceptedStages({ sql, store, keyRoot: ROOT, dryRun: false, nowMs: () => 1_893_542_400_000 });
    expect(landed).toBe(true);
    expect((await verdict(store, s.key))!.fence!.epochs[sessionKeyHash("claude", "s1")]).toBe(0); // the pre-control view
    const drained = await drain(store);
    expect(drained.objects).toMatchObject({ drained: 0, fence_moved_since_export: 1 });
  });

  test("finding 7: a stage that left the candidate set gets a create-only superseded record the drain honours", async () => {
    const store = scratch();
    await backfillFence(store, A);
    await grantAuthority(store);
    const s = await stageInBoth(store, one);
    const keep = await stageInBoth(store, [{ source: "claude", session: "s2", records: [{ id: "b", rev: 1 }] }]);
    expect(await run(store)).toMatchObject({ live: 2 });
    expect((await verdict(store, s.key))!.live).toBe(true);
    // The stage is no longer a candidate (its outbox row is gone), but its live sidecar remains from the first export.
    await db.exec("SET session_replication_role = replica");
    await db.query("DELETE FROM sessions.ingest_buffer_outbox WHERE active_stage_token = (SELECT stage_token FROM sessions.ingest_stage_intents WHERE stage_key = $1)", [s.key]);
    await db.exec("SET session_replication_role = origin");
    const dry = await run(store, { dryRun: true });
    expect(dry).toMatchObject({ would_supersede: 1, superseded: 0 });
    const rerun = await run(store);
    expect(rerun).toMatchObject({ rows: 1, superseded: 1 });
    const record = JSON.parse(await readText(store, s.key + SUPERSEDED_SUFFIX));
    expect(record).toEqual({ v: 1, kind: "buffered-superseded", stage_sha256: s.sha256 });
    expect(await exists(store, keep.key + SUPERSEDED_SUFFIX)).toBe(false);
    // A third run changes nothing (the record is create-only and already there).
    expect(await run(store)).toMatchObject({ superseded: 0 });
    const drained = await drain(store);
    expect(drained.objects).toMatchObject({ drained: 1, stage_superseded: 1 });
    expect(await exists(store, s.key + ".drained.json")).toBe(false);
  });

  test("CLI: the mode is never implied and --execute needs the KMS key", () => {
    expect(() => parseExportArgs(["--bucket", "my-bucket"])).toThrow("exactly one of");
    expect(() => parseExportArgs(["--bucket", "my-bucket", "--execute"])).toThrow("--kms-key-arn");
    expect(() => parseExportArgs(["--bucket", "my-bucket", "--dry-run", "--org", "../x"])).toThrow("--org");
    expect(parseExportArgs(["--dry-run", "--bucket", "my-bucket"])).toEqual({ mode: "dry_run", bucket: "my-bucket", region: "us-east-1", orgs: [], pageSize: 1_000, concurrency: 8, statementTimeoutMs: 30_000 });
  });
});
