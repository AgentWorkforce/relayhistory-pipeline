/**
 * FULL-to-drain materialization on a real PostgreSQL and MinIO (hold-harness/run.ts). It is the same chain as
 * accepted-stage-export.pglite.test.ts "FULL then export then drain", on the disposable fixtures:
 *
 *   one buffered batch as admission left it under the lazy policy (SQL rows plus the staged object in the bucket)
 *   → backfillUploadFences (the real FULL: it indexes each owned lazy slot as [revision, null, digest])
 *   → exportAcceptedStages → drainBufferedBatches → verifyDrainMaterialization, record by record.
 *
 *   bun src/session-ingest/__tests__/hold-harness/run.ts --pg 17 -- src/session-ingest/__tests__/drain-materialization.postgres.test.ts
 *
 * Without SESSION_INGEST_TEST_PG_URL it skips, unless SESSION_INGEST_TEST_PG_REQUIRED=1, which the runner sets. Synthetic
 * tenants, hashes and times only (the repo is public).
 */
import { afterAll, beforeAll, describe, expect, setDefaultTimeout, test } from "bun:test";
import { createHash, randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { zstdDecompressSync } from "node:zlib";
import pg from "pg";
import { objectKey, readAll, type ObjectStore } from "../../shared/vendor.ts";
import { exportAcceptedStages, type ExportSql } from "../accepted-stage-export.ts";
import { DRAINED_SUFFIX, drainBufferedBatches, parseStagedKey, readLiveStaged } from "../buffered-drain.ts";
import { verifyDrainMaterialization } from "../drain-materialization.ts";
import { orgFenceAuthorityKey } from "../ledger-s3.ts";
import { BUFFER_STAGE_MAX_BYTES, prepareBufferedStage, putBufferedStage } from "../staging.ts";
import { backfillUploadFences } from "../upload-fence-backfill.ts";
import type { Group } from "../writer.ts";
import { SESSION_BOOTSTRAP_FILES } from "../../hoopdb/session-bootstrap-chain.ts";
import { migrationFiles } from "./pglite-schema.ts";
import { fixturePgUrl } from "./hold-harness/pg.ts";
import { fixtureS3, scratchBucket, type ScratchBucket } from "./hold-harness/s3.ts";

setDefaultTimeout(300_000);
const url = fixturePgUrl();
const ROOT = "96bulls";
const A = { orgId: "org-a", workspaceId: "ws-a", userId: "user-1" };
const ORIGIN = "origin-1";
const hex = (c: string) => c.repeat(64);
const sha = (v: string | Uint8Array) => createHash("sha256").update(v).digest("hex");
const POLICY_IDENTITY = hex("1"), POLICY_EPOCH = hex("2"), CLOCK_EPOCH = hex("3"), CLOCK_IDENTITY = hex("4"), GEN = hex("5");
const RECEIVED = "2030-01-02T00:00:00.000Z";
const repo = join(import.meta.dir, "../../..");
const text = (p: string) => readFileSync(join(repo, p), "utf8").split("\n").filter((l) => !l.startsWith("\\")).join("\n");
const decompress = (b: Uint8Array) => new Uint8Array(zstdDecompressSync(b, { maxOutputLength: BUFFER_STAGE_MAX_BYTES }));

let admin: pg.Client;
let db: pg.Client;
let dbName = "";
let bucket: ScratchBucket | undefined;
let store: ObjectStore;

beforeAll(async () => {
  if (!url) return;
  admin = new pg.Client({ connectionString: url });
  await admin.connect();
  dbName = "drain_mat_" + randomUUID().replaceAll("-", "").slice(0, 20);
  await admin.query(`CREATE DATABASE ${dbName}`);
  const target = new URL(url);
  target.pathname = "/" + dbName;
  db = new pg.Client({ connectionString: target.href });
  await db.connect();
  await db.query(text("supabase/tests/supabase_shim.sql"));
  await db.query("CREATE EXTENSION IF NOT EXISTS pg_trgm; CREATE EXTENSION IF NOT EXISTS btree_gin");
  for (const f of SESSION_BOOTSTRAP_FILES) await db.query(text(f));
  // Through the reader whose predicates the export transcribes (20261105047020), as the PGlite export suite.
  for (const f of migrationFiles(repo, "20261105047020_buffered_session_stages_paged.sql")) await db.query(text(`supabase/migrations/${f}`));
  await db.query("SET session_replication_role = replica");
  await db.query(`INSERT INTO sessions.ingest_lazy_policy (singleton, baseline_mode, generation, source_identity_sha256, control_epoch, privacy_baseline_sha256,
      retention_contract_sha256, source_proof_sha256, issuer, valid_until) VALUES (true, 'lazy-current', $1, $2, $3, $1, $1, $1, 'relayhistory-cloud', now() + interval '1 day')`,
    [GEN, POLICY_IDENTITY, POLICY_EPOCH]);
  await db.query(`INSERT INTO sessions.ingest_receipt_seat_clock (singleton, control_epoch, source_identity_sha256, allocated_high_water, last_seat, baseline, initial_generation)
      VALUES (true, $1, $2, 0, 0, '{}', $3)`, [CLOCK_EPOCH, CLOCK_IDENTITY, GEN]);
  await db.query(`CREATE TABLE IF NOT EXISTS sessions.__96bulls_role_validation(name text PRIMARY KEY, generation text, status text)`);
  await db.query(`INSERT INTO sessions.__96bulls_role_validation (name, generation, status) VALUES ('session_directory_search_worker', $1, 'pass')
      ON CONFLICT (name) DO UPDATE SET generation = excluded.generation, status = 'pass'`, [GEN]);
  await db.query("UPDATE sessions.ingest_claim_sessions_policy SET write_enabled = true, backfill_complete_at = now(), read_enabled = true");
  await db.query("INSERT INTO sessions.ingest_privacy_authority (singleton, state, control_epoch, neon_fingerprint_sha256, activated_at) VALUES (true, 'active', $1, $2, now())", [POLICY_EPOCH, hex("6")]);
  await db.query("SET session_replication_role = origin");
  const s3 = fixtureS3();
  if (!s3) throw new Error("SESSION_INGEST_TEST_S3_* required: run through hold-harness/run.ts");
  bucket = await scratchBucket(s3, { prefix: "drainmat", versioned: true });
  store = bucket.store();
});
afterAll(async () => {
  if (!url) return;
  await db?.end().catch(() => {});
  await admin?.query(`DROP DATABASE IF EXISTS ${dbName} WITH (FORCE)`).catch(() => {});
  await admin?.end().catch(() => {});
  await bucket?.drop();
});

const sql: ExportSql = { async query(t, p) { return { rows: (await db.query(t, p as unknown[])).rows as Record<string, unknown>[] }; } };
type SeedGroup = { source: string; session: string; records: { id: string; rev: number; accepted?: boolean }[] };
const line = (id: string, rev: number) => JSON.stringify({ kind: "session_event", origin_id: ORIGIN, record_id: id, revision: String(rev), operation: "upsert", received_at: RECEIVED, payload: { text: `t-${id}`, ts_ms: 1_893_542_400_000 } });

/** One buffered batch as admission left it under the lazy policy, in SQL and in the bucket (the PGlite suite's seed). */
async function stageInBoth(groups: SeedGroup[]): Promise<string> {
  const batchId = randomUUID(), digest = sha(randomUUID()), claim = randomUUID(), token = randomUUID();
  const objectGroups: Group[] = groups.map((g) => ({ source: g.source, sessionId: g.session, lines: g.records.map((r) => line(r.id, r.rev)), meta: g.records.map((r) => [`${r.id}:r${r.rev}`, sha(`${r.id}:${r.rev}`), g.source, g.session]) }));
  let ord = 0;
  const groupOrdinals = groups.map((g) => g.records.map(() => ord++));
  const prepared = prepareBufferedStage(ROOT, { claim_token: claim, batch_digest: digest, received_at: RECEIVED, org_id: A.orgId, workspace_id: A.workspaceId, origin_id: ORIGIN, user_id: A.userId }, objectGroups, groupOrdinals);
  await putBufferedStage(store, prepared);
  const lines = groups.flatMap((g, gi) => g.records.map((r, j) => ({ ord: groupOrdinals[gi]![j]!, g, r, digest: sha(`${r.id}:${r.rev}`) })));
  const records = lines.map((l) => ({ ord: l.ord, source: l.g.source, session_id: l.g.session, record_id: l.r.id, revision: l.r.rev, digest: l.digest }));
  const sessions = [...new Map(groups.map((g) => [JSON.stringify([g.source, g.session]), g])).values()];
  await db.query("SET session_replication_role = replica");
  await db.query(`INSERT INTO sessions.ingest_batches (org_id, workspace_id, origin_id, batch_id, user_id, digest, receipt_id, state, committed_at, publication_state)
    VALUES ($1,$2,$3,$4,$5,$6,$7,'committed',now(),'buffered')`, [A.orgId, A.workspaceId, ORIGIN, batchId, A.userId, digest, `rhr_${hex("c")}`]);
  await db.query(`INSERT INTO sessions.ingest_claim_attempts (claim_token, org_id, workspace_id, user_id, origin_id, batch_id, digest, epochs, records, reset_epoch, source_control_version, source_identity_sha256)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9::jsonb,0,0,$10)`, [claim, A.orgId, A.workspaceId, A.userId, ORIGIN, batchId, digest,
    JSON.stringify(sessions.map((g) => ({ source: g.source, session_id: g.session, epoch: 0, reset_epoch: 0 }))), JSON.stringify(records), POLICY_IDENTITY]);
  for (const g of sessions)
    await db.query(`INSERT INTO sessions.ingest_claim_sessions (claim_token, source, session_id, org_id, workspace_id, user_id, origin_id, batch_id, digest, reset_epoch, accepted_at)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,0,now())`, [claim, g.source, g.session, A.orgId, A.workspaceId, A.userId, ORIGIN, batchId, digest]);
  await db.query(`INSERT INTO sessions.ingest_stage_intents (stage_token, claim_token, org_id, workspace_id, user_id, origin_id, batch_id, digest, stage_key, stage_sha256, stage_bytes, group_ordinals, reset_epoch, receipt_state, cleanup_state, expires_at)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,100,$11::jsonb,0,'acknowledged','live',now() + interval '1 day')`,
    [token, claim, A.orgId, A.workspaceId, A.userId, ORIGIN, batchId, digest, prepared.spec.key, prepared.spec.sha256, JSON.stringify(groupOrdinals)]);
  await db.query(`INSERT INTO sessions.ingest_buffer_outbox (org_id, workspace_id, origin_id, batch_id, user_id, digest, active_stage_token, state) VALUES ($1,$2,$3,$4,$5,$6,$7,'buffered')`,
    [A.orgId, A.workspaceId, ORIGIN, batchId, A.userId, digest, token]);
  let seat = Math.floor(Math.random() * 1e6);
  for (const l of lines) {
    const seatKey = sha(`seat:${claim}:${l.ord}`), slotKey = sha(`slot:${claim}:${l.ord}`);
    await db.query(`INSERT INTO sessions.ingest_receipt_seats (key_sha256, key_material, seat, first_digest, source, session_id, fence_epoch) VALUES ($1,'[]',$2,$3,$4,$5,0)`,
      [seatKey, ++seat, l.digest, l.g.source, l.g.session]);
    await db.query(`INSERT INTO sessions.ingest_lazy_native_slots (key_sha256, key_material, org_id, workspace_id, origin_id, record_id, source_identity_sha256, control_epoch,
        initialized_witness_sha256, current_owner, current_source, current_session, current_revision, current_digest, current_reset_epoch, current_fence_epoch, current_seat_key)
      VALUES ($1,'[]',$2,$3,$4,$5,$6,$7,$6,$8,$9,$10,$11,$12,0,0,$13)`,
      [slotKey, A.orgId, A.workspaceId, ORIGIN, l.r.id, POLICY_IDENTITY, POLICY_EPOCH, A.userId, l.g.source, l.g.session, l.r.rev, l.digest, seatKey]);
    await db.query(`INSERT INTO sessions.ingest_lazy_ack_decisions (claim_token, ord, native_key_sha256, native_lineage, candidate_key_sha256, candidate_digest, first_digest,
        body_accepted, seat_key_sha256, reset_epoch, fence_epoch, generation, source_control_version, source_witness_sha256)
      SELECT $1, $2, $3, 0, encode(sha256(convert_to(jsonb_build_array('buffer-first-ack-seat-v1', $4::text, $5::text, $6::text, $7::text, $8::text, $9::text,
               $10::text, $11::text, '0'::text, $12::text, $13::text, '0'::text)::text, 'UTF8')), 'hex'),
             $14, $14, $15, $16, 0, 0, $17, 0, $17`,
      [claim, l.ord, slotKey, CLOCK_EPOCH, CLOCK_IDENTITY, A.orgId, A.workspaceId, A.userId, ORIGIN, l.r.id, String(l.r.rev), l.g.source, l.g.session,
        l.digest, l.r.accepted !== false, seatKey, GEN]);
  }
  // The owner's G2 parity proof for the user (no control head).
  await db.query("DELETE FROM sessions.owner_buffered_parity WHERE org_id = $1 AND workspace_id = $2 AND user_id = $3", [A.orgId, A.workspaceId, A.userId]);
  await db.query(`INSERT INTO sessions.owner_buffered_parity (org_id, workspace_id, user_id, cutover_epoch, head_epoch, head_version, head_digest, head_kind, no_head,
      privacy_baseline_sha256, deletion_set_digest, reset_set_digest, target_lineage_digest, target_generation)
    SELECT $1, $2, $3, $4, NULL, NULL, NULL, NULL, true, $5, e.deletion_set_digest, e.reset_set_digest, e.target_lineage_digest, e.target_generation
      FROM sessions.owner_buffered_parity_expected($1, $2, $3, $4, NULL::bigint, NULL::text, NULL::text) AS e`, [A.orgId, A.workspaceId, A.userId, POLICY_EPOCH, GEN]);
  await db.query("SET session_replication_role = origin");
  return prepared.spec.key;
}

async function listKeys(prefix: string): Promise<string[]> {
  const out: string[] = [];
  let cursor: string | undefined;
  do {
    const page = await store.list(objectKey(prefix), cursor);
    out.push(...page.items.map((i) => String(i.key)));
    cursor = page.nextCursor;
  } while (cursor);
  return out.sort();
}

describe.skipIf(!url)("FULL-to-drain materialization on PostgreSQL + MinIO", () => {
  test("the real FULL, export and drain materialize the owned lazy slots; the proof is exact and a re-drain is a no-op", async () => {
    await store.put(objectKey(orgFenceAuthorityKey(ROOT, A.orgId)), new TextEncoder().encode(JSON.stringify({ v: 1, fenceBackfill: "complete", revisionBackfill: "complete",
      ownershipBackfill: "complete", controlMirror: "on", rearmSweeper: "live", revisionLayout: "4096", workerHttpAdmission: "off", writtenAtMs: 1, orgId: A.orgId })));
    const key = await stageInBoth([
      { source: "claude", session: "s1", records: [{ id: "m1", rev: 1 }, { id: "m2", rev: 1, accepted: false }] },
      { source: "claude", session: "s2", records: [{ id: "m3", rev: 1 }] },
    ]);
    const full = await backfillUploadFences({ sql, store, keyRoot: ROOT, dryRun: false, sleep: async () => {} });
    expect(full.revisions.slot_rows).toBe(3);
    expect(await exportAcceptedStages({ sql, store, keyRoot: ROOT, dryRun: false, nowMs: () => 1_893_542_400_000 })).toMatchObject({ live: 1 });
    const drain = () => drainBufferedBatches({ store, keyRoot: ROOT, orgs: [A.orgId], dryRun: false, sleep: async () => {}, decompress });
    const first = await drain();
    expect(first.objects.drained).toBe(1);
    // m2 is not body-accepted (SQL does not keep it); m1 and m3 are FULL slot entries, materialized.
    expect(first.lines).toMatchObject({ candidate: 2, written: 2, materialized: 2, already_stored: 0, not_kept: 1 });
    const deps = { list: listKeys, readStaged: (k: string) => readLiveStaged(store, k, decompress), parseStagedKey };
    const proof = await verifyDrainMaterialization({ store, keyRoot: ROOT, orgId: A.orgId, decompress }, deps);
    expect(proof.failures).toEqual([]);
    expect(proof).toMatchObject({ ok: true, exact: 2, excluded_absent: 1 });
    // A crash before the marker: the rerun reads the bytes back and writes nothing.
    const canonicalKeys = (await listKeys(`${ROOT}/${A.orgId}/v1`));
    await store.delete(objectKey(key + DRAINED_SUFFIX));
    const again = await drain();
    expect(again.lines).toMatchObject({ written: 0, materialized: 0, already_stored: 2 });
    expect(await listKeys(`${ROOT}/${A.orgId}/v1`)).toEqual(canonicalKeys);
    expect((await verifyDrainMaterialization({ store, keyRoot: ROOT, orgId: A.orgId, decompress }, deps)).ok).toBe(true);
    const marker = JSON.parse(new TextDecoder().decode(await readAll(await store.get(objectKey(key + DRAINED_SUFFIX)))));
    expect(marker).toMatchObject({ outcome: "drained", materialization: 1 });
  });
});
