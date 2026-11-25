import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import type { CurrentCustodyPacket } from "../custody-protocol.ts";
import { deliverBatch } from "../deliver.ts";
import { prepareDelivery } from "../vendor/delivery/prepare.ts";
import { frozenSupabaseFixture, FROZEN_GENERATION } from "./frozen-supabase-support.ts";
import { batch, record, TENANT } from "./support.ts";

const target = { generation: FROZEN_GENERATION, keyRoot: "96bulls", bucket: "synthetic", region: "us-east-1" };
let f: Awaited<ReturnType<typeof frozenSupabaseFixture>>;
const cleanup: (() => Promise<unknown> | void)[] = [];

async function stage(batchId: string) {
  const raw = await batch([record({ record_id: batchId, session_id: batchId })], { batch_id: batchId });
  const p = await prepareDelivery(TENANT, raw);
  const witness = {
    kind: "batch", tenant: TENANT, originId: p.originId, batchId: p.batchId,
    batchDigest: p.batchDigest, originalRecordCount: raw.records.length,
    candidates: p.records.map((r, i) => ({ preparedOrd: i, recordId: r.record_id,
      revision: String(r.revision), revisionId: r.revision_id, candidateDigest: r.digest,
      source: r.source, sessionId: r.session_id })),
  };
  const result: { rows: { r: CurrentCustodyPacket }[] } = await f.run(
    "SELECT sessions.ingest_frozen_source_witness($1::jsonb) AS r", [JSON.stringify(witness)]);
  await deliverBatch({ ledger: f.ledger, target: { store: f.store, keyRoot: target.keyRoot },
    acceptanceMode: "buffered", custody: result.rows[0]!.r }, TENANT, raw);
  const outbox = await f.db.query<{ token: string }>(
    "SELECT active_stage_token AS token FROM sessions.ingest_buffer_outbox WHERE batch_id=$1", [batchId]);
  return outbox.rows[0]!.token;
}

async function candidates(limit = 20, overrides: Partial<typeof target> = {}) {
  const result: { rows: { r: { stageTokens: string[] } }[] } = await f.run(
    "SELECT sessions.ingest_publication_candidates($1::jsonb) AS r", [JSON.stringify({ ...target, ...overrides, limit })]);
  return result.rows[0]!.r.stageTokens;
}

async function installFrontier() {
  await f.db.query(`INSERT INTO sessions.ingest_publication_frontiers
    (org_id,workspace_id,user_id,origin_id,source_frontier_sha256,record_maxima_sha256,valid_until,target_bucket,target_region,key_root,schema_generation)
    VALUES ($1,$2,$3,'origin-1',repeat('b',64),repeat('c',64),now()+interval '1 hour',$4,$5,$6,$7)`,
    [TENANT.orgId, TENANT.workspaceId, TENANT.userId, target.bucket, target.region, target.keyRoot, target.generation]);
}

describe("migrated publication candidate RPC on synthetic buffered receipts", () => {
  beforeEach(async () => { f = await frozenSupabaseFixture((close) => cleanup.push(close), { until: null }); }, 60000);
  afterEach(async () => { for (const close of cleanup.splice(0).reverse()) await close(); });

  test("each call reconsiders older newly eligible rows; ties use C-order batch IDs before LIMIT", async () => {
    const older = await stage("older");
    const lowercase = await stage("b-tie");
    const uppercase = await stage("Z-tie");
    await f.db.exec(`UPDATE sessions.ingest_buffer_outbox SET created_at =
      CASE WHEN batch_id='older' THEN timestamptz '2026-01-01' ELSE timestamptz '2026-01-02' END`);
    await installFrontier();
    await f.db.query(`INSERT INTO sessions.ingest_publication_deferrals(stage_token,retry_after,reason)
      VALUES ($1,now()+interval '1 hour','40001')`, [older]);

    expect(await candidates(1)).toEqual([uppercase]);
    expect(await candidates()).toEqual([uppercase, lowercase]);
    await f.db.query("UPDATE sessions.ingest_publication_deferrals SET retry_after=now()-interval '1 hour' WHERE stage_token=$1", [older]);
    expect(await candidates(1)).toEqual([older]);
    expect(await candidates()).toEqual([older, uppercase, lowercase]);

    // A separate committed delivery becomes visible between polls. Synthetic
    // owner-set created_at models a delayed commit without pretending PGlite
    // runs two concurrent connections.
    const late = await stage("visible-late");
    await f.db.query("UPDATE sessions.ingest_buffer_outbox SET created_at=timestamptz '2025-12-31' WHERE batch_id=$1", ["visible-late"]);
    expect(await candidates(1)).toEqual([late]);
    expect(await candidates()).toEqual([late, older, uppercase, lowercase]);
  });

  test("holds absent, expired, wrong-generation and wrong-target frontiers", async () => {
    const token = await stage("frontier-case");
    expect(await candidates()).toEqual([]);
    await installFrontier();
    expect(await candidates()).toEqual([token]);
    for (const mismatch of [{ bucket: "elsewhere" }, { keyRoot: "elsewhere" }, { region: "us-east-2" }])
      expect(await candidates(20, mismatch)).toEqual([]);
    await expect(candidates(20, { generation: "f".repeat(64) })).rejects.toMatchObject({ code: "55000" });
    await f.db.query("UPDATE sessions.ingest_publication_frontiers SET schema_generation=$1", ["e".repeat(64)]);
    expect(await candidates()).toEqual([]);
    await f.db.query("UPDATE sessions.ingest_publication_frontiers SET schema_generation=$1,valid_until=now()-interval '1 hour'", [target.generation]);
    expect(await candidates()).toEqual([]);
  });

  test("rechecks durable receipt state, cleanup and privacy while another candidate remains eligible", async () => {
    const held = await stage("hold-case");
    const other = await stage("control-case");
    await installFrontier();
    expect(await candidates()).toEqual([held, other]);
    await f.db.query("UPDATE sessions.ingest_stage_intents SET receipt_state='preparing' WHERE stage_token=$1", [held]);
    expect(await candidates()).toEqual([other]);
    await f.db.query("UPDATE sessions.ingest_stage_intents SET receipt_state='acknowledged',cleanup_state='retired' WHERE stage_token=$1", [held]);
    expect(await candidates()).toEqual([other]);
    await f.db.query("UPDATE sessions.ingest_stage_intents SET cleanup_state='live' WHERE stage_token=$1", [held]);
    expect(await candidates()).toEqual([held, other]);
    await f.db.query(`INSERT INTO sessions.upload_deletions(org_id,workspace_id,user_id,origin_id,source,session_id)
      VALUES ($1,$2,$3,'origin-1','claude','hold-case')`, [TENANT.orgId, TENANT.workspaceId, TENANT.userId]);
    expect(await candidates()).toEqual([other]);
    await f.db.query("UPDATE sessions.ingest_buffer_outbox SET state='erased',active_stage_token=NULL WHERE active_stage_token=$1", [other]);
    expect(await candidates()).toEqual([]);
  });
});
