/**
 * TEST-ONLY: the frozen-supabase custody fixture shared by sync-admission-frozen-receipt.pglite.test.ts
 * (#387) and g4-deploy-settings.pglite.test.ts. Real buffered SQL on PGlite: the schema (pglite-schema.ts,
 * so `until` / HOOPDB_MIGRATE_UNTIL can cut it off), the role-validation generation, the lazy custody
 * policy, the first-ACK receipt seat, a sealed frozen copy (ingest_frozen_source_completeness), a
 * scratch store, and `legacy` to store a pre-cutover receipt in sessions.ingest_frozen_source_receipts.
 */
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { sqlLedger } from "../ledger-sql.ts";
import { prepareDelivery } from "../vendor/delivery/prepare.ts";
import type { HistoryExportBatch } from "../vendor/delivery/delivery-contracts.ts";
import { applySupabaseSchema, guardUndefinedObjects, type UndefinedObjectError } from "./pglite-schema.ts";
import { TENANT, scratchStore } from "./support.ts";

export const FROZEN_GENERATION = "a".repeat(64);
export const FROZEN_EPOCH = "3".repeat(64);
export const FROZEN_SOURCE_IDENTITY = "c".repeat(64);
export const FROZEN_BASELINE = "a".repeat(64);

export interface FrozenFixtureOptions {
  /** Migration cutoff (pglite-schema.ts migrationFiles): omitted = HOOPDB_MIGRATE_UNTIL if set, null = every migration. */
  until?: string | null;
  /** Install the undefined-object guard before anything runs (`undefinedObjects` is then the live record). */
  guard?: boolean;
  /** Called with every RPC statement the ingest role runs through `run` (before it runs). */
  onStatement?: (sql: string) => void;
}

/** `onClose` receives every close callback; the caller runs them (reverse order) after each test. */
export async function frozenSupabaseFixture(onClose: (close: () => Promise<unknown> | void) => void, options: FrozenFixtureOptions = {}) {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } }); onClose(() => db.close());
  const undefinedObjects: UndefinedObjectError[] = options.guard ? guardUndefinedObjects(db) : [];
  const applied = await applySupabaseSchema(db, options.until === undefined ? {} : { until: options.until });
  const generation = FROZEN_GENERATION, epoch = FROZEN_EPOCH, sourceIdentity = FROZEN_SOURCE_IDENTITY, baseline = FROZEN_BASELINE;
  await db.exec("CREATE TABLE sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text);INSERT INTO sessions.__96bulls_role_validation VALUES('session_directory_search_worker','" + generation + "','pass')");
  const owner = async (name: string, p: unknown) => (await db.query("SELECT sessions." + name + "($1::jsonb) AS r", [JSON.stringify(p)])).rows[0];
  await owner("ingest_lazy_policy_set", { baselineMode: "lazy-current", generation, sourceIdentitySha256: sourceIdentity, controlEpoch: epoch, privacyBaselineSha256: baseline, retentionContractSha256: "b".repeat(64), sourceProofSha256: "d".repeat(64), validUntil: new Date(Date.now() + 3600000).toISOString(), issuer: "relayhistory-cloud" });
  await owner("ingest_receipt_seat_initialize", { protocol: "delivery-acceptance-floor-v1", controlEpoch: epoch, capturedAt: "2026-11-04T00:00:00.000000Z", sourceSequences: ["sessions.delivery_catalog_acceptance_seq", "sessions.delivery_projection_acceptance_seq"].map((sequenceRegclass, i) => ({ sequenceRegclass, sequenceOid: String(i + 1), allocatedHighWater: "9007199254740993", isCalled: true, incrementBy: "1", cycle: false })), allocatedHighWater: "9007199254740993", generation, sourceIdentitySha256: sourceIdentity, sourceProofSha256: "d".repeat(64) });
  await db.query("INSERT INTO sessions.ingest_frozen_source_completeness(source_identity_sha256,control_epoch,privacy_baseline_sha256,org_count,record_rows,manifest_sha256,neon_snapshot) VALUES($1,$2,$3,0,0,$4,'{}')", [sourceIdentity, epoch, baseline, "e".repeat(64)]);
  /** The Worker's Hyperdrive `run`, as bulls96_session_ingest, one transaction per statement. */
  const run = async (s: string, p: unknown[] = []) => {
    options.onStatement?.(s);
    await db.exec("BEGIN;SET LOCAL ROLE bulls96_session_ingest");
    try { const r = await db.query(s, p as never); await db.exec("COMMIT"); return r as never; } catch (e) { await db.exec("ROLLBACK"); throw e; }
  };
  const fs = scratchStore(); onClose(fs.done);
  /** A legacy (pre-cutover) receipt for this batch, stored with `digest`. */
  const legacy = async (b: HistoryExportBatch, digest: string, withReceiptId = true) => {
    const receipt = { protocolVersion: 1, batchId: b.batch_id, acceptanceLevel: "durable", acceptedRevisionIds: b.records.map((r) => r.revision_id), unsupportedRevisionIds: [], ...(withReceiptId ? { receiptId: "rhr_legacy" } : {}) };
    await db.query("INSERT INTO sessions.ingest_frozen_source_receipts VALUES($1,$2,$3,$4,$5,$6)", [TENANT.orgId, TENANT.workspaceId, b.origin_id, b.batch_id, digest, JSON.stringify(receipt)]);
    return receipt;
  };
  return { db, run, applied, undefinedObjects, store: fs.store, ledger: sqlLedger(run, { acceptanceMode: "buffered", expectedGeneration: generation }), legacy };
}

/** The batch digest the claim (and the frozen receipt comparison) uses. */
export const digestOf = async (b: HistoryExportBatch) => (await prepareDelivery(TENANT, b)).batchDigest;
