/**
 * The synthetic org for the cutover qualification: one org on the buffered SQL ledger with a backlog admitted through
 * the REAL buffered path (deliverBatch over sqlLedger, custody witness, every migration on main), plus the erasures
 * and live uploads the harness drives during the cutover. Local backend: PGlite (every supabase migration through
 * main) and a filesystem object store under $TMPDIR. Synthetic ids only (the repository is public).
 *
 * What is seeded below the real entry points, and why (each is named in the report):
 *   - owner_buffered_parity: the owner's G2 parity proof per user (the export's per-user gate), as
 *     accepted-stage-export.pglite.test.ts seeds it. Production has it from the parity job.
 *   - ingest_privacy_authority 'active' on the policy epoch, set after the backlog is admitted (likewise).
 *   - upload_deletions (a user's own session deletion): the S3 fence through the real S3 ledger, the SQL row at the
 *     table (a lazy-policy SQL selection is an ordered control that needs Neon custody; see selectionDeletion).
 *   - session_erasure_fences: the operator session erase entry refuses a buffered org (RH011,
 *     ingest_operator_erase_session), so a session erasure of a buffered org is recorded at the table, as the
 *     062340 suite does. The 062360 manifest lane (hard-delete-owner) owns the real path.
 */
import { createHash } from "node:crypto";
import { deliverBatch, type DeliverResult } from "../../src/session-ingest/deliver.ts";
import { sqlLedger } from "../../src/session-ingest/ledger-sql.ts";
import { s3Ledger } from "../../src/session-ingest/ledger-s3.ts";
import { deliveryAccount, prepareDelivery } from "../../src/session-ingest/vendor/delivery/prepare.ts";
import { batch, record } from "../../src/session-ingest/__tests__/support.ts";
import { epoch, generation, identity } from "../../src/session-ingest/__tests__/privacy-authority-support.ts";
import { pgliteBackend, type Backend } from "./backend.ts";
import type { LedgerTenant } from "../../src/session-ingest/ledger.ts";
import type { ObjectStore } from "../../src/shared/vendor.ts";

export const KEY_ROOT = "96bulls";
export const ORG = "org-synth-cutover";
export const WORKSPACE = "ws-synth-cutover";
export const SOURCE = "claude";

export interface FixtureSize {
  users: number;
  sessionsPerUser: number;
  batchesPerSession: number;
  recordsPerBatch: number;
  /** Bytes of synthetic text per record payload. */
  payloadBytes: number;
}
/** CI preset: small enough for a unit shard. */
export const SIZE_CI: FixtureSize = { users: 8, sessionsPerUser: 2, batchesPerSession: 2, recordsPerBatch: 5, payloadBytes: 256 };
/** The representative preset (the reported run): 1,536 buffered batches, 30,720 records. */
export const SIZE_REPRESENTATIVE: FixtureSize = { users: 32, sessionsPerUser: 12, batchesPerSession: 4, recordsPerBatch: 20, payloadBytes: 1024 };
/** The erasure plan needs these users: 0..5 are erased or deleted in some way, 6.. stay untouched. */
export const MIN_USERS = 8;

export const userId = (i: number) => `user-${String(i).padStart(3, "0")}`;
export const tenantOf = (i: number): LedgerTenant => ({ orgId: ORG, workspaceId: WORKSPACE, userId: userId(i) });
/** Two device origins per user, never shared (a shared origin refuses the operator user erase, 55000). */
export const originsOf = (i: number) => [`machine-${String(i).padStart(3, "0")}-a`, `machine-${String(i).padStart(3, "0")}-b`];
export const sessionOf = (i: number, s: number) => `sess-${String(i).padStart(3, "0")}-${String(s).padStart(3, "0")}`;
/** Session s of user i is uploaded from origin s % 2. */
export const originOfSession = (i: number, s: number) => originsOf(i)[s % 2]!;

/** The synthetic payload text of a record: deterministic, so a read can compare it exactly. */
export const text = (n: number, seed: string) => {
  const base = createHash("sha256").update(seed).digest("hex");
  return base.repeat(Math.ceil(n / base.length)).slice(0, n);
};

export type Fx = Awaited<ReturnType<typeof openFixture>>;

export async function openFixture(backend?: Backend) {
  const b = backend ?? (await pgliteBackend());
  const db = b.owner;
  // The v2 claim-sessions reader gate on (the export refuses otherwise), as admission runs in production.
  await b.replica((a) => a.query("UPDATE sessions.ingest_claim_sessions_policy SET write_enabled = true, backfill_complete_at = now(), read_enabled = true"));
  const store = b.store;
  /** The SQLSTATEs the ledger's database calls raised (deliverBatch maps them to a 503; the hold suite reads them so). */
  const sqlStates: string[] = [];
  const run = async (q: string, p: unknown) => {
    try { return await b.ingest(q, p as unknown[]); } catch (e) { sqlStates.push(String((e as { code?: unknown }).code)); throw e; }
  };
  const sqlLed = sqlLedger(run as never, { acceptanceMode: "buffered", expectedGeneration: generation });
  const s3Led = s3Ledger(store, KEY_ROOT, { acceptanceMode: "canonical", fenceAuthority: "required" });
  const sqlDeps = { acceptanceMode: "buffered" as const, target: { store, keyRoot: KEY_ROOT } };

  async function witness(tenant: LedgerTenant, raw: any) {
    const p = await prepareDelivery(tenant, raw);
    const w = { version: 1, issuer: "relayhistory-cloud", sourceIdentitySha256: identity, controlEpoch: epoch, privacyBaselineSha256: "a".repeat(64), tenant,
      originId: p.originId, batchId: p.batchId, batchDigest: p.batchDigest, sourceControlVersion: "0", pending: false, pendingControl: null, privacyVersion: "0",
      originalRecordCount: raw.records.length, candidateCount: p.records.length,
      records: p.records.map((r, i) => ({ preparedOrd: i, processingRank: i, candidateDigest: r.digest, recordId: r.record_id, revision: String(r.revision),
        revisionId: r.revision_id, candidateFenced: false, currentFenced: false, current: null })),
      privacy: { complete: true, deletions: [], resets: [] } };
    const d = (await db.query<{ d: string }>("SELECT encode(sha256(convert_to($1::jsonb::text,'UTF8')),'hex') AS d", [JSON.stringify(w)])).rows[0]!.d;
    return { witness: w, sourceDigest: d };
  }

  /** One batch of `records` records of one session at `revision`. */
  async function rawBatch(u: number, session: string, originId: string, batchId: string, revision: number, records: number, payloadBytes: number) {
    const recs = Array.from({ length: records }, (_, k) => record({ record_id: `${session}-r${String(k).padStart(4, "0")}`, revision, session_id: session, source: SOURCE,
      origin_id: originId, revision_id: `${session}-r${k}:v${revision}`, payload: { text: text(payloadBytes, `${session}/${k}/${revision}`), ts_ms: 1_761_955_200_000 + revision * 1000 + k } }));
    return batch(recs, { batch_id: batchId, origin_id: originId, account_id: await deliveryAccount(tenantOf(u)), job_id: `job-${batchId}` });
  }

  /** Through the buffered SQL ledger (what the Worker HTTP path / SQL consumer admits before the cutover). */
  async function deliverSql(u: number, raw: any): Promise<DeliverResult> {
    const t = tenantOf(u);
    return deliverBatch({ ...sqlDeps, ledger: sqlLed, custody: await witness(t, raw) } as never, t, raw);
  }
  /** Through the S3 ledger with the per-org fence authority required (the 0005 consumer after the marker). */
  async function deliverS3(u: number, raw: any, via?: ObjectStore): Promise<DeliverResult> {
    // `via`: this one request's view of the store (the harness's barrier store); the ledger and the writer both use it.
    const led = via ? s3Ledger(via, KEY_ROOT, { acceptanceMode: "canonical", fenceAuthority: "required" }) : s3Led;
    return deliverBatch({ ledger: led, acceptanceMode: "canonical", target: { store: via ?? store, keyRoot: KEY_ROOT } } as never, tenantOf(u), raw);
  }

  const one = async <T = Record<string, any>>(q: string, p: unknown[] = []) => (await db.query<T>(q, p)).rows[0]!;

  /** The owner's parity proof and the active privacy authority (see the header); after the backlog. */
  async function proveOwnerParity(users: number) {
    await b.replica(async (a) => {
      await a.query("INSERT INTO sessions.ingest_privacy_authority (singleton, state, control_epoch, neon_fingerprint_sha256, activated_at) VALUES (true, 'active', $1, $2, now()) ON CONFLICT (singleton) DO NOTHING", [epoch, "6".repeat(64)]);
      for (let u = 0; u < users; u++) {
        const t = tenantOf(u);
        await a.query("DELETE FROM sessions.owner_buffered_parity WHERE org_id = $1 AND workspace_id = $2 AND user_id = $3", [t.orgId, t.workspaceId, t.userId]);
        await a.query(`INSERT INTO sessions.owner_buffered_parity (org_id, workspace_id, user_id, cutover_epoch, head_epoch, head_version, head_digest, head_kind, no_head,
            privacy_baseline_sha256, deletion_set_digest, reset_set_digest, target_lineage_digest, target_generation)
          SELECT $1, $2, $3, $4, h.epoch, h.version, h.digest, h.kind, h.org_id IS NULL, $5, e.deletion_set_digest, e.reset_set_digest, e.target_lineage_digest, e.target_generation
            FROM (SELECT 1) AS one
            LEFT JOIN sessions.ingest_source_control_heads AS h ON h.org_id = $1 AND h.workspace_id = $2 AND h.user_id = $3
           CROSS JOIN LATERAL sessions.owner_buffered_parity_expected($1, $2, $3, $4, h.version, h.digest, h.kind) AS e`, [t.orgId, t.workspaceId, t.userId, epoch, generation]);
      }
    });
  }

  /** The operator user erase, SQL side, through its real entry points (begin, step to done, complete). */
  async function operatorUserErase(u: number, requestId: string) {
    const t = tenantOf(u);
    await one("SELECT sessions.operator_user_erase_begin($1,$2,$3,$4) r", [requestId, t.orgId, t.workspaceId, t.userId]);
    for (let i = 0; i < 1000; i++) if ((await one<{ r: any }>("SELECT sessions.operator_user_erase_step($1,$2) r", [requestId, 500])).r?.done) break;
    return (await one<{ r: any }>("SELECT sessions.operator_user_erase_complete($1) r", [requestId])).r;
  }
  /**
   * A user's own session deletion, mirrored: the S3 fence first (applyOrderedControl's order, controlMirror on), then
   * the SQL row. On a lazy-policy deployment SQL serves a selection only as an ORDERED control with Neon custody
   * (ingest_set_session_uploads_internal -> ingest_apply_ordered_control), which this fixture has no Neon for, so the
   * SQL side is the upload_deletions row at the table (as the 062340 suite records one). See the header.
   */
  async function selectionDeletion(u: number, s: number) {
    const t = tenantOf(u);
    await s3Ledger(store, KEY_ROOT, { acceptanceMode: "canonical" }).setSessionUploads({ tenant: t, originId: originOfSession(u, s), enabled: false, sessions: [[SOURCE, sessionOf(u, s)]] });
    await db.query("INSERT INTO sessions.upload_deletions(org_id,workspace_id,user_id,origin_id,source,session_id) VALUES($1,$2,$3,$4,$5,$6)",
      [t.orgId, t.workspaceId, t.userId, originOfSession(u, s), SOURCE, sessionOf(u, s)]);
  }
  /** A session erasure fence at the table (see the header). */
  async function sessionErasureFence(u: number, s: number) {
    await db.query("INSERT INTO sessions.session_erasure_fences(org_id,workspace_id,source,session_id) VALUES($1,$2,$3,$4)", [ORG, WORKSPACE, SOURCE, sessionOf(u, s)]);
  }

  /** "accepted" plus the result, or the refusal's code (the SQLSTATE behind a ledger refusal when there is one). */
  async function attempt(p: () => Promise<DeliverResult>): Promise<{ ok: true; result: DeliverResult } | { ok: false; code: string }> {
    sqlStates.length = 0;
    try { return { ok: true, result: await p() }; } catch (e) {
      const c = String((e as { code?: unknown })?.code ?? "");
      if (c.startsWith("ledger_sqlstate_")) return { ok: false, code: c.slice("ledger_sqlstate_".length) };
      return { ok: false, code: sqlStates.at(-1) ?? (c || "error") };
    }
  }

  return { kind: b.kind, db, attempt, store, one, rawBatch, deliverSql, deliverS3, proveOwnerParity, operatorUserErase, selectionDeletion, sessionErasureFence, close: b.close };
}

