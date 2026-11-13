/**
 * The one-time accepted-stage export (docs/decisions/0005-s3-native-upload-ledger.md, "The buffered backlog",
 * step 0b). S3 alone cannot tell a stage SQL would publish from one it would not: a stage object can exist after
 * `putBufferedStage` without an acknowledged receipt (the consent gate refused it between the PUT and the ACK), a
 * stage can be acknowledged and later superseded (a disable then a re-enable bumps the session's fence version, so
 * SQL never shows the pre-disable stage again), and SQL keeps lines per group and per line (the GC's kept groups,
 * the lazy ACK decisions' `body_accepted`). This export records SQL's OWN verdict per stage, read-only, so the drain
 * (buffered-drain.ts) writes exactly what SQL's buffered reader would show and nothing else.
 *
 *   the verdict   the predicates of SQL's owner buffered reader, `buffered_session_stages_for_target_paged`
 *                 (20261105047020, its `stg` / `grp` / `bad_stage` / `rec` / `recn` / `ev` / `judged` / `kept` CTEs),
 *                 evaluated per candidate stage over all its sessions instead of one target session:
 *                   - the stage: the outbox row's ACTIVE stage (so a GC original is never named), acknowledged, not
 *                     redacted, of a committed buffered batch, with matching identities; the user's reset count
 *                     equals the stage's reset epoch and no reset was recorded at or after the batch's commit (an
 *                     exact check, both times on the database clock); the lazy source identity and the admission's
 *                     control version are the current ones; a GC lineage of one claim;
 *                   - each line (claim ordinal): its record resolves uniquely; its session's recorded epoch equals
 *                     the CURRENT `ingest_fence_versions` epoch and its reset epoch the current reset count (so a
 *                     disable then re-enable makes it not live); no upload or frozen-source deletion; under the lazy
 *                     policy, its ACK decision is `body_accepted` and the native slot still names it;
 *                   - a group is kept only when all its lines are one session, and (outside lazy) all are live;
 *                     under lazy each live line counts.
 *                 Two inputs the reader takes from its caller are derived here: the admission epoch is the lazy
 *                 policy's control epoch, and the admission version is the user's mirrored current control head
 *                 (`ingest_source_control_heads`), never max(receipts) (see `the gate`). The claim-sessions side-row
 *                 check applies as in the reader; the export refuses to run with that reader gate off
 *                 (`export_claim_sessions_gate_off`), because the reader then answers from 047010's fallback, which
 *                 is not transcribed here. Any divergence makes a stage NOT live, never live.
 *   the gate      the reader's PER-USER gate (`owner_buffered_read_scope`, 20261105043700), read-only, per user: the
 *                 privacy authority not fenced; the lazy policy valid now; the mirrored head at the policy's control
 *                 epoch and not pending, with `owner_buffered_parity_expected` (reused as is) proving receipts exactly
 *                 1..version at that epoch with the head's tip digest and kind and none on another epoch; an absent
 *                 head counts as version 0 only when the privacy authority is ACTIVE on the policy's control epoch,
 *                 the user has no receipt on any epoch, and the current parity row proves no_head; else it is
 *                 unproven; and the owner's G2 parity row matching that head and the deletion-set,
 *                 reset-set and lineage digests recomputed now. A closed gate is not live, counted by reason
 *                 (`gated`). The export role must be able to run the owner-only parity function (the owner login).
 *   the S3 fence  the user's S3 fence as the export saw it (instance, resets, resetsCompleted, and the epoch of every
 *                 kept session), read BEFORE the SQL snapshot of the verdict (a candidate page lists the users first):
 *                 the drain refuses a stage when any of these has moved since (`fence_moved_since_export`), so a
 *                 control that lands after the fence read, during the snapshot or before the drain, is honoured. A
 *                 row whose user was not read first (it entered the page range in between) is `fence_late`: no
 *                 sidecar, and the org stays incomplete.
 *   per stage     a sidecar `<stage key>.accepted.json` (v2): {stage_sha256, live, kept: per object group the line
 *                 indexes SQL keeps, fence}. Replaced by CAS when a rerun's verdict differs.
 *   superseded    a create-only `<stage key>.superseded.json` for an earlier LIVE sidecar whose stage is no longer a
 *                 candidate (its outbox row erased or replaced, its receipt or batch no longer accepted): the drain
 *                 never writes a superseded stage (`stage_superseded`). Never deleted or undone.
 *   per org       `<keyRoot>/.drain/accepted-stages/v1/<org>.complete.json`, create-only, once every candidate stage
 *                 of the org has its verdict and its stale sidecars are superseded. Without it the drain writes
 *                 nothing of the org.
 *
 * One export at a time: an --execute run holds a fenced lease (`.drain/export-lock.json`, as the drain's).
 *
 * Cases outside a snapshot verdict, and how they stay safe:
 *   - a GC swap after the export: the replacement has no sidecar (`stage_unacknowledged`), and the original, once its
 *     key is a redaction barrier, is `redacted`; a GC that dropped lines because of a deletion moved the S3 fence too.
 *   - an outbox erased after the export (a user erase): the erase deletes the user's S3 fence, so the drain refuses
 *     the user (`fence_absent`); the next export also supersedes the stage's sidecar.
 *   - an org with no stage intent left in SQL is not visited by the stale pass unless named with --org (its users'
 *     erase deleted their fences: `fence_absent`).
 *
 * SQL: every page is its own short `BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY` transaction, checked with
 * `SHOW transaction_read_only` and always ended by ROLLBACK; SELECTs only, keyset-paged by stage key, optionally scoped
 * to a set of orgs. Counts only: never a key, tenant id or hash.
 */
import { createHash, randomUUID } from "node:crypto";
import { isObjectStoreError, objectKey, readAll, type ObjectStore, type ObjectVersion } from "../shared/vendor.ts";
import {
  ACCEPTED_SUFFIX, SUPERSEDED_SUFFIX, acceptedCompleteKey, acquireExclusiveLock, assertDrainUnlocked, assertPeerUnlocked, drainLockKey, exportLockKey, parseAcceptedComplete, parseAcceptedMarker, parseStagedKey,
  DRAIN_LOCK_TTL_MS, type AcceptedComplete, type AcceptedMarker, type DrainLease, type SupersededRecord,
} from "./buffered-drain.ts";
import { fenceEpoch, s3LedgerObjects, sessionKeyHash, type FenceDoc } from "./ledger-s3.ts";
import { liveMachineId } from "./writer.ts";
import { layoutComponent } from "../shared/layout-component.ts";

const ORG = /^(?!\.)[A-Za-z0-9._-]{1,128}$/;
const HEX64 = /^[0-9a-f]{64}$/;
const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true });
const sha256Hex = (v: string) => createHash("sha256").update(v).digest("hex");
export const MAX_EXPORT_CONCURRENCY = 16;
export { exportLockKey };

export interface ExportSql {
  query(sql: string, params?: unknown[]): Promise<{ rows: Record<string, unknown>[] }>;
}

export interface ExportOptions {
  sql: ExportSql;
  store: ObjectStore;
  keyRoot: string;
  dryRun: boolean;
  orgs?: string[];
  pageSize?: number;
  concurrency?: number;
  statementTimeoutMs?: number;
  nowMs?: () => number;
  lockTtlMs?: number;
}

/** Why the reader's per-user gate is closed (closed codes, from VERDICT_PAGE's `gate`). */
export const GATE_REASONS = ["privacy_fenced", "policy_invalid", "head_unproven", "head_pending", "head_foreign_epoch", "receipts_mismatch", "parity_missing", "parity_mismatch"] as const;
export type GateReason = (typeof GATE_REASONS)[number];

export class ExportError extends Error {
  constructor(readonly code: "export_claim_sessions_gate_off") {
    super(code);
    this.name = "ExportError";
  }
}

export interface ExportSummary {
  mode: "dry_run" | "execute";
  /** Candidate stages (active, acknowledged, of a committed buffered batch). */
  rows: number;
  /** SQL's verdict: live (some lines kept), or not live. */
  live: number;
  not_live: number;
  /** Live by SQL but the user has no S3 fence yet: recorded with no fence view (the drain refuses until re-exported). */
  fence_absent: number;
  written: number;
  updated: number;
  already: number;
  would_write: number;
  /** A row whose stage key, tenant or sha does not bind (never written). */
  invalid: number;
  failed: number;
  /** Not live because the reader's per-user gate is closed, by reason (stages, not users). */
  gated: Partial<Record<GateReason, number>>;
  /**
   * A row of a user whose S3 fence was not read before the verdict snapshot (the stage entered the page range between
   * the two reads): no sidecar, and the org stays incomplete until a rerun.
   */
  fence_late: number;
  /** Earlier live sidecars of stages no longer candidates: create-only superseded records written / that would be. */
  superseded: number;
  would_supersede: number;
  orgs: { complete: number; would_complete: number; already_complete: number; incomplete: number };
}

/** The candidate stages: active, acknowledged, not redacted, of a committed buffered batch (one definition, three uses). */
const CANDIDATE_FROM = `
    FROM sessions.ingest_buffer_outbox AS o
    JOIN sessions.ingest_stage_intents AS s ON s.stage_token = o.active_stage_token
    JOIN sessions.ingest_claim_attempts AS a ON a.claim_token = s.claim_token
    JOIN sessions.ingest_batches AS b
      ON b.org_id = o.org_id AND b.workspace_id = o.workspace_id AND b.origin_id = o.origin_id AND b.batch_id = o.batch_id
   WHERE o.state = 'buffered' AND s.receipt_state = 'acknowledged' AND s.cleanup_state <> 'redacted'
     AND b.state = 'committed' AND b.digest = o.digest`;

/** The reader's v2 claim-sessions gate (044090): the export only runs while it is on (finding 5). */
export const CLAIM_SESSIONS_GATE = `
SELECT coalesce(bool_or(cp.read_enabled AND cp.write_enabled AND cp.backfill_complete_at IS NOT NULL), false) AS v2
  FROM sessions.ingest_claim_sessions_policy AS cp WHERE cp.singleton`;

/**
 * A page's candidate keys and users, BEFORE the verdict snapshot (finding 6): the export reads these users' S3 fences
 * first, then takes SQL's verdict over the same key range.
 */
export const CANDIDATE_PAGE = `
SELECT s.stage_key, s.org_id, s.workspace_id, s.user_id${CANDIDATE_FROM}
     AND ($1::text[] IS NULL OR o.org_id = ANY($1::text[]))
     AND s.stage_key > $2::text
   ORDER BY s.stage_key
   LIMIT $3::int`;

/** Which of these stage keys are candidates now (the stale-sidecar pass, finding 7). */
export const CANDIDATES_IN = `
SELECT s.stage_key${CANDIDATE_FROM}
     AND s.stage_key = ANY($1::text[])`;

/** Orgs with any stage at all, so an org whose stages were all refused still completes (with 0). */
const ORGS = `
SELECT DISTINCT org_id FROM sessions.ingest_stage_intents
WHERE ($1::text[] IS NULL OR org_id = ANY($1::text[]))
ORDER BY org_id`;

/**
 * SQL's verdict for a page of candidate stages. Transcribed from 20261105047020's keep predicate (see the header);
 * `st.*` stands for the reader's `v_org` / `v_ws` / `v_user`, per stage.
 */
export const VERDICT_PAGE = `
WITH pol AS MATERIALIZED (
  SELECT EXISTS (SELECT 1 FROM sessions.ingest_lazy_policy AS lp WHERE lp.singleton) AS lazy,
         (SELECT lp.source_identity_sha256 FROM sessions.ingest_lazy_policy AS lp WHERE lp.singleton) AS pol_identity,
         (SELECT lp.control_epoch FROM sessions.ingest_lazy_policy AS lp WHERE lp.singleton) AS pol_epoch,
         (SELECT lp.privacy_baseline_sha256 FROM sessions.ingest_lazy_policy AS lp WHERE lp.singleton) AS pol_baseline,
         EXISTS (SELECT 1 FROM sessions.ingest_lazy_policy AS lp WHERE lp.singleton AND lp.valid_until > clock_timestamp()) AS pol_valid,
         EXISTS (SELECT 1 FROM sessions.ingest_privacy_authority AS pa WHERE pa.singleton AND pa.state = 'fenced') AS authority_fenced,
         EXISTS (SELECT 1 FROM sessions.ingest_privacy_authority AS pa WHERE pa.singleton AND pa.state = 'active') AS authority_active,
         (SELECT pa.control_epoch FROM sessions.ingest_privacy_authority AS pa WHERE pa.singleton) AS authority_epoch,
         (SELECT ck.control_epoch FROM sessions.ingest_receipt_seat_clock AS ck WHERE ck.singleton) AS clock_epoch,
         (SELECT ck.source_identity_sha256 FROM sessions.ingest_receipt_seat_clock AS ck WHERE ck.singleton) AS clock_identity,
         coalesce((SELECT bool_or(cp.read_enabled AND cp.write_enabled AND cp.backfill_complete_at IS NOT NULL)
                     FROM sessions.ingest_claim_sessions_policy AS cp WHERE cp.singleton), false) AS v2
), cand AS MATERIALIZED (
  SELECT s.stage_token, s.claim_token, s.stage_key, s.stage_sha256, s.org_id, s.workspace_id, s.user_id, s.origin_id,
         s.batch_id, s.digest, s.group_ordinals, s.reset_epoch, s.parent_stage_token,
         a.records, a.epochs, a.reset_epoch AS a_reset, a.source_control_version AS adm_version,
         a.source_identity_sha256 AS a_identity, b.committed_at,
         (ROW(s.org_id, s.workspace_id, s.user_id, s.origin_id, s.batch_id, s.digest)
            = ROW(o.org_id, o.workspace_id, o.user_id, o.origin_id, o.batch_id, o.digest)
          AND ROW(a.org_id, a.workspace_id, a.user_id, a.origin_id, a.batch_id, a.digest, a.reset_epoch)
            IS NOT DISTINCT FROM ROW(s.org_id, s.workspace_id, s.user_id, s.origin_id, s.batch_id, s.digest, s.reset_epoch)
          AND b.user_id = s.user_id AND b.publication_state = 'buffered' AND b.committed_at IS NOT NULL
          AND jsonb_typeof(a.records) = 'array' AND jsonb_typeof(a.epochs) = 'array'
          AND jsonb_typeof(s.group_ordinals) = 'array' AND a.reset_epoch IS NOT NULL) AS shape_ok
${CANDIDATE_FROM}
     AND ($1::text[] IS NULL OR o.org_id = ANY($1::text[]))
     AND s.stage_key > $2::text AND s.stage_key <= $4::text
   ORDER BY s.stage_key
   LIMIT $3::int
), users AS MATERIALIZED (
  SELECT DISTINCT c.org_id, c.workspace_id, c.user_id FROM cand AS c
), gate AS MATERIALIZED (
  -- The reader's per-user gate (owner_buffered_read_scope, 20261105043700) and its control head, per user, read-only.
  -- The head is the mirrored current head (ingest_source_control_heads), never max(receipts): it must be at the
  -- policy's control epoch and not pending, and owner_buffered_parity_expected (reused as is) must prove the user's
  -- receipts are exactly 1..version at that epoch with the head's tip digest and kind, and none on another epoch. An
  -- absent head is version 0 only when proven absent (all four conditions below).
  -- Then the owner's G2 parity row must match that head and the digests recomputed now. The first failing check names
  -- the reason; a NULL reason is an open gate.
  SELECT u.org_id, u.workspace_id, u.user_id, coalesce(h.version, 0) AS head_version,
         CASE
           WHEN pol.authority_fenced THEN 'privacy_fenced'
           WHEN NOT pol.pol_valid OR pol.pol_epoch IS NULL OR pol.pol_baseline IS NULL THEN 'policy_invalid'
           -- An absent head is version 0 only when ALL hold: the privacy authority is active, on the policy's control
           -- epoch; the user has no receipt on ANY epoch; and the current G2 parity row proves no_head.
           WHEN h.org_id IS NULL AND NOT coalesce(pol.authority_active AND pol.authority_epoch = pol.pol_epoch
             AND NOT EXISTS (SELECT 1 FROM sessions.ingest_control_receipts AS r
                              WHERE r.org_id = u.org_id AND r.workspace_id = u.workspace_id AND r.user_id = u.user_id)
             AND par.org_id IS NOT NULL AND par.no_head, false) THEN 'head_unproven'
           WHEN h.pending IS NOT FALSE AND h.org_id IS NOT NULL THEN 'head_pending'
           WHEN h.org_id IS NOT NULL AND h.epoch IS DISTINCT FROM pol.pol_epoch THEN 'head_foreign_epoch'
           WHEN NOT coalesce(x.receipts_ok, false) THEN 'receipts_mismatch'
           WHEN par.org_id IS NULL THEN 'parity_missing'
           WHEN NOT coalesce(x.target_generation IS NOT NULL
             AND par.cutover_epoch = pol.pol_epoch AND par.privacy_baseline_sha256 = pol.pol_baseline
             AND par.no_head = (h.org_id IS NULL)
             AND par.head_epoch IS NOT DISTINCT FROM h.epoch AND par.head_version IS NOT DISTINCT FROM h.version
             AND par.head_digest IS NOT DISTINCT FROM h.digest AND par.head_kind IS NOT DISTINCT FROM h.kind
             AND par.target_generation = x.target_generation
             AND par.deletion_set_digest = x.deletion_set_digest AND par.reset_set_digest = x.reset_set_digest
             AND par.target_lineage_digest = x.target_lineage_digest, false) THEN 'parity_mismatch'
           ELSE NULL
         END AS gate_reason
    FROM users AS u CROSS JOIN pol
    LEFT JOIN sessions.ingest_source_control_heads AS h
      ON h.org_id = u.org_id AND h.workspace_id = u.workspace_id AND h.user_id = u.user_id
    LEFT JOIN sessions.owner_buffered_parity AS par
      ON par.org_id = u.org_id AND par.workspace_id = u.workspace_id AND par.user_id = u.user_id
    LEFT JOIN LATERAL sessions.owner_buffered_parity_expected(u.org_id, u.workspace_id, u.user_id, pol.pol_epoch,
                                                              h.version, h.digest, h.kind) AS x ON true
), stg AS MATERIALIZED (
  SELECT c.*, pol.lazy, pol.pol_identity, pol.pol_epoch, pol.clock_epoch, pol.clock_identity, pol.v2,
         (SELECT count(*) FROM sessions.upload_resets AS ur
           WHERE ur.org_id = c.org_id AND ur.workspace_id = c.workspace_id AND ur.user_id = c.user_id) AS resets,
         g.head_version, g.gate_reason
    FROM cand AS c CROSS JOIN pol
    JOIN gate AS g ON g.org_id = c.org_id AND g.workspace_id = c.workspace_id AND g.user_id = c.user_id
), stage_ok AS (
  SELECT st.stage_token,
         coalesce(st.gate_reason IS NULL AND st.shape_ok AND st.resets = st.reset_epoch
           AND NOT EXISTS (SELECT 1 FROM sessions.upload_resets AS ur
                            WHERE ur.org_id = st.org_id AND ur.workspace_id = st.workspace_id AND ur.user_id = st.user_id
                              AND (ur.reset_at IS NULL OR ur.reset_at >= st.committed_at))
           AND st.adm_version IS NOT NULL AND st.a_identity IS NOT NULL
           AND st.pol_identity IS NOT NULL AND st.pol_epoch IS NOT NULL
           AND st.a_identity = st.pol_identity AND st.adm_version = st.head_version
           AND NOT EXISTS (
             WITH RECURSIVE anc(claim_token, parent_stage_token, depth) AS (
               SELECT p.claim_token, p.parent_stage_token, 1
                 FROM sessions.ingest_stage_intents AS p WHERE p.stage_token = st.parent_stage_token
               UNION ALL
               SELECT p.claim_token, p.parent_stage_token, an.depth + 1
                 FROM anc AS an JOIN sessions.ingest_stage_intents AS p ON p.stage_token = an.parent_stage_token
                WHERE an.depth < 64)
             SELECT 1 FROM anc
              WHERE anc.claim_token IS DISTINCT FROM st.claim_token
                 OR (anc.depth >= 64 AND anc.parent_stage_token IS NOT NULL)), false) AS ok
    FROM stg AS st
), grp AS (
  SELECT st.stage_token, (g.idx - 1)::integer AS group_index, (e.value::text)::integer AS ord
    FROM stg AS st
   CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(st.group_ordinals) = 'array' THEN st.group_ordinals ELSE '[]'::jsonb END) WITH ORDINALITY AS g(value, idx)
   CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(g.value) = 'array' THEN g.value ELSE '[null]'::jsonb END) AS e(value)
   WHERE jsonb_typeof(e.value) = 'number' AND e.value::text ~ '^(0|[1-9][0-9]{0,2})$'
), bad_stage AS MATERIALIZED (
  SELECT st.stage_token
    FROM stg AS st
   WHERE jsonb_typeof(st.group_ordinals) IS DISTINCT FROM 'array'
      OR EXISTS (SELECT 1 FROM jsonb_array_elements(st.group_ordinals) AS g(value)
                  WHERE jsonb_typeof(g.value) IS DISTINCT FROM 'array' OR jsonb_array_length(g.value) = 0
                     OR EXISTS (SELECT 1 FROM jsonb_array_elements(g.value) AS e(value)
                                 WHERE jsonb_typeof(e.value) IS DISTINCT FROM 'number'
                                    OR e.value::text !~ '^(0|[1-9][0-9]{0,2})$'))
      OR st.stage_token IN (SELECT g2.stage_token FROM grp AS g2 GROUP BY g2.stage_token, g2.ord HAVING count(*) > 1)
), rec AS (
  SELECT st.stage_token, r.ord, r.source, r.session_id, r.record_id, r.revision, r.digest
    FROM stg AS st
   CROSS JOIN LATERAL jsonb_to_recordset(CASE WHEN jsonb_typeof(st.records) = 'array' THEN st.records ELSE '[]'::jsonb END)
         AS r(ord integer, source text, session_id text, record_id text, revision bigint, digest text)
), recn AS (
  SELECT w.stage_token, w.ord, w.source, w.session_id, w.record_id, w.revision, w.digest
    FROM (SELECT rc.*, count(*) OVER (PARTITION BY rc.stage_token, rc.ord) AS n FROM rec AS rc) AS w
   WHERE w.ord IS NOT NULL AND w.n = 1
), ev AS (
  SELECT st.stage_token, x.source, x.session_id, min(x.epoch) AS epoch, max(x.epoch) AS epoch_max,
         min(x.reset_epoch) AS reset_epoch, max(x.reset_epoch) AS reset_max, count(*) AS n
    FROM stg AS st
   CROSS JOIN LATERAL jsonb_to_recordset(CASE WHEN jsonb_typeof(st.epochs) = 'array' THEN st.epochs ELSE '[]'::jsonb END)
         AS x(source text, session_id text, epoch bigint, reset_epoch bigint)
   GROUP BY st.stage_token, x.source, x.session_id
), judged AS (
  SELECT gr.stage_token, gr.group_index, gr.ord, rc.source, rc.session_id,
         coalesce(rc.session_id IS NOT NULL AND ev.n IS NOT NULL
          AND ev.epoch = ev.epoch_max AND ev.reset_epoch = ev.reset_max
          AND coalesce(fv.epoch, 0) = ev.epoch AND st.resets = ev.reset_epoch
          AND NOT EXISTS (SELECT 1 FROM sessions.upload_deletions AS d
                           WHERE d.org_id = st.org_id AND d.workspace_id = st.workspace_id AND d.user_id = st.user_id
                             AND d.origin_id = st.origin_id AND d.source = rc.source AND d.session_id = rc.session_id)
          AND rc.source IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM sessions.ingest_frozen_source_deletions AS fd
                           WHERE fd.org_id = st.org_id AND fd.workspace_id = st.workspace_id AND fd.user_id = st.user_id
                             AND fd.source = rc.source AND fd.session_id = rc.session_id)
          AND NOT EXISTS (SELECT 1 FROM sessions.upload_deletions AS d2
                           WHERE d2.org_id = st.org_id AND d2.workspace_id = st.workspace_id AND d2.user_id = st.user_id
                             AND d2.source = rc.source AND d2.session_id = rc.session_id)
          -- The reader's side-row identity, when its claim-sessions reader gate is on.
          AND (NOT st.v2 OR EXISTS (SELECT 1 FROM sessions.ingest_claim_sessions AS cs
                                     WHERE cs.claim_token = st.claim_token AND cs.source = rc.source AND cs.session_id = rc.session_id
                                       AND cs.org_id = st.org_id AND cs.workspace_id = st.workspace_id AND cs.user_id = st.user_id))
          AND (NOT st.lazy OR EXISTS (
            SELECT 1 FROM sessions.ingest_lazy_ack_decisions AS ad
              JOIN sessions.ingest_lazy_native_slots AS ns ON ns.key_sha256 = ad.native_key_sha256
             WHERE ad.claim_token = st.claim_token AND ad.ord = gr.ord AND ad.body_accepted
               AND ad.source_control_version = st.adm_version
               AND rc.record_id IS NOT NULL AND rc.source IS NOT NULL AND rc.revision > 0
               AND rc.digest ~ '^[0-9a-f]{64}$' AND ev.n = 1 AND ev.epoch >= 0 AND ev.reset_epoch = st.a_reset
               AND ad.candidate_key_sha256 = encode(sha256(convert_to(jsonb_build_array('buffer-first-ack-seat-v1',
                     st.clock_epoch, st.clock_identity, st.org_id, st.workspace_id, st.user_id, st.origin_id, rc.record_id,
                     rc.revision::text, st.a_reset::text, rc.source, rc.session_id, ev.epoch::text)::text, 'UTF8')), 'hex')
               AND ad.candidate_digest = rc.digest AND ad.reset_epoch = st.a_reset AND ad.fence_epoch = ev.epoch
               AND ns.org_id = st.org_id AND ns.workspace_id = st.workspace_id AND ns.origin_id = st.origin_id
               AND ns.record_id = rc.record_id AND ns.current_owner = st.user_id
               AND ns.current_seat_key = ad.seat_key_sha256 AND ns.current_digest = ad.candidate_digest
               AND ns.current_revision = rc.revision
               AND ns.current_source IS NOT DISTINCT FROM rc.source
               AND ns.current_session IS NOT DISTINCT FROM rc.session_id)), false) AS live
    FROM grp AS gr
    JOIN stg AS st ON st.stage_token = gr.stage_token
    LEFT JOIN recn AS rc ON rc.stage_token = gr.stage_token AND rc.ord = gr.ord
    LEFT JOIN ev ON ev.stage_token = gr.stage_token AND ev.source = rc.source AND ev.session_id = rc.session_id
    LEFT JOIN sessions.ingest_fence_versions AS fv
      ON fv.org_id = st.org_id AND fv.workspace_id = st.workspace_id AND fv.user_id = st.user_id AND fv.origin_id = st.origin_id
     AND fv.source = rc.source AND fv.session_id = rc.session_id
   WHERE NOT EXISTS (SELECT 1 FROM bad_stage AS bs WHERE bs.stage_token = gr.stage_token)
), kept AS (
  SELECT j.stage_token, j.group_index, min(j.source) AS source, min(j.session_id) AS session_id,
         array_agg(j.ord ORDER BY j.ord) FILTER (WHERE j.live) AS ords
    FROM judged AS j JOIN stg AS st ON st.stage_token = j.stage_token
   GROUP BY j.stage_token, j.group_index, st.lazy
  HAVING count(j.session_id) = count(*) AND count(j.source) = count(*)
     AND count(DISTINCT j.source) = 1 AND count(DISTINCT j.session_id) = 1
     AND count(*) FILTER (WHERE j.live) > 0
     AND (st.lazy OR count(*) FILTER (WHERE j.live) = count(*))
)
SELECT st.stage_key, st.stage_sha256, st.claim_token::text AS claim_token, st.org_id, st.workspace_id, st.user_id,
       st.origin_id, st.digest, st.group_ordinals, st.v2, st.gate_reason,
       (so.ok AND NOT EXISTS (SELECT 1 FROM bad_stage AS bs WHERE bs.stage_token = st.stage_token)) AS stage_ok,
       coalesce((SELECT jsonb_agg(jsonb_build_object('group', k.group_index, 'ords', to_jsonb(k.ords), 'source', k.source, 'session', k.session_id))
                   FROM kept AS k WHERE k.stage_token = st.stage_token), '[]'::jsonb) AS kept
  FROM stg AS st JOIN stage_ok AS so ON so.stage_token = st.stage_token
 ORDER BY st.stage_key`;

async function mapLimit<T>(items: readonly T[], limit: number, fn: (item: T) => Promise<void>): Promise<void> {
  let next = 0;
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (next < items.length) await fn(items[next++]!);
  }));
}

/** The stage key must be exactly the one this row's tenant, batch, claim and bytes produce (staging.ts stagedKey). */
function binds(keyRoot: string, r: Record<string, unknown>): boolean {
  const key = r.stage_key, sha = r.stage_sha256;
  if (typeof key !== "string" || typeof sha !== "string" || !HEX64.test(sha)) return false;
  const p = parseStagedKey(key);
  if (!p || p.keyRoot !== keyRoot || p.orgId !== r.org_id || typeof r.workspace_id !== "string" || typeof r.origin_id !== "string" ||
      typeof r.user_id !== "string" || typeof r.digest !== "string" || typeof r.claim_token !== "string") return false;
  return p.workspace === layoutComponent(r.workspace_id) && p.machine === liveMachineId(r.workspace_id, r.origin_id, r.user_id) &&
    p.batch32 === sha256Hex(r.digest).slice(0, 32) && p.claim32 === sha256Hex(r.claim_token).slice(0, 32) && p.content16 === sha.slice(0, 16);
}

/** SQL's kept ordinals -> per object group (staging.ts: group g, line j = original ordinal group_ordinals[g][j]) the kept line indexes. */
export function keptLines(groupOrdinals: unknown, kept: { group: number; ords: number[] }[]): number[][] | null {
  if (!Array.isArray(groupOrdinals)) return null;
  const byGroup = new Map(kept.map((k) => [k.group, new Set(k.ords)]));
  const out: number[][] = [];
  for (let g = 0; g < groupOrdinals.length; g++) {
    const ords = groupOrdinals[g];
    if (!Array.isArray(ords)) return null;
    const keep = byGroup.get(g);
    out.push(keep ? ords.flatMap((o, j) => (keep.has(o as number) ? [j] : [])) : []);
  }
  return out;
}

export async function exportAcceptedStages(opts: ExportOptions): Promise<ExportSummary> {
  const pageSize = opts.pageSize ?? 500;
  const concurrency = opts.concurrency ?? 8;
  const statementTimeoutMs = opts.statementTimeoutMs ?? 30_000;
  if (!Number.isInteger(pageSize) || pageSize < 1 || pageSize > 5_000) throw new Error("export_page_size_invalid");
  if (!Number.isInteger(concurrency) || concurrency < 1 || concurrency > MAX_EXPORT_CONCURRENCY) throw new Error("export_concurrency_invalid");
  if (!Number.isInteger(statementTimeoutMs) || statementTimeoutMs < 1_000 || statementTimeoutMs > 600_000) throw new Error("export_statement_timeout_invalid");
  if (!/^[a-z0-9][a-z0-9-]{0,62}$/.test(opts.keyRoot)) throw new Error("export_key_root_invalid");
  if (opts.orgs) for (const org of opts.orgs) if (!ORG.test(org)) throw new Error("export_org_invalid");
  const orgScope = opts.orgs && opts.orgs.length ? [...new Set(opts.orgs)] : null;
  const nowMs = opts.nowMs ?? Date.now;
  // The dry run's defence in depth: every mutation throws.
  const store: ObjectStore = opts.dryRun
    ? new Proxy(opts.store, {
        get(target, prop) {
          if (prop === "put" || prop === "delete") return async () => { throw new Error("export_dry_run_write_refused"); };
          const value = (target as unknown as Record<PropertyKey, unknown>)[prop];
          return typeof value === "function" ? (value as (...a: unknown[]) => unknown).bind(target) : value;
        },
      })
    : opts.store;
  const io = s3LedgerObjects(store, opts.keyRoot);
  const summary: ExportSummary = {
    mode: opts.dryRun ? "dry_run" : "execute", rows: 0, live: 0, not_live: 0, fence_absent: 0,
    written: 0, updated: 0, already: 0, would_write: 0, invalid: 0, failed: 0, gated: {}, fence_late: 0, superseded: 0, would_supersede: 0,
    orgs: { complete: 0, would_complete: 0, already_complete: 0, incomplete: 0 },
  };
  const perOrg = new Map<string, { stages: number; bad: boolean }>();
  const { sql } = opts;

  /** One short, verified read-only transaction (finding 8's rule here too): never one long snapshot. */
  async function readOnly<T>(work: () => Promise<T>): Promise<T> {
    await sql.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
    try {
      const ro = (await sql.query("SHOW transaction_read_only")).rows[0]?.transaction_read_only;
      if (ro !== "on") throw new Error("export_not_read_only");
      await sql.query(`SET LOCAL statement_timeout = ${statementTimeoutMs}`);
      return await work();
    } finally {
      await sql.query("ROLLBACK").catch(() => {});
    }
  }

  // One export at a time (finding 9): the drain's fenced lease, under its own key. Never beside a drain (third
  // review): both modes refuse while the drain's lease is live (drain_peer_locked).
  let lease: DrainLease | undefined;
  if (opts.dryRun) {
    await assertDrainUnlocked(opts.store, opts.keyRoot, nowMs(), exportLockKey(opts.keyRoot));
    await assertPeerUnlocked(opts.store, drainLockKey(opts.keyRoot), nowMs());
  } else lease = await acquireExclusiveLock(store, opts.keyRoot, { holder: `export-${randomUUID()}`, ttlMs: opts.lockTtlMs ?? DRAIN_LOCK_TTL_MS, now: nowMs, key: exportLockKey(opts.keyRoot), peerKey: drainLockKey(opts.keyRoot) });
  const ttl = opts.lockTtlMs ?? DRAIN_LOCK_TTL_MS;
  const renew = lease ? setInterval(() => { lease!.renew().catch(() => {}); }, Math.max(500, Math.floor(ttl / 3))) : undefined;
  const leaseOk = () => opts.dryRun || (lease !== undefined && lease.alive());

  try {
    const orgRows = await readOnly(async () => {
      // Finding 5: with the v2 claim-sessions reader gate off, the reader answers from 047010's fallback, which this
      // export does not transcribe. Refuse before any write rather than claim equivalence.
      if ((await sql.query(CLAIM_SESSIONS_GATE)).rows[0]?.v2 !== true) throw new ExportError("export_claim_sessions_gate_off");
      return (await sql.query(ORGS, [orgScope])).rows;
    });
    for (const r of orgRows)
      if (typeof r.org_id === "string") perOrg.set(r.org_id, { stages: 0, bad: !ORG.test(r.org_id) });
    // Lander P2-1 (#585): an execute run first REVOKES each org's completion, and writes it again only if this run ends
    // clean for the org. A rerun that fails for some stages, or dies part way, leaves the org refused by the drain
    // (stage_export_missing) instead of opening it on an earlier run's verdicts for the stages this run could not
    // refresh. Overwritten in place under the export lease (which excludes the drain and every other export).
    // P3-2: an org with a completion but no stage intent left in SQL (in --org, or, unscoped, any completion object) joins
    // the run too, so its stale sidecars are superseded and its completion is revoked and renewed like every other org's.
    for (const org of orgScope ?? (await completionOrgs())) {
      if (ORG.test(org) && !perOrg.has(org) && (await completionState(org)) !== "none") perOrg.set(org, { stages: 0, bad: false });
    }
    const hadCompletion = new Set<string>();
    if (!opts.dryRun) {
      for (const org of [...perOrg.keys()].filter((o) => ORG.test(o)).sort()) {
        // P3-3: a revocation left by an aborted run still means an earlier export completed the org.
        if ((await completionState(org)) !== "none") hadCompletion.add(org);
        await putCompletion(org, { v: 1, kind: "buffered-accepted-revoked", revokedAtMs: nowMs() });
      }
    }
    /** The user's S3 fence, once per page per user (the backfill wrote it; absent means not backfilled). */
    let fences = new Map<string, Promise<FenceDoc | null | "invalid">>();
    const fenceOf = (org: string, ws: string, user: string) => {
      const k = JSON.stringify([org, ws, user]);
      if (!fences.has(k)) fences.set(k, (async () => {
        try {
          const read = await io.readFence(io.keysFor({ orgId: org, workspaceId: ws, userId: user }).fence);
          return read.state === "present" ? read.fence : null;
        } catch {
          return "invalid" as const;
        }
      })());
      return fences.get(k)!;
    };
    let cursor = "";
    for (;;) {
      // Finding 6: the page's users and their S3 fences FIRST, then SQL's verdict over the same key range. A control
      // landing after the fence read moves the fence past what the sidecar records, so the drain refuses
      // (fence_moved_since_export); read the other way round, the sidecar could carry the post-control fence beside a
      // pre-control verdict.
      const page = (await readOnly(() => sql.query(CANDIDATE_PAGE, [orgScope, cursor, pageSize]))).rows;
      if (!page.length) break;
      const upper = String(page[page.length - 1]!.stage_key);
      fences = new Map();
      const userKey = (r: Record<string, unknown>) => JSON.stringify([r.org_id, r.workspace_id, r.user_id]);
      const preRead = new Set<string>();
      await mapLimit([...new Map(page.map((r) => [userKey(r), r])).values()], concurrency, async (r) => {
        if (typeof r.org_id !== "string" || typeof r.workspace_id !== "string" || typeof r.user_id !== "string") return;
        preRead.add(userKey(r));
        await fenceOf(r.org_id, r.workspace_id, r.user_id);
      });
      const rows = (await readOnly(() => sql.query(VERDICT_PAGE, [orgScope, cursor, pageSize, upper]))).rows;
      // The v2 gate turned off mid-export: stop before writing this page.
      if (rows.some((r) => r.v2 !== true)) throw new ExportError("export_claim_sessions_gate_off");
      cursor = rows.length ? String(rows[rows.length - 1]!.stage_key) : upper;
      await mapLimit(rows, concurrency, async (r) => {
        summary.rows++;
        const org = typeof r.org_id === "string" ? r.org_id : "";
        const tally = perOrg.get(org) ?? { stages: 0, bad: true };
        perOrg.set(org, tally);
        tally.stages++;
        if (!binds(opts.keyRoot, r)) {
          summary.invalid++;
          tally.bad = true;
          return;
        }
        if (!preRead.has(userKey(r))) {
          summary.fence_late++;
          tally.bad = true;
          return;
        }
        const keptRows = (Array.isArray(r.kept) ? r.kept : []) as { group: number; ords: number[]; source: string; session: string }[];
        const lines = keptLines(r.group_ordinals, keptRows);
        // Finding 3: a closed per-user gate is not live (r.stage_ok is false then too), counted by reason.
        const gate = typeof r.gate_reason === "string" ? r.gate_reason : null;
        if (gate !== null) {
          const reason = (GATE_REASONS as readonly string[]).includes(gate) ? (gate as GateReason) : "parity_mismatch";
          summary.gated[reason] = (summary.gated[reason] ?? 0) + 1;
        }
        const live = gate === null && r.stage_ok === true && lines !== null && lines.some((l) => l.length > 0);
        let fenceView: AcceptedMarker["fence"] = null;
        if (live) {
          const fence = await fenceOf(org, r.workspace_id as string, r.user_id as string);
          if (fence === "invalid") {
            summary.failed++;
            tally.bad = true;
            return;
          }
          if (fence === null) {
            // Not backfilled yet: SQL's verdict is kept, with no fence view; the drain refuses the user until a fresh
            // export after the backfill records one (fence_absent, then fence_moved_since_export).
            summary.fence_absent++;
          } else {
            const epochs: Record<string, number> = {};
            for (const k of keptRows) {
              const sk = sessionKeyHash(k.source, k.session);
              epochs[sk] = fenceEpoch(fence, r.origin_id as string, sk);
            }
            fenceView = { instance: fence.instance, resets: fence.resets, resetsCompleted: fence.resetsCompleted, epochs };
          }
        }
        if (live) summary.live++;
        else summary.not_live++;
        const marker: AcceptedMarker = {
          v: 2, kind: "buffered-accepted", stage_sha256: r.stage_sha256 as string, live,
          kept: live ? lines! : [], fence: fenceView,
        };
        const key = (r.stage_key as string) + ACCEPTED_SUFFIX;
        try {
          const existing = await readMarker(store, key);
          const same = existing !== null && existing !== "invalid" && JSON.stringify(existing.doc) === JSON.stringify(marker);
          if (same) { summary.already++; return; }
          if (opts.dryRun) { summary.would_write++; return; }
          if (!leaseOk()) throw new Error("export_lock_lost");
          // A verdict that changed since an earlier export replaces it (CAS): the drain acts on the latest.
          await store.put(objectKey(key), encoder.encode(JSON.stringify(marker)),
            existing && existing !== "invalid" ? { ifMatch: existing.version, contentType: "application/json" } : existing === "invalid" ? { contentType: "application/json" } : { ifNoneMatch: true, contentType: "application/json" });
          if (existing) summary.updated++;
          else summary.written++;
        } catch {
          summary.failed++;
          tally.bad = true;
        }
      });
    }
    // Finding 7: an earlier export's LIVE sidecar whose stage is no longer a candidate (its outbox row erased or
    // replaced, its receipt or batch no longer accepted) gets a create-only superseded record, which the drain honours
    // over any sidecar (stage_superseded). Never deleted, never undone. Before the completion objects, so a failure
    // here leaves a first completion unwritten.
    // Every org SQL still names, and every org the operator scoped (one with no stage intent left in SQL included).
    for (const org of [...new Set([...perOrg.keys(), ...(orgScope ?? [])])].sort()) {
      if (!ORG.test(org)) continue;
      try {
        await supersedeStale(org);
      } catch {
        summary.failed++;
        const tally = perOrg.get(org);
        if (tally) tally.bad = true;
      }
    }
    // Completion objects (finding 9), after every candidate stage of the org has its verdict: written only for an org
    // this run ended clean for; a bad org stays revoked (above), so the drain refuses it.
    for (const [org, t] of [...perOrg].sort(([a], [b]) => (a < b ? -1 : 1))) {
      if (t.bad || !ORG.test(org)) {
        summary.orgs.incomplete++;
        continue;
      }
      if (opts.dryRun) {
        summary.orgs.would_complete++;
        continue;
      }
      const doc: AcceptedComplete = { v: 1, kind: "buffered-accepted-complete", stages: t.stages, completedAtMs: nowMs() };
      try {
        await putCompletion(org, doc);
        // A renewal of an earlier export's completion counts as already_complete.
        if (hadCompletion.has(org)) summary.orgs.already_complete++;
        else summary.orgs.complete++;
      } catch {
        summary.orgs.incomplete++;
      }
    }
  } finally {
    if (renew) clearInterval(renew);
    await lease?.release();
  }
  return summary;

  /** An org's completion object: a valid completion, a revocation (an execute run began and has not renewed it), or none. */
  async function completionState(org: string): Promise<"complete" | "revoked" | "none"> {
    const text = await store.getObject(objectKey(acceptedCompleteKey(opts.keyRoot, org))).then(async (g) => decoder.decode(await readAll(g.body)), () => null);
    if (text === null) return "none";
    if (parseAcceptedComplete(text)) return "complete";
    try {
      const v = JSON.parse(text) as { v?: unknown; kind?: unknown };
      return v.v === 1 && v.kind === "buffered-accepted-revoked" ? "revoked" : "none";
    } catch {
      return "none";
    }
  }
  /** Every org with an object under the completion prefix (an unscoped run's view of orgs an earlier export completed). */
  async function completionOrgs(): Promise<string[]> {
    const prefix = `${opts.keyRoot}/.drain/accepted-stages/v1/`;
    const out: string[] = [];
    let cursor: string | undefined;
    do {
      const page = await store.list(objectKey(prefix.slice(0, -1)), cursor);
      cursor = page.nextCursor;
      for (const item of page.items) {
        const m = /^(.+)\.complete\.json$/.exec(String(item.key).slice(prefix.length));
        if (m && String(item.key).startsWith(prefix)) out.push(m[1]!);
      }
    } while (cursor);
    return out.sort();
  }
  /** Replace an org's completion object (a completion, or a revocation the drain reads as absent), under the lease. */
  async function putCompletion(org: string, doc: AcceptedComplete | { v: 1; kind: "buffered-accepted-revoked"; revokedAtMs: number }): Promise<void> {
    if (!leaseOk()) throw new Error("export_lock_lost");
    await store.put(objectKey(acceptedCompleteKey(opts.keyRoot, org)), encoder.encode(JSON.stringify(doc)), { contentType: "application/json" });
  }

  /** The stale-sidecar pass for one org: list its sidecars, ask SQL which stages are still candidates, supersede the rest. */
  async function supersedeStale(org: string): Promise<void> {
    // The listing is on a `/` boundary (object-store list), so this prefix is exactly the org's staged objects.
    const prefix = `${opts.keyRoot}/${org}/buffered-batches/v1`;
    // Per stage key: has a sidecar / a superseded record (they list right after the stage, in key order).
    let batch: string[] = [];
    const superseded = new Set<string>();
    const flush = async () => {
      const keys = batch.filter((k) => !superseded.has(k));
      batch = [];
      if (!keys.length) return;
      const live: { key: string; sha: string }[] = [];
      for (const k of keys) {
        const got = await readMarker(store, k + ACCEPTED_SUFFIX);
        if (got === "invalid") throw new Error("export_sidecar_invalid");
        if (got && got.doc.live) live.push({ key: k, sha: got.doc.stage_sha256 });
      }
      if (!live.length) return;
      const still = new Set((await readOnly(() => sql.query(CANDIDATES_IN, [live.map((l) => l.key)]))).rows.map((r) => String(r.stage_key)));
      for (const l of live) {
        if (still.has(l.key)) continue;
        if (opts.dryRun) {
          summary.would_supersede++;
          continue;
        }
        if (!leaseOk()) throw new Error("export_lock_lost");
        const doc: SupersededRecord = { v: 1, kind: "buffered-superseded", stage_sha256: l.sha };
        try {
          await store.put(objectKey(l.key + SUPERSEDED_SUFFIX), encoder.encode(JSON.stringify(doc)), { ifNoneMatch: true, contentType: "application/json" });
          summary.superseded++;
        } catch (error) {
          // Already superseded (an earlier run, or a lost response): it is there either way.
          if (!isObjectStoreError(error, "already_exists") && !isObjectStoreError(error, "precondition_failed")) throw error;
        }
      }
    };
    let cursor: string | undefined;
    do {
      const listed = await store.list(objectKey(prefix), cursor);
      cursor = listed.nextCursor;
      for (const item of listed.items) {
        const key = String(item.key);
        if (key.endsWith(SUPERSEDED_SUFFIX)) superseded.add(key.slice(0, -SUPERSEDED_SUFFIX.length));
        else if (key.endsWith(ACCEPTED_SUFFIX)) {
          const stage = key.slice(0, -ACCEPTED_SUFFIX.length);
          if (parseStagedKey(stage)?.orgId === org) batch.push(stage);
        }
      }
      // A stage's superseded record lists right after its sidecar, possibly on the next listing page: `superseded` only
      // saves a write, the create-only put is what makes a second record impossible.
      if (batch.length >= pageSize) await flush();
    } while (cursor);
    await flush();
  }
}

async function readMarker(store: ObjectStore, key: string): Promise<{ doc: AcceptedMarker; version: ObjectVersion } | "invalid" | null> {
  try {
    const got = await store.getObject(objectKey(key));
    const bytes = await readAll(got.body);
    if (bytes.byteLength > 1024 * 1024) return "invalid";
    const doc = parseAcceptedMarker(decoder.decode(bytes));
    return doc ? { doc, version: got.meta.version } : "invalid";
  } catch (error) {
    if (isObjectStoreError(error, "not_found")) return null;
    if (error instanceof TypeError) return "invalid"; // not UTF-8
    throw error;
  }
}
