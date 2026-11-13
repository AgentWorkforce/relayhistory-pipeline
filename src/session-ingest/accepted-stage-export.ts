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
 *                 policy's control epoch, and the admission version is the user's latest `ingest_control_receipts`
 *                 version in that epoch (0 with none). The claim-sessions side-row check applies when its reader
 *                 gate is on, as in the reader. Any divergence makes a stage NOT live, never live.
 *   the S3 fence  the user's S3 fence as the export saw it (instance, resets, resetsCompleted, and the epoch of every
 *                 kept session): the drain refuses a stage when any of these has moved since (`fence_moved_since_export`),
 *                 so a control that lands between export and drain is honoured exactly.
 *   per stage     a sidecar `<stage key>.accepted.json` (v2): {stage_sha256, live, kept: per object group the line
 *                 indexes SQL keeps, fence}. Replaced by CAS when a rerun's verdict differs.
 *   per org       `<keyRoot>/.drain/accepted-stages/v1/<org>.complete.json`, create-only, once every candidate stage
 *                 of the org has its verdict. Without it the drain writes nothing of the org.
 *
 * One export at a time: an --execute run holds a fenced lease (`.drain/export-lock.json`, as the drain's).
 *
 * Cases outside a snapshot verdict, and how they stay safe:
 *   - a GC swap after the export: the replacement has no sidecar (`stage_unacknowledged`), and the original, once its
 *     key is a redaction barrier, is `redacted`; a GC that dropped lines because of a deletion moved the S3 fence too.
 *   - an outbox erased after the export (a user erase): the erase deletes the user's S3 fence, so the drain refuses
 *     the user (`fence_absent`).
 *
 * SQL: every page is its own short `BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY` transaction, checked with
 * `SHOW transaction_read_only` and always ended by ROLLBACK; SELECTs only, keyset-paged by stage key, optionally scoped
 * to a set of orgs. Counts only: never a key, tenant id or hash.
 */
import { createHash, randomUUID } from "node:crypto";
import { isObjectStoreError, objectKey, readAll, type ObjectStore, type ObjectVersion } from "../shared/vendor.ts";
import {
  ACCEPTED_SUFFIX, acceptedCompleteKey, acquireDrainLock, assertDrainUnlocked, parseAcceptedComplete, parseAcceptedMarker, parseStagedKey,
  DRAIN_LOCK_TTL_MS, type AcceptedComplete, type AcceptedMarker, type DrainLease,
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
export const exportLockKey = (keyRoot: string) => `${keyRoot}/.drain/export-lock.json`;

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
  orgs: { complete: number; would_complete: number; already_complete: number; incomplete: number };
}

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
    FROM sessions.ingest_buffer_outbox AS o
    JOIN sessions.ingest_stage_intents AS s ON s.stage_token = o.active_stage_token
    JOIN sessions.ingest_claim_attempts AS a ON a.claim_token = s.claim_token
    JOIN sessions.ingest_batches AS b
      ON b.org_id = o.org_id AND b.workspace_id = o.workspace_id AND b.origin_id = o.origin_id AND b.batch_id = o.batch_id
   WHERE o.state = 'buffered' AND s.receipt_state = 'acknowledged' AND s.cleanup_state <> 'redacted'
     AND b.state = 'committed' AND b.digest = o.digest
     AND ($1::text[] IS NULL OR o.org_id = ANY($1::text[]))
     AND s.stage_key > $2::text
   ORDER BY s.stage_key
   LIMIT $3::int
), stg AS MATERIALIZED (
  SELECT c.*, pol.lazy, pol.pol_identity, pol.pol_epoch, pol.clock_epoch, pol.clock_identity, pol.v2,
         (SELECT count(*) FROM sessions.upload_resets AS ur
           WHERE ur.org_id = c.org_id AND ur.workspace_id = c.workspace_id AND ur.user_id = c.user_id) AS resets,
         coalesce((SELECT max(r.version) FROM sessions.ingest_control_receipts AS r
                    WHERE r.org_id = c.org_id AND r.workspace_id = c.workspace_id AND r.user_id = c.user_id
                      AND r.epoch = pol.pol_epoch), 0) AS head_version
    FROM cand AS c CROSS JOIN pol
), stage_ok AS (
  SELECT st.stage_token,
         coalesce(st.shape_ok AND st.resets = st.reset_epoch
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
       st.origin_id, st.digest, st.group_ordinals,
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
    written: 0, updated: 0, already: 0, would_write: 0, invalid: 0, failed: 0,
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

  // One export at a time (finding 9): the drain's fenced lease, under its own key.
  let lease: DrainLease | undefined;
  if (opts.dryRun) await assertDrainUnlocked(opts.store, opts.keyRoot, nowMs(), exportLockKey(opts.keyRoot));
  else lease = await acquireDrainLock(store, opts.keyRoot, { holder: `export-${randomUUID()}`, ttlMs: opts.lockTtlMs ?? DRAIN_LOCK_TTL_MS, now: nowMs, key: exportLockKey(opts.keyRoot) });
  const ttl = opts.lockTtlMs ?? DRAIN_LOCK_TTL_MS;
  const renew = lease ? setInterval(() => { lease!.renew().catch(() => {}); }, Math.max(500, Math.floor(ttl / 3))) : undefined;
  const leaseOk = () => opts.dryRun || (lease !== undefined && lease.alive());

  try {
    for (const r of (await readOnly(() => sql.query(ORGS, [orgScope]))).rows)
      if (typeof r.org_id === "string") perOrg.set(r.org_id, { stages: 0, bad: !ORG.test(r.org_id) });
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
      const rows = (await readOnly(() => sql.query(VERDICT_PAGE, [orgScope, cursor, pageSize]))).rows;
      if (!rows.length) break;
      cursor = String(rows[rows.length - 1]!.stage_key);
      fences = new Map();
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
        const keptRows = (Array.isArray(r.kept) ? r.kept : []) as { group: number; ords: number[]; source: string; session: string }[];
        const lines = keptLines(r.group_ordinals, keptRows);
        const live = r.stage_ok === true && lines !== null && lines.some((l) => l.length > 0);
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
    // Completion objects, create-only (finding 9), after every candidate stage of the org has its verdict.
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
        if (!leaseOk()) throw new Error("export_lock_lost");
        const ack = await store.put(objectKey(acceptedCompleteKey(opts.keyRoot, org)), encoder.encode(JSON.stringify(doc)), { ifNoneMatch: true, contentType: "application/json" });
        // An identical object already there is an idempotent create (created: false), not a new completion.
        if (ack.created) summary.orgs.complete++;
        else summary.orgs.already_complete++;
      } catch (error) {
        if (isObjectStoreError(error, "already_exists") || isObjectStoreError(error, "precondition_failed")) {
          // An earlier export completed the org; this run's verdicts were (re)written above.
          const prior = await store.getObject(objectKey(acceptedCompleteKey(opts.keyRoot, org))).then(async (g) => parseAcceptedComplete(decoder.decode(await readAll(g.body))), () => null);
          if (prior) summary.orgs.already_complete++;
          else summary.orgs.incomplete++;
        } else summary.orgs.incomplete++;
      }
    }
  } finally {
    if (renew) clearInterval(renew);
    await lease?.release();
  }
  return summary;
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
