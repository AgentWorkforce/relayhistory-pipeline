/**
 * The one-time S3 ledger backfill (docs/decisions/0005-s3-native-upload-ledger.md, "The buffered backlog",
 * step 0, and the fence-authority marker's `fenceBackfill` / `ownershipBackfill` / `revisionBackfill`). At cutover
 * S3 must know what SQL knows, in ledger-s3's formats and through its exported rules only:
 *
 *   fences     every user with any upload state (upload_deletions, upload_resets, ingest_session_ownership,
 *              ingest_record_revisions, owned ingest_lazy_native_slots, ingest_batches) gets `upload-fences.json`:
 *              `editFence` (absent starts from `newFence()`; main sets v1/v2), SQL's deletions and resets applied
 *              with `applyFenceChange` (a new deletion bumps the session's epoch; a new reset request id is recorded
 *              with its tombstone count, increments `resets`, raises `lastResetAtMs`; resetsCompleted == resets).
 *              - A deletion S3 has SUPERSEDED is not re-applied (finding 5), but only when that is PROVEN: S3's
 *                ordered control for the session (`controlOrder`, [epoch, version]) is on the one control epoch of
 *                the user's SQL receipts and above SQL's latest receipt version read in the SAME snapshot as the
 *                deletion. SQL is the ordering authority and writes each receipt with its control's effect, so
 *                such a control was decided after the snapshot, after the deletion. And never while the user has a
 *                SQL reset S3 has not completed for its request id (that reset's tombstones are among the
 *                deletions). Anything else (an unordered S3 re-enable with no version, receipts on another or on
 *                several epochs, no receipt at all: the operator user erase deletes them) is applied, counted in
 *                `deletions_unproven` when S3 had lifted or ordered the session (second review, finding 8).
 *              - A COLD-IMPORTED reset (the seed's baseline import, `ingest_seed_fence_baseline`: its `reset_at` is
 *                the import time, not the reset's) is recorded at time 1: the baseline import requires the user to
 *                have no batch yet, so it predates every batch of the user. If that does not hold for the user
 *                (a batch claimed at or before the import), the user gets NO fence and is `needs_review`: the drain
 *                refuses them rather than drop their stages as pre-reset.
 *              A user with no controls still gets an (empty) fence: the drain refuses an absent one.
 *   ownership  the distinct sessions SQL's reset would own (finding 3): `ingest_session_ownership` (claims, pending
 *              and committed), the sessions of `ingest_record_revisions`, and the sessions of owned lazy native
 *              slots; each a stub (`registerOwner`) at the backfilled fence's generation, with the writer machine.
 *   revisions  SQL's stored revisions into the per-origin shards (`mergeIndex`, never lowering a stored revision):
 *              `ingest_record_revisions` (seeded rows as revision-only entries) and owned `ingest_lazy_native_slots`
 *              (under the slot's owner and session, so S3 forgets them exactly when SQL's `ingest_lazy_clear` does).
 *
 * SQL (finding 8): no long snapshot, and only small per-run sets in memory (one entry per user, one per owned
 * session). Every page is its own short `BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY` transaction, checked
 * with `SHOW transaction_read_only`, always ended by ROLLBACK, one at a time on the connection; SELECTs only, keyset-paged in a stable order (the lazy slots: heap block windows, or with `slotsByOrg` one org-index cursor per
 * org read to the end in one short transaction), optionally scoped to orgs. Fences are read from S3 per page.
 *
 * Catch-up (`revisionsSinceMs`, decision 0005's two-phase cutover, docs/backlog-ops-workflow.md): the
 * ingest_record_revisions pages keep their keyset order over the primary key but return only the rows whose
 * `updated_at` (floored to the ms) is at or after the given time, plus each page's last scanned key (the cursor), so a
 * page scans at most `rowPageSize` rows. No index serves `updated_at` (the primary key and the session index both lead
 * with the org), so the SQL cost is still one walk of the scope's primary key, as a full run's; only the S3 work (fence
 * reads and shard merges) shrinks to the changed rows. Every revision upsert stamps updated_at (insert default now(),
 * each ON CONFLICT DO UPDATE sets now() or clock_timestamp()). Owned ingest_lazy_native_slots have no custody
 * timestamp (initialized_at is the insert time only; a custody move rewrites current_* in place), so they are re-read
 * in full, as are fences and ownership; merges of what is already stored write nothing. Monotone merges (never
 * lowering) make a full run plus a catch-up since before it began converge to one full run over the final SQL state.
 *
 * Fail closed: an unreadable fence is never rewritten; a user with any unrepresentable row gets no fence. Each
 * section reports `complete` only when nothing was skipped for want of a fence, needed review, was invalid or failed.
 * Nothing here logs; the summary holds counts only.
 */
import type { ObjectStore } from "../shared/vendor.ts";
import {
  applyFenceChange, checkIndexEntryInput, fenceEpoch, isDeleted, newFence, recordGeneration, s3LedgerObjects, sessionKeyHash,
  type FenceChange, type FenceDoc, type IndexEntryInput, type OwnerDoc,
} from "./ledger-s3.ts";
import type { LedgerTenant } from "./ledger.ts";
import { liveMachineId } from "./writer.ts";

const ORG = /^(?!\.)[A-Za-z0-9._-]{1,128}$/;
export const MAX_BACKFILL_CONCURRENCY = 16;
export type BackfillSection = "fences" | "ownership" | "revisions";
export const BACKFILL_SECTIONS: readonly BackfillSection[] = ["fences", "ownership", "revisions"];
/** A cold-imported reset's time: before every batch of the user (see the header). */
export const COLD_RESET_AT_MS = 1;

export interface BackfillSql {
  query(sql: string, params?: unknown[]): Promise<{ rows: Record<string, unknown>[] }>;
}

export interface BackfillOptions {
  sql: BackfillSql;
  store: ObjectStore;
  keyRoot: string;
  dryRun: boolean;
  orgs?: string[];
  sections?: BackfillSection[];
  /**
   * Catch-up: only ingest_record_revisions rows with updated_at at or after this epoch ms (see the header). Needs the
   * revisions section. Owned lazy slots, fences and ownership are still read in full.
   */
  revisionsSinceMs?: number;
  /** Users per fence page (default 500); rows per ownership/revision page (default 5000). */
  pageSize?: number;
  rowPageSize?: number;
  concurrency?: number;
  statementTimeoutMs?: number;
  /** Heap blocks per slot window (default SLOT_WINDOW_BLOCKS); tests use a small one to cross windows. */
  slotWindowBlocks?: number;
  /**
   * Read a scoped run's owned slots once per org through the org_id index (SLOTS_ORG_CURSOR) instead of the heap block
   * windows. Off by default (CLI `--slots-by-org`); backlog-ops passes it only for the input slots_read=org.
   */
  slotsByOrg?: boolean;
  /** With slotsByOrg, the per-org slot read cap (default SLOTS_ORG_READ_MAX_ROWS); above it the org uses the windows. */
  slotOrgReadMaxRows?: number;
  /**
   * With slotsByOrg only: milliseconds slept between two FETCHes of an org's slot cursor (default 0: no pause, the
   * read unchanged), in [0, SLOT_PACE_MAX_MS]. A paced org read that would outlive SLOTS_ORG_PACED_READ_MAX_MS fails
   * closed (slot_read_too_long, counted failed). See SLOTS_ORG_CURSOR.
   */
  slotPaceMs?: number;
  /**
   * With slotsByOrg only: rows per FETCH of the org slot cursor, in [SLOT_FETCH_ROWS_MIN, SLOT_FETCH_ROWS_MAX]. Default
   * 5000 unpaced (the read unchanged) and SLOT_FETCH_ROWS_PACED (2000) when slotPaceMs > 0: smaller, evener bursts.
   */
  slotFetchRows?: number;
  /** The clock for the paced read's duration bound (default Date.now); tests inject one. */
  now?: () => number;
  /**
   * Revision entries buffered across pages and windows before one merge pass (default MERGE_BUFFER_ENTRIES): each
   * shard is read and written once per pass, not once per page or window.
   */
  mergeBufferEntries?: number;
  sleep?: (ms: number) => Promise<void>;
  random?: () => number;
  /**
   * A stop the caller raises mid-run (scripts/upload-fence-backfill.ts: the ledger-home re-check's sticky refusal).
   * Checked at every boundary of the ownership and revision sections (before each SQL page, slot window or org read,
   * after each merge pass): once it returns an error the run throws it there, reading no further page and starting no
   * further write. The fences section runs first and is not checked (it writes no ledger key).
   */
  halt?: () => unknown;
}

export interface BackfillSummary {
  mode: "dry_run" | "execute";
  users: number;
  fences: {
    deletion_rows: number; reset_rows: number;
    created: number; updated: number; unchanged: number; would_create: number; would_update: number;
    pairs_added: number; resets_added: number;
    /** SQL deletions not re-applied because S3's control for the session is provably newer (finding 5). */
    deletions_superseded: number;
    /** SQL deletions re-applied although S3 had lifted or ordered the session since: newer not provable (finding 8). */
    deletions_unproven: number;
    /**
     * Exhaustive, counts-only evidence for every `deletions_unproven` row. The values sum exactly to
     * `deletions_unproven`: an ordered tuple on SQL's one receipt epoch can be compared, while a missing receipt,
     * several receipt epochs, a foreign epoch or an unordered lift cannot prove recency either way.
     */
    deletion_order: DeletionOrderProofCounts;
    /** Cold-imported resets recorded at COLD_RESET_AT_MS. */
    cold_resets: number;
    /** Users given no fence because a cold-imported reset could not be placed before all their batches. */
    needs_review: number;
    unreadable: number; contended: number; tenant_invalid: number; input_invalid: number; failed: number;
    resets_in_flight: number;
  };
  ownership: { rows: number; registered: number; would_register: number; unchanged: number; skipped_deleted: number; skipped_no_fence: number; invalid: number; failed: number;
    /** `failed` by error code (fixed lowercase codes, never a message): the receipt says why, not just how many. */
    failed_codes: Record<string, number> };
  revisions: {
    rows: number; seed_rows: number;
    slot_rows: number; slots_empty: number;
    merged: number; would_merge: number; skipped_deleted: number; skipped_no_fence: number; invalid: number; shards: number; failed: number;
    /** `failed` by error code, as ownership's. */
    failed_codes: Record<string, number>;
  };
  sections: Record<BackfillSection, "complete" | "incomplete" | "not_run">;
  /**
   * The run's input fingerprint (BackfillFingerprint): read before any section, except that a scoped slotsByOrg run with a
   * slot pass sums its three slots values from that pass (each org checked before its slot writes).
   */
  fingerprint: BackfillFingerprint;
  /** A catch-up's `revisionsSinceMs` (an epoch ms, not identifying); absent on a full run. */
  revisions_since_ms?: number;
}

export const DELETION_ORDER_PROOF_CODES = [
  "ordered_not_newer",
  "ordered_newer",
  "receipts_absent",
  "receipts_multiple_epochs",
  "epoch_mismatch",
  "unordered_lift",
  "order_invalid",
] as const;
export type DeletionOrderProofCode = (typeof DELETION_ORDER_PROOF_CODES)[number];
export type DeletionOrderProofCounts = Record<DeletionOrderProofCode, number>;
const emptyDeletionOrderProof = (): DeletionOrderProofCounts => Object.fromEntries(
  DELETION_ORDER_PROOF_CODES.map((code) => [code, 0]),
) as DeletionOrderProofCounts;

/**
 * The scope's revision-section inputs, summarised at the start of every run (dry and execute) in ONE REPEATABLE READ
 * READ ONLY transaction (one snapshot): ingest_record_revisions (rows, and the latest updated_at in epoch ms, 0 when
 * none: every revision upsert sets it), ingest_session_ownership (rows, and a content hash of every row's key) and
 * the owned ingest_lazy_native_slots (rows, the sum of current_revision, and a content hash of every row's key and
 * current custody). The backlog-ops gate lets a drain bind a fences+ownership REFRESH beside a FULL run only when the
 * two fingerprints are equal: no revision, ownership or owned-slot row the refresh did not re-read changed since the
 * full run began (an insert or delete moves a count; an upsert moves the revisions' max updated_at; a swapped or
 * rewritten ownership or slot row moves its hash).
 *
 * Every value is a non-negative safe integer (receipts are counts only). `slots_revision_sum` is taken modulo 2^53
 * and the three hashes are sums of Postgres's hashtext (a signed int4) normalised into [0, 2^40); all are equality
 * checks, so the wrap loses only changes of an exact multiple of the modulus. hashtext is a server-internal hash,
 * stable within one server but not a cross-version contract: the fingerprint is compared only between two receipts
 * at the same commit from the same database, under 24 h apart, and is never stored outside the receipts.
 *
 * Snapshot caveat (slotsByOrg): a scoped slotsByOrg run whose slot pass runs (ownership or revisions) does not send
 * FINGERPRINT_SLOTS; its three slots values are summed from the pass's own per-org cursor reads (SlotFingerprint, with
 * hashtext computed by the server per row), equal to FINGERPRINT_SLOTS over the same rows, but read in each org's
 * cursor snapshot, later than (and per org apart from) the one snapshot of the revisions and ownership values. They
 * then describe exactly the slots the run processed. The gate's equality check keeps its meaning: an owned-slot row
 * changed between two runs' reads still moves a value.
 */
export interface BackfillFingerprint {
  revisions_count: number;
  revisions_max_updated_ms: number;
  revisions_hash: number;
  ownership_count: number;
  ownership_hash: number;
  slots_owned_count: number;
  slots_revision_sum: number;
  slots_hash: number;
}
export const SLOTS_REVISION_SUM_MODULUS = "9007199254740992"; // 2^53
export const FINGERPRINT_HASH_MODULUS = "1099511627776"; // 2^40

const SCOPE = `($1::text[] IS NULL OR org_id = ANY($1::text[]))`;
/**
 * A scoped run's form of SCOPE (scopedSql). The "$1 IS NULL OR ..." form lets one text serve both scopes, but a generic
 * plan for it cannot use an org_id index: on production FINGERPRINT_SLOTS planned a parallel sequential scan of the
 * whole multi-GB ingest_lazy_native_slots heap for a six-org scope, and that read saturated the database's IO (an
 * aborted backfill-dry, 2026-11-17). A scoped run sends plain `org_id = ANY($1)`, which the org index serves.
 */
const SCOPED = `org_id = ANY($1::text[])`;
/** The text a run sends: unscoped runs unchanged (byte-identical); scoped runs with every SCOPE as SCOPED. */
export function scopedSql(query: string, scoped: boolean): string {
  return scoped ? query.split(SCOPE).join(SCOPED) : query;
}
/**
 * The users, collected ONCE per run (not per page): a keyset page over a DISTINCT union of six tables re-read and
 * re-sorted the whole (scoped) union on every page, minutes each on production. Every table but ingest_batches has an
 * index leading with (org_id, workspace_id, <user>), so its distinct users are a loose index scan (a recursive CTE
 * that jumps from one user to the next, one index probe per user); ingest_batches has no such index and is read once
 * with a plain DISTINCT. The union is merged, sorted and paged in memory (a small set: one entry per user).
 */
const USER_SOURCES: { table: string; user: string; where?: string }[] = [
  { table: "upload_deletions", user: "user_id" },
  { table: "upload_resets", user: "user_id" },
  { table: "ingest_session_ownership", user: "user_id" },
  { table: "ingest_record_revisions", user: "user_id" },
  { table: "ingest_lazy_native_slots", user: "current_owner", where: "current_owner IS NOT NULL" },
];
/** Distinct (org, workspace, user) of one indexed table, scoped to one org ($1) or every org (no $1). */
export function usersSkipScan(src: { table: string; user: string; where?: string }, scoped: boolean): string {
  const conds = [...(scoped ? ["org_id = $1::text"] : []), ...(src.where ? [src.where] : [])];
  const pre = conds.length ? conds.join(" AND ") + " AND " : "";
  const key = `org_id, workspace_id, ${src.user}`;
  return `
WITH RECURSIVE k AS (
  (SELECT org_id, workspace_id, ${src.user} AS user_id FROM sessions.${src.table}
    WHERE ${pre}(${key}) > ('', '', '') ORDER BY ${key} LIMIT 1)
  UNION ALL
  SELECT n.org_id, n.workspace_id, n.user_id FROM k CROSS JOIN LATERAL (
    SELECT org_id, workspace_id, ${src.user} AS user_id FROM sessions.${src.table}
     WHERE ${pre}(${key}) > (k.org_id, k.workspace_id, k.user_id) ORDER BY ${key} LIMIT 1) AS n
)
SELECT org_id, workspace_id, user_id FROM k`;
}
const BATCH_USERS = `SELECT DISTINCT org_id, workspace_id, user_id FROM sessions.ingest_batches WHERE ${SCOPE}`;
/**
 * Deletions, each with the user's control watermark from the SAME snapshot (finding 8): how many control epochs the
 * user's SQL receipts span, that epoch, and the latest receipt version on it.
 */
const DELETIONS_FOR = `
SELECT d.org_id, d.workspace_id, d.user_id, d.origin_id, d.source, d.session_id, wm.ctl_epochs, wm.ctl_epoch, wm.ctl_version
FROM sessions.upload_deletions AS d
JOIN unnest($1::text[], $2::text[], $3::text[]) AS p(o, w, u) ON d.org_id = p.o AND d.workspace_id = p.w AND d.user_id = p.u
CROSS JOIN LATERAL (
  SELECT count(DISTINCT r.epoch) AS ctl_epochs, min(r.epoch) AS ctl_epoch, max(r.version)::text AS ctl_version
    FROM sessions.ingest_control_receipts AS r
   WHERE r.org_id = d.org_id AND r.workspace_id = d.workspace_id AND r.user_id = d.user_id
) AS wm
ORDER BY d.org_id, d.workspace_id, d.user_id, d.origin_id, d.source, d.session_id`;
/** Resets, with the cold-import evidence: the user's seed baseline import time, and whether a batch predates it. */
const RESETS_FOR = `
SELECT r.org_id, r.workspace_id, r.user_id, r.request_id, r.deleted,
       ceil(extract(epoch FROM r.reset_at) * 1000)::bigint AS at_ms,
       (bl.imported_at IS NOT NULL AND r.reset_at <= bl.imported_at) AS cold,
       EXISTS (SELECT 1 FROM sessions.ingest_batches AS b
                WHERE b.org_id = r.org_id AND b.workspace_id = r.workspace_id AND b.user_id = r.user_id
                  AND bl.imported_at IS NOT NULL AND b.claimed_at <= bl.imported_at) AS batch_before_import
FROM sessions.upload_resets AS r
JOIN unnest($1::text[], $2::text[], $3::text[]) AS p(o, w, u) ON r.org_id = p.o AND r.workspace_id = p.w AND r.user_id = p.u
LEFT JOIN sessions.ingest_seed_fence_baselines AS bl
  ON bl.org_id = r.org_id AND bl.workspace_id = r.workspace_id AND bl.user_id = r.user_id
ORDER BY r.org_id, r.workspace_id, r.user_id, r.reset_at, r.request_id`;
/**
 * The sessions SQL's reset would own (finding 3), distinct, keyset-paged. The cursor, order and limit sit inside each
 * branch so each reads its own index from the cursor (ownership's primary key, revisions' session index) instead of
 * the whole union per page. Owned lazy slots are not here: the slot pass, which reads every slot of the scope once,
 * registers their sessions (registerSlotOwners).
 */
const OWN_KEY = "org_id, workspace_id, user_id, origin_id, source, session_id";
const OWN_AFTER = "(org_id, workspace_id, user_id, origin_id, source, session_id) > ($2::text, $3::text, $4::text, $5::text, $6::text, $7::text)";
const OWNERSHIP_PAGE = `
SELECT DISTINCT ${OWN_KEY} FROM (
  (SELECT ${OWN_KEY} FROM sessions.ingest_session_ownership WHERE ${SCOPE} AND ${OWN_AFTER} ORDER BY ${OWN_KEY} LIMIT $8::int)
  UNION ALL
  (SELECT DISTINCT ${OWN_KEY} FROM sessions.ingest_record_revisions
    WHERE ${SCOPE} AND session_id IS NOT NULL AND ${OWN_AFTER} ORDER BY ${OWN_KEY} LIMIT $8::int)
) AS o
ORDER BY ${OWN_KEY}
LIMIT $8::int`;
/**
 * The slots read: one window of heap blocks (a TID range scan), filtered to the scope's orgs when scoped. Keyset paging on
 * key_sha256 with an org filter walks the primary key and fetches every slot's heap tuple at random to test its org
 * (about 21 s a page on production's tens of millions of slots, scoped or not); a block window reads the heap in
 * order instead, so a whole pass is one sequential read. A slot rewritten during the pass can move to a block already
 * read and be missed, so a scoped run is exact only while its orgs admit nothing: run the execute after the scope's
 * HTTP admission stop (decision 0005's cutover order). The unscoped pass reads the same windows: run while other orgs
 * still admit, it pre-warms their shards and may miss their moved slots, so it certifies no org that was not stopped
 * before it began. Each org's cutover still takes a run that re-reads the slots after its own stop: a full run, or a
 * catch-up after a full run made while the org was live (merges of what is already stored change nothing, so that run
 * is mostly reads).
 */
const SLOT_WINDOW_BLOCKS = 4096;
/** About 4M entries (a few GB of heap) per merge pass; a 10M-slot org merges in three passes. */
const MERGE_BUFFER_ENTRIES = 4_000_000;
const SLOTS_BLOCKS = `SELECT (pg_relation_size('sessions.ingest_lazy_native_slots') / current_setting('block_size')::bigint)::bigint AS blocks`;
const SLOTS_WINDOW = `
SELECT key_sha256, org_id, workspace_id, origin_id, record_id, current_owner, current_source, current_session, current_revision, current_digest
FROM sessions.ingest_lazy_native_slots
WHERE ($1::text[] IS NULL OR org_id = ANY($1::text[])) AND ctid >= $2::tid AND ctid < $3::tid`;
/**
 * The owned slot's row text the input fingerprint hashes (FINGERPRINT_SLOTS, and per row in SLOTS_ORG_CURSOR): every
 * nullable column coalesced to a fixed sentinel, so a NULL keeps its position and the row still counts.
 */
const SLOT_FINGERPRINT_ROW = "concat_ws(E'\\t', key_sha256, current_owner, coalesce(current_source, '~'), coalesce(current_session, '~'), coalesce(current_revision::text, '~'), coalesce(current_digest, '~'))";
/**
 * A scoped run's slots, one org at a time through ingest_lazy_native_slots' org_id index in ONE read-only transaction
 * (a server-side cursor fetched to the end before any S3 work, so no snapshot outlives the read and no S3 latency
 * lengthens it). A keyset page over key_sha256 re-sorts the whole org on every page, and the block walk reads the
 * whole heap for any scope.
 *
 * Each row also carries `fp_hash`, Postgres's own hashtext of SLOT_FINGERPRINT_ROW (the exact expression
 * FINGERPRINT_SLOTS sums), so the run's slots fingerprint is summed from this pass in TypeScript (slotFingerprintOf)
 * instead of a second read of the same rows: hashtext is server-internal and not reproduced in TypeScript; only the
 * exact integer sum and the modulus normalisation are, and those are plain integer arithmetic (BigInt).
 *
 * Pacing (slotPaceMs, slotFetchRows): the read sleeps between FETCHes, so a multi-million-row org is read as a stream
 * of small index-scan bursts instead of one IO-saturating sweep. The rows are still buffered to the end and processed
 * after the ROLLBACK (not per batch): S3 work inside the open transaction would stretch the snapshot by minutes of
 * merges, and the read must stay one snapshot (the fingerprint's and the ownership/revision pass's view of the org).
 * Pacing lengthens the transaction by design, so a paced read is bounded by SLOTS_ORG_PACED_READ_MAX_MS: a read that
 * would outlive it is abandoned (ROLLBACK) and the org fails closed (slot_read_too_long, counted failed in each run
 * section, so the receipt is incomplete and binds nothing); it is never retried through the block windows, which
 * would read the heap that pacing is meant to spare. Each FETCH stays under statement_timeout and the pause under the
 * session's idle_in_transaction_session_timeout (60 s; SLOT_PACE_MAX_MS is 5 s).
 */
const SLOTS_ORG_CURSOR = `
DECLARE backfill_slots NO SCROLL CURSOR FOR
SELECT key_sha256, org_id, workspace_id, origin_id, record_id, current_owner, current_source, current_session, current_revision, current_digest,
       hashtext(${SLOT_FINGERPRINT_ROW})::bigint AS fp_hash
FROM sessions.ingest_lazy_native_slots WHERE org_id = $1::text`;
const slotsOrgFetch = (rows: number) => `FETCH FORWARD ${rows} FROM backfill_slots`;
/**
 * Above this many slots in one org the org-scoped read stops buffering rows (bounded memory) and the org's rows are
 * processed through the block walk; the cursor is still read to the end for the org's fingerprint.
 */
// A memory guard for the runner, not a correctness bound: the org's rows are buffered (roughly rows x ~350 B), so 4M is
// about 1.4 GB, inside a GitHub-hosted runner; the largest production org has ~2.9M owned slots.
export const SLOTS_ORG_READ_MAX_ROWS = 4_000_000;
/** slotPaceMs bounds (ms): 0 is unpaced. */
export const SLOT_PACE_MAX_MS = 5_000;
/** slotFetchRows bounds, and the defaults: unpaced (the original 5000) and paced. */
export const SLOT_FETCH_ROWS_MIN = 100;
export const SLOT_FETCH_ROWS_MAX = 50_000;
export const SLOT_FETCH_ROWS_DEFAULT = 5_000;
export const SLOT_FETCH_ROWS_PACED = 2_000;
/**
 * The longest a PACED org slot read may hold its read-only transaction (30 min). Long enough for an org of about 3M
 * slots at 2000 rows and 500 ms a fetch (1500 fetches, about 13 min of pauses plus the reads); short enough that one
 * snapshot does not hold back vacuum on the primary for hours. Unpaced reads are not bounded by it (unchanged).
 */
export const SLOTS_ORG_PACED_READ_MAX_MS = 30 * 60_000;
const REVISIONS_PAGE = `
SELECT org_id, workspace_id, origin_id, record_id, revision, revision_id, digest, user_id, source, session_id
FROM sessions.ingest_record_revisions
WHERE ${SCOPE} AND (org_id, workspace_id, origin_id, record_id) > ($2::text, $3::text, $4::text, $5::text)
ORDER BY org_id, workspace_id, origin_id, record_id
LIMIT $6::int`;
/**
 * The catch-up's page (revisionsSinceMs): the same keyset window of at most $6 rows over the primary key, returning
 * only the rows updated at or after $7 (epoch ms, the fingerprint's floored-ms form) plus the window's last row, so the
 * cursor always advances by one window whatever matched. `fresh` marks the rows to apply.
 */
const REVISIONS_SINCE_PAGE = `
WITH p AS (
  SELECT org_id, workspace_id, origin_id, record_id, revision, revision_id, digest, user_id, source, session_id,
         floor(extract(epoch FROM updated_at) * 1000) >= $7::bigint AS fresh
  FROM sessions.ingest_record_revisions
  WHERE ${SCOPE} AND (org_id, workspace_id, origin_id, record_id) > ($2::text, $3::text, $4::text, $5::text)
  ORDER BY org_id, workspace_id, origin_id, record_id
  LIMIT $6::int
), l AS (
  SELECT org_id, workspace_id, origin_id, record_id FROM p ORDER BY org_id DESC, workspace_id DESC, origin_id DESC, record_id DESC LIMIT 1
)
SELECT p.* FROM p CROSS JOIN l
WHERE p.fresh OR (p.org_id, p.workspace_id, p.origin_id, p.record_id) = (l.org_id, l.workspace_id, l.origin_id, l.record_id)
ORDER BY p.org_id, p.workspace_id, p.origin_id, p.record_id`;

/** The fingerprint's three reads (BackfillFingerprint), run in one read-only transaction, so one snapshot. */
/** A row-content hash sum in [0, 2^40): hashtext is a signed int4 and % keeps the sign, so it is normalised. */
const hashSum = (row: string) =>
  `(((coalesce(sum(hashtext(${row})::bigint), 0) % ${FINGERPRINT_HASH_MODULUS}) + ${FINGERPRINT_HASH_MODULUS}) % ${FINGERPRINT_HASH_MODULUS})::bigint`;
// The revisions' content hash as well as count + max(updated_at): a writer whose transaction began before the full
// run's snapshot commits rows stamped at or below that max, and two updates in one floored millisecond are invisible
// to the max; the content hash sees both.
const FINGERPRINT_REVISIONS = `
SELECT count(*)::bigint AS n, coalesce(floor(extract(epoch FROM max(updated_at)) * 1000), 0)::bigint AS max_ms,
       ${hashSum("concat_ws(E'\\t', org_id, workspace_id, origin_id, record_id, revision::text, coalesce(revision_id, '~'), coalesce(digest, '~'), user_id, source, coalesce(session_id, '~'))")} AS h
FROM sessions.ingest_record_revisions WHERE ${SCOPE}`;
const FINGERPRINT_OWNERSHIP = `
SELECT count(*)::bigint AS n,
       ${hashSum("concat_ws(E'\\t', org_id, workspace_id, user_id, origin_id, source, session_id)")} AS h
FROM sessions.ingest_session_ownership WHERE ${SCOPE}`;
// Every nullable column coalesced to a fixed sentinel (SLOT_FINGERPRINT_ROW), so a NULL keeps its position and the row
// still counts. A scoped slotsByOrg run does not send it: it sums the same values from its slot cursor (slotFingerprintOf).
const FINGERPRINT_SLOTS = `
SELECT count(*)::bigint AS n, (coalesce(sum(current_revision), 0) % ${SLOTS_REVISION_SUM_MODULUS})::bigint AS rev_sum,
       ${hashSum(SLOT_FINGERPRINT_ROW)} AS h
FROM sessions.ingest_lazy_native_slots WHERE ${SCOPE} AND current_owner IS NOT NULL`;

/**
 * FINGERPRINT_SLOTS's three values, accumulated in TypeScript over slot rows read with SLOTS_ORG_CURSOR (each with its
 * server-computed `fp_hash`): count of owned rows, the exact sum of their non-NULL current_revision, the exact sum of
 * their fp_hash. `result` applies the SQL's normalisation exactly: rev_sum = sum % 2^53, h = ((sum % 2^40) + 2^40) %
 * 2^40, where BigInt's % keeps the dividend's sign as numeric's does. Sums over disjoint org sets add, so per-org
 * accumulators combine into the scope's value.
 */
export class SlotFingerprint {
  count = 0n;
  revisionSum = 0n;
  hashSum = 0n;
  add(r: Record<string, unknown>): void {
    if (r.current_owner === null || r.current_owner === undefined) return;
    this.count++;
    if (r.current_revision !== null && r.current_revision !== undefined) this.revisionSum += bigIntOf(r.current_revision);
    this.hashSum += bigIntOf(r.fp_hash);
  }
  merge(o: SlotFingerprint): void {
    this.count += o.count;
    this.revisionSum += o.revisionSum;
    this.hashSum += o.hashSum;
  }
  /** As FINGERPRINT_SLOTS's row: { n, rev_sum, h } (strings, as pg returns bigint). */
  result(): { n: string; rev_sum: string; h: string } {
    const rev = this.revisionSum % BigInt(SLOTS_REVISION_SUM_MODULUS), m = BigInt(FINGERPRINT_HASH_MODULUS);
    return { n: String(this.count), rev_sum: String(rev), h: String(((this.hashSum % m) + m) % m) };
  }
}
/** A SlotFingerprint's three values, each a non-negative safe integer (int()), or backfill_fingerprint_invalid. */
function checkedSlotsFingerprint(fp: SlotFingerprint): [number, number, number] {
  const r = fp.result();
  const values = [int(r.n), int(r.rev_sum), int(r.h)];
  if (values.some((v) => v === undefined)) throw new Error("backfill_fingerprint_invalid");
  return values as [number, number, number];
}
const bigIntOf = (v: unknown): bigint => {
  if (typeof v === "bigint") return v;
  if (typeof v === "number" && Number.isSafeInteger(v)) return BigInt(v);
  if (typeof v === "string" && /^-?[0-9]{1,20}$/.test(v)) return BigInt(v);
  throw new Error("backfill_fingerprint_invalid");
};

const userKey = (o: unknown, w: unknown, u: unknown) => JSON.stringify([o, w, u]);
/**
 * Attempts per store operation (see storeRetry), and the store failures worth retrying: `transient` (network, 408,
 * 429, 5xx), `backend` (any other backend status, e.g. S3's 400 RequestTimeout) and `error` (a non-store error, e.g.
 * a reset while reading a 200's body). Never contended / invalid / refused codes.
 */
const STORE_ATTEMPTS = 5;
const RETRYABLE_STORE_CODES = new Set(["ledger_store_transient", "ledger_store_backend", "ledger_store_error"]);
const text = (v: unknown): string => {
  if (typeof v !== "string") throw new Error("backfill_row_invalid");
  return v;
};
const id = (v: unknown): v is string => typeof v === "string" && v.length > 0 && v !== "__proto__";
const int = (v: unknown): number | undefined => {
  const n = typeof v === "bigint" || typeof v === "string" ? Number(v) : v;
  return typeof n === "number" && Number.isSafeInteger(n) && n >= 0 ? n : undefined;
};

async function mapLimit<T>(items: readonly T[], limit: number, fn: (item: T) => Promise<void>): Promise<void> {
  let next = 0;
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (next < items.length) await fn(items[next++]!);
  }));
}

/**
 * The user's SQL control watermark, read in the deletions' snapshot: the number of control epochs their receipts span,
 * that epoch (when one), and the latest receipt version on it. `null` when unreadable.
 */
export interface SqlControlWatermark { epochs: number; epoch: string | null; version: bigint | null }

/** One user's SQL control state, as read. */
interface UserControls {
  tenant: LedgerTenant;
  deletions: Map<string, [string, string][]>;
  watermark: SqlControlWatermark | null;
  resets: { requestId: string; atMs: number; result: number; cold: boolean }[];
  invalid: boolean;
  needsReview: boolean;
}

const VERSION = /^[1-9][0-9]{0,18}$/;
const HEX64 = /^[0-9a-f]{64}$/;
function watermarkOf(r: Record<string, unknown>): SqlControlWatermark | null {
  const epochs = int(r.ctl_epochs);
  if (epochs === undefined) return null;
  if (epochs === 0) return { epochs, epoch: null, version: null };
  if (typeof r.ctl_epoch !== "string" || !HEX64.test(r.ctl_epoch) || typeof r.ctl_version !== "string" || !VERSION.test(r.ctl_version)) return null;
  return { epochs, epoch: r.ctl_epoch, version: BigInt(r.ctl_version) };
}

/**
 * S3's ordered control `at` ([epoch, version]) for a session is provably newer than everything SQL's snapshot holds:
 * the user's receipts are on exactly one epoch, `at`'s, and `at` is above their latest version. SQL orders controls
 * and writes a receipt with each control's effect, contiguously per epoch, so `at` was decided after the snapshot.
 * No receipt at all proves nothing (an operator erase deletes them); several epochs, or another one, are unordered.
 */
export function provablyNewer(at: unknown, wm: SqlControlWatermark | null | undefined): boolean {
  if (!wm || wm.epochs !== 1 || wm.epoch === null || wm.version === null) return false;
  if (!Array.isArray(at) || at.length !== 2 || at[0] !== wm.epoch || typeof at[1] !== "string" || !VERSION.test(at[1])) return false;
  return BigInt(at[1]) > wm.version;
}

/**
 * Counts-only evidence for an S3 state that made the legacy backfill call a SQL deletion `unproven`.
 * `ordered_not_newer` is the only positive proof requested by the guarded cutover: S3's tuple is on SQL's sole
 * receipt epoch and no higher than the latest receipt in the same SQL snapshot. `ordered_newer` proves the opposite;
 * every other code is deliberately inconclusive and must block a proof-required execute.
 */
export function deletionOrderProof(
  at: unknown,
  wm: SqlControlWatermark | null | undefined,
  lifted: boolean,
): DeletionOrderProofCode | null {
  if (at === undefined) return lifted ? "unordered_lift" : null;
  if (!Array.isArray(at) || at.length !== 2 || typeof at[0] !== "string" || !HEX64.test(at[0])
    || typeof at[1] !== "string" || !VERSION.test(at[1])) return "order_invalid";
  if (!wm) return "order_invalid";
  if (wm.epochs === 0) return "receipts_absent";
  if (wm.epochs !== 1) return "receipts_multiple_epochs";
  if (wm.epoch === null || wm.version === null) return "order_invalid";
  if (at[0] !== wm.epoch) return "epoch_mismatch";
  return BigInt(at[1]) <= wm.version ? "ordered_not_newer" : "ordered_newer";
}

/**
 * SQL's controls applied to `f` with ledger-s3's rules (in place). A deletion is skipped only when S3's ordered control
 * for the session is provably newer than it (provablyNewer, against the deletion snapshot's watermark), and never while
 * the user has a SQL reset S3 has not completed for its request id. A deletion applied although S3 had lifted or
 * ordered the session is counted `unproven`.
 */
export function applySqlControls(f: FenceDoc, st: Pick<UserControls, "deletions" | "resets"> & { watermark?: SqlControlWatermark | null }): {
  changed: boolean; pairs: number; resets: number; superseded: number; unproven: number; deletionOrder: DeletionOrderProofCounts;
} {
  let changed = false, pairs = 0, resets = 0, superseded = 0, unproven = 0;
  const deletionOrder = emptyDeletionOrderProof();
  const deletions: FenceChange["deletions"] = [];
  // A SQL reset S3 has not recorded, or has recorded but not finished: its tombstones are SQL's deletions, so none of
  // them may be skipped, whatever S3's order says.
  const resetOpen = st.resets.some((r) => !Object.hasOwn(f.resetRequests, r.requestId) || f.resetRequests[r.requestId] === null);
  for (const [originId, sessions] of st.deletions) {
    const keep = sessions.filter(([s, sid]) => {
      const sk = sessionKeyHash(s, sid);
      const perOrigin = f.controlOrder?.sessions && Object.hasOwn(f.controlOrder.sessions, originId) ? f.controlOrder.sessions[originId]! : undefined;
      const at = perOrigin && Object.hasOwn(perOrigin, sk) ? perOrigin[sk] : undefined;
      if (!resetOpen && at !== undefined && provablyNewer(at, st.watermark)) { superseded++; return false; }
      const lifted = fenceEpoch(f, originId, sk) > 0 && !isDeleted(f, originId, s, sid);
      if (at !== undefined || lifted) {
        unproven++;
        const proof = deletionOrderProof(at, st.watermark, lifted);
        deletionOrder[proof ?? "order_invalid"]++;
      }
      return true;
    });
    if (keep.length) deletions.push({ originId, sessions: keep });
  }
  for (const c of [{ deletions } as FenceChange, ...st.resets.map((r) => ({ reset: { requestId: r.requestId, atMs: r.atMs, result: r.result } }))]) {
    const r = applyFenceChange(f, c);
    changed ||= r.changed;
    pairs += r.newlyDeleted.length;
    if (r.reset === "recorded") resets++;
  }
  return { changed, pairs, resets, superseded, unproven, deletionOrder };
}

/** Paged, short-transaction, read-only S3 ledger backfill. Counts only. */
export async function backfillUploadFences(opts: BackfillOptions): Promise<BackfillSummary> {
  const pageSize = opts.pageSize ?? 500;
  const rowPageSize = opts.rowPageSize ?? 5_000;
  const concurrency = opts.concurrency ?? 4;
  const statementTimeoutMs = opts.statementTimeoutMs ?? 30_000;
  const slotWindow = opts.slotWindowBlocks ?? SLOT_WINDOW_BLOCKS;
  if (!Number.isInteger(slotWindow) || slotWindow < 1 || slotWindow > 1_048_576) throw new Error("backfill_slot_window_invalid");
  const slotOrgMax = opts.slotOrgReadMaxRows ?? SLOTS_ORG_READ_MAX_ROWS;
  if (!Number.isInteger(slotOrgMax) || slotOrgMax < 0) throw new Error("backfill_slot_org_max_invalid");
  const slotPaceMs = opts.slotPaceMs ?? 0;
  const slotFetchRows = opts.slotFetchRows ?? (slotPaceMs > 0 ? SLOT_FETCH_ROWS_PACED : SLOT_FETCH_ROWS_DEFAULT);
  if (!Number.isInteger(slotPaceMs) || slotPaceMs < 0 || slotPaceMs > SLOT_PACE_MAX_MS ||
      !Number.isInteger(slotFetchRows) || slotFetchRows < SLOT_FETCH_ROWS_MIN || slotFetchRows > SLOT_FETCH_ROWS_MAX ||
      ((opts.slotPaceMs !== undefined || opts.slotFetchRows !== undefined) && opts.slotsByOrg !== true)) throw new Error("backfill_slot_pace_invalid");
  const sleep = opts.sleep ?? ((ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms)));
  const now = opts.now ?? Date.now;
  const mergeBuffer = opts.mergeBufferEntries ?? MERGE_BUFFER_ENTRIES;
  if (!Number.isInteger(mergeBuffer) || mergeBuffer < 1 || mergeBuffer > 50_000_000) throw new Error("backfill_merge_buffer_invalid");
  const sections = new Set(opts.sections ?? BACKFILL_SECTIONS);
  if (!Number.isInteger(pageSize) || pageSize < 1 || pageSize > 5_000) throw new Error("backfill_page_size_invalid");
  if (!Number.isInteger(rowPageSize) || rowPageSize < 1 || rowPageSize > 50_000) throw new Error("backfill_row_page_size_invalid");
  if (!Number.isInteger(concurrency) || concurrency < 1 || concurrency > MAX_BACKFILL_CONCURRENCY) throw new Error("backfill_concurrency_invalid");
  if (!Number.isInteger(statementTimeoutMs) || statementTimeoutMs < 1_000 || statementTimeoutMs > 600_000) throw new Error("backfill_statement_timeout_invalid");
  if (!/^[a-z0-9][a-z0-9-]{0,62}$/.test(opts.keyRoot)) throw new Error("backfill_key_root_invalid");
  if (opts.orgs) for (const org of opts.orgs) if (!ORG.test(org)) throw new Error("backfill_org_invalid");
  if (![...sections].every((s) => BACKFILL_SECTIONS.includes(s)) || sections.size === 0) throw new Error("backfill_sections_invalid");
  const since = opts.revisionsSinceMs;
  if (since !== undefined && (!Number.isSafeInteger(since) || since < 1)) throw new Error("backfill_revisions_since_invalid");
  if (since !== undefined && !sections.has("revisions")) throw new Error("backfill_revisions_since_sections");
  const orgs = opts.orgs && opts.orgs.length ? [...new Set(opts.orgs)] : null;
  /** A scoped run sends its scoped reads as plain `org_id = ANY($1)` (scopedSql) with custom plans. */
  const scopedRun = orgs !== null;
  /**
   * A scoped slotsByOrg run whose slot pass runs (ownership or revisions) takes the slots fingerprint from that pass's
   * per-org cursors (SlotFingerprint) instead of FINGERPRINT_SLOTS: each org's slots are read once. Those three values
   * are then from each org's cursor snapshot, not from the run-start snapshot of the revisions and ownership values.
   */
  const slotsFromCursor = opts.slotsByOrg === true && orgs !== null && (sections.has("revisions") || sections.has("ownership"));
  // The dry run's defence in depth: every mutation throws (and is counted `failed`), so nothing is written.
  const store: ObjectStore = opts.dryRun
    ? new Proxy(opts.store, {
        get(target, prop) {
          if (prop === "put" || prop === "delete") return async () => { throw new Error("backfill_dry_run_write_refused"); };
          const value = (target as unknown as Record<PropertyKey, unknown>)[prop];
          return typeof value === "function" ? (value as (...a: unknown[]) => unknown).bind(target) : value;
        },
      })
    : opts.store;
  const io = s3LedgerObjects(store, opts.keyRoot, { ...(opts.sleep ? { sleep: opts.sleep } : {}), ...(opts.random ? { random: opts.random } : {}) });
  const summary: BackfillSummary = {
    mode: opts.dryRun ? "dry_run" : "execute", users: 0,
    fences: { deletion_rows: 0, reset_rows: 0, created: 0, updated: 0, unchanged: 0, would_create: 0, would_update: 0, pairs_added: 0, resets_added: 0,
      deletions_superseded: 0, deletions_unproven: 0, deletion_order: emptyDeletionOrderProof(), cold_resets: 0, needs_review: 0, unreadable: 0, contended: 0, tenant_invalid: 0, input_invalid: 0, failed: 0, resets_in_flight: 0 },
    ownership: { rows: 0, registered: 0, would_register: 0, unchanged: 0, skipped_deleted: 0, skipped_no_fence: 0, invalid: 0, failed: 0, failed_codes: {} },
    revisions: { rows: 0, seed_rows: 0, slot_rows: 0, slots_empty: 0, merged: 0, would_merge: 0, skipped_deleted: 0, skipped_no_fence: 0, invalid: 0, shards: 0, failed: 0, failed_codes: {} },
    sections: { fences: "not_run", ownership: "not_run", revisions: "not_run" },
    fingerprint: { revisions_count: 0, revisions_max_updated_ms: 0, revisions_hash: 0, ownership_count: 0, ownership_hash: 0, slots_owned_count: 0, slots_revision_sum: 0, slots_hash: 0 },
    ...(since !== undefined ? { revisions_since_ms: since } : {}),
  };
  const { sql } = opts;

  /** One short, verified read-only transaction per page (finding 8): every query in it reads ONE snapshot. */
  // One transaction at a time on the single connection: concurrent fenceFor reads (mapLimit) must never interleave
  // their BEGIN/ROLLBACK, or a peer's ROLLBACK would leave this one's reads in autocommit, unbounded, mixed snapshots.
  let txTail: Promise<unknown> = Promise.resolve();
  function readOnlyAll(queries: [string, unknown[]][], heapOnly = false): Promise<Record<string, unknown>[][]> {
    const run = txTail.then(() => readOnlyTx(queries, heapOnly));
    txTail = run.catch(() => {});
    return run;
  }
  async function readOnlyTx(queries: [string, unknown[]][], heapOnly: boolean): Promise<Record<string, unknown>[][]> {
    await sql.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
    try {
      const ro = (await sql.query("SHOW transaction_read_only")).rows[0]?.transaction_read_only;
      if (ro !== "on") throw new Error("backfill_not_read_only");
      await sql.query(`SET LOCAL statement_timeout = ${statementTimeoutMs}`);
      // A scoped run plans each statement for its actual org list (belt and braces with scopedSql).
      if (scopedRun) await sql.query("SET LOCAL plan_cache_mode = force_custom_plan");
      // The block window must read the heap in order: never the org index once per window.
      if (heapOnly) { await sql.query("SET LOCAL enable_indexscan = off"); await sql.query("SET LOCAL enable_bitmapscan = off"); }
      const out: Record<string, unknown>[][] = [];
      for (const [query, params] of queries) out.push((await sql.query(scopedSql(query, scopedRun), params)).rows);
      return out;
    } finally {
      await sql.query("ROLLBACK").catch(() => {});
    }
  }
  const readOnly = async (query: string, params: unknown[]) => (await readOnlyAll([[query, params]]))[0]!;
  /**
   * One org's slots via SLOTS_ORG_CURSOR in one read-only transaction, read to the end (paced when slotPaceMs > 0), with
   * the org's slot fingerprint. `rows` is null when over SLOTS_ORG_READ_MAX_ROWS (rows dropped, the cursor still read to
   * the end for the fingerprint). A paced read that would outlive SLOTS_ORG_PACED_READ_MAX_MS throws `slot_read_too_long`.
   */
  function readOnlySlotsOfOrg(org: string): Promise<{ rows: Record<string, unknown>[] | null; fingerprint: SlotFingerprint }> {
    const fetch = slotsOrgFetch(slotFetchRows);
    const run = txTail.then(async () => {
      const started = now();
      const tooLong = (more: number) => {
        if (slotPaceMs > 0 && now() - started + more > SLOTS_ORG_PACED_READ_MAX_MS) throw Object.assign(new Error("slot_read_too_long"), { code: "slot_read_too_long" });
      };
      await sql.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
      try {
        const ro = (await sql.query("SHOW transaction_read_only")).rows[0]?.transaction_read_only;
        if (ro !== "on") throw new Error("backfill_not_read_only");
        await sql.query(`SET LOCAL statement_timeout = ${statementTimeoutMs}`);
        await sql.query(SLOTS_ORG_CURSOR, [org]);
        let out: Record<string, unknown>[] | null = [];
        const fingerprint = new SlotFingerprint();
        for (let first = true; ; first = false) {
          if (!first && slotPaceMs > 0) { tooLong(slotPaceMs); await sleep(slotPaceMs); }
          const rows = (await sql.query(fetch)).rows;
          tooLong(0);
          if (rows.length === 0) return { rows: out, fingerprint };
          for (const r of rows) {
            fingerprint.add(r);
            delete r.fp_hash;
            out?.push(r);
          }
          if (out !== null && out.length > slotOrgMax) out = null;
        }
      } finally {
        await sql.query("ROLLBACK").catch(() => {});
      }
    });
    txTail = run.catch(() => {});
    return run;
  }

  /** A failure's code for the receipt: the error's own short code, lowercased; never its message. */
  const failureCode = (error: unknown): string => {
    const raw = (error as { code?: unknown })?.code;
    const code = typeof raw === "string" ? raw.toLowerCase().replace(/[^a-z0-9_]/g, "_").slice(0, 40) : "";
    return /^[a-z]/.test(code) ? code : "error";
  };
  const failOwner = (code: string) => {
    summary.ownership.failed++;
    summary.ownership.failed_codes[code] = (summary.ownership.failed_codes[code] ?? 0) + 1;
  };
  /**
   * A store operation retried with backoff on a retryable store failure (RETRYABLE_STORE_CODES), up to STORE_ATTEMPTS:
   * one transient request in tens of thousands must not leave a section incomplete (the EXECUTE receipt is what binds
   * the chain). Only reads and the ledger's idempotent read-then-CAS operations go through it
   * (editFence, registerOwner, mergeIndex re-read and compare-and-swap, so a retry after a write that landed but
   * answered with an error converges; it may count as `unchanged` instead of `created`). Anything else (an invalid
   * document, a refused key, a contended CAS) is not retried.
   */
  const storeRetry = async <R>(read: () => Promise<R>): Promise<R> => {
    for (let attempt = 0; ; attempt++) {
      try {
        return await read();
      } catch (error) {
        const code = (error as { code?: unknown })?.code;
        if (attempt + 1 >= STORE_ATTEMPTS || !RETRYABLE_STORE_CODES.has(code as string)) throw error;
        await io.backoff(attempt);
      }
    }
  };

  /**
   * SQL's controls for a page of users: deletions and resets in ONE read-only snapshot (third review), so a reset is
   * never seen without the tombstones it wrote (or the reverse).
   */
  async function controlsFor(users: [string, string, string][]): Promise<Map<string, UserControls>> {
    const cols = [users.map((p) => p[0]), users.map((p) => p[1]), users.map((p) => p[2])];
    const states = new Map<string, UserControls>(users.map(([o, w, u]) => [userKey(o, w, u), { tenant: { orgId: o, workspaceId: w, userId: u }, deletions: new Map(), watermark: null, resets: [], invalid: false, needsReview: false }]));
    const [deletionRows, resetRows] = await readOnlyAll([[DELETIONS_FOR, cols], [RESETS_FOR, cols]]);
    for (const r of deletionRows!) {
      const st = states.get(userKey(r.org_id, r.workspace_id, r.user_id))!;
      summary.fences.deletion_rows++;
      if (!id(r.origin_id) || !id(r.source) || !id(r.session_id)) { st.invalid = true; continue; }
      st.watermark ??= watermarkOf(r);
      st.deletions.set(r.origin_id, [...(st.deletions.get(r.origin_id) ?? []), [r.source, r.session_id]]);
    }
    for (const r of resetRows!) {
      const st = states.get(userKey(r.org_id, r.workspace_id, r.user_id))!;
      summary.fences.reset_rows++;
      const atMs = int(r.at_ms), result = int(r.deleted);
      if (!id(r.request_id) || atMs === undefined || result === undefined) { st.invalid = true; continue; }
      const cold = r.cold === true;
      if (cold && r.batch_before_import === true) st.needsReview = true;
      st.resets.push({ requestId: r.request_id, atMs: cold ? COLD_RESET_AT_MS : atMs, result, cold });
    }
    return states;
  }

  /** The user's fence as the later sections need it: S3's, or (dry run, not yet written) SQL's view on a new fence. */
  async function fenceFor(tenant: LedgerTenant, cache: Map<string, Promise<FenceDoc | null>>): Promise<FenceDoc | null> {
    const uk = userKey(tenant.orgId, tenant.workspaceId, tenant.userId);
    if (!cache.has(uk)) cache.set(uk, (async () => {
      try {
        const read = await storeRetry(() => io.readFence(io.keysFor(tenant).fence));
        if (read.state === "present") return read.fence;
        if (!opts.dryRun) return null;
        const st = (await controlsFor([[tenant.orgId, tenant.workspaceId, tenant.userId]])).get(uk)!;
        if (st.invalid || st.needsReview) return null;
        const f = newFence();
        applySqlControls(f, st);
        return f;
      } catch {
        return null;
      }
    })());
    return cache.get(uk)!;
  }

  // ---- input fingerprint: before any section, one snapshot, every value a safe integer or the run refuses. On the
  // cursor path (slotsFromCursor) this holds for the revisions and ownership values only; the three slots values are
  // summed in the slot pass, and the running per-org total is checked the same way right after each org's cursor read,
  // so an invalid one refuses (backfill_fingerprint_invalid) before that org's slot rows reach S3 (the fences and
  // ownership sections, and earlier orgs' slots, have run by then). ----
  {
    const [revs, owns, slots] = await readOnlyAll([[FINGERPRINT_REVISIONS, [orgs]], [FINGERPRINT_OWNERSHIP, [orgs]],
      // From the slot pass instead (slotsFromCursor): placeholders here, replaced after it.
      ...(slotsFromCursor ? [] : [[FINGERPRINT_SLOTS, [orgs]] as [string, unknown[]]])]);
    const values = [int(revs![0]?.n), int(revs![0]?.max_ms), int(revs![0]?.h), int(owns![0]?.n), int(owns![0]?.h), ...(slotsFromCursor ? [0, 0, 0] : [int(slots![0]?.n), int(slots![0]?.rev_sum), int(slots![0]?.h)])];
    if (values.some((v) => v === undefined)) throw new Error("backfill_fingerprint_invalid");
    const [revisionsCount, maxMs, revisionsHash, ownershipCount, ownershipHash, slotsCount, revisionSum, slotsHash] = values as number[];
    summary.fingerprint = { revisions_count: revisionsCount!, revisions_max_updated_ms: maxMs!, revisions_hash: revisionsHash!, ownership_count: ownershipCount!,
      ownership_hash: ownershipHash!, slots_owned_count: slotsCount!, slots_revision_sum: revisionSum!, slots_hash: slotsHash! };
  }

  // ---- fences ----
  if (sections.has("fences")) {
    const seen = new Map<string, [string, string, string]>();
    const collect = (rows: Record<string, unknown>[]) => {
      for (const r of rows) { const u: [string, string, string] = [text(r.org_id), text(r.workspace_id), text(r.user_id)]; seen.set(userKey(...u), u); }
    };
    for (const src of USER_SOURCES) {
      if (orgs) for (const org of orgs) collect(await readOnly(usersSkipScan(src, true), [org]));
      else collect(await readOnly(usersSkipScan(src, false), []));
    }
    collect(await readOnly(BATCH_USERS, [orgs]));
    const cmp = (a: string, b: string) => (a < b ? -1 : a > b ? 1 : 0);
    const users = [...seen.values()].sort((a, b) => cmp(a[0], b[0]) || cmp(a[1], b[1]) || cmp(a[2], b[2]));
    for (let at = 0; at < users.length; at += pageSize) {
      const page = users.slice(at, at + pageSize);
      summary.users += page.length;
      const states = await controlsFor(page);
      await mapLimit([...states.values()], concurrency, async (st) => {
        let key: string;
        try {
          key = io.keysFor(st.tenant).fence;
        } catch {
          summary.fences.tenant_invalid++;
          return;
        }
        if (st.invalid) { summary.fences.input_invalid++; return; }
        // A cold-imported reset that cannot be placed before every batch: no fence, the drain refuses (finding 8).
        if (st.needsReview) { summary.fences.needs_review++; return; }
        const cold = st.resets.filter((r) => r.cold).length;
        try {
          if (opts.dryRun) {
            const read = await storeRetry(() => io.readFence(key));
            const f = read.state === "present" ? structuredClone(read.fence) : newFence();
            const t = applySqlControls(f, st);
            summary.fences.pairs_added += t.pairs;
            summary.fences.resets_added += t.resets;
            summary.fences.deletions_superseded += t.superseded;
            summary.fences.deletions_unproven += t.unproven;
            for (const code of DELETION_ORDER_PROOF_CODES) summary.fences.deletion_order[code] += t.deletionOrder[code];
            summary.fences.cold_resets += cold;
            if (f.resetsCompleted < f.resets) summary.fences.resets_in_flight++;
            if (read.state === "absent") summary.fences.would_create++;
            else if (t.changed) summary.fences.would_update++;
            else summary.fences.unchanged++;
            return;
          }
          type Landed = { outcome: "created" | "updated" | "unchanged"; t: ReturnType<typeof applySqlControls>; inFlight: boolean };
          // editFence may run the edit more than once (CAS retries): only the landed attempt's result counts.
          const landed = await storeRetry(() => io.editFence<Landed>(key, (f) => {
            const absent = f.version === 0; // editFence starts an absent fence from newFence() (version 0)
            const t = applySqlControls(f, st);
            const inFlight = f.resetsCompleted < f.resets;
            if (!t.changed && !absent) return { next: null, result: { outcome: "unchanged", t, inFlight } };
            return { next: f, result: { outcome: absent ? "created" : "updated", t, inFlight } };
          }));
          summary.fences[landed.outcome]++;
          summary.fences.pairs_added += landed.t.pairs;
          summary.fences.resets_added += landed.t.resets;
          summary.fences.deletions_superseded += landed.t.superseded;
          summary.fences.deletions_unproven += landed.t.unproven;
          for (const code of DELETION_ORDER_PROOF_CODES) summary.fences.deletion_order[code] += landed.t.deletionOrder[code];
          summary.fences.cold_resets += cold;
          if (landed.inFlight) summary.fences.resets_in_flight++;
        } catch (error) {
          const code = (error as { code?: unknown })?.code;
          if (code === "ledger_fence_invalid") summary.fences.unreadable++;
          else if (code === "ledger_fence_contended") summary.fences.contended++;
          else summary.fences.failed++;
        }
      });
    }
    const f = summary.fences;
    summary.sections.fences = f.unreadable + f.contended + f.tenant_invalid + f.input_invalid + f.failed + f.resets_in_flight + f.needs_review === 0 ? "complete" : "incomplete";
  }

  // ---- ownership ----
  /** Register one session's owner stub (the reset's scope): from the ownership page or an owned lazy slot. */
  const ownRow = async (r: Record<string, unknown>, fences: Map<string, Promise<FenceDoc | null>>) => {
    summary.ownership.rows++;
    if (!id(r.org_id) || !id(r.user_id) || typeof r.workspace_id !== "string" || !id(r.origin_id) || !id(r.source) || !id(r.session_id)) { summary.ownership.invalid++; return; }
    const tenant = { orgId: r.org_id, workspaceId: r.workspace_id, userId: r.user_id };
    const fence = await fenceFor(tenant, fences);
    if (!fence) { summary.ownership.skipped_no_fence++; return; }
    if (isDeleted(fence, r.origin_id, r.source, r.session_id)) { summary.ownership.skipped_deleted++; return; }
    const [, epoch, resets] = recordGeneration(fence, r.origin_id, r.source, r.session_id);
    let key: string, machineId: string;
    try {
      key = io.keysFor(tenant).owner(r.origin_id, r.source, r.session_id);
      machineId = liveMachineId(r.workspace_id, r.origin_id, r.user_id);
    } catch {
      failOwner("key_invalid");
      return;
    }
    try {
      const stored = (await storeRetry(() => io.readOwner(key)))?.doc;
      if (stored && stored.epoch >= epoch && stored.machineId !== undefined) { summary.ownership.unchanged++; return; }
      if (opts.dryRun) { summary.ownership.would_register++; return; }
      // The writer machine, as admission stamps it: the re-arm sweeper binds markers to it.
      const stub: OwnerDoc = { v: 1, originId: r.origin_id, source: r.source, sessionId: r.session_id, epoch, resets, machineId };
      await storeRetry(() => io.registerOwner(key, stub));
      summary.ownership.registered++;
    } catch (error) {
      failOwner(failureCode(error));
    }
  };
  /**
   * Every session already visited this run (an ownership page or an owned slot), so each is counted and written once:
   * a session can be in ingest_session_ownership or a revision AND be an owned slot, and a session has many slots.
   * One short key per session of the scope, held for the run.
   */
  const ownerKey = (r: Record<string, unknown>) => JSON.stringify([r.org_id, r.workspace_id, r.user_id, r.origin_id, r.source, r.session_id]);
  const slotOwners = new Set<string>();
  const registerSlotOwners = async (rows: Record<string, unknown>[]) => {
    const fresh: Record<string, unknown>[] = [];
    for (const r of rows) {
      if (r.current_owner === null || r.current_source === null || r.current_session === null) continue;
      const row = { org_id: r.org_id, workspace_id: r.workspace_id, user_id: r.current_owner, origin_id: r.origin_id, source: r.current_source, session_id: r.current_session };
      const k = ownerKey(row);
      if (slotOwners.has(k)) continue;
      slotOwners.add(k);
      fresh.push(row);
    }
    const fences = new Map<string, Promise<FenceDoc | null>>();
    await mapLimit(fresh, concurrency, (r) => ownRow(r, fences));
  };
  /** The caller's stop (opts.halt), at a section boundary. */
  const stopIfHalted = () => {
    const halted = opts.halt?.();
    if (halted !== undefined) throw halted;
  };
  if (sections.has("ownership")) {
    let cursor = ["", "", "", "", "", ""];
    for (;;) {
      stopIfHalted();
      const rows = await readOnly(OWNERSHIP_PAGE, [orgs, ...cursor, rowPageSize]);
      if (!rows.length) break;
      const last = rows[rows.length - 1]!;
      cursor = [text(last.org_id), text(last.workspace_id), text(last.user_id), text(last.origin_id), text(last.source), text(last.session_id)];
      const fences = new Map<string, Promise<FenceDoc | null>>();
      for (const r of rows) slotOwners.add(ownerKey(r));
      await mapLimit(rows, concurrency, (r) => ownRow(r, fences));
    }
  }

  // ---- revisions, and the slot pass shared with ownership ----
  if (sections.has("revisions") || sections.has("ownership")) {
    type Group = { key: string; tenant: LedgerTenant; originId: string; batches: IndexEntryInput[][]; batch: number; entries: number };
    /**
     * One pass's buffer: shard groups gathered across pages and windows, merged together when `entries` reaches the
     * budget and at the end of each source. Each page or window stays its own batch inside a group
     * (mergeIndexBatches), so a record seen in several windows (a record has one slot per source identity and control
     * epoch) is merged exactly as one mergeIndex call per window would, with one shard read and write.
     */
    type Buffer = { groups: Map<string, Group>; entries: number; batch: number };
    const newBuffer = (): Buffer => ({ groups: new Map(), entries: 0, batch: 0 });
    const add = async (buf: Buffer, fences: Map<string, Promise<FenceDoc | null>>, row: { org: unknown; ws: unknown; user: unknown; origin: unknown; record: unknown; revision: unknown; revisionId: unknown; digest: unknown; source: unknown; session: unknown }) => {
      if (!id(row.org) || !id(row.user) || typeof row.ws !== "string") { summary.revisions.invalid++; return; }
      const fence = await fenceFor({ orgId: row.org, workspaceId: row.ws, userId: row.user }, fences);
      if (!fence) { summary.revisions.skipped_no_fence++; return; }
      const revision = int(row.revision);
      if (!id(row.origin) || !id(row.record) || !id(row.source) || revision === undefined || !(row.session === null || id(row.session)) ||
          !(row.revisionId === null || typeof row.revisionId === "string") || !(row.digest === null || typeof row.digest === "string")) { summary.revisions.invalid++; return; }
      const sessionId = row.session as string | null;
      if (sessionId !== null && isDeleted(fence, row.origin, row.source, sessionId)) { summary.revisions.skipped_deleted++; return; }
      const [sessionKey, epoch, resets] = recordGeneration(fence, row.origin, row.source, sessionId);
      const entry: IndexEntryInput = { recordId: row.record, revision, revisionId: row.revisionId as string | null, digest: row.digest as string | null, sessionKey, epoch, resets };
      let key: string;
      try {
        checkIndexEntryInput(entry);
        key = io.keysFor({ orgId: row.org, workspaceId: row.ws, userId: row.user }).revisions(row.origin, row.record);
      } catch {
        summary.revisions.invalid++;
        return;
      }
      const tenant = { orgId: row.org, workspaceId: row.ws, userId: row.user };
      const g = buf.groups.get(key) ?? { key, tenant, originId: row.origin, batches: [], batch: -1, entries: 0 };
      if (g.batch !== buf.batch) { g.batches.push([]); g.batch = buf.batch; }
      g.batches[g.batches.length - 1]!.push(entry);
      g.entries++;
      buf.groups.set(key, g);
      if (++buf.entries >= mergeBuffer) await flush(buf);
    };
    /**
     * Merge a buffer and empty it. A buffered group may have waited across windows, so each tenant's fence is read
     * again here (once per pass) and the merge runs under that one: mergeIndex then drops what a control written since
     * has forgotten. A fence that cannot be read now fails its groups (incomplete), never merges under the stale one.
     */
    async function flush(buf: Buffer): Promise<void> {
      const groups = [...buf.groups.values()];
      buf.groups = new Map();
      buf.entries = 0;
      const fresh = new Map<string, Promise<FenceDoc | null>>();
      await mapLimit(groups, concurrency, async (g) => {
      if (opts.dryRun) { summary.revisions.would_merge += g.entries; return; }
      const fence = await fenceFor(g.tenant, fresh);
      if (!fence) {
        summary.revisions.failed += g.entries;
        summary.revisions.failed_codes.fence_unreadable = (summary.revisions.failed_codes.fence_unreadable ?? 0) + g.entries;
        return;
      }
      try {
        await storeRetry(() => io.mergeIndexBatches(g.key, fence, g.originId, g.batches));
        summary.revisions.merged += g.entries;
        summary.revisions.shards++;
      } catch (error) {
        const code = failureCode(error);
        summary.revisions.failed += g.entries;
        summary.revisions.failed_codes[code] = (summary.revisions.failed_codes[code] ?? 0) + g.entries;
      }
      });
      stopIfHalted();
    }
    // (a) ingest_record_revisions, seeded revision-only rows included; a catch-up only those updated since `since`.
    let cursor = ["", "", "", ""];
    const revisionBuf = newBuffer();
    for (;;) {
      if (!sections.has("revisions")) break;
      stopIfHalted();
      const rows = since === undefined
        ? await readOnly(REVISIONS_PAGE, [orgs, ...cursor, rowPageSize])
        : await readOnly(REVISIONS_SINCE_PAGE, [orgs, ...cursor, rowPageSize, since]);
      if (!rows.length) break;
      const last = rows[rows.length - 1]!;
      cursor = [text(last.org_id), text(last.workspace_id), text(last.origin_id), text(last.record_id)];
      const fences = new Map<string, Promise<FenceDoc | null>>();
      revisionBuf.batch++;
      for (const r of rows) {
        // A catch-up page's last row is returned for the cursor even when it was not updated since.
        if (since !== undefined && r.fresh !== true) continue;
        summary.revisions.rows++;
        if (r.revision_id === null && r.digest === null) summary.revisions.seed_rows++;
        await add(revisionBuf, fences, { org: r.org_id, ws: r.workspace_id, user: r.user_id, origin: r.origin_id, record: r.record_id, revision: r.revision, revisionId: r.revision_id, digest: r.digest, source: r.source, session: r.session_id });
      }
    }
    stopIfHalted();
    await flush(revisionBuf);
    // (b) owned ingest_lazy_native_slots, under their owner and session (ingest_lazy_clear's scope): first their
    // sessions' owner stubs (the ownership section), then their revisions.
    const slotBuf = newBuffer();
    const slotRows = async (rows: Record<string, unknown>[]) => {
      stopIfHalted();
      if (sections.has("ownership")) await registerSlotOwners(rows);
      stopIfHalted();
      if (!sections.has("revisions")) return;
      const fences = new Map<string, Promise<FenceDoc | null>>();
      slotBuf.batch++;
      for (const r of rows) {
        summary.revisions.slot_rows++;
        if (r.current_owner === null) { summary.revisions.slots_empty++; continue; }
        await add(slotBuf, fences, { org: r.org_id, ws: r.workspace_id, user: r.current_owner, origin: r.origin_id, record: r.record_id, revision: r.current_revision, revisionId: null, digest: r.current_digest, source: r.current_source, session: r.current_session });
      }
    };
    // A scoped run reads each org's slots once through the org index (SLOTS_ORG_CURSOR): seconds for an org with a few
    // hundred thousand slots, where the block walk reads the whole multi-GB heap. An org above SLOTS_ORG_READ_MAX_ROWS,
    // or the all-orgs scope, uses the block walk below.
    let walkOrgs: string[] | null = orgs ?? null;
    const slotsFingerprint = new SlotFingerprint();
    if (opts.slotsByOrg === true && orgs && orgs.length > 0) {
      walkOrgs = [];
      for (const org of orgs) {
        stopIfHalted();
        let read: Awaited<ReturnType<typeof readOnlySlotsOfOrg>>;
        try {
          read = await readOnlySlotsOfOrg(org);
        } catch (error) {
          if ((error as { code?: unknown })?.code !== "slot_read_too_long") throw error;
          // Fail closed: the org's slots are neither processed nor walked, and each run section is incomplete.
          if (sections.has("revisions")) { summary.revisions.failed++; summary.revisions.failed_codes.slot_read_too_long = (summary.revisions.failed_codes.slot_read_too_long ?? 0) + 1; }
          if (sections.has("ownership")) failOwner("slot_read_too_long");
          continue;
        }
        slotsFingerprint.merge(read.fingerprint);
        // The running total checked as the run-start values are (int(): a non-negative safe integer), before this
        // org's rows are handed to slotRows (any S3 write).
        if (slotsFromCursor) checkedSlotsFingerprint(slotsFingerprint);
        let rows = read.rows;
        if (rows === null) { walkOrgs.push(org); continue; }
        // Slices are handed on and dropped as they go, so the org's rows are freed while the buffers fill.
        while (rows.length > 0) await slotRows(rows.splice(0, 5000));
        rows = null;
      }
    }
    // Block windows (SLOTS_WINDOW): a TID range reads the heap in order, where keyset paging on key_sha256 cost about
    // 21 s a page on production's tens of millions of slots. The heap's size is re-read in each window's snapshot so
    // growth is followed.
    for (let block = 0; walkOrgs === null || walkOrgs.length > 0; block += slotWindow) {
      stopIfHalted();
      const [size, rows] = await readOnlyAll([[SLOTS_BLOCKS, []], [SLOTS_WINDOW, [walkOrgs, `(${block},0)`, `(${block + slotWindow},0)`]]], true);
      const blocks = int(size![0]?.blocks);
      if (blocks === undefined) throw new Error("backfill_slot_blocks_invalid");
      await slotRows(rows!);
      if (block + slotWindow >= blocks) break;
    }
    stopIfHalted();
    await flush(slotBuf);
    if (slotsFromCursor) {
      // FINGERPRINT_SLOTS's values from the pass (an org that failed contributes nothing, but its sections are incomplete).
      [summary.fingerprint.slots_owned_count, summary.fingerprint.slots_revision_sum, summary.fingerprint.slots_hash] = checkedSlotsFingerprint(slotsFingerprint);
    }
    if (sections.has("revisions")) {
      const v = summary.revisions;
      summary.sections.revisions = v.skipped_no_fence + v.invalid + v.failed === 0 ? "complete" : "incomplete";
    }
    if (sections.has("ownership")) {
      const o = summary.ownership;
      summary.sections.ownership = o.skipped_no_fence + o.invalid + o.failed === 0 ? "complete" : "incomplete";
    }
  }
  return summary;
}
