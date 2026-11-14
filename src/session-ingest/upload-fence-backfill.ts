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
 * SQL (finding 8): no long snapshot and nothing held whole in memory. Every page is its own short `BEGIN ISOLATION
 * LEVEL REPEATABLE READ READ ONLY` transaction, checked with `SHOW transaction_read_only`, always ended by ROLLBACK;
 * SELECTs only, keyset-paged in a stable order, optionally scoped to orgs. Fences are read from S3 per page.
 *
 * Fail closed: an unreadable fence is never rewritten; a user with any unrepresentable row gets no fence. Each
 * section reports `complete` only when nothing was skipped for want of a fence, needed review, was invalid or failed.
 * Nothing here logs; the summary holds counts only.
 */
import type { ObjectStore } from "../shared/vendor.ts";
import {
  applyFenceChange, checkIndexEntryInput, fenceEpoch, isDeleted, newFence, recordGeneration, s3LedgerObjects, sessionKeyHash,
  type FenceChange, type FenceDoc, type IndexEntryInput,
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
  /** Users per fence page (default 500); rows per ownership/revision page (default 5000). */
  pageSize?: number;
  rowPageSize?: number;
  concurrency?: number;
  statementTimeoutMs?: number;
  /** Heap blocks per scoped slot window (default SLOT_WINDOW_BLOCKS); tests use a small one to cross windows. */
  slotWindowBlocks?: number;
  sleep?: (ms: number) => Promise<void>;
  random?: () => number;
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
    /** Cold-imported resets recorded at COLD_RESET_AT_MS. */
    cold_resets: number;
    /** Users given no fence because a cold-imported reset could not be placed before all their batches. */
    needs_review: number;
    unreadable: number; contended: number; tenant_invalid: number; input_invalid: number; failed: number;
    resets_in_flight: number;
  };
  ownership: { rows: number; registered: number; would_register: number; unchanged: number; skipped_deleted: number; skipped_no_fence: number; invalid: number; failed: number };
  revisions: {
    rows: number; seed_rows: number;
    slot_rows: number; slots_empty: number;
    merged: number; would_merge: number; skipped_deleted: number; skipped_no_fence: number; invalid: number; shards: number; failed: number;
  };
  sections: Record<BackfillSection, "complete" | "incomplete" | "not_run">;
}

const SCOPE = `($1::text[] IS NULL OR org_id = ANY($1::text[]))`;
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
const SLOTS_PAGE = `
SELECT key_sha256, org_id, workspace_id, origin_id, record_id, current_owner, current_source, current_session, current_revision, current_digest
FROM sessions.ingest_lazy_native_slots
WHERE ${SCOPE} AND key_sha256 > $2::text
ORDER BY key_sha256
LIMIT $3::int`;
/**
 * The scoped slots read: one window of heap blocks (a TID range scan), filtered to the scope's orgs. Keyset paging on
 * key_sha256 with an org filter walks the primary key and fetches every slot's heap tuple at random to test its org
 * (about 21 s a page on production's tens of millions of slots, scoped or not); a block window reads the heap in
 * order instead, so a whole pass is one sequential read. A slot rewritten during the pass can move to a block already
 * read and be missed, so a scoped run is exact only while its orgs admit nothing: run the execute after the scope's
 * HTTP admission stop (decision 0005's cutover order). The unscoped pass keeps the key_sha256 keyset.
 */
const SLOT_WINDOW_BLOCKS = 4096;
const SLOTS_BLOCKS = `SELECT (pg_relation_size('sessions.ingest_lazy_native_slots') / current_setting('block_size')::bigint)::bigint AS blocks`;
const SLOTS_WINDOW = `
SELECT key_sha256, org_id, workspace_id, origin_id, record_id, current_owner, current_source, current_session, current_revision, current_digest
FROM sessions.ingest_lazy_native_slots
WHERE org_id = ANY($1::text[]) AND ctid >= $2::tid AND ctid < $3::tid`;
const REVISIONS_PAGE = `
SELECT org_id, workspace_id, origin_id, record_id, revision, revision_id, digest, user_id, source, session_id
FROM sessions.ingest_record_revisions
WHERE ${SCOPE} AND (org_id, workspace_id, origin_id, record_id) > ($2::text, $3::text, $4::text, $5::text)
ORDER BY org_id, workspace_id, origin_id, record_id
LIMIT $6::int`;

const userKey = (o: unknown, w: unknown, u: unknown) => JSON.stringify([o, w, u]);
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
 * SQL's controls applied to `f` with ledger-s3's rules (in place). A deletion is skipped only when S3's ordered control
 * for the session is provably newer than it (provablyNewer, against the deletion snapshot's watermark), and never while
 * the user has a SQL reset S3 has not completed for its request id. A deletion applied although S3 had lifted or
 * ordered the session is counted `unproven`.
 */
export function applySqlControls(f: FenceDoc, st: Pick<UserControls, "deletions" | "resets"> & { watermark?: SqlControlWatermark | null }): { changed: boolean; pairs: number; resets: number; superseded: number; unproven: number } {
  let changed = false, pairs = 0, resets = 0, superseded = 0, unproven = 0;
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
      if (at !== undefined || lifted) unproven++;
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
  return { changed, pairs, resets, superseded, unproven };
}

/** Paged, short-transaction, read-only S3 ledger backfill. Counts only. */
export async function backfillUploadFences(opts: BackfillOptions): Promise<BackfillSummary> {
  const pageSize = opts.pageSize ?? 500;
  const rowPageSize = opts.rowPageSize ?? 5_000;
  const concurrency = opts.concurrency ?? 4;
  const statementTimeoutMs = opts.statementTimeoutMs ?? 30_000;
  const slotWindow = opts.slotWindowBlocks ?? SLOT_WINDOW_BLOCKS;
  if (!Number.isInteger(slotWindow) || slotWindow < 1 || slotWindow > 1_048_576) throw new Error("backfill_slot_window_invalid");
  const sections = new Set(opts.sections ?? BACKFILL_SECTIONS);
  if (!Number.isInteger(pageSize) || pageSize < 1 || pageSize > 5_000) throw new Error("backfill_page_size_invalid");
  if (!Number.isInteger(rowPageSize) || rowPageSize < 1 || rowPageSize > 50_000) throw new Error("backfill_row_page_size_invalid");
  if (!Number.isInteger(concurrency) || concurrency < 1 || concurrency > MAX_BACKFILL_CONCURRENCY) throw new Error("backfill_concurrency_invalid");
  if (!Number.isInteger(statementTimeoutMs) || statementTimeoutMs < 1_000 || statementTimeoutMs > 600_000) throw new Error("backfill_statement_timeout_invalid");
  if (!/^[a-z0-9][a-z0-9-]{0,62}$/.test(opts.keyRoot)) throw new Error("backfill_key_root_invalid");
  if (opts.orgs) for (const org of opts.orgs) if (!ORG.test(org)) throw new Error("backfill_org_invalid");
  if (![...sections].every((s) => BACKFILL_SECTIONS.includes(s)) || sections.size === 0) throw new Error("backfill_sections_invalid");
  const orgs = opts.orgs && opts.orgs.length ? [...new Set(opts.orgs)] : null;
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
      deletions_superseded: 0, deletions_unproven: 0, cold_resets: 0, needs_review: 0, unreadable: 0, contended: 0, tenant_invalid: 0, input_invalid: 0, failed: 0, resets_in_flight: 0 },
    ownership: { rows: 0, registered: 0, would_register: 0, unchanged: 0, skipped_deleted: 0, skipped_no_fence: 0, invalid: 0, failed: 0 },
    revisions: { rows: 0, seed_rows: 0, slot_rows: 0, slots_empty: 0, merged: 0, would_merge: 0, skipped_deleted: 0, skipped_no_fence: 0, invalid: 0, shards: 0, failed: 0 },
    sections: { fences: "not_run", ownership: "not_run", revisions: "not_run" },
  };
  const { sql } = opts;

  /** One short, verified read-only transaction per page (finding 8): every query in it reads ONE snapshot. */
  async function readOnlyAll(queries: [string, unknown[]][], heapOnly = false): Promise<Record<string, unknown>[][]> {
    await sql.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
    try {
      const ro = (await sql.query("SHOW transaction_read_only")).rows[0]?.transaction_read_only;
      if (ro !== "on") throw new Error("backfill_not_read_only");
      await sql.query(`SET LOCAL statement_timeout = ${statementTimeoutMs}`);
      // The block window must read the heap in order: never the org index once per window.
      if (heapOnly) { await sql.query("SET LOCAL enable_indexscan = off"); await sql.query("SET LOCAL enable_bitmapscan = off"); }
      const out: Record<string, unknown>[][] = [];
      for (const [query, params] of queries) out.push((await sql.query(query, params)).rows);
      return out;
    } finally {
      await sql.query("ROLLBACK").catch(() => {});
    }
  }
  const readOnly = async (query: string, params: unknown[]) => (await readOnlyAll([[query, params]]))[0]!;

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
        const read = await io.readFence(io.keysFor(tenant).fence);
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
            const read = await io.readFence(key);
            const f = read.state === "present" ? structuredClone(read.fence) : newFence();
            const t = applySqlControls(f, st);
            summary.fences.pairs_added += t.pairs;
            summary.fences.resets_added += t.resets;
            summary.fences.deletions_superseded += t.superseded;
            summary.fences.deletions_unproven += t.unproven;
            summary.fences.cold_resets += cold;
            if (f.resetsCompleted < f.resets) summary.fences.resets_in_flight++;
            if (read.state === "absent") summary.fences.would_create++;
            else if (t.changed) summary.fences.would_update++;
            else summary.fences.unchanged++;
            return;
          }
          type Landed = { outcome: "created" | "updated" | "unchanged"; t: ReturnType<typeof applySqlControls>; inFlight: boolean };
          // editFence may run the edit more than once (CAS retries): only the landed attempt's result counts.
          const landed = await io.editFence<Landed>(key, (f) => {
            const absent = f.version === 0; // editFence starts an absent fence from newFence() (version 0)
            const t = applySqlControls(f, st);
            const inFlight = f.resetsCompleted < f.resets;
            if (!t.changed && !absent) return { next: null, result: { outcome: "unchanged", t, inFlight } };
            return { next: f, result: { outcome: absent ? "created" : "updated", t, inFlight } };
          });
          summary.fences[landed.outcome]++;
          summary.fences.pairs_added += landed.t.pairs;
          summary.fences.resets_added += landed.t.resets;
          summary.fences.deletions_superseded += landed.t.superseded;
          summary.fences.deletions_unproven += landed.t.unproven;
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
    try {
      const key = io.keysFor(tenant).owner(r.origin_id, r.source, r.session_id);
      const stored = (await io.readOwner(key))?.doc;
      const machineId = liveMachineId(r.workspace_id, r.origin_id, r.user_id);
      if (stored && stored.epoch >= epoch && stored.machineId !== undefined) { summary.ownership.unchanged++; return; }
      if (opts.dryRun) { summary.ownership.would_register++; return; }
      // The writer machine, as admission stamps it: the re-arm sweeper binds markers to it.
      await io.registerOwner(key, { v: 1, originId: r.origin_id, source: r.source, sessionId: r.session_id, epoch, resets, machineId });
      summary.ownership.registered++;
    } catch {
      summary.ownership.failed++;
    }
  };
  /** Sessions of owned lazy slots, each once per run (a session has many slots). */
  const slotOwners = new Set<string>();
  const registerSlotOwners = async (rows: Record<string, unknown>[]) => {
    const fresh: Record<string, unknown>[] = [];
    for (const r of rows) {
      if (r.current_owner === null || r.current_source === null || r.current_session === null) continue;
      const k = JSON.stringify([r.org_id, r.workspace_id, r.current_owner, r.origin_id, r.current_source, r.current_session]);
      if (slotOwners.has(k)) continue;
      slotOwners.add(k);
      fresh.push({ org_id: r.org_id, workspace_id: r.workspace_id, user_id: r.current_owner, origin_id: r.origin_id, source: r.current_source, session_id: r.current_session });
    }
    const fences = new Map<string, Promise<FenceDoc | null>>();
    await mapLimit(fresh, concurrency, (r) => ownRow(r, fences));
  };
  if (sections.has("ownership")) {
    let cursor = ["", "", "", "", "", ""];
    for (;;) {
      const rows = await readOnly(OWNERSHIP_PAGE, [orgs, ...cursor, rowPageSize]);
      if (!rows.length) break;
      const last = rows[rows.length - 1]!;
      cursor = [text(last.org_id), text(last.workspace_id), text(last.user_id), text(last.origin_id), text(last.source), text(last.session_id)];
      const fences = new Map<string, Promise<FenceDoc | null>>();
      await mapLimit(rows, concurrency, (r) => ownRow(r, fences));
    }
  }

  // ---- revisions, and the slot pass shared with ownership ----
  if (sections.has("revisions") || sections.has("ownership")) {
    type Group = { key: string; fence: FenceDoc; originId: string; entries: IndexEntryInput[] };
    const add = async (groups: Map<string, Group>, fences: Map<string, Promise<FenceDoc | null>>, row: { org: unknown; ws: unknown; user: unknown; origin: unknown; record: unknown; revision: unknown; revisionId: unknown; digest: unknown; source: unknown; session: unknown }) => {
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
      const g = groups.get(key) ?? { key, fence, originId: row.origin, entries: [] };
      g.entries.push(entry);
      groups.set(key, g);
    };
    const flush = (groups: Map<string, Group>) => mapLimit([...groups.values()], concurrency, async (g) => {
      if (opts.dryRun) { summary.revisions.would_merge += g.entries.length; return; }
      try {
        await io.mergeIndex(g.key, g.fence, g.originId, g.entries);
        summary.revisions.merged += g.entries.length;
        summary.revisions.shards++;
      } catch {
        summary.revisions.failed += g.entries.length;
      }
    });
    // (a) ingest_record_revisions, seeded revision-only rows included.
    let cursor = ["", "", "", ""];
    for (;;) {
      if (!sections.has("revisions")) break;
      const rows = await readOnly(REVISIONS_PAGE, [orgs, ...cursor, rowPageSize]);
      if (!rows.length) break;
      const last = rows[rows.length - 1]!;
      cursor = [text(last.org_id), text(last.workspace_id), text(last.origin_id), text(last.record_id)];
      const groups = new Map<string, Group>(), fences = new Map<string, Promise<FenceDoc | null>>();
      for (const r of rows) {
        summary.revisions.rows++;
        if (r.revision_id === null && r.digest === null) summary.revisions.seed_rows++;
        await add(groups, fences, { org: r.org_id, ws: r.workspace_id, user: r.user_id, origin: r.origin_id, record: r.record_id, revision: r.revision, revisionId: r.revision_id, digest: r.digest, source: r.source, session: r.session_id });
      }
      await flush(groups);
    }
    // (b) owned ingest_lazy_native_slots, under their owner and session (ingest_lazy_clear's scope): first their
    // sessions' owner stubs (the ownership section), then their revisions.
    const slotRows = async (rows: Record<string, unknown>[]) => {
      if (sections.has("ownership")) await registerSlotOwners(rows);
      if (!sections.has("revisions")) return;
      const groups = new Map<string, Group>(), fences = new Map<string, Promise<FenceDoc | null>>();
      for (const r of rows) {
        summary.revisions.slot_rows++;
        if (r.current_owner === null) { summary.revisions.slots_empty++; continue; }
        await add(groups, fences, { org: r.org_id, ws: r.workspace_id, user: r.current_owner, origin: r.origin_id, record: r.record_id, revision: r.current_revision, revisionId: null, digest: r.current_digest, source: r.current_source, session: r.current_session });
      }
      await flush(groups);
    };
    if (orgs) {
      // Scoped: block windows (SLOTS_WINDOW), the heap's size re-read in each window's snapshot so growth is followed.
      for (let block = 0; ; block += slotWindow) {
        const [size, rows] = await readOnlyAll([[SLOTS_BLOCKS, []], [SLOTS_WINDOW, [orgs, `(${block},0)`, `(${block + slotWindow},0)`]]], true);
        const blocks = int(size![0]?.blocks);
        if (blocks === undefined) throw new Error("backfill_slot_blocks_invalid");
        await slotRows(rows!);
        if (block + slotWindow >= blocks) break;
      }
    } else {
      let slotCursor = "";
      for (;;) {
        const rows = await readOnly(SLOTS_PAGE, [orgs, slotCursor, rowPageSize]);
        if (!rows.length) break;
        slotCursor = text(rows[rows.length - 1]!.key_sha256);
        await slotRows(rows);
      }
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
