/**
 * The one-time S3 ledger backfill (docs/decisions/0005-s3-native-upload-ledger.md, "The buffered backlog",
 * step 0, and #580's fence-authority preconditions). At cutover S3 must know what SQL knows, in #580's
 * ledger-s3 formats and through its exported rules only (no local copy of any format or rule):
 *
 *   fences     every user with any upload state (upload_deletions, upload_resets, ingest_session_ownership,
 *              ingest_record_revisions, ingest_batches) gets `upload-fences.json`: `editFence` (an absent fence
 *              starts from `newFence()`), SQL's deletions and resets applied with `applyFenceChange` (a new
 *              deletion bumps the session's epoch; a new reset request id is recorded with its tombstone
 *              count, increments `resets` and raises `lastResetAtMs` to SQL's `reset_at`, rounded up to the
 *              ms). Each backfilled reset is recorded with its final count, so `resetsCompleted == resets`
 *              (a backfilled reset is complete by definition); a fence that still holds a reset in flight
 *              (the control mirror's) is counted `resets_in_flight` and leaves the section incomplete.
 *              A user with no controls still gets an (empty) fence: the drain refuses an absent one.
 *   ownership  `ingest_session_ownership` (written at claim, so pending and committed claims alike) into the
 *              ownership stubs (`registerOwner`), at the backfilled fence's current generation
 *              (`recordGeneration`), so a later reset lists sessions owned only in SQL.
 *   revisions  SQL's stored revisions into the per-origin revision shards (`mergeIndex`) as current-generation
 *              entries, so #250 stale checks hold at cutover. mergeIndex never lowers a stored revision (an
 *              equal one keeps the stored entry). Two sources, as SQL's claim reads them:
 *                - `ingest_record_revisions`, including the operator seed's revision-only rows
 *                  (`ingest_seed_proven_records`: no revision id or digest), as revision-only entries;
 *                - `ingest_lazy_native_slots` (the buffered/lazy stale source: revision + digest, no revision id),
 *                  under the slot's owner (`current_owner`) and session (`current_source`, `current_session`),
 *                  so S3 forgets them exactly when SQL's `ingest_lazy_clear` clears them (a disable of that
 *                  session, or a reset; a session-less slot never). Slots with no owner (cleared, or an erase
 *                  tombstone) hold no revision and are skipped.
 *              Every entry is validated with ledger-s3 `checkIndexEntryInput`.
 *
 * SQL is read-only: one `BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY` snapshot, checked with
 * `SHOW transaction_read_only`, always ended by ROLLBACK; SELECTs only, keyset-paged, optionally scoped to
 * a set of orgs. Every write is a ledger-s3 CAS or create; rerunning is a no-op for what is already there.
 *
 * Fail closed: a fence that is unreadable is never rewritten, and a user with ANY row the ledger cannot
 * represent (an empty id, a `__proto__` key) gets no fence at all, so the drain keeps refusing them. Rows of
 * a user without a backfilled fence are skipped (`skipped_no_fence`). Each section reports `complete` only
 * when nothing was skipped for those reasons or failed: #580's fence-authority marker records
 * `fenceBackfill`, `ownershipBackfill` and `revisionBackfill` from these.
 *
 * Nothing here logs; the summary holds counts only: never a tenant id, session id, record id or key.
 */
import type { ObjectStore } from "../shared/vendor.ts";
import { applyFenceChange, checkIndexEntryInput, isDeleted, newFence, recordGeneration, s3LedgerObjects, type FenceChange, type FenceDoc, type IndexEntryInput } from "./ledger-s3.ts";
import type { LedgerTenant } from "./ledger.ts";

const ORG = /^(?!\.)[A-Za-z0-9._-]{1,128}$/;
export const MAX_BACKFILL_CONCURRENCY = 16;
export type BackfillSection = "fences" | "ownership" | "revisions";
export const BACKFILL_SECTIONS: readonly BackfillSection[] = ["fences", "ownership", "revisions"];

export interface BackfillSql {
  query(sql: string, params?: unknown[]): Promise<{ rows: Record<string, unknown>[] }>;
}

export interface BackfillOptions {
  sql: BackfillSql;
  store: ObjectStore;
  keyRoot: string;
  dryRun: boolean;
  /** Only these orgs (exact org ids). Default: every org in the tables. */
  orgs?: string[];
  /** Default all three. Ownership and revisions need the fences (backfilled now, or in an earlier run). */
  sections?: BackfillSection[];
  /** Users per fence page (default 500); rows per ownership/revision page (default 5000). */
  pageSize?: number;
  rowPageSize?: number;
  concurrency?: number;
  statementTimeoutMs?: number;
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
    unreadable: number; contended: number; tenant_invalid: number; input_invalid: number; failed: number;
    /** Fences left with a reset begun and not finished (resetsCompleted < resets): a mirrored reset in flight. */
    resets_in_flight: number;
  };
  ownership: { rows: number; registered: number; would_register: number; unchanged: number; skipped_deleted: number; skipped_no_fence: number; invalid: number; failed: number };
  revisions: {
    /** ingest_record_revisions rows, of which `seed_rows` revision-only (the operator seed). */
    rows: number; seed_rows: number;
    /** ingest_lazy_native_slots rows, of which `slots_empty` hold no revision (no owner). */
    slot_rows: number; slots_empty: number;
    merged: number; would_merge: number; skipped_deleted: number; skipped_no_fence: number; invalid: number; shards: number; failed: number;
  };
  /** Per section: complete (nothing skipped for want of a fence, nothing invalid or failed), incomplete, or not run. */
  sections: Record<BackfillSection, "complete" | "incomplete" | "not_run">;
}

const USERS = `
SELECT DISTINCT org_id, workspace_id, user_id FROM (
  SELECT org_id, workspace_id, user_id FROM sessions.upload_deletions
  UNION ALL SELECT org_id, workspace_id, user_id FROM sessions.upload_resets
  UNION ALL SELECT org_id, workspace_id, user_id FROM sessions.ingest_session_ownership
  UNION ALL SELECT org_id, workspace_id, user_id FROM sessions.ingest_record_revisions
  UNION ALL SELECT org_id, workspace_id, user_id FROM sessions.ingest_batches
  UNION ALL SELECT org_id, workspace_id, current_owner FROM sessions.ingest_lazy_native_slots WHERE current_owner IS NOT NULL
) AS u
WHERE ($1::text[] IS NULL OR org_id = ANY($1::text[]))
ORDER BY org_id, workspace_id, user_id`;
const DELETIONS_FOR = `
SELECT d.org_id, d.workspace_id, d.user_id, d.origin_id, d.source, d.session_id
FROM sessions.upload_deletions AS d
JOIN unnest($1::text[], $2::text[], $3::text[]) AS p(o, w, u) ON d.org_id = p.o AND d.workspace_id = p.w AND d.user_id = p.u
ORDER BY d.org_id, d.workspace_id, d.user_id, d.origin_id, d.source, d.session_id`;
const RESETS_FOR = `
SELECT r.org_id, r.workspace_id, r.user_id, r.request_id, r.deleted,
       ceil(extract(epoch FROM r.reset_at) * 1000)::bigint AS at_ms
FROM sessions.upload_resets AS r
JOIN unnest($1::text[], $2::text[], $3::text[]) AS p(o, w, u) ON r.org_id = p.o AND r.workspace_id = p.w AND r.user_id = p.u
ORDER BY r.org_id, r.workspace_id, r.user_id, r.reset_at, r.request_id`;
const OWNERSHIP_PAGE = `
SELECT org_id, workspace_id, user_id, origin_id, source, session_id FROM sessions.ingest_session_ownership
WHERE ($1::text[] IS NULL OR org_id = ANY($1::text[]))
  AND (org_id, workspace_id, user_id, origin_id, source, session_id) > ($2::text, $3::text, $4::text, $5::text, $6::text, $7::text)
ORDER BY org_id, workspace_id, user_id, origin_id, source, session_id
LIMIT $8::int`;
const SLOTS_PAGE = `
SELECT key_sha256, org_id, workspace_id, origin_id, record_id, current_owner, current_source, current_session, current_revision, current_digest
FROM sessions.ingest_lazy_native_slots
WHERE ($1::text[] IS NULL OR org_id = ANY($1::text[])) AND key_sha256 > $2::text
ORDER BY key_sha256
LIMIT $3::int`;
const REVISIONS_PAGE = `
SELECT org_id, workspace_id, origin_id, record_id, revision, revision_id, digest, user_id, source, session_id
FROM sessions.ingest_record_revisions
WHERE ($1::text[] IS NULL OR org_id = ANY($1::text[]))
  AND (org_id, workspace_id, origin_id, record_id) > ($2::text, $3::text, $4::text, $5::text)
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

/** One read-only snapshot; S3 written only through ledger-s3's exports. Counts only. */
export async function backfillUploadFences(opts: BackfillOptions): Promise<BackfillSummary> {
  const pageSize = opts.pageSize ?? 500;
  const rowPageSize = opts.rowPageSize ?? 5_000;
  const concurrency = opts.concurrency ?? 4;
  const statementTimeoutMs = opts.statementTimeoutMs ?? 30_000;
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
    fences: { deletion_rows: 0, reset_rows: 0, created: 0, updated: 0, unchanged: 0, would_create: 0, would_update: 0, pairs_added: 0, resets_added: 0, unreadable: 0, contended: 0, tenant_invalid: 0, input_invalid: 0, failed: 0, resets_in_flight: 0 },
    ownership: { rows: 0, registered: 0, would_register: 0, unchanged: 0, skipped_deleted: 0, skipped_no_fence: 0, invalid: 0, failed: 0 },
    revisions: { rows: 0, merged: 0, would_merge: 0, skipped_deleted: 0, seed_rows: 0, slot_rows: 0, slots_empty: 0, skipped_no_fence: 0, invalid: 0, shards: 0, failed: 0 },
    sections: { fences: "not_run", ownership: "not_run", revisions: "not_run" },
  };
  /** Each user's fence as it stands after the fence section (or as read, when that section is not run). */
  const fences = new Map<string, FenceDoc>();
  const { sql } = opts;

  await sql.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
  try {
    const ro = (await sql.query("SHOW transaction_read_only")).rows[0]?.transaction_read_only;
    if (ro !== "on") throw new Error("backfill_not_read_only");
    await sql.query(`SET LOCAL statement_timeout = ${statementTimeoutMs}`);
    const users = (await sql.query(USERS, [orgs])).rows.map((r) => [text(r.org_id), text(r.workspace_id), text(r.user_id)] as [string, string, string]);
    summary.users = users.length;

    // ---- fences ----
    for (let at = 0; at < users.length; at += pageSize) {
      const page = users.slice(at, at + pageSize);
      const cols = [page.map((p) => p[0]), page.map((p) => p[1]), page.map((p) => p[2])];
      type State = { tenant: LedgerTenant; deletions: Map<string, [string, string][]>; resets: { requestId: string; atMs: number; result: number }[]; invalid: boolean };
      const states = new Map<string, State>(page.map(([o, w, u]) => [userKey(o, w, u), { tenant: { orgId: o, workspaceId: w, userId: u }, deletions: new Map(), resets: [], invalid: false }]));
      if (sections.has("fences")) {
        for (const r of (await sql.query(DELETIONS_FOR, cols)).rows) {
          const st = states.get(userKey(r.org_id, r.workspace_id, r.user_id))!;
          summary.fences.deletion_rows++;
          if (!id(r.origin_id) || !id(r.source) || !id(r.session_id)) { st.invalid = true; continue; }
          st.deletions.set(r.origin_id, [...(st.deletions.get(r.origin_id) ?? []), [r.source, r.session_id]]);
        }
        for (const r of (await sql.query(RESETS_FOR, cols)).rows) {
          const st = states.get(userKey(r.org_id, r.workspace_id, r.user_id))!;
          summary.fences.reset_rows++;
          const atMs = int(r.at_ms), result = int(r.deleted);
          if (!id(r.request_id) || atMs === undefined || result === undefined) { st.invalid = true; continue; }
          st.resets.push({ requestId: r.request_id, atMs, result });
        }
      }
      await mapLimit([...states.values()], concurrency, async (st) => {
        let key: string;
        try {
          key = io.keysFor(st.tenant).fence;
        } catch {
          summary.fences.tenant_invalid++;
          return;
        }
        if (!sections.has("fences")) {
          // Ownership / revisions only: use the fence an earlier run backfilled.
          try {
            const read = await io.readFence(key);
            if (read.state === "present") fences.set(userKey(st.tenant.orgId, st.tenant.workspaceId, st.tenant.userId), read.fence);
          } catch { /* counted as skipped_no_fence by the sections that need it */ }
          return;
        }
        if (st.invalid) {
          summary.fences.input_invalid++;
          return;
        }
        const changes: FenceChange[] = [
          { deletions: [...st.deletions].map(([originId, sessions]) => ({ originId, sessions })) },
          ...st.resets.map((reset) => ({ reset })),
        ];
        /** ledger-s3's rules on `f` (in place). Tallies are returned, and counted once by the caller. */
        const apply = (f: FenceDoc) => {
          let changed = false, pairs = 0, resets = 0;
          for (const c of changes) {
            const r = applyFenceChange(f, c);
            changed ||= r.changed;
            pairs += r.newlyDeleted.length;
            if (r.reset === "recorded") resets++;
          }
          return { changed, pairs, resets };
        };
        const tally = (t: { pairs: number; resets: number }) => { summary.fences.pairs_added += t.pairs; summary.fences.resets_added += t.resets; };
        const uk = userKey(st.tenant.orgId, st.tenant.workspaceId, st.tenant.userId);
        try {
          if (opts.dryRun) {
            const read = await io.readFence(key);
            const f = read.state === "present" ? structuredClone(read.fence) : newFence();
            const t = apply(f);
            tally(t);
            if (f.resetsCompleted < f.resets) summary.fences.resets_in_flight++;
            if (read.state === "absent") summary.fences.would_create++;
            else if (t.changed) summary.fences.would_update++;
            else summary.fences.unchanged++;
            fences.set(uk, f);
            return;
          }
          // editFence may run the edit more than once (CAS retries): only the landed attempt's result counts.
          type Landed = { outcome: "created" | "updated" | "unchanged"; t: { changed: boolean; pairs: number; resets: number }; inFlight: boolean };
          const landed = await io.editFence<Landed>(key, (f) => {
            const absent = f.version === 0; // editFence starts an absent fence from newFence() (version 0)
            const t = apply(f);
            fences.set(uk, f);
            const inFlight = f.resetsCompleted < f.resets;
            if (!t.changed && !absent) return { next: null, result: { outcome: "unchanged", t, inFlight } };
            return { next: f, result: { outcome: absent ? "created" : "updated", t, inFlight } };
          });
          summary.fences[landed.outcome]++;
          tally(landed.t);
          if (landed.inFlight) summary.fences.resets_in_flight++;
        } catch (error) {
          const code = (error as { code?: unknown })?.code;
          if (code === "ledger_fence_invalid") summary.fences.unreadable++;
          else if (code === "ledger_fence_contended") summary.fences.contended++;
          else summary.fences.failed++;
          fences.delete(uk);
        }
      });
    }
    if (sections.has("fences")) {
      const f = summary.fences;
      summary.sections.fences = f.unreadable + f.contended + f.tenant_invalid + f.input_invalid + f.failed + f.resets_in_flight === 0 ? "complete" : "incomplete";
    }

    // ---- ownership ----
    if (sections.has("ownership")) {
      let cursor = ["", "", "", "", "", ""];
      for (;;) {
        const rows = (await sql.query(OWNERSHIP_PAGE, [orgs, ...cursor, rowPageSize])).rows;
        if (!rows.length) break;
        const last = rows[rows.length - 1]!;
        cursor = [text(last.org_id), text(last.workspace_id), text(last.user_id), text(last.origin_id), text(last.source), text(last.session_id)];
        await mapLimit(rows, concurrency, async (r) => {
          summary.ownership.rows++;
          const fence = fences.get(userKey(r.org_id, r.workspace_id, r.user_id));
          if (!fence) { summary.ownership.skipped_no_fence++; return; }
          if (!id(r.origin_id) || !id(r.source) || !id(r.session_id)) { summary.ownership.invalid++; return; }
          if (isDeleted(fence, r.origin_id, r.source, r.session_id)) { summary.ownership.skipped_deleted++; return; }
          const [, epoch, resets] = recordGeneration(fence, r.origin_id, r.source, r.session_id);
          try {
            const key = io.keysFor({ orgId: text(r.org_id), workspaceId: text(r.workspace_id), userId: text(r.user_id) }).owner(r.origin_id, r.source, r.session_id);
            const stored = (await io.readOwner(key))?.doc;
            // registerOwner keeps a stub at its current epoch (its oldest generation): nothing to do then.
            if (stored && stored.epoch >= epoch) { summary.ownership.unchanged++; return; }
            if (opts.dryRun) { summary.ownership.would_register++; return; }
            await io.registerOwner(key, { v: 1, originId: r.origin_id, source: r.source, sessionId: r.session_id, epoch, resets });
            summary.ownership.registered++;
          } catch {
            summary.ownership.failed++;
          }
        });
      }
      const o = summary.ownership;
      summary.sections.ownership = o.skipped_no_fence + o.invalid + o.failed === 0 ? "complete" : "incomplete";
    }

    // ---- revisions ----
    if (sections.has("revisions")) {
      type Group = { key: string; fence: FenceDoc; originId: string; entries: IndexEntryInput[] };
      /** One SQL stored revision -> an entry in its user's shard; false when skipped (counted). */
      const add = (groups: Map<string, Group>, row: { org: unknown; ws: unknown; user: unknown; origin: unknown; record: unknown; revision: unknown; revisionId: unknown; digest: unknown; source: unknown; session: unknown }) => {
        const fence = fences.get(userKey(row.org, row.ws, row.user));
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
          key = io.keysFor({ orgId: text(row.org), workspaceId: text(row.ws), userId: text(row.user) }).revisions(row.origin, row.record);
        } catch {
          summary.revisions.invalid++;
          return;
        }
        const g = groups.get(key) ?? { key, fence, originId: row.origin, entries: [] };
        g.entries.push(entry);
        groups.set(key, g);
      };
      const flush = async (groups: Map<string, Group>) => mapLimit([...groups.values()], concurrency, async (g) => {
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
        const rows = (await sql.query(REVISIONS_PAGE, [orgs, ...cursor, rowPageSize])).rows;
        if (!rows.length) break;
        const last = rows[rows.length - 1]!;
        cursor = [text(last.org_id), text(last.workspace_id), text(last.origin_id), text(last.record_id)];
        const groups = new Map<string, Group>();
        for (const r of rows) {
          summary.revisions.rows++;
          if (r.revision_id === null && r.digest === null) summary.revisions.seed_rows++;
          add(groups, { org: r.org_id, ws: r.workspace_id, user: r.user_id, origin: r.origin_id, record: r.record_id, revision: r.revision, revisionId: r.revision_id, digest: r.digest, source: r.source, session: r.session_id });
        }
        await flush(groups);
      }

      // (b) ingest_lazy_native_slots: the owner's current revision, under its session (ingest_lazy_clear's scope).
      let slotCursor = "";
      for (;;) {
        const rows = (await sql.query(SLOTS_PAGE, [orgs, slotCursor, rowPageSize])).rows;
        if (!rows.length) break;
        slotCursor = text(rows[rows.length - 1]!.key_sha256);
        const groups = new Map<string, Group>();
        for (const r of rows) {
          summary.revisions.slot_rows++;
          if (r.current_owner === null) { summary.revisions.slots_empty++; continue; }
          add(groups, { org: r.org_id, ws: r.workspace_id, user: r.current_owner, origin: r.origin_id, record: r.record_id, revision: r.current_revision, revisionId: null, digest: r.current_digest, source: r.current_source, session: r.current_session });
        }
        await flush(groups);
      }
      const v = summary.revisions;
      summary.sections.revisions = v.skipped_no_fence + v.invalid + v.failed === 0 ? "complete" : "incomplete";
    }
  } finally {
    await sql.query("ROLLBACK").catch(() => {});
  }
  return summary;
}
