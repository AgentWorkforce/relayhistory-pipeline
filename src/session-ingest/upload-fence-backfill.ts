/**
 * The one-time fence backfill (docs/decisions/0005-s3-native-upload-ledger.md, "The buffered backlog",
 * step 0). The backlog's deletions and resets were recorded in SQL (`sessions.upload_deletions`,
 * `sessions.upload_resets`). This copies each affected user's state into their S3 fence object,
 * `<per-user admission dir>/upload-fences.json`, so the buffered drain (which reads S3 alone and refuses
 * a user with no fence) can never republish a deleted session.
 *
 * SQL is read-only: one `BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY` snapshot, checked with
 * `SHOW transaction_read_only`, always ended by ROLLBACK. Only SELECTs on the two tables. Users are
 * paged by keyset over (org_id, workspace_id, user_id), optionally scoped to a set of orgs.
 *
 * S3 is merge-only, by CAS (`If-Match`, or `If-None-Match: *` to create), retried on contention:
 *   - `deleted`: the union of the stored pairs and SQL's, per origin. Never drops an entry (a session
 *     re-enabled in SQL since an earlier run stays fenced in S3: the fail-safe direction).
 *   - `resets`: max(stored, SQL row count).
 *   - `lastResetAtMs`: max(stored, SQL's latest `reset_at`, rounded UP to the millisecond so a batch
 *     received within that millisecond counts as pre-reset). The drain needs it to drop pre-reset batches.
 * A fence with fields this job does not know (a richer format written by another module) is never
 * rewritten: `fence_foreign_format`, reported. An unreadable fence fails closed: `fence_unreadable`.
 * A fence the merge would not change is not written (`unchanged`). Rerunning is therefore a no-op.
 *
 * The fence format is 0005's as the drain reads it ({v:1, resets, deleted:{origin:[[source, session]]},
 * lastResetAtMs}). TODO(ledger-s3): when src/session-ingest/ledger-s3.ts lands, use its fence key and
 * edit functions instead of the local ones here, so both writers agree on one format.
 *
 * Nothing here logs; the summary holds counts only: never a tenant id, session id or key.
 */
import { isObjectStoreError, objectKey, readAll, type ObjectStore, type ObjectVersion } from "../shared/vendor.ts";
import { journalKey } from "./async-admission.ts";
import type { LedgerTenant } from "./ledger.ts";
import { backoffMs } from "./writer.ts";

const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true });
const isObject = (v: unknown): v is Record<string, unknown> => v !== null && typeof v === "object" && !Array.isArray(v);
const ORG = /^(?!\.)[A-Za-z0-9._-]{1,128}$/;
const KNOWN_FENCE_KEYS = new Set(["v", "resets", "deleted", "lastResetAtMs"]);
const CAS_ATTEMPTS = 8;
export const MAX_BACKFILL_CONCURRENCY = 16;

export interface BackfillSql {
  query(sql: string, params?: unknown[]): Promise<{ rows: Record<string, unknown>[] }>;
}

export interface BackfillFence {
  v: 1;
  resets: number;
  deleted: Record<string, [source: string, sessionId: string][]>;
  lastResetAtMs?: number;
}

/** The per-user admission directory that holds `privacy-journal.json` (async-admission.ts). */
export function backfillFenceKey(keyRoot: string, tenant: LedgerTenant): string {
  const key = journalKey(keyRoot, tenant);
  return `${key.slice(0, key.length - "/privacy-journal.json".length)}/upload-fences.json`;
}

/** SQL's state for one user. */
export interface UserFenceState {
  tenant: LedgerTenant;
  deleted: Map<string, [string, string][]>;
  resetRows: number;
  lastResetAtMs?: number;
}

type Parsed = { kind: "ok"; fence: BackfillFence } | { kind: "unreadable" | "foreign" };

export function parseBackfillFence(text: string): Parsed {
  let v: unknown;
  try {
    v = JSON.parse(text);
  } catch {
    return { kind: "unreadable" };
  }
  if (!isObject(v)) return { kind: "unreadable" };
  if (Object.keys(v).some((k) => !KNOWN_FENCE_KEYS.has(k))) return { kind: "foreign" };
  if (v.v !== 1 || !Number.isSafeInteger(v.resets) || (v.resets as number) < 0 || !isObject(v.deleted)) return { kind: "unreadable" };
  if (v.lastResetAtMs !== undefined && (!Number.isSafeInteger(v.lastResetAtMs) || (v.lastResetAtMs as number) < 0)) return { kind: "unreadable" };
  for (const pairs of Object.values(v.deleted)) {
    if (!Array.isArray(pairs)) return { kind: "unreadable" };
    for (const p of pairs) if (!Array.isArray(p) || p.length !== 2 || typeof p[0] !== "string" || typeof p[1] !== "string") return { kind: "unreadable" };
  }
  return { kind: "ok", fence: v as unknown as BackfillFence };
}

/** Merge SQL's state into a stored fence. Never removes anything. Deterministic (sorted) output. */
export function mergeFence(stored: BackfillFence | null, state: UserFenceState): { fence: BackfillFence; added: number } {
  const deleted = new Map<string, Map<string, [string, string]>>();
  const add = (origin: string, pair: [string, string]) => {
    const m = deleted.get(origin) ?? new Map<string, [string, string]>();
    const k = JSON.stringify(pair);
    const fresh = !m.has(k);
    m.set(k, pair);
    deleted.set(origin, m);
    return fresh;
  };
  for (const [origin, pairs] of Object.entries(stored?.deleted ?? {})) for (const p of pairs) add(origin, [p[0], p[1]]);
  let added = 0;
  for (const [origin, pairs] of state.deleted) for (const p of pairs) if (add(origin, p)) added++;
  const out: BackfillFence = {
    v: 1,
    resets: Math.max(stored?.resets ?? 0, state.resetRows),
    deleted: Object.fromEntries(
      [...deleted.keys()].sort().map((origin) => [origin, [...deleted.get(origin)!.values()].sort((a, b) => (JSON.stringify(a) < JSON.stringify(b) ? -1 : 1))]),
    ),
  };
  const last = Math.max(stored?.lastResetAtMs ?? -1, state.lastResetAtMs ?? -1);
  if (last >= 0) out.lastResetAtMs = last;
  return { fence: out, added };
}

const sameFence = (a: BackfillFence, b: BackfillFence) => {
  const norm = (f: BackfillFence) => mergeFence(f, { tenant: { orgId: "", workspaceId: "", userId: "" }, deleted: new Map(), resetRows: 0 }).fence;
  return JSON.stringify(norm(a)) === JSON.stringify(norm(b));
};

export type FenceOutcome = "created" | "updated" | "unchanged" | "would_create" | "would_update" | "fence_unreadable" | "fence_foreign_format" | "fence_contended" | "tenant_invalid" | "failed";
const OUTCOMES: FenceOutcome[] = ["created", "updated", "unchanged", "would_create", "would_update", "fence_unreadable", "fence_foreign_format", "fence_contended", "tenant_invalid", "failed"];

export interface BackfillSummary {
  mode: "dry_run" | "execute";
  users: number;
  deletion_rows: number;
  reset_rows: number;
  /** (origin, source, session) pairs the merge added (or would add) to fences. */
  pairs_added: number;
  fences: Record<FenceOutcome, number>;
  stopped: "complete" | "max_users";
}

export interface BackfillOptions {
  sql: BackfillSql;
  store: ObjectStore;
  keyRoot: string;
  dryRun: boolean;
  /** Only these orgs (exact org ids). Default: every org in the two tables. */
  orgs?: string[];
  pageSize?: number;
  concurrency?: number;
  maxUsers?: number;
  statementTimeoutMs?: number;
  sleep?: (ms: number) => Promise<void>;
  random?: () => number;
}

const USERS_PAGE = `
SELECT org_id, workspace_id, user_id FROM (
  SELECT org_id, workspace_id, user_id FROM sessions.upload_deletions
  UNION
  SELECT org_id, workspace_id, user_id FROM sessions.upload_resets
) AS u
WHERE ($1::text[] IS NULL OR org_id = ANY($1::text[]))
  AND (org_id, workspace_id, user_id) > ($2::text, $3::text, $4::text)
ORDER BY org_id, workspace_id, user_id
LIMIT $5::int`;
const DELETIONS_FOR = `
SELECT d.org_id, d.workspace_id, d.user_id, d.origin_id, d.source, d.session_id
FROM sessions.upload_deletions AS d
JOIN unnest($1::text[], $2::text[], $3::text[]) AS p(o, w, u) ON d.org_id = p.o AND d.workspace_id = p.w AND d.user_id = p.u
ORDER BY d.org_id, d.workspace_id, d.user_id, d.origin_id, d.source, d.session_id`;
const RESETS_FOR = `
SELECT r.org_id, r.workspace_id, r.user_id, count(*)::bigint AS n,
       ceil(extract(epoch FROM max(r.reset_at)) * 1000)::bigint AS last_ms
FROM sessions.upload_resets AS r
JOIN unnest($1::text[], $2::text[], $3::text[]) AS p(o, w, u) ON r.org_id = p.o AND r.workspace_id = p.w AND r.user_id = p.u
GROUP BY r.org_id, r.workspace_id, r.user_id`;

const tenantKey = (o: unknown, w: unknown, u: unknown) => JSON.stringify([o, w, u]);
const str = (v: unknown): string => {
  if (typeof v !== "string") throw new Error("backfill_row_invalid");
  return v;
};
const int = (v: unknown): number => {
  const n = typeof v === "bigint" ? Number(v) : typeof v === "string" ? Number(v) : v;
  if (typeof n !== "number" || !Number.isSafeInteger(n) || n < 0) throw new Error("backfill_row_invalid");
  return n;
};

/** One read-only snapshot over the two tables; S3 fences merged by CAS. Counts only. */
export async function backfillUploadFences(opts: BackfillOptions): Promise<BackfillSummary> {
  const pageSize = opts.pageSize ?? 500;
  const concurrency = opts.concurrency ?? 4;
  const statementTimeoutMs = opts.statementTimeoutMs ?? 30_000;
  if (!Number.isInteger(pageSize) || pageSize < 1 || pageSize > 5_000) throw new Error("backfill_page_size_invalid");
  if (!Number.isInteger(concurrency) || concurrency < 1 || concurrency > MAX_BACKFILL_CONCURRENCY) throw new Error("backfill_concurrency_invalid");
  if (opts.maxUsers !== undefined && (!Number.isSafeInteger(opts.maxUsers) || opts.maxUsers < 1)) throw new Error("backfill_max_users_invalid");
  if (!Number.isInteger(statementTimeoutMs) || statementTimeoutMs < 1_000 || statementTimeoutMs > 600_000) throw new Error("backfill_statement_timeout_invalid");
  if (!/^[a-z0-9][a-z0-9-]{0,62}$/.test(opts.keyRoot)) throw new Error("backfill_key_root_invalid");
  if (opts.orgs) for (const org of opts.orgs) if (!ORG.test(org)) throw new Error("backfill_org_invalid");
  const summary: BackfillSummary = {
    mode: opts.dryRun ? "dry_run" : "execute", users: 0, deletion_rows: 0, reset_rows: 0, pairs_added: 0,
    fences: Object.fromEntries(OUTCOMES.map((o) => [o, 0])) as Record<FenceOutcome, number>, stopped: "complete",
  };
  const orgs = opts.orgs && opts.orgs.length ? [...new Set(opts.orgs)] : null;
  const { sql } = opts;
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

  await sql.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
  try {
    const ro = (await sql.query("SHOW transaction_read_only")).rows[0]?.transaction_read_only;
    if (ro !== "on") throw new Error("backfill_not_read_only");
    await sql.query(`SET LOCAL statement_timeout = ${statementTimeoutMs}`);
    let cursor: [string, string, string] = ["", "", ""];
    for (;;) {
      const remaining = opts.maxUsers === undefined ? pageSize : Math.min(pageSize, opts.maxUsers - summary.users);
      if (remaining <= 0) {
        summary.stopped = "max_users";
        break;
      }
      const page = (await sql.query(USERS_PAGE, [orgs, ...cursor, remaining])).rows.map((r) => [str(r.org_id), str(r.workspace_id), str(r.user_id)] as [string, string, string]);
      if (!page.length) break;
      cursor = page[page.length - 1]!;
      const cols = [page.map((p) => p[0]), page.map((p) => p[1]), page.map((p) => p[2])];
      const states = new Map<string, UserFenceState>(page.map(([o, w, u]) => [tenantKey(o, w, u), { tenant: { orgId: o, workspaceId: w, userId: u }, deleted: new Map(), resetRows: 0 }]));
      for (const r of (await sql.query(DELETIONS_FOR, cols)).rows) {
        const state = states.get(tenantKey(r.org_id, r.workspace_id, r.user_id));
        if (!state) throw new Error("backfill_row_invalid");
        const origin = str(r.origin_id);
        state.deleted.set(origin, [...(state.deleted.get(origin) ?? []), [str(r.source), str(r.session_id)]]);
        summary.deletion_rows++;
      }
      for (const r of (await sql.query(RESETS_FOR, cols)).rows) {
        const state = states.get(tenantKey(r.org_id, r.workspace_id, r.user_id));
        if (!state) throw new Error("backfill_row_invalid");
        state.resetRows = int(r.n);
        state.lastResetAtMs = int(r.last_ms);
        summary.reset_rows += state.resetRows;
      }
      summary.users += page.length;
      const queue = [...states.values()];
      await Promise.all(Array.from({ length: Math.min(concurrency, queue.length) }, async () => {
        for (let s = queue.shift(); s; s = queue.shift()) summary.fences[await applyFence(s)]++;
      }));
    }
  } finally {
    await sql.query("ROLLBACK").catch(() => {});
  }
  return summary;

  async function applyFence(state: UserFenceState): Promise<FenceOutcome> {
    if (!ORG.test(state.tenant.orgId)) return "tenant_invalid";
    let key: string;
    try {
      key = backfillFenceKey(opts.keyRoot, state.tenant);
      objectKey(key);
    } catch {
      return "tenant_invalid";
    }
    try {
      for (let attempt = 0; attempt < CAS_ATTEMPTS; attempt++) {
        let stored: { fence: BackfillFence; version: ObjectVersion } | null = null;
        try {
          const got = await store.getObject(objectKey(key));
          const bytes = await readAll(got.body);
          if (bytes.byteLength > 8 * 1024 * 1024) return "fence_unreadable";
          let text: string;
          try {
            text = decoder.decode(bytes);
          } catch {
            return "fence_unreadable";
          }
          const parsed = parseBackfillFence(text);
          if (parsed.kind !== "ok") return parsed.kind === "foreign" ? "fence_foreign_format" : "fence_unreadable";
          stored = { fence: parsed.fence, version: got.meta.version };
        } catch (error) {
          if (!isObjectStoreError(error, "not_found")) throw error;
        }
        const { fence, added } = mergeFence(stored?.fence ?? null, state);
        if (stored && sameFence(stored.fence, fence)) return "unchanged";
        if (opts.dryRun) {
          summary.pairs_added += added;
          return stored ? "would_update" : "would_create";
        }
        try {
          await store.put(objectKey(key), encoder.encode(JSON.stringify(fence)),
            stored ? { ifMatch: stored.version, contentType: "application/json" } : { ifNoneMatch: true, contentType: "application/json" });
          summary.pairs_added += added;
          return stored ? "updated" : "created";
        } catch (error) {
          if (!isObjectStoreError(error, "precondition_failed") && !isObjectStoreError(error, "already_exists")) throw error;
          await (opts.sleep ?? ((ms: number) => new Promise<void>((r) => setTimeout(r, ms))))(backoffMs(attempt, opts.random));
        }
      }
      return "fence_contended";
    } catch {
      return "failed";
    }
  }
}
