/**
 * Bounded summary sweep (SUMMARY_SWEEP_BOUNDED=on; default off). The legacy sweep's two SELECTs
 * (sweep.ts findMissingSummaries / findRegenDue) filter every live session_index row through the
 * summary, live-job and dead-letter anti-joins and only then sort and LIMIT, so each run reads most
 * of session_index and the three anti-join tables. Here every query first takes one keyset page of
 * at most `pageRows` live session_index rows, newest indexed_at first, and runs the same per-session
 * checks only on that page (primary-key / (org_id, session_id) index probes per row, forced with
 * LATERAL ... LIMIT 1 so no plan can hash a whole table), so a run reads
 * O(pageRows) rows however large the tables are:
 *
 * - window pass, every tick: the newest page with indexed_at inside the last `windowSeconds`
 *   (indexed_at moves on every manifest upsert, so new and resumed sessions land here first);
 * - catch-up walk, one page per tick per (query, stage) while a walk is active: the page after the
 *   walk's cursor (indexed_at, org_id, workspace_id, harness, session_id, machine_id), newest first,
 *   from the top of the table to its end; then it idles until `catchupSeconds` after its start. It
 *   is the rare full pass that finds what the window cannot (a summary never produced for an old
 *   session, a master-key rotation, rows from migration 0004).
 *
 * The checks are exactly the legacy ones, so a session with a live job or an unresolved dead letter
 * for the stage is never queued again. The cursor lives in the worker process: a restart starts a
 * new walk from the top (never skips, only re-reads).
 *
 * Walk latency (known trade-off; follow-up "bounded sweep walk latency" must land before anyone
 * recommends flag-on): work that becomes due without indexed_at moving is found only by the walk,
 * which takes ceil(live rows / pageRows) ticks and restarts from the top on a worker restart. That
 * covers a retry whose backoff rung (summary-regen/settings.ts backoffSeconds) runs past the window,
 * a summariser config change, a dead letter resolved on an old session, a summary lost or deleted, a
 * master-key rotation, idle-turn regeneration (prevented from falling outside the window by the
 * window >= SUMMARY_REGEN_IDLE_SECONDS startup rule), and rows pushed out of the window page when
 * more than pageRows sessions were indexed inside one window.
 *
 * Index: supabase/pending/summary_sweep_indexed_at.sql. With it, a page is a backward index range
 * scan that stops after pageRows entries. Without it the SQL is still correct (same rows), but each
 * page is a full scan of session_index with a top-n sort, so the worker refuses to start with the
 * flag on unless the index is present and valid (verifySummarySweepIndex, run at startup).
 */
import type { HoopDb } from "../shared/db.ts";
import { log } from "../shared/log.ts";
import { maxDueAttempts } from "../summary-regen/backoff.ts";
import { regenSettings } from "../summary-regen/settings.ts";
import type { BobCostasRegen } from "./regen.ts";

export const SUMMARY_SWEEP_BOUNDED_VAR = "SUMMARY_SWEEP_BOUNDED";

export interface BoundedSweepSettings {
  /** The window pass looks at rows with indexed_at >= now - windowSeconds. */
  windowSeconds: number;
  /** Rows read from session_index per query (one keyset page). */
  pageRows: number;
  /** A new catch-up walk starts at most this often (the first one at the first tick). */
  catchupSeconds: number;
}

export const BOUNDED_SWEEP_BOUNDS = {
  windowSeconds: { min: 300, max: 604_800, fallback: 3_600 },
  pageRows: { min: 10, max: 10_000, fallback: 1_000 },
  catchupSeconds: { min: 3_600, max: 2_592_000, fallback: 86_400 },
} as const;

const TUNING = {
  windowSeconds: "SUMMARY_SWEEP_WINDOW_SECONDS",
  pageRows: "SUMMARY_SWEEP_PAGE_ROWS",
  catchupSeconds: "SUMMARY_SWEEP_CATCHUP_SECONDS",
} as const;

function tuning(env: NodeJS.ProcessEnv, key: keyof typeof TUNING): number {
  const name = TUNING[key];
  const b = BOUNDED_SWEEP_BOUNDS[key];
  const raw = env[name];
  if (raw === undefined || raw === "") return b.fallback;
  const n = /^\d{1,9}$/.test(raw) ? Number(raw) : NaN;
  if (!(n >= b.min && n <= b.max)) throw new Error(`96bulls: ${name} must be an integer from ${b.min} to ${b.max}`);
  return n;
}

/**
 * SUMMARY_SWEEP_BOUNDED: on or off (unset / empty = off); anything else refuses startup. null when
 * off: the sweep then runs the legacy queries only, byte for byte. The tuning variables are read
 * (and a malformed one refuses startup) only when the flag is on. With the flag on, a window shorter
 * than the effective SUMMARY_REGEN_IDLE_SECONDS (summary-regen/settings.ts) also refuses startup: a
 * session goes idle only after its last indexed_at, so with a shorter window idle-turn regeneration
 * would be found by the walk only.
 */
export function boundedSweepFromEnv(env: NodeJS.ProcessEnv = process.env): BoundedSweepSettings | null {
  const raw = env[SUMMARY_SWEEP_BOUNDED_VAR];
  if (raw === undefined || raw === "" || raw === "off") return null;
  if (raw !== "on") throw new Error(`96bulls: ${SUMMARY_SWEEP_BOUNDED_VAR} must be on or off`);
  const settings = { windowSeconds: tuning(env, "windowSeconds"), pageRows: tuning(env, "pageRows"), catchupSeconds: tuning(env, "catchupSeconds") };
  const idleSeconds = regenSettings(env).turns.idleSeconds;
  if (settings.windowSeconds < idleSeconds)
    throw new Error(`96bulls: ${TUNING.windowSeconds} (${settings.windowSeconds}) must be at least SUMMARY_REGEN_IDLE_SECONDS (${idleSeconds})`);
  return settings;
}

/** The operator index the bounded pages need (supabase/pending/summary_sweep_indexed_at.sql). */
export const SUMMARY_SWEEP_INDEX_SCHEMA = "sessions";
export const SUMMARY_SWEEP_INDEX_TABLE = "session_index";
export const SUMMARY_SWEEP_INDEX_NAME = "session_index_summary_sweep_idx";

/**
 * The catalog probe: one row per index of that name on that table, `valid` true only when the
 * index is usable (indisvalid, indisready, indislive). A failed or in-progress CONCURRENTLY build
 * leaves indisvalid false (and IF NOT EXISTS then skips it: the operator must drop it first).
 * Catalogs are readable by every role. Constants only, no parameter.
 */
export const SUMMARY_SWEEP_INDEX_SQL =
  "SELECT (i.indisvalid AND i.indisready AND i.indislive) AS valid " +
  "FROM pg_catalog.pg_index i " +
  "JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid " +
  "JOIN pg_catalog.pg_class t ON t.oid = i.indrelid " +
  "JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace " +
  `WHERE n.nspname = '${SUMMARY_SWEEP_INDEX_SCHEMA}' AND t.relname = '${SUMMARY_SWEEP_INDEX_TABLE}' AND c.relname = '${SUMMARY_SWEEP_INDEX_NAME}'`;

export type SummarySweepIndexState = "valid" | "invalid" | "missing";

/** The refusal reasons (by name only), as READ_API_OWNER_BUFFERED_INDEX_VERIFY names them. */
export const SUMMARY_SWEEP_INDEX_REASONS = Object.freeze({
  missing: "index_missing",
  invalid: "index_invalid",
  unverified: "index_unverified",
} as const);

export async function probeSummarySweepIndex(db: Pick<HoopDb, "query">): Promise<SummarySweepIndexState> {
  const rows = (await db.query<{ valid?: unknown }>(SUMMARY_SWEEP_INDEX_SQL)).rows;
  if (rows.length === 0) return "missing";
  return rows.some((r) => r.valid === true) ? "valid" : "invalid";
}

/**
 * The startup check with the flag on: resolves only when the index is present and valid; otherwise
 * throws, so the worker refuses to start (fail closed, as READ_API_OWNER_BUFFERED_INDEX_VERIFY fences
 * buffered reads) instead of running a full session_index scan per page. A probe error refuses too.
 */
export async function verifySummarySweepIndex(db: Pick<HoopDb, "query">): Promise<void> {
  let state: SummarySweepIndexState | "unverified";
  try {
    state = await probeSummarySweepIndex(db);
  } catch {
    state = "unverified";
  }
  if (state === "valid") return;
  throw new Error(
    `96bulls: ${SUMMARY_SWEEP_BOUNDED_VAR}=on needs a valid ${SUMMARY_SWEEP_INDEX_SCHEMA}.${SUMMARY_SWEEP_INDEX_NAME} ` +
      `(${SUMMARY_SWEEP_INDEX_REASONS[state]}; see supabase/pending/summary_sweep_indexed_at.sql)`,
  );
}

/** The worker's startup check with the flag on (main.ts): the index probe, then one line with the settings. */
export function boundedSweepStartup(settings: BoundedSweepSettings) {
  return async (db: Pick<HoopDb, "query">): Promise<void> => {
    await verifySummarySweepIndex(db);
    log.info("bob costas summary sweep", {
      stage: "summary",
      state: "bounded_on",
      window_seconds: settings.windowSeconds,
      page_rows: settings.pageRows,
      catchup_seconds: settings.catchupSeconds,
    });
  };
}

/** A keyset position: the last session_index row a walk consumed. */
export interface SweepCursor {
  at: string;
  org_id: string;
  workspace_id: string;
  harness: string;
  session_id: string;
  machine_id: string;
}

/** Where a page starts: the window (indexed_at >= since) or a walk (from the top, or after a cursor). */
export type PageBound = { since: Date } | { after: SweepCursor | null };

/** Every page row the caller needs: the rows that need work, plus the page's last row (the edge). */
export interface PageRow {
  org_id: string;
  workspace_id: string;
  harness: string;
  session_id: string;
  machine_id: string;
  manifest_key: string;
  trace_id: string;
  revision: string;
  session_uuid: string | null;
  cursor_at: string;
  rn: number;
  page_rows: number;
  needs: boolean;
}

const KEY = ["org_id", "workspace_id", "harness", "session_id", "machine_id"] as const;
const DESC = "indexed_at DESC, org_id DESC, workspace_id DESC, harness DESC, session_id DESC, machine_id DESC";
const joinOn = (a: string, b: string) => KEY.map((k) => `${a}.${k}=${b}.${k}`).join(" AND ");

/** The page CTEs: `page` (at most $pageParam rows, newest first) and `numbered` (rn 1.., n = page size). */
function pageSql(bound: PageBound, pageParam: number, first: number, extra: string): { sql: string; params: unknown[] } {
  let where = "";
  let params: unknown[] = [];
  if ("since" in bound) {
    where = ` AND i.indexed_at >= $${first}::timestamptz`;
    params = [bound.since.toISOString()];
  } else if (bound.after) {
    const c = bound.after;
    where = ` AND (i.indexed_at, i.org_id, i.workspace_id, i.harness, i.session_id, i.machine_id)
             < ($${first}::timestamptz, $${first + 1}, $${first + 2}, $${first + 3}, $${first + 4}, $${first + 5})`;
    params = [c.at, c.org_id, c.workspace_id, c.harness, c.session_id, c.machine_id];
  }
  const sql = `WITH page AS MATERIALIZED (
       SELECT i.org_id, i.workspace_id, i.harness, i.session_id, i.machine_id, i.manifest_key,
              i.trace_id, i.revision, i.indexed_at${extra}
         FROM sessions.session_index i
        WHERE i.deleted_at IS NULL AND i.manifest_key IS NOT NULL${where}
        ORDER BY i.${DESC.split(", ").join(", i.")}
        LIMIT $${pageParam}
     ), numbered AS (
       SELECT p.*, row_number() OVER (ORDER BY ${DESC}) AS rn, count(*) OVER () AS n FROM page p
     )`;
  return { sql, params };
}

const PAGE_SELECT = `p.org_id, p.workspace_id, p.harness, p.session_id, p.machine_id, p.manifest_key,
            p.trace_id::text AS trace_id, p.revision::text AS revision, e.session_uuid::text AS session_uuid,
            p.indexed_at::text AS cursor_at, p.rn::int AS rn, p.n::int AS page_rows, (t.rn IS NOT NULL) AS needs`;
// Every per-row check is a LEFT JOIN LATERAL (... LIMIT 1): a subquery with LIMIT is never flattened
// into a join, so the planner cannot pick a hash (anti-)join that reads a whole table; each check
// stays one parameterized index probe per page row, whatever the statistics say.
const PAGE_TAIL = `LEFT JOIN LATERAL (SELECT e.session_uuid FROM sessions.session_index_ext e
                          WHERE ${joinOn("e", "p")} LIMIT 1) e ON t.rn IS NOT NULL
      WHERE t.rn IS NOT NULL OR p.rn = p.n
      ORDER BY p.rn`;
/** Joins jq / dq: a live job, or an unresolved dead letter, for this stage and session. */
const QUEUED_JOINS = `LEFT JOIN LATERAL (SELECT 1 AS hit FROM sessions.pipeline_jobs j
                          WHERE j.stage=$2 AND NOT j.dlq AND j.org_id=p.org_id AND j.workspace_id=p.workspace_id
                            AND j.session_id=p.session_id AND j.machine_id=p.machine_id LIMIT 1) jq ON true
         LEFT JOIN LATERAL (SELECT 1 AS hit FROM sessions.pipeline_dead_letters d
                          WHERE d.stage=$2 AND d.resolved_at IS NULL AND d.org_id=p.org_id AND d.workspace_id=p.workspace_id
                            AND d.session_id=p.session_id AND d.machine_id=p.machine_id LIMIT 1) dq ON true`;
const NOT_QUEUED = "jq.hit IS NULL AND dq.hit IS NULL";

/** The legacy missing-summary checks (sweep.ts findMissingSummaries), on one page only. */
export async function findMissingPage(
  db: Pick<HoopDb, "query">,
  stage: string,
  kind: string,
  pageRows: number,
  currentKeyId: string,
  bound: PageBound,
): Promise<PageRow[]> {
  const page = pageSql(bound, 3, 5, "");
  const r = await db.query<PageRow>(
    `${page.sql}, todo AS (
       SELECT p.rn FROM numbered p
         LEFT JOIN LATERAL (SELECT 1 AS hit FROM sessions.session_summaries s
                             WHERE ${joinOn("s", "p")} AND s.kind=$1
                               AND (s.status <> 'ready' OR s.key_id = $4) LIMIT 1) sq ON true
         ${QUEUED_JOINS}
        WHERE sq.hit IS NULL AND ${NOT_QUEUED}
     )
     SELECT ${PAGE_SELECT}
       FROM numbered p LEFT JOIN todo t ON t.rn = p.rn
       ${PAGE_TAIL}`,
    [kind, stage, pageRows, currentKeyId, ...page.params],
  );
  return r.rows;
}

export interface RegenPageRow extends PageRow {
  reason: "config_changed" | "retry_backoff" | "turns" | "stamp";
  ended: boolean;
  prior_status: string;
  prior_failure_code: string | null;
  prior_pipeline_version: string | null;
}

/** The legacy regeneration-due checks (sweep.ts findRegenDue), on one page only. */
export async function findRegenPage(
  db: Pick<HoopDb, "query">,
  stage: string,
  kind: string,
  pageRows: number,
  regen: BobCostasRegen,
  now: Date,
  bound: PageBound,
): Promise<RegenPageRow[]> {
  const { turns, retry } = regen.settings;
  const page = pageSql(bound, 3, 11, ", i.state, i.last_activity_at");
  const r = await db.query<RegenPageRow>(
    `${page.sql}, todo AS (
       SELECT p.rn,
              CASE WHEN $4::boolean AND s.degraded AND s.config_hash IS DISTINCT FROM $5 THEN 'config_changed'
                   WHEN $6::boolean AND s.degraded AND s.attempts BETWEEN 1 AND $7 AND s.next_retry_at <= $8::timestamptz THEN 'retry_backoff'
                   WHEN s.covered_turns IS NULL AND s.revision = p.revision THEN 'stamp'
                   ELSE 'turns' END AS reason,
              (p.state IN ('done', 'abandoned')) AS ended,
              s.status AS prior_status, s.failure_code AS prior_failure_code, s.pipeline_version AS prior_pipeline_version
         FROM numbered p
         JOIN LATERAL (SELECT * FROM sessions.session_summaries s WHERE ${joinOn("s", "p")} AND s.kind=$1 LIMIT 1) s ON true
         ${QUEUED_JOINS}
        WHERE s.status = 'ready' AND s.revision <= p.revision
          AND (   ($4::boolean AND s.degraded AND s.config_hash IS DISTINCT FROM $5)
               OR ($6::boolean AND s.degraded AND s.attempts BETWEEN 1 AND $7 AND s.next_retry_at <= $8::timestamptz)
               OR ($9::boolean AND (p.state IN ('done', 'abandoned') OR p.last_activity_at < $10::timestamptz)
                   AND (s.covered_turns IS NULL OR s.revision < p.revision)))
          AND ${NOT_QUEUED}
     )
     SELECT ${PAGE_SELECT}, t.reason, t.ended, t.prior_status, t.prior_failure_code, t.prior_pipeline_version
       FROM numbered p LEFT JOIN todo t ON t.rn = p.rn
       ${PAGE_TAIL}`,
    [
      kind,
      stage,
      pageRows,
      retry.configReset,
      regen.configHash,
      retry.enabled,
      maxDueAttempts(retry.backoffSeconds),
      now.toISOString(),
      turns.enabled,
      new Date(now.getTime() - turns.idleSeconds * 1000).toISOString(),
      ...page.params,
    ],
  );
  return r.rows;
}

const cursorOf = (r: PageRow): SweepCursor => ({
  at: r.cursor_at, org_id: r.org_id, workspace_id: r.workspace_id, harness: r.harness, session_id: r.session_id, machine_id: r.machine_id,
});

/**
 * Takes at most `limit` rows that need work, in page order. `cursor` is the last row consumed: the
 * last picked row when the limit stopped it (the rest of the page is read again next time), else
 * the page's edge. `exhausted`: the whole page was consumed and it was short, so the table ends here.
 */
export function consumePage<T extends PageRow>(rows: T[], limit: number, pageRows: number): { picked: T[]; cursor: SweepCursor | null; exhausted: boolean } {
  const picked: T[] = [];
  let cursor: SweepCursor | null = null;
  for (const r of rows) {
    if (r.needs) {
      if (picked.length === limit) return { picked, cursor, exhausted: false };
      picked.push(r);
    }
    cursor = cursorOf(r);
  }
  const size = rows.length ? rows[rows.length - 1]!.page_rows : 0;
  return { picked, cursor, exhausted: size < pageRows };
}

interface Walk {
  active: boolean;
  startedAt: number;
  cursor: SweepCursor | null;
}

/**
 * The per-(query, stage) catch-up walks of one sweep. `next` runs one page: the window page every
 * tick, then the walk's page while the walk is active (a new walk starts at most every catchupSeconds).
 * The two pages run one after the other, so rows the window page queued already have a live job and
 * the walk's page skips them.
 */
export function boundedWalks(settings: BoundedSweepSettings) {
  const walks = new Map<string, Walk>();
  return {
    async run<T extends PageRow>(
      id: string,
      now: Date,
      limit: number,
      query: (bound: PageBound) => Promise<T[]>,
      queue: (rows: T[], pass: "window" | "walk") => Promise<void>,
    ): Promise<void> {
      const window = consumePage(await query({ since: new Date(now.getTime() - settings.windowSeconds * 1000) }), limit, settings.pageRows);
      await queue(window.picked, "window");
      let walk = walks.get(id);
      if (!walk) walks.set(id, (walk = { active: false, startedAt: -Infinity, cursor: null }));
      if (!walk.active && now.getTime() - walk.startedAt >= settings.catchupSeconds * 1000) {
        walk.active = true;
        walk.startedAt = now.getTime();
        walk.cursor = null;
      }
      if (!walk.active) return;
      const page = consumePage(await query({ after: walk.cursor }), limit, settings.pageRows);
      await queue(page.picked, "walk");
      if (page.exhausted) {
        walk.active = false;
        walk.cursor = null;
      } else if (page.cursor) walk.cursor = page.cursor;
    },
    /** Test seam: the walk state for one id. */
    state(id: string): Readonly<Walk> | undefined {
      return walks.get(id);
    },
  };
}
