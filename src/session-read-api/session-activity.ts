/**
 * GET /v1/sessions/activity (non-q) — distinct sessions with at least one event
 * on each UTC day of the window, by source. One claim-bound SECURITY DEFINER
 * call; the SQL mirrors relayhistory's lib/session-activity.ts oracle exactly.
 * Every UTC day in the window appears even when it has no sessions.
 */
import type { SqlQuery, TenantRunner } from "./liveness.ts";

const DAY_MS = 86_400_000;
/** Contract cap: since/until may span at most this many UTC days. */
export const MAX_SESSION_ACTIVITY_DAYS = 92;
export interface SessionActivityDay {
  date: string;
  total: number;
  bySource: Record<string, number>;
}

/** Every UTC date from `since` through `until`, inclusive, as `YYYY-MM-DD`. */
export function activityDates(since: Date, until: Date): string[] {
  const dates: string[] = [];
  const last = Math.floor(until.getTime() / DAY_MS);
  for (let day = Math.floor(since.getTime() / DAY_MS); day <= last; day++)
    dates.push(new Date(day * DAY_MS).toISOString().slice(0, 10));
  return dates;
}

/** Number of UTC days the window touches; zero when `until` precedes `since`. */
export function activityDayCount(since: Date, until: Date): number {
  return Math.max(
    0,
    Math.floor(until.getTime() / DAY_MS) - Math.floor(since.getTime() / DAY_MS) + 1,
  );
}

export async function sessionActivity(
  run: TenantRunner,
  orgId: string,
  workspaceId: string,
  since: Date,
  until: Date,
): Promise<SessionActivityDay[]> {
  const { rows } = await run(orgId, workspaceId, (q: SqlQuery) =>
    q.query<{ day: string; source: string; sessions: number | string }>(
      "SELECT day, source, sessions FROM sessions.session_activity($1, $2, $3)",
      [workspaceId, since.toISOString(), until.toISOString()],
    ));
  const byDate = new Map<string, SessionActivityDay>(
    activityDates(since, until).map((date) => [
      date,
      { date, total: 0, bySource: {} },
    ]),
  );
  for (const row of rows) {
    const entry = byDate.get(row.day);
    if (!entry) continue;
    const sessions = Number(row.sessions);
    entry.bySource[row.source] = sessions;
    entry.total += sessions;
  }
  return [...byDate.values()];
}

interface ClickHouseQueryClient {
  select<T>(query: string, options?: { params?: Record<string, string | number | boolean | unknown[]> }): Promise<T[]>;
}

/**
 * The q lane over ClickHouse (all-CH backend): newest version per event key
 * first — the ReplacingMergeTree can hold several versions — then the day's
 * distinct sessions under the same q predicate the list uses (first 4096
 * content chars, or the task title). Shape matches the Supa lane's rows.
 */
export async function sessionActivityClickHouse(
  ch: ClickHouseQueryClient,
  orgId: string,
  workspaceId: string,
  since: Date,
  until: Date,
  q: string,
): Promise<SessionActivityDay[]> {
  const rows = await ch.select<{ day: string; source: string; sessions: number | string }>(`
      SELECT formatDateTime(toDate(ts, 'UTC'), '%Y-%m-%d') AS day,
             source, uniqExact(session_id) AS sessions
      FROM (
        SELECT ts, session_id, source,
               argMax(content, version) AS content, argMax(task_title, version) AS task_title
        FROM transcript_events
        WHERE org_id = {org:String} AND workspace_id = {ws:String}
          AND ts >= fromUnixTimestamp64Milli({since:Int64})
          AND ts <= fromUnixTimestamp64Milli({until:Int64})
        GROUP BY org_id, workspace_id, ts, session_id, source, machine_id, kind, event_id
      )
      WHERE (positionCaseInsensitiveUTF8(substringUTF8(content, 1, 4096), {q:String}) > 0
         OR positionCaseInsensitiveUTF8(task_title, {q:String}) > 0)
      GROUP BY day, source
      ORDER BY day, source`,
    { params: { org: orgId, ws: workspaceId, since: since.getTime(), until: until.getTime(), q } },
  );
  const byDate = new Map<string, SessionActivityDay>(
    activityDates(since, until).map((date) => [date, { date, total: 0, bySource: {} }]),
  );
  for (const row of rows) {
    const entry = byDate.get(row.day);
    if (!entry) continue;
    const sessions = Number(row.sessions);
    entry.bySource[row.source] = sessions;
    entry.total += sessions;
  }
  return [...byDate.values()];
}
