/**
 * The exporter's local watermark: one SQLite file (ids, revisions, counts and
 * object keys; never content). Losing it is safe: a fresh run re-reads Neon,
 * finds every unchanged chunk and manifest already stored and writes nothing,
 * but it then re-reads everything, so keep it.
 *
 * - `groups`: every `(org, session)` the last sweeps saw, with its Neon
 *   revision and activity, and the sweep id that last saw it.
 * - `exported`: what this exporter last wrote for a group: the revision it
 *   read before reading the content, and the manifest keys it owns.
 */
import { DatabaseSync } from "node:sqlite";

export interface ExportedRow {
  org_id: string;
  session_id: string;
  revision: number;
  exported_at: number;
  keys: string[];
  error: string | null;
  attempts: number;
}

export interface WorkItem {
  org_id: string;
  session_id: string;
  revision: number;
  kind: "delete" | "changed" | "recent" | "history" | "retry" | "reverify";
  events: number;
}

export interface Backlog {
  groups: number;
  exported: number;
  pending: number;
  pendingEvents: number;
  recentPending: number;
  recentPendingEvents: number;
  changed: number;
  errored: number;
  pendingDeletes: number;
}

export class SyncState {
  readonly db: DatabaseSync;

  constructor(path: string) {
    this.db = new DatabaseSync(path);
    this.db.exec("PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;");
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS groups (
        org_id TEXT NOT NULL, session_id TEXT NOT NULL,
        revision INTEGER NOT NULL DEFAULT 0, last_ms INTEGER, events INTEGER NOT NULL DEFAULT 0,
        rev_sweep INTEGER NOT NULL DEFAULT 0, act_sweep INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (org_id, session_id));
      CREATE TABLE IF NOT EXISTS exported (
        org_id TEXT NOT NULL, session_id TEXT NOT NULL,
        revision INTEGER NOT NULL, exported_at INTEGER NOT NULL, keys TEXT NOT NULL,
        error TEXT, attempts INTEGER NOT NULL DEFAULT 0, next_try INTEGER NOT NULL DEFAULT 0,
        failed_revision INTEGER,
        PRIMARY KEY (org_id, session_id));
      CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS truncations (
        org_id TEXT NOT NULL, session_id TEXT NOT NULL, manifest_key TEXT NOT NULL,
        strings INTEGER NOT NULL, recorded_at INTEGER NOT NULL,
        PRIMARY KEY (org_id, session_id, manifest_key));
      CREATE INDEX IF NOT EXISTS groups_last ON groups (last_ms DESC);
    `);
  }

  #tx(fn: () => void) {
    this.db.exec("BEGIN");
    try {
      fn();
      this.db.exec("COMMIT");
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }

  meta(k: string): string | undefined {
    return (this.db.prepare("SELECT v FROM meta WHERE k = ?").get(k) as { v: string } | undefined)?.v;
  }

  setMeta(k: string, v: string) {
    this.db.prepare("INSERT INTO meta (k, v) VALUES (?, ?) ON CONFLICT (k) DO UPDATE SET v = excluded.v").run(k, v);
  }

  /** Record a sweep page of `session_revisions`. */
  seeRevisions(sweep: number, rows: { org_id: string; session_id: string; revision: string }[]) {
    const q = this.db.prepare(
      `INSERT INTO groups (org_id, session_id, revision, rev_sweep) VALUES (?, ?, ?, ?)
       ON CONFLICT (org_id, session_id) DO UPDATE SET revision = excluded.revision, rev_sweep = excluded.rev_sweep`,
    );
    this.#tx(() => {
      for (const r of rows) q.run(r.org_id, r.session_id, Number(r.revision), sweep);
    });
  }

  /** Record a sweep page of rollup activity. */
  seeActivity(sweep: number, rows: { org_id: string; session_id: string; last_ms: string; events: string }[]) {
    const q = this.db.prepare(
      `INSERT INTO groups (org_id, session_id, last_ms, events, act_sweep) VALUES (?, ?, ?, ?, ?)
       ON CONFLICT (org_id, session_id) DO UPDATE SET last_ms = excluded.last_ms, events = excluded.events,
         act_sweep = excluded.act_sweep`,
    );
    this.#tx(() => {
      for (const r of rows) q.run(r.org_id, r.session_id, Number(r.last_ms), Number(r.events), sweep);
    });
  }

  /**
   * After complete sweeps of both sources: a group neither saw is gone from
   * Neon. Its row is dropped; its exported keys stay until the delete runs.
   */
  dropUnseen(revSweep: number, actSweep: number): number {
    return this.db
      .prepare("DELETE FROM groups WHERE rev_sweep < ? AND act_sweep < ?")
      .run(revSweep, actSweep).changes as number;
  }

  exported(org: string, session: string): ExportedRow | undefined {
    const row = this.db.prepare("SELECT * FROM exported WHERE org_id = ? AND session_id = ?").get(org, session) as
      | (Omit<ExportedRow, "keys"> & { keys: string })
      | undefined;
    return row ? { ...row, keys: JSON.parse(row.keys) as string[] } : undefined;
  }

  markExported(org: string, session: string, revision: number, keys: string[], at = Date.now()) {
    if (keys.length === 0 && !this.db.prepare("SELECT 1 FROM groups WHERE org_id = ? AND session_id = ?").get(org, session)) {
      this.db.prepare("DELETE FROM exported WHERE org_id = ? AND session_id = ?").run(org, session);
      return;
    }
    this.db
      .prepare(
        `INSERT INTO exported (org_id, session_id, revision, exported_at, keys, error, attempts, next_try)
         VALUES (?, ?, ?, ?, ?, NULL, 0, 0)
         ON CONFLICT (org_id, session_id) DO UPDATE SET revision = excluded.revision,
           exported_at = excluded.exported_at, keys = excluded.keys, error = NULL, attempts = 0, next_try = 0,
           failed_revision = NULL`,
      )
      .run(org, session, revision, at, JSON.stringify([...keys].sort()));
  }

  /**
   * A failed export keeps its old keys plus every key it may have written
   * before failing (so a later erasure still finds them), and is retried with
   * a backoff of at most 4 h, or at once when Neon's revision moves again.
   */
  markFailed(org: string, session: string, code: string, keys: string[], revision: number, at = Date.now()) {
    const prior = this.exported(org, session);
    const attempts = (prior?.attempts ?? 0) + 1;
    // Waiting for the eraser key is not a failure of the group: retry hourly, uncapped.
    const nextTry = at + (code === "eraser_unavailable" ? 1 : Math.min(4, 2 ** (attempts - 1))) * 60 * 60 * 1000;
    const all = JSON.stringify([...new Set([...(prior?.keys ?? []), ...keys])].sort());
    this.db
      .prepare(
        `INSERT INTO exported (org_id, session_id, revision, exported_at, keys, error, attempts, next_try, failed_revision)
         VALUES (?, ?, -1, 0, ?, ?, ?, ?, ?)
         ON CONFLICT (org_id, session_id) DO UPDATE SET keys = excluded.keys, error = excluded.error,
           attempts = excluded.attempts, next_try = excluded.next_try, failed_revision = excluded.failed_revision`,
      )
      .run(org, session, all, code, attempts, nextTry, revision);
  }

  /**
   * The truncation ledger: every exported manifest that holds a string cut to
   * the scrub bound, so it can be re-exported in full once the sealer takes
   * large strings. Replaced on each export of the group; T0 waits on it being empty.
   */
  recordTruncations(org: string, session: string, byKey: { key: string; strings: number }[], at = Date.now()) {
    this.#tx(() => {
      this.db.prepare("DELETE FROM truncations WHERE org_id = ? AND session_id = ?").run(org, session);
      const q = this.db.prepare(
        "INSERT INTO truncations (org_id, session_id, manifest_key, strings, recorded_at) VALUES (?, ?, ?, ?, ?)",
      );
      for (const t of byKey) if (t.strings > 0) q.run(org, session, t.key, t.strings, at);
    });
  }

  truncationTotals(): { sessions: number; manifests: number; strings: number } {
    const r = this.db
      .prepare("SELECT count(DISTINCT org_id || '/' || session_id) AS s, count(*) AS m, coalesce(sum(strings), 0) AS n FROM truncations")
      .get() as { s: number; m: number; n: number };
    return { sessions: Number(r.s), manifests: Number(r.m), strings: Number(r.n) };
  }

  /**
   * A deletion fence (tombstone or reset) changed without bumping the
   * session's revision: make the exported group look changed, past its debounce.
   */
  forceChanged(groups: { org_id: string; session_id: string }[]): number {
    const ok = this.db.prepare(
      `UPDATE exported SET revision = -2, exported_at = 0
        WHERE org_id = ? AND session_id = ? AND error IS NULL AND keys <> '[]' AND revision <> -2`,
    );
    // A failed group (even past its attempt cap) retries at once.
    const failed = this.db.prepare(
      `UPDATE exported SET failed_revision = -2, attempts = 0, next_try = 0
        WHERE org_id = ? AND session_id = ? AND error IS NOT NULL AND keys <> '[]'
          AND coalesce(failed_revision, 0) <> -2`,
    );
    let n = 0;
    this.#tx(() => {
      for (const g of groups)
        n += Number(ok.run(g.org_id, g.session_id).changes) + Number(failed.run(g.org_id, g.session_id).changes);
    });
    return n;
  }

  /**
   * The next groups to export, in priority order:
   *  1. gone from Neon but still holding exported objects (erasure);
   *  2. changed since their export and past their debounce (bigger sessions wait longer, at most 24 h);
   *  3. never exported, active in the last `recentMs`, newest first;
   *  4. never exported, the rest of history, newest first;
   *  5. failed ones due for a retry, or whose revision moved since;
   *  6. when nothing else waits, exports older than `reverifyMs`, oldest first.
   */
  nextWork(now: number, recentMs: number, limit: number, reverifyMs = 0): WorkItem[] {
    const out: WorkItem[] = [];
    const take = (sql: string, kind: WorkItem["kind"], ...params: (number | string)[]) => {
      if (out.length >= limit) return;
      const rows = this.db.prepare(sql).all(...params, limit - out.length) as Omit<WorkItem, "kind">[];
      for (const r of rows) out.push({ ...r, kind });
    };
    take(
      `SELECT e.org_id, e.session_id, -1 AS revision, 0 AS events FROM exported e
        LEFT JOIN groups g USING (org_id, session_id)
        WHERE g.org_id IS NULL AND e.keys <> '[]' AND e.next_try <= ? LIMIT ?`,
      "delete",
      now,
    );
    take(
      `SELECT g.org_id, g.session_id, g.revision, g.events FROM groups g JOIN exported e USING (org_id, session_id)
        WHERE e.error IS NULL AND g.revision <> e.revision
          AND e.exported_at + min(86400000, 1800000 + g.events * 600) <= ?
        ORDER BY g.last_ms DESC NULLS FIRST LIMIT ?`,
      "changed",
      now,
    );
    take(
      `SELECT g.org_id, g.session_id, g.revision, g.events FROM groups g
        LEFT JOIN exported e USING (org_id, session_id)
        WHERE e.org_id IS NULL AND (g.last_ms IS NULL OR g.last_ms >= ?)
        ORDER BY g.last_ms DESC NULLS LAST LIMIT ?`,
      "recent",
      now - recentMs,
    );
    take(
      `SELECT g.org_id, g.session_id, g.revision, g.events FROM groups g
        LEFT JOIN exported e USING (org_id, session_id)
        WHERE e.org_id IS NULL AND g.last_ms < ? ORDER BY g.last_ms DESC LIMIT ?`,
      "history",
      now - recentMs,
    );
    take(
      `SELECT g.org_id, g.session_id, g.revision, g.events FROM groups g JOIN exported e USING (org_id, session_id)
        WHERE e.error IS NOT NULL
          AND ((e.next_try <= ? AND (e.attempts < 6 OR e.error = 'eraser_unavailable'))
               OR g.revision <> coalesce(e.failed_revision, g.revision))
        ORDER BY e.next_try LIMIT ?`,
      "retry",
      now,
    );
    // Last: a rolling re-read of old exports, for changes Neon records without a
    // revision bump (delivery records that never project to an event). Only when
    // nothing else is waiting; a re-read of an unchanged session writes nothing.
    if (out.length === 0 && reverifyMs > 0)
      take(
        `SELECT g.org_id, g.session_id, g.revision, g.events FROM groups g JOIN exported e USING (org_id, session_id)
          WHERE e.error IS NULL AND e.exported_at < ? ORDER BY e.exported_at LIMIT ?`,
        "reverify",
        now - reverifyMs,
      );
    // A group can match two queries only across them (e.g. never twice); dedupe defensively.
    const seen = new Set<string>();
    return out.filter((w) => {
      const k = `${w.org_id}\u0000${w.session_id}`;
      if (seen.has(k)) return false;
      seen.add(k);
      return true;
    });
  }

  backlog(now: number, recentMs: number): Backlog {
    const one = (sql: string, ...p: number[]) =>
      Object.values(this.db.prepare(sql).get(...p) as Record<string, number>).map((v) => Number(v ?? 0));
    const [groups, exported] = one(
      `SELECT (SELECT count(*) FROM groups), (SELECT count(*) FROM exported WHERE error IS NULL)`,
    );
    const [pending, pendingEvents] = one(
      `SELECT count(*), sum(g.events) FROM groups g LEFT JOIN exported e USING (org_id, session_id) WHERE e.org_id IS NULL`,
    );
    const [recentPending, recentPendingEvents] = one(
      `SELECT count(*), sum(g.events) FROM groups g LEFT JOIN exported e USING (org_id, session_id)
        WHERE e.org_id IS NULL AND (g.last_ms IS NULL OR g.last_ms >= ?)`,
      now - recentMs,
    );
    const [changed] = one(
      `SELECT count(*) FROM groups g JOIN exported e USING (org_id, session_id) WHERE e.error IS NULL AND g.revision <> e.revision`,
    );
    const [errored] = one(`SELECT count(*) FROM exported WHERE error IS NOT NULL`);
    const [pendingDeletes] = one(
      `SELECT count(*) FROM exported e LEFT JOIN groups g USING (org_id, session_id) WHERE g.org_id IS NULL AND e.keys <> '[]'`,
    );
    return {
      groups: groups!,
      exported: exported!,
      pending: pending!,
      pendingEvents: pendingEvents!,
      recentPending: recentPending!,
      recentPendingEvents: recentPendingEvents!,
      changed: changed!,
      errored: errored!,
      pendingDeletes: pendingDeletes!,
    };
  }

  errorCounts(): Record<string, number> {
    const rows = this.db.prepare("SELECT error, count(*) AS n FROM exported WHERE error IS NOT NULL GROUP BY error").all() as {
      error: string;
      n: number;
    }[];
    return Object.fromEntries(rows.map((r) => [r.error, r.n]));
  }

  close() {
    this.db.close();
  }
}
