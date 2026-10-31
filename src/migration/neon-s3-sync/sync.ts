/**
 * The continuous loop: sweep Neon for changes, pick the next groups by
 * priority (erasure, changed, last 14 days newest first, then history newest
 * first), export them, and log progress as counts only.
 */
import type { Queryable } from "../relayhistory-export.ts";
import { deleteExported, exportUnit, type ExportTarget } from "./export-session.ts";
import {
  activityPage,
  lockWaiters,
  neonUnitSource,
  revisionsPage,
  sessionUnits,
  type PageHook,
  type UnitSource,
} from "./neon-reader.ts";
import type { SyncState, WorkItem } from "./state.ts";
import type { Throttle } from "./throttle.ts";

export interface LockGuard {
  maxNonAdvisory: number;
  maxTotal: number;
  pauseMs: number;
  checkEveryMs: number;
}

export interface SyncConfig {
  recentMs: number;
  revSweepEveryMs: number;
  actSweepEveryMs: number;
  progressEveryMs: number;
  idleSleepMs: number;
  lockGuard: LockGuard;
}

export const DEFAULT_SYNC_CONFIG: SyncConfig = {
  recentMs: 14 * 24 * 60 * 60 * 1000,
  revSweepEveryMs: 2 * 60 * 1000,
  actSweepEveryMs: 30 * 60 * 1000,
  progressEveryMs: 60 * 1000,
  idleSleepMs: 30 * 1000,
  lockGuard: { maxNonAdvisory: 3, maxTotal: 25, pauseMs: 2 * 60 * 1000, checkEveryMs: 30 * 1000 },
};

export type Log = (fields: Record<string, unknown>) => void;

/** Error code only: a Postgres or store error message can quote data. */
export function errorCode(error: unknown): string {
  const e = error as { code?: unknown; name?: unknown } | null;
  if (e && typeof e.code === "string" && /^[A-Za-z0-9_.-]{1,64}$/.test(e.code)) return e.code;
  if (e && typeof e.name === "string" && /^[A-Za-z0-9_]{1,64}$/.test(e.name)) return e.name;
  return "unknown";
}

export interface Counters {
  rows: number;
  bytes: number;
  groups: number;
  groupsFailed: number;
  manifestsWritten: number;
  manifestsUnchanged: number;
  chunksSealed: number;
  chunksReused: number;
  objectsDeleted: number;
  rawBytes: number;
  storedBytesWritten: number;
  eventsDone: number;
  lockPauses: number;
  fenced: number;
  truncatedStrings: number;
}

export class Syncer {
  readonly counters: Counters = {
    rows: 0,
    bytes: 0,
    groups: 0,
    groupsFailed: 0,
    manifestsWritten: 0,
    manifestsUnchanged: 0,
    chunksSealed: 0,
    chunksReused: 0,
    objectsDeleted: 0,
    rawBytes: 0,
    storedBytesWritten: 0,
    eventsDone: 0,
    lockPauses: 0,
    fenced: 0,
    truncatedStrings: 0,
  };
  readonly started: number;
  #lastLockCheck = 0;
  #lastProgress = 0;
  readonly source: UnitSource;
  readonly hook: PageHook;
  /** Extra read lanes (own connections) that export groups in parallel. */
  readonly lanes: { db: Queryable; source: UnitSource }[];

  constructor(
    readonly db: Queryable,
    readonly state: SyncState,
    readonly target: ExportTarget,
    readonly throttle: Throttle,
    readonly log: Log,
    readonly cfg: SyncConfig = DEFAULT_SYNC_CONFIG,
    readonly stopped: () => boolean = () => false,
    sourceFor: (db: Queryable, hook: PageHook) => UnitSource = neonUnitSource,
    extraDbs: Queryable[] = [],
  ) {
    this.started = throttle.now();
    this.hook = async (rows, bytes) => {
      this.counters.rows += rows;
      this.counters.bytes += bytes;
      await this.throttle.take(rows, bytes);
      await this.guardLocks();
    };
    this.source = sourceFor(db, this.hook);
    this.lanes = [{ db, source: this.source }, ...extraDbs.map((d) => ({ db: d, source: sourceFor(d, this.hook) }))];
  }

  /** Pause while Neon shows lock waiters beyond the guard (another backfill, a convoy). */
  async guardLocks(force = false) {
    const g = this.cfg.lockGuard;
    const now = this.throttle.now();
    if (!force && now - this.#lastLockCheck < g.checkEveryMs) return;
    this.#lastLockCheck = now;
    for (;;) {
      const w = await lockWaiters(this.db);
      if (w.nonAdvisory <= g.maxNonAdvisory && w.total <= g.maxTotal) return;
      this.counters.lockPauses += 1;
      this.log({ message: "paused: lock waiters", lock_waiters: w.total, non_advisory: w.nonAdvisory, pause_ms: g.pauseMs });
      if (this.stopped()) return;
      await this.throttle.sleep(g.pauseMs);
    }
  }

  #sweepId(k: string) {
    return Number(this.state.meta(k) ?? 0);
  }

  async sweepRevisions() {
    const id = this.#sweepId("rev_sweep_done") + 1;
    let after: { org_id: string; session_id: string } | null = null;
    let n = 0;
    for (;;) {
      const rows = await revisionsPage(this.db, after);
      // A sweep page is a narrow index-only read: weigh it lightly against the row budget.
      await this.hook(Math.ceil(rows.length / 50), 0);
      this.state.seeRevisions(id, rows);
      n += rows.length;
      if (rows.length === 0 || this.stopped()) break;
      after = rows.at(-1)!;
    }
    if (this.stopped()) return;
    this.state.setMeta("rev_sweep_done", String(id));
    this.state.setMeta("rev_sweep_at", String(this.throttle.now()));
    this.#dropUnseen();
    this.log({ message: "revision sweep", sweep: id, count: n });
  }

  async sweepActivity() {
    const id = this.#sweepId("act_sweep_done") + 1;
    let after: { org_id: string; session_id: string } | null = null;
    let n = 0;
    for (;;) {
      const rows = await activityPage(this.db, after);
      await this.hook(Math.ceil(rows.length / 50), 0);
      this.state.seeActivity(id, rows);
      n += rows.length;
      if (rows.length === 0 || this.stopped()) break;
      after = rows.at(-1)!;
    }
    if (this.stopped()) return;
    this.state.setMeta("act_sweep_done", String(id));
    this.state.setMeta("act_sweep_at", String(this.throttle.now()));
    this.#dropUnseen();
    this.log({ message: "activity sweep", sweep: id, count: n });
  }

  #dropUnseen() {
    const r = this.#sweepId("rev_sweep_done");
    const a = this.#sweepId("act_sweep_done");
    if (r > 0 && a > 0) {
      const dropped = this.state.dropUnseen(r, a);
      if (dropped) this.log({ message: "gone from neon", count: dropped });
    }
  }

  async sweepsIfDue() {
    const now = this.throttle.now();
    if (now - Number(this.state.meta("act_sweep_at") ?? 0) >= this.cfg.actSweepEveryMs) await this.sweepActivity();
    if (now - Number(this.state.meta("rev_sweep_at") ?? 0) >= this.cfg.revSweepEveryMs) await this.sweepRevisions();
  }

  /** Export (or erase) one `(org, session)` group. Never throws. */
  async processGroup(work: WorkItem, lane = this.lanes[0]!): Promise<boolean> {
    const prior = this.state.exported(work.org_id, work.session_id);
    const t0 = this.throttle.now();
    try {
      const keys: string[] = [];
      if (work.kind !== "delete") {
        const units = await sessionUnits(lane.db, work.org_id, work.session_id);
        await this.hook(units.length, 0);
        for (const unit of units) {
          const r = await exportUnit(this.target, unit, lane.source);
          if (r.fenced) this.counters.fenced += 1;
          for (const m of r.manifests) {
            keys.push(m.manifestKey);
            if (m.written) {
              this.counters.manifestsWritten += 1;
            } else this.counters.manifestsUnchanged += 1;
            this.counters.chunksSealed += m.chunksSealed;
            this.counters.chunksReused += m.chunksReused;
            this.counters.objectsDeleted += m.deletedObjects;
            this.counters.rawBytes += m.rawBytes;
            this.counters.truncatedStrings += m.truncatedStrings;
            if (m.chunksSealed) this.counters.storedBytesWritten += m.storedBytes;
          }
        }
      }
      const keep = new Set(keys);
      for (const key of prior?.keys ?? [])
        if (!keep.has(key)) this.counters.objectsDeleted += await deleteExported(this.target, work.org_id, key);
      this.state.markExported(work.org_id, work.session_id, work.revision, keys);
      this.counters.groups += 1;
      this.counters.eventsDone += work.events;
      if (work.events >= 20_000)
        this.log({ message: "large group done", kind: work.kind, count: work.events, duration_ms: this.throttle.now() - t0 });
      return true;
    } catch (error) {
      const code = errorCode(error);
      this.state.markFailed(work.org_id, work.session_id, code);
      this.counters.groupsFailed += 1;
      this.log({ message: "group failed", error_code: code, kind: work.kind });
      if (isConnectionError(error)) throw error;
      return false;
    }
  }

  progress(force = false) {
    const now = this.throttle.now();
    if (!force && now - this.#lastProgress < this.cfg.progressEveryMs) return;
    this.#lastProgress = now;
    const b = this.state.backlog(now, this.cfg.recentMs);
    const secs = Math.max(1, (now - this.started) / 1000);
    const eventsPerSec = this.counters.eventsDone / secs;
    const eta = (events: number) => (eventsPerSec > 0 ? +(events / eventsPerSec / 3600).toFixed(1) : null);
    const rates = this.throttle.rates(now);
    this.log({
      message: "progress",
      slow_window: rates.slow,
      rows_per_sec_cap: rates.rowsPerSec,
      rows_per_sec: +(this.counters.rows / secs).toFixed(1),
      mb_per_sec_read: +(this.counters.bytes / secs / 1e6).toFixed(2),
      events_per_sec: +eventsPerSec.toFixed(1),
      ...this.counters,
      backlog: b,
      eta_recent_h: eta(b.recentPendingEvents),
      eta_all_h: eta(b.pendingEvents),
      errors: this.state.errorCounts(),
    });
  }

  /** Run until stopped (or, with `once`, until there is no work left). */
  async run(opts: { once?: boolean; maxGroups?: number } = {}) {
    if (!this.state.meta("act_sweep_done")) await this.sweepActivity();
    if (!this.state.meta("rev_sweep_done")) await this.sweepRevisions();
    let done = 0;
    while (!this.stopped()) {
      await this.sweepsIfDue();
      const work = this.state.nextWork(this.throttle.now(), this.cfg.recentMs, 25);
      if (work.length === 0) {
        this.progress();
        if (opts.once) break;
        await this.throttle.sleep(this.cfg.idleSleepMs);
        continue;
      }
      const queue = [...work];
      let fatal: unknown;
      await Promise.all(
        this.lanes.map(async (lane) => {
          for (let item = queue.shift(); item && !this.stopped() && fatal === undefined; item = queue.shift()) {
            try {
              await this.processGroup(item, lane);
            } catch (error) {
              fatal = error;
              return;
            }
            done += 1;
            this.progress();
          }
        }),
      );
      if (fatal !== undefined) throw fatal;
      if (opts.maxGroups !== undefined && done >= opts.maxGroups) break;
      // Sweeps interleave with long backlogs so changes and erasures are seen.
      await this.sweepsIfDue();
    }
    this.progress(true);
  }
}

export function isConnectionError(error: unknown): boolean {
  const code = (error as { code?: unknown } | null)?.code;
  const message = String((error as { message?: unknown } | null)?.message ?? "");
  return (
    code === "ECONNRESET" ||
    code === "EPIPE" ||
    code === "57P01" ||
    code === "08006" ||
    code === "08003" ||
    /Connection terminated|Client has encountered a connection error/.test(message)
  );
}
