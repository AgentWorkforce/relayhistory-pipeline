/**
 * `neon-s3-erasure`: the only process that deletes from the transcript
 * bucket, and the only one holding the eraser key. The exporter
 * (`../neon-s3-sync`) is write-only.
 *
 * Within the 48 h erasure SLA it removes, by version id (permanently):
 *  - whole session prefixes of groups Neon no longer has: its own sweep of the
 *    rollups (the exporter's units come only from rollups, so a group with no
 *    rollup has nothing left to keep), every 10 min;
 *  - whole session prefixes of groups a deletion fence covers
 *    (`deleted_upload_sessions`, `session_resets`), every 10 min;
 *  - what the exporter queued: objects a rewritten manifest no longer
 *    references, and session prefixes it no longer produces.
 *
 * It reads Neon (read-only) and the shared watermark, never object content.
 * A session prefix is leased while it is erased, so the exporter never reuses
 * a chunk the eraser is deleting.
 */
import type { Queryable } from "../relayhistory-export.ts";
import { orgPrefix } from "../neon-s3-sync/export-session.ts";
import { activityPage, fencedGroups, lockWaiters } from "../neon-s3-sync/neon-reader.ts";
import type { SyncState } from "../neon-s3-sync/state.ts";
import { errorCode, type Log } from "../neon-s3-sync/sync.ts";
import type { Eraser } from "./eraser.ts";

export interface ErasureConfig {
  keyRoot: string;
  sweepEveryMs: number;
  idleSleepMs: number;
  progressEveryMs: number;
  /** Logged as a breach when the oldest queued erasure is older than this. */
  slaWarnMs: number;
}

export const DEFAULT_ERASURE_CONFIG: ErasureConfig = {
  keyRoot: "96bulls",
  sweepEveryMs: 10 * 60 * 1000,
  idleSleepMs: 30 * 1000,
  progressEveryMs: 60 * 1000,
  slaWarnMs: 24 * 60 * 60 * 1000,
};

const SESSION_PREFIX = /^[^/]+\/[^/]+\/v1\/ws=[^/]+\/m=[^/]+\/h=[^/]+\/d=\d{4}\/\d{2}\/\d{2}\/s=[^/]+\/$/;
const LEASE_HOLDER = `eraser:${process.pid}`;

/** The session prefix a queued key belongs to, if the key has this layout under the org's prefix. */
export function sessionPrefixOf(keyRoot: string, orgId: string, key: string): string | undefined {
  const root = `${orgPrefix(keyRoot, orgId)}/v1/`;
  if (!key.startsWith(root)) return undefined;
  const m = /^(.*?\/s=[^/]+\/)/.exec(key);
  return m && SESSION_PREFIX.test(m[1]!) ? m[1] : undefined;
}

export class ErasureJob {
  readonly counters = { swept: 0, prefixesErased: 0, objectsErased: 0, versionsDeleted: 0, failed: 0, refused: 0, deferred: 0, fenceSweepFailed: 0 };
  #lastSweep = 0;
  #lastProgress = 0;

  constructor(
    readonly db: Queryable,
    readonly state: SyncState,
    readonly eraser: Eraser,
    readonly log: Log,
    readonly cfg: ErasureConfig = DEFAULT_ERASURE_CONFIG,
    readonly now: () => number = Date.now,
    readonly sleep: (ms: number) => Promise<void> = (ms) => new Promise((r) => setTimeout(r, ms)),
    readonly stopped: () => boolean = () => false,
  ) {}

  /** Queue the session prefixes of `keys` (manifest keys) and drop them from the group's watermark. */
  #queueGroup(org: string, session: string, keys: string[], reason: string) {
    this.state.enqueueErasure(
      keys.map((k) => ({ key: k.slice(0, -"manifest.json".length), kind: "prefix" as const, org_id: org, reason })),
      this.now(),
    );
    this.state.forgetKeys(org, session, keys);
  }

  async sweep() {
    const waiters = await lockWaiters(this.db);
    if (waiters.nonAdvisory > 3 || waiters.total > 25) {
      this.log({ message: "sweep deferred: lock waiters", lock_waiters: waiters.total });
      return;
    }
    const id = Number(this.state.meta("erasure_sweep_done") ?? 0) + 1;
    let after: { org_id: string; session_id: string } | null = null;
    let seen = 0;
    for (;;) {
      const rows = await activityPage(this.db, after);
      this.state.seeForErasure(id, rows);
      seen += rows.length;
      if (rows.length === 0 || this.stopped()) break;
      after = rows.at(-1)!;
    }
    if (this.stopped()) return;
    // A sweep that saw nothing at all is a broken read, not an empty Neon: never erase on it.
    if (seen === 0) {
      this.log({ message: "sweep saw no rollups; nothing erased" });
      return;
    }
    this.state.setMeta("erasure_sweep_done", String(id));
    const gone = this.state.vanishedExports(id);
    for (const g of gone) this.#queueGroup(g.org_id, g.session_id, g.keys, "gone_from_neon");
    let fenced = 0;
    try {
      for (const g of await fencedGroups(this.db)) {
        const row = this.state.exported(g.org_id, g.session_id);
        if (row?.keys.length) {
          this.#queueGroup(g.org_id, g.session_id, row.keys, "fenced");
          fenced += 1;
        }
      }
    } catch (error) {
      this.counters.fenceSweepFailed += 1;
      this.log({ message: "fence sweep failed", error_code: errorCode(error) });
    }
    this.counters.swept += 1;
    this.log({ message: "erasure sweep", sweep: id, count: seen, gone: gone.length, fenced });
  }

  /** Erase what is queued and due. Returns how many items it erased or failed (not deferred). */
  async drain(limit = 100): Promise<number> {
    const items = this.state.nextErasures(this.now(), limit);
    let handled = 0;
    for (const item of items) {
      if (this.stopped()) break;
      const prefix =
        item.kind === "prefix"
          ? SESSION_PREFIX.test(item.key) && item.key.startsWith(`${orgPrefix(this.cfg.keyRoot, item.org_id)}/v1/`)
            ? item.key
            : undefined
          : sessionPrefixOf(this.cfg.keyRoot, item.org_id, item.key);
      if (!prefix) {
        // Never delete outside a session prefix of the item's own org.
        this.counters.refused += 1;
        this.state.erasureFailed(item.key, "erasure_refused_shape", this.now());
        handled += 1;
        continue;
      }
      if (!this.state.acquireLease(prefix, LEASE_HOLDER, undefined, this.now())) {
        this.counters.deferred += 1;
        continue;
      }
      try {
        // The exporter may have cancelled it before we took the lease.
        if (!this.state.erasureQueued(item.key, item.enqueued_at)) continue;
        const n = item.kind === "prefix" ? await this.eraser.erasePrefix(item.key) : await this.eraser.eraseObject(item.key);
        this.counters.versionsDeleted += n;
        if (item.kind === "prefix") this.counters.prefixesErased += 1;
        else this.counters.objectsErased += 1;
        this.state.erasureDone(item.key, item.enqueued_at);
        handled += 1;
      } catch (error) {
        this.counters.failed += 1;
        this.state.erasureFailed(item.key, errorCode(error), this.now());
        this.log({ message: "erasure failed", error_code: errorCode(error), kind: item.kind });
        handled += 1;
      } finally {
        this.state.releaseLeases(LEASE_HOLDER, [prefix]);
      }
    }
    return handled;
  }

  progress(force = false) {
    const now = this.now();
    if (!force && now - this.#lastProgress < this.cfg.progressEveryMs) return;
    this.#lastProgress = now;
    const queue = this.state.erasureBacklog(now);
    const breach = queue.oldestAgeH !== null && queue.oldestAgeH * 3_600_000 > this.cfg.slaWarnMs;
    this.log({ message: breach ? "erasure SLA at risk" : "progress", ...this.counters, queue });
  }

  async run(opts: { once?: boolean } = {}) {
    while (!this.stopped()) {
      if (this.now() - this.#lastSweep >= this.cfg.sweepEveryMs) {
        this.#lastSweep = this.now();
        await this.sweep();
      }
      const handled = await this.drain();
      this.progress();
      if (handled === 0) {
        if (opts.once) break;
        await this.sleep(this.cfg.idleSleepMs);
      }
    }
    this.progress(true);
  }
}
