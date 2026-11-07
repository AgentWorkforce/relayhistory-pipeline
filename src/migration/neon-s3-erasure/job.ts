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
 * It reads the privacy authority (read-only) and the shared watermark, never object content. The
 * authority is Neon today, or Supabase once C3 activated it (`./authority.ts`, S3X_PRIVACY_AUTHORITY).
 * A session prefix is leased while it is erased, so the exporter never reuses
 * a chunk the eraser is deleting.
 */
import type { Queryable } from "../relayhistory-export.ts";
import { orgPrefix } from "../neon-s3-sync/export-session.ts";
import type { LiveFencedGroup } from "../neon-s3-sync/neon-reader.ts";
import { isErasureAuthority, neonAuthority, type ErasureAuthority, type ErasurePresence } from "./authority.ts";
import { liveObjectPrefix, livePointerPrefix, liveSessionsIn, liveStagedPrefix, liveTargets } from "./live.ts";

/** Clock-skew margin between this host and S3 `LastModified`. */
export const LIVE_SKEW_MS = 2 * 60 * 1000;
/**
 * A staged batch (session ingest) holds several sessions, and its fan-out may
 * still be retrying or waiting for a DLQ redrive (runbooks/session-ingest-rollout.md:
 * redrive within 24 h). When a session of its (workspace, user, origin) is
 * tombstoned, its staged objects older than this are erased; younger ones on a
 * later sweep. The session's own manifests and chunks are erased at once, as before.
 */
export const STAGED_ERASE_AFTER_MS = 48 * 60 * 60 * 1000;

const DOT_SEGMENT = /(^|\/)\.{1,2}(\/|$)/;
import type { SyncState } from "../neon-s3-sync/state.ts";
import { errorCode, type Log } from "../neon-s3-sync/sync.ts";
import { inSessionAnyDate, type EraseCheckpoint, type Eraser, type ListedVersion, type PagedEraser } from "./eraser.ts";
import { createHash } from "node:crypto";

/** Per-item budget for one bounded erase step (the shared primitive also caps versions at 10k and pages at 100). */
export const ERASE_STEP_DEADLINE_MS = 60_000;
/** A durable scope for the shared paged eraser's checkpoint: operation + authority + tenant/session (or queued item). */
export function eraseScope(parts: readonly (string | number)[]): string {
  return createHash("sha256").update(JSON.stringify(["e3-erase-scope-v1", ...parts])).digest("hex");
}
const refusal = (code: string) => Object.assign(new Error(code), { code });

export interface ErasureConfig {
  keyRoot: string;
  sweepEveryMs: number;
  idleSleepMs: number;
  progressEveryMs: number;
  /** Logged as a breach when the oldest queued erasure is older than this. */
  slaWarnMs: number;
  /**
   * Whether the existence guard also counts legacy-only evidence (conversation
   * turns, links, outcomes). `S3X_ERASER_RECOGNIZES_LEGACY`; enable before the
   * exporter's `S3X_INCLUDE_LEGACY_SOURCES`.
   */
  recognizeLegacy: boolean;
}

export const DEFAULT_ERASURE_CONFIG: ErasureConfig = {
  keyRoot: "96bulls",
  sweepEveryMs: 10 * 60 * 1000,
  idleSleepMs: 30 * 1000,
  progressEveryMs: 60 * 1000,
  slaWarnMs: 24 * 60 * 60 * 1000,
  recognizeLegacy: false,
};

const SESSION_PREFIX = /^[^/]+\/[^/]+\/v1\/ws=[^/]+\/m=[^/]+\/h=[^/]+\/d=\d{4}\/\d{2}\/\d{2}\/s=[^/]+\/$/;
const LEASE_HOLDER = `eraser:${process.pid}`;

/** The session prefix a queued key belongs to, if the key has this layout under the org's prefix. */
export function sessionPrefixOf(keyRoot: string, orgId: string, key: string): string | undefined {
  const root = `${orgPrefix(keyRoot, orgId)}/v1/`;
  if (!key.startsWith(root) || DOT_SEGMENT.test(key)) return undefined;
  const m = /^(.*?\/s=[^/]+\/)/.exec(key);
  return m && SESSION_PREFIX.test(m[1]!) ? m[1] : undefined;
}

export class ErasureJob {
  readonly counters = { swept: 0, prefixesErased: 0, objectsErased: 0, versionsDeleted: 0, failed: 0, refused: 0, deferred: 0, fenceSweepFailed: 0, liveVersionsDeleted: 0, liveFenceFailed: 0, resumable: 0, unverified: 0 };
  #lastSweep = 0;
  #lastProgress = 0;
  readonly authority: ErasureAuthority;

  constructor(
    /** A Neon connection (today's authority) or an explicit `ErasureAuthority`. */
    db: Queryable | ErasureAuthority,
    readonly state: SyncState,
    readonly eraser: Eraser & PagedEraser,
    readonly log: Log,
    readonly cfg: ErasureConfig = DEFAULT_ERASURE_CONFIG,
    readonly now: () => number = Date.now,
    readonly sleep: (ms: number) => Promise<void> = (ms) => new Promise((r) => setTimeout(r, ms)),
    readonly stopped: () => boolean = () => false,
  ) {
    this.authority = isErasureAuthority(db) ? db : neonAuthority(db);
  }

  /** Queue the session prefixes of `keys` (manifest keys) and drop them from the group's watermark. */
  #queueGroup(org: string, session: string, keys: string[], reason: string) {
    this.state.enqueueErasure(
      keys.map((k) => ({ key: k.slice(0, -"manifest.json".length), kind: "prefix" as const, org_id: org, reason })),
      this.now(),
    );
    this.state.forgetKeys(org, session, keys);
  }

  /** With `dryRun`, reports what it would erase and changes nothing. */
  async sweep(opts: { dryRun?: boolean } = {}) {
    const startedAt = this.now();
    const authority = this.authority;
    try {
      await authority.ready();
    } catch (error) {
      // Not the authority right now: erase nothing at all (a stale fence could destroy a re-enabled session).
      this.counters.fenceSweepFailed += 1;
      this.log({ message: "sweep refused: privacy authority not ready", authority: authority.kind, error_code: errorCode(error) });
      return;
    }
    const waiters = await authority.lockWaiters();
    if (waiters.nonAdvisory > 3 || waiters.total > 25) {
      this.log({ message: "sweep deferred: lock waiters", lock_waiters: waiters.total });
      return;
    }
    // Live (srv_) erasure first and on its own: it reads only tombstones, so
    // a failure or an early return in the rollup and catalog walks below
    // must not hold it back.
    const live = await this.#eraseLiveFenced(opts.dryRun ?? false).catch((error) => {
      this.counters.liveFenceFailed += 1;
      this.log({ message: "live fence sweep failed", error_code: errorCode(error) });
      return { groups: 0, versions: 0 };
    });
    // Neon only: groups whose rollups and catalog rows vanished from Neon. Under the Supabase authority every
    // deletion is a fence row (a reset fences each owned session), so there is no presence sweep.
    let id = Number(this.state.meta("erasure_sweep_done") ?? 0);
    let seen = 0;
    let gone = 0;
    if (authority.presence) {
      const r = await this.#presenceSweep(authority.presence, startedAt, opts.dryRun ?? false);
      if (!r) return;
      ({ id, seen, gone } = r);
    }
    let fenced = 0;
    try {
      for (const g of await authority.fencedGroups()) {
        const row = this.state.exported(g.org_id, g.session_id);
        if (row?.keys.length) {
          if (!opts.dryRun) this.#queueGroup(g.org_id, g.session_id, row.keys, "fenced");
          fenced += 1;
        }
      }
    } catch (error) {
      this.counters.fenceSweepFailed += 1;
      this.log({ message: "fence sweep failed", error_code: errorCode(error) });
    }
    this.counters.swept += 1;
    if (!opts.dryRun) this.state.setMeta("erasure_sweep_at", String(this.now()));
    this.log({ message: opts.dryRun ? "erasure sweep (dry run)" : "erasure sweep", authority: authority.kind, sweep: id, count: seen, gone, fenced, live_fenced: live.groups, live_versions: live.versions });
  }

  /** The Neon presence sweep. Undefined: stopped, or a broken read (saw nothing); nothing is erased then. */
  async #presenceSweep(presence: ErasurePresence, startedAt: number, dryRun: boolean): Promise<{ id: number; seen: number; gone: number } | undefined> {
    const id = Number(this.state.meta("erasure_sweep_done") ?? 0) + 1;
    let after: { org_id: string; session_id: string } | null = null;
    let seen = 0;
    for (;;) {
      const rows = await presence.activityPage(after);
      // A dry run marks under a sweep id no real sweep uses (and the next real sweep clears).
      this.state.seeForErasure(dryRun ? -id : id, rows);
      seen += rows.length;
      if (rows.length === 0 || this.stopped()) break;
      after = rows.at(-1)!;
    }
    // Catalog sessions count as present too: the exporter exports them.
    let cat: Parameters<ErasurePresence["catalogPage"]>[0] = null;
    for (;;) {
      if (this.stopped()) break;
      const rows = await presence.catalogPage(cat);
      this.state.seeForErasure(dryRun ? -id : id, rows);
      seen += rows.length;
      if (rows.length === 0) break;
      cat = rows.at(-1)!;
    }
    if (this.stopped()) return undefined;
    // A sweep that saw nothing at all is a broken read, not an empty Neon: never erase on it.
    if (seen === 0) {
      this.log({ message: "sweep saw no rollups; nothing erased" });
      return undefined;
    }
    if (!dryRun) this.state.setMeta("erasure_sweep_done", String(id));
    let gone = 0;
    for (const g of this.state.vanishedExports(dryRun ? -id : id, startedAt, !dryRun)) {
      // Confirm on Neon right before erasing: no rollup and no catalog row now
      // (and, with recognizeLegacy, no legacy turn, link or outcome row).
      if (await presence.groupExists(g.org_id, g.session_id, this.cfg.recognizeLegacy)) continue;
      gone += 1;
      if (!dryRun) this.#queueGroup(g.org_id, g.session_id, g.keys, "gone_from_neon");
    }
    return { id, seen, gone };
  }

  /**
   * Tombstoned sessions written by relayhistory-api's live writer (live.ts),
   * erased directly on every sweep, not queued: the exporter never writes
   * or leases `m=srv_` prefixes.
   *
   * Cost: tombstones are paged by primary key (no cap) and grouped by
   * (workspace, user, origin); each group's objects and pointers are listed
   * once (two list calls), and only tombstones with a directory or a pointer
   * left are visited. A tombstone with nothing live (most, and every fully
   * erased one) costs a share of those two calls.
   *
   * Re-enable race: a page can be minutes old by the time a row is reached,
   * and a re-enabled session may already be re-uploading. Each tombstone is
   * re-read right before its erasure, and only versions last modified before
   * that read (less LIVE_SKEW_MS for clock skew) are erased. Anything newer is
   * kept, and so is the pointer while anything is kept, so the next sweep
   * finds the session again.
   */
  async #eraseLiveFenced(dryRun: boolean): Promise<{ groups: number; versions: number }> {
    let groups = 0;
    let versions = 0;
    let after: LiveFencedGroup | null = null;
    for (;;) {
      if (this.stopped()) break;
      let rows: LiveFencedGroup[];
      try {
        rows = await this.authority.fencedLivePage(after);
      } catch (error) {
        this.counters.liveFenceFailed += 1;
        this.log({ message: "live fence sweep failed", error_code: errorCode(error) });
        break;
      }
      if (rows.length === 0) break;
      after = rows.at(-1)!;
      // Per (workspace, user, origin), listed once per page: the session
      // directories that hold any object, and the pointers. A tombstone is
      // visited while either remains, so neither a directory left by a
      // re-upload nor an orphaned pointer is ever skipped for good.
      // A machine prefix with more versions than one strict listing allows (the shared primitive's 10k bound) is
      // "unknown" (null) rather than a failure: its tombstones are then always visited, and the paged eraser below
      // works through it across runs.
      const listed = new Map<string, { dirs: Set<string> | null; pointers: Set<string> | null; staged: string[] | null; stagedDone?: boolean } | null>();
      const bounded = async (prefix: string): Promise<string[] | null> => {
        try {
          return await this.eraser.listKeys(prefix);
        } catch (error) {
          if (errorCode(error) === "erasure_inventory") return null;
          throw error;
        }
      };
      for (const g of rows) {
        if (this.stopped()) break;
        let t;
        let objects: string;
        let pointerRoot: string;
        try {
          t = liveTargets(this.cfg.keyRoot, g);
          objects = liveObjectPrefix(this.cfg.keyRoot, g);
          pointerRoot = livePointerPrefix(this.cfg.keyRoot, g);
        } catch {
          // An id that is not one safe path segment was never written either.
          this.counters.refused += 1;
          continue;
        }
        // A failed listing is remembered for the page (null), so the group's
        // other tombstones wait for the next sweep instead of each retrying it.
        if (listed.has(objects) && listed.get(objects) === null) continue;
        let seen = listed.get(objects);
        try {
          if (!seen) {
            const dirs = await bounded(objects), pointers = await bounded(pointerRoot), staged = await bounded(liveStagedPrefix(this.cfg.keyRoot, g));
            seen = {
              dirs: dirs === null ? null : liveSessionsIn(objects, dirs),
              pointers: pointers === null ? null : new Set(pointers),
              // The session ingest's staged batches: a tombstone is revisited while any remain.
              staged: staged === null ? null : staged.filter((k) => k.endsWith(".staged.ndjson.zst")),
            };
            listed.set(objects, seen);
          }
        } catch (error) {
          listed.set(objects, null);
          this.counters.liveFenceFailed += 1;
          this.log({ message: "live listing failed", error_code: errorCode(error) });
          continue;
        }
        const dirKey = `${t.base.slice(objects.length).replace(/\/$/, "")}/${t.sessionSegment}`;
        if (seen.dirs !== null && seen.pointers !== null && seen.staged !== null
          && !seen.dirs.has(dirKey) && !seen.pointers.has(t.pointerKey) && seen.staged.length === 0) continue;
        groups += 1;
        if (dryRun) continue;
        try {
          const checkedAt = this.now();
          if (!(await this.authority.stillFenced(g))) continue; // re-enabled since the page was read
          const before = new Date(checkedAt - LIVE_SKEW_MS);
          // The session ingest's staged copies of this (workspace, user, origin)'s batches,
          // once past the fan-out redrive window. Shared by the group's tombstones: once a page.
          const stagedBefore = new Date(checkedAt - STAGED_ERASE_AFTER_MS);
          if (seen.staged !== null) {
            const stagedLeft: string[] = [];
            for (const key of seen.staged) {
              const st = await this.eraser.eraseObjectBefore(key, stagedBefore);
              versions += st.deleted;
              this.counters.liveVersionsDeleted += st.deleted;
              if (st.kept > 0) stagedLeft.push(key);
            }
            seen.staged = stagedLeft;
          } else if (!seen.stagedDone) {
            // Too many staged versions for one listing: the same retention through the paged eraser, resumable.
            seen.stagedDone = true;
            const stagedPrefix = liveStagedPrefix(this.cfg.keyRoot, g);
            const scope = eraseScope(["live-staged", this.authority.kind, g.org_id, g.workspace_id, g.user_id, g.origin_id]);
            const step = await this.eraser.eraseStep(stagedPrefix, {
              scope, checkpoint: this.state.eraseCheckpoint(scope) as EraseCheckpoint | null,
              select: (v) => v.key.endsWith(".staged.ndjson.zst") && v.lastModified !== undefined && v.lastModified < stagedBefore.getTime(),
              authorize: async () => {
                if (!(await this.authority.stillFenced(g))) throw refusal("erasure_reenabled");
              },
              budget: { deadlineAt: this.now() + ERASE_STEP_DEADLINE_MS }, now: this.now,
            });
            versions += step.deleted;
            this.counters.liveVersionsDeleted += step.deleted;
            if (step.status === "incomplete") {
              this.state.setEraseCheckpoint(scope, step.checkpoint, this.now());
              this.counters.resumable += 1;
            } else this.state.clearEraseCheckpoint(scope);
          }
          // The session's directory under the (workspace, user, origin) machine prefix, every date, through the shared
          // strict paged eraser: bounded per run, resumable from a durable checkpoint scoped to this group and the
          // authority, authority (still fenced) re-checked before every page's deletes, versions newer than the
          // re-read kept (successors), and done only after a fresh strict zero inventory.
          const scope = eraseScope(["live-session", this.authority.kind, g.org_id, g.workspace_id, g.user_id, g.origin_id, g.source, g.session_id]);
          // Successors (newer than the re-read, less clock skew) are kept; a version without a timestamp is never
          // deleted (S3 listings always carry LastModified).
          const select = (v: ListedVersion) => inSessionAnyDate(t.base, t.sessionSegment, v.key) && v.lastModified !== undefined && v.lastModified < before.getTime();
          const authorize = async () => {
            if (!(await this.authority.stillFenced(g))) throw refusal("erasure_reenabled");
          };
          const step = await this.eraser.eraseStep(t.base, {
            scope, select, authorize, checkpoint: this.state.eraseCheckpoint(scope) as EraseCheckpoint | null,
            budget: { deadlineAt: this.now() + ERASE_STEP_DEADLINE_MS }, now: this.now,
          });
          versions += step.deleted;
          this.counters.liveVersionsDeleted += step.deleted;
          if (step.status === "incomplete") {
            this.state.setEraseCheckpoint(scope, step.checkpoint, this.now());
            this.counters.resumable += 1;
            continue; // resumes from the checkpoint on the next sweep
          }
          this.state.clearEraseCheckpoint(scope);
          const proof = await this.eraser.verifyErased(t.base, { select, authorize, budget: { deadlineAt: this.now() + ERASE_STEP_DEADLINE_MS }, now: this.now });
          if (proof.status !== "erased") {
            this.counters.unverified += 1;
            continue; // not proven: the next sweep traverses again
          }
          const ptr = await this.eraser.eraseObjectBefore(t.pointerKey, before);
          versions += ptr.deleted;
          this.counters.liveVersionsDeleted += ptr.deleted;
        } catch (error) {
          // A re-enable during the run stops before any further delete; nothing to retry for it.
          if (errorCode(error) === "erasure_reenabled") continue;
          this.counters.liveFenceFailed += 1;
          this.log({ message: "live erasure failed", error_code: errorCode(error) });
        }
      }
    }
    return { groups, versions };
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
      try {
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
      } catch (error) {
        this.log({ message: "erasure state busy", error_code: errorCode(error) });
        continue;
      }
      try {
        // The exporter may have cancelled it before we took the lease.
        if (!this.state.erasureQueued(item.key, item.enqueued_at)) continue;
        if (item.kind === "object") {
          this.counters.versionsDeleted += await this.eraser.eraseObject(item.key);
          this.counters.objectsErased += 1;
          this.state.erasureDone(item.key, item.enqueued_at);
          handled += 1;
          continue;
        }
        // A session prefix: the shared strict paged eraser, resumable across runs from a durable checkpoint bound to
        // this queued item (key + enqueue time), the authority and the operation. Before every page's deletes the
        // item must still be queued, this process must still hold its lease, and the authority must still be ready.
        const scope = eraseScope(["queued-prefix", this.authority.kind, item.org_id, item.key, item.reason, item.enqueued_at]);
        const authorize = async () => {
          if (!this.state.erasureQueued(item.key, item.enqueued_at)) throw refusal("erasure_item_cancelled");
          if (!this.state.renewLease(prefix, LEASE_HOLDER, this.now())) throw refusal("erasure_lease_lost");
          await this.authority.ready();
        };
        const step = await this.eraser.eraseStep(item.key, {
          scope, authorize, checkpoint: this.state.eraseCheckpoint(scope) as EraseCheckpoint | null,
          budget: { deadlineAt: this.now() + ERASE_STEP_DEADLINE_MS }, now: this.now,
        });
        this.counters.versionsDeleted += step.deleted;
        handled += 1;
        if (step.status === "incomplete") {
          this.state.setEraseCheckpoint(scope, step.checkpoint, this.now());
          this.counters.resumable += 1;
          continue; // still queued: the next drain resumes from the checkpoint
        }
        this.state.clearEraseCheckpoint(scope);
        const proof = await this.eraser.verifyErased(item.key, { authorize, budget: { deadlineAt: this.now() + ERASE_STEP_DEADLINE_MS }, now: this.now });
        if (proof.status !== "erased") {
          this.counters.unverified += 1;
          continue; // still queued: never marked done without a fresh strict zero inventory
        }
        this.counters.prefixesErased += 1;
        this.state.erasureDone(item.key, item.enqueued_at);
      } catch (error) {
        this.counters.failed += 1;
        this.state.erasureFailed(item.key, errorCode(error), this.now());
        this.log({ message: "erasure failed", error_code: errorCode(error), kind: item.kind });
        handled += 1;
      } finally {
        try {
          this.state.releaseLeases(LEASE_HOLDER, [prefix]);
        } catch {}
      }
    }
    return handled;
  }

  progress(force = false) {
    const now = this.now();
    if (!force && now - this.#lastProgress < this.cfg.progressEveryMs) return;
    this.#lastProgress = now;
    const queue = this.state.erasureBacklog(now);
    const lastSweep = Number(this.state.meta("erasure_sweep_at") ?? 0);
    const sweepAgeH = lastSweep ? +((now - lastSweep) / 3_600_000).toFixed(2) : null;
    const stuck = this.state.stuckWithKeys();
    const breach =
      (queue.oldestAgeH !== null && queue.oldestAgeH * 3_600_000 > this.cfg.slaWarnMs) ||
      (sweepAgeH !== null && sweepAgeH > 6) ||
      stuck > 0;
    this.log({ message: breach ? "erasure SLA at risk" : "progress", ...this.counters, queue, sweep_age_h: sweepAgeH, stuck_with_keys: stuck });
  }

  async run(opts: { once?: boolean } = {}) {
    this.state.releaseStaleLeases("eraser:", LEASE_HOLDER);
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
