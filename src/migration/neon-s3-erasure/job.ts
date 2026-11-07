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
import { terminalInvocations } from "./write-obligation.ts";
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
  /**
   * TEST SEAM ONLY (never set by main.ts): whether exporter obligations may be treated as settled. Production has NO
   * value for it, so an exporter prefix or object is never marked erased (every genesis is unattested; there is no
   * unlock until a separately reviewed recovery contract exists). Tests of the completion path opt in explicitly.
   */
  obligationsSettled?: () => boolean;
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

/** One obligation-gated paged erasure's outcome (see ErasureJob#erasePaged). */
export type PagedOutcome = "incomplete" | "erased" | "unverified" | "unresolved" | "refused";

export class ErasureJob {
  readonly counters = { unresolved: 0, reservationsRefused: 0, swept: 0, prefixesErased: 0, objectsErased: 0, versionsDeleted: 0, failed: 0, refused: 0, deferred: 0, fenceSweepFailed: 0, liveVersionsDeleted: 0, liveFenceFailed: 0, resumable: 0, unverified: 0 };
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

  /**
   * One bounded, resumable, obligation-gated erasure of `prefix` through the shared strict paged eraser
   * (eraser.ts) and the shared unresolved-write model (write-obligation.ts v3), over SyncState:
   *  - resumes from the durable checkpoint bound to `scope` (a foreign scope restarts from the start);
   *  - `authorize` (the caller's still-fenced / queued / lease / authority re-check) runs before every page's deletes;
   *  - before ANY version is deleted, EVERY doomed version of the page is reserved atomically (compare-and-set on the
   *    prefix's revision, under the lock the exporter's publish takes): a writer change or an owned version refuses
   *    the page (nothing in it deleted), and a publish after the reservation is refused;
   *  - "erased" only after a fresh strict zero inventory AND no unresolved write invocation under the prefix: a
   *    dispatched PUT (unknown outcome) is never resolved by time, a zero inventory or a matching listed version;
   *    an acked one only when its exact version is absent from that complete inventory.
   */
  async #erasePaged(prefix: string, scope: string, select: ((v: ListedVersion) => boolean) | undefined, authorize: () => Promise<void>,
    keyFilter: (key: string) => boolean = () => true, exporterWritten = false, exactKey?: string,
    lease?: { prefix: string; holder: string; gen: string }): Promise<{ status: PagedOutcome; deleted: number; remaining: number; rev: number }> {
    const exact = exactKey !== undefined ? { exactKey } : {};
    let rev = this.state.obligationRev(prefix);
    let lost = false;
    const budget = () => ({ deadlineAt: this.now() + ERASE_STEP_DEADLINE_MS });
    const beforeDelete = async (doomed: readonly ListedVersion[]) => {
      if (!doomed.length) return;
      const r = this.state.reserveErase(prefix, rev, scope, doomed.map((v) => ({ key: v.key, versionId: v.versionId })));
      if (!r.ok) {
        lost = true;
        throw refusal("erasure_reservation_refused");
      }
      rev = r.rev;
    };
    // Legacy migration: a checkpoint persisted under this scope in the OTHER mode (directory vs exact-key, e.g. written
    // before the exact-key wiring) is not resumable; restart the traversal from the start instead of being refused.
    // Progress only: invocations, reservations and the terminal / erased history are untouched.
    let checkpoint = this.state.eraseCheckpoint(scope) as (EraseCheckpoint & { exactKey?: string }) | null;
    if (checkpoint && (checkpoint.scope !== scope || checkpoint.prefix !== prefix || (checkpoint.exactKey ?? null) !== (exactKey ?? null))) {
      this.state.clearEraseCheckpoint(scope);
      checkpoint = null;
    }
    const run = (cp: EraseCheckpoint | null) => this.eraser.eraseStep(prefix, {
      scope, checkpoint: cp, ...(select ? { select } : {}), authorize, beforeDelete, budget: budget(), now: this.now, ...exact,
    });
    let step;
    try {
      try {
        step = await run(checkpoint);
      } catch (error) {
        // A stale / foreign stored checkpoint (e.g. another bucket) is ignored and the traversal restarted, never a stall.
        if (!checkpoint || errorCode(error) !== "erasure_checkpoint_scope") throw error;
        this.state.clearEraseCheckpoint(scope);
        step = await run(null);
      }
    } catch (error) {
      if (lost) {
        this.counters.reservationsRefused += 1;
        return { status: "refused", deleted: 0, remaining: -1, rev };
      }
      throw error;
    }
    if (step.status === "incomplete") {
      this.state.setEraseCheckpoint(scope, step.checkpoint, this.now());
      this.counters.resumable += 1;
      return { status: "incomplete", deleted: step.deleted, remaining: -1, rev };
    }
    this.state.clearEraseCheckpoint(scope);
    // The reservation generation the proof below covers: captured BEFORE the inventory, so every reservation it can
    // retire existed when the inventory was taken (one added later stays reserved).
    const proofSeq = this.state.reservationHighWater(prefix);
    // A fresh strict COMPLETE inventory: which exact versions are present per key.
    // `present`: object versions (the invocation evidence); `listed`: EVERY listed entry INCLUDING delete markers (the
    // absence proof: a marker still listed is present, never "erased").
    const present = new Map<string, Set<string>>();
    const listed = new Map<string, Set<string>>();
    const proof = await this.eraser.verifyErased(prefix, {
      select: (v) => {
        if (keyFilter(v.key)) {
          (listed.get(v.key) ?? listed.set(v.key, new Set()).get(v.key)!).add(v.versionId);
          if (!v.deleteMarker) (present.get(v.key) ?? present.set(v.key, new Set()).get(v.key)!).add(v.versionId);
        }
        return select ? select(v) : true;
      },
      authorize, budget: budget(), now: this.now, ...exact,
    });
    let remaining = 0;
    for (const vs of present.values()) remaining += vs.size;
    if (this.state.obligationRev(prefix) !== rev) return { status: "refused", deleted: step.deleted, remaining, rev };
    // The obligations the subset depends on: those at its keys under the prefix, plus prefix-level ones covering the
    // prefix itself (an ancestor or equal: legacy imports, nonterminal), deduplicated.
    const seen = new Set<string>();
    const invocations = [...this.state.unresolvedWrites(prefix).filter((i) => keyFilter(i.key)), ...this.state.coveringWrites(prefix)].filter((i) => !seen.has(i.id) && !!seen.add(i.id));
    const owned = new Map<string, Set<string>>();
    for (const o of this.state.publishedUnder(prefix)) (owned.get(o.key) ?? owned.set(o.key, new Set()).get(o.key)!).add(o.versionId);
    const byKey = new Map<string, typeof invocations>();
    for (const i of invocations) (byKey.get(i.key) ?? byKey.set(i.key, []).get(i.key)!).push(i);
    const done: string[] = [];
    for (const [key, list] of byKey)
      done.push(...terminalInvocations(list, owned.get(key) ?? new Set(), proof.status === "erased" ? (present.get(key) ?? new Set()) : null));
    if (done.length) {
      const r = this.state.resolveWrites(prefix, rev, done);
      if (!r.ok) return { status: "refused", deleted: step.deleted, remaining, rev };
      rev = r.rev;
    }
    if (proof.status !== "erased") {
      this.counters.unverified += 1;
      return { status: "unverified", deleted: step.deleted, remaining, rev };
    }
    // A prefix the exporter writes is never done while its obligations may be incomplete: an unattested genesis (a
    // lost / recreated state file) may have forgotten outstanding PUTs, so it fails closed.
    if (invocations.length - done.length > 0 || (exporterWritten && !(this.cfg.obligationsSettled?.() ?? false))) {
      this.counters.unresolved += 1;
      return { status: "unresolved", deleted: step.deleted, remaining, rev };
    }
    // Retire EXACTLY the reserved versions this complete inventory shows absent (within keyFilter); every other
    // reservation (kept siblings, successors, other subsets) stays.
    // Bounded, resumable retirement of THIS scope's reservations proven absent (SQL-filtered by scope and exact key /
    // prefix; <= 1000 rows per transaction; durable cursor). Terminal completion only once every page is retired; at
    // the step deadline it stays incomplete and resumes from the cursor on the next run.
    const absent = (key: string, versionId: string) => keyFilter(key) && !(listed.get(key)?.has(versionId) ?? false);
    // The workset identity this retirement (and its durable cursor) is bound to: prefix, exact key, the reservation
    // generation and the COMPLETE listing it was proven against. Any change discards a saved cursor (restart).
    const proofId = createHash("sha256").update(JSON.stringify(["retire-v1", prefix, exactKey ?? null, proofSeq,
      [...listed].map(([k, vs]) => [k, [...vs].sort()]).sort((a, b) => (a[0]! < b[0]! ? -1 : a[0]! > b[0]! ? 1 : 0))])).digest("hex");
    const deadline = this.now() + ERASE_STEP_DEADLINE_MS;
    for (;;) {
      // Authority re-checked before EVERY page (async, outside the page's transaction; no network await inside it).
      await authorize();
      const page = this.state.retireReservationsPage(prefix, scope, exactKey, absent, 1000, proofSeq, proofId, rev, lease);
      // A write that landed during the awaited authority check (or since the proof) refuses the page: no done claim;
      // the next run takes a fresh proof, and the new pending effect is kept.
      if (!page.ok) {
        this.counters.reservationsRefused += 1;
        return { status: "refused", deleted: step.deleted, remaining, rev: page.rev };
      }
      rev = page.rev;
      if (page.done) break;
      if (this.now() >= deadline) {
        this.counters.resumable += 1;
        return { status: "incomplete", deleted: step.deleted, remaining, rev };
      }
    }
    // Final assertion under the CURRENT proof: no eligible (proof-absent) reservation of this scope remains (bounded,
    // read-only pages within the same deadline). Anything left, or no time to check: incomplete, never done.
    for (let after: [string, string] | null = null, first = true; first || after !== null; first = false) {
      if (this.now() >= deadline) {
        this.counters.resumable += 1;
        return { status: "incomplete", deleted: step.deleted, remaining, rev };
      }
      const pg = this.state.eligibleRemainingPage(prefix, scope, exactKey, absent, proofSeq, after);
      if (pg.eligible > 0) return { status: "incomplete", deleted: step.deleted, remaining, rev };
      after = pg.next;
    }
    return { status: "erased", deleted: step.deleted, remaining, rev };
  }

  /**
   * The same gate for ONE object key (every version at exactly `key` that `select` accepts): listed under the key's
   * directory, reserved before delete, done only after a strict empty inventory of the selected versions and no
   * unresolved PUT at exactly that key. `remaining`: versions still at the key (e.g. successors kept by `select`).
   */
  #eraseKey(key: string, scope: string, select: (v: ListedVersion) => boolean, authorize: () => Promise<void>, exporterWritten = false,
    lease?: { prefix: string; holder: string; gen: string }) {
    const dir = key.slice(0, key.lastIndexOf("/") + 1);
    if (!dir || dir === key) throw refusal("erasure_refused_shape");
    // The shared primitive's exact-key mode: listing and verification cover exactly this key (cut at the first other
    // key), so unrelated siblings never count against the budget.
    return this.#erasePaged(dir, scope, (v) => v.key === key && select(v), authorize, (k) => k === key, exporterWritten, key, lease);
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
  // UNQUALIFIED dependency: the live writer (relayhistory-api) and the ingest's staged batches are REMOTE writers
  // outside this local obligation ledger, so their unknown PUT outcomes are never settled here. Their tombstones are
  // re-swept every pass and never retired or reported complete; their own lanes must qualify their interlock.
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
              // Per staged object, through the gate (reserve before delete, strict empty, no unresolved PUT).
              const st = await this.#eraseKey(key, eraseScope(["live-staged-object", this.authority.kind, g.org_id, g.workspace_id, g.user_id, g.origin_id, key]),
                (v) => v.lastModified !== undefined && v.lastModified < stagedBefore.getTime(),
                async () => {
                  if (!(await this.authority.stillFenced(g))) throw refusal("erasure_reenabled");
                  await this.authority.ready();
                });
              versions += st.deleted;
              this.counters.liveVersionsDeleted += st.deleted;
              if (st.status !== "erased" || st.remaining > 0) stagedLeft.push(key);
            }
            seen.staged = stagedLeft;
          } else if (!seen.stagedDone) {
            // Too many staged versions for one listing: the same retention through the paged eraser, resumable.
            seen.stagedDone = true;
            const stagedPrefix = liveStagedPrefix(this.cfg.keyRoot, g);
            const scope = eraseScope(["live-staged", this.authority.kind, g.org_id, g.workspace_id, g.user_id, g.origin_id]);
            // Same obligation-gated paged erasure (every doomed version reserved first); retention, so its outcome
            // only decides whether the next sweep resumes (incomplete) or traverses again.
            const st = await this.#erasePaged(stagedPrefix, scope,
              (v) => v.key.endsWith(".staged.ndjson.zst") && v.lastModified !== undefined && v.lastModified < stagedBefore.getTime(),
              async () => {
                if (!(await this.authority.stillFenced(g))) throw refusal("erasure_reenabled");
                await this.authority.ready();
              });
            versions += st.deleted;
            this.counters.liveVersionsDeleted += st.deleted;
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
            await this.authority.ready();
          };
          const r = await this.#erasePaged(t.base, scope, select, authorize);
          versions += r.deleted;
          this.counters.liveVersionsDeleted += r.deleted;
          // incomplete: resumes from the checkpoint; unverified / unresolved / refused: the next sweep traverses again.
          if (r.status !== "erased") continue;
          // The pointer last, through the same gate (versions newer than the re-read kept).
          const ptr = await this.#eraseKey(t.pointerKey, eraseScope(["live-pointer", this.authority.kind, g.org_id, g.workspace_id, g.user_id, g.origin_id, g.source, g.session_id]),
            (v) => v.lastModified !== undefined && v.lastModified < before.getTime(), authorize);
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
        // The lease generation this eraser acquired: every retirement page and the done claim re-validate it.
        const itemLease = { prefix, holder: LEASE_HOLDER, gen: this.state.leaseGen(prefix) ?? "" };
        if (item.kind === "object") {
          // One queued object, through the SAME gate as a prefix: reserve every version before its delete, done only
          // after a strict empty inventory of the key and no unresolved PUT at exactly that key.
          const objScope = eraseScope(["queued-object", this.authority.kind, item.org_id, item.key, item.reason, item.enqueued_at, "lease:" + prefix]);
          const r = await this.#eraseKey(item.key, objScope, () => true, async () => {
            if (!this.state.erasureQueued(item.key, item.enqueued_at)) throw refusal("erasure_item_cancelled");
            if (!this.state.renewLease(prefix, LEASE_HOLDER, this.now())) throw refusal("erasure_lease_lost");
            await this.authority.ready();
          }, true, itemLease);
          this.counters.versionsDeleted += r.deleted;
          handled += 1;
          if (r.status === "unresolved") {
            this.state.erasureFailed(item.key, (this.cfg.obligationsSettled?.() ?? false) ? "erasure_write_unresolved" : "erasure_obligations_unattested", this.now());
            continue;
          }
          if (r.status !== "erased") continue;
          // The done claim re-validates the revision in the SAME transaction as the queue-row removal.
          if (!this.state.completeErasure(item.key, item.enqueued_at, item.key.slice(0, item.key.lastIndexOf("/") + 1), r.rev, itemLease)) {
            this.counters.reservationsRefused += 1;
            continue;
          }
          this.counters.objectsErased += 1;
          continue;
        }
        // A session prefix: the shared strict paged eraser, resumable across runs from a durable checkpoint bound to
        // this queued item (key + enqueue time), the authority and the operation. Before every page's deletes the
        // item must still be queued, this process must still hold its lease, and the authority must still be ready.
        // Scope: operation, authority, tenant + queued item (its enqueue time is the item's generation) and the leased resource.
        const scope = eraseScope(["queued-prefix", this.authority.kind, item.org_id, item.key, item.reason, item.enqueued_at, "lease:" + prefix]);
        const authorize = async () => {
          if (!this.state.erasureQueued(item.key, item.enqueued_at)) throw refusal("erasure_item_cancelled");
          if (!this.state.renewLease(prefix, LEASE_HOLDER, this.now())) throw refusal("erasure_lease_lost");
          await this.authority.ready();
        };
        const r = await this.#erasePaged(item.key, scope, undefined, authorize, undefined, true, undefined, itemLease);
        this.counters.versionsDeleted += r.deleted;
        handled += 1;
        // Still queued unless proven: incomplete resumes from the checkpoint; unverified / refused retry on the next
        // drain; an unresolved write (a PUT whose outcome is unknown) keeps it queued, re-swept on the bounded backoff.
        if (r.status === "unresolved") {
          this.state.erasureFailed(item.key, (this.cfg.obligationsSettled?.() ?? false) ? "erasure_write_unresolved" : "erasure_obligations_unattested", this.now());
          continue;
        }
        if (r.status !== "erased") continue;
        // The done claim re-validates the revision in the SAME transaction as the queue-row removal.
        if (!this.state.completeErasure(item.key, item.enqueued_at, item.key, r.rev, itemLease)) {
          this.counters.reservationsRefused += 1;
          continue;
        }
        this.counters.prefixesErased += 1;
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
