/**
 * The staged reconcile (Phase 3 large-session search, slice 2a): the streaming reconcile of
 * search-index.ts (`streamedReconcile`) with its diff state in Postgres (migration
 * 20261105062110) instead of in memory, committed in batches under a run lease, so a crash
 * resumes at the committed cursor once the lease expires instead of starting over.
 *
 * Behind SEARCH_INDEX_STAGED_RECONCILE_ENABLED (default off). This module reads no
 * environment and imports nothing from the consumer: every source read, index read and
 * write is a callback the caller injects (`StagedReconcileOps`), every transaction comes
 * from `deps.transact`. In this slice nothing calls it at runtime.
 *
 * Phases (sessions.search_reconcile_runs_v1.phase), each batch its own committed transaction:
 *   want   passes 1-3 (the caller's `readSource`): the in-window index identities, staged in
 *          key order; the cursor is the last key staged. Then the regenerable kinds are
 *          recorded and the run moves on.
 *   have   the index scan (`scanPage`), one keyset page per transaction; the cursor is the
 *          scan's own. A truncated scan abandons the run: the scope owes a pass.
 *   write  pass 4: pages of `diff_page('insert')` (the keys the index lacks or holds stale),
 *          regenerated (`regenerate`) and written in the same transaction that moves the
 *          cursor and the written/wanted counters.
 *   final  one transaction: the scope advisory lock, the run row, the source stamp re-read and
 *          compared (changed: the run is abandoned and restarts), the canonical advisory
 *          lock (the same keys and order as indexSearchScope; busy releases the lease), the
 *          read if this attempt has none yet, then, only when every
 *          wanted row was written, `diff_page('delete')` pages through `remove`, then
 *          `commit` (directory events, catalog, search state), then the run and its stage
 *          rows go (`finish`). A throw anywhere in it rolls all of it back.
 *
 * The lease: every call renews it; its expiry only lets another tick take the run over (a new
 * nonce), so a long source read keeps the run unless someone did. A taken-over, finished,
 * abandoned or erased run answers RH062 to the old holder.
 *
 * Passes 1-3 are not staged in this slice (the stage table reserves the 'fold' and
 * 'directory_seen' sides for that): a resumed run that still needs the read (staging
 * 'want', pass 4, or the final directory write) runs `readSource` again. Resuming saves the
 * staging already committed, the index scan already paged, and pass 4 writes already made.
 *
 * Nothing here logs content; errors carry codes only.
 */
import { createHash } from "node:crypto";
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import { scopeLockKey, type SessionScope } from "./pointer.ts";
import type { RelayhistoryStreamRecord } from "../../packages/pipeline/src/lib/pipeline/relayhistory-transform.ts";

export type Transact = <T>(work: (tx: SqlExecutor) => Promise<T>) => Promise<T>;

/** SQLSTATE of a call under a stale lease (expired, taken over, finished, abandoned or erased). */
export const STAGED_RECONCILE_STALE_LEASE = "RH062";
/** Kept in sync with search-index.ts BUFFERED_FINGERPRINT_PREFIX. */
export const STAGED_RECONCILE_KEEP_PREFIX = "bf";

const SEP = "\u001f";
const PHASES = ["want", "have", "write", "final"] as const;
export type StagedPhase = (typeof PHASES)[number];

/** What a run was started from; any change abandons it. */
export interface SourceStamp {
  canonicalId: string;
  /** The directory event_projection_epoch the pointer snapshot read (search-index.ts livePointerSnapshot). */
  epoch: string;
  /** `admittedManifestFingerprint` of the admitted manifests. */
  manifestFingerprint: string;
}

/** One in-window index identity passes 1-3 want, by its stream index key (streamIndexKey). */
export interface StagedWant {
  key: string;
  sessionId: string;
  workspaceId: string;
  tsMs: number;
  fingerprint: string;
}

/** One index row the scan found, with its stream index key. */
export interface StagedIdentity {
  key: string;
  machineId: string;
  sessionId: string;
  source: string;
  kind: string;
  eventId: string;
  workspaceId: string;
  tsMs: number;
  fingerprint: string;
}

/** The caller's side of a staged run. `R` is the caller's passes 1-3 result. */
export interface StagedReconcileOps<R> {
  /** Passes 1-3. Called lazily, at most once per attempt. null: not complete, the scope owes a pass. */
  readSource(): Promise<R | null>;
  /** Every in-window identity the read wants (any order; the module stages them in key order). */
  wanted(read: R): Iterable<StagedWant>;
  /** The kinds a delete may remove: S3_ACTIVITY_KINDS and the read's in-window kinds. */
  regenerableKinds(read: R): Iterable<string>;
  /**
   * One keyset page of the scope's index rows after `after` (null: the first). `next` null: the
   * last page; otherwise it must sort after `after` (C order).
   */
  scanPage(tx: SqlExecutor, after: string | null, limit: number): Promise<{ rows: StagedIdentity[]; next: string | null; truncated?: boolean }>;
  /** Pass 4: write the rows of these keys on `tx`; returns the rows the index accepted. */
  regenerate(tx: SqlExecutor, read: R, keys: ReadonlySet<string>, emit?: (rows: readonly unknown[]) => void): Promise<number>;
  /**
   * Inside the claim's transaction, after a claim succeeded (this attempt holds the run): e.g.
   * record the scope as owing a pass, so a crash leaves it for the next reconcile tick.
   */
  claimed?(tx: SqlExecutor): Promise<void>;
  /**
   * Only with `hooks.onBatch`: the facts pass. One source pass handing the whole-read facts input
   * (post-fold records and every projection row) to `emit` in source order, at most `chunk` items per
   * call, awaiting each.
   */
  factsPass?(read: R, chunk: number, emit: (events: StagedFactsEvents) => Promise<void>): Promise<void>;
  /** Only with `hooks.onFinish`: the source read's own completeness (default true: an incomplete read never reaches the final step). */
  sourceComplete?(read: R): boolean;
  /** The scope's current source stamp, read on `tx` under the scope lock. */
  currentSource(tx: SqlExecutor): Promise<SourceStamp>;
  /** Remove these index identities on `tx`; returns the rows removed. */
  remove(tx: SqlExecutor, identities: StagedIdentity[]): Promise<number>;
  /**
   * Last, on the final transaction: directory events, catalog and search state. `complete`
   * is false when a pass 4 write was refused (nothing was deleted then). Returns whether the
   * scope is now current, with any reasons of its own.
   */
  commit(tx: SqlExecutor, read: R, pass: { complete: boolean; inserted: number; deleted: number; wanted: number }): Promise<{ complete: boolean; reasons: string[] }>;
}

/** The run a hook is called for: its id and the lease nonce it holds (the writers' arguments). */
export interface StagedRunRef {
  runId: string;
  nonce: string;
}

/**
 * A "facts" chunk's events, in the shape outputFactsFromScope reads (output-facts.ts): the projection
 * rows with their table names (convergence_events, session_commit_links, ...) and the stream
 * records, both AFTER the fold (the winners readCanonicalScope would produce), never pre-fold copies.
 * Concatenated over every chunk of the pass they equal the whole read's projectionRows and records.
 */
export interface StagedFactsEvents {
  projectionRows: ReadonlyArray<{ table: string; row: Record<string, unknown> }>;
  records: readonly RelayhistoryStreamRecord[];
}

/** Where a "facts" chunk sits in the attempt's facts pass: `seq` from 0, `last` on the final chunk. */
export interface StagedFactsChunk {
  seq: number;
  last: boolean;
}

/**
 * Thrown by a hook (typically `onFinish`, when its stage does not cover the whole want pass) to send
 * the run round again: the module rolls the step back, abandons the run with `reason` (its stage rows
 * go, cascading anything keyed to them) and returns `restart`; the next attempt restages from
 * nothing with the hook attached from the first batch, so it does not loop.
 */
export class StagedRunRestart extends Error {
  constructor(readonly reason: "output_facts_stage_incomplete") {
    super(`staged reconcile restart: ${reason}`);
  }
}

/**
 * Hooks for a consumer of the run's batches (the output-facts slice, 2c). Both run inside the
 * module's own transaction for that step, so a throw rolls the step back (with any writes the hook
 * made on `tx`) and the run resumes there after the lease expires; the hook then sees the step
 * again, so it must be idempotent per run id. A hook may call further sessions functions on `tx`
 * under the same run id and lease nonce. The facts pass costs the wiring one more source
 * pass per attempt, made only when `onBatch` is set.
 */
export interface StagedReconcileHooks<W = StagedFactsEvents, R = unknown> {
  /**
   * "facts": the attempt's facts pass. At the end of the want phase (after its last batch, before the
   * want -> have advance), and only when `onBatch` is set, the module makes ONE extra source pass that
   * streams the whole-read facts input in source order: every post-fold record and every projection row
   * the whole read produces (convergence_events rows in and out of the window, catalog rows, display
   * values), in chunks, each delivered here in its own committed transaction. `chunk.seq` counts from 0
   * within the attempt; `chunk.last` marks the final chunk; a scope with no input gets one empty chunk
   * (seq 0, last). The pass is not resumable: a kill before the advance resumes in the want phase and
   * the pass runs again from seq 0 (reset on seq 0). An attempt that resumed past the want phase gets no
   * chunks and no seal.
   *
   * "write": once per pass 4 batch, in its transaction, after the rows were written (facts may ignore it).
   */
  onBatch?(
    tx: SqlExecutor,
    run: StagedRunRef,
    batch: { phase: "facts"; events: W; chunk: StagedFactsChunk } | { phase: "write"; events: readonly R[] },
  ): Promise<void>;
  /**
   * Once per want pass, in the same transaction as the want -> have advance, after the facts pass's
   * last chunk: the seal. `cursor` is the run's cursor at that point (the last want key staged, null
   * when the pass staged nothing; informational). Not called on an attempt that resumed past the want
   * phase.
   */
  onWantDone?(tx: SqlExecutor, run: StagedRunRef, pass: { cursor: string | null }): Promise<void>;
  /**
   * Once, in the final transaction, after `commit` and before the run is finished (its stage rows
   * dropped). `complete` is the pass's own verdict (false: a refused write, nothing deleted);
   * `sourceComplete` is the source read's own completeness (ScopeRead.complete, what
   * outputFactsFromScope takes as `complete`). They can differ.
   */
  onFinish?(
    tx: SqlExecutor,
    run: StagedRunRef,
    pass: { complete: boolean; sourceComplete: boolean; inserted: number; deleted: number; wanted: number },
  ): Promise<void>;
}

/** Where a test may stop a run. In-transaction points roll their transaction back. */
export type StagedBoundary =
  | "claimed"
  | "want:batch"
  | "want:facts"
  | "want:done"
  | "have:batch"
  | "have:done"
  | "write:page"
  | "write:done"
  | "final:locked"
  | "final:removed"
  | "final:committed"
  | "finished";

export interface StagedReconcileDeps {
  transact: Transact;
  /** Lease length, renewed by every call (default 120 s; 1 s to 1 h). */
  leaseMs?: number;
  /** Entries per staged batch (default 5000, at most 10000). */
  stageBatch?: number;
  /** Index rows per scan page (default 5000). */
  scanPage?: number;
  /** Keys per pass 4 regenerate (default 50000): each regenerate reads the source once. */
  writeKeys?: number;
  /** Index identities per delete page (default 5000, at most 10000). */
  deletePage?: number;
  /** A fingerprint prefix kept when its key is not wanted (default "bf", the buffered feed; 1 to 16 characters). */
  keepFingerprintPrefix?: string;
  /** An op error this returns a reason for abandons the run (the scope owes a pass) instead of throwing. */
  abandonOn?: (error: unknown) => string | null;
  /** Tests only. */
  boundary?: (point: StagedBoundary) => void | Promise<void>;
  /** On an error the run cannot map (thrown to the caller): release the lease first (best effort). */
  releaseOnError?: boolean;
  /** Items per facts chunk (default 5000). */
  factsChunk?: number;
  /** The output-facts hooks (slice 2c); absent: none is called. */
  hooks?: StagedReconcileHooks;
}

export type StagedReconcileOutcome =
  | { status: "busy"; reason: "run_leased" | "scope_busy" | "canonical_scope_busy" }
  | { status: "restart"; reason: "source_changed" | StagedRunRestart["reason"] }
  | { status: "incomplete"; reasons: string[] }
  | {
      status: "done";
      complete: boolean;
      /** Rows pass 4 wrote and wanted, over every attempt of the run. */
      inserted: number;
      wanted: number;
      deleted: number;
      reasons: string[];
      attempts: number;
      /** The phase and cursor a takeover resumed from; null on a run's first attempt or a restart. */
      resumedFrom: { phase: StagedPhase; cursor: string | null } | null;
    };

/** The fingerprint of a scope's admitted manifests (key and revision, order-free, unambiguous for any characters). */
export function admittedManifestFingerprint(sources: ReadonlyArray<{ manifestKey: string; revision: string }>): string {
  const pairs = sources.map((s): [string, string] => [s.manifestKey, s.revision]);
  pairs.sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : a[1] < b[1] ? -1 : a[1] > b[1] ? 1 : 0));
  return createHash("sha256").update(JSON.stringify(pairs)).digest("hex");
}

/** The canonical directory-events advisory lock key indexSearchScope takes. */
export function canonicalLockKey(scope: SessionScope, canonicalId: string): string {
  return ["96bulls:directory-events", scope.orgId, scope.workspaceId, scope.harness, canonicalId].join(SEP);
}

const ownerOf = (i: { workspaceId: string; sessionId: string }) => JSON.stringify([i.workspaceId, i.sessionId]);
const haveKey = (i: StagedIdentity) =>
  createHash("sha256").update(JSON.stringify([i.key, i.workspaceId, i.sessionId, i.tsMs, i.fingerprint, i.machineId, i.source, i.kind, i.eventId])).digest("base64url");

interface Lease {
  runId: string;
  nonce: string;
}

/** Erasure: every staged run of the scope (by session or canonical id) goes, leased or not. */
export async function eraseStagedReconcileRuns(tx: SqlExecutor, scope: SessionScope, sessionIds: readonly string[]): Promise<number> {
  const { rows } = await tx.query<{ n: string }>("SELECT sessions.search_reconcile_erase_scope_v1($1,$2,$3,$4::text[])::text AS n", [
    scope.orgId,
    scope.workspaceId,
    scope.harness,
    [...sessionIds],
  ]);
  return Number(rows[0]?.n ?? 0);
}

/**
 * One reconcile tick for a scope the whole read refused as too large: claims the scope's run
 * (or takes over an expired one), runs it from its committed phase and cursor to the end, and
 * returns the outcome. Throws what an op throws (the lease then expires and the next tick
 * resumes), except an error `abandonOn` names.
 */
export async function runStagedReconcile<R>(
  scope: SessionScope,
  stamp: SourceStamp,
  ops: StagedReconcileOps<R>,
  deps: StagedReconcileDeps,
): Promise<StagedReconcileOutcome> {
  const leaseMs = deps.leaseMs ?? 120_000;
  const stageBatch = Math.min(deps.stageBatch ?? 5000, 10_000);
  const scanSize = deps.scanPage ?? 5000;
  const writeKeys = deps.writeKeys ?? 50_000;
  const deletePage = Math.min(deps.deletePage ?? 5000, 10_000);
  const keep = deps.keepFingerprintPrefix ?? STAGED_RECONCILE_KEEP_PREFIX;
  const boundary = async (p: StagedBoundary) => {
    await deps.boundary?.(p);
  };

  if (deps.hooks?.onBatch && !ops.factsPass) throw new Error("staged reconcile: onBatch needs ops.factsPass");
  const factsChunk = deps.factsChunk ?? 5000;
  const claim = await deps.transact(async (tx) => {
    const { rows } = await tx.query<{
      run_id: string; lease_nonce: string; phase: StagedPhase; chunk_cursor: string | null;
      write_wanted: string; write_done: string; attempts: number; restarted: boolean;
    }>(
      `SELECT run_id::text, lease_nonce::text, phase, chunk_cursor, write_wanted::text, write_done::text, attempts, restarted
         FROM sessions.search_reconcile_claim_v1($1,$2,$3,$4,$5,$6::bigint,$7,$8)`,
      [scope.orgId, scope.workspaceId, scope.harness, scope.sessionId, stamp.canonicalId, stamp.epoch, stamp.manifestFingerprint, leaseMs],
    );
    const row = rows[0] ?? null;
    if (row) await ops.claimed?.(tx);
    return row;
  });
  if (!claim) return { status: "busy", reason: "run_leased" };
  const lease: Lease = { runId: claim.run_id, nonce: claim.lease_nonce };
  let phase = claim.phase;
  let cursor = claim.chunk_cursor;
  let wanted = Number(claim.write_wanted);
  let done = Number(claim.write_done);
  const resumedFrom = claim.attempts > 1 && !claim.restarted && (phase !== "want" || cursor !== null) ? { phase, cursor } : null;
  await boundary("claimed");

  const advance = (tx: SqlExecutor, to: StagedPhase, at: string | null, extra: { kinds?: string[]; wanted?: number; done?: number; release?: boolean } = {}) =>
    tx.query("SELECT sessions.search_reconcile_advance_v1($1::bigint,$2::uuid,$3,$4,$5::text[],$6::bigint,$7::bigint,$8)", [
      lease.runId, lease.nonce, to, at, extra.kinds ?? null, extra.wanted ?? 0, extra.done ?? 0, extra.release ?? false,
    ]);
  const stage = (tx: SqlExecutor, side: "want" | "have", pass: number, entries: unknown[], at: string | null) =>
    tx.query("SELECT sessions.search_reconcile_stage_batch_v1($1::bigint,$2::uuid,$3,$4::smallint,$5::jsonb,$6)", [
      lease.runId, lease.nonce, side, pass, JSON.stringify(entries), at,
    ]);
  const diffPage = async (tx: SqlExecutor, mode: "insert" | "delete", after: string | null, limit: number) =>
    (await tx.query<{ key_hash: string; payload: StagedIdentity | null }>(
      "SELECT key_hash, payload FROM sessions.search_reconcile_diff_page_v1($1::bigint,$2::uuid,$3,$4,$5,$6)",
      [lease.runId, lease.nonce, mode, after, limit, keep],
    )).rows;
  const abandon = async (reason: string) => {
    await deps.transact((tx) => tx.query("SELECT sessions.search_reconcile_abandon_v1($1::bigint,$2::uuid,$3)", [lease.runId, lease.nonce, reason]));
  };

  const tryLock = async (tx: SqlExecutor, key: string) =>
    (await tx.query<{ ok: boolean }>("SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0)) AS ok", [key])).rows[0]?.ok === true;
  const moved = (now: SourceStamp) =>
    now.canonicalId !== stamp.canonicalId || now.epoch !== stamp.epoch || now.manifestFingerprint !== stamp.manifestFingerprint;
  const exclusive = async (tx: SqlExecutor): Promise<"ok" | "busy" | "changed"> => {
    if (!(await tryLock(tx, scopeLockKey(scope, "search")))) return "busy";
    return moved(await ops.currentSource(tx)) ? "changed" : "ok";
  };
  /**
   * A long source read (passes 1-3, the facts pass) makes no step call, so nothing renews the lease:
   * renew it on a timer (a third of the lease) while `work` runs, so another consumer's tick does not
   * take the run over mid-read. A renewal that fails (taken over, erased) stops the timer; the next
   * step call then answers RH062.
   */
  const heartbeat = async <T>(work: () => Promise<T>): Promise<T> => {
    let stopped = false;
    let pending: Promise<unknown> = Promise.resolve();
    const timer = setInterval(() => {
      if (stopped) return;
      pending = pending.then(() =>
        deps.transact((tx) => advance(tx, phase, cursor)).catch(() => {
          stopped = true;
        }),
      );
    }, Math.max(250, Math.floor(leaseMs / 3)));
    try {
      return await work();
    } finally {
      stopped = true;
      clearInterval(timer);
      await pending;
    }
  };
  let read: R | null | undefined;
  // `beat` false inside the final transaction: it holds the run row (no takeover is possible), and a
  // renewal there would wait on that same row.
  const source = async (beat = true): Promise<R | null> =>
    read === undefined ? (read = await (beat ? heartbeat(() => ops.readSource()) : ops.readSource())) : read;

  try {
    if (phase === "want") {
      const r = await source();
      if (r === null) {
        await abandon("source_incomplete");
        return { status: "incomplete", reasons: ["source_incomplete"] };
      }
      const all = [...ops.wanted(r)].sort((a, b) => (a.key < b.key ? -1 : a.key > b.key ? 1 : 0));
      const todo = cursor === null ? all : all.filter((w) => w.key > cursor!);
      for (let i = 0; i < todo.length; i += stageBatch) {
        const batch = todo.slice(i, i + stageBatch);
        const last = batch[batch.length - 1]!.key;
        await deps.transact((tx) => stage(tx, "want", 3, batch.map((w) => ({ k: w.key, o: ownerOf(w), t: w.tsMs, f: w.fingerprint })), last));
        cursor = last;
        await boundary("want:batch");
      }
      // The facts pass (hooks only): one source pass, its chunks each in their own transaction, the
      // last flagged; an empty scope still gets one empty last chunk. Not resumable: rerun from seq 0.
      if (deps.hooks?.onBatch) {
        const ref = { runId: lease.runId, nonce: lease.nonce };
        let seq = 0;
        let pending: StagedFactsEvents | undefined;
        const deliver = (events: StagedFactsEvents, last: boolean) => {
          const chunk = { seq: seq++, last };
          return deps.transact((tx) => deps.hooks!.onBatch!(tx, ref, { phase: "facts", events, chunk }));
        };
        await heartbeat(() =>
          ops.factsPass!(r, factsChunk, async (events) => {
            if (pending) await deliver(pending, false);
            pending = events;
          }),
        );
        await deliver(pending ?? { projectionRows: [], records: [] }, true);
        await boundary("want:facts");
      }
      const kinds = [...new Set(ops.regenerableKinds(r))].sort();
      const sealed = cursor;
      await deps.transact(async (tx) => {
        await advance(tx, "have", null, { kinds });
        await deps.hooks?.onWantDone?.(tx, { runId: lease.runId, nonce: lease.nonce }, { cursor: sealed });
      });
      phase = "have";
      cursor = null;
      await boundary("want:done");
    }

    if (phase === "have") {
      for (;;) {
        const step = await deps.transact(async (tx) => {
          const page = await ops.scanPage(tx, cursor, scanSize);
          if (page.truncated) return "truncated" as const;
          // A scan that does not move forward would page forever.
          if (page.next !== null && cursor !== null && page.next <= cursor) throw new Error("staged reconcile: index scan cursor did not advance");
          const entries = page.rows.map((i) => ({
            k: haveKey(i), m: i.key, o: ownerOf(i), t: i.tsMs, f: i.fingerprint,
            p: { key: i.key, machineId: i.machineId, sessionId: i.sessionId, source: i.source, kind: i.kind, eventId: i.eventId, workspaceId: i.workspaceId, tsMs: i.tsMs, fingerprint: i.fingerprint },
          }));
          // The last page keeps the current cursor (a cursor is never cleared mid-phase); the advance resets it.
          const at = page.next ?? cursor;
          for (let i = 0; i < entries.length; i += stageBatch) await stage(tx, "have", 0, entries.slice(i, i + stageBatch), at);
          if (page.next === null) await advance(tx, "write", null);
          else if (!entries.length) await stage(tx, "have", 0, [], page.next);
          return page.next;
        });
        if (step === "truncated") {
          await abandon("search_index_truncated");
          return { status: "incomplete", reasons: ["search_index_truncated"] };
        }
        cursor = step;
        if (step === null) break;
        await boundary("have:batch");
      }
      phase = "write";
      await boundary("have:done");
    }

    if (phase === "write") {
      for (;;) {
        const r = await source();
        if (r === null) {
          await abandon("source_incomplete");
          return { status: "incomplete", reasons: ["source_incomplete"] };
        }
        const page = await deps.transact(async (tx) => {
          // Exclusive with a whole read and the erase pass of the scope: the same scope try-lock, and
          // the source stamp re-read under it, before anything is written to the index.
          const gate = await exclusive(tx);
          if (gate === "busy") {
            await advance(tx, "write", cursor, { release: true });
            return { gate } as const;
          }
          if (gate === "changed") {
            await tx.query("SELECT sessions.search_reconcile_abandon_v1($1::bigint,$2::uuid,$3)", [lease.runId, lease.nonce, "source_changed"]);
            return { gate } as const;
          }
          const keys: string[] = [];
          let after = cursor;
          while (keys.length < writeKeys) {
            const rows = await diffPage(tx, "insert", after, Math.min(10_000, writeKeys - keys.length));
            if (!rows.length) break;
            for (const row of rows) keys.push(row.key_hash);
            after = rows[rows.length - 1]!.key_hash;
          }
          if (!keys.length) {
            await advance(tx, "final", null);
            return null;
          }
          const emitted: unknown[] = [];
          const written = await ops.regenerate(tx, r, new Set(keys), deps.hooks?.onBatch ? (rows) => { emitted.push(...rows); } : undefined);
          if (deps.hooks?.onBatch) await deps.hooks.onBatch(tx, { runId: lease.runId, nonce: lease.nonce }, { phase: "write", events: emitted });
          await advance(tx, "write", after, { wanted: keys.length, done: written });
          return { after, wanted: keys.length, written };
        });
        if (page === null) break;
        if ("gate" in page) return page.gate === "busy" ? { status: "busy", reason: "scope_busy" } : { status: "restart", reason: "source_changed" };
        cursor = page.after;
        wanted += page.wanted;
        done += page.written;
        await boundary("write:page");
      }
      phase = "final";
      cursor = null;
      await boundary("write:done");
    }

    const outcome = await deps.transact(async (tx): Promise<StagedReconcileOutcome> => {
      // Locks first, in indexSearchScope's order (scope, then the source, then canonical); then
      // the run row, held to the end so no takeover can interleave with the final step.
      if (!(await tryLock(tx, scopeLockKey(scope, "search")))) {
        await advance(tx, "final", null, { release: true });
        return { status: "busy", reason: "scope_busy" };
      }
      await advance(tx, "final", null);
      if (moved(await ops.currentSource(tx))) {
        await tx.query("SELECT sessions.search_reconcile_abandon_v1($1::bigint,$2::uuid,$3)", [lease.runId, lease.nonce, "source_changed"]);
        return { status: "restart", reason: "source_changed" };
      }
      if (!(await tryLock(tx, canonicalLockKey(scope, stamp.canonicalId)))) {
        await advance(tx, "final", null, { release: true });
        return { status: "busy", reason: "canonical_scope_busy" };
      }
      // A run resumed at the final step reads the source here, under the locks.
      const r = await source(false);
      if (r === null) {
        await tx.query("SELECT sessions.search_reconcile_abandon_v1($1::bigint,$2::uuid,$3)", [lease.runId, lease.nonce, "source_incomplete"]);
        return { status: "incomplete", reasons: ["source_incomplete"] };
      }
      await boundary("final:locked");
      // As streamedReconcile: a refused write (stale version) deletes nothing and is not current.
      const complete = done === wanted;
      let deleted = 0;
      if (complete) {
        let after: string | null = null;
        for (;;) {
          const rows = await diffPage(tx, "delete", after, deletePage);
          if (!rows.length) break;
          deleted += await ops.remove(tx, rows.map((row) => row.payload!));
          after = rows[rows.length - 1]!.key_hash;
        }
      }
      await boundary("final:removed");
      const committed = await ops.commit(tx, r, { complete, inserted: done, deleted, wanted });
      if (deps.hooks?.onFinish)
        await deps.hooks.onFinish(tx, { runId: lease.runId, nonce: lease.nonce }, { complete, sourceComplete: ops.sourceComplete?.(r) ?? true, inserted: done, deleted, wanted });
      await boundary("final:committed");
      await tx.query("SELECT sessions.search_reconcile_finish_v1($1::bigint,$2::uuid)", [lease.runId, lease.nonce]);
      return {
        status: "done",
        complete: complete && committed.complete,
        inserted: done,
        wanted,
        deleted,
        reasons: [...(complete ? [] : ["search_version_conflict"]), ...committed.reasons],
        attempts: claim.attempts,
        resumedFrom,
      };
    });
    if (outcome.status === "done") await boundary("finished");
    return outcome;
  } catch (error) {
    // A hook sends the run round again: its step rolled back, the run restarts from nothing.
    if (error instanceof StagedRunRestart) {
      await abandon(error.reason);
      return { status: "restart", reason: error.reason };
    }
    const reason = deps.abandonOn?.(error) ?? null;
    if (reason === null) {
      // The step rolled back; with releaseOnError the lease is freed (best effort) so the next tick
      // retries without waiting for it to expire. Off (the default, and a crash): it expires.
      if (deps.releaseOnError) await deps.transact((tx) => advance(tx, phase, cursor, { release: true })).catch(() => {});
      throw error;
    }
    await abandon(reason);
    return { status: "incomplete", reasons: [reason] };
  }
}

export const STAGED_PHASES: readonly StagedPhase[] = PHASES;

/** The 062110 writers the staged reconcile calls. */
export const STAGED_RECONCILE_FUNCTIONS = [
  "sessions.search_reconcile_claim_v1(text,text,text,text,text,bigint,text,integer)",
  "sessions.search_reconcile_stage_batch_v1(bigint,uuid,text,smallint,jsonb,text)",
  "sessions.search_reconcile_advance_v1(bigint,uuid,text,text,text[],bigint,bigint,boolean)",
  "sessions.search_reconcile_diff_page_v1(bigint,uuid,text,text,integer,text)",
  "sessions.search_reconcile_finish_v1(bigint,uuid)",
  "sessions.search_reconcile_abandon_v1(bigint,uuid,text)",
  "sessions.search_reconcile_erase_scope_v1(text,text,text,text[])",
] as const;

/**
 * Startup with SEARCH_INDEX_STAGED_RECONCILE_ENABLED=true: every 062110 writer exists, or the
 * consumer refuses to start naming the migration (never on the first large scope).
 */
export async function assertStagedReconcileSchema(db: SqlExecutor): Promise<void> {
  const { rows } = await db.query<{ fn: string }>(
    "SELECT fn FROM unnest($1::text[]) AS fn WHERE to_regprocedure(fn) IS NULL",
    [[...STAGED_RECONCILE_FUNCTIONS]],
  );
  if (rows.length)
    throw new Error(
      "pipeline consumer: SEARCH_INDEX_STAGED_RECONCILE_ENABLED=true requires migration 20261105062110_search_reconcile_staging.sql (missing sessions.search_reconcile_* functions)",
    );
}
