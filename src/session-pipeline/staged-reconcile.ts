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
 *   final  one short transaction: the scope and canonical advisory locks (the same keys
 *          indexSearchScope takes; busy releases the lease), the source stamp re-read and
 *          compared (changed: the run is abandoned and restarts), then, only when every
 *          wanted row was written, `diff_page('delete')` pages through `remove`, then
 *          `commit` (directory events, catalog, search state), then the run and its stage
 *          rows go (`finish`). A throw anywhere in it rolls all of it back.
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
  /** One keyset page of the scope's index rows after `after` (null: the first). `next` null: the last page. */
  scanPage(tx: SqlExecutor, after: string | null, limit: number): Promise<{ rows: StagedIdentity[]; next: string | null; truncated?: boolean }>;
  /** Pass 4: write the rows of these keys on `tx`; returns the rows the index accepted. */
  regenerate(tx: SqlExecutor, read: R, keys: ReadonlySet<string>): Promise<number>;
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

/** Where a test may stop a run. In-transaction points roll their transaction back. */
export type StagedBoundary =
  | "claimed"
  | "want:batch"
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
  /** A fingerprint prefix kept when its key is not wanted (default "bf", the buffered feed). */
  keepFingerprintPrefix?: string;
  /** An op error this returns a reason for abandons the run (the scope owes a pass) instead of throwing. */
  abandonOn?: (error: unknown) => string | null;
  /** Tests only. */
  boundary?: (point: StagedBoundary) => void | Promise<void>;
}

export type StagedReconcileOutcome =
  | { status: "busy"; reason: "run_leased" | "scope_busy" | "canonical_scope_busy" }
  | { status: "restart"; reason: "source_changed" }
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

/** The fingerprint of a scope's admitted manifests (key and revision, order-free). */
export function admittedManifestFingerprint(sources: ReadonlyArray<{ manifestKey: string; revision: string }>): string {
  const lines = sources.map((s) => `${s.manifestKey}${SEP}${s.revision}`).sort();
  return createHash("sha256").update(lines.join("\n")).digest("hex");
}

/** The canonical directory-events advisory lock key indexSearchScope takes. */
export function canonicalLockKey(scope: SessionScope, canonicalId: string): string {
  return ["96bulls:directory-events", scope.orgId, scope.workspaceId, scope.harness, canonicalId].join(SEP);
}

const ownerOf = (i: { workspaceId: string; sessionId: string }) => `${i.workspaceId}${SEP}${i.sessionId}`;
const haveKey = (i: StagedIdentity) =>
  createHash("sha256").update([i.key, ownerOf(i), String(i.tsMs), i.fingerprint, i.machineId, i.source, i.kind, i.eventId].join(SEP)).digest("base64url");

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

  const claim = await deps.transact(async (tx) => {
    const { rows } = await tx.query<{
      run_id: string; lease_nonce: string; phase: StagedPhase; chunk_cursor: string | null;
      write_wanted: string; write_done: string; attempts: number; restarted: boolean;
    }>(
      `SELECT run_id::text, lease_nonce::text, phase, chunk_cursor, write_wanted::text, write_done::text, attempts, restarted
         FROM sessions.search_reconcile_claim_v1($1,$2,$3,$4,$5,$6::bigint,$7,$8)`,
      [scope.orgId, scope.workspaceId, scope.harness, scope.sessionId, stamp.canonicalId, stamp.epoch, stamp.manifestFingerprint, leaseMs],
    );
    return rows[0] ?? null;
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

  let read: R | null | undefined;
  const source = async (): Promise<R | null> => (read === undefined ? (read = await ops.readSource()) : read);

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
      const kinds = [...new Set(ops.regenerableKinds(r))].sort();
      await deps.transact((tx) => advance(tx, "have", null, { kinds }));
      phase = "have";
      cursor = null;
      await boundary("want:done");
    }

    if (phase === "have") {
      for (;;) {
        const step = await deps.transact(async (tx) => {
          const page = await ops.scanPage(tx, cursor, scanSize);
          if (page.truncated) return "truncated" as const;
          const entries = page.rows.map((i) => ({
            k: haveKey(i), m: i.key, o: ownerOf(i), t: i.tsMs, f: i.fingerprint,
            p: { key: i.key, machineId: i.machineId, sessionId: i.sessionId, source: i.source, kind: i.kind, eventId: i.eventId, workspaceId: i.workspaceId, tsMs: i.tsMs, fingerprint: i.fingerprint },
          }));
          for (let i = 0; i < entries.length; i += stageBatch) await stage(tx, "have", 0, entries.slice(i, i + stageBatch), page.next);
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
          const written = await ops.regenerate(tx, r, new Set(keys));
          await advance(tx, "write", after, { wanted: keys.length, done: written });
          return { after, wanted: keys.length, written };
        });
        if (page === null) break;
        cursor = page.after;
        wanted += page.wanted;
        done += page.written;
        await boundary("write:page");
      }
      phase = "final";
      cursor = null;
      await boundary("write:done");
    }

    const r = await source();
    if (r === null) {
      await abandon("source_incomplete");
      return { status: "incomplete", reasons: ["source_incomplete"] };
    }
    const outcome = await deps.transact(async (tx): Promise<StagedReconcileOutcome> => {
      const lock = async (key: string) =>
        (await tx.query<{ ok: boolean }>("SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0)) AS ok", [key])).rows[0]?.ok === true;
      if (!(await lock(scopeLockKey(scope, "search")))) {
        await advance(tx, "final", null, { release: true });
        return { status: "busy", reason: "scope_busy" };
      }
      const now = await ops.currentSource(tx);
      if (now.canonicalId !== stamp.canonicalId || now.epoch !== stamp.epoch || now.manifestFingerprint !== stamp.manifestFingerprint) {
        await tx.query("SELECT sessions.search_reconcile_abandon_v1($1::bigint,$2::uuid,$3)", [lease.runId, lease.nonce, "source_changed"]);
        return { status: "restart", reason: "source_changed" };
      }
      if (!(await lock(canonicalLockKey(scope, stamp.canonicalId)))) {
        await advance(tx, "final", null, { release: true });
        return { status: "busy", reason: "canonical_scope_busy" };
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
    const reason = deps.abandonOn?.(error) ?? null;
    if (reason === null) throw error;
    await abandon(reason);
    return { status: "incomplete", reasons: [reason] };
  }
}

export const STAGED_PHASES: readonly StagedPhase[] = PHASES;
