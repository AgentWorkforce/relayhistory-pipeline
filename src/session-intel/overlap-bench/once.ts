/**
 * Once-only held-out evaluations (docs/session-intel-overlap-bench.md, "Protocol"). Each evaluation has a canonical
 * identity: its kind and the digest of the labels' bytes, so neither copying or renaming the labels, the freeze or
 * the inputs, nor a new output directory, opens a second evaluation. Its record lives in one per-user registry that
 * no command-line path chooses.
 *
 * Every transition (claim, pinning a batch, finishing) runs under an exclusive lock on the record, across
 * processes. A claim names its owner process. An active claim (its owner alive) is never shared; a claim whose owner
 * died is interrupted and may be resumed under identical terms, since the evaluation is deterministic. Callers write
 * their output receipt before finishing, so an exit between the two only repeats the same deterministic run.
 */
import { closeSync, existsSync, openSync, unlinkSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { privateDir, readPrivate, writePrivate } from "../cloud-harvest/private-fs.ts";

export const REGISTRY = join(homedir(), ".local", "state", "overlap-bench", "heldout");

export interface OnceRecord {
  kind: "stage1" | "stage2"; labelsHash: string; terms: Record<string, unknown>; state: "claimed" | "done"; claimedAt: string;
  /** The process that holds the claim. */
  owner: number;
  /** Stage 2: the manifest hash of the batch made under the claim; once set, no other batch can be scored. */
  batch?: string;
  /** Stage 2: the validated verdicts' digest and the result, pinned before any receipt is published. */
  score?: { verdictsHash: string; result: unknown };
  result?: unknown;
}

const recordPath = (registry: string, kind: OnceRecord["kind"], labelsHash: string) => {
  if (!/^[0-9a-f]{64}$/.test(labelsHash)) throw new Error("labels digest is not a sha256");
  return join(privateDir(registry), `${kind}-${labelsHash}.json`);
};

const alive = (pid: number) => {
  if (!Number.isSafeInteger(pid) || pid <= 0) return false;
  try { process.kill(pid, 0); return true; } catch (e) { return (e as NodeJS.ErrnoException).code === "EPERM"; }
};

/** Runs `fn` holding the record's lock (exclusive create); a concurrent transition is refused, not waited for. */
function locked<T>(path: string, fn: () => T): T {
  const lock = `${path}.lock`;
  let fd: number;
  try { fd = openSync(lock, "wx", 0o600); } catch (e) {
    if ((e as NodeJS.ErrnoException).code === "EEXIST") throw new Error(`the held-out record is busy in another process; if none is running, check and remove ${lock}`);
    throw e;
  }
  try { writeFileSync(fd, String(process.pid)); closeSync(fd); return fn(); } finally { unlinkSync(lock); }
}

const read = (path: string) => JSON.parse(readPrivate(path)) as OnceRecord;

/**
 * Claims the evaluation for this process, or resumes an interrupted claim (its owner gone) made under identical
 * terms. Throws if the evaluation is done, claimed under other terms, or held by a live process.
 */
export function claimOnce(kind: OnceRecord["kind"], labelsHash: string, terms: Record<string, unknown>, registry = REGISTRY, pid = process.pid): OnceRecord {
  const path = recordPath(registry, kind, labelsHash);
  return locked(path, () => {
    if (!existsSync(path)) {
      const rec: OnceRecord = { kind, labelsHash, terms, state: "claimed", claimedAt: new Date().toISOString(), owner: pid };
      writePrivate(path, JSON.stringify(rec, null, 1));
      return rec;
    }
    const prev = read(path);
    if (prev.state === "done") throw new Error(`held-out ${kind} was already evaluated for these labels`);
    if (JSON.stringify(prev.terms) !== JSON.stringify(terms)) throw new Error(`held-out ${kind} is claimed for these labels under other terms`);
    if (prev.owner !== pid && alive(prev.owner)) throw new Error(`held-out ${kind} is being evaluated by process ${prev.owner}`);
    const next = { ...prev, owner: pid };
    writePrivate(path, JSON.stringify(next, null, 1));
    return next;
  });
}

function transition(rec: OnceRecord, registry: string, pid: number, change: (cur: OnceRecord) => OnceRecord): OnceRecord {
  const path = recordPath(registry, rec.kind, rec.labelsHash);
  return locked(path, () => {
    const cur = read(path);
    if (cur.state === "done") throw new Error(`held-out ${rec.kind} was already evaluated for these labels`);
    if (cur.owner !== pid) throw new Error(`held-out ${rec.kind} is now held by process ${cur.owner}`);
    const next = change(cur);
    writePrivate(path, JSON.stringify(next, null, 1));
    return next;
  });
}

/** Pins the batch made under a stage-2 claim; a claim can pin one batch only. */
export function noteOnce(rec: OnceRecord, batch: string, registry = REGISTRY, pid = process.pid): OnceRecord {
  return transition(rec, registry, pid, (cur) => {
    if (cur.batch && cur.batch !== batch) throw new Error(`held-out ${rec.kind} already has a batch for these labels`);
    return { ...cur, batch };
  });
}

/**
 * Pins a stage-2 score (the verdicts' digest and the result) before its receipt is published. A score is pinned
 * once: a resumed scoring must present the same verdicts and publishes the pinned result.
 */
export function pinScore(rec: OnceRecord, score: { verdictsHash: string; result: unknown }, registry = REGISTRY, pid = process.pid): OnceRecord {
  return transition(rec, registry, pid, (cur) => {
    if (cur.score) {
      if (cur.score.verdictsHash !== score.verdictsHash) throw new Error("the verdicts differ from the ones this held-out score was pinned with");
      return cur;
    }
    return { ...cur, score };
  });
}

/** Records the result; the evaluation can never run again. Write the output receipt before calling this. */
export function finishOnce(rec: OnceRecord, result: unknown, registry = REGISTRY, pid = process.pid): void {
  transition(rec, registry, pid, (cur) => ({ ...cur, state: "done", result }));
}

export function onceState(kind: OnceRecord["kind"], labelsHash: string, registry = REGISTRY): OnceRecord | null {
  const path = recordPath(registry, kind, labelsHash);
  return existsSync(path) ? read(path) : null;
}
