/**
 * Once-only held-out evaluations (docs/session-intel-overlap-bench.md, "Protocol"). Each evaluation has a canonical
 * identity: its kind and the digest of the labels' content, so neither copying the labels, the freeze or the
 * inputs, nor a new output directory, opens a second evaluation. Its record lives in one per-user registry that no
 * command-line path chooses.
 *
 * A claim is recoverable: it records exactly what it was claimed for (the freeze identity and the run's terms). An
 * interrupted run may finish under the same terms, since the evaluation is deterministic; any other run is refused.
 * Once a result is recorded, nothing can run again.
 */
import { existsSync, openSync, closeSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { privateDir, readPrivate, writePrivate } from "../cloud-harvest/private-fs.ts";

export const REGISTRY = join(homedir(), ".local", "state", "overlap-bench", "heldout");

export interface OnceRecord {
  kind: "stage1" | "stage2"; labelsHash: string; terms: Record<string, unknown>; state: "claimed" | "done"; claimedAt: string;
  /** Stage 2: the manifest hash of the batch made under the claim; once set, no other batch can be scored. */
  batch?: string;
  result?: unknown;
}

const recordPath = (registry: string, kind: OnceRecord["kind"], labelsHash: string) => {
  if (!/^[0-9a-f]{64}$/.test(labelsHash)) throw new Error("labels digest is not a sha256");
  return join(privateDir(registry), `${kind}-${labelsHash}.json`);
};

/**
 * Claims the evaluation, or resumes a claim made under identical terms that never finished. Throws if the evaluation
 * is done, or claimed under other terms.
 */
export function claimOnce(kind: OnceRecord["kind"], labelsHash: string, terms: Record<string, unknown>, registry = REGISTRY): OnceRecord {
  const path = recordPath(registry, kind, labelsHash);
  const rec: OnceRecord = { kind, labelsHash, terms, state: "claimed", claimedAt: new Date().toISOString() };
  try {
    const fd = openSync(path, "wx", 0o600);
    writeFileSync(fd, JSON.stringify(rec, null, 1)); closeSync(fd);
    return rec;
  } catch (e) {
    if ((e as NodeJS.ErrnoException).code !== "EEXIST") throw e;
  }
  const prev = JSON.parse(readPrivate(path)) as OnceRecord;
  if (prev.state === "done") throw new Error(`held-out ${kind} was already evaluated for these labels`);
  if (JSON.stringify(prev.terms) !== JSON.stringify(terms)) throw new Error(`held-out ${kind} is claimed for these labels under other terms`);
  return prev;
}

/** Records the batch made under a stage-2 claim. */
export function noteOnce(rec: OnceRecord, batch: string, registry = REGISTRY): OnceRecord {
  const path = recordPath(registry, rec.kind, rec.labelsHash);
  const cur = JSON.parse(readPrivate(path)) as OnceRecord;
  if (cur.state === "done" || (cur.batch && cur.batch !== batch)) throw new Error(`held-out ${rec.kind} already has a batch for these labels`);
  const next = { ...cur, batch };
  writePrivate(path, JSON.stringify(next, null, 1));
  return next;
}

/** Records the result; the evaluation can never run again. */
export function finishOnce(rec: OnceRecord, result: unknown, registry = REGISTRY): void {
  const path = recordPath(registry, rec.kind, rec.labelsHash);
  const cur = JSON.parse(readPrivate(path)) as OnceRecord;
  if (cur.state === "done") throw new Error(`held-out ${rec.kind} was already evaluated for these labels`);
  writePrivate(path, JSON.stringify({ ...cur, state: "done", result }, null, 1));
}

export function onceState(kind: OnceRecord["kind"], labelsHash: string, registry = REGISTRY): OnceRecord | null {
  const path = recordPath(registry, kind, labelsHash);
  return existsSync(path) ? JSON.parse(readPrivate(path)) as OnceRecord : null;
}
