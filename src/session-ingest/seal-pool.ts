/**
 * Seal pool: the raw-chunk seal's CPU work (source sha256, scrub, zstd, scrubbed
 * sha256) on worker threads, so one ingest edge process can use more than one core
 * (docs/design/admission-throughput-and-transcripts-region.md, step T2).
 *
 * `INGEST_SEAL_WORKERS`:
 * - unset, "" or "0": off. `sealPoolDeps` returns `{}` and nothing changes.
 * - an integer 1..32: that many worker threads.
 *
 * The output is today's, byte for byte. Each worker runs the pinned sealer
 * (`sealRawChunk`) unchanged against a capturing store (seal-pool-worker.ts). The
 * main thread then makes the one create-only PUT it captured, with the same key,
 * body and options, to the real store:
 * - a created ack: the entry takes `stored_bytes` / `stored_sha256` from the real
 *   ack, as the pinned sealer does;
 * - anything else (an existing object, `already_exists`, a worker error, a dead
 *   worker): the main thread runs the pinned sealer itself against the real store,
 *   so the result, or the error, is exactly today's. The re-put of an identical
 *   body is the store's idempotent success, and the sealer describes the stored
 *   object from its own metadata as always;
 * - any other PUT failure is thrown as it is, as the pinned sealer would throw it.
 *
 * TypeScript scrub engine only. `SCRUB_ENGINE=wasm-bindgen` installs its own
 * bound sealer (`routedRawChunkSealerDeps`), and the pool refuses to start with it
 * rather than pick one silently.
 */
import { Worker } from "node:worker_threads";
import { availableParallelism } from "node:os";
import { activeScrubMarker, routedRawChunkSealerDeps, sealRawChunk } from "@rhc/lib/scrub-engine/index.ts";
import type { BoundRawChunkSealer, EngineSealedChunk } from "../../packages/pipeline/src/lib/pipeline/engine-raw-chunks.ts";
import type { SealRawChunk, SealedChunk } from "../../packages/pipeline/src/lib/pipeline/raw-chunks.ts";
import type { ObjectStore, PutOpts } from "../../packages/object-store/src/types.ts";
import { isObjectStoreError, objectKey } from "../shared/vendor.ts";

export interface SealPoolRequest { id: number; input: SealRawChunk }
export type SealPoolResponse =
  | { id: number; ok: true; sealed: SealedChunk; put: { key: string; body: Uint8Array; opts: PutOpts } }
  | { id: number; ok: false; code: string };

export const SEAL_POOL_MAX_WORKERS = 32;

/** `INGEST_SEAL_WORKERS`: 0 (off) or 1..32. Anything else is a configuration error. */
export function parseSealWorkers(value: string | undefined): number {
  if (value === undefined || value.trim() === "") return 0;
  if (!/^\d+$/.test(value.trim())) throw new Error(`96bulls: INGEST_SEAL_WORKERS must be 0..${SEAL_POOL_MAX_WORKERS}, got a non-integer`);
  const n = Number(value.trim());
  if (n > SEAL_POOL_MAX_WORKERS) throw new Error(`96bulls: INGEST_SEAL_WORKERS must be 0..${SEAL_POOL_MAX_WORKERS}, got ${n}`);
  return n;
}

/** Counters only: no keys, no content. */
export interface SealPoolStats { pooled: number; fallback: number; workerErrors: number; respawned: number }

export interface SealPool {
  readonly sealer: BoundRawChunkSealer;
  readonly size: number;
  stats(): SealPoolStats;
  close(): Promise<void>;
}

interface Slot { worker: Worker; busy: boolean; pending?: { id: number; resolve: (r: SealPoolResponse) => void } }

export function createSealPool(size: number, options: { workerUrl?: URL } = {}): SealPool {
  if (!Number.isInteger(size) || size < 1 || size > SEAL_POOL_MAX_WORKERS) throw new Error(`96bulls: seal pool size must be 1..${SEAL_POOL_MAX_WORKERS}`);
  const url = options.workerUrl ?? new URL("./seal-pool-worker.ts", import.meta.url);
  const counters: SealPoolStats = { pooled: 0, fallback: 0, workerErrors: 0, respawned: 0 };
  const queue: { request: SealPoolRequest; resolve: (r: SealPoolResponse) => void }[] = [];
  let nextId = 1;
  let closed = false;

  const spawn = (slot?: Slot): Slot => {
    const worker = new Worker(url);
    worker.unref();
    const s: Slot = slot ?? { worker, busy: false };
    s.worker = worker;
    worker.on("message", (response: SealPoolResponse) => {
      const pending = s.pending;
      if (!pending || pending.id !== response.id) return;
      s.pending = undefined;
      s.busy = false;
      pending.resolve(response);
      pump();
    });
    const fail = () => {
      if (s.worker !== worker || closed) return;
      counters.workerErrors++;
      const pending = s.pending;
      s.pending = undefined;
      s.busy = false;
      // The task falls back to the main thread: never lost, never a different error.
      pending?.resolve({ id: pending.id, ok: false, code: "worker_exit" });
      if (!closed) { counters.respawned++; spawn(s); pump(); }
    };
    worker.on("error", fail);
    worker.on("exit", fail);
    return s;
  };
  const slots: Slot[] = Array.from({ length: size }, () => spawn());

  function pump(): void {
    for (const slot of slots) {
      if (slot.busy || queue.length === 0) continue;
      const next = queue.shift()!;
      slot.busy = true;
      slot.pending = { id: next.request.id, resolve: next.resolve };
      slot.worker.postMessage(next.request);
    }
  }

  const run = (input: SealRawChunk): Promise<SealPoolResponse> => {
    if (closed) return Promise.resolve({ id: 0, ok: false, code: "closed" });
    // A copy crosses the thread boundary: the caller's source stays usable for a fallback.
    const request: SealPoolRequest = { id: nextId++, input: { ...input, source: input.source.slice() } };
    return new Promise((resolve) => { queue.push({ request, resolve }); pump(); });
  };

  const fallback = (store: ObjectStore, input: SealRawChunk): Promise<SealedChunk> => {
    counters.fallback++;
    return sealRawChunk(store, input);
  };

  const sealer = (async (store: ObjectStore, input: SealRawChunk): Promise<EngineSealedChunk> => {
    const response = await run(input);
    if (!response.ok) return fallback(store, input);
    let ack;
    try {
      ack = await store.put(objectKey(response.put.key), response.put.body, response.put.opts);
    } catch (error) {
      if (!isObjectStoreError(error, "already_exists")) throw error;
      return fallback(store, input);
    }
    if (!ack.created) return fallback(store, input);
    counters.pooled++;
    const { sealed } = response;
    return { ...sealed, entry: { ...sealed.entry, stored_bytes: ack.size, stored_sha256: ack.contentSha256 } };
  }) as BoundRawChunkSealer;
  Object.defineProperty(sealer, "marker", { get: () => activeScrubMarker(), enumerable: true });

  return {
    sealer,
    size,
    stats: () => ({ ...counters }),
    async close() {
      closed = true;
      for (const { resolve, request } of queue.splice(0)) resolve({ id: request.id, ok: false, code: "closed" });
      await Promise.all(slots.map((s) => s.worker.terminate()));
    },
  };
}

/** The pool's size for this process: the setting, capped at the CPUs the process may use. */
export function sealPoolSize(env: Record<string, string | undefined>): number {
  const wanted = parseSealWorkers(env.INGEST_SEAL_WORKERS);
  return wanted === 0 ? 0 : Math.min(wanted, Math.max(1, availableParallelism()));
}

/**
 * `{ rawChunkSealer }` for a writer target. Off: exactly `routedRawChunkSealerDeps()`
 * (today's object). On: the pool's sealer; refused next to a routed engine sealer.
 */
export function sealPoolDeps(env: Record<string, string | undefined>, create: (size: number) => SealPool = createSealPool): { rawChunkSealer?: BoundRawChunkSealer; sealPool?: SealPool } {
  const size = sealPoolSize(env);
  const routed = routedRawChunkSealerDeps();
  if (size === 0) return routed;
  if (routed.rawChunkSealer) throw new Error("96bulls: INGEST_SEAL_WORKERS needs the TypeScript scrub engine (SCRUB_ENGINE=wasm-bindgen installs its own sealer)");
  const pool = create(size);
  return { rawChunkSealer: pool.sealer, sealPool: pool };
}
