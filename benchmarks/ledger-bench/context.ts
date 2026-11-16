/** What every step shares: the stores, the meter, the key prefix and the results sink. */
import { readAll } from "../../packages/object-store/src/core.ts";
import { isObjectStoreError } from "../../packages/object-store/src/errors.ts";
import { objectKey, type ObjectKey } from "../../packages/object-store/src/key.ts";
import type { ObjectStore } from "../../packages/object-store/src/types.ts";
import type { Dynamo, ExpressSessionStats, RequestMeter } from "./aws.ts";
import type { Profile, TargetId } from "./config.ts";
import { runLoad, type CellResult, type LoadOptions } from "./load.ts";

export const KIB = 1024;

export interface Cell {
  step: number;
  /** Stable id of the cell within its step, e.g. `put`, `race_create`, `plog`. */
  cell: string;
  target: TargetId | "R4+R5";
  params: Record<string, string | number | boolean>;
  result: unknown;
}

export interface Ctx {
  profile: Profile;
  meter: RequestMeter;
  /** `bench/<runId>/<lane>`: everything the lane writes is under it. */
  root: string;
  /** SSE-KMS stores (the bucket's own key). */
  stores: Partial<Record<Exclude<TargetId, "R6">, ObjectStore>>;
  /** SSE-S3 stores on the same buckets, for the KMS share of a round trip. */
  sseS3: Partial<Record<"R1" | "R2", ObjectStore>>;
  expressStats: Partial<Record<"R4" | "R5", ExpressSessionStats>>;
  dynamo?: Dynamo;
  emit: (cell: Cell) => void;
  log: (message: string) => void;
}

/** Bytes that do not compress and differ per call. */
export function randomBytes(size: number): Uint8Array {
  const out = new Uint8Array(size);
  for (let o = 0; o < size; o += 65536) crypto.getRandomValues(out.subarray(o, Math.min(size, o + 65536)));
  return out;
}

/** A JSON document padded to `size` bytes: a commit record, fence or log entry of realistic size. */
export function jsonDoc(fields: Record<string, unknown>, size: number): Uint8Array {
  const base = JSON.stringify({ ...fields, pad: "" });
  const pad = Math.max(0, size - base.length);
  return new TextEncoder().encode(JSON.stringify({ ...fields, pad: "x".repeat(pad) }));
}

export const key = (...parts: (string | number)[]): ObjectKey => objectKey(parts.join("/"));

/** Zero-padded so that key order is numeric order. */
export const seqName = (seq: number) => String(seq).padStart(12, "0");

export async function readBody(store: ObjectStore, k: ObjectKey): Promise<Uint8Array> {
  return readAll((await store.getObject(k)).body);
}

/** `undefined` for a missing object. */
export async function readOptional(store: ObjectStore, k: ObjectKey): Promise<{ body: Uint8Array; version: { eTag?: string; version?: string } } | undefined> {
  try {
    const got = await store.getObject(k);
    return { body: await readAll(got.body), version: got.meta.version };
  } catch (error) {
    if (isObjectStoreError(error, "not_found")) return undefined;
    throw error;
  }
}

/** Every key under a prefix, following pagination. Directory buckets do not list in key order, so callers sort. */
export async function listAll(store: ObjectStore, prefix: ObjectKey): Promise<string[]> {
  const out: string[] = [];
  let cursor: string | undefined;
  do {
    const page = await store.list(prefix, cursor);
    for (const item of page.items) out.push(item.key);
    cursor = page.nextCursor;
  } while (cursor);
  return out;
}

/** A latency cell: runs the load, stops at the request budget, and records the result. */
export async function cell(
  ctx: Ctx,
  meta: Omit<Cell, "result">,
  load: Pick<LoadOptions, "concurrency" | "op"> & { seconds?: number; maxOps?: number },
  extra: () => Record<string, unknown> = () => ({}),
): Promise<CellResult> {
  const result = await runLoad({
    concurrency: load.concurrency,
    warmupMs: ctx.profile.warmupSeconds * 1000,
    durationMs: (load.seconds ?? ctx.profile.cellSeconds) * 1000,
    maxOps: load.maxOps ?? ctx.profile.maxOpsPerCell,
    stop: () => ctx.meter.exhausted,
    op: load.op,
  });
  ctx.emit({ ...meta, result: { ...result, ...extra() } });
  ctx.log(`step ${meta.step} ${meta.cell} ${meta.target} ${JSON.stringify(meta.params)}: ${result.ops_per_sec}/s p50 ${result.latency.p50_ms} p99 ${result.latency.p99_ms} errors ${JSON.stringify(result.errors)}`);
  return result;
}

/** The store for a target, or a clear failure when the lane's config lacks it. */
export function need(ctx: Ctx, id: Exclude<TargetId, "R6">): ObjectStore {
  const store = ctx.stores[id];
  if (!store) throw new Error(`ledger-bench: target ${id} is not configured for this lane`);
  return store;
}
