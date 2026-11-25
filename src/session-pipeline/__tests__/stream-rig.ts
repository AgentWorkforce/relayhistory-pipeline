/**
 * The hosted streamed-pass rig shared by the SEARCH_INDEX_STREAMING_CH=1 guard tests: two sessions in one store (each
 * too large for the whole read, small enough to stream), the hosted harness, a pool of PGlite clients that can drop
 * their "connection", and the real exported Lambda handler over the consumer deps. Synthetic data only.
 */
import { EventEmitter } from "node:events";
import type { PGlite } from "@electric-sql/pglite";
import { STREAM_MIN_START_MS, type SearchIndexDeps } from "../search-index.ts";
import { createPipelineLambdaHandler, type Runtime } from "../lambda.ts";
import type { HoopDb } from "../../shared/db.ts";
import { ID, T0, USER, exportStreamScope, generate, type ExportedScope } from "../../session-read-api/__tests__/stream-fixture.ts";
import { exportUnit } from "../../migration/neon-s3-sync/export-session.ts";
import { hostedRig } from "./search-index-hosted-harness.ts";

export const TINY = { maxScopeBytes: 1, maxScopeEvents: 1 };
export const UNLIMITED = { maxReadBytes: 1 << 30, maxReadEvents: 1 << 24 };
export const MARGIN = 60_000;
export const STREAM_ENV = { searchStreamCh: { ...UNLIMITED, deadlineMarginMs: MARGIN } };
/** Remaining time at which the stream deadline is exactly STREAM_MIN_START_MS away. */
export const THRESHOLD = STREAM_MIN_START_MS + MARGIN;
export const SECOND = { ...ID, sessionId: "stream:second" };

/** Two sessions in one store, each too large for the whole read (TINY) and small enough to stream. */
export async function twoScopes(cleanup: Array<() => Promise<unknown>>): Promise<ExportedScope & { allKeys: string[] }> {
  const x = await exportStreamScope(generate(33), 1200);
  cleanup.push(x.dispose);
  const encoder = new TextEncoder();
  const second = await exportUnit({ store: x.store, keyRoot: "96bulls", grantId: "stream_parity", maxChunkBytes: 1200 },
    { orgId: SECOND.orgId, workspaceId: SECOND.workspaceId, source: SECOND.source, sessionId: SECOND.sessionId, firstMs: T0, lastMs: T0 + 60_000, machineIds: [], totals: {} },
    { fenced: async () => false, async *lines() { for (const { machine, record } of generate(8).filter((l) => l.record.kind !== "convergence_event")) yield { machine, userId: USER, bytes: encoder.encode(JSON.stringify(record) + "\n") }; } });
  return { ...x, allKeys: [...x.keys, ...second.manifests.map((m) => m.manifestKey)] };
}

/** A pg client over the PGlite session that can drop its "connection" (emit 'error'), as pg's client does. */
export class PgliteClient extends EventEmitter {
  released: Array<Error | undefined> = [];
  constructor(private pg: PGlite) { super(); }
  query(text: string, params?: unknown[]) { return this.pg.query(text, params as unknown[]); }
  release(error?: Error) { this.released.push(error); }
}
/** A HoopDb whose pool hands out PgliteClients (the last one is kept for the test). */
export function pgliteHoopDb(pg: PGlite) {
  const clients: PgliteClient[] = [];
  const db = {
    pool: { connect: async () => { const c = new PgliteClient(pg); clients.push(c); return c; } },
    query: async (text: string, params?: unknown[]) => ({ rows: (await pg.query(text, params as unknown[])).rows }),
    end: async () => {},
  } as unknown as HoopDb;
  return { db, clients };
}

export async function streamRig(cleanup: Array<() => Promise<unknown>>, opts: { env?: Partial<Runtime["env"]>; search?: Partial<SearchIndexDeps> } = {}) {
  const x = await twoScopes(cleanup);
  const r = await hostedRig(x, cleanup);
  const messages = x.allKeys.map((key, n) => ({ messageId: `m-${n}`, body: JSON.stringify({ reconcile: { key } }) }));
  const pool = pgliteHoopDb(r.db.pg);
  const base = r.consumer({ limits: TINY, stream: UNLIMITED, ...opts.search });
  const deps = { ...base, pointer: { ...base.pointer, db: { ...(base.pointer.db as object), pool: pool.db.pool } as never } };
  const rt = { env: { ...STREAM_ENV, ...opts.env }, deps } as unknown as Runtime;
  const handler = createPipelineLambdaHandler({ loadRuntime: async () => rt });
  return { x, r, rt, handler, messages, pool };
}

/** Runs `work` with Date.now frozen at `at` (so the threshold arithmetic is exact). */
export async function frozen<T>(at: number, work: () => Promise<T>): Promise<T> {
  const real = Date.now;
  Date.now = () => at;
  try { return await work(); } finally { Date.now = real; }
}
export const ctx = (remaining: number) => ({ getRemainingTimeInMillis: () => remaining });
export const reconciled = (lines: Array<Record<string, unknown>>) => lines.filter((l) => l.message === "search index reconciled");
export const deferred = (lines: Array<Record<string, unknown>>) => lines.filter((l) => l.message === "search index stream deferred, requeued");
