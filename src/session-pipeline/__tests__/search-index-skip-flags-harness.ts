/**
 * Shared pieces of the forward-ported skip-flag tests (SEARCH_INDEX_SKIP_ACCEPTANCE_MISSING_ENABLED,
 * SEARCH_INDEX_SKIP_OVER_BOUND_ENABLED): a ClickHouse wrapper over the hosted harness's fake that answers the
 * `search_ingest_state` and `search_ingest_session_state` reads from what the pass inserted, with failure
 * switches, and a driver for the real exported Lambda handler over the harness's consumer deps.
 * Synthetic, secret-free data only.
 */
import type { BatchResponse, ConsumerDeps } from "../consumer.ts";
import { createPipelineLambdaHandler, type Runtime } from "../lambda.ts";
import type { ClickHouseClient } from "../vendor/search-index/clickhouse.ts";
import { INGEST_STATE_TABLE, SESSION_STATE_TABLE } from "../vendor/search-index/schema.ts";
import type { fakeClickHouse } from "./search-index-hosted-harness.ts";

const chMs = (ts: unknown) => Date.parse(`${String(ts).replace(" ", "T")}Z`);

/** The fake's ClickHouse, plus the two state tables read back as ClickHouse would answer them. */
export function statefulClickHouse(fake: ReturnType<typeof fakeClickHouse>) {
  const ingest: Array<{ name: string; value: string; updated_at: string }> = [];
  const switches = { failStampRead: false, failStateRead: false, failIngestInsert: false, window: null as string | null };
  const ch: ClickHouseClient = {
    ...fake.ch,
    async select<T>(query: string, options?: { params?: Record<string, unknown>; timeoutMs?: number }) {
      if (query.includes(`FROM ${INGEST_STATE_TABLE}`) && query.includes("ORDER BY updated_at DESC")) {
        await fake.ch.select(query, options as never);
        if (switches.failStampRead) throw new Error("synthetic stamp read failure");
        const name = options?.params?.name;
        const row = ingest.filter((r) => r.name === name).sort((a, b) => chMs(b.updated_at) - chMs(a.updated_at))[0];
        return (row ? [{ value: row.value, at_ms: chMs(row.updated_at) }] : []) as T[];
      }
      if (query.includes(`FROM ${INGEST_STATE_TABLE}`) && query.includes("argMax(value, updated_at)")) {
        await fake.ch.select(query, options as never);
        return (switches.window === null || options?.params?.name !== "window_start" ? [] : [{ value: switches.window, n: 1 }]) as T[];
      }
      if (query.includes(`FROM ${SESSION_STATE_TABLE}`)) {
        fake.calls.push({ ch: "select", query: query.trim().replace(/\s+/g, " "), params: JSON.parse(JSON.stringify(options?.params ?? {})) });
        if (switches.failStateRead) throw new Error("synthetic state read failure");
        const p = options?.params ?? {};
        const row = fake.state
          .filter((s) => s.org_id === p.org && s.session_id === p.session)
          .sort((a, b) => chMs(b.synced_at) - chMs(a.synced_at))[0];
        return (row ? [{ revision: String(row.revision), needs_full: row.needs_full, scanned_rows: row.scanned_rows, synced_ms: chMs(row.synced_at) }] : []) as T[];
      }
      return fake.ch.select<T>(query, options as never);
    },
    async insert(table, rows, options) {
      if (table === INGEST_STATE_TABLE) {
        if (switches.failIngestInsert) {
          fake.calls.push({ ch: "insert", table, rows: JSON.parse(JSON.stringify(rows)), failed: true });
          throw new Error("synthetic insert refusal");
        }
        ingest.push(...(rows as typeof ingest));
      }
      return fake.ch.insert(table, rows, options as never);
    },
  };
  return { ch, ingest, switches };
}

/** The real exported Lambda handler over `deps` (loadRuntime replaces init's secret, pool and S3). */
export function realHandler(deps: ConsumerDeps, env: Partial<Runtime["env"]> = {}) {
  const rt = { env, deps } as unknown as Runtime;
  const handler = createPipelineLambdaHandler({ loadRuntime: async () => rt });
  return (messages: Array<{ messageId: string; body: string; receiptHandle?: string }>, context?: { getRemainingTimeInMillis(): number }) =>
    handler({ Records: messages } as never, context as never) as Promise<BatchResponse>;
}

/** Runs `work` with stdout/stderr tapped; returns the JSON log lines it wrote. */
export async function captureLogs<T>(work: () => Promise<T>): Promise<{ result: T; lines: Array<Record<string, unknown>> }> {
  const lines: Array<Record<string, unknown>> = [];
  const out = process.stdout.write, err = process.stderr.write;
  const tap = (w: typeof process.stdout.write, stream: NodeJS.WriteStream) =>
    ((chunk: unknown, ...rest: unknown[]) => {
      for (const l of String(chunk).split("\n"))
        if (l.startsWith("{"))
          try { lines.push(JSON.parse(l) as Record<string, unknown>); } catch {}
      return (w as (...a: unknown[]) => boolean).call(stream, chunk, ...rest);
    }) as typeof process.stdout.write;
  process.stdout.write = tap(out, process.stdout);
  process.stderr.write = tap(err, process.stderr);
  try {
    return { result: await work(), lines };
  } finally {
    process.stdout.write = out;
    process.stderr.write = err;
  }
}
