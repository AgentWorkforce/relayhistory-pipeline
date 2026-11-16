/**
 * Golden-snapshot helpers for "flags off is byte-identical" tests (summaries regeneration,
 * push A): a HoopDb wrapper that records every query's text and params in order, a capture
 * of the structured log lines (src/shared/log.ts writes JSON lines to stdout/stderr), and a
 * fixture compare that writes the fixture only when GOLDEN_WRITE=1.
 *
 * The fixtures were regenerated on main 4ff67765 (no regeneration code); a later change that
 * alters the flags-off SQL text, params or log lines fails here.
 */
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { expect } from "bun:test";
import type { HoopDb } from "../db.ts";

export interface Recorded {
  text: string;
  params: unknown[];
}

// Random lease ids (crypto.randomUUID); fixed test ids are v7, so they are kept.
const UUID_V4 = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
const ISO = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$/;

/** Values that differ run to run (wall clock, random IVs) are replaced by a stable tag. */
export function normalize(value: unknown, volatile: ReadonlySet<string> = new Set()): unknown {
  if (value instanceof Date) return "<date>";
  if (typeof value === "string") {
    if (ISO.test(value)) return "<iso>";
    if (volatile.has(value)) return "<volatile>";
    if (UUID_V4.test(value)) return "<uuid-v4>";
    // A W3C traceparent's span id is random per message.
    return value.replace(/(00-[0-9a-f]{32}-)[0-9a-f]{16}(-0[01])/g, "$1<span>$2");
  }
  if (Array.isArray(value)) return value.map((v) => normalize(v, volatile));
  if (value && typeof value === "object")
    return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, normalize(v, volatile)]));
  return value;
}

/** Wrap a HoopDb so every query (pool clients included) is recorded in order. */
export function recording(db: HoopDb): { db: HoopDb; log: Recorded[] } {
  const log: Recorded[] = [];
  const wrap = <T extends { query: HoopDb["query"] }>(target: T): T =>
    ({
      ...target,
      query: (text: string, params?: unknown[]) => {
        log.push({ text, params: params ? [...params] : [] });
        return target.query(text as never, params as never);
      },
    }) as T;
  const wrapped = {
    ...db,
    query: wrap(db).query,
    pool: {
      ...db.pool,
      connect: async () => {
        const client = await db.pool.connect();
        return Object.assign(Object.create(Object.getPrototypeOf(client)), client, wrap(client));
      },
    },
  } as unknown as HoopDb;
  return { db: wrapped, log };
}

/** Capture the JSON log lines written while `run` executes (ts dropped). */
export async function captureLogs<T>(run: () => Promise<T>): Promise<{ result: T; lines: Record<string, unknown>[] }> {
  const lines: Record<string, unknown>[] = [];
  const out = process.stdout.write.bind(process.stdout);
  const err = process.stderr.write.bind(process.stderr);
  const grab = (chunk: unknown) => {
    for (const line of String(chunk).split("\n")) {
      if (!line.trim().startsWith("{")) continue;
      try {
        const { ts: _ts, ...rest } = JSON.parse(line) as Record<string, unknown>;
        lines.push(rest);
      } catch {
        /* not a log line */
      }
    }
    return true;
  };
  process.stdout.write = ((c: unknown) => grab(c)) as typeof process.stdout.write;
  process.stderr.write = ((c: unknown) => grab(c)) as typeof process.stderr.write;
  try {
    return { result: await run(), lines };
  } finally {
    process.stdout.write = out;
    process.stderr.write = err;
  }
}

/** Compare against the committed fixture; GOLDEN_WRITE=1 (re)writes it instead. */
export function expectGolden(path: string, actual: unknown): void {
  const text = JSON.stringify(actual, null, 2) + "\n";
  if (process.env.GOLDEN_WRITE === "1") {
    writeFileSync(path, text);
    return;
  }
  if (!existsSync(path)) throw new Error(`golden fixture missing: ${path}`);
  expect(text).toBe(readFileSync(path, "utf8"));
}
