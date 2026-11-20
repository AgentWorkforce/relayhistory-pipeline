/**
 * SEARCH_INDEX_SKIP_CURRENT_ENABLED unset must leave the hosted search path byte-identical to the base commit:
 * the consumer's event path (processBatch) on the legacy ClickHouse binding (search-index-hosted-harness.ts),
 * every SQL statement of the search transaction with its parameters, every ClickHouse call, the object reads,
 * the batch results and the search stage's log records, compared to a golden captured at the base commit
 * before the skip existed. Each scenario repeats its pass: the repeat is exactly what the flag would skip.
 *
 * Regenerate (only on purpose, from the base commit):
 *   UPDATE_SKIP_OFF_GOLDEN=1 bun test ./src/session-pipeline/__tests__/search-index-skip-off-golden.test.ts
 *
 * Synthetic, secret-free data only.
 */
import { afterAll, afterEach, beforeAll, expect, test } from "bun:test";
import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import type { SearchIndexDeps } from "../search-index.ts";
import { exportStreamScope, generate } from "../../session-read-api/__tests__/stream-fixture.ts";
import { closeReplayTemplate, warmReplayTemplate } from "./replay-fixture.ts";
import { hostedRig, plain } from "./search-index-hosted-harness.ts";

const GOLDEN = join(import.meta.dir, "fixtures", "search-index-skip-off.golden.json");
const TINY = { maxScopeBytes: 1, maxScopeEvents: 1 };
const UNLIMITED = { maxReadBytes: 1 << 30, maxReadEvents: 1 << 24 };

const cleanup: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  for (const c of cleanup.splice(0).reverse()) await c();
});
beforeAll(() => warmReplayTemplate(), 120_000);
afterAll(() => closeReplayTemplate());

type Entry = Record<string, unknown>;
interface Scenario {
  name: string;
  seed: number;
  runs: Array<Partial<SearchIndexDeps>>;
}
const SCENARIOS: Scenario[] = [
  { name: "whole read: complete, then the same event again", seed: 31, runs: [{}, {}] },
  { name: "whole read: too large without streaming stays partial, then again", seed: 33, runs: [{ limits: TINY }, { limits: TINY }] },
  { name: "streamed (ClickHouse keyset pages): complete, then again", seed: 33, runs: [{ limits: TINY, stream: UNLIMITED }, { limits: TINY, stream: UNLIMITED }] },
];

async function record(s: Scenario): Promise<Entry[]> {
  const x = await exportStreamScope(generate(s.seed), 1200);
  cleanup.push(x.dispose);
  const r = await hostedRig(x, cleanup);
  const out: Entry[] = [];
  for (const [i, extra] of s.runs.entries()) {
    r.sql.length = 0;
    r.fake.calls.length = 0;
    const reads = r.reads.count;
    const lines: Entry[] = [];
    let phaseMetricError: string | undefined;
    const stdout = process.stdout.write, stderr = process.stderr.write;
    const tap = (w: typeof process.stdout.write, stream: NodeJS.WriteStream) =>
      ((chunk: unknown, ...rest: unknown[]) => {
        for (const line of String(chunk).split("\n"))
          if (line.startsWith("{"))
            try {
              const parsed = JSON.parse(line) as Entry;
              if (parsed.message === "search index reconciled")
                for (const field of ["source_read_ms", "index_scan_ms", "locked_apply_ms", "write_calls", "write_batches"])
                  if (typeof parsed[field] !== "number" || (parsed[field] as number) < 0) phaseMetricError = `missing search phase metric ${field}`;
              // Additive timing fields are validated above, then removed from the pre-existing
              // flag-off behavior golden because wall time is intentionally nondeterministic.
              const { ts: _ts, duration_ms: _d, source_read_ms: _sr, index_scan_ms: _is, locked_apply_ms: _la,
                write_calls: _wc, write_batches: _wb, ...entry } = parsed;
              if (entry.stage === "search-index") lines.push(entry);
            } catch {}
        return (w as (...a: unknown[]) => boolean).call(stream, chunk, ...rest);
      }) as typeof process.stdout.write;
    process.stdout.write = tap(stdout, process.stdout);
    process.stderr.write = tap(stderr, process.stderr);
    let result: unknown;
    try {
      result = plain(await r.tick(extra));
    } finally {
      process.stdout.write = stdout;
      process.stderr.write = stderr;
    }
    if (phaseMetricError) throw new Error(phaseMetricError);
    out.push({ run: i, result, reads: r.reads.count - reads, sql: [...r.sql], clickhouse: [...r.fake.calls], log: lines });
  }
  return out;
}

test("skip-current unset: SQL, ClickHouse calls, reads, results and log records equal the base commit's golden", async () => {
  const got: Record<string, Entry[]> = {};
  for (const s of SCENARIOS) got[s.name] = await record(s);
  if (process.env.UPDATE_SKIP_OFF_GOLDEN === "1") writeFileSync(GOLDEN, JSON.stringify(got, null, 1) + "\n");
  const golden = JSON.parse(readFileSync(GOLDEN, "utf8")) as Record<string, Entry[]>;
  expect(Object.keys(got)).toEqual(Object.keys(golden));
  for (const s of SCENARIOS) expect(got[s.name]).toEqual(golden[s.name]!);
  // The golden holds the repeated passes the flag would skip: a complete pass, repeated, read the source
  // again (the partial scenario is refused from its declared size and reads no chunk).
  for (const s of [SCENARIOS[0]!, SCENARIOS[2]!]) {
    expect(JSON.stringify(golden[s.name]![0]!.log)).toContain('"ok":true');
    expect(golden[s.name]![1]!.reads as number).toBeGreaterThan(0);
  }
}, 300_000);
