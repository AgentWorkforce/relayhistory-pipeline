/**
 * The forward-ported backport flags (SEARCH_INDEX_SKIP_ACCEPTANCE_MISSING_ENABLED, SEARCH_INDEX_SKIP_OVER_BOUND_ENABLED)
 * unset must leave the hosted search path byte-identical to the base commit 9d1adf84: the consumer's event path
 * (processBatch, search-index-hosted-harness.ts) and the managed search-jobs pass (runSearchJob), every SQL
 * statement of the search transaction with its parameters, every ClickHouse call, the object reads, the results
 * and the search stage's log records, compared to a golden captured at 9d1adf84 before either flag existed.
 * Each scenario repeats its pass: the repeat is exactly what a flag would skip.
 *
 * The M2 scenario is also replayed with both flags ON: the managed pass never applies them, so its transcript
 * must equal the flag-off golden too.
 *
 * Regenerate (only on purpose, from the base commit):
 *   UPDATE_FORWARD_PORT_OFF_GOLDEN=1 bun test ./src/session-pipeline/__tests__/search-index-forward-port-off-golden.pglite.test.ts
 *
 * Synthetic, secret-free data only.
 */
import { afterAll, afterEach, beforeAll, expect, test } from "bun:test";
import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import type { SearchIndexDeps } from "../search-index.ts";
import { runSearchJob } from "../search-jobs.ts";
import { SCOPE, activity, exportStreamScope, generate, type ExportedScope, type Line } from "../../session-read-api/__tests__/stream-fixture.ts";
import { closeReplayTemplate, warmReplayTemplate } from "./replay-fixture.ts";
import { crossManifestAcceptanceLines } from "./acceptance-fixture.ts";
import { hostedRig, plain } from "./search-index-hosted-harness.ts";

const GOLDEN = join(import.meta.dir, "fixtures", "search-index-forward-port-off.golden.json");
const TINY = { maxScopeBytes: 1, maxScopeEvents: 1 };
/** Below four whole reads of the fixture: the streamed path refuses it too. */
const SMALL_STREAM = { maxReadBytes: 8, maxReadEvents: 8 };
/** Both forward-ported flags on (cast: the base commit has neither field). */
const BOTH_ON = { skipAcceptanceMissing: true, skipOverBound: true } as unknown as Partial<SearchIndexDeps>;

const cleanup: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  for (const c of cleanup.splice(0).reverse()) await c();
});
beforeAll(() => warmReplayTemplate(), 120_000);
afterAll(() => closeReplayTemplate());

/** generate(5) plus two origins contending for one record id without acceptance seats (relayhistory_acceptance_missing). */
export function acceptanceLines(): Line[] {
  const lines: Line[] = generate(5);
  const machine = lines[0]!.machine;
  lines.push(
    activity(machine, `capacity:${machine}`, "session_event", "unseated", Date.parse("2026-11-01T10:00:00.600Z"), { role: "user", text: "a" }),
    activity(machine, "capacity:other", "session_event", "unseated", Date.parse("2026-11-01T10:00:00.700Z"), { role: "user", text: "b" }),
  );
  return lines;
}

type Entry = Record<string, unknown>;
type Rig = Awaited<ReturnType<typeof hostedRig>>;
interface Scenario {
  name: string;
  lines: () => Line[];
  runs: Array<Partial<SearchIndexDeps>>;
  /** Run the managed search-jobs pass instead of the S3-event consumer. */
  m2?: boolean;
}
const SCENARIOS: Scenario[] = [
  { name: "whole read: complete, then the same event again", lines: () => generate(31), runs: [{}, {}] },
  { name: "whole read: too large without streaming stays partial, then again", lines: () => generate(33), runs: [{ limits: TINY }, { limits: TINY }] },
  { name: "too large for the stream budgets too: partial, then again", lines: () => generate(33), runs: [{ limits: TINY, stream: SMALL_STREAM }, { limits: TINY, stream: SMALL_STREAM }] },
  { name: "acceptance missing: incomplete, then the same event again", lines: acceptanceLines, runs: [{}, {}] },
  { name: "M2 managed pass on an acceptance-held scope: held, then held again", lines: acceptanceLines, runs: [{}, {}], m2: true },
  { name: "acceptance missing across manifests: incomplete, then the same event again", lines: crossManifestAcceptanceLines, runs: [{}, {}] },
  { name: "M2 managed pass, acceptance missing across manifests: held, then held again", lines: crossManifestAcceptanceLines, runs: [{}, {}], m2: true },
];

/** The managed wake of the rig's canonical scope at its current directory epoch (design §2 message). */
async function wakeOf(r: Rig, x: ExportedScope) {
  const { rows } = await r.db.pg.query<{ epoch: string }>(
    "SELECT event_projection_epoch::text AS epoch FROM sessions.session_directory WHERE org_id=$1 AND workspace_id=$2 AND harness=$3 AND session_id=$4",
    [SCOPE.orgId, SCOPE.workspaceId, SCOPE.harness, x.canonicalId]);
  const scope = { orgId: SCOPE.orgId, workspaceId: SCOPE.workspaceId, harness: SCOPE.harness, sessionId: x.canonicalId };
  return {
    v: 1, stage: "search-index",
    ctx: { traceId: "00000000-0000-4000-8000-000000000001", ...scope },
    workKey: ["search", scope.orgId, scope.workspaceId, scope.harness, scope.sessionId].join("\u001f"),
    revision: 1,
    payload: { epoch: Number(rows[0]!.epoch), clock: { source: "publish", at: "2026-11-01T12:00:00.000Z" } },
  };
}

async function record(s: Scenario, overlay: Partial<SearchIndexDeps> = {}): Promise<Entry[]> {
  const x = await exportStreamScope(s.lines(), 1200);
  cleanup.push(x.dispose);
  const r = await hostedRig(x, cleanup);
  // The M2 scenario starts from a committed scope: one S3-event pass (flags off) first.
  if (s.m2) await r.tick(overlay);
  const out: Entry[] = [];
  for (const [i, extra] of s.runs.entries()) {
    r.sql.length = 0;
    r.fake.calls.length = 0;
    const reads = r.reads.count;
    const lines: Entry[] = [];
    const stdout = process.stdout.write, stderr = process.stderr.write;
    const tap = (w: typeof process.stdout.write, stream: NodeJS.WriteStream) =>
      ((chunk: unknown, ...rest: unknown[]) => {
        for (const line of String(chunk).split("\n"))
          if (line.startsWith("{"))
            try {
              const { ts: _ts, duration_ms: _d, event_age_ms: _ea, s3_event_age_ms: _sa, queue_origin: _qo, contender_seats: _cs, source_read_ms: _sr, index_scan_ms: _is, locked_apply_ms: _la,
                write_calls: _wc, write_batches: _wb, ...entry } = JSON.parse(line) as Entry;
              if (entry.stage === "search-index") lines.push(entry);
            } catch {}
        return (w as (...a: unknown[]) => boolean).call(stream, chunk, ...rest);
      }) as typeof process.stdout.write;
    process.stdout.write = tap(stdout, process.stdout);
    process.stderr.write = tap(stderr, process.stderr);
    let result: unknown;
    try {
      result = s.m2
        ? plain(await runSearchJob((await wakeOf(r, x)) as never, { search: r.search({ ...extra, ...overlay }), now: () => Date.parse("2026-11-01T12:00:01.000Z") }))
        : plain(await r.tick({ ...extra, ...overlay }));
    } finally {
      process.stdout.write = stdout;
      process.stderr.write = stderr;
    }
    // contenderSeats is additive telemetry (the backport's 9279264f), pinned in stream-protections.pglite.test.ts.
    const stripped = JSON.parse(JSON.stringify(result, (k, v) => (k === "contenderSeats" ? undefined : v)));
    out.push({ run: i, result: stripped, reads: r.reads.count - reads, sql: [...r.sql], clickhouse: [...r.fake.calls], log: lines });
  }
  return out;
}

test("both forward-ported flags unset: SQL, ClickHouse calls, reads, results and logs equal the 9d1adf84 golden", async () => {
  const got: Record<string, Entry[]> = {};
  for (const s of SCENARIOS) got[s.name] = await record(s);
  if (process.env.UPDATE_FORWARD_PORT_OFF_GOLDEN === "1") writeFileSync(GOLDEN, JSON.stringify(got, null, 1) + "\n");
  const golden = JSON.parse(readFileSync(GOLDEN, "utf8")) as Record<string, Entry[]>;
  expect(Object.keys(got)).toEqual(Object.keys(golden));
  for (const s of SCENARIOS) expect(got[s.name]).toEqual(golden[s.name]!);
  // Fixture soundness: the acceptance scenario is the refusal the flag targets, and its repeat reads again.
  const acceptance = golden[SCENARIOS[3]!.name]!;
  expect(JSON.stringify(acceptance[0]!.log)).toContain("relayhistory_acceptance_missing");
  expect(acceptance[1]!.reads as number).toBeGreaterThan(0);
  expect((golden[SCENARIOS[4]!.name]![1]!.result as { outcome: string }).outcome).toBe("held");
}, 300_000);

test("M2 managed pass with both flags ON: byte-identical to the flag-off golden (the managed pass never skips)", async () => {
  const golden = JSON.parse(readFileSync(GOLDEN, "utf8")) as Record<string, Entry[]>;
  for (const m2 of SCENARIOS.filter((s) => s.m2)) expect(await record(m2, BOTH_ON)).toEqual(golden[m2.name]!);
}, 300_000);
