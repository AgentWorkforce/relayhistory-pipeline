/**
 * SERVICE=session-pipeline-output-facts-bootstrap without a database: the env gate and its
 * refusals, the ledger gate on a fake executor, and the Phase A/B driver against a fake
 * consumer that drains the queue over simulated time. The SQL itself runs on PGlite in
 * output-facts-bootstrap.pglite.test.ts.
 */
import { describe, expect, test } from "bun:test";
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import {
  ALLOW_PARTIAL_VAR,
  BootstrapRefused,
  DEADLINE_VAR,
  EXIT,
  OUTPUT_FACTS_MIGRATION,
  assertOutputFactsLedger,
  bootstrapConfig,
  bootstrapOptions,
  runOutputFactsBootstrap,
  serve,
  type MarkSummary,
  type OutputFactsBootstrapIo,
  type Progress,
} from "../output-facts-bootstrap.ts";

/** A complete consumer config with placeholder values (no credential: an unroutable local URL). */
const ENV: NodeJS.ProcessEnv = {
  RELAYHISTORY_SELFHOST_PIPELINE_CONSUMER: "true",
  RELAYHISTORY_SELFHOST_OUTPUT_FACTS: "true",
  RELAYHISTORY_PIPELINE_CONSUMER_SECRET: JSON.stringify({ hoopdb_url: "postgres://owner@127.0.0.1:1/hoop" }),
  SESSION_POSTGRES_TLS_PROFILE: "supabase",
  SEARCH_INDEX_BACKEND: "postgres",
  SEARCH_INDEX_POSTGRES_BACKEND_ID: "selfhost-test",
  PIPELINE_KEY_PREFIX: "96bulls",
  READ_API_COMPLETE_SINCE: "epoch",
};

describe("env gate", () => {
  test("a complete config passes with the defaults", () => {
    const { config, options } = bootstrapConfig(ENV);
    expect(config.enabled).toBe(true);
    expect(options).toEqual({ deadlineMs: 7_200_000, allowPartial: false });
  });

  for (const value of [undefined, "", "false", "TRUE", "true ", "1"])
    test(`RELAYHISTORY_SELFHOST_OUTPUT_FACTS=${JSON.stringify(value)} refuses`, () => {
      const env = { ...ENV, RELAYHISTORY_SELFHOST_OUTPUT_FACTS: value };
      expect(() => bootstrapConfig(env)).toThrow(BootstrapRefused);
      expect(() => bootstrapConfig(env)).toThrow("RELAYHISTORY_SELFHOST_OUTPUT_FACTS must be exactly true");
    });

  test("a dormant consumer config refuses (the job needs its backend id, prefix and owner secret)", () => {
    expect(() => bootstrapConfig({ ...ENV, RELAYHISTORY_SELFHOST_PIPELINE_CONSUMER: "false" })).toThrow(BootstrapRefused);
  });

  test("an invalid consumer config refuses, naming the variable and never the value", () => {
    const env = { ...ENV, RELAYHISTORY_PIPELINE_CONSUMER_SECRET: "not json" };
    expect(() => bootstrapConfig(env)).toThrow(BootstrapRefused);
    expect(() => bootstrapConfig({ ...ENV, SEARCH_INDEX_POSTGRES_BACKEND_ID: "" })).toThrow("SEARCH_INDEX_POSTGRES_BACKEND_ID");
  });

  test("deadline and allow-partial are strict", () => {
    expect(bootstrapOptions({ [DEADLINE_VAR]: "60", [ALLOW_PARTIAL_VAR]: "true" })).toEqual({ deadlineMs: 60_000, allowPartial: true });
    expect(bootstrapOptions({ [DEADLINE_VAR]: "", [ALLOW_PARTIAL_VAR]: "false" })).toEqual({ deadlineMs: 7_200_000, allowPartial: false });
    for (const bad of ["0", "-1", "1.5", "1e3", " 60", "86401", "abc"])
      expect(() => bootstrapOptions({ [DEADLINE_VAR]: bad })).toThrow(BootstrapRefused);
    for (const bad of ["TRUE", "1", "yes", "true "]) expect(() => bootstrapOptions({ [ALLOW_PARTIAL_VAR]: bad })).toThrow(BootstrapRefused);
  });

  test("serve refuses with exit 2 before opening any connection", async () => {
    expect(await serve({ ...ENV, RELAYHISTORY_SELFHOST_OUTPUT_FACTS: undefined })).toBe(EXIT.refused);
    expect(await serve({ ...ENV, [DEADLINE_VAR]: "0" })).toBe(EXIT.refused);
  });
});

describe("ledger gate", () => {
  const SHA = "a".repeat(64);
  const ledger = (table: boolean, rows: { name: string; sha256: string }[]): SqlExecutor => ({
    query: async <R>(text: string): Promise<{ rows: R[] }> => {
      if (text.includes("to_regclass")) return { rows: [{ t: table ? "sessions.__96bulls_migrations" : null }] as R[] };
      if (text.includes("FROM sessions.__96bulls_migrations")) return { rows: rows as R[] };
      throw new Error(`unexpected query: ${text}`);
    },
  });
  const expected = { names: ["upstream:20261025000000_sessions_index.sql", OUTPUT_FACTS_MIGRATION], shas: { [OUTPUT_FACTS_MIGRATION]: SHA } };

  test("045020 recorded with the planned checksum passes", async () => {
    await assertOutputFactsLedger(ledger(true, [{ name: OUTPUT_FACTS_MIGRATION, sha256: SHA }]), expected);
  });

  test("refuses: image does not plan 045020, no ledger, not recorded, wrong checksum", async () => {
    const refused = (p: Promise<void>) => expect(p).rejects.toBeInstanceOf(BootstrapRefused);
    await refused(assertOutputFactsLedger(ledger(true, [{ name: OUTPUT_FACTS_MIGRATION, sha256: SHA }]), { names: [] }));
    await refused(assertOutputFactsLedger(ledger(false, []), expected));
    await refused(assertOutputFactsLedger(ledger(true, [{ name: "upstream:20261025000000_sessions_index.sql", sha256: SHA }]), expected));
    await refused(assertOutputFactsLedger(ledger(true, [{ name: OUTPUT_FACTS_MIGRATION, sha256: "b".repeat(64) }]), expected));
  });
});

/**
 * A fake consumer: `drainPerTick` owing sessions get facts per simulated poll interval
 * while pending rows remain; `stuck` sessions never complete (no projectable manifest).
 */
function fakeConsumer(opts: { owing: number; stuck?: number; drainPerTick: number; inFlight?: number; markIncomplete?: number }) {
  let clock = 0;
  let owing = opts.owing;
  let inFlight = opts.inFlight ?? opts.owing - (opts.stuck ?? 0);
  const calls: string[] = [];
  const sleeps: number[] = [];
  const io: OutputFactsBootstrapIo = {
    enqueue: async () => {
      calls.push("enqueue");
      return { keys: inFlight, enqueued: inFlight, batches: 1 };
    },
    progress: async (): Promise<Progress> => {
      calls.push("progress");
      return { owing, inFlight, dead: 0 };
    },
    mark: async (): Promise<MarkSummary> => {
      calls.push("mark");
      const incomplete = opts.markIncomplete ?? (owing > 0 ? 1 : 0);
      return { workspaces: 2, complete: 2 - incomplete, incomplete, marked: 2 - incomplete, alreadyMarked: 0, markedIncomplete: 0 };
    },
    now: () => clock,
    sleep: async (ms) => {
      sleeps.push(ms);
      clock += ms;
      const done = Math.min(opts.drainPerTick, inFlight);
      inFlight -= done;
      owing -= done;
    },
  };
  return { io, calls, sleeps };
}

describe("Phase A/B driver", () => {
  test("enqueues, waits for the consumer to drain, then marks; exit 0", async () => {
    const f = fakeConsumer({ owing: 10, drainPerTick: 4 });
    const out = await runOutputFactsBootstrap(f.io, { deadlineMs: 3_600_000, allowPartial: false, pollMs: 1000 });
    expect(out.code).toBe(EXIT.ok);
    expect(out.deadlineReached).toBe(false);
    expect(out.progress).toEqual({ owing: 0, inFlight: 0, dead: 0 });
    expect(f.calls[0]).toBe("enqueue");
    expect(f.calls.at(-1)).toBe("mark");
    expect(f.calls.filter((c) => c === "mark")).toHaveLength(1);
    expect(f.sleeps).toEqual([1000, 1000, 1000]);
  });

  test("nothing owing: no wait at all", async () => {
    const f = fakeConsumer({ owing: 0, drainPerTick: 1 });
    const out = await runOutputFactsBootstrap(f.io, { deadlineMs: 1000, allowPartial: false, pollMs: 1000 });
    expect(out.code).toBe(EXIT.ok);
    expect(f.sleeps).toEqual([]);
  });

  test("queue drained but sessions still owe (no projectable manifest): stops early, exit 3", async () => {
    const f = fakeConsumer({ owing: 5, stuck: 2, drainPerTick: 10 });
    const out = await runOutputFactsBootstrap(f.io, { deadlineMs: 3_600_000, allowPartial: false, pollMs: 1000 });
    expect(f.sleeps).toEqual([1000]);
    expect(out.deadlineReached).toBe(false);
    expect(out.progress.owing).toBe(2);
    expect(out.code).toBe(EXIT.incomplete);
  });

  test("the deadline bounds the wait (the last sleep is clipped to it), then still marks", async () => {
    const f = fakeConsumer({ owing: 100, drainPerTick: 1 });
    const out = await runOutputFactsBootstrap(f.io, { deadlineMs: 2500, allowPartial: false, pollMs: 1000 });
    expect(f.sleeps).toEqual([1000, 1000, 500]);
    expect(out.deadlineReached).toBe(true);
    expect(f.calls.at(-1)).toBe("mark");
    expect(out.code).toBe(EXIT.incomplete);
  });

  test("ALLOW_PARTIAL turns the incomplete exit into 0 (the marks are unchanged)", async () => {
    const f = fakeConsumer({ owing: 100, drainPerTick: 1 });
    const out = await runOutputFactsBootstrap(f.io, { deadlineMs: 1000, allowPartial: true, pollMs: 1000 });
    expect(out.code).toBe(EXIT.ok);
    expect(out.mark.incomplete).toBe(1);
    expect(out.mark.marked).toBe(1);
  });

  test("a failure in any phase propagates (serve maps it to exit 1)", async () => {
    const f = fakeConsumer({ owing: 1, drainPerTick: 1 });
    await expect(runOutputFactsBootstrap({ ...f.io, mark: async () => { throw new Error("boom"); } }, { deadlineMs: 1000, allowPartial: false, pollMs: 1 }))
      .rejects.toThrow("boom");
  });
});
