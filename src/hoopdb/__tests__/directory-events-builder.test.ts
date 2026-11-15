/**
 * DirectoryEventsBuilder against the whole-read function it replaced
 * (directory-events-oracle.ts, main at 1452b6ac): for generated inputs with
 * ties, repeats, conflicting repeats and malformed counters, in several arrival
 * orders, the builder returns the same metadata and fails on the same inputs.
 * The streaming reconcile (issue 610 Phase 2) adds rows chunk by chunk, so the
 * builder is also fed rows in reverse and shuffled orders.
 */
import { describe, expect, test } from "bun:test";
import {
  DirectoryEventsBuilder,
  directoryEventsFromProjection,
} from "../session-directory-events.ts";
import { legacyDirectoryEventsFromProjection } from "./directory-events-oracle.ts";

type Row = Record<string, unknown>;

function prng(seed: number) {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let t = state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function generate(seed: number, faults: boolean): Row[] {
  const random = prng(seed);
  const pick = <T,>(values: readonly T[]): T =>
    values[Math.floor(random() * values.length)]!;
  const chance = (p: number) => random() < p;
  const rows: Row[] = [];
  const size = 1 + Math.floor(random() * 40);
  for (let i = 0; i < size; i++) {
    if (rows.length > 0 && chance(0.1)) {
      // A repeat: the same row, a reordered copy, or (with faults) a changed one.
      const prior = pick(rows);
      const copy = Object.fromEntries(Object.entries(prior).reverse());
      if (faults && chance(0.3)) copy.content = `changed ${i}`;
      rows.push(copy);
      continue;
    }
    const create = pick([0, 0, 5, 7, null]);
    const row: Row = {
      machine_id: pick(["m1", "m2", "m\u00e9"]),
      source: "codex",
      session_id: "s1",
      kind: pick(["assistant", "user", "history", "session_event", "tool_call", "task", "prompt"]),
      event_id: pick(["e1", "e2", "e10", `e${i}`, `\u00e9${i % 3}`, `z${i}`]),
      ts: pick([
        "2026-10-01T00:00:00.000001Z",
        "2026-10-01T00:00:00Z",
        "2026-10-01T00:00:01.5Z",
        `2026-10-01T00:00:${String(i % 60).padStart(2, "0")}.000000Z`,
        "+012026-10-01T00:00:00Z",
      ]),
      actor_role: pick(["user", "assistant", null]),
      content: pick(["  ", "", "hello", null, " hi ", 7]),
      project_id: pick([null, "proj-a", "proj-b", "/home/a/repo", "~/b", "C:\\c", "file:x", "\u00e9proj"]),
      user_id: pick(["user-a", "user-b", "", null]),
      model: pick(["gpt", "claude", null, ""]),
      task_ref: pick([{}, null, { id: "pr:1", url: "u" }, { url: "u", id: "pr:1" }, { id: "pr:2" }]),
      task_title: pick([null, null, "Title one", "Title two", undefined]),
      task_status: pick([null, "done", "idle", "running", "weird", "COMPLETED"]),
      input_tokens: pick([0, 1, 2, "3", 10n, null]),
      output_tokens: pick([0, 4, null]),
      reasoning_tokens: pick([0, 1]),
      cache_read_tokens: pick([0, 9]),
      cache_create_tokens: create,
      cache_create_5m_tokens:
        create && chance(0.6) ? pick([create, 0, 2, null]) : pick([null, 0, 3]),
      cache_create_1h_tokens: pick([null, 0, 2, 5, 7]),
      cost_usd_micros: pick([null, null, 0, 12, "13"]),
      _index_fingerprint: pick(["s30123456789abcd", "s3fedcba98765432"]),
    };
    if (faults && chance(0.03)) row._index_fingerprint = "bad";
    if (faults && chance(0.04)) row.cache_create_5m_tokens = pick([-1, 1.5, "x"]);
    if (faults && chance(0.03)) row.cache_create_1h_tokens = "nope";
    if (faults && chance(0.02)) row.input_tokens = -2;
    if (faults && chance(0.02)) row.cost_usd_micros = "abc";
    if (faults && chance(0.02)) row.ts = "not a time";
    if (faults && chance(0.02)) row.task_title = pick([42, "x".repeat(201)]);
    if (faults && chance(0.02)) row.kind = "bad\u0001kind";
    if (faults && chance(0.02)) row.model = "m".repeat(1025);
    if (faults && chance(0.02)) row.task_ref = "scalar";
    if (faults && chance(0.02)) row.project_id = "p\u0007";
    if (faults && chance(0.02)) row.session_id = "";
    rows.push(row);
  }
  return rows;
}

type Outcome = { ok: unknown } | { failed: true };
function outcome(run: () => unknown): Outcome {
  try {
    return { ok: run() };
  } catch {
    return { failed: true };
  }
}
function viaBuilder(rows: readonly Row[]) {
  const builder = new DirectoryEventsBuilder();
  for (const row of rows) builder.add(row);
  return builder.finish();
}
function orders(rows: Row[], seed: number): Row[][] {
  const random = prng(seed ^ 0x9e3779b9);
  const shuffled = [...rows];
  for (let i = shuffled.length - 1; i > 0; i--) {
    const j = Math.floor(random() * (i + 1));
    [shuffled[i], shuffled[j]] = [shuffled[j]!, shuffled[i]!];
  }
  return [rows, [...rows].reverse(), shuffled];
}

describe("DirectoryEventsBuilder parity with the whole-read metadata", () => {
  for (const faults of [false, true])
    test(`same metadata and same failures, ${faults ? "with" : "without"} malformed rows`, () => {
      let compared = 0;
      let failures = 0;
      for (let seed = 1; seed <= 600; seed++) {
        const rows = generate(seed, faults);
        for (const order of orders(rows, seed)) {
          const expected = outcome(() => legacyDirectoryEventsFromProjection(order));
          expect({ seed, ...outcome(() => viaBuilder(order)) }).toEqual({ seed, ...expected });
          expect({ seed, ...outcome(() => directoryEventsFromProjection(order)) }).toEqual({ seed, ...expected });
          compared += 1;
          if ("failed" in expected) failures += 1;
        }
      }
      // The generator must reach both outcomes, or the comparison proves little.
      expect(compared).toBe(1800);
      expect(failures).toBeGreaterThan(faults ? 100 : 0);
      expect(compared - failures).toBeGreaterThan(faults ? 100 : 1000);
    });

  test("the metadata does not depend on arrival order when no two rows tie", () => {
    const rows = Array.from({ length: 50 }, (_, i) => ({
      machine_id: "m1",
      source: "codex",
      session_id: "s1",
      kind: i % 2 ? "assistant" : "user",
      event_id: `e${String(i).padStart(3, "0")}`,
      ts: `2026-10-01T00:${String(i).padStart(2, "0")}:00Z`,
      content: "text",
      project_id: i % 3 ? "/repo/a" : "/repo/b",
      input_tokens: i,
      cache_create_tokens: 0,
      task_title: i % 7 ? null : `t${i}`,
      task_status: i % 5 ? null : "done",
      _index_fingerprint: "s30123456789abcd",
    }));
    const expected = legacyDirectoryEventsFromProjection(rows);
    for (const order of orders(rows, 7)) expect(viaBuilder(order)).toEqual(expected);
  });

  test("the cache split fails only where the whole read evaluates the malformed counter", () => {
    const base = {
      machine_id: "m1",
      source: "codex",
      session_id: "s1",
      kind: "assistant",
      _index_fingerprint: "s30123456789abcd",
    };
    const early = { ...base, event_id: "a", ts: "2026-10-01T00:00:00Z" };
    const late = { ...base, event_id: "b", ts: "2026-10-01T00:00:01Z" };
    const incomplete = { cache_create_tokens: 5, cache_create_5m_tokens: null, cache_create_1h_tokens: null };
    const malformed = { cache_create_tokens: 5, cache_create_5m_tokens: "x" };
    const unused = { cache_create_tokens: 0, cache_create_5m_tokens: "x" };
    const cases: Row[][] = [
      // An earlier incomplete split stops the check before the malformed row.
      [{ ...late, ...malformed }, { ...early, ...incomplete }],
      // The malformed row comes first in canonical order: the check fails on it.
      [{ ...early, ...malformed }, { ...late, ...incomplete }],
      // Nothing to split on that row, but a complete split totals its counter.
      [{ ...early, ...unused }, { ...late, cache_create_tokens: 0 }],
      // An incomplete split never totals the malformed counter.
      [{ ...early, ...unused }, { ...late, ...incomplete }],
    ];
    const results = cases.map((rows) => {
      const expected = outcome(() => legacyDirectoryEventsFromProjection(rows));
      for (const order of orders(rows, 3)) expect(outcome(() => viaBuilder(order))).toEqual(expected);
      return "failed" in expected;
    });
    expect(results).toEqual([false, true, true, false]);
  });

  test("rows added out of list order with their order give the list-order metadata", () => {
    let ties = 0;
    let compared = 0;
    for (let seed = 1; seed <= 600; seed++) {
      const rows = generate(seed, true);
      const expected = outcome(() => legacyDirectoryEventsFromProjection(rows));
      const indexed = rows.map((row, i) => ({ row, order: i * 3 + 1 })); // distinct, ascending, not dense
      for (const arrival of orders(indexed as unknown as Row[], seed).slice(1) as unknown as Array<typeof indexed>) {
        const got = outcome(() => {
          const builder = new DirectoryEventsBuilder();
          for (const { row, order } of arrival) builder.add(row, order);
          return builder.finish();
        });
        if (!("failed" in expected)) expect({ seed, ...got }).toEqual({ seed, ...expected });
        else expect({ seed, failed: "failed" in got }).toEqual({ seed, failed: true });
        compared += 1;
      }
      const seen = new Set<string>();
      for (const r of rows) {
        const k = `${String(r.ts)}|${String(r.event_id)}`;
        if (seen.has(k)) ties += 1;
        seen.add(k);
      }
    }
    expect(compared).toBe(1200);
    expect(ties).toBeGreaterThan(100); // the order only matters where rows tie
    // Without the order the same arrivals may differ: the parameter is what restores list order.
    let differs = 0;
    for (let seed = 1; seed <= 600 && differs === 0; seed++) {
      const rows = generate(seed, false);
      const reversed = [...rows].reverse();
      const plain = outcome(() => viaBuilder(reversed));
      const ordered = outcome(() => {
        const builder = new DirectoryEventsBuilder();
        reversed.forEach((row, i) => builder.add(row, rows.length - i));
        return builder.finish();
      });
      if (JSON.stringify(plain) !== JSON.stringify(ordered)) differs += 1;
    }
    expect(differs).toBe(1);
  });

  test("a builder is used once", () => {
    const builder = new DirectoryEventsBuilder();
    builder.finish();
    expect(() => builder.finish()).toThrow("directory_events_builder_finished");
    expect(() => builder.add({})).toThrow("directory_events_builder_finished");
  });
});
