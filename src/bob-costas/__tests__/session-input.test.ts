import { describe, expect, test } from "bun:test";
import { zstdCompressSync } from "node:zlib";
import { rawChunkKey } from "../../shared/vendor.ts";
import {
  DEFAULT_SUMMARY_INPUT_CAPS,
  planChunks,
  readSessionDigest,
  summaryInputCapsFromEnv,
  type SummaryInputCaps,
} from "../session-input.ts";
import {
  RETAINED_HEAD_TURNS,
  RETAINED_MIDDLE_TURNS,
  RETAINED_TAIL_TURNS,
  RETAINED_USER_TURNS,
  TURN_TEXT_CHARS,
  TurnSampler,
  digestRecords,
  extractiveSummarizer,
} from "../summarizer.ts";
import { buildPromptInput, promptTurns } from "../model-summarizer.ts";

const MiB = 1024 * 1024;
const loc = { workspaceId: "w", machineId: "m", harness: "claude", sessionId: "s1", startDate: "2026-11-16" };

const user = (text: string) => JSON.stringify({ type: "user", message: { role: "user", content: text } });
const assistant = (tool: string) =>
  JSON.stringify({ type: "assistant", message: { role: "assistant", model: "m1", content: [{ type: "tool_use", name: tool, input: {} }] } });
/** A Claude tool result: role user, but no text part, so not a turn. Bulk for the byte count. */
const toolResult = (bytes: number) =>
  JSON.stringify({ type: "user", message: { role: "user", content: [{ type: "tool_result", content: "r".repeat(bytes) }] } });

/**
 * A session of `count` chunks whose bodies are built on demand at GET time, so the test
 * itself never holds the session: chunk i holds one user turn `turn <i>`, one assistant
 * tool call and `bulk` bytes of tool output.
 */
function syntheticSession(count: number, bulk: number, opts: { declare?: boolean } = {}) {
  const body = (i: number) => [user(`turn ${i}`), assistant("Bash"), toolResult(bulk)].join("\n") + "\n";
  const declaredBytes = Buffer.byteLength(body(0));
  const chunks = Array.from({ length: count }, (_, i) => ({
    seq: i,
    start_offset: i * declaredBytes,
    sha256_8: i.toString(16).padStart(8, "0"),
    ...(opts.declare === false ? {} : { bytes: declaredBytes }),
  }));
  const seqOf = new Map(chunks.map((c) => [String(rawChunkKey(loc, c.seq, c.start_offset, c.sha256_8, "jsonl")), c.seq]));
  const fetched: number[] = [];
  let peakHeap = 0;
  const store = {
    get: async (key: unknown) => {
      const i = seqOf.get(String(key));
      if (i === undefined) throw Object.assign(new Error("missing"), { code: "not_found" });
      fetched.push(i);
      // Sample the heap at every GET: the previous chunks must be collectable by now.
      if (fetched.length % 16 === 0) {
        Bun.gc(true);
        peakHeap = Math.max(peakHeap, process.memoryUsage().heapUsed);
      }
      return new Blob([zstdCompressSync(Buffer.from(body(i)))]).stream();
    },
  };
  const manifest = {
    session: { harness: "claude", workspace_id: "w", machine_id: "m", session_id: "s1", started_at: "2026-11-16T00:00:00Z" },
    raw: { format: "jsonl", scrub: { ruleset: "r", version: 1 }, chunks },
  };
  return { manifest: manifest as never, store: store as never, fetched, peak: () => peakHeap, declaredBytes };
}

describe("planChunks", () => {
  const sized = (n: number, bytes = 1_000) => Array.from({ length: n }, () => ({ bytes }));
  const caps = (over: Partial<SummaryInputCaps> = {}): SummaryInputCaps => ({ ...DEFAULT_SUMMARY_INPUT_CAPS, ...over });

  test("a session within the caps is read whole", () => {
    expect(planChunks(sized(10), caps())).toEqual({ indices: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9], sampled: false });
    expect(planChunks([], caps())).toEqual({ indices: [], sampled: false });
  });

  test("past the chunk cap: head, tail and an evenly spaced middle, ascending and distinct", () => {
    const { indices, sampled } = planChunks(sized(3_500), caps({ maxChunks: 100 }));
    expect(sampled).toBe(true);
    expect(indices.length).toBe(100);
    expect(new Set(indices).size).toBe(100);
    expect([...indices].sort((a, b) => a - b)).toEqual(indices);
    // A quarter from each end, contiguous.
    expect(indices.slice(0, 25)).toEqual(Array.from({ length: 25 }, (_, i) => i));
    expect(indices.slice(-25)).toEqual(Array.from({ length: 25 }, (_, i) => 3_475 + i));
    // The middle is evenly spaced: gaps differ by at most one.
    const middle = indices.slice(25, 75);
    const gaps = middle.slice(1).map((v, i) => v - middle[i]!);
    expect(Math.max(...gaps) - Math.min(...gaps)).toBeLessThanOrEqual(1);
    expect(middle[0]!).toBeGreaterThanOrEqual(25);
    expect(middle.at(-1)!).toBeLessThan(3_475);
  });

  test("past the byte cap: the chunk budget shrinks to what the bytes hold", () => {
    const { indices, sampled } = planChunks(sized(1_000, 1 * MiB), caps({ maxInputBytes: 64 * MiB }));
    expect(sampled).toBe(true);
    expect(indices.length).toBe(64);
    expect(indices[0]).toBe(0);
    expect(indices.at(-1)).toBe(999);
  });

  test("a chunk declared over the per-chunk cap is never read", () => {
    const chunks = [{ bytes: 10 }, { bytes: 64 * MiB }, { bytes: 10 }];
    expect(planChunks(chunks, caps())).toEqual({ indices: [0, 2], sampled: true });
  });

  test("caps read from env; out-of-range or malformed values keep the defaults", () => {
    expect(summaryInputCapsFromEnv({})).toEqual({ ...DEFAULT_SUMMARY_INPUT_CAPS });
    expect(summaryInputCapsFromEnv({ BOB_COSTAS_MAX_CHUNKS: "64", BOB_COSTAS_MAX_INPUT_BYTES: String(32 * MiB) }))
      .toMatchObject({ maxChunks: 64, maxInputBytes: 32 * MiB });
    expect(summaryInputCapsFromEnv({ BOB_COSTAS_MAX_CHUNKS: "0", BOB_COSTAS_MAX_INPUT_BYTES: "-1", BOB_COSTAS_MAX_LINE_BYTES: "lots" }))
      .toEqual({ ...DEFAULT_SUMMARY_INPUT_CAPS });
  });
});

describe("TurnSampler keeps a bounded, evenly spaced sample in order", () => {
  test("a short session keeps every turn", () => {
    const s = new TurnSampler();
    for (let i = 0; i < RETAINED_USER_TURNS; i++) s.add(`t${i}`);
    expect(s.turns()).toEqual(Array.from({ length: RETAINED_USER_TURNS }, (_, i) => `t${i}`));
  });

  test("10,000 turns: the head, the tail and an evenly spaced middle", () => {
    const s = new TurnSampler();
    const n = 10_000;
    for (let i = 0; i < n; i++) s.add(`${i}`);
    expect(s.count).toBe(n);
    const kept = s.turns().map(Number);
    expect(kept.length).toBeLessThanOrEqual(RETAINED_USER_TURNS);
    expect(kept.slice(0, RETAINED_HEAD_TURNS)).toEqual(Array.from({ length: RETAINED_HEAD_TURNS }, (_, i) => i));
    expect(kept.slice(-RETAINED_TAIL_TURNS)).toEqual(Array.from({ length: RETAINED_TAIL_TURNS }, (_, i) => n - RETAINED_TAIL_TURNS + i));
    const middle = kept.slice(RETAINED_HEAD_TURNS, -RETAINED_TAIL_TURNS);
    expect(middle.length).toBeGreaterThanOrEqual(RETAINED_MIDDLE_TURNS / 2);
    const gaps = new Set(middle.slice(1).map((v, i) => v - middle[i]!));
    expect(gaps.size).toBe(1); // one stride: evenly spaced
    expect([...kept].sort((a, b) => a - b)).toEqual(kept);
  });

  test("a kept turn is clipped", () => {
    const s = new TurnSampler();
    s.add("x".repeat(TURN_TEXT_CHARS * 3));
    expect(s.turns()[0]!.length).toBe(TURN_TEXT_CHARS);
  });
});

describe("readSessionDigest", () => {
  test("a small session is read whole, unmarked, and digests as before", async () => {
    const { manifest, store } = syntheticSession(5, 100);
    const d = await readSessionDigest({ manifest, location: loc, store });
    expect(d.input).toBeUndefined();
    expect(d.userTurnCount).toBe(5);
    expect(d.userTurns).toEqual(["turn 0", "turn 1", "turn 2", "turn 3", "turn 4"]);
    expect(d.toolCalls).toEqual({ Bash: 5 });
    // The same lines through digestRecords give the same digest.
    const lines = Array.from({ length: 5 }, (_, i) => [user(`turn ${i}`), assistant("Bash"), toolResult(100)]).flat();
    expect(d).toEqual(digestRecords("claude", lines));
  });

  test("3,500 chunks and ~1.4 GB: summarises from a bounded sample with bounded heap", async () => {
    const bulk = 400 * 1024;
    const session = syntheticSession(3_500, bulk);
    Bun.gc(true);
    const before = process.memoryUsage().heapUsed;
    const d = await readSessionDigest({ manifest: session.manifest, location: loc, store: session.store });
    const total = 3_500 * session.declaredBytes;
    expect(total).toBeGreaterThan(1.3e9);
    // Caps: at most maxChunks chunks and maxInputBytes bytes were read.
    expect(session.fetched.length).toBeLessThanOrEqual(DEFAULT_SUMMARY_INPUT_CAPS.maxChunks);
    expect(d.input).toBeDefined();
    expect(d.input!.chunksTotal).toBe(3_500);
    expect(d.input!.chunksRead).toBe(session.fetched.length);
    expect(d.input!.bytesRead).toBeLessThanOrEqual(DEFAULT_SUMMARY_INPUT_CAPS.maxInputBytes);
    // Head and tail of the session are both in the digest.
    expect(session.fetched[0]).toBe(0);
    expect(session.fetched.at(-1)).toBe(3_499);
    expect(d.userTurns[0]).toBe("turn 0");
    expect(d.userTurns.at(-1)).toBe("turn 3499");
    expect(d.userTurnCount).toBe(d.input!.chunksRead);
    expect(d.userTurns.length).toBeLessThanOrEqual(RETAINED_USER_TURNS);
    // Memory: the heap never held more than a few chunks' worth, far below the bytes read.
    const growth = session.peak() - before;
    expect(growth).toBeLessThan(64 * MiB);
    expect(d.input!.bytesRead).toBeGreaterThan(150 * MiB);
    // The summaries say the input was sampled.
    const long = await extractiveSummarizer.summarize("long", d);
    expect(long.text).toContain(`- Input: sampled ${d.input!.chunksRead} of 3500 chunks`);
    const input = JSON.parse(buildPromptInput("long", d));
    expect(input.input_sampled).toEqual({ chunks_read: d.input!.chunksRead, chunks_total: 3_500 });
    expect(input.user_turns[0]).toBe("turn 0");
    expect(input.user_turns.at(-1)).toBe("turn 3499");
  }, 120_000);

  test("undeclared sizes: the byte budget and the per-chunk cap still hold at read time", async () => {
    const session = syntheticSession(40, 64 * 1024, { declare: false });
    const caps = { ...DEFAULT_SUMMARY_INPUT_CAPS, maxInputBytes: 1 * MiB, maxChunkBytes: 128 * 1024 };
    const d = await readSessionDigest({ manifest: session.manifest, location: loc, store: session.store, caps });
    expect(d.input!.bytesRead).toBeLessThanOrEqual(1 * MiB);
    expect(d.input!.chunksRead).toBeLessThan(40);
    const tiny = { ...caps, maxInputBytes: 100 * MiB, maxChunkBytes: 64 * 1024 };
    const none = await readSessionDigest({ manifest: session.manifest, location: loc, store: session.store, caps: tiny });
    // Every chunk inflates past 64 KiB: none is read, and the digest is empty but marked.
    expect(none.input).toMatchObject({ chunksRead: 0, chunksTotal: 40 });
    expect(none.records).toBe(0);
  });

  test("a line over the line cap is counted, not parsed", async () => {
    const session = syntheticSession(2, 64 * 1024);
    const caps = { ...DEFAULT_SUMMARY_INPUT_CAPS, maxLineBytes: 16 * 1024 };
    const d = await readSessionDigest({ manifest: session.manifest, location: loc, store: session.store, caps });
    expect(d.input).toMatchObject({ linesSkipped: 2, chunksRead: 2 });
    expect(d.records).toBe(6);
    expect(d.userTurnCount).toBe(2);
  });

  test("a missing chunk is retryable chunk_missing, as before", async () => {
    const session = syntheticSession(2, 10);
    const store = { get: async () => { throw Object.assign(new Error("gone"), { code: "not_found" }); } };
    await expect(readSessionDigest({ manifest: session.manifest, location: loc, store: store as never }))
      .rejects.toMatchObject({ code: "chunk_missing", retryable: true });
  });
});

describe("envelope cap", () => {
  const env = (i: number, rev = 1) =>
    JSON.stringify({
      kind: "session_event", origin_id: "o", record_id: `r${i}`, revision: String(rev), operation: "upsert",
      received_at: "2026-11-16T00:00:00Z", payload: { kind: "text", role: "user", text: `prompt ${i}`, ts_ms: 1_700_000_000_000 + i, big: "z".repeat(1_000) },
    });

  test("envelopes past maxEnvelopes are dropped and counted; held ones keep only digest fields", () => {
    const lines = Array.from({ length: 5_000 }, (_, i) => env(i));
    const d = digestRecords("claude", lines, { maxEnvelopes: 1_000 });
    expect(d.records).toBe(1_000);
    expect(d.userTurnCount).toBe(1_000);
    expect(d.userTurns[0]).toBe("prompt 0");
    expect(d.userTurns.at(-1)).toBe("prompt 999");
  });

  test("a later revision still wins after compaction", () => {
    const d = digestRecords("claude", [env(1, 1), JSON.stringify({ ...JSON.parse(env(1, 2)), payload: { kind: "text", role: "user", text: "revised", ts_ms: 1 } })]);
    expect(d.userTurns).toEqual(["revised"]);
  });
});

describe("promptTurns", () => {
  test("an unsampled digest keeps its first n turns, as before", () => {
    const d = digestRecords("claude", Array.from({ length: 50 }, (_, i) => user(`u${i}`)));
    expect(d.userTurnsSampled).toBeUndefined();
    expect(promptTurns(d, 5)).toEqual(["u0", "u1", "u2", "u3", "u4"]);
  });

  test("a sampled digest spreads the turns to the last kept one", () => {
    const d = digestRecords("claude", Array.from({ length: 1_000 }, (_, i) => user(`u${i}`)));
    expect(d.userTurnsSampled).toBe(true);
    expect(d.userTurnCount).toBe(1_000);
    const picked = promptTurns(d, 20);
    expect(picked.length).toBe(20);
    expect(picked.slice(0, 10)).toEqual(Array.from({ length: 10 }, (_, i) => `u${i}`));
    expect(picked.at(-1)).toBe("u999");
    const input = JSON.parse(buildPromptInput("short", d));
    expect(input.user_turn_count).toBe(1_000);
    expect(input.user_turns.at(-1)).toBe("u999");
  });
});
