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
  MAX_MODELS,
  MAX_TOOL_NAMES,
  RETAINED_HEAD_TURNS,
  RETAINED_MIDDLE_TURNS,
  RETAINED_TAIL_TURNS,
  RETAINED_USER_TURNS,
  TURN_TEXT_CHARS,
  TurnSampler,
  clipTurn,
  digestRecords,
  extractiveSummarizer,
  type SessionDigest,
} from "../summarizer.ts";
import { buildModelRequest, buildPromptInput, modelSummarizerConfig, promptTurns } from "../model-summarizer.ts";
import { legacyDigestRecords, legacyExtractiveSummarizer } from "./fixtures/legacy-digest.ts";
import { codexRollout } from "./fixtures/codex-rollout.ts";
import { T0, deliveredSession, envelope, sessionEvent, toolCall } from "./fixtures/delivery-envelopes.ts";

const MiB = 1024 * 1024;
const loc = { workspaceId: "w", machineId: "m", harness: "claude", sessionId: "s1", startDate: "2026-11-16" };
const ON: SummaryInputCaps = { ...DEFAULT_SUMMARY_INPUT_CAPS, sampling: true };

const user = (text: string) => JSON.stringify({ type: "user", message: { role: "user", content: text } });
const assistant = (tool: string, model = "m1") =>
  JSON.stringify({ type: "assistant", message: { role: "assistant", model, content: [{ type: "tool_use", name: tool, input: {} }] } });
/** A Claude tool result: role user, but no text part, so not a turn. Bulk for the byte count. */
const toolResult = (bytes: number) =>
  JSON.stringify({ type: "user", message: { role: "user", content: [{ type: "tool_result", content: "r".repeat(bytes) }] } });

/**
 * A session whose chunk bodies are built on demand at GET time, so the test never holds the
 * session: `body(i)` is chunk i's text. Records every GET and samples the heap every 16.
 */
function lazySession(count: number, body: (i: number) => string, opts: { declare?: (i: number) => number | undefined } = {}) {
  const declare = opts.declare ?? ((i: number) => Buffer.byteLength(body(i)));
  const chunks = Array.from({ length: count }, (_, i) => {
    const bytes = declare(i);
    return { seq: i, start_offset: i, sha256_8: i.toString(16).padStart(8, "0"), ...(bytes === undefined ? {} : { bytes }) };
  });
  const seqOf = new Map(chunks.map((c) => [String(rawChunkKey(loc, c.seq, c.start_offset, c.sha256_8, "jsonl")), c.seq]));
  const fetched: number[] = [];
  let peakHeap = 0;
  const store = {
    get: async (key: unknown) => {
      const i = seqOf.get(String(key));
      if (i === undefined) throw Object.assign(new Error("missing"), { code: "not_found" });
      fetched.push(i);
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
  return { manifest: manifest as never, store: store as never, fetched, peak: () => peakHeap };
}

/** `lines` cut into chunks of `per` lines. */
const chunked = (lines: string[], per: number) => (i: number) => lines.slice(i * per, (i + 1) * per).join("\n") + "\n";

const bulky = (bulk: number) => (i: number) => [user(`turn ${i}`), assistant("Bash"), toolResult(bulk)].join("\n") + "\n";

describe("caps", () => {
  test("from env: sampling defaults off; out-of-range values keep the defaults", () => {
    expect(summaryInputCapsFromEnv({})).toEqual({ ...DEFAULT_SUMMARY_INPUT_CAPS });
    expect(DEFAULT_SUMMARY_INPUT_CAPS.sampling).toBe(false);
    expect(summaryInputCapsFromEnv({ BOB_COSTAS_INPUT_SAMPLING: "on", BOB_COSTAS_MAX_CHUNKS: "64" }))
      .toMatchObject({ sampling: true, maxChunks: 64 });
    expect(summaryInputCapsFromEnv({ BOB_COSTAS_INPUT_SAMPLING: "maybe", BOB_COSTAS_MAX_CHUNKS: "0", BOB_COSTAS_MAX_INPUT_BYTES: "-1" }))
      .toEqual({ ...DEFAULT_SUMMARY_INPUT_CAPS });
  });

  test("the byte budget is never below the per-chunk cap", () => {
    const caps = summaryInputCapsFromEnv({ BOB_COSTAS_MAX_INPUT_BYTES: String(2 * MiB), BOB_COSTAS_MAX_CHUNK_BYTES: String(8 * MiB) });
    expect(caps.maxInputBytes).toBe(8 * MiB);
  });
});

describe("planChunks (sampling on)", () => {
  const sized = (n: number, bytes = 1_000) => Array.from({ length: n }, () => ({ bytes }));

  test("a session within the caps is read whole", () => {
    expect(planChunks(sized(10), ON)).toEqual({ indices: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9], sampled: false });
  });

  test("past the chunk cap: head, tail and an evenly spaced middle, ascending and distinct", () => {
    const { indices, sampled } = planChunks(sized(3_500), { ...ON, maxChunks: 100 });
    expect(sampled).toBe(true);
    expect(indices.length).toBe(100);
    expect(new Set(indices).size).toBe(100);
    expect([...indices].sort((a, b) => a - b)).toEqual(indices);
    expect(indices.slice(0, 25)).toEqual(Array.from({ length: 25 }, (_, i) => i));
    expect(indices.slice(-25)).toEqual(Array.from({ length: 25 }, (_, i) => 3_475 + i));
    const middle = indices.slice(25, 75);
    const gaps = middle.slice(1).map((v, i) => v - middle[i]!);
    expect(Math.max(...gaps) - Math.min(...gaps)).toBeLessThanOrEqual(1);
  });

  test("heavy head chunks do not starve the tail of the byte budget", () => {
    // The first 50 chunks are 4 MiB each, the rest 64 KiB: a head-first budget would end in the head.
    const chunks = Array.from({ length: 1_000 }, (_, i) => ({ bytes: i < 50 ? 4 * MiB : 64 * 1024 }));
    const { indices } = planChunks(chunks, { ...ON, maxInputBytes: 64 * MiB, maxChunkBytes: 4 * MiB });
    expect(indices[0]).toBe(0);
    expect(indices).toContain(999);
    expect(indices).toContain(990);
    const bytes = indices.reduce((n, i) => n + chunks[i]!.bytes, 0);
    expect(bytes).toBeLessThanOrEqual(64 * MiB);
  });

  test("a chunk declared over the per-chunk cap is never read", () => {
    expect(planChunks([{ bytes: 10 }, { bytes: 64 * MiB }, { bytes: 10 }], ON)).toEqual({ indices: [0, 2], sampled: true });
  });
});

describe("TurnSampler and clipTurn (sampling on)", () => {
  test("a short session keeps every turn", () => {
    const s = new TurnSampler();
    for (let i = 0; i < RETAINED_USER_TURNS; i++) s.add(`t${i}`);
    expect(s.turns()).toEqual(Array.from({ length: RETAINED_USER_TURNS }, (_, i) => `t${i}`));
  });

  test("10,000 turns: the head, the tail and an evenly spaced middle, in order", () => {
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
    expect(new Set(middle.slice(1).map((v, i) => v - middle[i]!)).size).toBe(1);
    expect([...kept].sort((a, b) => a - b)).toEqual(kept);
  });

  test("clipTurn collapses whitespace before it clips", () => {
    expect(clipTurn("a \n\t b")).toBe("a b");
    expect(clipTurn("x".repeat(TURN_TEXT_CHARS * 3)).length).toBe(TURN_TEXT_CHARS);
  });

  test("whitespace padding cannot pull a token cut by the clip into a summary", async () => {
    const token = "ghp_A1b2C3d4E5f6G7h8I9j0K1l2M3n4O5p6Q7r8";
    const padded = [
      // Padding alone: collapsed, the whole token survives and the re-scrub matches it.
      user(" ".repeat(8_178) + token),
      // Padding plus filler: the token straddles the clip, far past what any summariser reads.
      user(" ".repeat(20_000) + "y".repeat(TURN_TEXT_CHARS - 10) + " " + token),
    ];
    const d = digestRecords("claude", padded, { sampling: true });
    const outputs = [
      (await extractiveSummarizer.summarize("short", d)).text,
      (await extractiveSummarizer.summarize("long", d)).text,
      buildPromptInput("short", d),
      buildPromptInput("long", d),
    ];
    for (const out of outputs) expect(out).not.toContain("ghp_A1b2C3d4E5");
  });
});

describe("readSessionDigest", () => {
  test("sampling off: 3,500 chunks, ~1.4 GB, every chunk read, heap bounded, digest whole", async () => {
    const session = lazySession(3_500, bulky(400 * 1024));
    Bun.gc(true);
    const before = process.memoryUsage().heapUsed;
    const d = await readSessionDigest({ manifest: session.manifest, location: loc, store: session.store });
    expect(session.fetched.length).toBe(3_500);
    expect(d.input).toBeUndefined();
    expect(d.userTurnCount).toBe(3_500);
    expect(d.userTurns.length).toBe(3_500);
    expect(d.toolCalls).toEqual({ Bash: 3_500 });
    // Streaming alone: the heap held a few chunks' worth, never the session.
    expect(session.peak() - before).toBeLessThan(64 * MiB);
  }, 120_000);

  test("sampling on: 3,500 chunks summarise from a bounded sample, head and tail kept, marked", async () => {
    const session = lazySession(3_500, bulky(400 * 1024));
    Bun.gc(true);
    const before = process.memoryUsage().heapUsed;
    const d = await readSessionDigest({ manifest: session.manifest, location: loc, store: session.store, caps: ON });
    expect(session.fetched.length).toBeLessThanOrEqual(ON.maxChunks);
    expect(d.input).toMatchObject({ chunksTotal: 3_500, chunksRead: session.fetched.length });
    expect(d.input!.bytesRead).toBeLessThanOrEqual(ON.maxInputBytes);
    expect(session.fetched[0]).toBe(0);
    expect(session.fetched.at(-1)).toBe(3_499);
    expect(d.userTurns[0]).toBe("turn 0");
    expect(d.userTurns.at(-1)).toBe("turn 3499");
    expect(session.peak() - before).toBeLessThan(64 * MiB);
    const long = await extractiveSummarizer.summarize("long", d);
    expect(long.text).toContain(`- Input: sampled ${d.input!.chunksRead} of 3500 chunks`);
    const request = buildModelRequest("long", d, modelSummarizerConfig("m1", {}));
    expect(JSON.parse(request.input).input_sampled).toEqual({ chunks_read: d.input!.chunksRead, chunks_total: 3_500 });
    expect(request.instructions).toContain("input_sampled");
  }, 120_000);

  test("sampling on: once the byte budget is spent, no further chunk is fetched", async () => {
    // Sizes undeclared: the plan reads every chunk, the budget stops it at read time.
    const session = lazySession(40, bulky(64 * 1024), { declare: () => undefined });
    const caps = { ...ON, maxInputBytes: 1 * MiB, maxChunkBytes: 128 * 1024 };
    const d = await readSessionDigest({ manifest: session.manifest, location: loc, store: session.store, caps });
    expect(d.input!.bytesRead).toBeLessThanOrEqual(1 * MiB);
    expect(session.fetched.length).toBeLessThanOrEqual(d.input!.chunksRead + 1);
  });

  test("a chunk past the per-chunk cap is never inflated, and the digest is marked", async () => {
    const session = lazySession(3, bulky(256 * 1024), { declare: () => undefined });
    const d = await readSessionDigest({ manifest: session.manifest, location: loc, store: session.store,
      caps: { ...DEFAULT_SUMMARY_INPUT_CAPS, maxChunkBytes: 128 * 1024 } });
    expect(d.records).toBe(0);
    expect(d.input).toMatchObject({ chunksRead: 0, chunksTotal: 3 });
  });

  test("a missing chunk is retryable chunk_missing, as before", async () => {
    const session = lazySession(2, bulky(10));
    const store = { get: async () => { throw Object.assign(new Error("gone"), { code: "not_found" }); } };
    await expect(readSessionDigest({ manifest: session.manifest, location: loc, store: store as never }))
      .rejects.toMatchObject({ code: "chunk_missing", retryable: true });
  });
});

describe("delivery envelopes are always folded whole", () => {
  const prompt = (n: number) => envelope("session_event", `m-${n}:0`, sessionEvent(n, "user", `prompt ${n}`));
  const tombstone = (n: number) => envelope("session_event", `m-${n}:0`, null, { revision: 2 });
  const revised = (n: number, text: string) => envelope("session_event", `m-${n}:0`, sessionEvent(n, "user", text), { revision: 2 });

  test("sampling on: a tombstone and a later revision in late chunks the sample would skip still apply", async () => {
    const lines = [
      ...Array.from({ length: 4_000 }, (_, n) => prompt(n)),
      ...Array.from({ length: 1_000 }, (_, k) => sessionEvent(5_000 + k, "assistant", "z".repeat(2_000))).map((p, k) =>
        envelope("session_event", `a-${k}:0`, p)),
      tombstone(1),
      revised(2, "the revised prompt"),
    ];
    const session = lazySession(Math.ceil(lines.length / 10), chunked(lines, 10));
    const caps = { ...ON, maxChunks: 8, maxInputBytes: 1 * MiB };
    const d = await readSessionDigest({ manifest: session.manifest, location: loc, store: session.store, caps });
    expect(session.fetched.length).toBe(Math.ceil(lines.length / 10));
    expect(d.userTurns).not.toContain("prompt 1");
    expect(d.userTurns).not.toContain("prompt 2");
    expect(d.userTurns).toContain("the revised prompt");
    expect(d.userTurnCount).toBe(3_999);
    const legacy = legacyDigestRecords("claude", lines);
    expect(d.userTurnCount).toBe(legacy.userTurns.length);
    expect(d.records).toBe(legacy.records);
  });

  test("envelopes found only after the sample skipped chunks: the session is read again, whole", async () => {
    const lines = [
      ...Array.from({ length: 200 }, (_, i) => user(`plain ${i}`)),
      prompt(1),
      tombstone(1),
    ];
    const session = lazySession(lines.length, chunked(lines, 1));
    const d = await readSessionDigest({ manifest: session.manifest, location: loc, store: session.store, caps: { ...ON, maxChunks: 8 } });
    expect(d.userTurns).not.toContain("prompt 1");
    expect(d.userTurnCount).toBe(200);
  });

  test("a session of envelopes with a chunk over the per-chunk cap fails, non-retryable", async () => {
    const lines = [prompt(1), envelope("session_event", "m-9:0", sessionEvent(9, "user", "x".repeat(300 * 1024)))];
    const session = lazySession(2, chunked(lines, 1), { declare: () => undefined });
    await expect(readSessionDigest({ manifest: session.manifest, location: loc, store: session.store,
      caps: { ...DEFAULT_SUMMARY_INPUT_CAPS, maxChunkBytes: 128 * 1024 } }))
      .rejects.toMatchObject({ code: "summary_input_incomplete", retryable: false });
  });
});

/** The legacy digest has neither userTurnCount nor userTurnsSampled; compare the rest. */
function comparable(d: SessionDigest) {
  const { userTurnCount: _c, userTurnsSampled: _s, input: _i, ...rest } = d;
  return rest;
}

describe("sampling off: byte-identical to the whole-session digest", () => {
  const e = (kind: string, uid: string, payload: Record<string, unknown> | null, o = {}) => envelope(kind, uid, payload, o);
  const corpus: Record<string, string[]> = {
    claude: [
      user("Please fix the login bug"),
      assistant("Bash", "claude-sonnet-4-6"),
      "not json",
      "",
      "   ",
      JSON.stringify([1, 2]),
      JSON.stringify({ type: "user", message: { role: "user", content: [{ type: "text", text: "part a" }, { type: "text", text: "part b" }] } }),
    ],
    codex: codexRollout,
    delivered: deliveredSession,
    deliveredReversed: [...deliveredSession].reverse(),
    manyTurns: Array.from({ length: 500 }, (_, i) => user(`${" ".repeat(i % 7)}turn ${i}\n${"w".repeat(i * 37)}`)),
    manyTools: Array.from({ length: MAX_TOOL_NAMES + 200 }, (_, i) => assistant(`tool-${i}`, `model-${i % (MAX_MODELS + 20)}`)),
    hugeTurn: [user(" ".repeat(9_000) + "x".repeat(50_000)), user("second")],
    envelopes: [
      e("session_event", "m-1:0", sessionEvent(1, "user", "one")),
      e("session_event", "m-1:0", sessionEvent(1, "user", "one, revised"), { revision: 3 }),
      e("session_event", "m-1:0", sessionEvent(1, "user", "one, stale"), { revision: 2 }),
      e("session_event", "m-2:0", sessionEvent(2, "user", "two")),
      e("session_event", "m-2:0", null, { revision: 2 }),
      e("session_event", "m-3:0", sessionEvent(3, "user", "three", { is_sidechain: "true" })),
      e("session_event", "m-4:0", sessionEvent(4, "assistant", "four")),
      e("tool_call", "toolu_5", toolCall(5, "Edit", {})),
      e("tool_call", "toolu_5", toolCall(5, "Edit", {})), // an identical copy
      e("session_event", "m-6:0", sessionEvent(6, "user", "six", { ts_ms: "1763110806000" })),
      e("session_event", "m-7:0", sessionEvent(7, "user", "seven", { ts_ms: -1, created_ms: T0 })),
      JSON.stringify({ kind: "session_event", record_id: "bad", revision: "x", payload: {} }),
    ],
    // Equal revisions with different content: the strict settle refuses, the fallback decides.
    conflict: [
      e("session_event", "m-1:0", sessionEvent(1, "user", "first copy")),
      e("session_event", "m-1:0", sessionEvent(1, "user", "first copy", { extra: "differs only off the digest fields" })),
      e("session_event", "m-2:0", sessionEvent(2, "user", "second")),
    ],
    // One record from two origins without an acceptance seq: newest-across-origins refuses.
    crossOrigin: [
      e("session_event", "m-1:0", sessionEvent(1, "user", "from a"), { origin: "origin-a" }),
      e("session_event", "m-1:0", sessionEvent(1, "user", "from b"), { origin: "origin-b" }),
    ],
    mixed: [user("plain first"), e("session_event", "m-1:0", sessionEvent(1, "user", "enveloped")), assistant("Read")],
  };

  for (const [name, lines] of Object.entries(corpus)) {
    test(`${name}: digest, extractive summaries and prompts match`, async () => {
      const legacy = legacyDigestRecords("claude", lines);
      const fresh = digestRecords("claude", lines);
      expect(comparable(fresh)).toEqual(legacy as never);
      expect(fresh.userTurnCount).toBe(legacy.userTurns.length);
      expect(fresh.userTurnsSampled).toBeUndefined();
      for (const kind of ["short", "long"] as const)
        expect((await extractiveSummarizer.summarize(kind, fresh)).text).toBe((await legacyExtractiveSummarizer.summarize(kind, legacy)).text);
      expect(promptTurns(fresh, 20)).toEqual(legacy.userTurns.slice(0, 20));
      expect(buildModelRequest("long", fresh, modelSummarizerConfig("m1", {})).instructions).not.toContain("input_sampled");
      // Streamed in chunks of three lines, the same digest.
      const session = lazySession(Math.max(1, Math.ceil(lines.length / 3)), chunked(lines, 3));
      const streamed = await readSessionDigest({ manifest: session.manifest, location: loc, store: session.store });
      expect(comparable(streamed)).toEqual(legacy as never);
      expect(streamed.input).toBeUndefined();
    });
  }
});

describe("promptTurns (sampling on)", () => {
  test("a sampled digest spreads the turns to the last kept one", () => {
    const d = digestRecords("claude", Array.from({ length: 1_000 }, (_, i) => user(`u${i}`)), { sampling: true });
    expect(d.userTurnsSampled).toBe(true);
    expect(d.userTurnCount).toBe(1_000);
    const picked = promptTurns(d, 20);
    expect(picked.slice(0, 10)).toEqual(Array.from({ length: 10 }, (_, i) => `u${i}`));
    expect(picked.at(-1)).toBe("u999");
    expect(JSON.parse(buildPromptInput("short", d)).user_turn_count).toBe(1_000);
  });
});
