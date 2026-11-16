import { describe, expect, test } from "bun:test";
import { digestRecords, extractiveSummarizer } from "../summarizer.ts";
import { CODEX_FIRST_PROMPT, CODEX_SECOND_PROMPT, codexRollout } from "./fixtures/codex-rollout.ts";
import { FIRST_PROMPT, SECOND_PROMPT, T0, deliveredSession, envelope, sessionEvent, toolCall } from "./fixtures/delivery-envelopes.ts";

const lines = [
  JSON.stringify({ type: "user", message: { role: "user", content: "Please fix the login bug" }, timestamp: "2026-10-28T10:00:00Z" }),
  JSON.stringify({ type: "assistant", message: { role: "assistant", model: "claude-sonnet-4-6", content: [{ type: "text", text: "On it" }, { type: "tool_use", name: "Bash", input: {} }] }, timestamp: "2026-10-28T10:00:05Z" }),
  JSON.stringify({ type: "assistant", message: { role: "assistant", model: "claude-sonnet-4-6", content: [{ type: "text", text: "Done" }] }, timestamp: "2026-10-28T10:00:09Z" }),
];

describe("bob costas extractive summariser", () => {
  test("digest counts turns, tools and models", () => {
    const d = digestRecords("claude", lines);
    expect(d.userTurns).toEqual(["Please fix the login bug"]);
    expect(d.assistantTurns).toBe(2);
    expect(d.toolCalls).toEqual({ Bash: 1 });
    expect(d.models).toEqual(["claude-sonnet-4-6"]);
  });
  test("short summary fits and names no model tokens", async () => {
    const s = await extractiveSummarizer.summarize("short", digestRecords("claude", lines));
    expect(s.text.length).toBeLessThanOrEqual(280);
    expect(s.text).toContain("login bug");
    expect(s.modelId).toBe("extractive/v1");
    expect(s.tokensIn + s.tokensOut).toBe(0);
  });
  test("summaries are scrubbed on the way out", async () => {
    const leaky = [JSON.stringify({ type: "user", message: { role: "user", content: "key AKIAIOSFODNN7EXAMPLE and mail a@b.com" } })];
    const s = await extractiveSummarizer.summarize("long", digestRecords("claude", leaky));
    expect(s.text).not.toContain("AKIAIOSFODNN7EXAMPLE");
    expect(s.text).not.toContain("a@b.com");
  });
});

describe("bob costas digest of a Codex rollout", () => {
  test("counts user and assistant turns, tools and the model from response_item and turn_context records", () => {
    const d = digestRecords("codex", codexRollout);
    expect(d.records).toBe(codexRollout.length);
    // Developer messages, AGENTS.md and environment context are not user turns.
    expect(d.userTurns).toEqual([CODEX_FIRST_PROMPT, CODEX_SECOND_PROMPT]);
    expect(d.assistantTurns).toBe(3);
    expect(d.toolCalls).toEqual({ shell: 2, apply_patch: 1 });
    expect(d.models).toEqual(["gpt-5-codex"]);
    expect(d.firstTs).toBe("2026-11-07T12:49:08.000Z");
    expect(d.lastTs).toBe("2026-11-07T12:50:05.000Z");
  });
  test("the fallback summary names the first prompt and real counts", async () => {
    const s = await extractiveSummarizer.summarize("short", digestRecords("codex", codexRollout));
    expect(s.text).toBe(`codex: "${CODEX_FIRST_PROMPT}" — 2 user turns, 3 assistant turns, tools: shell×2, apply_patch×1.`);
    const long = await extractiveSummarizer.summarize("long", digestRecords("codex", codexRollout));
    expect(long.text).toContain("- Models: gpt-5-codex");
    expect(long.text).toContain(`1. ${CODEX_FIRST_PROMPT}`);
  });
  test("a Claude-shaped record that happens to carry a payload is still read the Claude way", () => {
    const d = digestRecords("claude", [JSON.stringify({ type: "user", message: { role: "user", content: "hi" }, payload: { type: "message", role: "assistant" } })]);
    expect(d.userTurns).toEqual(["hi"]);
    expect(d.assistantTurns).toBe(0);
  });
  test("malformed Codex records and non-text parts are skipped, not counted", () => {
    const rec = (payload: unknown) => JSON.stringify({ type: "response_item", payload });
    const d = digestRecords("codex", [
      rec(null),
      rec({ type: "message", role: "user", content: "not an array" }),
      rec({ type: "message", role: "user", content: { type: "input_text", text: "an object" } }),
      rec({ type: "message", role: "user", content: [{ type: "input_image", image_url: "data:image/png;base64,AAAA" }] }),
      rec({ type: "message", role: "user", content: [null, { type: "input_text", text: 7 }] }),
      rec({ type: "function_call", arguments: "{}" }),
      JSON.stringify({ type: "turn_context", payload: null }),
      "null",
      "[1,2]",
      rec({ type: "message", role: "user", content: [{ type: "input_image", image_url: "x" }, { type: "input_text", text: "what is in this image?" }] }),
    ]);
    // A string content is the message text (as for Claude); everything else malformed is dropped.
    expect(d.userTurns).toEqual(["not an array", "what is in this image?"]);
    expect(d.assistantTurns).toBe(0);
    expect(Object.keys(d.toolCalls)).toEqual([]);
    expect(d.models).toEqual([]);
    expect(d.records).toBe(10);
  });
  test("tool names that collide with Object.prototype are counted like any other", async () => {
    const d = digestRecords("codex", [
      JSON.stringify({ type: "response_item", payload: { type: "function_call", name: "constructor" } }),
      JSON.stringify({ type: "response_item", payload: { type: "custom_tool_call", name: "__proto__" } }),
      JSON.stringify({ type: "assistant", message: { role: "assistant", content: [{ type: "tool_use", name: "toString" }, { type: "tool_use", name: "__proto__" }] } }),
    ]);
    expect(Object.entries(d.toolCalls)).toEqual([["constructor", 1], ["__proto__", 2], ["toString", 1]]);
    const s = await extractiveSummarizer.summarize("short", d);
    expect(s.text).toContain("tools: __proto__×2, constructor×1, toString×1.");
  });
});

describe("bob costas digest of delivery envelopes", () => {
  const iso = (ms: number) => new Date(ms).toISOString();

  test("a delivered session digests to its turns, tools, models and span", () => {
    const d = digestRecords("claude", deliveredSession);
    expect(d.records).toBe(deliveredSession.length);
    // Control, meta and sidechain user rows are not turns; the history prompt-log row is not one either.
    expect(d.userTurns).toEqual([FIRST_PROMPT, SECOND_PROMPT]);
    expect(d.assistantTurns).toBe(3);
    expect(d.toolCalls).toEqual({ Read: 1, Bash: 2 });
    expect(d.models).toEqual(["claude-sonnet-4-6", "claude-opus-4-6"]);
    expect(d.firstTs).toBe(iso(T0 + 1000));
    expect(d.lastTs).toBe(iso(T0 + 11_500));
  });

  test("the extractive summary names the first prompt, not (no user prompt)", async () => {
    const short = await extractiveSummarizer.summarize("short", digestRecords("claude", deliveredSession));
    expect(short.text).toContain("Fix the flaky upload retry");
    expect(short.text).not.toContain("(no user prompt)");
    expect(short.text).toContain("2 user turns, 3 assistant turns");
    const long = await extractiveSummarizer.summarize("long", digestRecords("claude", deliveredSession));
    expect(long.text).toContain(`- Span: ${iso(T0 + 1000)} → ${iso(T0 + 11_500)}`);
  });

  test("arrival order does not matter: first and last are by event time", () => {
    const d = digestRecords("claude", [...deliveredSession].reverse());
    expect(d.userTurns).toEqual([FIRST_PROMPT, SECOND_PROMPT]);
    expect(d.firstTs).toBe(iso(T0 + 1000));
    expect(d.lastTs).toBe(iso(T0 + 11_500));
  });

  test("a delete drops the record, whichever order its revisions arrive in", () => {
    const upsert = envelope("session_event", "m-1:0", sessionEvent(1, "user", FIRST_PROMPT));
    const tool = envelope("tool_call", "toolu_2", toolCall(2, "Bash", { command: "ls" }));
    const deleted = envelope("session_event", "m-1:0", null, { revision: 2 });
    const toolDeleted = envelope("tool_call", "toolu_2", null, { revision: 2 });
    // The parser accepts a delete that still carries its old payload; it is a delete all the same.
    const stale = JSON.parse(envelope("session_event", "m-1:0", sessionEvent(1, "user", FIRST_PROMPT), { revision: 2 }));
    const staleDigest = digestRecords("claude", [upsert, JSON.stringify({ ...stale, operation: "delete" })]);
    expect(staleDigest.userTurns).toEqual([]);
    expect(staleDigest.records).toBe(0);
    for (const lines of [[upsert, tool, deleted, toolDeleted], [toolDeleted, deleted, tool, upsert]]) {
      const d = digestRecords("claude", lines);
      expect(d.userTurns).toEqual([]);
      expect(d.toolCalls).toEqual({});
      expect(d.records).toBe(0);
    }
  });

  test("a re-uploaded higher revision counts once, with its own content", () => {
    const lines = [
      envelope("session_event", "m-1:0", sessionEvent(1, "user", "first draft of the prompt")),
      envelope("session_event", "m-2:0", sessionEvent(2, "assistant", "ok")),
      envelope("tool_call", "toolu_3", toolCall(3, "Read", { file_path: "a.ts" })),
      // Revision 2 of each, re-uploaded in a later chunk; the same revision twice is one copy.
      envelope("session_event", "m-1:0", sessionEvent(1, "user", FIRST_PROMPT), { revision: 2 }),
      envelope("session_event", "m-2:0", sessionEvent(2, "assistant", "ok"), { revision: 2 }),
      envelope("tool_call", "toolu_3", toolCall(3, "Read", { file_path: "a.ts" }), { revision: 2 }),
      envelope("tool_call", "toolu_3", toolCall(3, "Read", { file_path: "a.ts" }), { revision: 2 }),
    ];
    const d = digestRecords("claude", lines);
    expect(d.userTurns).toEqual([FIRST_PROMPT]);
    expect(d.assistantTurns).toBe(1);
    expect(d.toolCalls).toEqual({ Read: 1 });
    expect(d.records).toBe(3);
  });

  test("a revision conflict the read path refuses still digests, highest revision once", () => {
    // Two different payloads at one revision: normalizeRelayhistoryRecords throws; the digest must not.
    const lines = [
      envelope("session_event", "m-1:0", sessionEvent(1, "user", "old"), { revision: 1 }),
      envelope("session_event", "m-1:0", sessionEvent(1, "user", "copy A"), { revision: 3, receivedAt: iso(T0 + 1) }),
      envelope("session_event", "m-1:0", sessionEvent(1, "user", "copy B"), { revision: 3, receivedAt: iso(T0 + 2) }),
      envelope("tool_call", "toolu_2", toolCall(2, "Bash", { command: "ls" })),
      envelope("tool_call", "toolu_2", null, { revision: 2 }),
    ];
    const d = digestRecords("claude", lines);
    expect(d.userTurns).toEqual(["copy B"]);
    expect(d.toolCalls).toEqual({});
    expect(d.records).toBe(1);
  });

  test("a control_kind, is_meta or sidechain user event is not a turn", () => {
    const d = digestRecords("claude", [
      envelope("session_event", "m-1:0", sessionEvent(1, "user", "<command-name>/clear</command-name>", { control_kind: "slash_command" })),
      envelope("session_event", "m-2:0", sessionEvent(2, "user", "meta", { is_meta: "true" })),
      envelope("session_event", "m-3:0", sessionEvent(3, "user", "subagent prompt", { is_sidechain: true })),
      envelope("session_event", "m-4:0", sessionEvent(4, "user", "real prompt", { control_kind: "" })),
    ]);
    expect(d.userTurns).toEqual(["real prompt"]);
  });

  test("non-envelope lines beside envelopes are read exactly as before", () => {
    const d = digestRecords("claude", [...lines, envelope("session_event", "m-1:0", sessionEvent(1, "user", FIRST_PROMPT))]);
    expect(d.userTurns).toEqual(["Please fix the login bug", FIRST_PROMPT]);
    expect(d.assistantTurns).toBe(2);
    expect(d.toolCalls).toEqual({ Bash: 1 });
    expect(d.records).toBe(4);
    const alone = digestRecords("claude", lines);
    expect(alone.userTurns).toEqual(["Please fix the login bug"]);
    expect(alone.firstTs).toBe("2026-10-28T10:00:00Z");
    expect(alone.lastTs).toBe("2026-10-28T10:00:09Z");
    expect(digestRecords("codex", codexRollout).userTurns).toEqual([CODEX_FIRST_PROMPT, CODEX_SECOND_PROMPT]);
  });

  test("an empty or malformed envelope is skipped, not counted as a turn", () => {
    const base = { kind: "session_event", origin_id: "o", record_id: "r", revision: "1", operation: "upsert", received_at: iso(T0) };
    const d = digestRecords("claude", [
      JSON.stringify({ ...base, record_id: "empty", payload: {} }),
      JSON.stringify({ ...base, kind: "tool_call", record_id: "noname", payload: { name: "  " } }),
      JSON.stringify({ ...base, record_id: "nopayload", payload: null }),
      JSON.stringify({ ...base, record_id: "badrev", revision: "one", payload: sessionEvent(1, "user", "x") }),
      JSON.stringify({ ...base, record_id: "badtime", received_at: "never", payload: sessionEvent(2, "user", "y") }),
      JSON.stringify({ ...base, record_id: "notext", payload: sessionEvent(3, "user", "z", { text: 42 }) }),
      JSON.stringify({ ...base, record_id: "array", payload: [sessionEvent(4, "user", "w")] }),
      envelope("session_event", "m-5:0", sessionEvent(5, "user", "survivor")),
    ]);
    expect(d.userTurns).toEqual(["survivor"]);
    expect(d.assistantTurns).toBe(0);
    expect(d.toolCalls).toEqual({});
  });
});
