import { describe, expect, test } from "bun:test";
import { digestRecords, extractiveSummarizer } from "../summarizer.ts";
import { CODEX_FIRST_PROMPT, CODEX_SECOND_PROMPT, codexRollout } from "./fixtures/codex-rollout.ts";

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
