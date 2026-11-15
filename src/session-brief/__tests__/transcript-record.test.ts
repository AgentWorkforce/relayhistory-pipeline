import { describe, expect, test } from "bun:test";
import { eventFromTranscriptRecord } from "../input.ts";

const event = (record: Record<string, unknown>) => eventFromTranscriptRecord(record, "codex", "s-1", "m-1", 1);

describe("brief transcript records, Codex-shaped", () => {
  test("a response_item user message becomes a user text event", () => {
    const e = event({ timestamp: "2026-11-07T12:49:09.100Z", type: "response_item", payload: { type: "message", role: "user", content: [{ type: "input_text", text: "Fix the retry test" }] } });
    expect(e).toMatchObject({ source: "codex", sessionId: "s-1", machineId: "m-1", role: "user", kind: "text", text: "Fix the retry test", tsMs: Date.parse("2026-11-07T12:49:09.100Z") });
  });
  test("a response_item assistant message becomes an assistant text event", () => {
    const e = event({ type: "response_item", payload: { type: "message", role: "assistant", content: [{ type: "output_text", text: "Done." }] } });
    expect(e).toMatchObject({ role: "assistant", kind: "text", text: "Done." });
  });
  test("developer messages, tool calls and malformed payloads are not events", () => {
    expect(event({ type: "response_item", payload: { type: "message", role: "developer", content: [{ type: "input_text", text: "You are Codex." }] } })).toBeNull();
    expect(event({ type: "response_item", payload: { type: "function_call", name: "shell", arguments: "{}" } })).toBeNull();
    expect(event({ type: "response_item", payload: null })).toBeNull();
    expect(event({ type: "response_item", payload: { type: "message", role: "user", content: [{ type: "input_image", image_url: "x" }] } })).toBeNull();
  });
});
