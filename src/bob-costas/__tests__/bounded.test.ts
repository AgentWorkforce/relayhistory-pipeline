import { describe, expect, test } from "bun:test";
import { digestRecords } from "../summarizer.ts";
import { rawDigest, MAX_CANONICAL_SUMMARY_BYTES } from "../raw-digest.ts";
import { assertDerivedScope } from "../../shared/derived-session.ts";
import { zstdCompressSync } from "node:zlib";

describe("bounded digest and derived trust boundary", () => {
  test("counts all turns while retaining bounded samples and handles hostile tool names", () => {
    function* input() {
      for (let i = 0; i < 1000; i++) yield JSON.stringify({ role: "user", content: "x".repeat(3000) });
      yield JSON.stringify({ role: "assistant", content: [{ type: "tool_use", name: "__proto__" }] });
      for (let i = 0; i < 500; i++) yield JSON.stringify({ role: "assistant", model: `model-${i}`, content: [{ type: "tool_use", name: `tool-${i}` }] });
    }
    const d = digestRecords("claude", input());
    expect(d.userTurnCount).toBe(1000);
    expect(d.userTurns).toHaveLength(10);
    expect(d.userTurns.every(p => p.length <= 2000)).toBe(true);
    expect(d.models.length).toBeLessThanOrEqual(64);
    expect(Object.keys(d.toolCalls)).toHaveLength(128);
    expect(d.toolCalls["__proto__"]).toBe(1);
  });
  test("large canonical and finite-host budgets refuse before object reads", async () => {
    const m = { session: { harness: "claude" }, raw: { format: "ndjson", owner: "relayhistory-api:delivery", raw_bytes: MAX_CANONICAL_SUMMARY_BYTES + 1 } } as never;
    await expect(rawDigest(m, "k", {} as never)).rejects.toMatchObject({ code: "summary_canonical_budget", retryable: false });
    await expect(rawDigest(m, "k", {} as never, 10)).rejects.toMatchObject({ code: "summary_host_budget", retryable: false });
  });
  test("native stored-digest corruption refuses before summarization", async () => {
    const stored = zstdCompressSync(Buffer.from('{"role":"user","content":"tampered"}\n'));
    const m = { session: { workspace_id: "w", machine_id: "m", harness: "claude", session_id: "s", started_at: "2026-11-04T00:00:00Z" }, raw: {
      format: "jsonl", raw_bytes: 40, chunks: [{ seq: 0, start_offset: 0, sha256_8: "abcdef12", stored_bytes: stored.length, stored_sha256: "a".repeat(64) }],
    } } as never;
    const store = { get: async () => new ReadableStream({ start(c) { c.enqueue(stored); c.close(); } }) } as never;
    await expect(rawDigest(m, "k", store)).rejects.toMatchObject({ code: "chunk_digest_mismatch", retryable: false });
  });
  test("cross tenant/workspace claims and uncomposed managed publication jobs fail closed", () => {
    const m = { session: { workspace_id: "w", machine_id: "m", session_id: "s", harness: "claude" }, extensions: { "96bulls": { tenant_id: "org-a" } } } as never;
    expect(() => assertDerivedScope({ ctx: { orgId: "org-b" } } as never, m, "k")).toThrow("tenant/session binding");
    expect(() => assertDerivedScope({ ctx: { orgId: "org-a", workspaceId: "foreign" } } as never, m, "k")).toThrow("tenant/session binding");
    expect(() => assertDerivedScope({ ctx: { orgId: "org-a" } } as never, m, "96bulls/org-a/upload-generations/g=id/v1/x/publications/id/manifest.json")).toThrow("current-image authority");
  });
});
