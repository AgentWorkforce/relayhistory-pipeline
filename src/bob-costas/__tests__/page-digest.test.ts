import { expect, it } from "bun:test";
import { pageDigest, type DigestArtifactPage, type DigestArtifactQuery } from "../page-digest.ts";

const identity = { orgId: "org", workspaceId: "ws", source: "claude", sessionId: "session" };
const generation = "a".repeat(64);
const row = (extra = {}) => ({ table: "convergence_events", row: { source: "claude", session_id: "session", actor_role: "user", kind: "message", content: "question", ts: "2026-11-04T00:00:00Z", ...extra } });
const page = (extra = {}): DigestArtifactPage => ({ generation, exists: true, rows: [row()], nextCursor: null, ...extra });
const signal = () => new AbortController().signal;

it("consumes all pinned replay pages with bounded retained prompts/models/tools", async () => {
  const queries: DigestArtifactQuery[] = [];
  const digest = await pageDigest({ identity, generation, signal: signal(), readPage: async q => {
    queries.push(q);
    const rows = Array.from({ length: 1000 }, (_, i) => row({
      actor_role: i % 2 ? "assistant" : "user", kind: i % 3 ? "message" : "tool_call",
      tool_call: { name: `tool${i}` }, content: "x".repeat(2001), model: `model${i}`,
    }));
    return page({ rows, nextCursor: q.cursor ? null : "next" });
  } });
  expect(digest.records).toBe(2000);
  expect(digest.userTurnCount).toBe(1000);
  expect(digest.assistantTurns).toBe(1000);
  expect(digest.models).toHaveLength(64);
  expect(Object.keys(digest.toolCalls)).toHaveLength(128);
  expect(digest.userTurns).toHaveLength(10);
  expect(digest.userTurns.every(s => s.length === 2000)).toBe(true);
  expect(queries).toEqual([
    { table: "convergence_events", view: "replay", order: "asc", limit: 1000, maxContent: 2000, generation },
    { table: "convergence_events", view: "replay", order: "asc", limit: 1000, maxContent: 2000, generation, cursor: "next" },
  ]);
});

it("refuses changed/absent publication after earlier pages without returning a partial digest", async () => {
  for (const changed of [{ generation: "b".repeat(64) }, { exists: false }]) {
    await expect(pageDigest({ identity, generation, signal: signal(), readPage: async q => q.cursor ? page(changed) : page({ nextCursor: "next" }) }))
      .rejects.toMatchObject({ code: "summary_artifact_stale", retryable: true });
  }
});

it("refuses foreign rows, malformed envelopes and cursor loops", async () => {
  const badPages = [
    page({ rows: [row({ source: "codex" })] }), page({ rows: [row({ session_id: "other" })] }),
    page({ rows: [row({ org_id: "other" })] }), page({ rows: [row({ workspace_id: "other" })] }),
    page({ rows: [row({ content: "x".repeat(2002) })] }), page({ rows: [{ table: "other", row: row().row }] }),
    page({ rows: [row({ ts: "x".repeat(65) })] }),
    page({ rows: Array(1001).fill(row()) }), page({ rows: [], nextCursor: "next" }),
    page({ nextCursor: "x".repeat(4097) }), page({ nextCursor: "" }),
  ];
  for (const bad of badPages) {
    await expect(pageDigest({ identity, generation, signal: signal(), readPage: async () => bad }))
      .rejects.toMatchObject({ code: "summary_artifact_invalid", retryable: false });
  }
  let reads = 0;
  await expect(pageDigest({ identity, generation, signal: signal(), readPage: async () => { reads++; return page({ nextCursor: "loop" }); } }))
    .rejects.toMatchObject({ code: "summary_artifact_invalid" });
  expect(reads).toBe(2);
});

it("fails closed at the page budget and observes cancellation before/after IO", async () => {
  for (const bad of [{ identity: { orgId: "org" } }, { generation: "not-a-generation" }, { maxPages: 10_001 }]) {
    await expect(pageDigest({ identity, generation, signal: signal(), readPage: async () => { throw new Error("unexpected IO"); }, ...bad } as never))
      .rejects.toMatchObject({ code: "summary_artifact_invalid" });
  }
  await expect(pageDigest({ identity, generation, signal: signal(), maxPages: 1, readPage: async () => page({ nextCursor: "next" }) }))
    .rejects.toMatchObject({ code: "summary_artifact_budget", retryable: false });
  const abort = new AbortController();
  let reads = 0;
  const readPage = async () => { reads++; abort.abort(new Error("cancelled")); return page(); };
  await expect(pageDigest({ identity, generation, signal: abort.signal, readPage })).rejects.toThrow("cancelled");
  await expect(pageDigest({ identity, generation, signal: abort.signal, readPage })).rejects.toThrow("cancelled");
  expect(reads).toBe(1);
});
