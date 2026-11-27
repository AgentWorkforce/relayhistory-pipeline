/** Transcript build evidence and quote selection, on synthetic rows only. */
import { describe, expect, test } from "bun:test";
import { extractArtifacts } from "../backfill/artifacts.ts";
import { bestQuote, QUOTE_MAX } from "../features/extract.ts";

const ev = (id: string, type: string, content: unknown, extra: Record<string, unknown> = {}) =>
  ({ table: "convergence_events", row: { event_id: id, ts: "2026-11-17T09:55:33.761Z", type, kind: type === "tool_result" ? "session_event" : type, content, ...extra } });

describe("extractArtifacts", () => {
  test("push output gives repository, branch and the pushed commit; a new branch has no commit id", () => {
    const a = extractArtifacts([
      ev("e1", "tool_result", "To https://github.com/Acme/Widgets.git\n   1a2b3c4..9cf50f35  feat/x -> feat/x\n"),
      ev("e2", "tool_result", "To git@github.com:acme/widgets.git\n * [new branch]      feat/y -> feat/y\n"),
    ], null);
    expect(a.pushes.map((p) => [p.repo, p.branch, p.sha, p.eventId])).toEqual([["acme/widgets", "feat/x", "9cf50f35", "e1"], ["acme/widgets", "feat/y", null, "e2"]]);
  });
  test("commit lines; the default branch is not a feature branch", () => {
    const a = extractArtifacts([ev("e1", "tool_result", "[main 77aa88bb99] Fix typo\n"), ev("e2", "tool_result", "[feat/x abcdef1] Add\n")], null);
    expect(a.commits.map((c) => [c.sha, c.branch])).toEqual([["77aa88bb99", null], ["abcdef1", "feat/x"]]);
  });
  test("PR URLs only for a repository the session pushed to or is attributed to; fixtures are dropped", () => {
    const rows = [
      ev("e1", "tool_result", "To https://github.com/acme/widgets.git\n   1a2b3c4..9cf50f3  feat/x -> feat/x\n"),
      ev("e2", "tool_result", "https://github.com/acme/widgets/pull/42 and https://github.com/o/r/pull/1"),
      ev("e3", "tool_result", "https://github.com/acme/gadgets/pull/7"),
    ];
    expect(extractArtifacts(rows, null).prs.map((p) => p.url)).toEqual(["https://github.com/acme/widgets/pull/42"]);
    expect(extractArtifacts(rows, "acme/gadgets").prs.map((p) => p.url)).toEqual(["https://github.com/acme/widgets/pull/42", "https://github.com/acme/gadgets/pull/7"]);
  });
  test("delegated task ids from user and history events, never startup ids or tool output", () => {
    const a = extractArtifacts([
      ev("e1", "user", "Delegated task startup-4008f730. Sender label: x"),
      ev("e2", "user", "Delegated task worker-lead-w-memdocs-initial. Sender label: lead"),
      ev("e3", "tool_result", "Delegated task worker-other-initial."),
    ], null);
    expect(a.taskRefs.map((t) => t.ref)).toEqual(["worker-lead-w-memdocs-initial"]);
  });
  test("files are anchored at the repository root; home paths never survive", () => {
    const a = extractArtifacts([ev("e1", "file_edit", "", { files_touched: ["~/work/workers/memdocs/docs/a.md", "/Users/someone/notes.txt", "src/b.ts"] })], null);
    expect(a.files.map((f) => f.path)).toEqual(["docs/a.md", "src/b.ts"]);
  });
  test("rows without an event id are ignored", () => {
    expect(extractArtifacts([{ table: "convergence_events", row: { type: "tool_result", content: "[feat/x abcdef1] Add" } }], null).commits).toEqual([]);
  });
});

describe("bestQuote", () => {
  test("a short message is quoted whole, collapsed", () => {
    expect(bestQuote("Add  a retry\nto the uploader.", "retry")).toBe("Add a retry to the uploader.");
  });
  test("a long message is quoted at the sentences that match the request, verbatim", () => {
    const preamble = "Agent Relay transport: use the private MCP server for replies. ".repeat(8);
    const message = `${preamble}Please add exponential backoff to the uploader retry loop so a flaky network never loses a session. ${"Unrelated tail sentence here. ".repeat(10)}`;
    const q = bestQuote(message, "Add exponential backoff to the uploader retry");
    expect(q.startsWith("Please add exponential backoff")).toBe(true);
    expect(q.length).toBeLessThanOrEqual(QUOTE_MAX);
    expect(message.replace(/\s+/gu, " ").includes(q)).toBe(true);
  });
});

describe("extractArtifacts branches", () => {
  test("branches from quiet git commands and from briefs; default and remote-tracking names are not features", () => {
    const a = extractArtifacts([
      ev("e1", "tool_call", "git push -q origin review/claude-x && git worktree add -q -b w-memdocs/first-slice ../wt origin/main"),
      ev("e2", "user", "Branch under review: review/pipe-uploads-2-backfill-c (shared; do NOT git checkout)"),
      ev("e3", "user", "Work on branch `main` and compare with origin/main"),
      ev("e4", "tool_result", "Branch under review: review/should-not-count"),
    ], null);
    expect(a.branches.map((b) => [b.branch, b.via])).toEqual([["review/claude-x", "command"], ["w-memdocs/first-slice", "command"], ["review/pipe-uploads-2-backfill-c", "mention"]]);
  });
});
