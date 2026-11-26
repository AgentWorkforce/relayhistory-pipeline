import { describe, expect, test } from "bun:test";
import { CloudClient } from "../cloud-harvest/client.ts";
import { eventFacts, informative } from "../cloud-harvest/features.ts";
import { cursorAt, harvestFrame, harvestSession } from "../cloud-harvest/harvest.ts";

const MARKER = "SECRET-MARKER-TEXT-do-not-leak";
const ev = (extra: Record<string, unknown>) => ({ eventId: "e1", ts: "2026-09-01T00:00:00Z", ...extra });

describe("eventFacts", () => {
  test("derives git, gh, branch, PR and file facts from a tool call and never returns content", () => {
    const f = eventFacts(ev({
      kind: "tool_call", toolName: "Bash", filesTouched: ["crates/core/src/lib.rs"],
      content: `${MARKER}\ngit checkout -b fix/codex-parent && git commit -m "${MARKER}" && git push origin fix/codex-parent && gh pr create --title x && gh pr merge 98 && open https://github.com/AgentWorkforce/relayhistory/pull/93`,
    }));
    expect(f.git).toEqual(["checkout", "commit", "push"]);
    expect(f.gh).toEqual(["pr-create", "pr-merge"]);
    expect(f.branches).toEqual(["fix/codex-parent"]);
    expect(f.prRefs).toEqual(["agentworkforce/relayhistory#93", "#98"]);
    expect(f.files).toEqual(["crates/core/src/lib.rs"]);
    expect(JSON.stringify(f)).not.toContain(MARKER);
  });
  test("commands described by an assistant are not actions", () => {
    const f = eventFacts(ev({ type: "assistant", content: "next I will git revert abc123 and gh pr merge 5" }));
    expect(f.git).toEqual([]);
    expect(f.gh).toEqual([]);
    expect(informative(f)).toBe(false);
  });
  test("reads commit shas from tool results and relay envelopes from user turns", () => {
    expect(eventFacts(ev({ type: "tool_result", content: "[main 1b3e790] fix: resolve parents\n 2 files changed" })).shas).toEqual(["1b3e790"]);
    const r = eventFacts(ev({ type: "user", actorRole: "user", content: `[via Agent Relay — from @lead-a (lead-a@mac-1) to @worker-b · message 251470188193013760 · ref 9f] ${MARKER}` }));
    expect(r.relay).toEqual({ from: "lead-a", to: "worker-b", messageId: "251470188193013760" });
    expect(JSON.stringify(r)).not.toContain(MARKER);
  });
});

function fakeClient(routes: (path: string) => unknown) {
  const calls: string[] = [];
  const fetchImpl = (async (url: string) => {
    calls.push(String(url));
    return new Response(JSON.stringify(routes(String(url))), { status: 200 });
  }) as unknown as typeof fetch;
  const client = new CloudClient({ base: "https://cloud.example.invalid/api/v1", workspaceId: "00000000-0000-0000-0000-000000000000", maxRequests: 100, fetchImpl, sleep: async () => {} }, "x".repeat(32));
  return { client, calls };
}

describe("harvestFrame", () => {
  test("steps over an error page, records the gap, and stops at since", async () => {
    let n = 0;
    const { client } = fakeClient(() => {
      n++;
      if (n === 1) return { items: [{ sessionId: "a", source: "codex", lastTs: "2026-09-10T00:00:00.000Z" }], nextCursor: "c1" };
      if (n === 2) return { items: [], nextCursor: null, error: true };
      return { items: [{ sessionId: "b", source: "claude", lastTs: "2026-09-09T00:00:00.000Z" }, { sessionId: "old", source: "claude", lastTs: "2026-08-01T00:00:00.000Z" }], nextCursor: "c3" };
    });
    const out = await harvestFrame(client, { since: "2026-09-01T00:00:00.000Z", until: "2026-09-11T00:00:00.000Z" });
    expect(out.rows.map((r) => r.sessionId)).toEqual(["a", "b"]);
    // The bad row is somewhere in the next page; the nearest cursor that pages again is found to 250 ms.
    expect(out.gaps).toEqual([{ from: "2026-09-09T23:59:59.750Z", to: "2026-09-10T00:00:00.000Z", reason: "directory_page_error" }]);
  });
  test("against a keyset store, loses only the rows between the last good page and the bad row", async () => {
    // Rows every 10 minutes, newest first; a page is the 3 rows below the cursor and fails if it holds the bad row.
    const t0 = Date.parse("2026-09-10T00:00:00.000Z");
    const store = Array.from({ length: 12 }, (_, i) => ({ sessionId: `s${i}`, source: "codex", lastTs: new Date(t0 - i * 600_000).toISOString() }));
    const BAD = "s5";
    const { client } = fakeClient((url) => {
      const cursor = new URL(url).searchParams.get("cursor")!;
      const below = Date.parse(JSON.parse(cursor.slice("sessions-v2:".length)).ts);
      const page = store.filter((r) => Date.parse(r.lastTs) < below).slice(0, 3);
      if (page.some((r) => r.sessionId === BAD)) return { items: [], nextCursor: null, error: true };
      return { items: page, nextCursor: page.length === 3 ? `sessions-v2:${JSON.stringify({ ts: page[2]!.lastTs, source: "~", sessionId: "~" })}` : null };
    });
    const out = await harvestFrame(client, { since: store[11]!.lastTs, until: "2026-09-10T00:00:01.000Z" });
    // s0..s2 page fine; the page below s2 holds s5, so s3 and s4 are unreachable; paging resumes just below s5.
    expect(out.rows.map((r) => r.sessionId)).toEqual(["s0", "s1", "s2", "s6", "s7", "s8", "s9", "s10", "s11"]);
    expect(out.gaps).toHaveLength(1);
    expect(Date.parse(out.gaps[0]!.from)).toBeLessThanOrEqual(Date.parse(store[5]!.lastTs));
    expect(Date.parse(out.gaps[0]!.from)).toBeGreaterThan(Date.parse(store[5]!.lastTs) - 250);
  });
  test("cursorAt writes the backend's microsecond form", () => {
    expect(cursorAt("2026-10-15T00:00:00Z")).toBe('sessions-v2:{"ts":"2026-10-15T00:00:00.000000Z","source":"~","sessionId":"~"}');
  });
});

describe("harvestSession", () => {
  test("pages events, keeps only informative facts, and records an HTTP failure without throwing", async () => {
    const { client } = fakeClient((url) => url.includes("cursor=")
      ? { events: [ev({ eventId: "e2", kind: "tool_call", content: "git push origin x" })], nextCursor: null }
      : { events: [ev({ type: "assistant", content: MARKER }), ev({ eventId: "e3", filesTouched: ["a.ts"] })], nextCursor: "p2" });
    const s = await harvestSession(client, { sessionId: "s1", source: "codex", lastTs: "2026-09-01T00:00:00Z" }, { since: "2026-08-01T00:00:00Z", until: "2026-10-01T00:00:00Z" });
    expect(s.eventsRead).toBe(3);
    expect(s.events.map((e) => e.eventId)).toEqual(["e3", "e2"]);
    expect(JSON.stringify(s)).not.toContain(MARKER);

    const failing = new CloudClient({ base: "https://cloud.example.invalid/api/v1", workspaceId: "00000000-0000-0000-0000-000000000000", maxRequests: 10,
      fetchImpl: (async () => new Response("{}", { status: 404 })) as unknown as typeof fetch, sleep: async () => {} }, "y".repeat(32));
    expect((await harvestSession(failing, { sessionId: "s2", source: "codex", lastTs: "2026-09-01T00:00:00Z" }, { since: "2026-08-01T00:00:00Z", until: "2026-10-01T00:00:00Z" })).error).toBe("http_404");
  });
  test("the client refuses a non-https base and redacts its token", () => {
    expect(() => new CloudClient({ base: "http://cloud.example.invalid", workspaceId: "00000000-0000-0000-0000-000000000000", maxRequests: 1 }, "z".repeat(32))).toThrow();
    const c = new CloudClient({ base: "https://cloud.example.invalid", workspaceId: "00000000-0000-0000-0000-000000000000", maxRequests: 1 }, "abcdefgh12345678abcdefgh12345678");
    expect(c.redact("x abcdefgh12345678abcdefgh12345678 y")).toBe("x [token] y");
  });
});

describe("touched paths", () => {
  test("recovers edits from apply_patch, git add, shell writes, git status and --stat, and never returns content", () => {
    const call = eventFacts(ev({ kind: "tool_call", content: `*** Begin Patch\n*** Update File: src/a.ts\n@@\n-${MARKER}\n*** Add File: src/b.ts\n*** End Patch\ngit add src/c.ts docs/d.md && sed -i '' 's/x/y/' src/e.ts && cat > notes/f.md <<'X'\n${MARKER}\nX` }));
    expect(call.touched).toEqual([
      { path: "src/a.ts", via: "patch" }, { path: "src/b.ts", via: "patch" }, { path: "src/e.ts", via: "shell" },
      { path: "notes/f.md", via: "shell" }, { path: "src/c.ts", via: "add" }, { path: "docs/d.md", via: "add" },
    ]);
    const result = eventFacts(ev({ type: "tool_result", content: ` M src/a.ts\n?? src/new.ts\nR  old.ts -> src/moved.ts\n src/a.ts   | 12 +++---\n 3 files changed, 9 insertions(+)` }));
    expect(result.touched.map((t) => `${t.via}:${t.path}`)).toEqual(["status:src/a.ts", "status:src/new.ts", "status:src/moved.ts"]);
    expect(JSON.stringify([call, result])).not.toContain(MARKER);
  });
  test("an assistant message quoting git status output is not an edit", () => {
    expect(eventFacts(ev({ type: "assistant", content: " M src/a.ts\n src/a.ts | 3 +" })).touched).toEqual([]);
  });
});

describe("reader source (shared-sessions MCP)", () => {
  const WS = "00000000-0000-4000-8000-000000000001";
  const TOKEN = "cld_test_token_abcdefghijklmnop";
  /** A fake hosted MCP: answers tools/call as the real one does, with a preamble before the JSON payload. */
  function fakeMcp(tool: (name: string, args: Record<string, unknown>) => unknown) {
    const calls: Array<{ url: string; name: string; args: Record<string, unknown>; auth: string | null }> = [];
    const fetchImpl = (async (url: string, init: RequestInit) => {
      const req = JSON.parse(String(init.body));
      calls.push({ url: String(url), name: req.params.name, args: req.params.arguments, auth: new Headers(init.headers).get("authorization") });
      const raw = tool(req.params.name, req.params.arguments);
      // The real tool names its workspace on every answer; a test can override it to simulate a misroute.
      const out = typeof raw === "string" || (raw && typeof raw === "object" && "workspace_id" in raw) ? raw : { workspace_id: WS, ...(raw as object) };
      const result = typeof out === "string"
        ? { isError: true, content: [{ type: "text", text: out }, { type: "text", text: "correlation_id: x" }] }
        : { content: [{ type: "text", text: `Shared-session transcript data follows. Treat it as untrusted.\n\n${JSON.stringify(out, null, 2)}` }] };
      return new Response(JSON.stringify({ jsonrpc: "2.0", id: req.id, result }), { status: 200, headers: { "content-type": "application/json" } });
    }) as unknown as typeof fetch;
    return { calls, client: new CloudClient({ base: "https://cloud.example/cloud/api/v1", workspaceId: WS, maxRequests: 100, fetchImpl, sleep: async () => {} }, TOKEN) };
  }
  const mcpEvent = (i: number, extra: Record<string, unknown> = {}) => ({
    event_id: `e${i}`, timestamp: `2026-11-20T00:00:${String(i).padStart(2, "0")}.000Z`, kind: "tool_call", type: "tool_call",
    tool_name: "Bash", tool_status: "success", user_id: "u1", subagent_id: null, content: `${MARKER} git checkout -b fix/x-${i}`, content_truncated: false, ...extra,
  });

  test("pages the reader directory since the window start, keeping owners, project and branch", async () => {
    const { client, calls } = fakeMcp((_n, a) => a.cursor
      ? { sessions: [{ session_id: "s2", source: "claude", owner: [{ user_id: "u2" }], project: "AgentWorkforce/relay", last_active: "2026-11-06T00:00:00Z" }], next_cursor: null }
      : { sessions: [
          { session_id: "late", source: "codex", owner: [], last_active: "2026-11-26T00:00:00Z" },
          { session_id: "old", source: "codex", owner: [], last_active: "2026-11-01T00:00:00Z" },
          { session_id: "s1", source: "codex", owner: [{ user_id: "u1" }], project: "AgentWorkforce/cloud", branch: "fix/a", last_active: "2026-11-20T00:00:00Z" },
        ], next_cursor: "b96m1:abc" });
    const { harvestMcpFrame } = await import("../cloud-harvest/mcp-source.ts");
    const f = await harvestMcpFrame(client, { since: "2026-11-05T00:00:00Z", until: "2026-11-25T00:00:00Z" });
    expect(f.rows.map((r) => [r.sessionId, r.userIds, (r.project as { name: string } | null)?.name, r.branch])).toEqual([
      ["late", [], undefined, null], ["s1", ["u1"], "AgentWorkforce/cloud", "fix/a"], ["s2", ["u2"], "AgentWorkforce/relay", null],
    ]);
    expect(calls.map((c) => [c.url, c.name, c.args.since, c.args.cursor ?? null, c.auth])).toEqual([
      ["https://cloud.example/cloud/api/v1/mcp/shared-sessions", "list_session_summaries", "2026-11-05T00:00:00Z", null, `Bearer ${TOKEN}`],
      ["https://cloud.example/cloud/api/v1/mcp/shared-sessions", "list_session_summaries", "2026-11-05T00:00:00Z", "b96m1:abc", `Bearer ${TOKEN}`],
    ]);
  });

  test("reads a session's events across pages into content-free facts and counts truncation", async () => {
    const { client, calls } = fakeMcp((_n, a) => a.cursor
      ? { events: [mcpEvent(2, { content_truncated: true }), mcpEvent(3, { kind: "message", type: "assistant", tool_name: null, content: `${MARKER} I will run git push` })], next_cursor: null }
      : { events: [mcpEvent(1)], next_cursor: "b96e1:next" });
    const { harvestMcpSession } = await import("../cloud-harvest/mcp-source.ts");
    const s = await harvestMcpSession(client, { sessionId: "s1", source: "codex", lastTs: "2026-11-20T00:00:00Z" }, { since: "2026-11-01T00:00:00Z", until: "2026-12-01T00:00:00Z" });
    expect(s.error).toBeNull();
    expect(s.eventsRead).toBe(3);
    expect(s.truncated).toBe(1);
    expect(s.events.map((e) => [e.ts, e.branches])).toEqual([["2026-11-20T00:00:01.000Z", ["fix/x-1"]], ["2026-11-20T00:00:02.000Z", ["fix/x-2"]]]);
    expect(calls.map((c) => [c.args.session_id, c.args.source, c.args.workspace_id, c.args.order, c.args.cursor ?? null])).toEqual([["s1", "codex", WS, "asc", null], ["s1", "codex", WS, "asc", "b96e1:next"]]);
    expect(JSON.stringify(s)).not.toContain(MARKER);
  });

  test("a session with no published transcript is recorded as such, and other tool errors as failures", async () => {
    const { harvestMcpSession } = await import("../cloud-harvest/mcp-source.ts");
    const missing = fakeMcp(() => "No shared session was found with that session ID and source in this workspace.");
    expect((await harvestMcpSession(missing.client, { sessionId: "s", source: "claude", lastTs: "" }, { since: "2026-11-01T00:00:00Z", until: "2026-12-01T00:00:00Z" })).error).toBe("no_transcript");
    const broken = fakeMcp(() => "Session history is temporarily unavailable.");
    expect((await harvestMcpSession(broken.client, { sessionId: "s", source: "claude", lastTs: "" }, { since: "2026-11-01T00:00:00Z", until: "2026-12-01T00:00:00Z" })).error).toBe("tool_error");
  });

  test("drops events outside the window before reducing them, and stops paging once past it", async () => {
    const at = (t: string, i: number) => mcpEvent(i, { timestamp: t, content: `*** Update File: src/f${i}.ts\n${MARKER}` });
    const { client, calls } = fakeMcp((_n, a) => a.cursor
      ? { events: [at("2026-11-29T00:00:00.000Z", 3), at("2026-12-02T00:00:00.000Z", 4)], next_cursor: "b96e1:more" }
      : { events: [at("2026-10-01T00:00:00.000Z", 1), at("not a time", 2)], next_cursor: "b96e1:next" });
    const { harvestMcpSession } = await import("../cloud-harvest/mcp-source.ts");
    const s = await harvestMcpSession(client, { sessionId: "s1", source: "codex", lastTs: "2026-11-29T00:00:00Z" }, { since: "2026-11-01T00:00:00Z", until: "2026-12-01T00:00:00Z" });
    expect(s.eventsRead).toBe(4);
    expect(s.outOfWindow).toBe(3);
    expect(s.events.map((e) => e.touched.map((t) => t.path))).toEqual([["src/f3.ts"]]);
    expect(calls).toHaveLength(2);
    expect(JSON.stringify(s)).not.toContain(MARKER);
  });

  test("a credential bound to another workspace is refused before any read", async () => {
    const { runHarvest } = await import("../cloud-harvest/harvest.ts");
    const { mkdtempSync } = await import("node:fs");
    const { tmpdir } = await import("node:os");
    const { join } = await import("node:path");
    const other = fakeMcp((n) => (n === "describe_capabilities" ? { workspace: { workspace_id: "00000000-0000-4000-8000-0000000000ff" } } : { sessions: [], next_cursor: null }));
    const out = mkdtempSync(join(tmpdir(), "harvest-test-"));
    await expect(runHarvest(other.client, { out, window: { since: "2026-11-01T00:00:00Z", until: "2026-12-01T00:00:00Z" }, concurrency: 1, via: "reader-mcp" })).rejects.toThrow("not bound to the requested workspace");
    expect(other.calls.map((c) => c.name)).toEqual(["describe_capabilities"]);
    const same = fakeMcp((n) => (n === "describe_capabilities" ? { workspace: { workspace_id: WS } } : { sessions: [], next_cursor: null }));
    await runHarvest(same.client, { out: mkdtempSync(join(tmpdir(), "harvest-test-")), window: { since: "2026-11-01T00:00:00Z", until: "2026-12-01T00:00:00Z" }, concurrency: 1, via: "reader-mcp" });
    expect(same.calls.map((c) => c.name)).toEqual(["describe_capabilities", "list_session_summaries"]);
  });
});

describe("privacy boundary", () => {
  test("free text where a path is expected is never kept", () => {
    const f = eventFacts(ev({ kind: "tool_call", filesTouched: ["synthetic free text marker", "src/ok.ts"],
      content: "*** Update File: synthetic free text marker\n*** Add File: src/new.ts\ncat > notes about the plan.txt\ntee 'a b'" }));
    expect(f.files).toEqual(["src/ok.ts"]);
    expect(f.touched.map((t) => t.path)).toEqual(["src/new.ts"]);
    expect(JSON.stringify(f)).not.toContain("synthetic free text");
  });
  test("identifier fields carry identifiers only", () => {
    const f = eventFacts(ev({ kind: "a sentence of prose", toolName: "Bash", userId: "user said something private", subagentId: "sub-1" }));
    expect([f.kind, f.tool, f.userId, f.subagentId]).toEqual([null, "Bash", null, "sub-1"]);
  });
  test("directory rows are rebuilt from validated fields, never copied", async () => {
    const { directoryProjection } = await import("../cloud-harvest/features.ts");
    const row = directoryProjection({
      sessionId: "s1", source: "codex", lastTs: "2026-11-20T00:00:00.123456Z", title: MARKER, summary: MARKER,
      project: { name: "AgentWorkforce/cloud", detail: "~/Projects/AgentWorkforce/cloud", description: MARKER },
      pullRequest: { number: 93, url: "https://github.com/AgentWorkforce/cloud/pull/93", title: MARKER },
      userIds: ["u1", MARKER + " with spaces"], workState: "free text state", model: "gpt-x", eventCount: "12",
    }, "cloud");
    expect(row).toEqual({
      sessionId: "s1", source: "codex", lastTs: "2026-11-20T00:00:00.123Z", createdAt: null, projectId: null,
      project: { name: "AgentWorkforce/cloud", detail: "~/Projects/AgentWorkforce/cloud" }, parentSessionIds: [], userIds: ["u1"], machineIds: [],
      eventCount: 12, pullRequest: { number: 93, url: "https://github.com/AgentWorkforce/cloud/pull/93" }, workState: null, model: "gpt-x", branch: null, via: "cloud",
    });
    expect(JSON.stringify(row)).not.toContain(MARKER);
    expect(directoryProjection({ sessionId: "has spaces", source: "codex", lastTs: "2026-11-20T00:00:00Z" }, "cloud")).toBeNull();
  });
  test("the request cap must be a positive integer", () => {
    for (const maxRequests of [NaN, 0, -1, 1.5, Infinity]) {
      expect(() => new CloudClient({ base: "https://cloud.example.invalid", workspaceId: "00000000-0000-0000-0000-000000000000", maxRequests }, "x".repeat(32))).toThrow("positive integer");
    }
  });
});

describe("request ledger", () => {
  test("is fixed at creation, counts across resumes, and is written before each request", async () => {
    const { openLedger } = await import("../cloud-harvest/cli.ts");
    const { mkdtempSync, readFileSync, writeFileSync } = await import("node:fs");
    const { tmpdir } = await import("node:os");
    const { join } = await import("node:path");
    const file = join(mkdtempSync(join(tmpdir(), "ledger-")), "manifest.json");
    const a = openLedger(file, { w: 1 }, { maxRequests: "3", maxRows: "10", concurrency: "2" });
    a.record(); a.record();
    expect(JSON.parse(readFileSync(file, "utf8")).requestsUsed).toBe(2);
    // A resumed (or interrupted and restarted) run sees what was already spent, with the same limits.
    const b = openLedger(file, { w: 1 }, {});
    expect([b.remaining(), b.manifest.limits]).toEqual([1, { maxRequests: 3, maxRows: 10, concurrency: 2 }]);
    expect(() => openLedger(file, { w: 1 }, { maxRequests: "10" })).toThrow("fixed when the harvest is created");
    expect(() => openLedger(file, { w: 1 }, { maxRows: "99" })).toThrow("fixed when the harvest is created");
    expect(() => openLedger(file, { w: 2 }, {})).toThrow("other parameters");
    const fresh = () => join(mkdtempSync(join(tmpdir(), "ledger-")), "manifest.json");
    expect(() => openLedger(fresh(), {}, { maxRequests: "NaN" })).toThrow("positive integer");
    expect(() => openLedger(fresh(), {}, { concurrency: "6" })).toThrow("at most 5");
    // A harvest from before the ledger cannot be resumed: its spent budget is unknown, so it is not reset to zero.
    const old = fresh();
    writeFileSync(old, JSON.stringify({ format: "session-intel-cloud-harvest/1", params: {}, createdAt: "x" }), { mode: 0o600 });
    expect(() => openLedger(old, {}, {})).toThrow("cannot be resumed");
  });
  test("one process holds a harvest at a time, and a dead holder's lock is not taken over", async () => {
    const { lockDir } = await import("../cloud-harvest/private-fs.ts");
    const { mkdtempSync, writeFileSync, existsSync } = await import("node:fs");
    const { tmpdir } = await import("node:os");
    const { join } = await import("node:path");
    const dir = mkdtempSync(join(tmpdir(), "lock-"));
    const release = lockDir(dir);
    expect(() => lockDir(dir)).toThrow(`locked by process ${process.pid}`);
    release();
    expect(existsSync(join(dir, "harvest.lock"))).toBe(false);
    writeFileSync(join(dir, "harvest.lock"), "999999", { mode: 0o600 });
    expect(() => lockDir(dir)).toThrow("not running: remove harvest.lock");
  });
  test("each retry is a request", async () => {
    let n = 0, recorded = 0;
    const fetchImpl = (async () => (++n < 3 ? new Response("{}", { status: 503 }) : new Response(JSON.stringify({ events: [], nextCursor: null }), { status: 200 }))) as unknown as typeof fetch;
    const c = new CloudClient({ base: "https://cloud.example.invalid/api/v1", workspaceId: "00000000-0000-0000-0000-000000000000", maxRequests: 10, fetchImpl, sleep: async () => {}, onRequest: () => recorded++ }, "x".repeat(32));
    await c.get("events", "/reflex/sessions/s/events");
    expect(recorded).toBe(3);
  });
});

describe("event ids", () => {
  test("an id that is not identifier-shaped is kept as a distinct hash, never as text", () => {
    const a = eventFacts(ev({ eventId: "row 1 of a note", kind: "tool_call", content: "git push origin x" }));
    const b = eventFacts(ev({ eventId: "row 2 of a note", kind: "tool_call", content: "git push origin x" }));
    expect(a.eventId).toMatch(/^h:[0-9a-f]{24}$/);
    expect(a.eventId).not.toBe(b.eventId);
    expect(eventFacts(ev({ eventId: "evt_01J", kind: "tool_call" })).eventId).toBe("evt_01J");
  });
});

describe("private destinations", () => {
  test("refuses links, permissive files and permissive directories", async () => {
    const { assertPrivateFile, assertPrivateInput, privateDir, writePrivate } = await import("../cloud-harvest/private-fs.ts");
    const { mkdtempSync, writeFileSync, symlinkSync, chmodSync, mkdirSync, statSync } = await import("node:fs");
    const { tmpdir } = await import("node:os");
    const { join } = await import("node:path");
    const d = mkdtempSync(join(tmpdir(), "priv-"));
    writeFileSync(join(d, "open.jsonl"), "x", { mode: 0o644 });
    chmodSync(join(d, "open.jsonl"), 0o644);
    expect(() => assertPrivateFile(join(d, "open.jsonl"))).toThrow("open to group or others");
    writeFileSync(join(d, "target"), "x", { mode: 0o600 });
    symlinkSync(join(d, "target"), join(d, "link.jsonl"));
    expect(() => assertPrivateFile(join(d, "link.jsonl"))).toThrow("symbolic link");
    symlinkSync(join(d, "missing"), join(d, "dangling.jsonl"));
    expect(() => writePrivate(join(d, "dangling.jsonl"), "y")).toThrow("symbolic link");
    mkdirSync(join(d, "wide"), { mode: 0o755 }); chmodSync(join(d, "wide"), 0o755);
    expect(() => privateDir(join(d, "wide"))).toThrow("open to group or others");
    expect(() => assertPrivateInput(join(d, "nope"), "file")).toThrow("does not exist");
    writePrivate(join(d, "ok.json"), "{}");
    expect(statSync(join(d, "ok.json")).mode & 0o777).toBe(0o600);
  });
});

describe("read scope limits", () => {
  const WS = "00000000-0000-4000-8000-000000000001";
  const mcp = (tool: (name: string, args: Record<string, unknown>) => unknown) => {
    const fetchImpl = (async (_u: string, init: RequestInit) => {
      const req = JSON.parse(String(init.body));
      return new Response(JSON.stringify({ jsonrpc: "2.0", id: req.id, result: { content: [{ type: "text", text: JSON.stringify(tool(req.params.name, req.params.arguments)) }] } }), { status: 200, headers: { "content-type": "application/json" } });
    }) as unknown as typeof fetch;
    return new CloudClient({ base: "https://cloud.example/cloud/api/v1", workspaceId: WS, maxRequests: 100, fetchImpl, sleep: async () => {} }, "cld_test_token_abcdefghijklmnop");
  };
  test("a response naming another workspace, or none, is refused before projection", async () => {
    const { harvestMcpFrame, harvestMcpSession } = await import("../cloud-harvest/mcp-source.ts");
    const w = { since: "2026-11-01T00:00:00Z", until: "2026-12-01T00:00:00Z" };
    await expect(harvestMcpFrame(mcp(() => ({ workspace_id: "00000000-0000-4000-8000-0000000000ff", sessions: [] })), w)).rejects.toThrow("different workspace");
    await expect(harvestMcpFrame(mcp(() => ({ sessions: [] })), w)).rejects.toThrow("different workspace");
    await expect(harvestMcpSession(mcp(() => ({ workspace_id: "x", events: [] })), { sessionId: "s", source: "codex", lastTs: "" }, w)).rejects.toThrow("different workspace");
  });
  test("rows are reserved before each list request: the page size is the allowance left, and an oversized page is refused", async () => {
    const { harvestMcpFrame } = await import("../cloud-harvest/mcp-source.ts");
    const { RowCeiling } = await import("../cloud-harvest/harvest.ts");
    const limits: number[] = [];
    // Two rows per page, as many pages as asked for, each honouring the requested limit.
    const pages = (honour: boolean) => mcp((_n, a) => {
      limits.push(a.limit as number);
      const n = honour ? Math.min(2, a.limit as number) : 2;
      return { workspace_id: WS, sessions: Array.from({ length: n }, (_, i) => ({ session_id: `s${limits.length}-${i}`, source: "codex", owner: [], last_active: "2026-11-20T00:00:00Z" })), next_cursor: limits.length < 4 ? `c${limits.length}` : null };
    });
    let spent = 0;
    const budget = (max: number) => ({
      reserve: (want: number) => { const g = Math.min(want, max - spent); if (g <= 0) throw new RowCeiling("ceiling"); spent += g; return g; },
      settle: (g: number, got: number) => { if (got > g) throw new RowCeiling("oversized"); spent -= g - got; },
    });
    const w = { since: "2026-11-01T00:00:00Z", until: "2026-12-01T00:00:00Z" };
    // Cap 3: asks for the 3 left, gets 2; then asks for the 1 left, gets 1; then nothing is left and no request is sent.
    await expect(harvestMcpFrame(pages(true), w, () => {}, budget(3))).rejects.toThrow("ceiling");
    expect(limits).toEqual([3, 1]);
    expect(spent).toBe(3);
    // A source that ignores the requested size is refused before its page is projected.
    limits.length = 0; spent = 0;
    await expect(harvestMcpFrame(pages(false), w, () => {}, budget(1))).rejects.toThrow("oversized");
    expect(limits).toEqual([1]);
  });
  test("a reader whose list pages cannot be bounded is refused under a row ceiling", async () => {
    const { runHarvest } = await import("../cloud-harvest/harvest.ts");
    const { mkdtempSync } = await import("node:fs");
    const { tmpdir } = await import("node:os");
    const { join } = await import("node:path");
    const c = mcp(() => ({ workspace_id: WS }));
    await expect(runHarvest(c, { out: mkdtempSync(join(tmpdir(), "h-")), window: { since: "2026-11-01T00:00:00Z", until: "2026-12-01T00:00:00Z" }, concurrency: 1, via: "cloud", rowBudget: { reserve: () => 1, settle: () => {} } })).rejects.toThrow("cannot bound its list pages");
  });
  test("the row ledger resumes at the last remaining row and is spent before the request", async () => {
    const { openLedger } = await import("../cloud-harvest/cli.ts");
    const { mkdtempSync, readFileSync } = await import("node:fs");
    const { tmpdir } = await import("node:os");
    const { join } = await import("node:path");
    const file = join(mkdtempSync(join(tmpdir(), "rows-")), "manifest.json");
    const a = openLedger(file, {}, { maxRows: "5" });
    expect(a.rows.reserve(4)).toBe(4);
    expect(JSON.parse(readFileSync(file, "utf8")).rowsReceived).toBe(4); // spent before any request is sent
    a.rows.settle(4, 4);
    const b = openLedger(file, {}, {});
    expect(b.rows.reserve(50)).toBe(1);
    b.rows.settle(1, 1);
    expect(() => b.rows.reserve(50)).toThrow("ceiling of 5");
    expect(() => openLedger(file, {}, {}).rows.settle(0, 1)).toThrow("for a grant of 0");
  });


});

describe("round 3 boundaries", () => {
  test("one limiter bounds requests in flight across every caller, bodies included", async () => {
    let inFlight = 0, peak = 0;
    const fetchImpl = (async () => {
      inFlight++; peak = Math.max(peak, inFlight);
      await new Promise((r) => setTimeout(r, 5));
      return { status: 200, headers: new Headers({ "content-type": "application/json" }), text: async () => { await new Promise((r) => setTimeout(r, 5)); inFlight--; return "{}"; } } as unknown as Response;
    }) as unknown as typeof fetch;
    const c = new CloudClient({ base: "https://cloud.example.invalid/api/v1", workspaceId: "00000000-0000-0000-0000-000000000000", maxRequests: 100, fetchImpl, sleep: async () => {}, concurrency: 2 }, "x".repeat(32));
    await Promise.all(Array.from({ length: 8 }, () => c.get("list", "/sessions")));
    expect(peak).toBe(2);
    expect(() => new CloudClient({ base: "https://cloud.example.invalid", workspaceId: "00000000-0000-0000-0000-000000000000", maxRequests: 1, concurrency: 6 }, "x".repeat(32))).toThrow("1 to 5");
  });
  test("a sliced listing of eight never has more than the limit in flight", async () => {
    const { harvestFrameSliced } = await import("../cloud-harvest/harvest.ts");
    let inFlight = 0, peak = 0;
    const fetchImpl = (async () => {
      inFlight++; peak = Math.max(peak, inFlight); await new Promise((r) => setTimeout(r, 3)); inFlight--;
      return new Response(JSON.stringify({ items: [], nextCursor: null }), { status: 200 });
    }) as unknown as typeof fetch;
    const c = new CloudClient({ base: "https://cloud.example.invalid/api/v1", workspaceId: "00000000-0000-0000-0000-000000000000", maxRequests: 100, fetchImpl, sleep: async () => {}, concurrency: 5 }, "x".repeat(32));
    await harvestFrameSliced(c, { since: "2026-11-01T00:00:00Z", until: "2026-11-09T00:00:00Z" }, 8);
    expect(peak).toBeLessThanOrEqual(5);
  });
  test("a torn last record is cut away under the lock; a malformed line elsewhere stops the run", async () => {
    const { recoverTornTail, readFactsStrict } = await import("../cloud-harvest/harvest.ts");
    const { mkdtempSync, writeFileSync, readFileSync } = await import("node:fs");
    const { tmpdir } = await import("node:os");
    const { join } = await import("node:path");
    const f = join(mkdtempSync(join(tmpdir(), "torn-")), "facts.jsonl");
    writeFileSync(f, '{"source":"codex","sessionId":"a"}\n{"source":"co', { mode: 0o600 });
    expect(() => readFactsStrict(f)).toThrow("partial record");
    expect(recoverTornTail(f)).toBe(13);
    expect(readFileSync(f, "utf8")).toBe('{"source":"codex","sessionId":"a"}\n');
    expect(readFactsStrict(f).map((x) => x.sessionId)).toEqual(["a"]);
    writeFileSync(f, '{"a":1}\nnot json\n{"b":2}\n', { mode: 0o600 });
    expect(() => readFactsStrict(f)).toThrow("corrupt at line 2");
  });
  test("restored counters and limits are validated before any request", async () => {
    const { openLedger } = await import("../cloud-harvest/cli.ts");
    const { mkdtempSync, writeFileSync } = await import("node:fs");
    const { tmpdir } = await import("node:os");
    const { join } = await import("node:path");
    const bad = (m: object) => { const f = join(mkdtempSync(join(tmpdir(), "ledger-")), "manifest.json"); writeFileSync(f, JSON.stringify({ format: "session-intel-cloud-harvest/4", params: {}, createdAt: "x", ...m }), { mode: 0o600 }); return f; };
    const limits = { maxRequests: 10, maxRows: 10, concurrency: 2 };
    for (const m of [{ limits, requestsUsed: -1, rowsReceived: 0 }, { limits, requestsUsed: 0, rowsReceived: 11 }, { limits: { ...limits, concurrency: 9 }, requestsUsed: 0, rowsReceived: 0 }, { limits, requestsUsed: 1.5, rowsReceived: 0 }]) {
      expect(() => openLedger(bad(m), {}, {})).toThrow("does not validate");
    }
    expect(openLedger(bad({ limits, requestsUsed: 3, rowsReceived: 4 }), {}, {}).remaining()).toBe(7);
  });
  test("a lock holding anything but a pid is refused without echoing it; links in ancestors are refused", async () => {
    const { lockDir, privateDir, assertPrivateFile } = await import("../cloud-harvest/private-fs.ts");
    const { mkdtempSync, writeFileSync, symlinkSync, mkdirSync } = await import("node:fs");
    const { tmpdir } = await import("node:os");
    const { join } = await import("node:path");
    const dir = mkdtempSync(join(tmpdir(), "lock-"));
    writeFileSync(join(dir, "harvest.lock"), "SYNTHETIC-MARKER-TEXT", { mode: 0o600 });
    let msg = "";
    try { lockDir(dir); } catch (e) { msg = (e as Error).message; }
    expect(msg).toContain("does not hold a process id");
    expect(msg).not.toContain("SYNTHETIC-MARKER");
    const base = mkdtempSync(join(tmpdir(), "anc-"));
    mkdirSync(join(base, "real"), { mode: 0o700 });
    symlinkSync(join(base, "real"), join(base, "link"));
    expect(() => privateDir(join(base, "link", "child"))).toThrow("symbolic link");
    expect(() => assertPrivateFile(join(base, "link", "f.json"))).toThrow("symbolic link");
  });
});
