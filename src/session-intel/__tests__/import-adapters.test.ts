/** The trace import adapters and normalization (docs/session-intel-import.md), over synthetic fixtures only. */
import { describe, expect, test } from "bun:test";
import { symlinkSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { readClaudeCode, readCodex, readGeneric, SessionSet } from "../import/adapters.ts";
import { commitShaFromOutput, gitOps } from "../import/git.ts";
import { listInputFiles, readJsonl } from "../import/io.ts";
import { emptyStats, Hasher, IMPORT_BOUNDS, projectKey, repoRelativePath, SessionBuilder, toSnapshotSession } from "../import/normalize.ts";
import { tmp } from "./helpers.ts";

const FIX = join(import.meta.dir, "fixtures", "import");
const CLAUDE_SID = "00000000-0000-4000-8000-0000000c1a0d";
const CODEX_SID = "00000000-0000-4000-8000-00000000c0de";
const at = (hm: string) => `2026-11-01T${hm}:00.000Z`;

async function read(dir: string, fn: typeof readClaudeCode) {
  const set = new SessionSet(emptyStats());
  const files = await listInputFiles(dir);
  for (let i = 0; i < files.length; i++) await fn(files[i]!, i, set);
  return set;
}
const facts = (b: SessionBuilder) => ({
  writes: b.writes.map((w) => [w.path, w.ts]), commits: b.commits.map((c) => [c.sha, c.ts]),
  outcomes: b.outcomes.map((o) => [o.key, o.reverted, o.ts]), usage: b.usage.map((u) => u.tokens),
});

describe("claude code adapter", () => {
  test("file paths, commit sha, revert, deduped usage, and the subagent's parent link", async () => {
    const set = await read(join(FIX, "claude"), readClaudeCode);
    const main = set.byKey.get(`claude\u0000${CLAUDE_SID}`)!;
    const sub = set.byKey.get(`claude\u0000${CLAUDE_SID}#agent-fxsub1`)!;
    expect(set.byKey.size).toBe(2);
    expect(facts(main)).toEqual({
      // The Desktop file, the `..` path and the path that still names the user are dropped.
      writes: [["supabase/migrations/0042_add_index.sql", at("09:10")], ["src/util/strings.ts", at("09:10")]],
      commits: [["1a2b3c4", at("09:15")]],
      outcomes: [["9f8e7d6c", true, at("09:20")]],
      // msg_fx_1 appears on two records; its usage counts once (100 + 50 + 10 + 40).
      usage: [200, 300, 300, 300],
    });
    expect([main.first, main.last]).toEqual([at("09:05"), at("09:21")]);
    expect(main.branch).toBe("feat/fixture-a");
    expect([...main.parents]).toEqual([]);
    expect([...sub.parents]).toEqual([CLAUDE_SID]);
    expect(facts(sub).writes).toEqual([["notebooks/explore.ipynb", at("09:13")], ["docs/guide.md", at("09:13")]]);
    expect(set.stats).toMatchObject({ files: 2, malformedLines: 1, untimedRecords: 1, droppedPaths: 3 });
  });
});

describe("claude code adapter, hostile records", () => {
  test("prototype-named tools, wrong types and missing fields are ignored, never thrown", async () => {
    const dir = tmp("si-imp-hostile-");
    const rec = (content: unknown, extra: Record<string, unknown> = {}) => JSON.stringify({ type: "assistant", sessionId: "h", timestamp: at("09:00"), message: { id: "m", content, usage: { input_tokens: "9" } }, ...extra });
    writeFileSync(join(dir, "h.jsonl"), [
      rec([{ type: "tool_use", id: "a", name: "constructor", input: {} }, { type: "tool_use", id: "b", name: "__proto__", input: null }]),
      rec([{ type: "tool_use", id: "c", name: "Write", input: { file_path: 42 } }, { type: "tool_use", id: "d", name: "Bash", input: { command: ["git", "commit"] } }]),
      rec("plain text"), rec(null), rec([null, 1, "x"]), JSON.stringify([1, 2]), JSON.stringify("str"),
      rec([], { timestamp: "yesterday" }), rec([], { sessionId: { nested: true }, isSidechain: true, agentId: 7 }),
    ].join("\n"));
    const set = await read(dir, readClaudeCode);
    const all = [...set.byKey.values()];
    expect(all.flatMap((b) => [...b.writes, ...b.commits, ...b.usage])).toEqual([]);
    expect(set.stats).toMatchObject({ droppedPaths: 1, untimedRecords: 1, malformedLines: 2 });
  });
});

describe("codex adapter", () => {
  test("a paginated child imports only its own task, not retimestamped parent context", async () => {
    const dir = tmp("si-imp-codex-child-");
    const row = (type: string, payload: unknown) => ({ timestamp: at("09:00"), type, payload });
    const patch = (path: string) => ({ type: "custom_tool_call", name: "apply_patch", input: `*** Begin Patch\n*** Add File: ${path}\n+example\n*** End Patch` });
    const task = (recipient: string) => ({ type: "agent_message", recipient, content: [
      { type: "input_text", text: "Message Type: NEW_TASK\nTask name: fixture" },
      { type: "encrypted_content", encrypted_content: "fixture-encrypted-body" },
    ] });
    writeFileSync(join(dir, "child.jsonl"), [
      row("session_meta", { id: "fixture-child", cwd: "/work/child", agent_path: "/root/child", source: { subagent: { thread_spawn: { parent_thread_id: "fixture-parent" } } } }),
      // The inherited session_meta must not replace the child's cwd or identity.
      row("session_meta", { id: "fixture-parent", cwd: "/work/parent" }),
      row("response_item", patch("src/parent.ts")),
      row("response_item", task("/root")),
      row("response_item", { type: "message", role: "user", content: [{ type: "input_text", text: "Message Type: NEW_TASK\nTask name: fixture" }] }),
      row("response_item", { type: "function_call", name: "shell", call_id: "parent-commit", arguments: JSON.stringify({ command: "git commit -m fixture" }) }),
      row("event_msg", { type: "token_count", info: { total_token_usage: { total_tokens: 900 } } }),
      row("event_msg", { type: "thread_settings_applied", thread_id: "fixture-parent" }),
      row("response_item", task("/root/child")),
      row("event_msg", { type: "thread_settings_applied", thread_id: "fixture-child" }),
      row("response_item", task("/root/child")),
      row("response_item", patch("/work/child/src/own.ts")),
      row("event_msg", { type: "token_count", info: { total_token_usage: { total_tokens: 100 } } }),
      // Resuming the same child can repeat its metadata without a new spawn.
      row("session_meta", { id: "fixture-child", cwd: "/work/child", agent_path: "/root/child", source: { subagent: { thread_spawn: { parent_thread_id: "fixture-parent" } } } }),
      row("response_item", patch("src/resumed.ts")),
    ].map((r) => JSON.stringify(r)).join("\n"));
    const set = await read(dir, readCodex);
    const b = set.byKey.get("codex\u0000fixture-child")!;
    expect([b.cwd, ...b.parents]).toEqual(["/work/child", "fixture-parent"]);
    expect(facts(b)).toEqual({ writes: [["src/own.ts", at("09:00")], ["src/resumed.ts", at("09:00")]], commits: [], outcomes: [], usage: [100] });
    expect(set.stats).toMatchObject({ codexPrefixRecordsDropped: 9, codexChildrenWithoutTaskBoundary: 0 });
  });

  test("a child with inherited context but no verified own task fails closed", async () => {
    const dir = tmp("si-imp-codex-child-boundary-");
    for (const { id, path, recipient, marker } of [
      { id: "fixture-a", path: "/root/child", recipient: "/root/other", marker: true },
      { id: "fixture-b", path: undefined, recipient: "/root/child", marker: true },
      { id: "fixture-c", path: "/root/child", recipient: "/root/child", marker: false },
    ]) {
      writeFileSync(join(dir, `${id}.jsonl`), [
        { timestamp: at("09:00"), type: "session_meta", payload: { id, cwd: "/work/acme", agent_path: path, subagent_history_start_ordinal: 1, source: { subagent: { thread_spawn: { parent_thread_id: "fixture-parent" } } } } },
        ...(marker ? [{ timestamp: at("09:00"), type: "event_msg", payload: { type: "thread_settings_applied", thread_id: id } }] : []),
        { timestamp: at("09:00"), type: "response_item", payload: { type: "agent_message", recipient, content: [{ type: "input_text", text: "Message Type: NEW_TASK\nTask name: fixture" }] } },
        { timestamp: at("09:00"), type: "response_item", payload: { type: "custom_tool_call", name: "apply_patch", input: "*** Begin Patch\n*** Add File: src/inherited.ts\n+example\n*** End Patch" } },
      ].map((r) => JSON.stringify(r)).join("\n"));
    }
    const set = await read(dir, readCodex);
    expect([...set.byKey.values()].flatMap((b) => b.writes)).toEqual([]);
    expect(set.stats).toMatchObject({ codexPrefixRecordsDropped: 8, codexChildrenWithoutTaskBoundary: 3 });
  });

  test("code-mode exec imports nested static apply_patch writes like the production projection", async () => {
    const dir = tmp("si-imp-code-mode-");
    const file = join(dir, "code-mode.jsonl");
    const patch = "*** Begin Patch\n*** Update File: /work/acme-api/src/db/schema.ts\n@@\n-old\n+new\n*** End Patch";
    writeFileSync(file, [
      { timestamp: at("09:00"), type: "session_meta", payload: { id: CODEX_SID, cwd: "/work/acme-api" } },
      { timestamp: at("09:01"), type: "response_item", payload: { type: "custom_tool_call", name: "exec", call_id: "code-1", input: `const patch = ${JSON.stringify(patch)}; text(await tools.apply_patch(patch));` } },
    ].map((row) => JSON.stringify(row)).join("\n"));
    const set = await read(dir, readCodex);
    expect(facts([...set.byKey.values()][0]!).writes).toEqual([["src/db/schema.ts", at("09:01")]]);
  });

  test("function-call exec arguments use the same projection and never execute JavaScript", async () => {
    const dir = tmp("si-imp-code-mode-function-");
    const patch = "*** Begin Patch\n*** Add File: src/new.ts\n+example\n*** End Patch";
    const key = "__sessionIntelImportMustNotExecute";
    writeFileSync(join(dir, "exec.jsonl"), [
      { timestamp: at("09:00"), type: "session_meta", payload: { id: CODEX_SID, cwd: "/work/acme-api" } },
      { timestamp: at("09:01"), type: "response_item", payload: { type: "function_call", name: "exec", arguments: JSON.stringify({ input: `globalThis.${key} = true; text(await tools.apply_patch(${JSON.stringify(patch)}));` }) } },
    ].map((row) => JSON.stringify(row)).join("\n"));
    const set = await read(dir, readCodex);
    expect(facts([...set.byKey.values()][0]!).writes).toEqual([["src/new.ts", at("09:01")]]);
    expect((globalThis as Record<string, unknown>)[key]).toBeUndefined();
  });

  test("code-mode is_error accepts only booleans and malformed values never abort the import", async () => {
    const dir = tmp("si-imp-code-mode-error-flag-");
    // Build the deeply nested JSON directly to exercise an invalid error flag
    // without serializing it in this fixture or forwarding it to the projection.
    const flags = ["true", "false", "null", '"true"', '"false"', "1", "0", "[]", '{"failed":true}', "[".repeat(10_000) + "0" + "]".repeat(10_000)];
    const calls = flags.map((flag, i) => {
      const patch = `*** Begin Patch\n*** Add File: src/flag-${i}.ts\n+example\n*** End Patch`;
      const row = JSON.stringify({ timestamp: at("09:01"), type: "response_item", payload: { type: i % 2 ? "function_call" : "custom_tool_call", name: "exec", call_id: `flag-${i}`, input: `tools.apply_patch(${JSON.stringify(patch)})`, arguments: JSON.stringify({ input: `tools.apply_patch(${JSON.stringify(patch)})` }) } });
      return row.slice(0, -2) + `,"is_error":${flag}}}`;
    });
    writeFileSync(join(dir, "exec.jsonl"), [
      JSON.stringify({ timestamp: at("09:00"), type: "session_meta", payload: { id: CODEX_SID, cwd: "/work/acme-api" } }),
      ...calls,
    ].join("\n"));
    const set = await read(dir, readCodex);
    expect(set.byKey.size).toBe(1);
    expect(facts([...set.byKey.values()][0]!).writes).toEqual(flags.slice(1).map((_, i) => [`src/flag-${i + 1}.ts`, at("09:01")]));
    expect(set.stats.malformedLines).toBe(0);
  });

  test("code-mode comments, quoted code, dynamic templates, oversize input and failed calls give no paths", async () => {
    const dir = tmp("si-imp-code-mode-negative-");
    const patch = "*** Begin Patch\n*** Add File: src/ignored.ts\n+example\n*** End Patch";
    const call = `tools.apply_patch(${JSON.stringify(patch)})`;
    const inputs = [
      { input: `// ${call}` },
      { input: `/* ${call} */` },
      { input: JSON.stringify(call) },
      { input: "tools.apply_patch(`*** Begin Patch\n*** Add File: ${path}\n*** End Patch`)" },
      { input: " ".repeat(1024 * 1024 + 1) + call },
      { input: call, is_error: true },
    ];
    writeFileSync(join(dir, "exec.jsonl"), [
      { timestamp: at("09:00"), type: "session_meta", payload: { id: CODEX_SID, cwd: "/work/acme-api" } },
      ...inputs.map(({ input, ...extra }) => ({ timestamp: at("09:01"), type: "response_item", payload: { type: "custom_tool_call", name: "exec", input, ...extra } })),
    ].map((row) => JSON.stringify(row)).join("\n"));
    const set = await read(dir, readCodex);
    expect(facts([...set.byKey.values()][0]!).writes).toEqual([]);
  });

  test("apply_patch paths (custom tool and shell form), the shell commit sha, token counts", async () => {
    const set = await read(join(FIX, "codex"), readCodex);
    expect(set.byKey.size).toBe(1);
    const b = set.byKey.get(`codex\u0000${CODEX_SID}`)!;
    expect(facts(b)).toEqual({
      writes: [["supabase/migrations/0042_add_index.sql", at("09:31")], ["src/db/schema.ts", at("09:33")]],
      commits: [["abcdef1", at("09:32")]], outcomes: [],
      // last_token_usage, then the delta of total_token_usage when last is absent.
      usage: [1500, 800],
    });
    expect([b.remote, b.branch, b.first, b.last]).toEqual(["git@github.com:fixture-co/acme-api.git", "feat/fixture-b", at("09:20"), at("09:42")]);
    expect(set.stats).toMatchObject({ malformedLines: 1 });
  });

  test("a rollout with no session_meta takes its id from the file name", async () => {
    const dir = tmp("si-imp-codex-");
    const file = join(dir, `rollout-2026-11-01T09-00-00-${CODEX_SID.replace("c0de", "beef")}.jsonl`);
    writeFileSync(file, `${JSON.stringify({ timestamp: at("09:00"), type: "response_item", payload: { type: "local_shell_call", call_id: "x", action: { type: "exec", command: ["git", "revert", "abc1234"] } } })}\n`);
    const set = await read(dir, readCodex);
    const b = [...set.byKey.values()][0]!;
    expect(b.sourceId).toBe(CODEX_SID.replace("c0de", "beef"));
    expect(facts(b).outcomes).toEqual([["abc1234", true, at("09:00")]]);
  });
});

describe("generic adapter", () => {
  test("events become facts; lines that break the format are skipped and counted", async () => {
    const set = await read(join(FIX, "generic"), readGeneric);
    expect([...set.byKey.keys()].sort()).toEqual(["generic\u0000gen-1", "generic\u0000gen-2"]);
    const g1 = set.byKey.get("generic\u0000gen-1")!, g2 = set.byKey.get("generic\u0000gen-2")!;
    expect(facts(g1)).toEqual({ writes: [["src/a.ts", "2026-11-01T09:05:00.000Z"], ["src/b.ts", "2026-11-01T09:05:00.000Z"]], commits: [["deadbee", "2026-11-01T09:06:00.000Z"]], outcomes: [], usage: [100] });
    expect(facts(g2).outcomes).toEqual([["deadbee", true, "2026-11-01T09:20:00.000Z"]]);
    expect([...g2.parents]).toEqual(["gen-1"]);
    expect([g1.project, g1.remote, g1.branch]).toEqual(["fixture-proj", "fixture-co/widgets", "main"]);
    // A non-ISO ts, `files` that is not an array, and a line that is not JSON.
    expect(set.stats.malformedLines).toBe(3);
    // The customer's own kind label is never copied.
    expect([...g1.writes, ...g2.writes].map((w) => w.kind)).toEqual(["write", "write", "write"]);
  });
});

describe("git", () => {
  test("commit and revert detection", () => {
    expect(gitOps("git add -A && git commit -m 'x; y'")).toEqual({ commit: true, reverts: [] });
    expect(gitOps("git -C /tmp/x -c user.name=a commit --amend")).toEqual({ commit: true, reverts: [] });
    expect(gitOps("FOO=1 git revert --no-edit ABC1234 def5678")).toEqual({ commit: false, reverts: ["abc1234", "def5678"] });
    expect(gitOps("git revert HEAD")).toEqual({ commit: false, reverts: [null] });
    expect(gitOps("git revert --abort")).toEqual({ commit: false, reverts: [] });
    expect(gitOps("git commit --dry-run")).toEqual({ commit: false, reverts: [] });
    expect(gitOps("echo git commit")).toEqual({ commit: false, reverts: [] });
    expect(gitOps("git log --oneline")).toEqual({ commit: false, reverts: [] });
  });
  test("sha from git's summary line", () => {
    expect(commitShaFromOutput("[main 1a2b3c4] msg")).toBe("1a2b3c4");
    expect(commitShaFromOutput("x\n[main (root-commit) abcdef12] msg")).toBe("abcdef12");
    expect(commitShaFromOutput("nothing to commit")).toBeNull();
  });
});

describe("path scrubbing", () => {
  const cwd = "/Users/fixture-user/work/acme-api";
  test.each([
    ["/Users/fixture-user/work/acme-api/src/a.ts", cwd, "src/a.ts"],
    ["src/./a.ts", cwd, "src/a.ts"],
    ["C:\\Users\\fixture-user\\repo\\src\\a.ts", null, "src/a.ts"],
    ["/Users/fixture-user/other/lib/b.ts", cwd, "lib/b.ts"],
    ["/home/fixture-user/notes/todo.txt", null, null],
    ["/Users/fixture-user/work/acme-api", cwd, null],
    ["~/x/src/a.ts", null, null],
    ["../src/a.ts", cwd, null],
    ["src/../../etc/passwd", cwd, null],
    ["/Users/fixture-user/x/src/fixture-user/a.ts", cwd, null],
    ["Users/fixture-user/src/a.ts", cwd, null],
    ["home/src/a.ts", null, null],
    ["src/a\u0000.ts", cwd, null],
    ["/etc/passwd", null, null],
    ["", cwd, null],
  ])("%s", (raw, c, want) => {
    expect(repoRelativePath(raw, c)).toBe(want);
  });
  test("no home directory or user name survives any import fixture", async () => {
    for (const [dir, fn] of [["claude", readClaudeCode], ["codex", readCodex], ["generic", readGeneric]] as const) {
      const set = await read(join(FIX, dir), fn);
      for (const b of set.byKey.values()) for (const w of b.writes) expect(w.path).not.toMatch(/fixture-user|^\/|Users|home/u);
    }
  });
  test("project keys come from the remote's repository name or the cwd's basename, never a home directory", () => {
    expect(projectKey("git@github.com:fixture-co/Acme-API.git", "/x/y")).toBe("acme-api");
    expect(projectKey("https://github.com/fixture-co/acme-api/", null)).toBe("acme-api");
    expect(projectKey(null, "/Users/fixture-user/work/acme-api/")).toBe("acme-api");
    expect(projectKey(null, "/Users/fixture-user")).toBeNull();
    expect(projectKey(null, null)).toBeNull();
  });
});

describe("id hashing", () => {
  test("salted, deterministic, domain-separated; branch hashing keeps git's branch equality", () => {
    const a = new Hasher("fixture-salt-0123456789"), b = new Hasher("fixture-salt-9876543210");
    expect(a.session("claude", "x")).toBe(a.session("claude", "x"));
    expect(a.session("claude", "x")).toMatch(/^s_[0-9a-f]{32}$/u);
    expect(a.session("claude", "x")).not.toBe(b.session("claude", "x"));
    expect(a.session("claude", "x")).not.toBe(a.session("codex", "x"));
    expect(a.project("x").slice(2)).not.toBe(a.repo("x").slice(2));
    expect(a.branch("refs/heads/Feat/A")).toBe(a.branch("origin/feat/a"));
    expect(a.branch(null)).toBeNull();
  });

  test("the snapshot line hashes every id, clamps the times to the records and passes the M0 loader", () => {
    const stats = emptyStats();
    const s = new SessionBuilder("claude", "raw-session-id", stats);
    s.cwd = "/Users/fixture-user/work/acme-api";
    s.branch = "feat/x";
    s.parent("raw-parent-id");
    s.touch(at("09:00"), { file: 0, line: 1 });
    s.write("/Users/fixture-user/work/acme-api/src/a.ts", at("09:05"), { file: 0, line: 2 });
    s.commit("not-a-sha", at("09:07"), { file: 0, line: 3 });
    s.tokens(10, at("09:06"), { file: 0, line: 4 });
    const h = new Hasher("fixture-salt-0123456789");
    const r = toSnapshotSession(s, h, { orgId: "o", workspaceId: "w" })!;
    const text = JSON.stringify(r.line);
    for (const raw of ["raw-session-id", "raw-parent-id", "acme-api", "feat/x", "fixture-user"]) expect(text).not.toContain(raw);
    expect(r.session).toMatchObject({
      sessionId: h.session("claude", "raw-session-id"), parentSessionIds: [h.session("claude", "raw-parent-id")],
      projectId: h.project("acme-api"), repo: h.repo("acme-api"), branch: h.branch("feat/x"),
      createdAt: at("09:00"), lastActivityAt: at("09:07"), summaryTimeline: [], claims: [], title: null, userId: null,
    });
    expect(r.session.commits[0]!.sha).toBe("unknown");
    expect(r.mapped).toMatchObject({ sourceSessionId: "raw-session-id", sourceProject: "acme-api" });
    expect(toSnapshotSession(new SessionBuilder("claude", "empty", stats), h, { orgId: "o", workspaceId: "w" })).toBeNull();
  });

  test("facts past the M0 per-list bound are truncated and counted", () => {
    const stats = emptyStats();
    const s = new SessionBuilder("generic", "big", stats);
    for (let i = 0; i < IMPORT_BOUNDS.facts + 5; i++) s.tokens(1, at("09:00"), { file: 0, line: i });
    expect(s.usage).toHaveLength(IMPORT_BOUNDS.facts);
    expect(stats.truncatedFacts).toBe(5);
  });
});

describe("bounded reading", () => {
  test("a symlinked input file is refused at open, not followed", async () => {
    const dir = tmp("si-imp-sym-");
    const link = join(dir, "link.jsonl");
    symlinkSync(join(FIX, "generic", "events.jsonl"), link);
    await expect(readJsonl(link, emptyStats(), () => {})).rejects.toThrow("symlinks are refused");
    expect(await listInputFiles(dir)).toEqual([]);
  });

  test("the import-wide fact bound stops the import", () => {
    const stats = emptyStats();
    stats.heldFacts = IMPORT_BOUNDS.totalFacts;
    expect(() => new SessionBuilder("generic", "x", stats).tokens(1, at("09:00"), { file: 0, line: 1 })).toThrow("facts");
  });

  test("an oversize line is skipped without failing the file; malformed lines are counted", async () => {
    const dir = tmp("si-imp-big-");
    const file = join(dir, "big.jsonl");
    const big = JSON.stringify({ x: "y".repeat(IMPORT_BOUNDS.inputLineBytes + 10) });
    writeFileSync(file, `{"a":1}\n${big}\nnot json\n{"b":2}`);
    const stats = emptyStats();
    const got: [unknown, number][] = [];
    await readJsonl(file, stats, (v, line) => got.push([v, line]));
    expect(got).toEqual([[{ a: 1 }, 1], [{ b: 2 }, 4]]);
    expect(stats).toMatchObject({ oversizeLines: 1, malformedLines: 1, lines: 3 });
  });
});
