/** Trust boundaries of the relayfacts tools: output paths, credentials, corpus refusals, build evidence. */
import { afterAll, describe, expect, test } from "bun:test";
import { chmodSync, existsSync, mkdtempSync, rmSync, symlinkSync, writeFileSync, mkdirSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { SessionAnalysisSnapshot } from "../../session-analysis/engine/session-analysis-result.ts";
import { CORPUS_FORMAT, loadCorpus, sha256Hex } from "../bench/corpus.ts";
import { extractArtifacts } from "../backfill/artifacts.ts";
import { readPool } from "../backfill/source.ts";
import { deriveState } from "../features/build.ts";
import { extractSessionFacts } from "../features/extract.ts";
import { outsideDir, PrivateDir, REPO_ROOT, writeOutsideFile } from "../out-path.ts";
import { bindScope, readScope } from "../backfill/scope.ts";
import { errorCode, fileName, scopedSessions } from "../backfill/cli.ts";
import { buildFeatures, factsAsOf as factsAt, withCommitBranches } from "../features/build.ts";
import { closeSync, linkSync, lstatSync, openSync, readdirSync, readFileSync, statSync, writeSync } from "node:fs";
import { DEFAULT_LINK_PARAMS, linkSessions } from "../features/link.ts";
import { utc, type SessionFacts } from "../features/types.ts";

const tmp = mkdtempSync(join(tmpdir(), "relayfacts-bounds-"));
chmodSync(tmp, 0o700);
afterAll(() => rmSync(tmp, { recursive: true, force: true }));

describe("output paths", () => {
  test("a path inside the repository is refused and nothing is created", () => {
    const target = join(REPO_ROOT, "relayfacts-out-should-not-exist", "nested");
    expect(() => outsideDir(target)).toThrow(/outside this repository/);
    expect(existsSync(join(REPO_ROOT, "relayfacts-out-should-not-exist"))).toBe(false);
  });
  test("a symlink outside that points into the repository is refused", () => {
    const link = join(tmp, "into-repo");
    symlinkSync(REPO_ROOT, link);
    expect(() => outsideDir(join(link, "out"))).toThrow(/outside this repository/);
    expect(existsSync(join(REPO_ROOT, "out"))).toBe(false);
  });
  test("a path outside is created", () => {
    expect(existsSync(outsideDir(join(tmp, "a", "b")))).toBe(true);
  });
});

describe("credentials", () => {
  test("readPool takes the tenant and password out of the environment before connecting", async () => {
    const env: NodeJS.ProcessEnv = { RF_ORG_ID: "org-test", RF_WORKSPACE_ID: "ws-test", PGPASSWORD: "synthetic-secret" };
    const { cfg, pool } = readPool(env, 1);
    expect(cfg).toEqual({ orgId: "org-test", workspaceId: "ws-test" });
    expect(env.PGPASSWORD).toBeUndefined();
    expect(env.RF_ORG_ID).toBeUndefined();
    expect((pool as unknown as { options: { ssl: { rejectUnauthorized: boolean } } }).options.ssl.rejectUnauthorized).toBe(true);
    await pool.end();
  });
  test("a missing variable is refused", () => {
    expect(() => readPool({ RF_ORG_ID: "o", RF_WORKSPACE_ID: "w" }, 1)).toThrow(/PGPASSWORD is not set/);
    expect(() => readPool({ RF_WORKSPACE_ID: "w", PGPASSWORD: "p" }, 1)).toThrow(/RF_ORG_ID is not set/);
  });
});

describe("corpus refusals", () => {
  const write = (name: string, over: { deleteBy?: string; sessionsSha?: string; extra?: boolean; line?: string } = {}) => {
    const dir = join(tmp, name);
    mkdirSync(dir, { recursive: true });
    const sessions = `${over.line ?? ""}\n`;
    const forge = JSON.stringify({ prs: [], commits: [], flags: [] });
    writeFileSync(join(dir, "sessions.jsonl"), sessions, { mode: 0o600 });
    writeFileSync(join(dir, "forge.json"), forge, { mode: 0o600 });
    writeFileSync(join(dir, "manifest.json"), JSON.stringify({
      format: CORPUS_FORMAT, repo: "example/widgets", window: { since: "2026-10-01T00:00:00Z", until: "2026-11-01T00:00:00Z" },
      createdAt: "2026-11-01T00:00:00Z", deleteBy: over.deleteBy ?? "2026-11-20T00:00:00Z",
      files: { "sessions.jsonl": over.sessionsSha ?? sha256Hex(sessions), "forge.json": sha256Hex(forge) }, ...(over.extra ? { note: "x" } : {}),
    }), { mode: 0o600 });
    return dir;
  };
  const now = new Date("2026-11-05T00:00:00Z");
  test("a sound corpus loads", () => expect(loadCorpus(write("ok"), now).sessions).toEqual([]));
  test("past its delete-by date", () => expect(() => loadCorpus(write("old"), new Date("2026-11-21T00:00:00Z"))).toThrow(/delete-by/));
  test("a changed file", () => expect(() => loadCorpus(write("sha", { sessionsSha: "0".repeat(64) }), now)).toThrow(/sha256/));
  test("an unknown manifest field", () => expect(() => loadCorpus(write("extra", { extra: true }), now)).toThrow());
  test("a context field outside the schema", () => {
    const line = JSON.stringify({ source: "claude", sessionId: "s", context: { secretToken: "x" }, snapshot: { source: "claude", sessionId: "s" } });
    expect(() => loadCorpus(write("ctx", { line }), now)).toThrow();
  });
  test("too many parent ids", () => {
    const line = JSON.stringify({ source: "claude", sessionId: "s", context: { parentSessionIds: Array.from({ length: 17 }, (_, i) => `p${i}`) }, snapshot: { source: "claude", sessionId: "s" } });
    expect(() => loadCorpus(write("parents", { line }), now)).toThrow(/parentSessionIds/);
  });
});

describe("build evidence", () => {
  const ref = (id: string, kind: string, summary: string, detail: string | null = null) => ({ id, kind, at: "2026-11-10T10:00:00Z", summary, detail, source: "claude", sessionId: "s", truncated: false, isError: null, toolCallId: null, locator: { eventUid: null, toolUseId: null, fileEditId: null } });
  const snap = (refs: unknown[]) => ({ source: "claude", sessionId: "s", title: "t", analyzedAt: "2026-11-20T00:00:00.000Z", evidence: { references: refs }, metrics: { filesChanged: [] }, understanding: { intents: [], constraints: [], decisions: [], outcomes: [] } }) as unknown as SessionAnalysisSnapshot;
  test("a listed or viewed PR and a viewed commit URL are link keys, never built", () => {
    const f = extractSessionFacts(snap([
      ref("c1", "tool-call", "gh pr list -R example/widgets"),
      ref("r1", "tool-result", "gh pr list", "https://github.com/example/widgets/pull/7\nhttps://github.com/example/widgets/pull/8"),
      ref("r2", "tool-result", "read docs", "see https://github.com/example/widgets/commit/abc1234def"),
    ]));
    expect(f.built.filter((b) => b.kind === "pr" || b.kind === "commit")).toEqual([]);
    expect(f.keys.prs.length).toBe(2);
    expect(f.keys.commits).toEqual(["abc1234def"]);
  });
  test("only the successful result paired with the session's own gh pr create is built", () => {
    // Shaped like session-evidence.ts: the call reference's id is the call id; its own toolCallId is null.
    const call = { ...ref("k1", "tool-call", "Bash", JSON.stringify({ command: "cd repo && gh pr create --fill" })), toolCallId: null };
    const paired = { ...ref("r1", "tool-result", "ok", "https://github.com/example/widgets/pull/9"), toolCallId: "k1", isError: false };
    expect(extractSessionFacts(snap([call, paired])).built.filter((b) => b.kind === "pr").map((b) => b.ref)).toEqual(["https://github.com/example/widgets/pull/9"]);
    // Adjacent but another call's result, a failed result, or a result whose summary only names the command: not built.
    const otherCall = { ...ref("k2", "tool-call", "Bash", JSON.stringify({ command: "gh pr list" })), toolCallId: null };
    // Calls that only mention the command: quoted, in a comment, in a non-shell tool, or truncated.
    const echo = { ...call, detail: JSON.stringify({ command: 'echo "gh pr create" && gh pr list' }) };
    const comment = { ...call, detail: JSON.stringify({ command: "gh pr list # then gh pr create" }) };
    const notShell = { ...call, summary: "Read" };
    const truncated = { ...call, truncated: true };
    for (const [c, r] of [[call, { ...paired, toolCallId: "k2" }], [call, { ...paired, isError: true }], [call, { ...paired, toolCallId: null, summary: "gh pr create" }], [{ ...call, isError: true }, paired], [echo, paired], [comment, paired], [notShell, paired], [truncated, paired]] as const) {
      const f = extractSessionFacts(snap([c, otherCall, r]));
      expect(f.built.filter((b) => b.kind === "pr")).toEqual([]);
      expect(f.keys.prs).toEqual(["https://github.com/example/widgets/pull/9"]);
    }
  });
  test("transcript artifacts: a created PR versus a listed one; tag pushes are not branches", () => {
    const ev = (id: string, type: string, content: string, payload: Record<string, unknown> = {}, toolCalls?: unknown[]) => ({ table: "convergence_events", row: { event_id: id, ts: "2026-11-10T10:00:00Z", type, content, record: { payload }, ...(toolCalls ? { tool_calls: toolCalls } : {}) } });
    const call = (id: string, command: string, name = "Bash") => ev(`c-${id}`, "tool_call", command, { tool_use_id: id }, [{ name, args: { command }, toolUseId: id, isError: false }]);
    const art = extractArtifacts([
      call("u1", "gh pr list"), ev("2", "tool_result", "https://github.com/example/widgets/pull/1", { tool_use_id: "u1" }),
      call("u3", "gh pr create --fill"), ev("4", "tool_result", "https://github.com/example/widgets/pull/2", { tool_use_id: "u3", result_status: "success" }),
      // adjacency to a create call proves nothing: another call's result, and a failed paired result
      call("u5", "gh pr create --fill"), ev("6", "tool_result", "https://github.com/example/widgets/pull/3", { tool_use_id: "zz" }),
      call("u7", "gh pr create --fill"), ev("8", "tool_result", "https://github.com/example/widgets/pull/4", { tool_use_id: "u7", result_status: "error" }),
      // a call that only quotes the command, and a non-shell tool, paired with a PR URL result
      call("u9", 'echo "gh pr create"'), ev("10", "tool_result", "https://github.com/example/widgets/pull/5", { tool_use_id: "u9" }),
      call("u11", "gh pr create --fill", "WebFetch"), ev("12", "tool_result", "https://github.com/example/widgets/pull/6", { tool_use_id: "u11" }),
      ev("13", "tool_result", "To https://github.com/example/widgets.git\n * [new tag]         v1.2.3 -> v1.2.3\n"),
    ], "example/widgets");
    expect(art.prs.map((p) => [p.url.split("/").pop(), p.via])).toEqual([["1", "mention"], ["2", "created"], ["3", "mention"], ["4", "mention"], ["5", "mention"], ["6", "mention"]]);
    expect(art.branches).toEqual([]);
    expect(art.pushes).toEqual([]);
  });
});

describe("branch landing", () => {
  test("a branch whose tip is long before the feature's first session is not its landing", () => {
    const s: SessionFacts = { source: "claude", sessionId: "s", analyzedAt: "2026-10-05T00:00:00.000Z", startedAt: "2026-10-05T00:00:00.000Z", endedAt: "2026-10-05T01:00:00.000Z", title: "t", asked: [], built: [], gaps: [], rationale: [], outcomeStatuses: [], dropped: {}, keys: { repo: "example/widgets", branches: ["feat/empty"], prs: [], commits: [], files: [], taskRefs: [], parents: [] } };
    const forge = { prs: [], commits: [], flags: [], branches: [{ name: "feat/empty", landed: true, landedAt: "2026-09-01T00:00:00.000Z", tipAt: "2026-09-01T00:00:00.000Z" }] };
    expect(deriveState([s], forge, "2026-10-06T00:00:00.000Z").state).not.toBe("shipped");
    const later = { ...forge, branches: [{ ...forge.branches[0]!, landedAt: "2026-10-05T00:30:00.000Z", tipAt: "2026-10-05T00:30:00.000Z" }] };
    expect(deriveState([s], later, "2026-10-06T00:00:00.000Z").state).toBe("shipped");
  });
});

describe("flag defaults from docs", () => {
  test("tables win, prose names a single setting, value settings are not gates", async () => {
    const { flagDefaults } = await import("../backfill/forge.ts");
    const md = [
      "| `ALPHA_ENABLED` | Default on: off with `false` |",
      "Ingest commits through the log behind `LOG_COMMIT`, default off.",
      "The `ALPHA_ENABLED` switch defaults to off in old builds.",
      "`WAIT_MS` defaults to off when unset.",
      "Both `ONE_FLAG` and `TWO_FLAG` are off by default.",
    ].join("\n");
    const m = flagDefaults(md);
    expect(m.get("ALPHA_ENABLED")).toBe(true);
    expect(m.get("LOG_COMMIT")).toBe(false);
    expect(m.has("WAIT_MS")).toBe(false);
    expect(m.has("ONE_FLAG")).toBe(false);
  });
});

describe("private output", () => {
  test("directories are 0700, files 0600, names opaque, links refused", () => {
    const dir = PrivateDir.open(join(tmp, "priv"));
    expect(statSync(dir.path).mode & 0o777).toBe(0o700);
    dir.write("a.json", "{}");
    expect(statSync(join(dir.path, "a.json")).mode & 0o777).toBe(0o600);
    const child = dir.child("sessions");
    expect(statSync(child.path).mode & 0o777).toBe(0o700);
    // An existing child that is a link is never followed, for a file or a directory.
    symlinkSync(join(tmp, "elsewhere.json"), join(dir.path, "link.json"));
    expect(() => dir.write("link.json", "x")).toThrow();
    expect(existsSync(join(tmp, "elsewhere.json"))).toBe(false);
    symlinkSync(tmp, join(dir.path, "linkdir"));
    expect(() => dir.child("linkdir")).toThrow();
    for (const bad of ["../x", "a/b", ".hidden", "", "x".repeat(200)]) expect(() => dir.write(bad, "x")).toThrow(/refused output name/);
    expect(fileName({ harness: "claude", sessionId: "s-1" })).toMatch(/^[0-9a-f]{32}\.json$/u);
  });
  test("an existing directory is never re-permissioned: one open to others is refused", () => {
    const shared = join(tmp, "shared");
    mkdirSync(shared);
    chmodSync(shared, 0o755);
    expect(() => PrivateDir.open(shared)).toThrow(/open to group or others/);
    expect(() => writeOutsideFile(join(shared, "forge.json"), "{}")).toThrow(/open to group or others/);
    expect(statSync(shared).mode & 0o777).toBe(0o755);
    expect(existsSync(join(shared, "forge.json"))).toBe(false);
  });
  test("a file output inside the repository is refused, creating nothing", () => {
    expect(() => writeOutsideFile(join(REPO_ROOT, "forge-should-not-exist.json"), "{}")).toThrow(/outside this repository/);
    expect(existsSync(join(REPO_ROOT, "forge-should-not-exist.json"))).toBe(false);
  });
});

describe("collection scope", () => {
  const now = new Date("2026-11-26T00:00:00Z");
  const want = { orgId: "org-a", workspaceId: "ws-a", repos: ["example/widgets"], since: "2026-10-01T00:00:00.000Z", until: "2026-11-01T00:00:00.000Z" };
  test("bound before collection; later commands must match", () => {
    const dir = PrivateDir.open(join(tmp, "scope-ok"));
    expect(() => bindScope(dir, want, now)).toThrow(/--delete-by is required/);
    bindScope(dir, { ...want, deleteBy: "2026-12-10T00:00:00.000Z" }, now);
    expect(statSync(join(dir.path, "scope.json")).mode & 0o777).toBe(0o600);
    expect(bindScope(dir, want, now).deleteBy).toBe("2026-12-10T00:00:00.000Z");
    expect(() => bindScope(dir, { ...want, workspaceId: "ws-b" }, now)).toThrow(/another scope/);
    expect(() => bindScope(dir, { ...want, until: "2026-11-02T00:00:00.000Z" }, now)).toThrow(/another scope/);
    expect(() => readScope(dir, new Date("2026-12-11T00:00:00Z"))).toThrow(/delete-by/);
  });
  test("retention is at most 30 days", () => {
    expect(() => bindScope(PrivateDir.open(join(tmp, "scope-long")), { ...want, deleteBy: "2027-02-01T00:00:00.000Z" }, now)).toThrow(/within 30 days/);
  });
  test("a --sessions list is cut to the scope and the directory, whatever the file claims", () => {
    const scope = { format: "relayfacts-scope/1" as const, ...want, deleteBy: "2026-12-10T00:00:00.000Z" };
    const directory = new Map([["claude\u0000s", "2026-10-10T00:00:00.000Z"], ["claude\u0000t", "2026-10-11T00:00:00.000Z"]]);
    const row = { harness: "claude", sessionId: "s", repo: "example/widgets", status: "ok", lastActivityAt: "1999-01-01T00:00:00Z" };
    const { rows, excluded } = scopedSessions([row, row, { ...row, sessionId: "t", repo: "example/other" }, { ...row, sessionId: "u" }, { ...row, harness: "slack" }, { ...row, sessionId: "t", status: "refused" }, { ...row, sessionId: "t", repo: undefined }], scope, directory);
    expect(rows.map((r) => [r.sessionId, r.lastActivityAt])).toEqual([["s", "2026-10-10T00:00:00.000Z"]]);
    expect(excluded).toEqual({ duplicate: 1, other_repo: 2, not_in_directory_window: 1, not_coding: 1, not_located: 1 });
  });
  test("log lines carry fixed codes, never messages", () => {
    expect(errorCode(Object.assign(new Error("password=hunter2 at host"), { code: "28P01" }))).toBe("pg_28");
    expect(errorCode(new TypeError("leaked text"))).toBe("TypeError");
    expect(errorCode({ code: "has spaces and secrets" })).toBe("error");
    // A plausible-looking but unknown value (a private name, a token) is never echoed: the set is closed.
    expect(errorCode({ reason: "acme_internal_tenant_7" })).toBe("error");
    expect(errorCode({ code: "ENOENT" })).toBe("ENOENT");
    expect(errorCode({ reason: "canonical_projection_incomplete" })).toBe("canonical_projection_incomplete");
  });
});

describe("strict as-of", () => {
  test("a replay at T is invariant to everything observed after T", () => {
    const T = "2026-10-05T12:00:00.000Z";
    const ev = (at: string | null) => ({ source: "claude", sessionId: "s", refId: "r", kind: "tool-result", at });
    const base: SessionFacts = { source: "claude", sessionId: "s", analyzedAt: "2026-10-06T00:00:00.000Z", startedAt: "2026-10-05T10:00:00.000Z", endedAt: "2026-10-05T11:00:00.000Z", title: "t",
      asked: [{ id: "a", text: "add a retry", quotes: [], at: "2026-10-05T10:00:00.000Z", session: { source: "claude", sessionId: "s" }, outcomeId: null, evidence: [ev("2026-10-05T10:00:00.000Z")] }],
      built: [{ id: "b", kind: "commit", what: "commit", ref: "aaaaaaa", at: "2026-10-05T10:30:00.000Z", session: { source: "claude", sessionId: "s" }, outcomeId: null, evidence: [ev("2026-10-05T10:30:00.000Z")] }],
      gaps: [], rationale: [], outcomeStatuses: ["implemented"], dropped: {},
      keys: { repo: "example/widgets", branches: ["feat/x"], prs: [], commits: ["aaaaaaa"], files: [], taskRefs: [], parents: [] } };
    // The same session observed later: it ran on past T, with later evidence, mentions, statuses and keys.
    const later: SessionFacts = { ...base, title: "A title written after the session ended", endedAt: "2026-10-05T15:00:00.000Z", outcomeStatuses: ["superseded"],
      asked: [{ ...base.asked[0]!, evidence: [...base.asked[0]!.evidence, ev("2026-10-05T14:00:00.000Z")], quotes: [{ text: "later words", at: "2026-10-05T14:00:00.000Z", evidence: ev("2026-10-05T14:00:00.000Z") }] }],
      built: [...base.built, { ...base.built[0]!, id: "c", ref: "bbbbbbb", at: "2026-10-05T14:00:00.000Z", evidence: [ev("2026-10-05T14:00:00.000Z")] }],
      keys: { ...base.keys, branches: ["feat/x", "feat/y"], commits: ["aaaaaaa", "bbbbbbb", "ccccccc"], prs: ["https://github.com/example/widgets/pull/1"], files: ["src/a.ts"] } };
    const other: SessionFacts = { ...base, sessionId: "o", startedAt: "2026-10-05T13:00:00.000Z", endedAt: "2026-10-05T13:30:00.000Z", keys: { ...base.keys, commits: ["ccccccc"] } };
    // What a snapshot taken at T would hold: the session still running (its last reference at T).
    const atT: SessionFacts = { ...base, endedAt: T };
    const replay = JSON.stringify(buildFeatures([later, other], { asOf: T }));
    expect(replay).toBe(JSON.stringify(buildFeatures([atT], { asOf: T })));
    const [f] = JSON.parse(replay);
    expect(f.built.map((b: { ref: string }) => b.ref)).toEqual(["aaaaaaa"]);
    expect(f.asked[0].quotes).toEqual([]);
    expect(f.state).toBe("in_progress");
    expect(replay).not.toContain("written after");
    // An item's untimed pointer survives with the item (grounding needs it mid-session).
    const untimed: SessionFacts = { ...base, built: [{ ...base.built[0]!, evidence: [ev(null)] }] };
    expect(buildFeatures([untimed], { asOf: T })[0]!.built[0]!.evidence).toHaveLength(1);
  });
});

describe("round-2 controls", () => {
  const now = new Date("2026-11-26T00:00:00Z");
  const want = { orgId: "org-a", workspaceId: "ws-a", repos: ["example/widgets", "example/gadgets"], since: "2026-10-01T00:00:00.000Z", until: "2026-11-01T00:00:00.000Z" };
  const deleteBy = new Date(Date.now() + 10 * 86_400_000).toISOString();
  test("a first bind needs an empty directory; a concurrent incompatible initialisation fails", () => {
    const dir = PrivateDir.open(join(tmp, "r2-populated"));
    dir.child("sessions").write("0".repeat(32) + ".json", "{}");
    expect(() => bindScope(dir, { ...want, deleteBy }, now)).toThrow(/empty --out/);
    const fresh = PrivateDir.open(join(tmp, "r2-race"));
    fresh.create("scope.json", JSON.stringify({ format: "relayfacts-scope/1", ...want, orgId: "org-b", deleteBy }));
    expect(() => bindScope(fresh, { ...want, deleteBy }, now)).toThrow(/another scope/);
    expect(() => fresh.create("scope.json", "{}")).toThrow();
  });
  test("seal admits only rows stamped with this scope, and only the exact repository set", async () => {
    const { main } = await import("../backfill/cli.ts");
    const dir = PrivateDir.open(join(tmp, "r2-seal"));
    const scope = bindScope(dir, { ...want, deleteBy });
    const forgePath = join(tmp, "r2-forge.json");
    writeFileSync(forgePath, JSON.stringify({ prs: [], commits: [], flags: [] }), { mode: 0o600 });
    const sessions = dir.child("sessions");
    const row = (scopeIdValue: string) => JSON.stringify({ scope: scopeIdValue, source: "claude", sessionId: "s1", context: {}, snapshot: { source: "claude", sessionId: "s1" } });
    const args = (repos: string) => ["seal", "--out", dir.path, "--repo", repos, "--since", want.since, "--until", want.until, "--forge", forgePath];
    sessions.write(fileName({ harness: "claude", sessionId: "s1" }), row("stale-scope"));
    await expect(main(args("example/widgets,example/gadgets"))).rejects.toThrow(/not collected under this scope/);
    sessions.write(fileName({ harness: "claude", sessionId: "s1" }), row(scope.id));
    await expect(main(args("example/widgets"))).rejects.toThrow(/exactly/);
    // A row whose name is not its own key's is refused too.
    sessions.write("f".repeat(32) + ".json", row(scope.id));
    await expect(main(args("example/widgets,example/gadgets"))).rejects.toThrow(/not collected under this scope/);
  });
  test("located rows from another collection, or with no scope stamp, are excluded", () => {
    const scope = { format: "relayfacts-scope/1" as const, ...want, deleteBy, id: "scope-1" };
    const directory = new Map([["claude\u0000s", "2026-10-10T00:00:00.000Z"]]);
    const row = { harness: "claude", sessionId: "s", repo: "example/widgets", status: "ok" };
    expect(scopedSessions([{ ...row, scope: "scope-1" }], scope, directory).rows).toHaveLength(1);
    expect(scopedSessions([row, { ...row, scope: "scope-2" }], scope, directory).excluded).toEqual({ other_collection: 2 });
  });
  test("private reads check the opened file; writes replace atomically", () => {
    const dir = PrivateDir.open(join(tmp, "r2-io"));
    writeFileSync(join(dir.path, "open.json"), "{}", { mode: 0o644 });
    expect(() => dir.read("open.json")).toThrow(/open to group or others/);
    dir.write("a.json", "one");
    linkSync(join(dir.path, "a.json"), join(dir.path, "alias.json"));
    expect(() => dir.read("a.json")).toThrow(/hard links/);
    // Replacing a file never opens it: the old inode (and its other name) keeps the old content.
    dir.write("a.json", "two");
    expect(dir.read("a.json")).toBe("two");
    expect(readFileSync(join(dir.path, "alias.json"), "utf8")).toBe("one");
    expect(statSync(join(dir.path, "a.json")).mode & 0o777).toBe(0o600);
    expect(readdirSync(dir.path).filter((n) => n.startsWith(".tmp-"))).toEqual([]);
    expect(lstatSync(join(dir.path, "a.json")).nlink).toBe(1);
  });
  test("refusal messages never echo a file's own values", () => {
    const line = JSON.stringify({ source: "claude", sessionId: "s", context: { "private-field-name": "x" }, snapshot: { source: "claude", sessionId: "s" } });
    const dir = join(tmp, "r2-msg");
    mkdirSync(dir, { recursive: true, mode: 0o700 });
    const sessions = `${line}\n`, forge = JSON.stringify({ prs: [], commits: [], flags: [] });
    writeFileSync(join(dir, "sessions.jsonl"), sessions, { mode: 0o600 });
    writeFileSync(join(dir, "forge.json"), forge, { mode: 0o600 });
    writeFileSync(join(dir, "manifest.json"), JSON.stringify({ format: CORPUS_FORMAT, repo: "example/widgets", window: { since: "2026-10-01T00:00:00Z", until: "2026-11-01T00:00:00Z" }, createdAt: "2026-11-01T00:00:00Z", deleteBy: "2026-11-20T00:00:00Z", files: { "sessions.jsonl": sha256Hex(sessions), "forge.json": sha256Hex(forge) } }), { mode: 0o600 });
    let message = "";
    try { loadCorpus(dir, new Date("2026-11-05T00:00:00Z")); } catch (error) { message = (error as Error).message; }
    expect(message).toMatch(/unknown field/);
    expect(message).not.toContain("private-field-name");
  });
  test("a branch the mirror shows only after the cut never links an earlier replay", () => {
    const sha = "a".repeat(40);
    const later = { prs: [], flags: [], commits: [], branches: [{ name: "feat/later", landed: true, landedAt: "2099-01-01T00:00:00.000Z", tipAt: "2099-01-01T00:00:00.000Z", commits: [sha] }] };
    const base = { source: "claude", analyzedAt: "2026-10-01T00:00:00.000Z", title: "t", asked: [], built: [], gaps: [], rationale: [], outcomeStatuses: [], dropped: {} };
    const s1: SessionFacts = { ...base, sessionId: "s1", startedAt: "2026-10-01T10:00:00.000Z", endedAt: "2026-10-01T11:00:00.000Z", keys: { repo: "example/widgets", branches: ["feat/later"], prs: [], commits: [], files: [], taskRefs: [], parents: [] } };
    const s2: SessionFacts = { ...base, sessionId: "s2", startedAt: "2026-10-01T12:00:00.000Z", endedAt: "2026-10-01T13:00:00.000Z", keys: { repo: "example/widgets", branches: [], prs: [], commits: ["aaaaaaa"], files: [], taskRefs: [], parents: [] } };
    const built = buildFeatures([s1, s2], { asOf: "2026-11-01T00:00:00.000Z", forge: later, link: { ...DEFAULT_LINK_PARAMS, intent: false } });
    expect(built.length).toBe(2);
    expect(built.every((f) => f.state !== "shipped")).toBe(true);
  });
});

describe("round-2 re-review controls", () => {
  test("PR creation abstains on anything that could print another URL or hide what runs", async () => {
    const { runsPrCreate, shellCommand } = await import("../features/commands.ts");
    for (const ok of ["gh pr create --fill", "cd repo && gh pr create --title \"fix (x): y\" --body 'a; b'", "git push -u origin feat/x && gh pr create -f", "GH_TOKEN=x gh pr create -f"])
      expect(runsPrCreate(ok)).toBe(true);
    for (const bad of ["cat <<'EOF'\ngh pr create --fill\nhttps://github.com/o/r/pull/5\nEOF", "gh pr create --fill || gh pr view --json url", "echo \"gh pr create\"",
      "gh pr list # gh pr create", "gh pr create --body \"$(cat b.md)\"", "gh pr create -f | tee x", "git pull && gh pr create -f", "gh pr create -f &", "gh pr create; gh pr view", "gh pr create --title \"open"])
      expect(runsPrCreate(bad)).toBe(false);
    expect(shellCommand("shell", { command: ["bash", "-lc", "gh pr create --fill"] })).toBe("gh pr create --fill");
    expect(shellCommand("exec_command", { cmd: "gh pr create --fill" })).toBe("gh pr create --fill");
    expect(shellCommand("Read", { command: "gh pr create" })).toBeNull();
  });
  test("an interrupted append loses only the torn line on resume", () => {
    const dir = PrivateDir.open(join(tmp, "r3-append"));
    dir.append("log.jsonl", `${JSON.stringify({ n: 1 })}\n`);
    // A torn write: half a line, no newline (an interrupted earlier run).
    const fd = openSync(join(dir.path, "log.jsonl"), "a");
    writeSync(fd, "{\"n\":2,\"half");
    closeSync(fd);
    dir.append("log.jsonl", `${JSON.stringify({ n: 3 })}\n`);
    const rows = dir.read("log.jsonl").split("\n").filter(Boolean).flatMap((l) => { try { return [JSON.parse(l)]; } catch { return []; } });
    expect(rows).toEqual([{ n: 1 }, { n: 3 }]);
  });
  test("a write that fails partway keeps the old file and leaves no temporary", () => {
    const dir = PrivateDir.open(join(tmp, "r3-write"));
    dir.write("a.json", "old");
    expect(() => dir.write("a.json", Symbol("not data") as unknown as string)).toThrow();
    expect(dir.read("a.json")).toBe("old");
    expect(readdirSync(dir.path).filter((n: string) => n.startsWith(".tmp-"))).toEqual([]);
  });
  test("create is exclusive and atomic: a racing initialisation fails cleanly", () => {
    const dir = PrivateDir.open(join(tmp, "r3-race"));
    const scope = { orgId: "org-a", workspaceId: "ws-a", repos: ["example/widgets"], since: "2026-10-01T00:00:00.000Z", until: "2026-11-01T00:00:00.000Z", deleteBy: new Date(Date.now() + 86_400_000).toISOString() };
    // Another process creates scope.json between this bind's emptiness check and its create.
    const create = dir.create.bind(dir);
    (dir as unknown as { create: typeof create }).create = (name, data) => { create(name, JSON.stringify({ format: "relayfacts-scope/1", ...scope, orgId: "org-b" })); create(name, data); };
    expect(() => bindScope(dir, scope)).toThrow(/another collection initialised/);
    expect(JSON.parse(dir.read("scope.json")).orgId).toBe("org-b");
  });
  test("resume never skips an unsafe planted file: has() refuses it", () => {
    const dir = PrivateDir.open(join(tmp, "r3-has"));
    writeFileSync(join(dir.path, "x.json"), "{}", { mode: 0o644 });
    expect(() => dir.has("x.json")).toThrow(/open to group or others/);
  });
  test("seal refuses an artifacts row from another scope", async () => {
    const { main } = await import("../backfill/cli.ts");
    const dir = PrivateDir.open(join(tmp, "r3-seal"));
    const want = { orgId: "org-a", workspaceId: "ws-a", repos: ["example/widgets"], since: "2026-10-01T00:00:00.000Z", until: "2026-11-01T00:00:00.000Z" };
    const scope = bindScope(dir, { ...want, deleteBy: new Date(Date.now() + 86_400_000).toISOString() });
    const name = fileName({ harness: "claude", sessionId: "s1" });
    dir.child("sessions").write(name, JSON.stringify({ scope: scope.id, source: "claude", sessionId: "s1", context: {}, snapshot: { source: "claude", sessionId: "s1" } }));
    dir.child("artifacts").write(name, JSON.stringify({ scope: "another", harness: "claude", sessionId: "s1", artifacts: {} }));
    const forgePath = join(tmp, "r3-forge.json");
    writeFileSync(forgePath, JSON.stringify({ prs: [], commits: [], flags: [] }), { mode: 0o600 });
    await expect(main(["seal", "--out", dir.path, "--repo", "example/widgets", "--since", want.since, "--until", want.until, "--forge", forgePath])).rejects.toThrow(/artifacts row was not collected under this scope for this session/);
  });
  test("forge: when each commit reached the default branch, and how that is known", async () => {
    const { execFileSync } = await import("node:child_process");
    const { forgeFacts } = await import("../backfill/forge.ts");
    const repo = join(tmp, "r3-git");
    mkdirSync(repo);
    const git = (args: string[], date: string) => execFileSync("git", ["-C", repo, ...args], { env: { ...process.env, GIT_AUTHOR_DATE: date, GIT_COMMITTER_DATE: date, GIT_AUTHOR_NAME: "t", GIT_AUTHOR_EMAIL: "t@example.invalid", GIT_COMMITTER_NAME: "t", GIT_COMMITTER_EMAIL: "t@example.invalid" }, encoding: "utf8" }).trim();
    const commit = (file: string, date: string) => { writeFileSync(join(repo, file), file); git(["add", file], date); git(["commit", "-q", "-m", file], date); return git(["rev-parse", "HEAD"], date); };
    git(["init", "-q", "-b", "main"], "2026-10-01T00:00:00Z");
    commit("base.txt", "2026-10-01T00:00:00Z");
    git(["checkout", "-q", "-b", "feat/merged"], "2026-10-02T00:00:00Z");
    const merged = commit("merged.txt", "2026-10-02T00:00:00Z");
    git(["checkout", "-q", "main"], "2026-10-03T00:00:00Z");
    git(["merge", "-q", "--no-ff", "-m", "merge", "feat/merged"], "2026-10-05T00:00:00Z");
    git(["checkout", "-q", "-b", "feat/recut"], "2026-10-06T00:00:00Z");
    const recut = commit("recut.txt", "2026-10-06T00:00:00Z");
    git(["checkout", "-q", "main"], "2026-10-07T00:00:00Z");
    git(["cherry-pick", recut], "2026-10-08T00:00:00Z");
    const direct = commit("direct.txt", "2026-10-09T00:00:00Z");
    const facts = forgeFacts("example/widgets", repo, "2026-09-30T00:00:00Z", "2026-10-31T00:00:00Z", "main", () => []);
    const by = (sha: string) => facts.commits.find((c) => c.sha === sha)!;
    expect([by(merged).defaultAt, by(merged).defaultAtBasis]).toEqual(["2026-10-05T00:00:00.000Z", "merge"]);
    expect([by(recut).onDefaultBranch, by(recut).defaultAtBasis]).toEqual([true, "patch"]);
    expect(Date.parse(by(recut).defaultAt!)).toBe(Date.parse("2026-10-08T00:00:00Z"));
    expect([by(direct).defaultAt, by(direct).defaultAtBasis]).toEqual(["2026-10-09T00:00:00.000Z", "committer"]);
    // As of before the merge, the merged commit is not on the default branch yet (not a base for a session then).
    const { withoutBaseCommits } = await import("../features/build.ts");
    const s = { source: "claude", sessionId: "s", analyzedAt: "2026-10-04T00:00:00.000Z", startedAt: "2026-10-04T00:00:00.000Z", endedAt: "2026-10-04T01:00:00.000Z", title: "", asked: [], built: [], gaps: [], rationale: [], outcomeStatuses: [], dropped: {},
      keys: { repo: "example/widgets", branches: [], prs: [], commits: [merged.slice(0, 7)], files: [], taskRefs: [], parents: [] } } as SessionFacts;
    expect(withoutBaseCommits(s, facts).keys.commits).toEqual([merged.slice(0, 7)]);
    expect(withoutBaseCommits({ ...s, startedAt: "2026-10-06T00:00:00.000Z" }, facts).keys.commits).toEqual([]);
  });
});

describe("round-3 controls", () => {
  test("default selection needs the exact owner/name remote; path-only rows abstain", async () => {
    const { rowInRepo } = await import("../backfill/cli.ts");
    expect(rowInRepo({ gitRepo: "https://github.com/example/widgets.git", projectId: null }, ["example/widgets"])).toBe(true);
    expect(rowInRepo({ gitRepo: "git@github.com:other/widgets.git", projectId: null }, ["example/widgets"])).toBe(false);
    expect(rowInRepo({ gitRepo: null, projectId: "/work/widgets" }, ["example/widgets"])).toBe(false);
  });
  test("a PR viewed on day 1 and created on day 3 is created from the day-3 result", () => {
    const ev = (id: string, ts: string, type: string, content: string, payload: Record<string, unknown> = {}, toolCalls?: unknown[]) => ({ table: "convergence_events", row: { event_id: id, ts, type, content, record: { payload }, ...(toolCalls ? { tool_calls: toolCalls } : {}) } });
    const art = extractArtifacts([
      ev("v1", "2026-10-01T00:00:00Z", "tool_call", "gh pr view 9", { tool_use_id: "a" }, [{ name: "Bash", args: { command: "gh pr view 9" }, toolUseId: "a" }]),
      ev("v2", "2026-10-01T00:00:01Z", "tool_result", "https://github.com/example/widgets/pull/9", { tool_use_id: "a" }),
      ev("c1", "2026-10-03T00:00:00Z", "tool_call", "gh pr create -f", { tool_use_id: "b" }, [{ name: "Bash", args: { command: "gh pr create -f" }, toolUseId: "b" }]),
      ev("c2", "2026-10-03T00:00:01Z", "tool_result", "https://github.com/example/widgets/pull/9", { tool_use_id: "b", result_status: "success" }),
    ], "example/widgets");
    expect(art.prs).toEqual([{ eventId: "c2", at: "2026-10-03T00:00:01.000Z", url: "https://github.com/example/widgets/pull/9", via: "created" }]);
    // As of day 2 the session's facts carry no built PR.
    const snap = { source: "claude", sessionId: "s", title: "t", analyzedAt: "2026-10-04T00:00:00.000Z", evidence: { references: [{ id: "h", kind: "human-message", at: "2026-10-01T00:00:00Z", summary: "x", detail: null, source: "claude", sessionId: "s", truncated: false, isError: null, toolCallId: null, locator: { eventUid: null, toolUseId: null, fileEditId: null } }, { id: "z", kind: "assistant-message", at: "2026-10-03T12:00:00Z", summary: "done", detail: null, source: "claude", sessionId: "s", truncated: false, isError: null, toolCallId: null, locator: { eventUid: null, toolUseId: null, fileEditId: null } }] }, metrics: { filesChanged: [] }, understanding: { intents: [], constraints: [], decisions: [], outcomes: [] } } as unknown as SessionAnalysisSnapshot;
    const f = extractSessionFacts(snap, { repo: "https://github.com/example/widgets" }, art);
    expect(f.built.filter((b) => b.kind === "pr").map((b) => b.at)).toEqual(["2026-10-03T00:00:01.000Z"]);
    expect(factsAt(f, "2026-10-02T00:00:00Z")!.built.filter((b) => b.kind === "pr")).toEqual([]);
  });
  test("seal refuses artifacts swapped between two sessions of the same collection", async () => {
    const { main } = await import("../backfill/cli.ts");
    const dir = PrivateDir.open(join(tmp, "r4-swap"));
    const want = { orgId: "org-a", workspaceId: "ws-a", repos: ["example/widgets"], since: "2026-10-01T00:00:00.000Z", until: "2026-11-01T00:00:00.000Z" };
    const scope = bindScope(dir, { ...want, deleteBy: new Date(Date.now() + 86_400_000).toISOString() });
    const ses = dir.child("sessions"), art = dir.child("artifacts");
    for (const id of ["a", "b"]) ses.write(fileName({ harness: "claude", sessionId: id }), JSON.stringify({ scope: scope.id, source: "claude", sessionId: id, context: {}, snapshot: { source: "claude", sessionId: id } }));
    art.write(fileName({ harness: "claude", sessionId: "a" }), JSON.stringify({ scope: scope.id, harness: "claude", sessionId: "b", artifacts: {} }));
    const forgePath = join(tmp, "r4-forge.json");
    writeFileSync(forgePath, JSON.stringify({ prs: [], commits: [], flags: [] }), { mode: 0o600 });
    await expect(main(["seal", "--out", dir.path, "--repo", "example/widgets", "--since", want.since, "--until", want.until, "--forge", forgePath])).rejects.toThrow(/for this session/);
  });
  test("times compare as instants: offsets and Z agree, and a tip exactly at the cut is excluded", () => {
    const sha = "a".repeat(40);
    const forge = { prs: [], flags: [], commits: [], branches: [
      { name: "feat/at-cut", landed: false, landedAt: null, tipAt: "2026-10-31T19:00:00-05:00", commits: [sha] },
    ] };
    const s = { source: "claude", sessionId: "s", analyzedAt: "2026-10-01T00:00:00.000Z", startedAt: "2026-10-01T00:00:00.000Z", endedAt: "2026-10-01T01:00:00.000Z", title: "", asked: [], built: [], gaps: [], rationale: [], outcomeStatuses: [], dropped: {}, keys: { repo: "example/widgets", branches: [], prs: [], commits: ["aaaaaaa"], files: [], taskRefs: [], parents: [] } } as SessionFacts;
    // 2026-10-31T19:00:00-05:00 is exactly 2026-11-01T00:00:00Z: not before the cut.
    expect(withCommitBranches(s, forge, "2026-11-01T00:00:00Z").keys.branches).toEqual([]);
    expect(withCommitBranches(s, forge, "2026-11-01T00:00:00.001Z").keys.branches).toEqual(["feat/at-cut"]);
    expect(withCommitBranches(s, { ...forge, branches: [{ ...forge.branches[0]!, tipAt: "2026-11-01T00:00:00+00:00" }] }, "2026-11-01T00:00:00Z").keys.branches).toEqual([]);
  });
  test("intent never links sessions with no known repository, or across repositories", () => {
    const one = (id: string, repo: string | null) => ({ source: "claude", sessionId: id, analyzedAt: "2026-10-01T00:00:00.000Z", startedAt: "2026-10-01T00:00:00.000Z", endedAt: "2026-10-01T01:00:00.000Z", title: "inflight lease release on worker crash", asked: [], built: [], gaps: [], rationale: [], outcomeStatuses: [], dropped: {}, keys: { repo, branches: [], prs: [], commits: [], files: [], taskRefs: [], parents: [] } }) as SessionFacts;
    // Unrelated work in the same corpus gives the shared words their weight.
    const others = ["dashboard palette legend colours", "billing invoice export csv", "search index shard rebalance"].map((t, i) => ({ ...one(`o${i}`, "example/widgets"), title: t }));
    expect(linkSessions([...others, one("a", null), one("b", null)]).groups).toHaveLength(5);
    expect(linkSessions([...others, one("a", null), one("b", "example/widgets")]).groups).toHaveLength(5);
    expect(linkSessions([...others, one("a", "example/widgets"), one("b", "example/other")]).groups).toHaveLength(5);
    expect(linkSessions([...others, one("a", "example/widgets"), one("b", "example/widgets")]).groups).toHaveLength(4);
  });
  test("a log's interior corruption is refused; only a torn final fragment is recoverable", async () => {
    const { parseLines } = await import("../backfill/cli.ts");
    expect(parseLines('{"n":1}\n{"n":2')).toEqual([{ n: 1 }]);
    expect(() => parseLines('{"n":1}\n{"n":2\n{"n":3}\n')).toThrow(/corrupt line/);
    const dir = PrivateDir.open(join(tmp, "r4-log"));
    dir.append("log.jsonl", '{"n":1}\n');
    const fd = openSync(join(dir.path, "log.jsonl"), "a"); writeSync(fd, '{"n":2,"ha'); closeSync(fd);
    dir.append("log.jsonl", '{"n":3}\n');
    expect(dir.read("log.jsonl")).toBe('{"n":1}\n{"n":3}\n');
    expect(parseLines(dir.read("log.jsonl"))).toEqual([{ n: 1 }, { n: 3 }]);
  });
});

describe("round-4 controls", () => {
  test("one exact GitHub parser: a mirror host whose path contains github.com abstains everywhere", async () => {
    const { githubSlug } = await import("../features/repo.ts");
    const { attributeRepo, slugFromRemote } = await import("../backfill/locate.ts");
    for (const ok of ["example/widgets", "github.com/example/widgets", "https://github.com/example/widgets.git", "https://tok@github.com/example/widgets", "ssh://git@github.com/example/widgets.git", "git@github.com:example/widgets.git", "HTTPS://GitHub.com/Example/Widgets/"])
      expect(githubSlug(ok)).toBe("example/widgets");
    for (const bad of ["https://mirror.example/github.com/example/widgets.git", "https://github.com.evil/example/widgets", "https://github.com:8443/example/widgets", "https://github.com/example/widgets/tree/main", "https://github.com/example/widgets?x=1", "git@mirror.example:github.com/example/widgets", "[REDACTED]", "../widgets"])
      expect(githubSlug(bad)).toBeNull();
    expect(slugFromRemote("https://mirror.example/github.com/example/widgets.git")).toBeNull();
    // The catalog's own remote is the strongest attribution: a mirror remote must not become example/widgets.
    const mirror = [{ table: "session_catalog", row: { repo_url: "https://mirror.example/github.com/example/widgets.git", cwd: "/work/widgets" } }];
    expect(attributeRepo(mirror).repo).toBeNull();
    expect(attributeRepo([{ table: "session_catalog", row: { repo_url: "https://github.com/example/widgets.git", cwd: "/work/widgets" } }]).repo).toBe("example/widgets");
    // And a path-only attribution (no owner) is never selected for a scoped collection.
    const { scopedSessions } = await import("../backfill/cli.ts");
    const scope = { format: "relayfacts-scope/1" as const, orgId: "o", workspaceId: "w", repos: ["example/widgets"], since: "2026-10-01T00:00:00.000Z", until: "2026-11-01T00:00:00.000Z", deleteBy: "2026-12-01T00:00:00.000Z", id: "s1" };
    const located = { scope: "s1", harness: "claude", sessionId: "x", status: "ok", ...attributeRepo(mirror) };
    expect(scopedSessions([located], scope, new Map([["claude\u0000x", "2026-10-10T00:00:00.000Z"]])).rows).toEqual([]);
  });
  test("invalid times fail closed: no build from a bad cut, no rolled-over calendar dates", () => {
    for (const bad of ["invalid", "2099-02-30T00:00:00Z", "2026-10-01", "2026-10-01T24:00:00Z", "2026-10-01T00:00:00", "2026-10-01T00:00:00+15:00"]) expect(utc(bad)).toBeNull();
    expect(utc("2026-10-31T19:00:00-05:00")).toBe("2026-11-01T00:00:00.000Z");
    const s = { source: "claude", sessionId: "s", analyzedAt: "2026-10-01T00:00:00.000Z", startedAt: "2026-10-01T00:00:00.000Z", endedAt: "2026-10-01T01:00:00.000Z", title: "", asked: [], built: [], gaps: [], rationale: [], outcomeStatuses: [], dropped: {}, keys: { repo: "example/widgets", branches: [], prs: [], commits: [], files: [], taskRefs: [], parents: [] } } as SessionFacts;
    expect(() => buildFeatures([s], { asOf: "invalid" })).toThrow(/asOf is not a valid/);
    expect(() => parseForge({ prs: [], commits: [{ sha: "a".repeat(40), at: "2099-02-30T00:00:00Z", onDefaultBranch: true }], flags: [] })).toThrow();
  });
});

describe("round-5 controls", () => {
  test("a query, fragment or backslash in what looks like userinfo never makes a mirror github.com, at admission too", async () => {
    const { githubSlug } = await import("../features/repo.ts");
    const { attributeRepo } = await import("../backfill/locate.ts");
    const tricks = ["https://mirror.invalid?@github.com/example/widgets", "https://mirror.invalid#@github.com/example/widgets.git", "https://u@mirror.invalid?x@github.com/example/widgets",
      "https://mirror.invalid\\@github.com/example/widgets", "ssh://git@mirror.invalid?@github.com/example/widgets", "https://github.com/example/widgets#main", "https://git hub@github.com/example/widgets"];
    for (const bad of tricks) {
      expect(githubSlug(bad)).toBeNull();
      // The WHATWG parser really does see another host (or a fragment) in each: the regex alone was not enough.
      if (!/\s/u.test(bad)) { const u = new URL(bad); expect(u.hostname !== "github.com" || u.hash !== "").toBe(true); }
    }
    expect(githubSlug("https://user:tok%40en@github.com/example/widgets.git")).toBe("example/widgets");
    const scope = { format: "relayfacts-scope/1" as const, orgId: "o", workspaceId: "w", repos: ["example/widgets"], since: "2026-10-01T00:00:00.000Z", until: "2026-11-01T00:00:00.000Z", deleteBy: "2026-12-01T00:00:00.000Z", id: "s1" };
    const admit = (remote: string) => {
      const located = { scope: "s1", harness: "claude", sessionId: "x", status: "ok", ...attributeRepo([{ table: "session_catalog", row: { repo_url: remote, cwd: "/work/widgets" } }]) };
      return scopedSessions([located], scope, new Map([["claude\u0000x", "2026-10-10T00:00:00.000Z"]])).rows.length;
    };
    expect(admit("https://github.com/example/widgets.git")).toBe(1);
    expect(admit("https://mirror.invalid/example/widgets.git")).toBe(0);
    for (const bad of tricks) expect(admit(bad)).toBe(0);
  });

  test("an impossible artifact date is undated, not rolled over to the next month", () => {
    const art = extractArtifacts([
      { table: "convergence_events", row: { event_id: "e1", ts: "2099-02-30T00:00:00Z", type: "tool_result", content: "[feat/x abcdef1] add widgets\n" } },
      { table: "convergence_events", row: { event_id: "e2", ts: "2026-10-13T04:59:07.460095Z", type: "tool_result", content: "[feat/x 1234567] more widgets\n" } },
    ], "example/widgets");
    expect(art.commits.map((c) => [c.sha, c.at])).toEqual([["abcdef1", null], ["1234567", "2026-10-13T04:59:07.460Z"]]);
  });

  test("finer-than-millisecond times floor exactly; cuts finer than a millisecond are refused", async () => {
    const { utcCut } = await import("../features/types.ts");
    expect(utc("2026-01-01T00:00:00.123999999Z")).toBe("2026-01-01T00:00:00.123Z");
    expect(utc("2026-01-01T00:00:00.9999Z")).toBe("2026-01-01T00:00:00.999Z");
    expect(utcCut("2026-01-01T00:00:00.123000Z")).toBe("2026-01-01T00:00:00.123Z");
    // Two distinct cuts within one millisecond would compare equal: neither is accepted.
    expect(utcCut("2026-01-01T00:00:00.123000001Z")).toBeNull();
    expect(utcCut("2026-01-01T00:00:00.123400Z")).toBeNull();
    // Strict-before with a millisecond cut is exact on floored evidence.
    const cut = utcCut("2026-01-01T00:00:00.124Z")!;
    expect(utc("2026-01-01T00:00:00.123999Z")! < cut).toBe(true);
    expect(utc("2026-01-01T00:00:00.124001Z")! < cut).toBe(false);
  });

  test("packets and score refuse a run with an impossible delete-by date or cut before writing", async () => {
    const { main } = await import("../bench/cli.ts");
    const dir = PrivateDir.open(join(tmp, "r5-run"));
    const feature = { id: "f1", title: "widgets", repos: ["example/widgets"], state: "in-progress", stateBasis: "analysis", stateReason: "r", rationale: [], links: [], firstAt: "2026-10-01T00:00:00.000Z", updatedAt: "2026-10-02T00:00:00.000Z", sessions: [{ source: "claude", sessionId: "s", role: "founding", startedAt: "2026-10-01T00:00:00.000Z" }], asked: [], built: [], gaps: [] };
    const run = (over: Record<string, unknown>) => JSON.stringify({ format: "relayfacts-features/1", corpus: "c", asOf: "2026-11-01T00:00:00.000Z", deleteBy: "2099-01-01T00:00:00.000Z", features: [feature], ...over });
    const cases: Array<[string, Record<string, unknown>, RegExp]> = [
      ["bad-delete-by.json", { deleteBy: "2099-02-30T00:00:00Z" }, /delete-by/],
      ["bad-as-of.json", { asOf: "2026-11-01T00:00:00.0000001Z" }, /as-of/],
      ["bad-record.json", { features: [{ ...feature, built: [{ kind: "commit", what: "x", at: "2099-02-30T00:00:00Z", evidence: [] }] }] }, /invalid time/],
    ];
    for (const [name, over, why] of cases) {
      dir.write(name, run(over));
      const out = join(tmp, `r5-packets-${name}`);
      expect(() => main(["packets", "--features", join(dir.path, name), "--out", out])).toThrow(why);
      expect(existsSync(join(out, "packets.jsonl"))).toBe(false);
    }
    dir.write("good.json", run({}));
    const out = join(tmp, "r5-packets-good");
    expect(main(["packets", "--features", join(dir.path, "good.json"), "--out", out])).toBe(0);
    expect(existsSync(join(out, "packets.jsonl"))).toBe(true);
  });

  test("every as-of entry point refuses a cut finer than a millisecond, called directly", () => {
    const fine = "2026-01-01T00:00:00.000999Z";
    const s = { source: "claude", sessionId: "s", analyzedAt: "2026-01-01T00:00:00.000Z", startedAt: "2026-01-01T00:00:00.000Z", endedAt: "2026-01-01T00:00:00.000Z", title: "", asked: [], built: [], gaps: [], rationale: [], outcomeStatuses: [], dropped: {}, keys: { repo: "example/widgets", branches: [], prs: [], commits: [], files: [], taskRefs: [], parents: [] } } as SessionFacts;
    const forge = { prs: [], commits: [], flags: [] };
    expect(() => factsAt(s, fine)).toThrow(/millisecond/);
    expect(() => deriveState([s], forge, fine)).toThrow(/millisecond/);
    expect(() => withCommitBranches(s, forge, fine)).toThrow(/millisecond/);
    expect(() => buildFeatures([], { asOf: fine })).toThrow(/millisecond/);
    expect(() => buildFeatures([s], { asOf: fine })).toThrow(/millisecond/);
    // A whole-millisecond cut still works, and a session strictly before it is kept.
    expect(factsAt(s, "2026-01-01T00:00:00.001Z")).not.toBeNull();
  });

  test("offsets stop at ±14:00", () => {
    expect(utc("2026-01-01T00:00:00+14:00")).toBe("2025-12-31T10:00:00.000Z");
    expect(utc("2026-01-01T00:00:00-14:00")).toBe("2026-01-01T14:00:00.000Z");
    for (const bad of ["2026-01-01T00:00:00+14:01", "2026-01-01T00:00:00-14:01", "2026-01-01T00:00:00+14:59", "2026-01-01T00:00:00-14:59"]) expect(utc(bad)).toBeNull();
    expect(utc("2026-01-01T00:00:00+13:59")).toBe("2025-12-31T10:01:00.000Z");
  });

  test("state is prefix-invariant: a PR close or branch landing on or after the cut never changes it", () => {
    const sess = (over: Partial<SessionFacts> = {}) => ({ source: "claude", sessionId: "s", analyzedAt: "2026-11-01T00:00:00.000Z", startedAt: "2026-11-01T00:00:00.000Z", endedAt: "2026-11-01T01:00:00.000Z", title: "", asked: [], built: [], gaps: [], rationale: [], outcomeStatuses: [], dropped: {}, keys: { repo: "example/widgets", branches: [], prs: [], commits: [], files: [], taskRefs: [], parents: [] }, ...over }) as SessionFacts;
    const cut = "2026-11-10T00:00:00.000Z";
    const pr = "https://github.com/example/widgets/pull/1";
    const s1 = sess({ built: [{ kind: "pr", ref: pr, what: "opened #1", at: "2026-11-01T00:30:00.000Z", evidence: [] }] as SessionFacts["built"] });
    const closedAt = (t: string | undefined) => ({ prs: [{ ref: pr, at: "2026-11-01T00:30:00.000Z", state: "closed" as const, ...(t ? { closedAt: t } : {}) }], commits: [], flags: [] });
    expect(deriveState([s1], { prs: [{ ref: pr, at: "2026-11-01T00:30:00.000Z", state: "open" as const }], commits: [], flags: [] }, cut).state).toBe("in_progress");
    expect(deriveState([s1], closedAt("2026-11-09T23:59:59.999Z"), cut).state).toBe("abandoned");
    expect(deriveState([s1], closedAt(cut), cut).state).toBe("in_progress");
    expect(deriveState([s1], closedAt("2026-11-11T00:00:00.000Z"), cut).state).toBe("in_progress");
    expect(deriveState([s1], closedAt(undefined), cut).state).toBe("in_progress");

    // A PR merged Nov 2, and a second branch of the feature (tipped Nov 3) that adds a default-off flag when it lands.
    const s2 = sess({ built: [{ kind: "pr", ref: pr, what: "opened #1", at: "2026-11-01T00:30:00.000Z", evidence: [] }] as SessionFacts["built"], keys: { repo: "example/widgets", branches: ["feat/flagged"], prs: [], commits: [], files: [], taskRefs: [], parents: [] } });
    const landing = (landedAt: string | null) => ({
      prs: [{ ref: pr, at: "2026-11-01T00:30:00.000Z", state: "merged" as const, mergedAt: "2026-11-02T00:00:00.000Z" }], commits: [], flags: [],
      branches: [{ name: "feat/flagged", tipAt: "2026-11-03T00:00:00.000Z", landed: landedAt !== null, landedAt, flagsAdded: ["WIDGETS_NEW_ENABLED"] }],
    });
    const landedUnknown = { ...landing(null), branches: [{ ...landing(null).branches[0]!, landed: true }] };
    expect(deriveState([s2], landing("2026-11-09T00:00:00.000Z"), cut).state).toBe("shipped_off");
    expect(deriveState([s2], landing(cut), cut).state).toBe("shipped");
    expect(deriveState([s2], landing("2026-11-11T00:00:00.000Z"), cut).state).toBe("shipped");
    expect(deriveState([s2], landedUnknown, cut).state).toBe("shipped");
  });

  test("a forge commit matching a built sha counts only when made strictly before the cut", () => {
    const cut = "2026-11-10T00:00:00.000Z";
    const s = { source: "claude", sessionId: "s", analyzedAt: "2026-11-01T00:00:00.000Z", startedAt: "2026-11-01T00:00:00.000Z", endedAt: "2026-11-01T01:00:00.000Z", title: "", asked: [], built: [{ kind: "commit", ref: "abc1234", what: "commit abc1234", at: "2026-11-01T00:30:00.000Z", evidence: [] }], gaps: [], rationale: [], outcomeStatuses: ["superseded"], dropped: {}, keys: { repo: "example/widgets", branches: [], prs: [], commits: [], files: [], taskRefs: [], parents: [] } } as unknown as SessionFacts;
    const forge = (at: string) => ({ prs: [], flags: [], commits: [{ sha: "abc1234" + "0".repeat(33), at, onDefaultBranch: false }] });
    const without = deriveState([s], { prs: [], commits: [], flags: [] }, cut);
    expect(without).toMatchObject({ state: "superseded", basis: "analysis" });
    for (const at of [cut, "2026-11-11T00:00:00.000Z"]) expect(deriveState([s], forge(at), cut)).toEqual(without);
    expect(deriveState([s], forge("2026-11-01T00:30:00.000Z"), cut)).toMatchObject({ state: "in_progress", basis: "forge" });
  });

  test("forge sealed after a later landing never changes an earlier cut (cherry-pick, --no-ff merge, merged revert)", async () => {
    const { execFileSync } = await import("node:child_process");
    const { forgeFacts } = await import("../backfill/forge.ts");
    const cut = "2026-11-10T00:00:00.000Z", after = "2026-11-13T00:00:00.000Z";
    const fixture = (name: string) => {
      const repo = join(tmp, `r9-${name}`);
      mkdirSync(repo);
      const git = (args: string[], date: string) => execFileSync("git", ["-C", repo, ...args], { env: { ...process.env, GIT_AUTHOR_DATE: date, GIT_COMMITTER_DATE: date, GIT_AUTHOR_NAME: "t", GIT_AUTHOR_EMAIL: "t@example.invalid", GIT_COMMITTER_NAME: "t", GIT_COMMITTER_EMAIL: "t@example.invalid" }, encoding: "utf8" }).trim();
      const commit = (file: string, date: string) => { writeFileSync(join(repo, file), file); git(["add", file], date); git(["commit", "-q", "-m", file], date); return git(["rev-parse", "HEAD"], date); };
      git(["init", "-q", "-b", "main"], "2026-11-01T00:00:00Z");
      commit("base.txt", "2026-11-01T00:00:00Z");
      const seal = () => forgeFacts("example/widgets", repo, "2026-10-31T00:00:00Z", "2026-11-30T00:00:00Z", "main", () => []);
      return { git, commit, seal };
    };
    const session = (sha: string, branches: string[] = []) => ({ source: "claude", sessionId: "s", analyzedAt: "2026-11-02T01:00:00.000Z", startedAt: "2026-11-02T00:00:00.000Z", endedAt: "2026-11-02T01:00:00.000Z", title: "", asked: [],
      built: [{ kind: "commit", ref: sha.slice(0, 7), what: `commit ${sha.slice(0, 7)}`, at: "2026-11-02T00:30:00.000Z", evidence: [] }], gaps: [], rationale: [], outcomeStatuses: ["superseded"], dropped: {},
      keys: { repo: "example/widgets", branches, prs: [], commits: [], files: [], taskRefs: [], parents: [] } }) as unknown as SessionFacts;

    // 1. A Nov 2 topic commit cherry-picked onto main on Nov 12.
    {
      const { git, commit, seal } = fixture("pick");
      git(["checkout", "-q", "-b", "feat/pick"], "2026-11-02T00:00:00Z");
      const topic = commit("pick.txt", "2026-11-02T00:00:00Z");
      const s = session(topic);
      const before = deriveState([s], seal(), cut);
      expect(before).toMatchObject({ state: "in_progress", basis: "forge" });
      git(["checkout", "-q", "main"], "2026-11-12T00:00:00Z");
      git(["cherry-pick", topic], "2026-11-12T00:00:00Z");
      const later = seal();
      const c = later.commits.find((x) => x.sha === topic)!;
      expect([c.at, c.defaultAt, c.defaultAtBasis, c.onDefaultBranch]).toEqual(["2026-11-02T00:00:00.000Z", "2026-11-12T00:00:00.000Z", "patch", true]);
      expect(deriveState([s], later, cut)).toEqual(before);
      expect(deriveState([s], later, after).state).toBe("shipped");
    }
    // 2. A Nov 2 topic tip merged --no-ff on Nov 12.
    {
      const { git, commit, seal } = fixture("noff");
      git(["checkout", "-q", "-b", "feat/noff"], "2026-11-02T00:00:00Z");
      const topic = commit("noff.txt", "2026-11-02T00:00:00Z");
      const s = session(topic, ["feat/noff"]);
      const before = deriveState([s], seal(), cut);
      expect(before).toMatchObject({ state: "in_progress", basis: "forge" });
      git(["checkout", "-q", "main"], "2026-11-12T00:00:00Z");
      git(["merge", "-q", "--no-ff", "-m", "merge", "feat/noff"], "2026-11-12T00:00:00Z");
      const later = seal();
      expect(later.branches!.find((b) => b.name === "feat/noff")).toMatchObject({ landed: true, landedAt: "2026-11-12T00:00:00.000Z", tipAt: "2026-11-02T00:00:00.000Z" });
      expect(deriveState([s], later, cut)).toEqual(before);
      expect(deriveState([s], later, after).state).toBe("shipped");
    }
    // 3. A Nov 1 main commit reverted on a side branch on Nov 3, the revert merged --no-ff on Nov 12.
    {
      const { git, commit, seal } = fixture("revert");
      const mainWork = commit("work.txt", "2026-11-01T12:00:00Z");
      git(["checkout", "-q", "-b", "fix/revert"], "2026-11-03T00:00:00Z");
      git(["revert", "--no-edit", mainWork], "2026-11-03T00:00:00Z");
      const s = session(mainWork);
      const before = deriveState([s], seal(), cut);
      expect(before).toMatchObject({ state: "shipped", basis: "forge" });
      git(["checkout", "-q", "main"], "2026-11-12T00:00:00Z");
      git(["merge", "-q", "--no-ff", "-m", "merge revert", "fix/revert"], "2026-11-12T00:00:00Z");
      const later = seal();
      expect(later.commits.find((x) => x.sha === mainWork)!.revertedAt).toBe("2026-11-12T00:00:00.000Z");
      expect(deriveState([s], later, cut)).toEqual(before);
      expect(deriveState([s], later, after).state).toBe("regressed");
    }
  });

  test("forge: a tag named like a branch is not ambiguous, and a landing, revert and re-landing replay in order", async () => {
    const { execFileSync } = await import("node:child_process");
    const { forgeFacts } = await import("../backfill/forge.ts");
    const repo = join(tmp, "r10-git");
    mkdirSync(repo);
    const git = (args: string[], date: string) => execFileSync("git", ["-C", repo, ...args], { env: { ...process.env, GIT_AUTHOR_DATE: date, GIT_COMMITTER_DATE: date, GIT_AUTHOR_NAME: "t", GIT_AUTHOR_EMAIL: "t@example.invalid", GIT_COMMITTER_NAME: "t", GIT_COMMITTER_EMAIL: "t@example.invalid" }, encoding: "utf8" }).trim();
    const commit = (file: string, date: string) => { writeFileSync(join(repo, file), file); git(["add", file], date); git(["commit", "-q", "-m", file], date); return git(["rev-parse", "HEAD"], date); };
    const seal = () => forgeFacts("example/widgets", repo, "2026-10-31T00:00:00Z", "2026-11-30T00:00:00Z", "main", () => []);
    git(["init", "-q", "-b", "main"], "2026-11-01T00:00:00Z");
    commit("base.txt", "2026-11-01T00:00:00Z");
    git(["checkout", "-q", "-b", "topic"], "2026-11-02T00:00:00Z");
    const topic = commit("topic.txt", "2026-11-02T00:00:00Z");
    git(["tag", "topic"], "2026-11-02T00:00:00Z");
    git(["checkout", "-q", "main"], "2026-11-06T00:00:00Z");
    git(["cherry-pick", topic], "2026-11-06T00:00:00Z");
    const firstCopy = git(["rev-parse", "HEAD"], "2026-11-06T00:00:00Z");
    git(["revert", "--no-edit", firstCopy], "2026-11-08T00:00:00Z");
    const s = { source: "claude", sessionId: "s", analyzedAt: "2026-11-02T01:00:00.000Z", startedAt: "2026-11-02T00:00:00.000Z", endedAt: "2026-11-02T01:00:00.000Z", title: "", asked: [],
      built: [{ kind: "commit", ref: topic.slice(0, 7), what: "c", at: "2026-11-02T00:30:00.000Z", evidence: [] }], gaps: [], rationale: [], outcomeStatuses: [], dropped: {},
      keys: { repo: "example/widgets", branches: ["topic"], prs: [], commits: [], files: [], taskRefs: [], parents: [] } } as unknown as SessionFacts;
    // 1. The branch and the tag share a name: the seal does not abort, and the branch is the branch.
    const mid = seal();
    expect(mid.branches!.map((b) => b.name)).toEqual(["topic"]);
    expect(mid.branches![0]).toMatchObject({ landed: true, landedAt: "2026-11-06T00:00:00.000Z", tipAt: "2026-11-02T00:00:00.000Z" });
    const atMid = ["2026-11-07T00:00:00.000Z", "2026-11-09T00:00:00.000Z"].map((cut) => deriveState([s], mid, cut));
    expect(atMid.map((r) => r.state)).toEqual(["shipped", "regressed"]);
    // 2. Re-landed on Nov 12: shipped, regressed, shipped across the three events; earlier cuts unchanged.
    git(["cherry-pick", topic], "2026-11-12T00:00:00Z");
    const later = seal();
    const c = later.commits.find((x) => x.sha === topic)!;
    expect(c.transitions!.map((t) => [t.kind, t.at])).toEqual([["landed", "2026-11-06T00:00:00.000Z"], ["reverted", "2026-11-08T00:00:00.000Z"], ["landed", "2026-11-12T00:00:00.000Z"]]);
    expect(["2026-11-07T00:00:00.000Z", "2026-11-09T00:00:00.000Z"].map((cut) => deriveState([s], later, cut))).toEqual(atMid);
    expect(deriveState([s], later, "2026-11-13T00:00:00.000Z").state).toBe("shipped");
    expect(deriveState([s], later, "2026-11-12T00:00:00.000Z").state).toBe("regressed");
  });

  test("forge: pull requests are listed in full and filtered locally, never through the 1,000-result search", async () => {
    const { prsUpdatedSince } = await import("../backfill/forge.ts");
    const pr = (n: number, updatedAt: string | null) => ({ url: `https://github.com/example/widgets/pull/${n}`, state: "OPEN", createdAt: "2026-01-01T00:00:00Z", updatedAt, mergedAt: null, closedAt: null, headRefName: null });
    const many = Array.from({ length: 1500 }, (_, i) => pr(i, "2026-11-0" + (1 + (i % 9)) + "T00:00:00Z"));
    expect(prsUpdatedSince([...many, pr(9999, "2026-10-01T00:00:00Z"), pr(9998, "bad"), pr(9997, null)], "2026-11-01T00:00:00Z")).toHaveLength(1500);
    expect(() => prsUpdatedSince(many, "2026-11-01T00:00:00Z", 1500)).toThrow(/partial/);
    const src = readFileSync(join(import.meta.dir, "../backfill/forge.ts"), "utf8");
    expect(src).not.toMatch(/"--search"/);
  });

  test("forge history: same-second order, a long history kept whole or refused, first-landing provenance fixed at the first seal", async () => {
    const { execFileSync } = await import("node:child_process");
    const { forgeFacts } = await import("../backfill/forge.ts");
    const fixture = (name: string) => {
      const repo = join(tmp, `r11-${name}`);
      mkdirSync(repo);
      const git = (args: string[], date: string) => execFileSync("git", ["-C", repo, ...args], { env: { ...process.env, GIT_AUTHOR_DATE: date, GIT_COMMITTER_DATE: date, GIT_AUTHOR_NAME: "t", GIT_AUTHOR_EMAIL: "t@example.invalid", GIT_COMMITTER_NAME: "t", GIT_COMMITTER_EMAIL: "t@example.invalid" }, encoding: "utf8" }).trim();
      const commit = (file: string, date: string) => { writeFileSync(join(repo, file), file); git(["add", file], date); git(["commit", "-q", "-m", file], date); return git(["rev-parse", "HEAD"], date); };
      git(["init", "-q", "-b", "main"], "2026-11-01T00:00:00Z");
      commit("base.txt", "2026-11-01T00:00:00Z");
      git(["checkout", "-q", "-b", "topic"], "2026-11-02T00:00:00Z");
      const topic = commit("topic.txt", "2026-11-02T00:00:00Z");
      git(["checkout", "-q", "main"], "2026-11-02T00:00:00Z");
      const seal = (max?: number) => forgeFacts("example/widgets", repo, "2026-10-31T00:00:00Z", "2026-11-30T00:00:00Z", "main", () => [], max);
      const s = { source: "claude", sessionId: "s", analyzedAt: "2026-11-02T01:00:00.000Z", startedAt: "2026-11-02T00:00:00.000Z", endedAt: "2026-11-02T01:00:00.000Z", title: "", asked: [],
        built: [{ kind: "commit", ref: topic.slice(0, 7), what: "c", at: "2026-11-02T00:30:00.000Z", evidence: [] }], gaps: [], rationale: [], outcomeStatuses: [], dropped: {},
        keys: { repo: "example/widgets", branches: [], prs: [], commits: [], files: [], taskRefs: [], parents: [] } } as unknown as SessionFacts;
      return { git, topic, seal, s, repo };
    };
    // 1. Revert and re-pick in the same second: the default branch's order decides, and the file is restored.
    {
      const { git, topic, seal, s, repo } = fixture("tie");
      git(["cherry-pick", topic], "2026-11-06T00:00:00Z");
      git(["revert", "--no-edit", "HEAD"], "2026-11-08T00:00:00Z");
      git(["cherry-pick", topic], "2026-11-08T00:00:00Z");
      expect(existsSync(join(repo, "topic.txt"))).toBe(true);
      const f = seal();
      const t = f.commits.find((x) => x.sha === topic)!.transitions!;
      expect(t.map((x) => [x.kind, x.at])).toEqual([["landed", "2026-11-06T00:00:00.000Z"], ["reverted", "2026-11-08T00:00:00.000Z"], ["landed", "2026-11-08T00:00:00.000Z"]]);
      expect(t[1]!.seq! < t[2]!.seq!).toBe(true);
      expect(deriveState([s], f, "2026-11-09T00:00:00.000Z").state).toBe("shipped");
    }
    // 2. Twenty-one alternating transitions on distinct days, ending in a landing: kept whole, and over the bound refused.
    {
      const { git, topic, seal, s, repo } = fixture("long");
      for (let i = 0; i < 21; i++) {
        const day = `2026-11-${String(3 + i).padStart(2, "0")}T00:00:00Z`;
        if (i % 2 === 0) git(["cherry-pick", topic], day); else git(["revert", "--no-edit", "HEAD"], day);
      }
      expect(existsSync(join(repo, "topic.txt"))).toBe(true);
      const f = seal();
      expect(f.commits.find((x) => x.sha === topic)!.transitions).toHaveLength(21);
      expect(deriveState([s], f, "2026-11-25T00:00:00.000Z").state).toBe("shipped");
      expect(deriveState([s], f, "2026-11-23T00:00:00.000Z").state).toBe("regressed");
      expect(() => seal(20)).toThrow(/cannot be kept whole/);
    }
    // 3. A first landing by a copy reads the same before and after a later revert is sealed.
    {
      const { git, topic, seal, s } = fixture("prov");
      git(["cherry-pick", topic], "2026-11-06T00:00:00Z");
      const before = deriveState([s], seal(), "2026-11-07T00:00:00.000Z");
      expect(before.reason).toMatch(/landed as/);
      git(["revert", "--no-edit", "HEAD"], "2026-11-08T00:00:00Z");
      expect(deriveState([s], seal(), "2026-11-07T00:00:00.000Z")).toEqual(before);
    }
  });

  test("forge history: one merge's transitions keep the side branch's order; branch evidence never masks commit order", async () => {
    const { execFileSync } = await import("node:child_process");
    const { forgeFacts } = await import("../backfill/forge.ts");
    const fixture = (name: string) => {
      const repo = join(tmp, `r12-${name}`);
      mkdirSync(repo);
      const git = (args: string[], date: string) => execFileSync("git", ["-C", repo, ...args], { env: { ...process.env, GIT_AUTHOR_DATE: date, GIT_COMMITTER_DATE: date, GIT_AUTHOR_NAME: "t", GIT_AUTHOR_EMAIL: "t@example.invalid", GIT_COMMITTER_NAME: "t", GIT_COMMITTER_EMAIL: "t@example.invalid" }, encoding: "utf8" }).trim();
      const commit = (file: string, date: string) => { writeFileSync(join(repo, file), file); git(["add", file], date); git(["commit", "-q", "-m", file], date); return git(["rev-parse", "HEAD"], date); };
      git(["init", "-q", "-b", "main"], "2026-11-01T00:00:00Z");
      commit("base.txt", "2026-11-01T00:00:00Z");
      git(["checkout", "-q", "-b", "topic"], "2026-11-02T00:00:00Z");
      const topic = commit("topic.txt", "2026-11-02T00:00:00Z");
      git(["checkout", "-q", "main"], "2026-11-02T00:00:00Z");
      const seal = () => forgeFacts("example/widgets", repo, "2026-10-31T00:00:00Z", "2026-11-30T00:00:00Z", "main", () => []);
      const s = (branches: string[] = []) => ({ source: "claude", sessionId: "s", analyzedAt: "2026-11-02T01:00:00.000Z", startedAt: "2026-11-02T00:00:00.000Z", endedAt: "2026-11-02T01:00:00.000Z", title: "", asked: [],
        built: [{ kind: "commit", ref: topic.slice(0, 7), what: "c", at: "2026-11-02T00:30:00.000Z", evidence: [] }], gaps: [], rationale: [], outcomeStatuses: [], dropped: {},
        keys: { repo: "example/widgets", branches, prs: [], commits: [], files: [], taskRefs: [], parents: [] } }) as unknown as SessionFacts;
      return { git, topic, seal, s, repo };
    };
    const cut = "2026-11-13T00:00:00.000Z";
    // 1. Picked and reverted on a side branch, merged --no-ff on Nov 12: the file is absent, and the replay says so.
    for (const repick of [false, true]) {
      const { git, topic, seal, s, repo } = fixture(repick ? "side-repick" : "side");
      git(["checkout", "-q", "-b", "side"], "2026-11-05T00:00:00Z");
      git(["cherry-pick", topic], "2026-11-06T00:00:00Z");
      git(["revert", "--no-edit", "HEAD"], "2026-11-07T00:00:00Z");
      if (repick) git(["cherry-pick", topic], "2026-11-08T00:00:00Z");
      git(["checkout", "-q", "main"], "2026-11-12T00:00:00Z");
      git(["merge", "-q", "--no-ff", "-m", "merge side", "side"], "2026-11-12T00:00:00Z");
      expect(existsSync(join(repo, "topic.txt"))).toBe(repick);
      const f = seal();
      const t = f.commits.find((x) => x.sha === topic)!.transitions!;
      expect(t.every((x) => x.at === "2026-11-12T00:00:00.000Z" && x.seq !== undefined)).toBe(true);
      expect(deriveState([s()], f, cut).state).toBe(repick ? "shipped" : "regressed");
    }
    // 2. Same-second pick, revert and re-pick on main: adding the topic branch as evidence keeps the commit order.
    {
      const { git, topic, seal, s, repo } = fixture("mask");
      git(["cherry-pick", topic], "2026-11-06T00:00:00Z");
      git(["revert", "--no-edit", "HEAD"], "2026-11-06T00:00:00Z");
      git(["cherry-pick", topic], "2026-11-06T00:00:00Z");
      expect(existsSync(join(repo, "topic.txt"))).toBe(true);
      const f = seal();
      expect(f.branches!.find((b) => b.name === "topic")!.landedSeq).toBeDefined();
      expect(deriveState([s()], f, cut).state).toBe("shipped");
      expect(deriveState([s(["topic"])], f, cut).state).toBe("shipped");
    }
  });
});
