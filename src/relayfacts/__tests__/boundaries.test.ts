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
import { buildFeatures } from "../features/build.ts";
import { lstatSync, readFileSync, statSync } from "node:fs";
import type { SessionFacts } from "../features/types.ts";

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
    writeFileSync(join(dir, "sessions.jsonl"), sessions);
    writeFileSync(join(dir, "forge.json"), forge);
    writeFileSync(join(dir, "manifest.json"), JSON.stringify({
      format: CORPUS_FORMAT, repo: "example/widgets", window: { since: "2026-10-01T00:00:00Z", until: "2026-11-01T00:00:00Z" },
      createdAt: "2026-11-01T00:00:00Z", deleteBy: over.deleteBy ?? "2026-11-20T00:00:00Z",
      files: { "sessions.jsonl": over.sessionsSha ?? sha256Hex(sessions), "forge.json": sha256Hex(forge) }, ...(over.extra ? { note: "x" } : {}),
    }));
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
    const call = { ...ref("k1", "tool-call", "Bash", "gh pr create --fill"), toolCallId: null };
    const paired = { ...ref("r1", "tool-result", "ok", "https://github.com/example/widgets/pull/9"), toolCallId: "k1", isError: false };
    expect(extractSessionFacts(snap([call, paired])).built.filter((b) => b.kind === "pr").map((b) => b.ref)).toEqual(["https://github.com/example/widgets/pull/9"]);
    // Adjacent but another call's result, a failed result, or a result whose summary only names the command: not built.
    const otherCall = { ...ref("k2", "tool-call", "Bash", "gh pr list"), toolCallId: null };
    for (const [c, r] of [[call, { ...paired, toolCallId: "k2" }], [call, { ...paired, isError: true }], [call, { ...paired, toolCallId: null, summary: "gh pr create" }], [{ ...call, isError: true }, paired]] as const) {
      const f = extractSessionFacts(snap([c, otherCall, r]));
      expect(f.built.filter((b) => b.kind === "pr")).toEqual([]);
      expect(f.keys.prs).toEqual(["https://github.com/example/widgets/pull/9"]);
    }
  });
  test("transcript artifacts: a created PR versus a listed one; tag pushes are not branches", () => {
    const ev = (id: string, type: string, content: string, payload: Record<string, unknown> = {}) => ({ table: "convergence_events", row: { event_id: id, ts: "2026-11-10T10:00:00Z", type, content, record: { payload } } });
    const art = extractArtifacts([
      ev("1", "tool_call", "gh pr list", { tool_use_id: "u1" }), ev("2", "tool_result", "https://github.com/example/widgets/pull/1", { tool_use_id: "u1" }),
      ev("3", "tool_call", "gh pr create --fill", { tool_use_id: "u3" }), ev("4", "tool_result", "https://github.com/example/widgets/pull/2", { tool_use_id: "u3", result_status: "success" }),
      // adjacency to a create call proves nothing: another call's result, and a failed paired result
      ev("5", "tool_call", "gh pr create --fill", { tool_use_id: "u5" }), ev("6", "tool_result", "https://github.com/example/widgets/pull/3", { tool_use_id: "zz" }),
      ev("7", "tool_call", "gh pr create --fill", { tool_use_id: "u7" }), ev("8", "tool_result", "https://github.com/example/widgets/pull/4", { tool_use_id: "u7", result_status: "error" }),
      ev("9", "tool_result", "To https://github.com/example/widgets.git\n * [new tag]         v1.2.3 -> v1.2.3\n"),
    ], "example/widgets");
    expect(art.prs.map((p) => [p.url.split("/").pop(), p.via])).toEqual([["1", "mention"], ["2", "created"], ["3", "mention"], ["4", "mention"]]);
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
    expect(errorCode(Object.assign(new Error("password=hunter2 at host"), { code: "28P01" }))).toBe("28P01");
    expect(errorCode(new TypeError("leaked text"))).toBe("TypeError");
    expect(errorCode({ code: "has spaces and secrets" })).toBe("error");
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
