/** Trust boundaries of the relayfacts tools: output paths, credentials, corpus refusals, build evidence. */
import { afterAll, describe, expect, test } from "bun:test";
import { existsSync, mkdtempSync, rmSync, symlinkSync, writeFileSync, mkdirSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { SessionAnalysisSnapshot } from "../../session-analysis/engine/session-analysis-result.ts";
import { CORPUS_FORMAT, loadCorpus, sha256Hex } from "../bench/corpus.ts";
import { extractArtifacts } from "../backfill/artifacts.ts";
import { readPool } from "../backfill/source.ts";
import { deriveState } from "../features/build.ts";
import { extractSessionFacts } from "../features/extract.ts";
import { outsideDir, REPO_ROOT } from "../out-path.ts";
import type { SessionFacts } from "../features/types.ts";

const tmp = mkdtempSync(join(tmpdir(), "relayfacts-bounds-"));
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
  test("the result of the session's own gh pr create is built", () => {
    const f = extractSessionFacts(snap([ref("c1", "tool-call", "gh pr create --fill"), ref("r1", "tool-result", "ok", "https://github.com/example/widgets/pull/9")]));
    expect(f.built.filter((b) => b.kind === "pr").map((b) => b.ref)).toEqual(["https://github.com/example/widgets/pull/9"]);
  });
  test("transcript artifacts: a created PR versus a listed one; tag pushes are not branches", () => {
    const ev = (id: string, type: string, content: string) => ({ table: "convergence_events", row: { event_id: id, ts: "2026-11-10T10:00:00Z", type, content } });
    const art = extractArtifacts([
      ev("1", "tool_call", "gh pr list"), ev("2", "tool_result", "https://github.com/example/widgets/pull/1"),
      ev("3", "tool_call", "gh pr create --fill"), ev("4", "tool_result", "https://github.com/example/widgets/pull/2"),
      ev("5", "tool_result", "To https://github.com/example/widgets.git\n * [new tag]         v1.2.3 -> v1.2.3\n"),
    ], "example/widgets");
    expect(art.prs.map((p) => [p.url.split("/").pop(), p.via])).toEqual([["1", "mention"], ["2", "created"]]);
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
