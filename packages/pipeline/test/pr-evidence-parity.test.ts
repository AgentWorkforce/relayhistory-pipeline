/**
 * SP PR evidence against Neon's: the same delivered records go through the
 * vendored relayhistory migrations (live delivery triggers, then the real
 * `sessions.session_pull_request_evidence_v1`, 0065) and through
 * `projectRelayhistorySession` + `extractPullRequestEvidence`. The row sets
 * must be equal. Cases follow the comments and branches of 0065.
 */
import { PGlite } from "@electric-sql/pglite";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { readdirSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import {
  canonicalTimestamp,
  projectRelayhistorySession,
} from "../src/lib/pipeline/relayhistory-projection.js";
import {
  normalizeRelayhistoryRecords,
  parseRelayhistoryRecord,
  type RelayhistoryStreamRecord,
} from "../src/lib/pipeline/relayhistory-transform.js";
import { extractPullRequestEvidence } from "../../../src/session-pr-evidence/extract.js";

const MIGRATIONS = fileURLToPath(new URL("../../../vendor/relayhistory-cloud/migrations/", import.meta.url).href);
const forPglite = (sql: string) =>
  sql
    .replace(/CREATE EXTENSION IF NOT EXISTS vector[^;]*;/gi, "")
    .replace(/CREATE (UNIQUE )?INDEX[^;]*USING hnsw[^;]*;/gi, "")
    .replace(/public\.vector\(\d+\)/g, "text")
    .replace(/\bCONCURRENTLY\b/gi, "");
const WORKSPACE = "workspace-1";
const SESSION = "session-1";
let db: PGlite;

beforeAll(async () => {
  db = new PGlite({ extensions: { pg_trgm } });
  await db.exec(`SET search_path = pg_catalog, public;
    CREATE SCHEMA IF NOT EXISTS sessions;
    CREATE TABLE IF NOT EXISTS sessions.__migrations (
      name text PRIMARY KEY, checksum text NOT NULL, applied_at timestamptz NOT NULL DEFAULT now());`);
  for (const file of readdirSync(MIGRATIONS).filter((n) => n.endsWith(".sql")).sort())
    await db.exec(forPglite(readFileSync(MIGRATIONS + file, "utf8")));
  const { rows } = await db.query<{ index_name: string; definition: string }>(
    "SELECT index_name, definition FROM sessions.delivery_rollout_indexes()");
  for (const index of rows) await db.exec(`CREATE INDEX ${index.index_name} ${index.definition}`);
  await db.exec(`SET session_replication_role = replica;
    UPDATE sessions.upload_cleanup_rollout SET phase = 5 WHERE singleton;
    SET session_replication_role = origin;`);
  await db.exec(`SELECT sessions.activate_delivery_catalog();
    SELECT sessions.activate_delivery_projection_v2();
    SET search_path = public`);
}, 120_000);
afterAll(async () => { await db?.close(); });
beforeEach(async () => {
  await db.exec(`SET session_replication_role = replica;
    TRUNCATE sessions.delivery_records, sessions.convergence_events, sessions.session_catalog,
      sessions.session_catalog_agents, sessions.session_relationships, sessions.session_markers,
      sessions.session_commit_links, sessions.session_rollups CASCADE;
    SET session_replication_role = origin;`);
});

type Line = Omit<RelayhistoryStreamRecord, "operation"> & { operation?: "upsert" | "delete" };
const RECEIVED = "2026-10-30 12:00:00.123456+00";
const T = 1_793_000_000_000;
let serial = 0;
const line = (kind: string, payload: Record<string, unknown>, record_id = `r${++serial}`): Line => ({
  kind, origin_id: "origin-a", record_id, revision: "1", received_at: RECEIVED, payload,
});
/** One tool call and its successful result. */
const tool = (id: string, at: number, name: string, args: unknown, result: string, extra: Record<string, unknown> = {}): Line[] => [
  line("tool_call", { name, tool_use_id: id, args_json: typeof args === "string" ? args : JSON.stringify(args), is_error: 0, ts_ms: at, ...extra }),
  line("session_event", { role: "tool_result", kind: "tool_result", tool_use_id: id, text: result, ts_ms: at + 1 }),
];
const bash = (id: string, at: number, command: string, result: string) => tool(id, at, "Bash", { command }, result);

async function deliver(l: Line, source: string) {
  await db.query(
    `INSERT INTO sessions.delivery_records
       (org_id, workspace_id, origin_id, record_id, revision_id, revision, digest, kind, source,
        session_id, operation, payload, user_id, received_at)
     VALUES ('org', $1, $2, $3, $3 || ':' || $4, $4::bigint, 'digest', $5, $6, $7, $8, $9::jsonb, 'u', $10::timestamptz)
     ON CONFLICT (org_id, workspace_id, origin_id, record_id) DO UPDATE SET
       revision = excluded.revision, revision_id = excluded.revision_id, kind = excluded.kind,
       operation = excluded.operation, payload = excluded.payload, received_at = excluded.received_at`,
    [WORKSPACE, l.origin_id, l.record_id, l.revision, l.kind, source, SESSION, l.operation ?? "upsert",
      l.payload === null ? null : JSON.stringify(l.payload), l.received_at]);
}

type Row = Record<string, unknown>;
const norm = (r: Row): Row => ({
  ...r,
  detected_at: canonicalTimestamp(String(r.detected_at)),
  last_seen_at: canonicalTimestamp(String(r.last_seen_at)),
});
const key = (r: Row) => JSON.stringify(Object.keys(r).sort().map((k) => [k, r[k]]));

/** Neon rows and SP rows for the same lines; asserts equality and returns them. */
async function expectParity(lines: Line[], source = "claude") {
  for (const l of lines) await deliver(l, source);
  const neon = (await db.query<{ e: Row }>(
    "SELECT to_jsonb(e) AS e FROM sessions.session_pull_request_evidence_v1('org', $1, $2, $3) e",
    [WORKSPACE, source, SESSION])).rows.map((r) => norm(r.e));
  const projected = projectRelayhistorySession(
    normalizeRelayhistoryRecords(lines.map((l) => parseRelayhistoryRecord(JSON.stringify(l)))),
    { workspaceId: WORKSPACE, source, sessionId: SESSION, machineId: "machine:1", userId: "u" });
  const sp = extractPullRequestEvidence(projected.rows).map((r) => norm(r as unknown as Row));
  expect([...sp].sort((a, b) => key(a).localeCompare(key(b)))).toEqual([...neon].sort((a, b) => key(a).localeCompare(key(b))));
  return neon;
}

describe("PR evidence equals Neon's session_pull_request_evidence_v1", () => {
  it("gh pr create with --head, and its URL", async () => {
    const rows = await expectParity(bash("t1", T, "gh pr create --title 'x' --head feat/a --body \"see https://github.com/o/other/pull/9\"",
      "https://github.com/acme/repo/pull/42\n"));
    expect(rows.map((r) => [r.number, r.head_ref, r.role, r.evidence_kind])).toEqual([[42, "feat/a", "creator", "gh_pr_create"]]);
  });

  it("gh's 'Creating pull request for' line supplies head_ref", async () => {
    await expectParity(bash("t1", T, "gh pr create -f",
      "Creating pull request for feat/b into main in acme/repo\n\nhttps://github.com/acme/repo/pull/7"));
  });

  it("Codex shell {output} wrapper is unwrapped", async () => {
    await expectParity(tool("t1", T, "shell", { command: ["bash", "-lc", "gh pr create --fill"] },
      JSON.stringify({ output: "https://github.com/acme/repo/pull/8\n", metadata: { exit_code: 0 } })));
  });

  it("MCP create_pull_request reads only the top-level html_url / url", async () => {
    await expectParity([
      ...tool("t1", T, "mcp__github__create_pull_request", { owner: "acme", repo: "repo", head: "acme:feat/c", title: "x" },
        JSON.stringify({ html_url: "https://github.com/acme/repo/pull/11", body: "see https://github.com/acme/repo/pull/99" })),
      ...tool("t2", T + 10, "mcp__github__create_pull_request", { owner: "acme", repo: "repo" },
        JSON.stringify({ url: "https://github.com/acme/repo/pull/12" })),
      ...tool("t3", T + 20, "github.create_pull_request", { owner: "acme", repo: "repo" },
        "created https://github.com/acme/repo/pull/13 ok"),
    ]);
  });

  it("git push remote/ref parsing joins a branch to a numbered PR by timeline", async () => {
    const rows = await expectParity([
      ...bash("p1", T, "git push -u origin feat/d",
        "remote:\nTo github.com:acme/repo.git\n * [new branch]      feat/d -> feat/d\nbranch 'feat/d' set up"),
      ...bash("c1", T + 10, "gh pr create --head feat/d", "https://github.com/acme/repo/pull/21"),
      ...bash("p2", T + 20, "git -C /x push", "To https://github.com/acme/repo\n   abc1234..def5678  feat/d -> feat/d"),
      ...bash("p3", T + 30, "git push other", "To https://github.com/acme/elsewhere.git\n + aaaa111...bbbb222 x -> refs/heads/lonely"),
    ]);
    expect(rows.find((r) => r.number === 21)?.role).toBe("creator");
    expect(rows.some((r) => r.number === null && r.head_ref === "lonely")).toBe(true);
  });

  it("a branch between two PRs on the same branch stays branch-only", async () => {
    await expectParity([
      ...bash("c1", T, "gh pr create --head feat/e", "https://github.com/acme/repo/pull/31"),
      ...bash("p1", T + 10, "git push", "To github.com:acme/repo.git\n   1111aaaa..2222bbbb  feat/e -> feat/e"),
      ...bash("c2", T + 20, "gh pr create --head feat/e", "https://github.com/acme/repo/pull/32"),
    ]);
  });

  it("gh pr <sub> positional targets: URL, number with -R / catalog / push repo, branch, flag values", async () => {
    await expectParity([
      line("session", { repo_url: "git@github.com:acme/repo.git", title: "t" }),
      ...bash("g1", T, "gh pr comment https://github.com/acme/repo/pull/5 --body 'see https://github.com/x/y/pull/1'", "done"),
      ...bash("g2", T + 1, "gh pr merge 6 --squash -R acme/other", "merged"),
      ...bash("g3", T + 2, "gh pr edit --title \"7 fixes\" 7", "https://github.com/acme/repo/pull/7"),
      ...bash("g4", T + 3, "gh pr checkout feat/f", "Switched"),
      ...bash("g5", T + 4, "gh pr review --approve #8 && echo ok", "Approved"),
      ...bash("g6", T + 5, "gh pr close -c \"9\" 10", "closed"),
      ...bash("g7", T + 6, "gh pr ready", "https://github.com/acme/repo/pull/11 is ready"),
      ...bash("g8", T + 7, "gh pr edit --body=\"x y\" 12", "ok"),
    ]);
  });

  it("gh pr number with no catalog resolves through the one pushed repo, else the output URL", async () => {
    await expectParity([
      ...bash("p1", T, "git push", "To https://github.com/Acme/Repo.git\n * [new branch] feat/g -> feat/g"),
      ...bash("g1", T + 1, "gh pr view 3; gh pr comment 3 -b hi", "https://github.com/acme/repo/pull/3#issuecomment-1"),
      ...bash("g2", T + 2, "gh pr merge 4", "https://github.com/z/w/pull/4"),
    ]);
  });

  it("MCP pull request tools, and add_issue_comment only on GitHub's own /pull/ comment URL", async () => {
    await expectParity([
      ...tool("m1", T, "mcp__github__merge_pull_request", { owner: "acme", repo: "repo", pull_number: 14 }, "{\"merged\":true}"),
      ...tool("m2", T + 1, "mcp__github__add_issue_comment", { owner: "acme", repo: "repo", issue_number: 15 },
        JSON.stringify({ html_url: "https://github.com/acme/repo/pull/15#issuecomment-123" })),
      ...tool("m3", T + 2, "mcp__github__add_issue_comment", { owner: "acme", repo: "repo", issue_number: 16 },
        JSON.stringify({ html_url: "https://github.com/acme/repo/issues/16#issuecomment-1", body: "https://github.com/acme/repo/pull/16#issuecomment-2" })),
      ...tool("m4", T + 3, "mcp__github__create_pull_request_review", { owner: "acme", repo: "repo", pullNumber: "17" }, "ok"),
    ]);
  });

  it("commit links contribute github_pr ids", async () => {
    await expectParity([
      line("commit_link", { commit_sha: "deadbeef", match_method: "trailer", created_at_ms: T,
        evidence_json: JSON.stringify({ github_pr: { id: "acme/repo#18", system: "github" } }) }),
      line("commit_link", { commit_sha: "cafebabe", match_method: "trailer", created_at_ms: T + 5,
        evidence_json: JSON.stringify({ github_pr: { id: "acme/repo#19", system: "gitlab" } }) }),
      ...bash("c1", T + 10, "gh pr create", "https://github.com/acme/repo/pull/18"),
    ]);
  });

  it("errored calls and errored / cancelled / signalled results are ignored", async () => {
    const rows = await expectParity([
      line("tool_call", { name: "Bash", tool_use_id: "e1", args_json: JSON.stringify({ command: "gh pr create" }), is_error: "1", ts_ms: T }),
      line("session_event", { role: "tool_result", kind: "tool_result", tool_use_id: "e1", text: "https://github.com/acme/repo/pull/1", ts_ms: T + 1 }),
      line("tool_call", { name: "Bash", tool_use_id: "e2", args_json: JSON.stringify({ command: "gh pr create" }), is_error: 0, ts_ms: T + 2 }),
      line("session_event", { role: "tool_result", kind: "tool_result", tool_use_id: "e2", result_status: "errored", text: "https://github.com/acme/repo/pull/2", ts_ms: T + 3 }),
      line("tool_call", { name: "Bash", tool_use_id: "e3", args_json: JSON.stringify({ command: "gh pr create" }), is_error: 0, ts_ms: T + 4 }),
      line("session_event", { role: "tool_result", kind: "tool_result", tool_use_id: "e3", error_signal: "exit_1", text: "https://github.com/acme/repo/pull/3", ts_ms: T + 5 }),
      ...tool("ok", T + 6, "Bash", { cmd: "gh pr create" }, "https://github.com/acme/repo/pull/4"),
    ]);
    expect(rows.map((r) => r.number)).toEqual([4]);
  });

  it("non-shell tools never match shell kinds, and Neon's target fallback applies", async () => {
    await expectParity([
      ...tool("r1", T, "Read", { command: "gh pr create" }, "https://github.com/acme/repo/pull/1"),
      line("tool_call", { name: "exec_command", tool_use_id: "x1", target: "gh pr create --head tgt", is_error: 0, ts_ms: T + 1 }),
      line("session_event", { role: "tool_result", kind: "tool_result", tool_use_id: "x1", text: "https://github.com/acme/repo/pull/2", ts_ms: T + 2 }),
    ]);
  });

  it("multiple matches in one output and multiple kinds in one call", async () => {
    await expectParity(bash("t1", T, "git push && gh pr create --head feat/h",
      "To github.com:acme/repo.git\n * [new branch] feat/h -> feat/h\nhttps://github.com/acme/repo/pull/51\nhttps://github.com/acme/repo/pull/52"));
  });

  it("an owner:branch head keeps the fork owner apart", async () => {
    await expectParity([
      ...tool("t1", T, "mcp__github__create_pull_request", { owner: "acme", repo: "repo", head: "fork:feat/i" },
        JSON.stringify({ html_url: "https://github.com/acme/repo/pull/61" })),
      ...bash("p1", T + 10, "git push", "To github.com:acme/repo.git\n   1111aaaa..2222bbbb  feat/i -> feat/i"),
      ...bash("p2", T + 20, "git push fork", "To github.com:fork/repo.git\n   1111aaaa..2222bbbb  feat/i -> feat/i"),
    ]);
  });

  it("an empty session and a session with no PR activity", async () => {
    expect(await expectParity([])).toEqual([]);
    expect(await expectParity(bash("t1", T, "ls", "a b"))).toEqual([]);
  });

  it("codex source and a timeless tool settled to its result time", async () => {
    await expectParity([
      line("tool_call", { name: "shell", tool_use_id: "c1", args_json: JSON.stringify({ command: ["gh", "pr", "create"] }), is_error: "false" }),
      line("session_event", { role: "tool_result", kind: "tool_result", tool_use_id: "c1", text: "https://github.com/acme/repo/pull/71", ts_ms: T }),
    ], "codex");
  });
  it("randomized sessions from the 0065 grammar match row for row", async () => {
    let seed = 0x5eed;
    const rand = (n: number) => { seed = (seed * 1103515245 + 12345) & 0x7fffffff; return seed % n; };
    const pick = <T,>(xs: readonly T[]) => xs[rand(xs.length)]!;
    const repos = ["acme/repo", "Acme/Repo", "acme/other", "fork/repo"];
    const branches = ["feat/a", "feat/b", "main", "fork:feat/a", "refs/heads/feat/b"];
    const commands = [
      () => [`gh pr create --head ${pick(branches)}`, `https://github.com/${pick(repos)}/pull/${1 + rand(5)}`],
      () => ["gh pr create -f", `Creating pull request for ${pick(branches)} into main in x\nhttps://github.com/${pick(repos)}/pull/${1 + rand(5)}`],
      () => [`git push origin ${pick(branches)}`, `To github.com:${pick(repos)}.git\n * [new branch] ${pick(branches)} -> ${pick(branches)}`],
      () => ["git push", `To https://github.com/${pick(repos)}\n   1234abcd..5678ef90  x -> ${pick(branches)}`],
      () => [`gh pr ${pick(["comment", "merge", "edit", "checkout", "review"])} ${pick(["#3", "4", pick(branches), `https://github.com/${pick(repos)}/pull/2`])} ${pick(["", "-R acme/repo", "--body 'x 9'", "-b \"https://github.com/z/z/pull/7\""])}`, pick(["ok", `https://github.com/${pick(repos)}/pull/4`])],
      () => ["gh pr ready", `https://github.com/${pick(repos)}/pull/${1 + rand(5)}`],
    ];
    let total = 0;
    for (let session = 0; session < 25; session++) {
      await db.exec(`SET session_replication_role = replica;
        TRUNCATE sessions.delivery_records, sessions.convergence_events, sessions.session_catalog,
          sessions.session_catalog_agents, sessions.session_relationships, sessions.session_markers,
          sessions.session_commit_links, sessions.session_rollups CASCADE;
        SET session_replication_role = origin;`);
      const lines: Line[] = [];
      if (rand(2)) lines.push(line("session", { repo_url: `https://github.com/${pick(repos)}.git` }));
      if (rand(3) === 0) lines.push(line("commit_link", { commit_sha: `c${session}`, match_method: "trailer", created_at_ms: T + rand(50),
        evidence_json: JSON.stringify({ github_pr: { id: `${pick(repos)}#${1 + rand(5)}` } }) }));
      const calls = 1 + rand(6);
      for (let i = 0; i < calls; i++) {
        const [command, out] = pick(commands)();
        if (rand(5) === 0) lines.push(...tool(`m${session}-${i}`, T + i * 10, "mcp__github__create_pull_request",
          { owner: "acme", repo: "repo", head: pick(branches) }, JSON.stringify({ html_url: `https://github.com/acme/repo/pull/${1 + rand(5)}` })));
        else lines.push(...bash(`b${session}-${i}`, T + rand(4) * 10, command!, out!));
      }
      total += (await expectParity(lines)).length;
    }
    expect(total).toBeGreaterThan(25);
  }, 120_000);
});
