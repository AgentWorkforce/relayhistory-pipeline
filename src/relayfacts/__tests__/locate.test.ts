/** Repository attribution from canonical evidence, on synthetic rows only. */
import { describe, expect, test } from "bun:test";
import { attributeRepo, attributionInRepos, repoNameFromCwd, slugFromRemote } from "../backfill/locate.ts";
import { rowInRepo } from "../backfill/cli.ts";

describe("slugFromRemote", () => {
  test("ssh, https and credential-bearing remotes give owner/repo; redacted or junk gives null", () => {
    expect(slugFromRemote("git@github.com:Acme/Widgets.git")).toBe("acme/widgets");
    expect(slugFromRemote("https://github.com/acme/widgets")).toBe("acme/widgets");
    expect(slugFromRemote("https://x-access-token:secret@github.com/acme/widgets.git")).toBe("acme/widgets");
    expect(slugFromRemote("acme/widgets")).toBe("acme/widgets");
    expect(slugFromRemote("[REDACTED]")).toBeNull();
    expect(slugFromRemote("/home/someone/widgets")).toBeNull();
  });
});

describe("repoNameFromCwd", () => {
  test("repository directory by layout, never a generic folder", () => {
    expect(repoNameFromCwd("/Users/someone/Projects/Acme/widgets")).toBe("widgets");
    expect(repoNameFromCwd("/Users/someone/Projects/Acme/widgets/.claude/worktrees/fix-1")).toBe("widgets");
    expect(repoNameFromCwd("/Volumes/T7/Projects/Acme/worktrees/widgets/fix-1")).toBe("widgets");
    expect(repoNameFromCwd("/home/someone/work/workers")).toBeNull();
    expect(repoNameFromCwd(null)).toBeNull();
  });
});

describe("attributeRepo", () => {
  test("a catalog remote beats event slugs; event slugs beat the path", () => {
    const rows = [
      { table: "session_catalog", row: { repo_url: null, project_key: "github.com/acme/widgets", project_key_method: "remote", cwd: "/x/gadgets", git_branch: "feat/a" } },
      { table: "convergence_events", row: { project_id: "acme/other" } },
    ];
    expect(attributeRepo(rows)).toEqual({ repo: "acme/widgets", basis: "remote", repoName: "gadgets", branch: "feat/a" });
    expect(attributeRepo(rows.slice(1))).toMatchObject({ repo: "acme/other", basis: "events" });
  });
  test("a redacted remote key falls through to the events", () => {
    const rows = [
      { table: "session_catalog", row: { project_key: "[REDACTED]", project_key_method: "remote", cwd: "/x/widgets" } },
      { table: "convergence_events", row: { project_id: "acme/widgets" } },
      { table: "convergence_events", row: { project_id: "acme/widgets" } },
      { table: "convergence_events", row: { project_id: "acme/rare" } },
    ];
    expect(attributeRepo(rows)).toMatchObject({ repo: "acme/widgets", basis: "events" });
  });
  test("no catalog: the events' path stands in for the working directory, as a name only", () => {
    const a = attributeRepo([{ table: "convergence_events", row: { project_id: "/home/someone/src/widgets" } }]);
    expect(a).toEqual({ repo: null, basis: "path", repoName: "widgets", branch: null });
    expect(attributionInRepos(a, ["acme/widgets"])).toBe(true);
    expect(attributionInRepos(a, ["acme/gadgets"])).toBe(false);
  });
  test("nothing at all", () => {
    expect(attributeRepo([])).toEqual({ repo: null, basis: "none", repoName: null, branch: null });
  });
});

describe("rowInRepo", () => {
  test("a renamed repository matches under either name", () => {
    const repos = ["acme/widgets", "acme/widgets-old"];
    expect(rowInRepo({ gitRepo: "git@github.com:acme/widgets-old.git", projectId: null }, repos)).toBe(true);
    expect(rowInRepo({ gitRepo: null, projectId: "Acme/widgets" }, repos)).toBe(true);
    expect(rowInRepo({ gitRepo: null, projectId: null }, repos)).toBe(false);
  });
});
