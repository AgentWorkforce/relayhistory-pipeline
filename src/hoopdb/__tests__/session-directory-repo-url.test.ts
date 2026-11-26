/** The directory's repository URL: credentials never pass, and a remote-derived project key fills a missing URL. */
import { describe, expect, test } from "bun:test";
import { directoryCatalogFromProjection, repoUrlFromProjectKey, safeRepoUrl } from "../session-directory-catalog.ts";

describe("safeRepoUrl", () => {
  test("credential-bearing URLs lose their userinfo and keep host and path", () => {
    expect(safeRepoUrl("https://x-access-token:ghs_secretvalue@github.com/acme/widgets.git")).toBe("https://github.com/acme/widgets.git");
    expect(safeRepoUrl("https://ghp_token@github.com/acme/widgets")).toBe("https://github.com/acme/widgets");
    expect(safeRepoUrl("ssh://git:secret@github.com/acme/widgets.git")).toBe("ssh://github.com/acme/widgets.git");
  });
  test("forms without a credential are kept", () => {
    expect(safeRepoUrl("git@github.com:acme/widgets.git")).toBe("git@github.com:acme/widgets.git");
    expect(safeRepoUrl("ssh://git@github.com/acme/widgets.git")).toBe("ssh://git@github.com/acme/widgets.git");
    expect(safeRepoUrl("https://github.com/acme/widgets.git")).toBe("https://github.com/acme/widgets.git");
    expect(safeRepoUrl("github.com/acme/widgets")).toBe("github.com/acme/widgets");
  });
  test("scrubbed, local, parameterised or unparseable values give null", () => {
    for (const v of ["[REDACTED]", "https://github.com/[REDACTED]", "/home/someone/widgets", "file:///home/someone/widgets", "https://github.com/acme/widgets?token=abc", "https://github.com/acme/widgets#x", "not a url", "ftp://github.com/acme/widgets"])
      expect(safeRepoUrl(v)).toBeNull();
    expect(safeRepoUrl(null)).toBeNull();
  });
});

describe("repoUrlFromProjectKey", () => {
  test("only a remote-derived host/owner/repo key becomes a URL", () => {
    expect(repoUrlFromProjectKey("github.com/acme/widgets", "remote")).toBe("https://github.com/acme/widgets");
    expect(repoUrlFromProjectKey("github.com/acme/widgets", "git_remote")).toBe("https://github.com/acme/widgets");
    expect(repoUrlFromProjectKey("github.com/acme/widgets", "path")).toBeNull();
    expect(repoUrlFromProjectKey("[REDACTED]", "remote")).toBeNull();
    expect(repoUrlFromProjectKey("acme/widgets", "remote")).toBeNull();
  });
});

describe("directoryCatalogFromProjection gitRepo", () => {
  const catalog = (row: Record<string, unknown>) => directoryCatalogFromProjection([{ table: "session_catalog", row: { models: [], ...row } }]);
  test("a credential in the recorded URL never reaches the directory", () => {
    expect(catalog({ repo_url: "https://x-access-token:ghs_secretvalue@github.com/acme/widgets.git" }).gitRepo).toBe("https://github.com/acme/widgets.git");
  });
  test("no URL but a remote project key: the key fills it", () => {
    expect(catalog({ repo_url: null, project_key: "github.com/acme/widgets", project_key_method: "remote" }).gitRepo).toBe("https://github.com/acme/widgets");
  });
  test("a scrubbed URL falls back to the key, and a scrubbed key gives nothing", () => {
    expect(catalog({ repo_url: "[REDACTED]", project_key: "github.com/acme/widgets", project_key_method: "remote" }).gitRepo).toBe("https://github.com/acme/widgets");
    expect(catalog({ repo_url: null, project_key: "[REDACTED]", project_key_method: "remote" }).gitRepo).toBeNull();
  });
  test("a path-derived key never becomes a repository", () => {
    expect(catalog({ repo_url: null, project_key: "/home/someone/widgets", project_key_method: "path" }).gitRepo).toBeNull();
  });
});
