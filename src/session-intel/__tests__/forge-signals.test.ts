import { describe, expect, test } from "bun:test";
import { GIT_LOG_FORMAT, parseGitLog } from "../forge/git.ts";
import { collisionNotes, crossIdentityReverts, migrationNumberCollisions, supersededPrs } from "../forge/signals.ts";
import type { Commit, PullRequest } from "../forge/types.ts";

const commit = (sha: string, author: string, at: string, files: Commit["files"] = [], extra: Partial<Commit> = {}): Commit =>
  ({ sha: sha.padEnd(40, "0"), author, authoredAt: at, parents: ["p"], subject: `s-${sha}`, body: "", files, ...extra });
const pr = (number: number, author: string, extra: Partial<PullRequest> = {}): PullRequest => ({
  number, author, title: `t${number}`, body: "", state: "MERGED", headRef: `b${number}`, createdAt: "2026-09-01T00:00:00Z",
  closedAt: "2026-09-02T00:00:00Z", mergedAt: "2026-09-02T00:00:00Z", files: ["a.ts"], comments: [], ...extra,
});

describe("parseGitLog", () => {
  test("reads fields, parents and name-status lines, keeping a rename's new path", () => {
    const raw = `${GIT_LOG_FORMAT.replace("%H", "a".repeat(40)).replace("%an", "Ann").replace("%aI", "2026-09-01T10:00:00+02:00")
      .replace("%P", "p1 p2").replace("%s", "subj").replace("%b", "line1\nline2")}\nA\tx/1.sql\nR100\told.ts\tnew.ts\n`;
    const [c] = parseGitLog(raw);
    expect(c!.authoredAt).toBe("2026-09-01T08:00:00.000Z");
    expect(c!.parents).toEqual(["p1", "p2"]);
    expect(c!.body).toBe("line1\nline2");
    expect(c!.files).toEqual([{ status: "A", path: "x/1.sql" }, { status: "R", path: "new.ts" }]);
  });
});

describe("migrationNumberCollisions", () => {
  test("two names on one number across identities is a cross-identity candidate", () => {
    const out = migrationNumberCollisions("o/r", [
      commit("1", "Ann", "2026-09-01T00:00:00Z", [{ status: "A", path: "db/drizzle/0115_a.sql" }]),
      commit("2", "Bo", "2026-09-02T00:00:00Z", [{ status: "A", path: "db/drizzle/0115_b.sql" }]),
      commit("3", "Bo", "2026-09-03T00:00:00Z", [{ status: "A", path: "db/drizzle/0115_b.sql" }]), // a re-add is not a third side
    ]);
    expect(out).toHaveLength(1);
    expect(out[0]!.crossIdentity).toBe(true);
    expect(out[0]!.sides.map((s) => s.identity)).toEqual(["Ann", "Bo"]);
    expect(out[0]!.harmAt).toBe("2026-09-02T00:00:00Z");
  });
  test("one commit adding two files with one number, merges and non-migration paths are ignored", () => {
    expect(migrationNumberCollisions("o/r", [
      commit("1", "Ann", "2026-09-01T00:00:00Z", [{ status: "A", path: "m/migrations/0041_a.sql" }, { status: "A", path: "m/migrations/0041_b.sql" }]),
      commit("2", "Bo", "2026-09-02T00:00:00Z", [{ status: "A", path: "m/migrations/0042_c.sql" }], { parents: ["x", "y"] }),
      commit("3", "Cy", "2026-09-02T00:00:00Z", [{ status: "A", path: "src/0042_c.sql" }]),
    ])).toEqual([]);
  });
});

describe("crossIdentityReverts", () => {
  test("a revert of another identity's commit, tagged process or manual", () => {
    const orig = commit("a1", "Ann", "2026-09-01T00:00:00Z", [{ status: "M", path: "x.ts" }]);
    const out = crossIdentityReverts("o/r", [orig,
      commit("b1", "Bo", "2026-09-01T02:00:00Z", [], { body: `This reverts commit ${orig.sha.slice(0, 9)}.` }),
      commit("b2", "bot", "2026-09-01T03:00:00Z", [], { subject: "Revert #5 from trunk-train/x: severe review finding", body: `This reverts commit ${orig.sha}.` }),
    ]);
    expect(out.map((c) => c.detail)).toEqual(["manual,repeated", "process,repeated"]);
    expect(out[0]!.artifacts).toEqual(["x.ts"]);
  });
  test("a self-revert is not a candidate unless another identity also reverted it", () => {
    const orig = commit("a1", "Ann", "2026-09-01T00:00:00Z");
    expect(crossIdentityReverts("o/r", [orig, commit("a2", "Ann", "2026-09-01T01:00:00Z", [], { body: `This reverts commit ${orig.sha}.` })])).toEqual([]);
  });
});

describe("supersededPrs", () => {
  test("links a closed PR to the merged PRs its comments name, and skips train heads", () => {
    const prs = [
      pr(10, "bob", { state: "CLOSED", mergedAt: null, comments: [{ author: "k", at: "2026-09-03T00:00:00Z", body: "Closing as superseded by #11 (not #12)." }] }),
      pr(11, "alice"), pr(12, "alice", { state: "OPEN", mergedAt: null }),
      pr(13, "bot", { state: "CLOSED", mergedAt: null, headRef: "trunk-train/1", body: "superseded" }),
    ];
    const out = supersededPrs("o/r", prs, { skipHeadRef: /^trunk-train\// });
    expect(out).toHaveLength(1);
    expect(out[0]!.sides.map((s) => s.ref)).toEqual(["pr:10", "pr:11"]);
    expect(out[0]!.crossIdentity).toBe(true);
  });
  test("a closed PR without supersede language is not a candidate", () => {
    expect(supersededPrs("o/r", [pr(10, "w", { state: "CLOSED", mergedAt: null, body: "stale" })])).toEqual([]);
  });
});

describe("collisionNotes", () => {
  test("a rebase note naming another identity's commit links both sides", () => {
    const theirs = commit("d00dfeed01", "MachineB", "2026-09-01T21:18:10Z");
    const prs = [pr(93, "machine-c", { createdAt: "2026-09-01T16:36:18Z", comments: [
      { author: "alice", at: "2026-09-09T14:43:58Z", body: "### Behaviour collision\n`d00dfee` asserts the exact behaviour this PR changes." },
    ] })];
    const [c] = collisionNotes("o/r", prs, [theirs]);
    expect(c!.sides.map((s) => s.identity)).toEqual(["machine-c", "MachineB"]);
    expect(c!.crossIdentity).toBe(true);
    expect(c!.harmAt).toBe("2026-09-09T14:43:58Z");
  });
  test("ordinary review chatter is not a collision note", () => {
    expect(collisionNotes("o/r", [pr(1, "a", { comments: [{ author: "b", at: "2026-09-02T00:00:00Z", body: "LGTM, deduplicate the helper later" }] })], [])).toEqual([]);
  });
});
