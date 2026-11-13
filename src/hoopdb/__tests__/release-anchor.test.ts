/**
 * The protected release anchor (src/hoopdb/release-target.ts requireReleaseAnchor,
 * releaseAnchorBinding): a held-step target_sha must be the tip of release/schema-<slot>,
 * linear off main through lower anchors only, with an allowlisted diff byte-identical to
 * the release sha, and both the release sha and the anchor proving the same prefix.
 * Real temporary Git repositories; no database.
 */
import { expect, test } from "bun:test";
import { execFileSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { RELEASE_TREE, manifestGeneration, sha256Text } from "../migrate.ts";
import {
  RELEASE_ANCHOR_FIXED_PATHS,
  RELEASE_ANCHOR_WORKFLOW_PATHS,
  releaseAnchorBinding,
  releaseAnchorPathAllowed,
  releaseAnchorWorkflowTransform,
  requireReleaseAnchor,
  treeSchemaSteps,
} from "../release-target.ts";
import { SESSION_BOOTSTRAP_FILES } from "../session-bootstrap-chain.ts";
import { releaseBootstrapFiles } from "../schema-release-bootstrap.ts";
import { run as syncBoundary } from "../../../scripts/sync-sessions-function-boundary.ts";

const git = (repo: string, ...args: string[]) =>
  execFileSync(
    "git",
    ["-C", repo, "-c", "user.name=t", "-c", "user.email=t@example.invalid", "-c", "commit.gpgsign=false", ...args],
    { encoding: "utf8" },
  ).trim();
const code = (f: () => unknown) => {
  try {
    f();
    return null;
  } catch (e) {
    return (e as { code?: string }).code ?? "no_code";
  }
};

const UP = RELEASE_TREE.lanes[0]!.dir;
const HIST = RELEASE_TREE.lanes[1]!.dir;
const up = (slot: string, n = "s") => `upstream:20261105${slot}_${n}.sql`;
const upPath = (slot: string, n = "s") => `${UP}/20261105${slot}_${n}.sql`;
const rbPath = (slot: string, n = "s") => `supabase/rollbacks/20261105${slot}_${n}.down.sql`;
const verifiers = (tag: string) => Object.fromEntries(RELEASE_TREE.verifiers.map((v, i) => [v.path, `-- ${tag} ${i}\n`]));
const bootstrap = (tag: string) => Object.fromEntries(SESSION_BOOTSTRAP_FILES.map((f, i) => [f, `-- ${tag} ${i}\n`]));

/**
 * main: M0 (046000, 047000, 96bulls 0001; bootstrap b0) -> R (+047010, 050000, 050010,
 * 96bulls 0002, the 047010 rollback, migrate-guards; bootstrap b1), the release sha.
 * Anchors: A (parent M0, bootstrap at R's bytes; target 047000) -> B (+047010 and its
 * rollback and migrate-guards at R's bytes; target 047010) -> C (+050000, 050010; target
 * 050010). The B and C step files are R's bytes.
 */
const repo = mkdtempSync(join(tmpdir(), "release-anchor-"));
git(repo, "init", "-q", "-b", "main");
const write = (files: Record<string, string | null>) => {
  for (const [path, content] of Object.entries(files)) {
    const abs = join(repo, path);
    if (content === null) rmSync(abs);
    else {
      mkdirSync(dirname(abs), { recursive: true });
      writeFileSync(abs, content);
    }
  }
};
const commit = (files: Record<string, string | null>, message = "c") => {
  write(files);
  git(repo, "add", "-A");
  git(repo, "commit", "-q", "--allow-empty", "-m", message);
  return git(repo, "rev-parse", "HEAD");
};
const step = (slot: string) => `-- step ${slot}\nSELECT 1;\n`;
/**
 * ci.yml and pg-proof.yml at the main base, in the pre-42797f3c form (push on main only,
 * as at 113dc657), and the exact hunk 42797f3c added: the only form an anchor may carry.
 */
const CI = ".github/workflows/ci.yml";
const PG = ".github/workflows/pg-proof.yml";
const RTH_TEST = "src/session-ingest/__tests__/rth-verifier.test.ts";
const CI_BASE = [
  "name: ci",
  "on:",
  "  pull_request:",
  "  push:",
  "    branches: [main]",
  "permissions:",
  "  contents: read",
  "jobs:",
  "  checks:",
  "    runs-on: ${{ vars.CI_LINUX_RUNNER || 'ubuntu-latest' }}",
  "    steps:",
  "      - uses: actions/checkout@v4",
  "  session-read-worker:",
  "    uses: ./.github/workflows/session-read-worker-checks.yml",
  "",
].join("\n");
const PG_BASE = [
  "name: pg-proof",
  "# Real PostgreSQL (disposable service containers).",
  "on:",
  "  pull_request:",
  "  push:",
  "    branches: [main]",
  "permissions:",
  "  contents: read",
  "jobs:",
  "  real-postgres:",
  "    name: real PostgreSQL ${{ matrix.pg }}",
  "    runs-on: ubuntu-latest",
  "",
].join("\n");
const RELEASE_PUSH = [
  "    # release/schema-*: the protected release anchor branches (a held-step target_sha",
  "    # needs green push runs on its own branch; scripts/schema-release-github.ts).",
  "    branches: [main, 'release/schema-*']",
].join("\n");
/** Written out, not computed by the transform under test. */
const CI_ANCHOR = CI_BASE.replace("    branches: [main]\n", `${RELEASE_PUSH}\n`);
const PG_ANCHOR = PG_BASE.replace("    branches: [main]\n", `${RELEASE_PUSH}\n`);
/** Main moved on after the base (as current main has): the release sha's workflows differ further. */
const CI_MAIN = CI_ANCHOR + "  selfhost-e2e:\n    runs-on: ubuntu-latest\n";
const PG_MAIN = PG_ANCHOR.replace("name: pg-proof\n", "name: pg-proof\n# More proofs.\n");
const M0 = commit({
  [upPath("046000")]: step("046000"),
  [upPath("047000")]: step("047000"),
  [`${HIST}/0001_h.sql`]: "-- h1\n",
  ...verifiers("v0"),
  ...bootstrap("b0"),
  "src/hoopdb/session-bootstrap-chain.ts": readFileSync(new URL("../session-bootstrap-chain.ts", import.meta.url), "utf8"),
  [CI]: CI_BASE,
  [PG]: PG_BASE,
  [RTH_TEST]: "old fixed-clock fixture\n",
  "README.md": "main\n",
}, "M0");
const R = commit({
  [upPath("047010")]: step("047010"),
  [upPath("050000")]: step("050000"),
  [upPath("050010")]: step("050010"),
  [`${HIST}/0002_h.sql`]: "-- h2\n",
  [rbPath("047010")]: "-- rb 047010\n",
  "src/hoopdb/migrate-guards.ts": "export const g = 1;\n",
  ...bootstrap("b1"),
  [CI]: CI_MAIN,
  [PG]: PG_MAIN,
  [RTH_TEST]: "explicit test clock\n",
  "README.md": "main 2\n",
}, "R");
git(repo, "checkout", "-q", "-b", "anchors", M0);
const A = commit({ ...bootstrap("b1"), [RTH_TEST]: "explicit test clock\n" }, "A");
const B = commit({ [upPath("047010")]: step("047010"), [rbPath("047010")]: "-- rb 047010\n", "src/hoopdb/migrate-guards.ts": "export const g = 1;\n" }, "B");
const C = commit({ [upPath("050000")]: step("050000"), [upPath("050010")]: step("050010") }, "C");
git(repo, "checkout", "-q", "main");
const NS = "refs/remotes/origin/";
const setRef = (branch: string, sha: string | null) =>
  sha ? git(repo, "update-ref", `${NS}${branch}`, sha) : git(repo, "update-ref", "-d", `${NS}${branch}`);
setRef("release/schema-047000", A);
setRef("release/schema-047010", B);
setRef("release/schema-050010", C);

/** The release prefix for a set of step names: R's [name, sha256], migrator order. */
const releaseSteps = treeSchemaSteps(repo, R);
const prefixOf = (names: string[]) => releaseSteps.filter(([n]) => names.includes(n));
const PREFIX = {
  A: prefixOf([up("046000"), up("047000"), "96bulls:0001_h.sql"]),
  B: prefixOf([up("046000"), up("047000"), up("047010"), "96bulls:0001_h.sql"]),
  C: prefixOf([up("046000"), up("047000"), up("047010"), up("050000"), up("050010"), "96bulls:0001_h.sql"]),
};
const bind = (targetSha: string, target: string, prefix: [string, string][], r = repo) =>
  releaseAnchorBinding({ repo: r, releaseSha: R, targetSha, target, prefix });
/** A commit with `tree` built from `base`'s tree plus `files`, with the given parents; no checkout. */
const forge = (base: string, files: Record<string, string | null>, parents: string[], message = "forged", mode = "100644") => {
  const index = join(mkdtempSync(join(tmpdir(), "release-anchor-index-")), "index");
  const env = { ...process.env, GIT_INDEX_FILE: index };
  const g = (args: string[], input?: string) =>
    execFileSync("git", ["-C", repo, ...args], { encoding: "utf8", env, input }).trim();
  g(["read-tree", base]);
  for (const [path, content] of Object.entries(files)) {
    if (content === null) g(["update-index", "--force-remove", "--", path]);
    else g(["update-index", "--add", "--cacheinfo", `${mode},${g(["hash-object", "-w", "--stdin"], content)},${path}`]);
  }
  const tree = g(["write-tree"]);
  return execFileSync(
    "git",
    ["-C", repo, "commit-tree", tree, ...parents.flatMap((p) => ["-p", p]), "-m", message],
    { encoding: "utf8", env: { ...process.env, GIT_AUTHOR_NAME: "t", GIT_AUTHOR_EMAIL: "t@example.invalid", GIT_COMMITTER_NAME: "t", GIT_COMMITTER_EMAIL: "t@example.invalid" } },
  ).trim();
};
/** Points release/schema-<slot> at `sha` for the duration of `f`, then restores the original tip. */
const withRef = <T>(branch: string, sha: string | null, f: () => T): T => {
  const before = (() => {
    try {
      return git(repo, "rev-parse", "--verify", "--quiet", `${NS}${branch}`);
    } catch {
      return null;
    }
  })();
  setRef(branch, sha);
  try {
    return f();
  } finally {
    setRef(branch, before);
  }
};

test("A -> B -> C: each anchor binds its own prefix; B and C chain through the lower anchors to main base M0", () => {
  const a = bind(A, up("047000"), PREFIX.A);
  expect(a.targetSha).toBe(A);
  expect(a.manifest.steps).toEqual(PREFIX.A);
  // Non-circular: the generation from the committed texts, not from treeSchemaSteps.
  const verifierPairs = RELEASE_TREE.verifiers.map((v, i) => [v.name, sha256Text(`-- v0 ${i}\n`)] as [string, string]);
  expect(a.targetGeneration).toBe(
    manifestGeneration({
      targetSha: A,
      target: up("047000"),
      steps: [
        [up("046000"), sha256Text(step("046000"))],
        [up("047000"), sha256Text(step("047000"))],
        ["96bulls:0001_h.sql", sha256Text("-- h1\n")],
      ],
      verifiers: verifierPairs,
    }),
  );
  expect(requireReleaseAnchor({ repo, releaseSha: R, targetSha: A, target: up("047000") })).toEqual({
    branch: "release/schema-047000",
    mainBase: M0,
    below: [],
  });
  expect(bind(B, up("047010"), PREFIX.B).manifest.steps).toEqual(PREFIX.B);
  expect(requireReleaseAnchor({ repo, releaseSha: R, targetSha: B, target: up("047010") })).toEqual({
    branch: "release/schema-047010",
    mainBase: M0,
    below: ["release/schema-047000"],
  });
  expect(bind(C, up("050010"), PREFIX.C).manifest.steps).toEqual(PREFIX.C);
  expect(requireReleaseAnchor({ repo, releaseSha: R, targetSha: C, target: up("050010") })).toEqual({
    branch: "release/schema-050010",
    mainBase: M0,
    below: ["release/schema-047010", "release/schema-047000"],
  });
  // The bootstrap stage (mode=bootstrap) reads A, B and C's chain files at the release bytes.
  for (const anchorSha of [A, B, C])
    for (const f of releaseBootstrapFiles(repo, R, anchorSha)) expect(f.target?.equals(f.release!)).toBe(true);
  // Nothing held: the release sha is its own anchor and needs no branch.
  expect(bind(R, "96bulls:0002_h.sql", releaseSteps).targetSha).toBe(R);
});

test("anchor ref: missing, pointing elsewhere, or a slot the release tree names twice refuses", () => {
  withRef("release/schema-047000", null, () =>
    expect(code(() => bind(A, up("047000"), PREFIX.A))).toBe("release_anchor_ref_missing"),
  );
  // The ref moved (to B, or to a twin of A with the same tree): target_sha is not its tip.
  withRef("release/schema-047000", B, () =>
    expect(code(() => bind(A, up("047000"), PREFIX.A))).toBe("release_anchor_ref_mismatch"),
  );
  const twin = forge(A, {}, [M0], "A twin");
  withRef("release/schema-047000", twin, () => {
    expect(code(() => bind(A, up("047000"), PREFIX.A))).toBe("release_anchor_ref_mismatch");
    // The twin itself, now the tip, binds its own generation: the gate pins the commit by sha.
    expect(bind(twin, up("047000"), PREFIX.A).targetSha).toBe(twin);
  });
  // Only refs under the release namespace count: a local branch of that name does not.
  git(repo, "branch", "release/schema-047000", A);
  withRef("release/schema-047000", null, () =>
    expect(code(() => bind(A, up("047000"), PREFIX.A))).toBe("release_anchor_ref_missing"),
  );
  git(repo, "branch", "-D", "release/schema-047000");
  // An annotated tag object at the ref is not a commit tip.
  git(repo, "tag", "-a", "-m", "t", "anchor-tag", A);
  const tag = git(repo, "rev-parse", "anchor-tag");
  withRef("release/schema-047000", tag, () =>
    expect(code(() => bind(A, up("047000"), PREFIX.A))).toBe("release_anchor_ref_mismatch"),
  );
  git(repo, "tag", "-d", "anchor-tag");
  // A 6-digit slot the release tree names twice cannot anchor.
  const dup = mkdtempSync(join(tmpdir(), "release-anchor-dup-"));
  execFileSync("git", ["clone", "-q", "--no-local", repo, dup]);
  git(dup, "checkout", "-q", "main");
  writeFileSync(join(dup, `${UP}/20261101047000_s.sql`), "-- older date, same slot\n");
  git(dup, "add", "-A");
  git(dup, "commit", "-q", "-m", "dup");
  const R2 = git(dup, "rev-parse", "HEAD");
  git(dup, "update-ref", `${NS}release/schema-047000`, A);
  expect(code(() => requireReleaseAnchor({ repo: dup, releaseSha: R2, targetSha: A, target: up("047000") }))).toBe("release_anchor_ref_ambiguous");
});

test("linear chain: a merge anchor, a root anchor, a side parent, a parent that is no lower anchor, or a higher anchor below refuses", () => {
  // A merge with A's tree (side parent R): nonlinear.
  const merge = forge(A, {}, [M0, R], "merge");
  withRef("release/schema-047000", merge, () =>
    expect(code(() => bind(merge, up("047000"), PREFIX.A))).toBe("release_anchor_nonlinear"),
  );
  // A root commit (no parent).
  const root = forge(A, {}, [], "root");
  withRef("release/schema-047000", root, () =>
    expect(code(() => bind(root, up("047000"), PREFIX.A))).toBe("release_anchor_nonlinear"),
  );
  // B rebuilt on an unreferenced copy of A: its parent is neither main nor an anchor tip.
  const loose = forge(A, {}, [M0], "unreferenced A");
  const bLoose = forge(B, {}, [loose], "B on loose");
  withRef("release/schema-047010", bLoose, () =>
    expect(code(() => bind(bLoose, up("047010"), PREFIX.B))).toBe("release_anchor_chain"),
  );
  // B with a merged intermediate below it: nonlinear.
  const mergedA = forge(A, {}, [M0, R], "merged A");
  const bOnMerge = forge(B, {}, [mergedA], "B on merge");
  withRef("release/schema-047000", mergedA, () =>
    withRef("release/schema-047010", bOnMerge, () =>
      expect(code(() => bind(bOnMerge, up("047010"), PREFIX.B))).toBe("release_anchor_nonlinear"),
    ),
  );
  // A below B, but A's tip carried by a HIGHER slot's ref only (050010): not below 047010.
  withRef("release/schema-047000", null, () =>
    withRef("release/schema-050010", A, () =>
      expect(code(() => bind(B, up("047010"), PREFIX.B))).toBe("release_anchor_chain"),
    ),
  );
  // C stacked out of order: C's chain must strictly descend (047010's ref at C's parent is
  // fine; 050010 cannot sit below 047010).
  const bOnC = forge(B, { [upPath("050000")]: step("050000"), [upPath("050010")]: step("050010") }, [C], "B above C");
  withRef("release/schema-047010", bOnC, () =>
    expect(code(() => bind(bOnC, up("047010"), PREFIX.B))).toBe("release_anchor_chain"),
  );
  // target_sha on main itself is not an anchor.
  withRef("release/schema-047000", M0, () =>
    expect(code(() => bind(M0, up("047000"), PREFIX.A))).toBe("release_anchor_chain"),
  );
  // A side commit off an older main commit is fine only through main's first-parent
  // history: an anchor based on R's side parent is refused (not on main).
  const side = forge(M0, { "x.txt": "x" }, [M0], "side");
  const aOnSide = forge(A, {}, [side], "A on side");
  withRef("release/schema-047000", aOnSide, () =>
    expect(code(() => bind(aOnSide, up("047000"), PREFIX.A))).toBe("release_anchor_chain"),
  );
});

test("allowlisted, reproducible diff: an extra file, a tampered or re-moded file, or a rollback outside the prefix refuses", () => {
  for (const [label, files, expected] of [
    ["extra file", { "README.md": "anchor\n" }, "release_anchor_extra_file"],
    ["extra test file", { "src/hoopdb/__tests__/x.test.ts": "x\n" }, "release_anchor_extra_file"],
    ["nested lane path", { [`${UP}/nested/20261105047000_s.sql`]: "x\n" }, "release_anchor_extra_file"],
    ["rollback of a held step", { [rbPath("050000")]: "-- rb\n" }, "release_anchor_extra_file"],
    ["tampered bootstrap file", { [SESSION_BOOTSTRAP_FILES[2]]: "-- tampered\n" }, "release_anchor_file_drift"],
    ["bootstrap file left at main base bytes", bootstrap("b0"), "release_anchor_file_drift"],
    ["tampered step", { [upPath("047000")]: step("047000") + "-- tampered\n" }, "release_anchor_file_drift"],
    ["verifier differing from the release", { [RELEASE_TREE.verifiers[0]!.path]: "-- other\n" }, "release_anchor_file_drift"],
    ["sentinels absent at the release", { "src/hoopdb/migration-sentinels.ts": "x\n" }, "release_anchor_file_drift"],
    ["test clock fix differing from the release", { [RTH_TEST]: "tampered\n" }, "release_anchor_file_drift"],
  ] as [string, Record<string, string>, string][]) {
    const forged = forge(A, files, [M0], label);
    withRef("release/schema-047000", forged, () =>
      expect([label, code(() => bind(forged, up("047000"), PREFIX.A))]).toEqual([label, expected]),
    );
  }
  // A mode change alone (same bytes, executable) is drift.
  const index = join(mkdtempSync(join(tmpdir(), "release-anchor-index-")), "index");
  const env = { ...process.env, GIT_INDEX_FILE: index };
  execFileSync("git", ["-C", repo, "read-tree", A], { env });
  execFileSync("git", ["-C", repo, "update-index", "--chmod=+x", "--", SESSION_BOOTSTRAP_FILES[0]], { env });
  const tree = execFileSync("git", ["-C", repo, "write-tree"], { env, encoding: "utf8" }).trim();
  const exec = execFileSync("git", ["-C", repo, "commit-tree", tree, "-p", M0, "-m", "mode"], {
    encoding: "utf8",
    env: { ...process.env, GIT_AUTHOR_NAME: "t", GIT_AUTHOR_EMAIL: "t@example.invalid", GIT_COMMITTER_NAME: "t", GIT_COMMITTER_EMAIL: "t@example.invalid" },
  }).trim();
  withRef("release/schema-047000", exec, () => expect(code(() => bind(exec, up("047000"), PREFIX.A))).toBe("release_anchor_file_drift"));
  // A missing verifier is its own refusal, before the diff.
  const noVerifier = forge(A, { [RELEASE_TREE.verifiers[3]!.path]: null }, [M0], "no verifier");
  withRef("release/schema-047000", noVerifier, () =>
    expect(code(() => bind(noVerifier, up("047000"), PREFIX.A))).toBe("release_target_verifier_missing"),
  );
});

test("workflow transform: the one push-selector line becomes main's 42797f3c hunk; any other base shape refuses", () => {
  expect(releaseAnchorWorkflowTransform(CI_BASE)).toBe(CI_ANCHOR);
  expect(releaseAnchorWorkflowTransform(PG_BASE)).toBe(PG_ANCHOR);
  expect(RELEASE_ANCHOR_WORKFLOW_PATHS).toEqual([CI, PG]);
  const drift = (base: string) => code(() => releaseAnchorWorkflowTransform(base));
  for (const [label, base] of [
    ["no push selector", CI_BASE.replace("    branches: [main]\n", "")],
    ["already transformed (current main)", CI_ANCHOR],
    ["selector twice", CI_BASE.replace("  pull_request:\n", "  pull_request:\n    branches: [main]\n")],
    ["selector under pull_request only", CI_BASE.replace("  pull_request:\n  push:\n    branches: [main]\n", "  push:\n  pull_request:\n    branches: [main]\n")],
    ["push not in the top-level on block", CI_BASE.replace("on:\n  pull_request:\n  push:\n    branches: [main]\n", "on:\n  pull_request:\nx:\n  push:\n    branches: [main]\n")],
    ["other indentation", CI_BASE.replace("    branches: [main]", "      branches: [main]")],
    ["CRLF base", CI_BASE.replaceAll("\n", "\r\n")],
    ["empty", ""],
  ] as [string, string][])
    expect([label, drift(base)]).toEqual([label, "release_anchor_workflow_drift"]);
});

test("workflow files: an anchor may carry ci.yml and pg-proof.yml only as transform(main base), mode 100644", () => {
  // The two transformed files (or just one) pass, though the release sha's copies differ:
  // they are bound to the main base, not the release sha.
  const wf = forge(A, { [CI]: CI_ANCHOR, [PG]: PG_ANCHOR }, [M0], "A with workflows");
  const ciOnly = forge(A, { [CI]: CI_ANCHOR }, [M0], "A with ci.yml");
  for (const sha of [wf, ciOnly])
    withRef("release/schema-047000", sha, () => {
      expect(bind(sha, up("047000"), PREFIX.A).targetSha).toBe(sha);
      expect(requireReleaseAnchor({ repo, releaseSha: R, targetSha: sha, target: up("047000") }).mainBase).toBe(M0);
    });
  // Stacked: B on top of the workflow anchor still diffs against M0 and passes.
  const bOnWf = forge(B, { [CI]: CI_ANCHOR, [PG]: PG_ANCHOR }, [wf], "B on workflow A");
  withRef("release/schema-047000", wf, () =>
    withRef("release/schema-047010", bOnWf, () => expect(bind(bOnWf, up("047010"), PREFIX.B).targetSha).toBe(bOnWf)),
  );
  // Unchanged workflows (A itself) are not in the diff at all.
  expect(bind(A, up("047000"), PREFIX.A).targetSha).toBe(A);
  const tweak = (text: string, from: string, to: string) => {
    expect(text).toContain(from);
    return text.replace(from, to);
  };
  for (const [label, files] of [
    ["ci.yml copied from the release sha (current main)", { [CI]: CI_MAIN, [PG]: PG_ANCHOR }],
    ["pg-proof.yml copied from the release sha", { [CI]: CI_ANCHOR, [PG]: PG_MAIN }],
    ["other branch glob", { [CI]: tweak(CI_ANCHOR, "[main, 'release/schema-*']", "[main, 'release/*']") }],
    ["branches line without the comments", { [CI]: CI_BASE.replace("[main]", "[main, 'release/schema-*']") }],
    ["comment reworded", { [CI]: tweak(CI_ANCHOR, "protected release anchor", "release anchor") }],
    ["job renamed", { [CI]: tweak(CI_ANCHOR, "  checks:\n", "  check:\n") }],
    ["permissions widened", { [CI]: tweak(CI_ANCHOR, "contents: read", "contents: write") }],
    ["runner changed", { [PG]: tweak(PG_ANCHOR, "runs-on: ubuntu-latest", "runs-on: self-hosted") }],
    ["action version changed", { [CI]: tweak(CI_ANCHOR, "actions/checkout@v4", "actions/checkout@v5") }],
    ["event added", { [CI]: tweak(CI_ANCHOR, "  pull_request:\n", "  pull_request:\n  workflow_dispatch:\n") }],
    ["trailing newline dropped", { [CI]: CI_ANCHOR.slice(0, -1) }],
    ["CRLF anchor", { [CI]: CI_ANCHOR.replaceAll("\n", "\r\n") }],
    ["workflow deleted", { [CI]: null }],
    ["third workflow file", { [CI]: CI_ANCHOR, [PG]: PG_ANCHOR, ".github/workflows/other.yml": CI_ANCHOR }],
    ["third workflow file alone", { ".github/workflows/session-read-worker-checks.yml": "name: x\n" }],
  ] as [string, Record<string, string | null>][]) {
    const forged = forge(A, files, [M0], label);
    withRef("release/schema-047000", forged, () =>
      expect([label, code(() => bind(forged, up("047000"), PREFIX.A))]).toEqual([label, "release_anchor_workflow_drift"]),
    );
  }
  // Mode: the transformed bytes, executable.
  const exec = forge(A, { [CI]: CI_ANCHOR }, [M0], "executable ci.yml", "100755");
  withRef("release/schema-047000", exec, () => expect(code(() => bind(exec, up("047000"), PREFIX.A))).toBe("release_anchor_workflow_drift"));
  // A main base without the main-only selector: nothing an anchor carries can match.
  const other = mkdtempSync(join(tmpdir(), "release-anchor-wf-base-"));
  execFileSync("git", ["clone", "-q", "--no-local", repo, other]);
  git(other, "checkout", "-q", "main");
  writeFileSync(join(other, CI), CI_BASE.replace("[main]", "[main, develop]"));
  git(other, "commit", "-q", "-am", "main base with another push selector");
  const R3 = git(other, "rev-parse", "HEAD");
  for (const text of [CI_ANCHOR, CI_BASE.replace("[main]", "[main, develop, 'release/schema-*']")]) {
    git(other, "checkout", "-q", "--detach", R3);
    writeFileSync(join(other, CI), text);
    git(other, "commit", "-q", "-am", "anchor");
    const anchor = git(other, "rev-parse", "HEAD");
    git(other, "update-ref", `${NS}release/schema-047000`, anchor);
    expect(code(() => requireReleaseAnchor({ repo: other, releaseSha: R3, targetSha: anchor, target: up("047000") }))).toBe(
      "release_anchor_workflow_drift",
    );
  }
});

test("prefix proof: the release sha and the anchor must both prove the prefix; no step above the target in the anchor", () => {
  // A prefix that skips a release step below the target, or carries other bytes.
  expect(code(() => bind(B, up("047010"), PREFIX.B.filter(([n]) => n !== up("047000"))))).toBe("release_anchor_prefix_mismatch");
  expect(code(() => bind(A, up("047000"), PREFIX.A.map(([n, d]) => [n, n === up("046000") ? "0".repeat(64) : d] as [string, string])))).toBe(
    "release_anchor_prefix_mismatch",
  );
  // A prefix without the target itself.
  expect(code(() => bind(A, up("047000"), PREFIX.A.filter(([n]) => n !== up("047000"))))).toBe("release_anchor_prefix_mismatch");
  // An anchor carrying a step above its target (050000 at release bytes on the 047010 slot): tail.
  const tall = forge(B, { [upPath("050000")]: step("050000") }, [A], "B with 050000");
  withRef("release/schema-047010", tall, () => expect(code(() => bind(tall, up("047010"), PREFIX.B))).toBe("release_target_tail"));
  // C itself on the 047010 slot: its parent is B, the same slot, not a lower anchor.
  withRef("release/schema-047010", C, () => expect(code(() => bind(C, up("047010"), PREFIX.B))).toBe("release_anchor_chain"));
  // An anchor without the target step file at all.
  const thin = forge(A, {}, [A], "thin B");
  withRef("release/schema-047010", thin, () =>
    expect(["release_target_tail", "release_target_tree_mismatch"]).toContain(code(() => bind(thin, up("047010"), PREFIX.B))),
  );
  // A rollback for the target without its step: not allowlisted.
  const rbOnly = forge(A, { [rbPath("047010")]: "-- rb 047010\n" }, [A], "rollback only");
  withRef("release/schema-047010", rbOnly, () => expect(code(() => bind(rbOnly, up("047010"), PREFIX.B))).toBe("release_anchor_extra_file"));
});

test("a shallow clone without the anchor's history refuses (never a fetch)", () => {
  const shallow = mkdtempSync(join(tmpdir(), "release-anchor-shallow-"));
  // Depth 1: C (the tip of the "anchors" branch) is present, its parent B is not.
  execFileSync("git", ["clone", "-q", "--depth", "1", "--no-single-branch", `file://${repo}`, shallow]);
  git(shallow, "update-ref", `${NS}release/schema-050010`, C);
  expect(code(() => bind(C, up("050010"), PREFIX.C, shallow))).toBe("release_target_sha_unknown");
});

test("the allowlist covers every function-boundary generator output and the bootstrap/verifier inputs, nothing broader", async () => {
  const { generated } = await syncBoundary({ check: true });
  const all = new Set(treeSchemaStepsAll());
  for (const path of Object.keys(generated.outputs)) expect([path, releaseAnchorPathAllowed(path, all)]).toEqual([path, true]);
  for (const f of [...SESSION_BOOTSTRAP_FILES, ...RELEASE_TREE.verifiers.map((v) => v.path)])
    expect(RELEASE_ANCHOR_FIXED_PATHS).toContain(f);
  expect(RELEASE_ANCHOR_FIXED_PATHS).toContain(RTH_TEST);
  for (const path of [
    "README.md",
    "package.json",
    "bun.lock",
    ".github/workflows/ci.yml",
    "scripts/schema-release-database.ts",
    "src/hoopdb/release-target.ts",
    "src/hoopdb/migrate.ts",
    "supabase/bootstrap/other.sql",
    "supabase/tests/supabase_shim.sql",
    `${UP}/x/20261105047000_s.sql`,
    `${UP}/20261105047000_s.txt`,
    "scripts/sessions-function-boundary/lanes/nested/x.ts",
    "supabase/rollbacks/20261105047000_s.down.sql",
  ])
    expect([path, releaseAnchorPathAllowed(path, new Set())]).toEqual([path, false]);
  expect(releaseAnchorPathAllowed("supabase/rollbacks/20261105047000_s.down.sql", new Set([up("047000")]))).toBe(true);
});

/** Every schema step name of this checkout (rollbacks of all of them allowed for the coverage check). */
function treeSchemaStepsAll(): string[] {
  const names: string[] = [];
  for (const lane of RELEASE_TREE.lanes)
    for (const f of execFileSync("git", ["ls-files", "--", `${lane.dir}/*.sql`], { encoding: "utf8" }).split("\n").filter(Boolean))
      names.push(`${lane.prefix}${f.slice(lane.dir.length + 1)}`);
  return names;
}
