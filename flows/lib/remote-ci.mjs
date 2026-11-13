// GitHub Actions half of flows/ci.flow.ts. Adapted from relay-app's
// flows/lib/remote-ci.mjs (flows/README.md): the snapshot, the sweep and the
// wait are unchanged; what is dispatched and the pre-push checks are this repo's.
//
//   node flows/lib/remote-ci.mjs watch <base-sha> <workflow file>[,<workflow file>...]
//
// Work lands on main as one squash commit and this repository takes no pull
// requests (AGENTS.md), so there is no branch to run CI on. Instead:
//
// 1. Snapshot the working tree as a commit without touching HEAD, the index
//    or the files: tracked edits plus whatever is staged, so stage a new file
//    for GitHub to see it. Untracked files are never pushed.
// 2. Refuse to push a snapshot that holds machine-local state, a secret-like
//    file, or a production identifier (the repository is public), then push it
//    to a throwaway ci/<sha> branch and dispatch the workflows there
//    (workflow_dispatch; ci.yml and pg-proof.yml carry the trigger). GitHub
//    dispatches a workflow by file name only once its trigger is on the default
//    branch, so the change that adds the trigger is gated by main's own push run.
// 3. Wait (up to two hours) for exactly the dispatched runs, print the failed
//    jobs' log tails and then one verdict line per run, delete the branch, and
//    exit 1 unless every run passed. A SIGTERM or SIGINT (the fleet stopping
//    this step) deletes the branch too, and each watch first sweeps snapshot
//    branches an earlier, harder kill left behind.
import { execFileSync } from "node:child_process";
import { copyFileSync, existsSync, mkdtempSync, rmSync, statSync, utimesSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { MACHINE_LOCAL } from "./changes.ts";

// Failed-job logs run to megabytes; the default 1 MB buffer would throw them away.
const sh = (cmd, args, env) =>
  execFileSync(cmd, args, {
    encoding: "utf8", stdio: ["ignore", "pipe", "pipe"], env: { ...process.env, ...env }, maxBuffer: 256 * 1024 * 1024,
  }).trim();
const git = (args, env) => sh("git", args, env);
const gh = (...args) => sh("gh", args);
const sleep = (ms) => new Promise((done) => setTimeout(done, ms));
// The listing is polled for up to two hours; a few failures in a row are a
// passing API hiccup, not a verdict.
const LISTING_RETRIES = 10;

/** The workflows the gate may dispatch, by file, and the run name GitHub lists each under. */
export const WORKFLOWS = { "ci.yml": "ci", "pg-proof.yml": "pg-proof" };

/**
 * A commit of HEAD plus the working tree's tracked and staged changes, and
 * any untracked, non-ignored files under `include`; HEAD, the index and the
 * files are untouched.
 */
export function snapshot(include = []) {
  const scratch = mkdtempSync(join(tmpdir(), "relay-ci-index-"));
  try {
    const index = join(scratch, "index");
    const real = git(["rev-parse", "--git-path", "index"]);
    if (existsSync(real)) {
      copyFileSync(real, index);
      // Keep the index's own timestamp: git trusts an entry's stat data only
      // when the file is older than the index, so a fresh copy would hide an
      // edit made in the same instant the real index was written.
      const { atime, mtime } = statSync(real);
      utimesSync(index, atime, mtime);
    }
    const env = { GIT_INDEX_FILE: index };
    git(["add", "--update", "--", "."], env);
    const extra = include.length
      ? git(["ls-files", "--others", "--exclude-standard", "-z", "--", ...include], env).split("\0").filter(Boolean)
      : [];
    if (extra.length) git(["add", "--", ...extra], env);
    const tree = git(["write-tree"], env);
    return git(["commit-tree", tree, "-p", "HEAD", "-m", "CI snapshot (flows/ci.flow.ts)"]);
  } finally {
    rmSync(scratch, { recursive: true, force: true });
  }
}

/** ci.yml's "nothing secret-like committed" guard, widened: env files, keys, credentials files. */
export const SECRET_LIKE = /(^|\/)(\.env(\.[^/]*)?|inputs\.json|[^/]*\.(pem|key|p12|pfx)|id_(rsa|dsa|ecdsa|ed25519)|\.npmrc|\.netrc|\.pypirc|credentials(\.json)?)$/;
const SECRET_ALLOWED = /(^|\/)\.env\.example$/;

/**
 * Why a snapshot must not leave this machine, or "" when it may. The remote
 * is public (and a sandbox upload leaves too): machine-local state and
 * secret-like files never go; the engine roots must pass the production
 * identifier guard; and no line the snapshot adds over `base`, in any file,
 * may carry a production identifier. The guard is gate code (publish-guard.mjs
 * beside this file), never the snapshot's own script.
 */
export async function refuseToPublish(sha, base, root = ".") {
  const files = git(["-C", root, "ls-tree", "-r", "--name-only", sha]).split("\n");
  const local = files.filter((file) => MACHINE_LOCAL.some((dir) => file === dir || file.startsWith(`${dir}/`) || file.includes(`/${dir}/`)));
  if (local.length) return `the snapshot holds machine-local state (${local.slice(0, 5).join(", ")}); unstage it`;
  const secret = files.filter((file) => SECRET_LIKE.test(file) && !SECRET_ALLOWED.test(file));
  if (secret.length) return `the snapshot holds secret-like files (${secret.slice(0, 5).join(", ")}); unstage them`;
  try {
    const { addedHits } = await import("./publish-guard.mjs");
    const guard = await import(new URL("../../scripts/check-engine-identifiers.ts", import.meta.url).href);
    const hits = [...guard.scanRepository(root), ...(base ? addedHits(root, base, sha) : [])];
    if (hits.length) return `production identifiers, so nothing was published:\n${hits.slice(0, 10).map((h) => `${h.path}:${h.line} (${h.kind})`).join("\n")}`;
  } catch (error) {
    return `the identifier guard could not run, so nothing was published: ${error.message}`;
  }
  return "";
}

const STALE_SNAPSHOT_SECONDS = 3 * 60 * 60;

/**
 * Snapshot branches this checkout left behind: named `ci/<first 12 of their
 * own sha>`, the commit made here (so present locally), and older than any
 * watch can last. Another agent's in-flight snapshot, or a teammate's
 * ci/<topic> branch, is never among them.
 */
export function staleSnapshots(lsRemote, now = Date.now() / 1000) {
  return lsRemote
    .split("\n")
    .map((line) => line.split(/\s+/))
    .filter(([sha, ref]) => {
      if (!sha || ref !== `refs/heads/ci/${sha.slice(0, 12)}`) return false;
      try {
        return now - Number(git(["show", "-s", "--format=%ct", sha])) > STALE_SNAPSHOT_SECONDS;
      } catch {
        return false; // not made here
      }
    })
    .map(([, ref]) => ref.slice("refs/heads/".length));
}

const deleteBranch = (branch) => {
  try {
    git(["push", "--quiet", "origin", "--delete", branch]);
  } catch (error) {
    console.log(`could not delete ${branch}: ${error.message}`);
  }
};

/** The newest run per dispatched workflow, among runs created after `since`. */
export function latestRuns(runs, since, workflows) {
  const byWorkflow = new Map();
  for (const run of runs) {
    if (run.createdAt < since || !workflows.includes(run.workflowName)) continue;
    const seen = byWorkflow.get(run.workflowName);
    if (!seen || run.databaseId > seen.databaseId) byWorkflow.set(run.workflowName, run);
  }
  return [...byWorkflow.values()];
}

/** The workflow files to dispatch, validated against WORKFLOWS. */
export function parseWorkflows(csv) {
  const files = String(csv ?? "").split(",").map((s) => s.trim()).filter(Boolean);
  if (!files.length) throw new Error("nothing to dispatch");
  for (const file of files) if (!Object.hasOwn(WORKFLOWS, file)) throw new Error(`not a gate workflow: ${file}`);
  return files;
}

async function watch(base, csv) {
  if (!/^[0-9a-f]{40}$/.test(base ?? "")) throw new Error(`watch needs the merge base as a full sha, got ${base}`);
  const files = parseWorkflows(csv);
  const workflows = files.map((file) => WORKFLOWS[file]);
  // Snapshots a SIGKILL stopped before they could clean up. Best effort: a
  // failed sweep must not stop this run.
  try {
    for (const stale of staleSnapshots(git(["ls-remote", "--heads", "origin", "ci/*"]))) deleteBranch(stale);
  } catch (error) {
    console.log(`could not sweep old snapshot branches: ${error.message}`);
  }
  const sha = snapshot();
  const refused = await refuseToPublish(sha, base);
  if (refused) throw new Error(refused);
  const branch = `ci/${sha.slice(0, 12)}`;
  git(["push", "--quiet", "origin", `${sha}:refs/heads/${branch}`]);
  // The runs seen so far: a stop cancels the ones still going, as the deadline does.
  let runs = [];
  const cancelRunning = () => {
    for (const run of runs.filter((r) => r.status !== "completed")) {
      try { gh("run", "cancel", String(run.databaseId)); } catch (error) { console.log(`could not cancel run ${run.databaseId}: ${error.message}`); }
    }
  };
  const stop = (signal) => {
    cancelRunning();
    deleteBranch(branch);
    process.exit(signal === "SIGINT" ? 130 : 143);
  };
  process.once("SIGTERM", stop);
  process.once("SIGINT", stop);
  // Every other exit below, including a thrown error, goes through this finally.
  try {
    // Generous: runs are matched by the snapshot's own sha, and a local clock
    // running ahead of GitHub's must not hide them.
    const since = new Date(Date.now() - 10 * 60_000).toISOString();
    for (const file of files) gh("workflow", "run", file, "--ref", branch);
    console.log(`Testing snapshot ${sha} on ${branch} (merge base ${base}): ${workflows.join(", ")}`);

    const deadline = Date.now() + 120 * 60_000;
    let misses = 0;
    for (;;) {
      let listed;
      try {
        listed = JSON.parse(gh(
          "run", "list", "--commit", sha, "--event", "workflow_dispatch", "--limit", "50",
          "--json", "databaseId,workflowName,status,conclusion,url,createdAt",
        ));
        misses = 0;
      } catch (error) {
        if (++misses > LISTING_RETRIES) throw new Error(`could not list runs ${misses} times in a row: ${error.message}`);
      }
      if (listed) {
        runs = latestRuns(listed, since, workflows);
        if (runs.length === workflows.length && runs.every((run) => run.status === "completed")) break;
      }
      if (Date.now() > deadline) {
        // Never leave a dispatched run going past the gate's own deadline.
        cancelRunning();
        throw new Error(`GitHub Actions still running after two hours on ${sha}; the dispatched runs were cancelled`);
      }
      await sleep(30_000);
    }
    for (const run of runs.filter((r) => r.conclusion !== "success")) {
      try {
        console.log(gh("run", "view", String(run.databaseId), "--log-failed").split("\n").slice(-80).join("\n"));
      } catch (error) {
        const partial = String(error.stdout ?? "").trim();
        console.log(partial ? partial.split("\n").slice(-80).join("\n") : `could not read the failed log: ${error.message}`);
      }
    }
    // Last, so a size-capped tail of this output always keeps them.
    for (const run of runs) {
      console.log(`${run.conclusion === "success" ? "PASS" : "FAIL"} ${run.workflowName}: ${run.conclusion} ${run.url}`);
    }
    return runs.every((run) => run.conclusion === "success");
  } finally {
    // Runs and their logs outlive the branch; only the ref is removed.
    deleteBranch(branch);
  }
}

/** Whether every gate workflow carries workflow_dispatch on the given ref (the default branch): else main's push run gates. */
export function dispatchable(ref = "origin/main") {
  return Object.keys(WORKFLOWS).every((file) => {
    try {
      return /^\s*workflow_dispatch\s*:/m.test(git(["show", `${ref}:.github/workflows/${file}`]));
    } catch {
      return false;
    }
  });
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const [command, base, csv] = process.argv.slice(2);
  try {
    if (command === "watch") process.exitCode = (await watch(base, csv)) ? 0 : 1;
    else if (command === "dispatchable") console.log(dispatchable(base || undefined) ? 1 : 0);
    else throw new Error("usage: remote-ci.mjs watch <base-sha> <workflow>[,<workflow>] | dispatchable [ref]");
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
