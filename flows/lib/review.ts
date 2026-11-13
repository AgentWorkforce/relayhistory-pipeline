// Shared plumbing for the two reviewer fleets. Both flows run from the
// repository root (`flows run flows/<name>.ts`), write only under their state
// dir (`.relayflow/<flow>/`, or `$RELAYFLOW_STATE_DIR/<flow>/`:
// lib/state-dir.mjs), and judge a snapshot of the working tree: the first run
// against the merge base, each later run only against what changed since the
// snapshot the last one reviewed (flows/lib/review-state.mjs).
import type { Ctx } from "@relayflows/surface";
import { MACHINE_LOCAL } from "./changes.ts";
import { preempted, runFleet, writeFile } from "./fleet.ts";
import { shellWord } from "./shell.ts";

export { shellWord };

/**
 * Fetch the base branch. All three flows start together (AGENTS.md), and two
 * fetches updating the same remote-tracking ref collide on its lock, so a
 * failed fetch is retried a few times before it fails the step.
 */
export function fetchBase(base: string): string | undefined {
  const slash = base.indexOf("/");
  if (slash <= 0) return undefined;
  const remote = shellWord(base.slice(0, slash));
  const ref = shellWord(base.slice(slash + 1));
  // A stalled remote gives up after 20 seconds under 1 KB/s. A black-holed one,
  // which never answers and so never stalls, is cut off after 50 seconds where
  // `timeout` (or Homebrew's gtimeout) exists, and not tried again: a cut-off
  // fetch keeps nothing, so a slow but moving fetch has that long. Three
  // attempts and the sleeps between them come to 156 seconds, inside the
  // 3-minute lease every caller gives this command.
  const fetch = `$t env GIT_HTTP_LOW_SPEED_LIMIT=1000 GIT_HTTP_LOW_SPEED_TIME=20 git fetch --no-tags --quiet ${remote} ${ref}`;
  // The input reaches git fetch as a refspec: only a branch name may, never
  // something like `+refs/heads/x:refs/heads/main` that would move a local branch.
  // A local branch with a slash in its name (`feature/x`) has nothing to fetch.
  const branchName = base.slice(slash + 1) === "HEAD" ? "true" : `git check-ref-format --branch ${ref} >/dev/null 2>&1`;
  return (
    `${branchName} || { echo ${shellWord(`base ${base} is not a branch name`)} >&2; exit 1; }; ` +
    `git remote | grep -qxF ${remote} || exit 0; t=$(command -v timeout || command -v gtimeout || true); t=\${t:+$t 50}; ` +
    `for i in 1 2; do ${fetch} && exit 0; s=$?; [ $s = 124 ] && exit 124; sleep $((i * 2)); done; ${fetch}`
  );
}

/** What exit 124 from fetchBase's command means. */
const CUT_OFF = "the fetch was cut off by the timeout program after 50 seconds without finishing (a black-holed remote, or one too slow for the cap)";

/**
 * Why a fetchBase command failed, for the author: git's last line where it
 * said anything, what a cut-off (exit 124) means, and what to do. One message
 * for every flow that runs the fetch.
 */
export function fetchProblem(base: string, fetched: { exitCode: number; stderr: string }): string {
  // The last line git (or the fetch command) wrote; the base is named once, by the caller.
  const last = (fetched.stderr.trim().split("\n").pop() || "").replace(/^base \S+ is not a branch name$/, "it is not a branch name");
  const remedy = /is not a branch name/.test(fetched.stderr)
    ? "The part after the remote must be a branch name"
    : /couldn't find remote ref|Remote branch .* not found|fatal: invalid refspec/i.test(fetched.stderr)
      ? "The remote has no such branch: pass a base the remote has"
      : "Check the network or the VPN, or pass a local branch or commit as the base (the review then compares against what this checkout has)";
  const why = fetched.exitCode === 124 ? `${last ? `${last}; ` : ""}${CUT_OFF}. ${remedy}` : `${last || `the fetch failed with no message (exit ${fetched.exitCode})`}. ${remedy}`;
  return `base ${base} could not be fetched: ${why}`;
}

/**
 * Fetch the base for a round, or say why it cannot be used: the fetch failed
 * or overran its lease (a throw under either policy), the name is not a
 * commit here, or it shares no history with HEAD. Returns the message for
 * the author, or undefined when the base is good.
 */
export async function resolveBase(f: Ctx, base: string): Promise<string | undefined> {
  const fetch = fetchBase(base);
  if (fetch) {
    let fetched: { exitCode: number; stderr: string };
    try {
      fetched = await f.run(fetch, { timeout: "3m", onNonZero: "record" });
    } catch (error) {
      // A lease overrun ends the round here, with the explanation in the failure: the run may not get
      // another step after an overrun, so the message must not wait for the report.
      if ((error as { code?: string })?.code !== "lease_exceeded") throw error;
      throw new Error(fetchProblem(base, { exitCode: -1, stderr: `the fetch did not finish within its lease (${String((error as Error)?.message ?? error).split("\n")[0]})` }));
    }
    if (fetched.exitCode !== 0) return fetchProblem(base, fetched);
  }
  const known = await f.run(`git rev-parse --verify --quiet ${shellWord(`${base}^{commit}`)}`, { onNonZero: "record" });
  if (known.exitCode !== 0) {
    const remotes = (await f.run("git remote", { onNonZero: "record" })).stdout.split("\n").filter(Boolean);
    return remotes.length
      ? `base ${base} is not a commit here: fetch it, or pass base as <remote>/<branch> for a remote this checkout has (${remotes.join(", ")})`
      : `base ${base} is not a commit here, and this checkout has no remote: add one and fetch the branch, or pass a local branch that exists`;
  }
  const related = await f.run(`git merge-base ${shellWord(base)} HEAD`, { onNonZero: "record" });
  if (related.exitCode !== 0) return `base ${base} shares no history with this branch; pass the branch the change lands on`;
  return undefined;
}

export interface ReviewInput {
  /** The branch the change lands on. Default `origin/main`. */
  base?: string;
  /** Agent CLIs to spread the fleet across, round-robin. Default `["claude"]`. */
  clis?: string[];
  /**
   * Untracked paths (files or directories) that belong to the change. `git
   * diff` cannot see a new file until it is committed; anything else left
   * untracked in the checkout stays out of the review.
   */
  include?: string[];
  /** Review the whole change against the merge base, not only what changed since the last run. */
  full?: boolean;
  /** The model every reviewer and the verifier run on, as the CLI names it. Default: the CLI's own. */
  model?: string;
}

export interface Lens {
  name: string;
  brief: string;
}

export interface Prepared {
  /** The repository's absolute path: each agent starts in its own scratch dir and works from here. */
  root: string;
  /** This flow's state dir: `.relayflow/<flow>`, or absolute under RELAYFLOW_STATE_DIR. */
  dir: string;
  /** The dir holding every flow's state dir. */
  stateRoot: string;
  patch: string;
  files: string;
  /** The changed files plus those the earlier open findings cite: what the verifier may report on. */
  scopeFiles: string;
  /** The earlier run's open blocker and major findings, for the verifier to re-check. */
  previous: string;
  snapshot: string;
  mergeBase: string;
  /** Only what changed since the last reviewed snapshot is in the patch. */
  incremental: boolean;
  changed: number;
  open: number;
  /** New files neither staged nor in `include`: the review cannot see them. */
  unseen: string[];
}

/** A fresh, git-excluded work dir holding the patch under review (flows/lib/review-state.mjs). */
export async function prepare(f: Ctx, flowName: string, input: ReviewInput | undefined): Promise<Prepared> {
  const base = input?.base ?? "origin/main";
  // The same fetch and the same explanation as the sized round: a cut-off says what it was.
  const problem = await resolveBase(f, base);
  if (problem) throw new Error(problem);
  const state = JSON.parse(
    await f.run(
      `node flows/lib/review-state.mjs prepare ${shellWord(flowName)} ${shellWord(base)} ${input?.full ? 1 : 0} ` +
        (input?.include ?? []).map(shellWord).join(" "),
      { timeout: "2m" },
    ),
  ) as { dir: string; stateRoot: string; snapshot: string; mergeBase: string; incremental: boolean; files: number; previous: number; unseen: string[] };
  const dir = state.dir;
  const root = (await f.run("git rev-parse --show-toplevel")).trim();
  return {
    root, dir, stateRoot: state.stateRoot, patch: `${dir}/change.patch`, files: `${dir}/files.txt`, scopeFiles: `${dir}/scope-files.txt`,
    previous: `${dir}/previous.json`, snapshot: state.snapshot, mergeBase: state.mergeBase,
    incremental: state.incremental, changed: state.files, open: state.previous, unseen: state.unseen,
  };
}

/** Record the reviewed snapshot and its open findings for the next run. */
export const saveState = (f: Ctx, flowName: string, prepared: Prepared) =>
  f.run(`node flows/lib/review-state.mjs save ${shellWord(flowName)} ${shellWord(prepared.snapshot)} ${shellWord(prepared.mergeBase)}`);

/** How each flow tells its fleet which part of the change the patch is. */
export const roundNote = (p: Prepared): string =>
  p.incremental
    ? "This is a follow-up round. Earlier rounds reviewed the rest of the change; the patch holds only what " +
      "changed since then (often fixes to earlier findings). Review that, including what it breaks around it."
    : "";

const FINDINGS_FORMAT = [
  "Write your result as JSON to the path you are given, exactly this shape:",
  '{ "reviewer": "<your lens>", "findings": [ { "severity": "blocker" | "major" | "minor" | "nit",',
  '  "file": "<repo-relative path>", "line": <number>, "title": "<one line>",',
  '  "failure_scenario": "<concrete inputs/state and the wrong outcome>",',
  '  "evidence": "<the code or command output that proves it>", "fix": "<smallest fix>" } ] }',
  'An empty "findings" array is a valid answer. Only report what you verified by reading the code or running',
  "something; no speculation, no style preferences dressed up as defects. blocker = must not ship;",
  "major = a real defect a user or a later change will hit; minor = real but low impact; nit = optional.",
].join("\n");

// The fleets review the change, not the codebase. The gate enforces the file
// half of this; the verifier enforces the line half.
const scope = (files: string): string =>
  [
    "SCOPE: review the change only. A finding must be about code the patch adds, removes or modifies,",
    `in a file listed in ${files} (write "file" exactly as it is listed there), and its "line" must be a line the patch touches (or, for a removal, the`,
    "line where the removed code was). Read code outside the patch only to understand a changed line.",
    "A defect that already existed in unchanged code is out of scope even if you notice it, unless the",
    "change newly reaches it (a new call into it, a new input that triggers it); then report it at the",
    "changed line that reaches it. Something the change should have touched but did not (a test, the",
    "matching platform code, a changelog bullet) is reported at the changed line that needs it.",
    "Do not audit the rest of the repository.",
  ].join("\n");

// The runtime runs agents that share a working directory one at a time, so it
// can attribute each file change to one step (flows docs/SURFACE.md). Each
// reviewer therefore starts in its own scratch dir, writes its findings
// there, and reads the repository by absolute path: the fleet runs at once.
const where = (root: string): string =>
  [
    `Your working directory is a scratch directory for your output and small experiments. The repository is at ${root}:`,
    "run every command from there. Every path in these instructions, other than your output file, is relative to it.",
    "Never copy the repository, or any part of it, into your working directory or anywhere under the repository:",
    "the runtime records every file written there, and your run cannot be checked past a few thousand. An",
    "experiment that needs a modified copy (a mutant to build or test) goes in a directory from `mktemp -d`,",
    "outside both; delete it when you are done. That copy is the one thing that belongs outside your working",
    "directory: your output and every other experiment stay inside it.",
    "Other reviewers and the CI flow run on this machine beside you. Never signal a process you did not start: no",
    "pkill, killall or kill by name or pattern; stop only a background process you started, by the pid you got for it.",
  ].join("\n");

export const findingsPath = (dir: string, lens: string): string => `${dir}/${lens}/findings.json`;

// A reviewer or verifier past its time limit ends; whatever it wrote stands.
// Headroom over the eight minutes the task asks for.
const REVIEWER_TIMEOUT = "12m";
/** The review fleet's polls: 2 minutes each (lib/fleet.ts says why they are short), 26 in all, covering the reviewers' 20-minute run budget. */
export const REVIEW_POLL_MINUTES = 2;
export const REVIEW_POLLS = 13;
const VERIFIER_TIMEOUT = "12m";
const WRITE_AS_YOU_GO =
  "Aim to finish within eight minutes. Write findings.json with an empty findings array first, then rewrite " +
  "it after each finding you verify, so the file is always a valid answer if your time runs out.";

/** The gate every reviewer's file must pass: parseable, the shape above, and every finding in a changed file. */
export const findingsGate = (path: string, files: string) => ({
  type: "subprocess_gate" as const,
  command: `node flows/lib/check-findings.mjs ${shellWord(path)} ${shellWord(files)}`,
});

/**
 * Fan the lenses out at once: each reviewer is its own `reviewer.flow.ts` run,
 * started together and gated there on writing valid findings. Returns the
 * findings files, or the lenses whose run failed.
 */
export interface FleetOptions {
  flow: string;
  root: string;
  dir: string;
  files: string;
  lenses: readonly Lens[];
  clis: string[];
  /** Given to every reviewer and the verifier; absent, each CLI's default. */
  model?: string;
  preamble: string;
}

/** What flows/reviewer.flow.ts reads for one lens. */
export interface ReviewerInput {
  lens: string;
  cli: string;
  /** The model the reviewer runs on, as the CLI names it; absent for the CLI's default. */
  model?: string;
  /** The reviewer's scratch dir: relative to the repository root, or absolute under RELAYFLOW_STATE_DIR. */
  cwd: string;
  /** Where the findings land, and the changed-file list they are checked against. */
  findings: string;
  files: string;
  task: string;
  /** The reviewer's time limit; it ends there and its findings file stands. */
  timeout: string;
}

/** One reviewer input per lens, CLIs round-robin, the model carried exactly when given. */
export const reviewerInputs = (opts: FleetOptions): ReviewerInput[] =>
  opts.lenses.map((lens, i) => ({
    lens: lens.name,
    cli: opts.clis[i % opts.clis.length],
    ...(opts.model ? { model: opts.model } : {}),
    cwd: `${opts.dir}/${lens.name}`,
    findings: findingsPath(opts.dir, lens.name),
    files: opts.files,
    task: [
      where(opts.root),
      opts.preamble,
      scope(opts.files),
      `Your lens is ${lens.name.toUpperCase()}. Stay inside it; other reviewers cover the rest.`,
      lens.brief,
      "Do not edit, stage, commit or delete anything in the repository. You may run read-only commands and the",
      "quick single-file bun and node tests; do not run the whole suite, docker, or the e2e lanes, which the CI flow runs alongside you.",
      FINDINGS_FORMAT,
      "Write to findings.json in your working directory with a shell heredoc or node, not python (bare `python` is not installed on the Cloud runner); confirm the file exists with cat before you report it written.",
      WRITE_AS_YOU_GO,
    ].join("\n\n"),
    timeout: REVIEWER_TIMEOUT,
  }));

export async function reviewFleet(f: Ctx, opts: FleetOptions): Promise<{ paths: string[]; failed: string[]; preempted: boolean }> {
  const inputs = reviewerInputs(opts);
  const paths = inputs.map((input) => input.findings);
  await f.run(
    inputs.map((input) => `mkdir -p ${shellWord(input.cwd)} && ${writeFile(`${input.cwd}/input.json`, JSON.stringify(input))}`).join(" && "),
  );
  // One daemon for the whole fleet; it runs the reviewers' runs side by side.
  const flows = `flows/node_modules/.bin/flows run --local-agent --no-observer-link --data-dir .relayflowd/${opts.flow}-fleet`;
  const results = await runFleet(
    f,
    `${opts.dir}/fleet`,
    inputs.map((input) => ({
      name: input.lens,
      steps: [{ name: "review", command: `${flows} flows/reviewer.flow.ts --input ${shellWord(`${input.cwd}/input.json`)}` }],
    })),
    REVIEW_POLLS,
    REVIEW_POLL_MINUTES,
  );
  // A newer fleet in this directory stopped these reviewers: whatever they
  // wrote first (an empty answer) is no review.
  if (preempted(results)) return { paths: [], failed: inputs.map((input) => `${input.lens} (preempted by a newer review)`), preempted: true };
  // The runtime does not always hold a reviewer to its time limit, so a run
  // can end red after the reviewer wrote a complete answer. A lens counts as
  // reviewed when its findings file passes the same gate its run applies.
  const red = results.filter((r) => !r.ok);
  const failed: string[] = [];
  const kept: string[] = [];
  for (const [i, input] of inputs.entries()) {
    if (!red.some((r) => r.job === input.lens)) {
      kept.push(paths[i]);
      continue;
    }
    const gate = await f.run(findingsGate(paths[i], opts.files).command, { onNonZero: "record" });
    if (gate.exitCode === 0) kept.push(paths[i]);
    else failed.push(`${input.lens} (see ${red.find((r) => r.job === input.lens)?.errLog ?? "its fleet log"})`);
  }
  return { paths: kept, failed, preempted: false };
}

/**
 * Reviewers run with whatever permissions their CLI is launched with, so the
 * "readonly" declaration is only advice. Fingerprint the working tree before
 * the fleet and compare after: a reviewer that changed it fails the review.
 * It covers tracked edits, every untracked file's name and contents, and the
 * ignored flows/node_modules the flows execute; machine-local state (the
 * relay's and the CLIs', lib/changes.ts) changes while the fleet runs and is
 * left out. The last
 * command is git's own hash, so the pipeline cannot pass with empty output
 * (an embedded repository lists as a directory and counts by name).
 */
export const TREE_FINGERPRINT =
  "{ git rev-parse HEAD; git diff HEAD --no-ext-diff --binary; " +
  `git ls-files --others --exclude-standard -- . :!:.relayflow :!:.relayflowd ${MACHINE_LOCAL.map((p) => `:!:${p}`).join(" ")} | ` +
  "while IFS= read -r p; do echo \"$p\"; [ ! -f \"$p\" ] || git hash-object -- \"$p\"; done; " +
  "find flows/node_modules -type f -print0 | sort -z | xargs -0 git hash-object --; " +
  // Ignored or outside the work tree, so the listing above never shows them,
  // but each can make a later command in this checkout run code: the CLIs'
  // settings and the repository's hooks and config.
  "find .claude flows/.claude .git/hooks .git/config -type f 2>/dev/null | sort | " +
  "while IFS= read -r p; do echo \"$p\"; git hash-object -- \"$p\"; done; } | git hash-object --stdin";

/**
 * One verifier re-checks every finding against the code, drops what does not
 * hold up, merges duplicates, and writes report.json plus a readable
 * report.md. The flow then reads the blocking count back deterministically.
 */
export interface VerifierOptions {
  root: string;
  dir: string;
  files: string;
  scopeFiles: string;
  previous: string;
  open: number;
  paths: string[];
  cli: string;
  model?: string;
  preamble: string;
}

/** The verifier's agent options: its CLI, the model exactly when given, its scratch dir and its task. */
export const verifierAgent = (opts: VerifierOptions) => ({
  cli: opts.cli,
  ...(opts.model ? { model: opts.model } : {}),
  cwd: opts.dir,
  task: [
    where(opts.root),
    opts.preamble,
    scope(opts.files),
    `You are the VERIFIER for a fleet of reviewers. Read ${opts.paths.join(", ") || "(no reviewer files: every reviewer run failed or nothing changed)"}.`,
    opts.open
      ? `${opts.previous} holds ${opts.open} blocker/major finding(s) still open from the last round. Check each ` +
        "against the code as it is now: keep it (marked \"raised before\" in its title) if the defect is still " +
        "there, at its current line; leave it out if the code no longer has it, and list it under an " +
        "`Addressed since the last round` heading in report.md. These may cite files outside the patch."
      : "",
    "For every finding: open the code it cites and try to disprove it. Keep it only if it survives,",
    "marking it CONFIRMED (you reproduced or traced it) or PLAUSIBLE (the reasoning holds but you could not",
    "prove it). Merge findings that describe the same defect, keep the most severe rating that is justified,",
    "and downgrade any severity the evidence does not support. Drop any finding that falls outside SCOPE.",
    "Where reviewers contradict each other, decide",
    "and say why in the finding.",
    "Do not edit, stage, commit or delete anything in the repository.",
    'Each reviewer file may carry a "dropped" list of out-of-scope findings; end report.md with one line',
    "per dropped finding under a `Dropped as out of scope` heading, so none disappears silently.",
    FINDINGS_FORMAT,
    'Add "verdict": "CONFIRMED" | "PLAUSIBLE" and "reviewers": [<lens names>] to every finding you keep.',
    'Write report.json in your working directory with "reviewer": "verifier", then report.md beside it: one line per finding,',
    "most severe first, as `- [severity] file:line - title (verdict)` followed by its failure scenario and fix.",
    "Minor and nit findings are listed for the author and do not hold up the change; only blocker and major do.",
  ].filter(Boolean).join("\n\n"),
  permissions: { accessPreset: "readonly" as const },
  timeout: VERIFIER_TIMEOUT,
});

export async function reconcile(f: Ctx, opts: VerifierOptions): Promise<{ blocking: number; total: number }> {
  const report = `${opts.dir}/report.json`;
  await f.agent("verifier", verifierAgent(opts)).gate(findingsGate(report, opts.scopeFiles));
  const counts = await f.run(
    `node -e ${shellWord(
      "const r=JSON.parse(require('fs').readFileSync(process.argv[1],'utf8'));" +
        "const b=r.findings.filter(x=>x.severity==='blocker'||x.severity==='major').length;" +
        "console.log(b+' '+r.findings.length)",
    )} ${shellWord(report)}`,
  );
  const [blocking, total] = counts.trim().split(" ").map(Number);
  return { blocking, total };
}

export interface Verdict {
  reason: "success" | "step_failed";
  detail: string;
}

/**
 * One review round: the fleet over the patch, the verifier over its findings
 * and the earlier open ones, then the verdict. Only blocker and major
 * findings fail it. The reviewed snapshot is saved only when every lens
 * reviewed it and the tree was left alone, so the next round starts from it.
 */
export async function runReview(
  f: Ctx,
  opts: { flow: string; prepared: Prepared; lenses: readonly Lens[]; clis: string[]; model?: string; preamble: string },
): Promise<Verdict> {
  const p = opts.prepared;
  const report = `${p.dir}/report.md`;
  if (p.unseen.length) {
    return {
      reason: "step_failed",
      detail:
        `${p.unseen.length} new file(s) are neither staged nor in include, so no reviewer would see them ` +
        `(first: ${p.unseen.slice(0, 5).join(", ")}); stage the ones that belong to the change, list machine-local ones in .git/info/exclude`,
    };
  }
  if (p.changed === 0 && !p.incremental) return { reason: "step_failed", detail: "nothing to review: no changes against the base" };
  if (p.changed === 0) {
    return p.open
      ? { reason: "step_failed", detail: `nothing changed since the last review, and ${p.open} blocker/major finding(s) are still open` }
      : { reason: "success", detail: "nothing changed since the last clean review" };
  }
  const preamble = [opts.preamble, roundNote(p)].filter(Boolean).join("\n");
  const before = (await f.run(TREE_FINGERPRINT)).trim();
  const { paths, failed, preempted: taken } = await reviewFleet(f, {
    flow: opts.flow, root: p.root, dir: p.dir, files: p.files, lenses: opts.lenses, clis: opts.clis, model: opts.model, preamble,
  });
  if (taken) return { reason: "step_failed", detail: "preempted by a newer review in this checkout; nothing was reviewed or saved" };
  if (paths.length === 0 && !p.open) return { reason: "step_failed", detail: `every reviewer run failed: ${failed.join("; ")}` };
  const { blocking, total } = await reconcile(f, {
    root: p.root, dir: p.dir, files: p.files, scopeFiles: p.scopeFiles, previous: p.previous, open: p.open,
    paths, cli: opts.clis[0], model: opts.model, preamble,
  });
  if ((await f.run(TREE_FINGERPRINT)).trim() !== before) {
    return { reason: "step_failed", detail: "a reviewer changed the working tree; inspect git status before trusting this review" };
  }
  if (failed.length) return { reason: "step_failed", detail: `reviewed without ${failed.join("; ")}; see ${report} and rerun` };
  await saveState(f, opts.flow, p);
  const round = p.incremental ? `follow-up round over ${p.changed} changed file(s)` : `full review of ${p.changed} file(s)`;
  return blocking > 0
    ? { reason: "step_failed", detail: `${round}: ${blocking} blocker/major of ${total} verified finding(s); see ${report}` }
    : { reason: "success", detail: `${round}: no blocker or major; ${total} minor/nit finding(s) listed in ${report}` };
}
