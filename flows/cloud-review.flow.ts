// cloud-review: one review round as an Agent Relay Cloud flow run. agent/review.sh
// sizes the round on the host (plan-review.flow.ts: Jev, the floors, the
// author's depth), writes the change under cloud-review/input/, and submits
// this flow with `flows run --cloud --sync-code`; reviews never run on the host.
// It runs the planned review flows (adversarial-review, context-review) with
// their lenses, briefs, preambles and verifier copied from lib/review.ts,
// adversarial-review.flow.ts and context-review.flow.ts, so a Cloud round
// asks the reviewers exactly what a local round asked them.
//
// Self-contained: it imports only @relayflows/surface. The Cloud bundle is
// built from this one module, and the round must not depend on which flows/
// the reviewed ref carries. In the sandbox it runs only
// `node flows/lib/check-findings.mjs` (node built-ins only) as the reviewers'
// gate, which every reviewed ref tracks.
//
// Input (review.sh builds it):
//   {
//     "depth": "small" | "standard" | "deep",   the planned depth: it picks the lenses, the timeouts and how
//                                              many second opinions run (DEPTH_PLANS); small runs
//                                              adversarial-review only
//     "flows": ["adversarial-review", "context-review"],   the planned flows, in any order
//     "model": "claude-opus-5-5",              optional: the Claude model, used only by reviewers that run
//                                              on claude (the lead or an extra lens); codex and grok get
//                                              no model override. review.sh passes the plan's model
//                                              (sonnet for small, opus for standard and deep), never fable.
//                                              Anything but a claude-sonnet-* or claude-opus-* model ends the
//                                              run needs_human before any reviewer starts: Fable is the
//                                              persistent lead and never a worker
//     "lead": "claude" | "codex",              optional, default "claude": every lens of every flow and
//                                              each verifier runs on this CLI
//     "lenses": { "claude": true, "codex": true, "grok": true },  optional extra second-opinion
//                                              reviewers in adversarial-review; one on the lead's CLI is ignored,
//                                              and those past the depth's room (none for small, one for
//                                              standard, two for deep) are listed under lensesUnavailable
//     "dir": "cloud-review"                    where the input is and the results go; nothing else is accepted
//   }
// It reads cloud-review/input/:
//   change.patch   the change against its merge base with main
//   files.txt      the files it touches, one per line (the gate's scope)
//   meta.json      { ref, head, mergeBase, depth, model }; head must be a string
//   manifest       `git ls-files -s -z` of the uploaded files (mode, blob id, stage, path), from a
//                  scratch index of HEAD with the files' content on disk (review.sh upload_manifest)
//   intent.md      only when context-review is planned: what the change was for. Copied to a
//                  mktemp dir outside the checkout and deleted from cloud-review/input/ before any
//                  reviewer starts, so the adversarial fleet never comes across it.
// It writes cloud-review/<flow>/report.json and report.md (one verifier per flow),
// each reviewer's cloud-review/<flow>/<lens>/findings.json, and
// cloud-review/summary.json:
//   { depth, lead, lenses, flows: { [name]: { blocking, total, report: "cloud-review/<name>/report.md", failed? } },
//     lensesUnavailable: [...] }
// Before any reviewer starts, the input must match the tree, else the run
// ends step_failed: the paths the patch's `diff --git`, rename and copy lines
// name are exactly the lines of files.txt; the tree is a git checkout whose
// index matches manifest by path, mode and blob id (inputDescribesTree), with
// cloud-review/ and the runtime's .relayflow and .relayflowd left out; and
// `git status` shows nothing outside those. The sandbox's HEAD is not
// compared with meta.json's head: Cloud checks the upload out as a commit of
// its own. A workspace with no git checkout is refused.
// cloud-review/ must stay un-ignored (.gitignore): `flows sync` brings back
// only what git would see, and review.sh moves the results into its state dir
// and removes cloud-review/ after pulling them.
//
// Verdict: success once summary.json is written, whatever it says. Blocking
// findings, and a flow whose result is `failed` (every lead lens failed, the
// verifier produced no valid file, context-review had no intent.md, or a
// reviewer changed the tree outside cloud-review/, which marks every flow
// failed), are the round's verdict, read by review.sh from summary.json and
// reported red; the done detail names the failed flows. step_failed only for
// a run that wrote no summary: incomplete input, or a step that broke before
// the summary was written. A second-opinion lens whose CLI is missing or whose
// run failed is listed under "Lenses unavailable" in report.md and in
// summary.json instead.
//
// The reviewers run one after another. With relayflows 2.0.59 an `f.agent`
// runs in parallel only in a module that imports nothing but `flow`, has no
// other top-level statements, and whose Promise.all branches hold only
// `f.run` steps; any other body stays serial (sdk authored-parallel
// eligibility). The reviewer.flow.ts fan-out that local rounds use needs a
// second module and a flows daemon, neither of which a Cloud bundle carries.
// So a round runs fewer lenses than a local one, chosen per depth for the
// defects that must not ship (DEPTH_PLANS), to finish within its cap:
//   small     30 min: adversarial correctness, security, failure-modes; no
//                     second opinion; reviewers and verifier cut off at 6m:
//                     24 minutes at worst
//   standard  60 min: those three, plus context intent; one second opinion;
//                     reviewers cut off at 8m, verifiers at 7m: 54 at worst
//   deep      90 min: those three and platform, plus context intent and
//                     verification; two second opinions; reviewers and
//                     verifiers cut off at 8m: 80 at worst
// The rest of each cap is for the sandbox's own steps. Each reviewer and
// verifier is asked to finish two minutes before its cut-off. review.sh
// passes the cap as --budget-minutes; this flow's wallclock, a static header
// the input cannot change, is the deep cap.
import { flow, type Ctx } from "@relayflows/surface";

type ReviewFlow = "adversarial-review" | "context-review";
type Lead = "claude" | "codex";

interface CloudReviewInput {
  depth: "small" | "standard" | "deep";
  flows: ReviewFlow[];
  model?: string;
  lead?: Lead;
  lenses?: { claude?: boolean; codex?: boolean; grok?: boolean };
  dir: "cloud-review";
}

interface Lens {
  name: string;
  brief: string;
}

interface FlowResult {
  blocking: number;
  total: number;
  report: string;
  failed?: string;
}

const DIR = "cloud-review";
const INPUT = `${DIR}/input`;
const PATCH = `${INPUT}/change.patch`;
const FILES = `${INPUT}/files.txt`;
const FLOWS: readonly ReviewFlow[] = ["adversarial-review", "context-review"];
const DEPTHS = ["small", "standard", "deep"] as const;
const META = `${INPUT}/meta.json`;
const MANIFEST = `${INPUT}/manifest`;
/** The Claude models a reviewer may run on; Fable is the persistent lead and never a worker. */
const WORKER_MODEL = /^claude-(sonnet|opus)-/;

const shellWord = (value: string): string => `'${value.replaceAll("'", "'\\''")}'`;
const writeFile = (path: string, text: string): string =>
  `printf %s ${shellWord(Buffer.from(text, "utf8").toString("base64"))} | base64 -d > ${shellWord(path)}`;

// Copied from flows/adversarial-review.flow.ts.
const ADVERSARIAL_LENSES: readonly Lens[] = [
  {
    name: "correctness",
    brief:
      "Find logic errors: wrong conditions, off-by-one, inverted checks, unhandled states and enum cases, " +
      "broken invariants between the changed code and its callers. Follow each changed function to its callers " +
      "and callees only to check the change did not break them.",
  },
  {
    name: "security",
    brief:
      "Find trust-boundary defects: untrusted input (peer messages, webhooks, files, env, network) reaching " +
      "shell commands, paths, SQL, or auth decisions; secrets or tokens logged, persisted or sent; file modes; " +
      "socket and token exposure; anything that widens who can act as whom.",
  },
  {
    name: "failure-modes",
    brief:
      "Find what breaks under stress: races and ordering between threads, tasks and processes; error paths " +
      "that swallow, retry forever, or leave state half-written; leaked processes, sockets, files and timers; " +
      "behaviour on restart, offline, slow or partial responses, and duplicate delivery.",
  },
  {
    name: "tests",
    brief:
      "Judge the tests that cover the changed code. Which changed behaviour has no test? Which tests would " +
      "still pass if the change were reverted or broken? Run the narrowest relevant quick tests you can " +
      "(bun test <one file>, node --test flows/lib/*.test.mjs; the CI flow runs the shards, pg-proof and the pipeline e2e) and report real failures.",
  },
  {
    name: "platform",
    brief:
      "Find defects across the places this pipeline runs: the self-host services (docker/entrypoint.sh SERVICE, " +
      "docs/selfhost-composition.md), the hosted Worker and Lambda entry points, and the local stack; PostgreSQL " +
      "15, 16 and 17 and Supabase (roles, grants, row-level security, migrations that are not additive or not " +
      "idempotent); S3, MinIO and GCS object stores (conditional writes, key layout, per-org prefixes). A change " +
      "to one entry point with no matching change to its siblings, or an env flag whose default is not off, is a defect.",
  },
];

// Copied from flows/context-review.flow.ts.
const CONTEXT_LENSES: readonly Lens[] = [
  {
    name: "intent",
    brief:
      "List every requirement the intent states or clearly implies, then check each against the code: done, " +
      "partly done, done differently, or missing. Report each gap and each misreading of the ask.",
  },
  {
    name: "scope",
    brief:
      "Find what the change does that was not asked for: unrelated edits, drive-by refactors, behaviour removed " +
      "or altered as a side effect, leftover debug output, files that look accidentally included.",
  },
  {
    name: "conventions",
    brief:
      "Check the change follows this repository's own rules: read AGENTS.md, CONTRIBUTING.md, the READMEs and " +
      "the docs/ page for the area it touches. In particular: scrubbing lives in the relayscrubber crate, never new " +
      "scrub logic here; new behaviour is behind a flag that defaults off; no production identifiers or anything " +
      "credential-like in the tree (scripts/check-engine-identifiers.ts); migrations are new files, never edits " +
      "of applied ones; docs and the env tables follow behaviour changes. Match new code to the idiom of the code around it.",
  },
  {
    name: "verification",
    brief:
      "Check the tests prove the behaviour the intent asked for, not merely that the code runs. Name each " +
      "asked-for behaviour with no test, and each test that would still pass if that behaviour were broken. " +
      "Run the narrowest relevant tests you can and report real failures.",
  },
  {
    name: "user-facing",
    brief:
      "Judge what an API client, a self-host operator and the Agent Relay apps see: status codes, error codes " +
      "and reasons and their remedies, defaults, env settings and their startup refusals, and whether the docs " +
      "(docs/, the OpenAPI contracts under docs/contracts/) describe what the code now does.",
  },
];

/** The extra reviewers: a different CLI over the whole change, independent of the lenses. */
const SECOND_OPINION_BRIEF =
  "You are a second opinion from a different agent CLI. Review the whole change, not one lens: logic errors, " +
  "trust-boundary defects, what breaks under stress, tests that do not prove the changed behaviour, and " +
  "defects across the self-host, hosted and local deployments. Other reviewers work independently of you: do not read " +
  `their files under ${DIR}/.`;

// Adapted from adversarial-review.flow.ts: the round's results dir takes the
// place of the state root, and its input dir is the one part of it to read.
const offLimits = (dir: string): string =>
  `anything under .agentworkforce/, .claude/, .relayflow/, or ${DIR}/\noutside ${INPUT}/ and ${dir}/.`;

// Copied from flows/adversarial-review.flow.ts.
const adversarialPreamble = (patch: string, files: string, dir: string): string =>
  [
    "You are an adversarial code reviewer. Your job is to find defects in a change before it ships.",
    `The change is the patch in ${patch}; the files it touches are listed in ${files}. Read the patch, and`,
    "read the code around it only as far as you need to understand the changed lines.",
    "You are deliberately given no description of what the change is for. Judge it only by what the code does.",
    "Do not look for its purpose: do not run git log, git show, git blame or gh; do not read CHANGELOG.md,",
    `PR descriptions, issue text, relay messages, or ${offLimits(dir)}`,
    "Comments in the patch are claims to verify, not facts.",
  ].join("\n");

// Copied from flows/context-review.flow.ts.
const contextPreamble = (intentPath: string, patch: string, files: string): string =>
  [
    "You are reviewing a change against what it was meant to do.",
    `What it was meant to do is in ${intentPath}. Read it first.`,
    `The change is the patch in ${patch}; the files it touches are listed in ${files}. Read the patch, and`,
    "read the code around it only as far as you need to understand the changed lines.",
    `Do not read ${DIR}/adversarial-review/: that fleet works independently of yours.`,
  ].join("\n");

// Copied from flows/lib/review.ts.
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

// Copied from flows/lib/review.ts.
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

// Copied from flows/lib/review.ts.
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

type Depth = CloudReviewInput["depth"];

/** What a round of each depth runs; worstCaseMinutes(depth) must leave CAP_SLACK_MINUTES of its cap. */
export interface DepthPlan {
  /** The minutes the whole run may take: review.sh's --budget-minutes. */
  cap: number;
  /** adversarial-review's lenses, by name, in the order they run. */
  adversarial: readonly string[];
  /** context-review's lenses; none for small, which never plans context-review. */
  context: readonly string[];
  /** Each reviewer's cut-off, second opinions included, in minutes. */
  reviewerMinutes: number;
  /** Each verifier's cut-off, in minutes. */
  verifierMinutes: number;
  /** How many of the asked-for second opinions run. */
  secondOpinions: number;
}

/**
 * Per depth, the lenses that catch what must not ship: logic (correctness),
 * trust boundaries (security), races, half-written state and data loss
 * (failure-modes), and whether the change does what was asked (intent);
 * deep adds the cross-platform lens and the intent's tests.
 */
export const DEPTH_PLANS: Readonly<Record<Depth, DepthPlan>> = {
  small: { cap: 30, adversarial: ["correctness", "security", "failure-modes"], context: [], reviewerMinutes: 6, verifierMinutes: 6, secondOpinions: 0 },
  standard: { cap: 60, adversarial: ["correctness", "security", "failure-modes"], context: ["intent"], reviewerMinutes: 8, verifierMinutes: 7, secondOpinions: 1 },
  deep: {
    cap: 90,
    adversarial: ["correctness", "security", "failure-modes", "platform"],
    context: ["intent", "verification"],
    reviewerMinutes: 8,
    verifierMinutes: 8,
    secondOpinions: 2,
  },
};

/** Minutes of each cap left for the sandbox's own steps (checks, gates, the summary). */
export const CAP_SLACK_MINUTES = 5;
/** The fewest minutes a reviewer is given: below this it cannot read a change and verify a finding. */
export const MIN_REVIEWER_MINUTES = 5;

/** The longest a round of this depth can run its agents, with every second opinion it has room for. */
export const worstCaseMinutes = (depth: Depth, flows: readonly ReviewFlow[] = depth === "small" ? ["adversarial-review"] : FLOWS): number => {
  const plan = DEPTH_PLANS[depth];
  return flows.reduce((sum, name) => {
    const lenses = name === "adversarial-review" ? plan.adversarial.length + plan.secondOpinions : plan.context.length;
    return sum + lenses * plan.reviewerMinutes + plan.verifierMinutes;
  }, 0);
};

/** The lenses a flow runs at this depth, briefs and all. */
export const lensesFor = (flow: ReviewFlow, depth: Depth): Lens[] => {
  const all = flow === "adversarial-review" ? ADVERSARIAL_LENSES : CONTEXT_LENSES;
  const names = flow === "adversarial-review" ? DEPTH_PLANS[depth].adversarial : DEPTH_PLANS[depth].context;
  return names.map((name) => {
    const lens = all.find((l) => l.name === name);
    if (!lens) throw new Error(`no ${flow} lens ${name}`);
    return lens;
  });
};

const NUMBER_WORDS = ["zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten"];
const minutesWord = (n: number): string => NUMBER_WORDS[n] ?? String(n);

/** What a reviewer is asked to aim for: two minutes inside its cut-off. Eight, a local round's, by default. */
const writeAsYouGo = (aimMinutes = 8): string =>
  `Aim to finish within ${minutesWord(aimMinutes)} minutes. Write findings.json with an empty findings array first, then rewrite ` +
  "it after each finding you verify, so the file is always a valid answer if your time runs out.";

/**
 * The findings gate is gate code (docs/maintainer-workflow.md "Trust"): it runs from beside this module, the
 * checkout the flow was loaded from (main's, when the lander runs main's flows), never from the reviewed tree.
 * A Cloud bundle has no sibling lib/ (this module is bundled alone), and there the uploaded tree's flows/ IS
 * main's: the submitter uploads main's flows/ over the change (relay-app agent/review.sh upload_manifest), so
 * that copy is the gate. No import is needed for this: URL and import.meta are globals.
 */
export const MODULE_GATE = ((): string => {
  try {
    const url = new URL("./lib/check-findings.mjs", import.meta.url);
    return url.protocol === "file:" ? decodeURIComponent(url.pathname) : "";
  } catch {
    return "";
  }
})();

export const gateCommand = (path: string, files: string): string =>
  `g=${shellWord(MODULE_GATE)}; { [ -n "$g" ] && [ -f "$g" ]; } || g=flows/lib/check-findings.mjs; node "$g" ${shellWord(path)} ${shellWord(files)}`;

/** A reviewer's task, in lib/review.ts reviewerInputs' order; a second opinion covers the whole change. */
export const reviewerTask = (root: string, preamble: string, lens: Lens, secondOpinion = false, aimMinutes?: number): string =>
  [
    where(root),
    preamble,
    scope(FILES),
    secondOpinion
      ? `Your lens is ${lens.name.toUpperCase()}. You cover the whole change on your own.`
      : `Your lens is ${lens.name.toUpperCase()}. Stay inside it; other reviewers cover the rest.`,
    lens.brief,
    "Do not edit, stage, commit or delete anything in the repository. You may run read-only commands and the",
    "quick single-file bun and node tests; do not run the whole suite, docker, or the e2e lanes, which the CI flow runs alongside you.",
    FINDINGS_FORMAT,
    "Write to findings.json in your working directory with a shell heredoc or node, not python (bare `python` is not installed on the Cloud runner); confirm the file exists with cat before you report it written.",
    writeAsYouGo(aimMinutes),
  ].join("\n\n");

/**
 * The verifier's task, from lib/review.ts verifierAgent (a Cloud round carries no earlier open findings);
 * with `aimMinutes`, it is also told to finish within them.
 */
export const verifierTask = (root: string, preamble: string, paths: string[], aimMinutes?: number): string =>
  [
    where(root),
    preamble,
    scope(FILES),
    `You are the VERIFIER for a fleet of reviewers. Read ${paths.join(", ") || "(no reviewer files: every reviewer run failed or nothing changed)"}.`,
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
    ...(aimMinutes === undefined
      ? []
      : [`Aim to finish within ${minutesWord(aimMinutes)} minutes: you are cut off two minutes later, and a verifier that has not written report.json and report.md by then fails the flow.`]),
  ].join("\n\n");

const EXTRA_CLIS = ["claude", "codex", "grok"] as const;

/** Which second-opinion lenses a round asks for: never one on the lead's own CLI. */
export const secondOpinions = (lead: Lead, lenses: CloudReviewInput["lenses"]): { name: string; cli: string }[] =>
  EXTRA_CLIS.filter((cli) => lenses?.[cli] && cli !== lead).map((cli) => ({ name: `${cli}-second-opinion`, cli }));

/** The asked-for second opinions split into those a depth has room for and those it drops, in asked order. */
export const secondOpinionsFor = (
  depth: Depth,
  lead: Lead,
  lenses: CloudReviewInput["lenses"],
): { run: { name: string; cli: string }[]; dropped: string[] } => {
  const asked = secondOpinions(lead, lenses);
  const room = DEPTH_PLANS[depth].secondOpinions;
  return {
    run: asked.slice(0, room),
    dropped: asked
      .slice(room)
      .map((l) => `${l.name}: a ${depth} round has room for ${room === 0 ? "no second opinion" : `${minutesWord(room)} second opinion${room === 1 ? "" : "s"}`} within its ${DEPTH_PLANS[depth].cap}-minute cap`),
  };
};

/** The model option for an agent on `cli`: only claude takes the round's model; codex and grok keep their own. */
export const modelFor = (cli: string, model: string | undefined): { model?: string } =>
  cli === "claude" && model ? { model } : {};

/**
 * meta.json's head, or a reason it has none: run in the sandbox as
 * `node -e`, so it uses nothing from this module.
 */
export function metaHead(text: string): { head?: string; why?: string } {
  let meta: unknown;
  try {
    meta = JSON.parse(text);
  } catch (error) {
    return { why: String((error as Error).message).split("\n")[0] };
  }
  const head = (meta as { head?: unknown } | null)?.head;
  return typeof head === "string" && head !== "" ? { head } : { why: "no string head" };
}

/**
 * How the paths a patch names differ from the lines of files.txt; empty when
 * they are the same set. A patch is read as review.sh's patch_outside_paths
 * reads one: each `diff --git a/X b/Y` header (a quoted pair as git quotes
 * it, which is how files.txt lists it too) with its rename and copy lines,
 * which name the paths exactly. A copy's source is not changed, so it is left
 * out. Run in the sandbox as `node -e`, so it uses nothing from this module.
 */
export function scopeDifference(patch: string, files: string): string[] {
  const named = new Set<string>();
  const header = (rest: string): [string | null, string] => {
    const quoted = rest.match(/^"a\/(.*)" "b\/(.*)"$/);
    if (quoted) return [`"${quoted[1]}"`, `"${quoted[2]}"`];
    // The same path on both sides (any change but a rename or copy), even one holding " b/".
    const n = (rest.length - 5) / 2;
    if (Number.isInteger(n) && rest.startsWith("a/") && rest.slice(2 + n, 5 + n) === " b/" && rest.slice(2, 2 + n) === rest.slice(5 + n)) {
      return [rest.slice(2, 2 + n), rest.slice(5 + n)];
    }
    const i = rest.indexOf(" b/");
    // A header in no known shape names its whole rest, so files.txt must list exactly that.
    return rest.startsWith("a/") && i > 2 ? [rest.slice(2, i), rest.slice(i + 3)] : [rest, rest];
  };
  let block: [string | null, string] | null = null;
  const flush = () => {
    if (block?.[0] != null) named.add(block[0]);
    if (block) named.add(block[1]);
    block = null;
  };
  for (const line of patch.split("\n")) {
    if (line.startsWith("diff --git ")) {
      flush();
      block = header(line.slice("diff --git ".length));
      continue;
    }
    const moved = line.match(/^(rename|copy) (from|to) (.*)$/);
    if (!moved || !block) continue;
    if (moved[2] === "to") block[1] = moved[3];
    else block[0] = moved[1] === "rename" ? moved[3] : null;
  }
  flush();
  const listed = new Set(files.split("\n").filter(Boolean));
  const some = (paths: string[]) => paths.slice(0, 5).join(", ") + (paths.length > 5 ? ` (+${paths.length - 5} more)` : "");
  const onlyPatch = [...named].filter((p) => !listed.has(p)).sort();
  const onlyFiles = [...listed].filter((p) => !named.has(p)).sort();
  return [
    ...(onlyPatch.length ? [`only in the patch: ${some(onlyPatch)}`] : []),
    ...(onlyFiles.length ? [`only in files.txt: ${some(onlyFiles)}`] : []),
  ];
}

/** `node -e` source that reads the files named in argv, calls `fn` on their text and prints its JSON answer. */
const nodeCall = (fn: (...texts: string[]) => unknown): string =>
  `const fs=require("fs");console.log(JSON.stringify((${String(fn)})(...process.argv.slice(1).map((p)=>fs.readFileSync(p,"utf8")))))`;

/** Run `fn` in the sandbox over the files at `paths`; undefined when it did not run or printed no JSON. */
async function sandboxCall<T>(f: Ctx, fn: (...texts: string[]) => unknown, paths: string[]): Promise<T | undefined> {
  const out = await f.run(`node -e ${shellWord(nodeCall(fn))} ${paths.map(shellWord).join(" ")}`, { onNonZero: "record" });
  if (out.exitCode !== 0) return undefined;
  try {
    return JSON.parse(out.stdout) as T;
  } catch {
    return undefined;
  }
}

/** Untracked or changed paths outside cloud-review/ (and the runtime's own state), as TREE_FINGERPRINT leaves them out. */
const DIRTY_OUTSIDE = `git status --porcelain --untracked-files=all -- . ':!:${DIR}' ':!:.relayflow' ':!:.relayflowd'`;

/**
 * How `manifest`, review.sh's `git ls-files -s -z` listing of the files it
 * uploaded, differs from the same listing of the sandbox's index: the files
 * missing, changed (mode or content) and extra, a few of each with counts;
 * empty when they match. Blob ids are content hashes, so they survive the
 * commit Cloud makes of the upload, whose commit and tree ids never match the
 * source's. cloud-review/ and the runtime's .relayflow and .relayflowd are
 * left out of both, and nothing else. Self-contained (it runs in the sandbox
 * through sandboxCall), hence the literal directory names.
 */
export function manifestDifference(manifest: string, index: string): string[] {
  const own = (path: string) => ["cloud-review", ".relayflow", ".relayflowd"].some((d) => path === d || path.startsWith(`${d}/`));
  const read = (text: string): Map<string, { mode: string; id: string }> | undefined => {
    const entries = new Map<string, { mode: string; id: string }>();
    for (const entry of text.split("\0")) {
      if (entry === "") continue;
      const tab = entry.indexOf("\t");
      const [mode, id, stage] = entry.slice(0, Math.max(tab, 0)).split(" ");
      if (tab < 0 || !/^[0-7]{6}$/.test(mode ?? "") || !/^[0-9a-f]{40,64}$/.test(id ?? "") || !/^[0-3]$/.test(stage ?? "")) return undefined;
      const path = entry.slice(tab + 1);
      // An unmerged entry never matches: its stage rides on the mode.
      if (!own(path)) entries.set(path, { mode: stage === "0" ? mode : `${mode} stage ${stage}`, id });
    }
    return entries;
  };
  const want = read(manifest);
  const have = read(index);
  if (!want) return ["the manifest is not a `git ls-files -s -z` listing"];
  if (!have) return ["the sandbox's index listing is not a `git ls-files -s -z` listing"];
  const missing: string[] = [];
  const changed: string[] = [];
  for (const [path, w] of want) {
    const h = have.get(path);
    if (!h) missing.push(path);
    else if (h.mode !== w.mode) changed.push(`${path} (mode ${w.mode} -> ${h.mode})`);
    else if (h.id !== w.id) changed.push(`${path} (content)`);
  }
  const extra = [...have.keys()].filter((path) => !want.has(path));
  const some = (label: string, paths: string[]) =>
    paths.length ? [`${label} ${paths.length}: ${paths.slice(0, 3).join(", ")}${paths.length > 3 ? ` (+${paths.length - 3} more)` : ""}`] : [];
  return [...some("missing", missing), ...some("changed", changed), ...some("extra", extra)];
}

/**
 * Why the synced tree is not the one the round's input describes, or
 * undefined when it is. Cloud checks the upload out as a commit of its own,
 * so the sandbox's HEAD is never `head` and says nothing about the source;
 * the check is by content alone, the same whatever HEAD is. The sandbox's
 * index must match the manifest review.sh wrote (by path, mode and blob id),
 * and `git status` must show nothing outside cloud-review/ and the runtime's
 * state, so no edit on disk hides behind a matching index. Without a git
 * checkout the manifest cannot be judged (an in-process walk could not tell
 * an extra file from an ignored one), so that is refused too.
 */
export async function inputDescribesTree(f: Ctx, head: string): Promise<string | undefined> {
  const tree = await f.run("git rev-parse --verify -q HEAD", { onNonZero: "record" });
  if (tree.exitCode !== 0) {
    return `the synced tree has no git checkout, so ${MANIFEST} cannot be verified against it (the round's input says ${head})`;
  }
  const at = `the synced tree (sandbox HEAD ${tree.stdout.trim()}) is not the round's input (head ${head})`;
  if ((await f.run(`test -s ${shellWord(MANIFEST)}`, { onNonZero: "record" })).exitCode !== 0) return `${at}: the input has no ${MANIFEST}`;
  const listed = await f.run('d=$(mktemp -d "${TMPDIR:-/tmp}/relay-index.XXXXXX") && echo "$d" && git ls-files -s -z > "$d/index"', { onNonZero: "record" });
  const scratch = listed.stdout.trim().split("\n")[0] ?? "";
  try {
    if (listed.exitCode !== 0) return `${at}: its index could not be listed: ${listed.stderr.trim().split("\n")[0] || `exit ${listed.exitCode}`}`;
    const diff = await sandboxCall<string[]>(f, manifestDifference, [MANIFEST, `${scratch}/index`]);
    if (!Array.isArray(diff)) return `${at}: the comparison with ${MANIFEST} did not run`;
    if (diff.length) return `${at}: its index differs from ${MANIFEST}: ${diff.join("; ")}`;
  } finally {
    if (scratch.startsWith("/")) await f.run(`rm -rf ${shellWord(scratch)}`);
  }
  const dirty = await f.run(DIRTY_OUTSIDE, { onNonZero: "record" });
  const paths = dirty.stdout.split("\n").filter(Boolean);
  if (dirty.exitCode !== 0 || paths.length) {
    const some = paths.slice(0, 3).map((line) => line.slice(3)).join(", ") + (paths.length > 3 ? ` (+${paths.length - 3} more)` : "");
    return `${at}: it has changes outside ${DIR}/ before any reviewer started: ${some || dirty.stderr.trim().split("\n")[0]}`;
  }
  return undefined;
}

/**
 * The tree outside cloud-review/, hashed before the reviewers and after the
 * verifiers: a reviewer that edited it fails the round, since `flows sync`
 * would carry the edit back to the host's clone. From lib/review.ts
 * TREE_FINGERPRINT, without the host-only parts.
 */
const TREE_FINGERPRINT =
  `{ git rev-parse HEAD; git diff HEAD --no-ext-diff --binary -- . ':!:${DIR}'; ` +
  `git ls-files --others --exclude-standard -- . ':!:${DIR}' ':!:.relayflow' ':!:.relayflowd' | ` +
  "while IFS= read -r p; do echo \"$p\"; [ ! -f \"$p\" ] || git hash-object -- \"$p\"; done; } | git hash-object --stdin";

/** Run one agent and judge its file by the gate, whether the agent step itself ended well or not. */
async function gated(f: Ctx, name: string, options: Parameters<Ctx["agent"]>[1], path: string, files: string): Promise<string | undefined> {
  let why = "";
  try {
    await f.agent(name, options).gate({ type: "subprocess_gate", command: gateCommand(path, files) });
    return undefined;
  } catch (error) {
    why = String((error as Error)?.message ?? error).split("\n")[0];
  }
  // A run can end red after the agent wrote a complete answer (lib/review.ts
  // reviewFleet): the file counts when it passes the same gate.
  const gate = await f.run(gateCommand(path, files), { onNonZero: "record" });
  return gate.exitCode === 0 ? undefined : why || gate.stderr.trim().split("\n").pop() || "no valid file";
}

async function reviewOne(
  f: Ctx,
  opts: {
    flow: ReviewFlow;
    root: string;
    lead: Lead;
    model?: string;
    preamble: string;
    lenses: readonly Lens[];
    extra: { name: string; cli: string }[];
    /** Second opinions asked for but past the depth's room: listed as unavailable. */
    dropped: string[];
    plan: DepthPlan;
  },
): Promise<{ result: FlowResult; unavailable: string[] }> {
  const dir = `${DIR}/${opts.flow}`;
  const report = `${dir}/report.md`;
  const unavailable: string[] = [...opts.dropped];
  const reviewerTimeout = `${opts.plan.reviewerMinutes}m`;
  const reviewerAim = opts.plan.reviewerMinutes - 2;
  const all = [...opts.lenses.map((l) => l.name), ...opts.extra.map((l) => l.name)];
  // The runtime refuses an agent whose cwd does not exist at dispatch.
  await f.run(`mkdir -p ${all.map((name) => shellWord(`${dir}/${name}`)).join(" ")}`);
  const paths: string[] = [];
  const failed: string[] = [];
  for (const lens of opts.lenses) {
    const findings = `${dir}/${lens.name}/findings.json`;
    const why = await gated(f, `${opts.flow}-${lens.name}`, {
      cli: opts.lead,
      ...modelFor(opts.lead, opts.model),
      cwd: `${dir}/${lens.name}`,
      task: reviewerTask(opts.root, opts.preamble, lens, false, reviewerAim),
      timeout: reviewerTimeout,
      permissions: { accessPreset: "readonly" },
    }, findings, FILES);
    if (why === undefined) paths.push(findings);
    else failed.push(`${lens.name} (${why})`);
  }
  for (const extra of opts.extra) {
    const present = await f.run(`command -v ${shellWord(extra.cli)}`, { onNonZero: "record" });
    if (present.exitCode !== 0) {
      unavailable.push(`${extra.name}: the ${extra.cli} CLI is not installed in this sandbox`);
      continue;
    }
    const findings = `${dir}/${extra.name}/findings.json`;
    const why = await gated(f, `${opts.flow}-${extra.name}`, {
      cli: extra.cli,
      ...modelFor(extra.cli, opts.model),
      cwd: `${dir}/${extra.name}`,
      task: reviewerTask(opts.root, opts.preamble, { name: extra.name, brief: SECOND_OPINION_BRIEF }, true, reviewerAim),
      timeout: reviewerTimeout,
      permissions: { accessPreset: "readonly" },
    }, findings, FILES);
    if (why === undefined) paths.push(findings);
    else unavailable.push(`${extra.name}: its run produced no valid findings (${why})`);
  }
  const note = unavailable.length ? `\n\n## Lenses unavailable\n\n${unavailable.map((u) => `- ${u}`).join("\n")}\n` : "";
  if (failed.length === opts.lenses.length) {
    await f.run(writeFile(report, `# ${opts.flow}: no review\n\nEvery lens failed: ${failed.join("; ")}\n${note}`));
    return { result: { blocking: 0, total: 0, report, failed: `every lens failed: ${failed.join("; ")}` }, unavailable };
  }
  const reportJson = `${dir}/report.json`;
  const verifierWhy = await gated(f, `${opts.flow}-verifier`, {
    cli: opts.lead,
    ...modelFor(opts.lead, opts.model),
    cwd: dir,
    task: verifierTask(opts.root, opts.preamble, paths, opts.plan.verifierMinutes - 2),
    timeout: `${opts.plan.verifierMinutes}m`,
    permissions: { accessPreset: "readonly" },
  }, reportJson, FILES);
  const hasMd = (await f.run(`test -s ${shellWord(report)}`, { onNonZero: "record" })).exitCode === 0;
  if (verifierWhy !== undefined || !hasMd) {
    const why = verifierWhy ?? "the verifier wrote no report.md";
    await f.run(writeFile(report, `# ${opts.flow}: not verified\n\nThe verifier failed: ${why}\n${note}`));
    return { result: { blocking: 0, total: 0, report, failed: `the verifier failed: ${why}` }, unavailable };
  }
  // Copied from lib/review.ts reconcile: the blocking count, read back deterministically.
  const counts = await f.run(
    `node -e ${shellWord(
      "const r=JSON.parse(require('fs').readFileSync(process.argv[1],'utf8'));" +
        "const b=r.findings.filter(x=>x.severity==='blocker'||x.severity==='major').length;" +
        "console.log(b+' '+r.findings.length)",
    )} ${shellWord(reportJson)}`,
  );
  const [blocking, total] = counts.trim().split(" ").map(Number);
  if (note) await f.run(`${writeFile(`${dir}/.lenses-note`, note)} && cat ${shellWord(`${dir}/.lenses-note`)} >> ${shellWord(report)} && rm -f ${shellWord(`${dir}/.lenses-note`)}`);
  return {
    result: { blocking, total, report, ...(failed.length ? { failed: `reviewed without ${failed.join("; ")}` } : {}) },
    unavailable,
  };
}

export default flow<CloudReviewInput>(
  "sessions-pipeline.cloud-review",
  // Static: the deep cap. review.sh's --budget-minutes holds each depth to its own.
  { budget: { wallclock: "90m" } },
  async (f, input) => {
    const lead: Lead = input?.lead ?? "claude";
    const planned = FLOWS.filter((name) => input?.flows?.includes(name));
    if ((input?.dir ?? DIR) !== DIR) return f.done("needs_human", { detail: `dir must be ${DIR}` });
    if (lead !== "claude" && lead !== "codex") return f.done("needs_human", { detail: `lead must be claude or codex, not ${JSON.stringify(lead)}` });
    // Checked whatever the lead: a codex lead still hands the model to a claude second opinion.
    if (input?.model !== undefined && !(typeof input.model === "string" && WORKER_MODEL.test(input.model))) {
      return f.done("needs_human", { detail: `model must be a Claude Sonnet or Opus model, not ${JSON.stringify(input.model)}; Fable is the persistent lead and never a worker` });
    }
    if (!DEPTHS.includes(input?.depth)) return f.done("needs_human", { detail: `depth must be one of ${DEPTHS.join(", ")}` });
    if (!planned.length || planned.length !== (input?.flows?.length ?? 0)) {
      return f.done("needs_human", { detail: `flows must name adversarial-review and/or context-review, not ${JSON.stringify(input?.flows)}` });
    }
    const depthPlan = DEPTH_PLANS[input.depth];
    if (planned.some((name) => lensesFor(name, input.depth).length === 0)) {
      return f.done("needs_human", { detail: `a ${input.depth} round runs adversarial-review only, not ${JSON.stringify(input.flows)}` });
    }
    const extras = secondOpinionsFor(input.depth, lead, input.lenses);
    const missing = await f.run(`for p in ${[PATCH, FILES, META].map(shellWord).join(" ")}; do [ -s "$p" ] || echo "$p"; done`);
    if (missing.trim()) return f.done("step_failed", { detail: `the round's input is incomplete: ${missing.trim().split("\n").join(", ")}` });

    // The input must describe this tree, before any reviewer reads either.
    const meta = await sandboxCall<ReturnType<typeof metaHead>>(f, metaHead, [META]);
    const head = meta?.head;
    if (head === undefined) return f.done("step_failed", { detail: `${META} is unreadable: ${meta?.why ?? "the check did not run"}` });
    const scopeDiff = await sandboxCall<string[]>(f, scopeDifference, [PATCH, FILES]);
    if (!Array.isArray(scopeDiff) || scopeDiff.length) {
      return f.done("step_failed", { detail: `the patch and ${FILES} name different paths: ${Array.isArray(scopeDiff) ? scopeDiff.join("; ") : "the check did not run"}` });
    }
    const mismatch = await inputDescribesTree(f, head);
    if (mismatch !== undefined) return f.done("step_failed", { detail: mismatch });

    // Outside the checkout, before any reviewer starts: the adversarial fleet
    // runs in the same tree and must not come across what the change was for.
    const intentDir = (await f.run('mktemp -d "${TMPDIR:-/tmp}/relay-context-intent.XXXXXX"')).trim();
    const intentPath = `${intentDir}/intent.md`;
    const results: Record<string, FlowResult> = {};
    const unavailable: string[] = [];
    try {
      const copied = await f.run(
        `if [ -s ${shellWord(`${INPUT}/intent.md`)} ]; then cp ${shellWord(`${INPUT}/intent.md`)} ${shellWord(intentPath)}; fi; ` +
          `rm -f ${shellWord(`${INPUT}/intent.md`)}; test -s ${shellWord(intentPath)}`,
        { onNonZero: "record" },
      );
      const root = (await f.run("git rev-parse --show-toplevel 2>/dev/null || pwd")).trim();
      const before = (await f.run(TREE_FINGERPRINT)).trim();
      for (const name of planned) {
        if (name === "context-review" && copied.exitCode !== 0) {
          await f.run(`mkdir -p ${shellWord(`${DIR}/${name}`)} && ${writeFile(`${DIR}/${name}/report.md`, `# ${name}: no review\n\nNo intent.md in ${INPUT}/.\n`)}`);
          results[name] = { blocking: 0, total: 0, report: `${DIR}/${name}/report.md`, failed: `no intent.md in ${INPUT}/` };
          continue;
        }
        const { result, unavailable: missed } = await reviewOne(f, {
          flow: name,
          root,
          lead,
          model: input.model,
          preamble:
            name === "adversarial-review"
              ? adversarialPreamble(PATCH, FILES, `${DIR}/${name}`)
              : contextPreamble(intentPath, PATCH, FILES),
          lenses: lensesFor(name, input.depth),
          extra: name === "adversarial-review" ? extras.run : [],
          dropped: name === "adversarial-review" ? extras.dropped : [],
          plan: depthPlan,
        });
        results[name] = result;
        unavailable.push(...missed);
      }
      if ((await f.run(TREE_FINGERPRINT)).trim() !== before) {
        for (const result of Object.values(results)) {
          result.failed = `a reviewer changed the working tree outside ${DIR}/${result.failed ? `; ${result.failed}` : ""}`;
        }
      }
    } finally {
      await f.run(`rm -rf ${shellWord(intentDir)}`);
    }
    const summary = {
      depth: input.depth,
      lead,
      lenses: extras.run.map((l) => l.name),
      flows: results,
      lensesUnavailable: unavailable,
    };
    await f.run(writeFile(`${DIR}/summary.json`, `${JSON.stringify(summary, null, 2)}\n`));
    // The summary is written: from here the run succeeds, and a failed flow
    // is the round's verdict (review.sh reports it red), not this run's.
    const failed = Object.entries(results).filter(([, r]) => r.failed);
    if (failed.length) {
      return f.done("success", { detail: `failed: ${failed.map(([name, r]) => `${name}: ${r.failed}`).join("; ")}; see ${DIR}/summary.json` });
    }
    const blocking = Object.values(results).reduce((n, r) => n + r.blocking, 0);
    f.done("success", { detail: `${planned.join(", ")} verified on ${lead}: ${blocking} blocker/major finding(s); see ${DIR}/summary.json` });
  },
);
