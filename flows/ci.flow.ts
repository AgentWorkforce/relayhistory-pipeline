// ci: classify the change, then run the CI it needs, and only that, as fast as it can go.
//
//   flows/node_modules/.bin/flows run --local-agent flows/ci.flow.ts --input '{}'                every round
//   flows/node_modules/.bin/flows run --local-agent flows/ci.flow.ts --input '{"github":true}'   the final gate
//
// Adapted from relay-app's flows/ci.flow.ts (flows/README.md has the provenance).
//
// 1. Classify (flows/lib/classify.mjs): deterministic path rules over the
//    paths changed since the merge base with main (tracked edits included)
//    give none (docs only), targeted (unit lanes plus the pipeline e2e steps
//    the paths reach) or full (unit lanes, pg-proof and the whole pipeline
//    e2e). With `"jev": true`, Jev may escalate the verdict, never lower it
//    (JEV_TOKEN from the environment; no token, the step is skipped and the
//    report says so). The verdict and its reasons head the report.
// 2. Start every selected lane at once (flows/lib/fleet.mjs, one process per
//    job): the guards always; typecheck, the four unit shards (each with its
//    own disposable ClickHouse) and the aux job for targeted and full;
//    pg-proof's real-postgres job on PostgreSQL 15, 16 and 17 for full; the
//    flows' own check when flows/ changed; and the pipeline e2e
//    (e2e/pipeline/run.ts) on a fresh, isolated stack, `"e2eTarget":
//    "sandbox"` running it in a throwaway Blaxel microVM instead of this machine.
// 3. With `github: true`, once, before the squash push: also run GitHub
//    Actions (ci.yml and pg-proof.yml, where the hardening mutations, the AMP
//    image, the read Worker and the other pg-proof jobs run) on a snapshot
//    pushed to a throwaway ci/<sha> branch (flows/lib/remote-ci.mjs), and wait.
//
// Result: .relayflow/ci/report.md (under $RELAYFLOW_STATE_DIR/ci/ when set), a
// pass/fail line per step with the output tail for each red one, and the
// pipeline e2e's own per-step report inlined. The run ends step_failed while
// any lane is red. Without `github`, a green run says the GitHub gate is still to come.
import { flow } from "@relayflows/surface";
import { classifyCommand, GITHUB_STEP, UNTRACKED } from "./lib/changes.ts";
import { runFleet, writeFile, type FleetJob } from "./lib/fleet.ts";
import { resolveBase } from "./lib/review.ts";
import { LIB, lib, shellWord } from "./lib/shell.ts";

// 2-minute polls (lib/fleet.ts says why they are short), 130 minutes in all: they cover GitHub's 125-minute step, inside the 150m budget.
export const POLL_MINUTES = 2;
export const POLLS = Math.ceil(130 / POLL_MINUTES);

interface CiInput {
  /** The branch the change lands on. Default `origin/main`. */
  base?: string;
  /** Also run GitHub Actions on a pushed snapshot: the final gate before the squash push. Default false. */
  github?: boolean;
  /** Let Jev escalate the classifier's verdict (needs JEV_TOKEN in the environment). Default false. */
  jev?: boolean;
  /** Force a verdict at or above the classifier's: "targeted" or "full". It never lowers one. */
  verdict?: "targeted" | "full";
  /** Where the pipeline e2e runs: a fresh compose project here ("local", default) or a Blaxel sandbox. */
  e2eTarget?: "local" | "sandbox";
}

export interface Classification {
  verdict: "none" | "targeted" | "full";
  rulesVerdict: string;
  reasons: string[];
  steps: string[];
  escalatedBy?: string;
  jev: { asked: boolean; choice?: string; confidence?: number; reason?: string };
  lanes: { unit: boolean; pgProof: boolean; flows: boolean; e2e: "none" | "targeted" | "full" };
  paths: number;
}

const VERDICT_RANK = { none: 0, targeted: 1, full: 2 } as const;

/** An author's override raises the verdict, never lowers it. */
export function applyOverride(c: Classification, given: CiInput["verdict"]): Classification {
  if (!given || VERDICT_RANK[given] <= VERDICT_RANK[c.verdict]) return c;
  return {
    ...c,
    verdict: given,
    steps: given === "full" ? [] : c.steps,
    escalatedBy: `given in the input (${given})`,
    lanes: { ...c.lanes, unit: true, pgProof: given === "full", e2e: given },
  };
}

const E2E_STEP = "pipeline e2e";

/** The marker a state dir this flow made carries; a non-empty directory without it is never deleted. */
export const STATE_MARKER = ".sessions-pipeline-ci";
export const cleanStateDir = (dir: string): string => {
  const d = shellWord(dir);
  return `if [ -d ${d} ] && [ -n "$(ls -A ${d})" ] && [ ! -f ${d}/${STATE_MARKER} ]; then echo ${shellWord(`refusing to empty ${dir}: ci.flow.ts did not make it (no ${STATE_MARKER})`)} >&2; exit 1; fi; ` +
    `rm -rf ${d} && mkdir -p ${d} && : > ${d}/${STATE_MARKER}`;
};

export function jobs(c: Classification, dir: string, remote: string | undefined, e2eTarget: "local" | "sandbox", workflows: string): FleetJob[] {
  const lane = (name: string, ...args: string[]) => ({ name, command: `bash ${lib("lane.sh")} ${args.map(shellWord).join(" ")}` });
  // Concurrency is capped by grouping: every job runs at once, its steps in order. Measured on an 8-core
  // machine, eleven heavy jobs at once (four shards, aux, three PostgreSQL proofs, the e2e) drove the load
  // past 35, starved Docker and timed out 5-second vitest cases that pass alone. So: two shard jobs of two
  // shards each, the aux suite after the light checks, and the PostgreSQL majors one after another.
  const checks: FleetJob = { name: "checks", steps: [lane("guards: helm hooks, secret-like files, production identifiers", "guards")] };
  const all: FleetJob[] = [checks];
  if (c.lanes.unit) {
    checks.steps.push(lane("stack and packages/pipeline typecheck", "typecheck"), { ...lane("package tests, shard plan, G4-only ingest, stacks/, scrub receipt", "aux"), timeoutMinutes: 60 });
    for (const pair of [["1", "3"], ["2", "4"]]) {
      all.push({ name: `shards ${pair.join("+")}`, steps: pair.map((n) => ({ ...lane(`unit tests, shard ${n} of 4`, "shard", n), timeoutMinutes: 90 })) });
    }
  }
  if (c.lanes.pgProof) all.push({ name: "pg-proof", steps: ["15", "16", "17"].map((major) => lane(`real PostgreSQL ${major}`, "pg-proof", major)) });
  if (c.lanes.flows) checks.steps.push(lane("typecheck, flows check and helper tests", "flows"));
  const steps = c.verdict === "full" ? [] : c.steps;
  if (c.verdict === "full" || steps.length) {
    // The e2e writes its own report under the CI state dir; ci.flow.ts inlines it.
    const select = steps.length ? ` --steps ${shellWord(steps.join(","))}` : "";
    all.push({
      name: "e2e",
      steps: [{
        name: E2E_STEP,
        command: `RELAYFLOW_STATE_DIR=${shellWord(dir)} bash ${lib("lane.sh")} e2e --target ${e2eTarget}${select}`,
        timeoutMinutes: 45,
      }],
    });
  }
  if (remote) {
    all.push({ name: "github", steps: [{ name: GITHUB_STEP, command: `node ${lib("remote-ci.mjs")} watch ${shellWord(remote)} ${shellWord(workflows)}`, timeoutMinutes: 125 }] });
  }
  return all;
}

export function describeClassification(c: Classification): string[] {
  const jev = c.jev.asked
    ? c.jev.choice ? `Jev answered ${c.jev.choice} at ${c.jev.confidence?.toFixed(2)}${c.escalatedBy?.startsWith("Jev") ? " and escalated" : " (it can only escalate, at >= 0.80)"}` : `Jev: ${c.jev.reason}`
    : `Jev: ${c.jev.reason}`;
  return [
    `Classifier: **${c.verdict}** (path rules: ${c.rulesVerdict}${c.escalatedBy ? `; raised by ${c.escalatedBy}` : ""}) over ${c.paths} changed path(s). ${jev}.`,
    ...c.reasons.map((r) => `  - ${r}`),
    `E2E: ${c.verdict === "full" ? "the whole pipeline" : c.steps.length ? `targeted steps ${c.steps.join(", ")}` : "none selected"}.`,
  ];
}

export default flow<CiInput>(
  "sessions-pipeline.ci",
  { budget: { wallclock: "150m" } },
  async (f, input) => {
    const started = Date.now();
    const base = input?.base ?? "origin/main";
    const dir = (await f.run(`node ${lib("state-dir.mjs")} ci`)).trim();
    // Never delete a directory this flow did not make: RELAYFLOW_STATE_DIR may point anywhere.
    await f.run(cleanStateDir(dir));
    // The same fetch, lease handling and explanation as the review flows.
    const problem = await resolveBase(f, base);
    if (problem) throw new Error(problem);
    // The tree under test must be the commit being landed: no tracked edits, and the report names the commit.
    const head = (await f.run("git rev-parse HEAD")).trim();
    const dirty = (await f.run("git diff --no-ext-diff --name-only HEAD --", { onNonZero: "record" })).stdout.split("\n").filter(Boolean);
    if (dirty.length) {
      await f.run(writeFile(`${dir}/report.md`, `# CI: NOT RUN: the working tree differs from ${head}\n\nCommit or drop these first (${dirty.length}): ${dirty.slice(0, 10).join(", ")}\n`));
      return f.done("needs_human", { detail: `${dirty.length} tracked file(s) differ from HEAD ${head.slice(0, 12)}; CI tests a commit, so commit or drop them and rerun` });
    }
    const gateSource = (await f.run(`git -C ${shellWord(LIB)} rev-parse HEAD`, { onNonZero: "record" })).stdout.trim() || "unknown";
    const mergeBase = (await f.run(`git merge-base ${shellWord(base)} HEAD`)).trim();
    const classification = applyOverride(
      JSON.parse(await f.run(classifyCommand(mergeBase, input?.jev === true), { timeout: "2m" })) as Classification,
      input?.verdict,
    );
    // New files are not in the commit, so they are never tested: a change with untracked files is never reported CI-green.
    const untracked = (await f.run(UNTRACKED)).split("\n").filter(Boolean);
    const dispatchable = (await f.run(`node ${lib("remote-ci.mjs")} dispatchable`)).trim() === "1";
    // The GitHub gate runs for every verdict, docs-only included: it is the run main's own push gets.
    const wantGithub = input?.github === true;
    const remote = wantGithub && !untracked.length && dispatchable ? mergeBase : undefined;
    const e2eTarget = input?.e2eTarget === "sandbox" ? "sandbox" : "local";

    const results = await runFleet(f, `${dir}/fleet`, jobs(classification, dir, remote, e2eTarget, "ci.yml,pg-proof.yml"), POLLS, POLL_MINUTES);
    const red = results.filter((r) => !r.ok);
    const e2eRan = results.some((r) => r.step === E2E_STEP);
    const e2eReport = e2eRan ? (await f.run(`cat ${shellWord(`${dir}/pipeline-e2e/report.md`)} 2>/dev/null || true`)).trim() : "";

    const localOnly = input?.github !== true;
    const minutes = Math.round((Date.now() - started) / 60_000);
    const report = [
      `# CI: ${red.length ? `${red.length} red` : untracked.length ? "NOT CI-green: untracked files were not tested" : localOnly ? "local checks green; GitHub gate still to come" : "green"}`,
      "",
      `Commit under test: ${head} (merge base ${mergeBase}). Gate code: ${LIB} at ${gateSource}.`,
      ...describeClassification(classification),
      `Wall clock: ${minutes} min for a ${classification.verdict} verdict.`,
      localOnly
        ? `GitHub Actions: not run; before the squash push, rerun with --input '{"github":true}'`
        : remote ? "" : !dispatchable ? "GitHub Actions: not dispatchable yet (ci.yml and pg-proof.yml gain workflow_dispatch on main with this change); main's own push run gates it"
          : "GitHub Actions: skipped until the untracked files are committed or excluded",
      untracked.length ? `Not covered: ${untracked.length} untracked file(s); stage the ones that belong to the change and list machine-local ones in .git/info/exclude (first: ${untracked.slice(0, 5).join(", ")})` : "",
      "Left to GitHub: the AWS hardening mutations, the AMP transport image, the session read Worker and pg-proof's other jobs (RDS emulation, ingest, consumer, buffered feed, credential probe).",
      "",
      ...results.map((r) => `- ${r.ok ? "PASS" : "FAIL"} ${r.job}: ${r.step}`),
      ...(e2eReport ? ["", e2eReport.replace(/^# /, "## ")] : []),
      ...red.flatMap((r) => ["", `## ${r.job}: ${r.step}`, "", ...(r.log ? [`Full log: ${r.log} (stderr: ${r.errLog})`, ""] : []), "```", r.output, "```"]),
    ].filter((line, i, all) => line !== "" || all[i - 1] !== "").join("\n");
    await f.run(`${writeFile(`${dir}/report.md`, report)} && ${writeFile(`${dir}/report.json`, JSON.stringify({ head, mergeBase, gate: { lib: LIB, commit: gateSource }, classification, results, minutes }, null, 2))}`);

    if (red.length) {
      return f.done("step_failed", { detail: `${red.map((r) => `${r.job}: ${r.step}`).join("; ")}; see ${dir}/report.md` });
    }
    if (untracked.length) {
      return f.done("needs_human", { detail: `${untracked.length} untracked file(s) were not tested; stage the ones that belong to the change and rerun` });
    }
    if (!localOnly && !dispatchable) {
      // Green locally, but the gate could not run: never report that as the final gate passing.
      return f.done("needs_human", { detail: `${classification.verdict}: local checks green in ${minutes} min; the GitHub gate could not be dispatched (ci.yml or pg-proof.yml has no workflow_dispatch on origin/main)` });
    }
    if (localOnly) {
      // Green for a fix round; the squash push still waits on the GitHub gate.
      return f.done("success", { detail: `${classification.verdict}: local checks green in ${minutes} min; GitHub gate still to come: rerun with {"github":true} before pushing` });
    }
    f.done("success", { detail: `${classification.verdict}: ${results.length} check(s) green in ${minutes} min` });
  },
);
