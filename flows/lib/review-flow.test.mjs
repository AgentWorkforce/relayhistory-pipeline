// The flow-side plumbing that no pure helper covers, driven through a fake
// Ctx: the fleet token hand-off, a preempted review fleet, and the model
// reaching the reviewers' inputs and the verifier.
import assert from "node:assert/strict";
import test from "node:test";

import { PREEMPTED, runFleet } from "./fleet.ts";
import { POLL_MINUTES as CI_POLL_MINUTES, POLLS as CI_POLLS } from "../ci.flow.ts";
import { fetchProblem, prepare, resolveBase, REVIEW_POLL_MINUTES, REVIEW_POLLS, reviewerInputs, reviewFleet, runReview, verifierAgent } from "./review.ts";

/** A Ctx whose run answers by command pattern and whose agent records its options. */
function fakeCtx(answers) {
  const runs = [];
  const agents = [];
  const run = async (command, options = {}) => {
    runs.push(command);
    const hit = answers.find(([pattern]) => pattern.test(command));
    const out = hit ? (typeof hit[1] === "function" ? hit[1](command) : hit[1]) : "";
    if (options.onNonZero === "record") return typeof out === "object" ? out : { exitCode: 0, stdout: out, stderr: "" };
    return typeof out === "object" ? out.stdout : out;
  };
  const agent = (name, options) => {
    agents.push({ name, options });
    return { gate: async () => ({}) };
  };
  return { f: { run, agent, done() {} }, runs, agents };
}

const started = "started 1 job(s) token TOKEN-abc123";
const done = (results) => JSON.stringify({ state: "done", results });

test("runFleet waits on, and stops, the fleet by the token its start printed", async () => {
  const { f, runs } = fakeCtx([
    [/fleet\.mjs' start/, started],
    [/fleet\.mjs' wait/, done([{ job: "a", step: "s", ok: true, output: "" }])],
  ]);
  const results = await runFleet(f, ".relayflow/x/fleet", [{ name: "a", steps: [{ name: "s", command: "true" }] }], 2);
  assert.equal(results.length, 1);
  assert.ok(runs.some((c) => c.includes("fleet.mjs' wait '.relayflow/x/fleet' 'TOKEN-abc123'")), runs.join("\n"));
  const pending = fakeCtx([
    [/fleet\.mjs' start/, started],
    [/fleet\.mjs' wait/, JSON.stringify({ state: "pending", results: [{ job: "a", step: "fleet", ok: false, output: "still running when this wait ended" }] })],
  ]);
  const late = await runFleet(pending.f, ".relayflow/x/fleet", [{ name: "a", steps: [{ name: "s", command: "sleep 9" }] }], 1);
  assert.match(late[0].output, /^still running after 13 minutes/);
  assert.ok(pending.runs.some((c) => c.includes("fleet.mjs' stop '.relayflow/x/fleet' 'TOKEN-abc123'")), "the out-of-polls stop names the fleet too");
});

test("runFleet polls for the minutes it is given and counts them when it runs out", async () => {
  const pending = fakeCtx([
    [/fleet\.mjs' start/, started],
    [/fleet\.mjs' wait/, JSON.stringify({ state: "pending", results: [{ job: "a", step: "fleet", ok: false, output: "still running when this wait ended" }] })],
  ]);
  const late = await runFleet(pending.f, ".relayflow/x/fleet", [{ name: "a", steps: [{ name: "s", command: "sleep 9" }] }], 3, 2);
  const waits = pending.runs.filter((c) => c.includes("fleet.mjs' wait"));
  assert.equal(waits.length, 3);
  assert.ok(waits.every((c) => c.endsWith("fleet.mjs' wait '.relayflow/x/fleet' 'TOKEN-abc123' 2")), waits.join("\n"));
  assert.match(late[0].output, /^still running after 6 minutes/);
  await assert.rejects(runFleet(pending.f, ".relayflow/x/fleet", [], 1, 14), /at most 13 minutes/, "a poll never outgrows the command ceiling");
});

test("the ci and review fleets poll as briefly and wait as long in all as before", () => {
  // Before: ci ten 13-minute polls, the review fleet two.
  for (const [name, polls, minutes, total] of [["ci", CI_POLLS, CI_POLL_MINUTES, 10 * 13], ["review", REVIEW_POLLS, REVIEW_POLL_MINUTES, 2 * 13]]) {
    assert.ok(minutes <= 3, `${name}: a ${minutes}-minute poll can spend every root retry before the root sees it`);
    assert.ok(polls * minutes >= total, `${name}: the fleet wait (${polls * minutes} min) must not shrink below ${total} min`);
  }
});

const lenses = [{ name: "correctness", brief: "find bugs" }, { name: "tests", brief: "judge tests" }];
const fleetOptions = { flow: "adversarial-review", root: "/repo", dir: ".relayflow/adversarial-review", files: ".relayflow/adversarial-review/files.txt", lenses, clis: ["claude"], preamble: "P" };

test("a review fleet a newer fleet took over counts every lens as unreviewed", async () => {
  const { f } = fakeCtx([
    [/fleet\.mjs' start/, started],
    [/fleet\.mjs' wait/, done([{ job: "correctness", step: "review", ok: true, output: "" }, { job: "fleet", step: "fleet", ok: false, output: PREEMPTED }])],
  ]);
  const result = await reviewFleet(f, { ...fleetOptions, model: "claude-opus-5-5" });
  assert.equal(result.preempted, true);
  assert.deepEqual(result.paths, []);
  assert.deepEqual(result.failed, ["correctness (preempted by a newer review)", "tests (preempted by a newer review)"]);
});

/** The reviewer inputs a fleet wrote, decoded from the writeFile commands. */
const writtenInputs = (runs) =>
  runs.flatMap((c) => [...c.matchAll(/printf %s '([A-Za-z0-9+/=]+)' \| base64 -d > '[^']*\/input\.json'/g)].map((m) => JSON.parse(Buffer.from(m[1], "base64").toString("utf8"))));

test("runReview hands the model to every reviewer input and to the verifier, and ends step_failed before the verifier when preempted", async () => {
  const prepared = {
    root: "/repo", dir: ".relayflow/adversarial-review", stateRoot: ".relayflow", patch: ".relayflow/adversarial-review/change.patch",
    files: ".relayflow/adversarial-review/files.txt", scopeFiles: ".relayflow/adversarial-review/scope-files.txt", previous: ".relayflow/adversarial-review/previous.json",
    snapshot: "s".repeat(40), mergeBase: "m".repeat(40), incremental: false, changed: 1, open: 0, unseen: [],
  };
  const green = fakeCtx([
    [/git hash-object --stdin/, "fingerprint"],
    [/fleet\.mjs' start/, started],
    [/fleet\.mjs' wait/, done([{ job: "correctness", step: "review", ok: true, output: "" }, { job: "tests", step: "review", ok: true, output: "" }])],
    [/^node -e /, "0 2"],
  ]);
  const verdict = await runReview(green.f, { flow: "adversarial-review", prepared, lenses, clis: ["claude"], model: "claude-fable-5-1", preamble: "P" });
  assert.equal(verdict.reason, "success");
  const inputs = writtenInputs(green.runs);
  assert.equal(inputs.length, 2);
  assert.ok(inputs.every((input) => input.model === "claude-fable-5-1"), JSON.stringify(inputs.map((i) => i.model)));
  assert.equal(green.agents.length, 1);
  assert.equal(green.agents[0].name, "verifier");
  assert.equal(green.agents[0].options.model, "claude-fable-5-1");
  assert.ok(green.runs.some((c) => c.includes("review-state.mjs' save 'adversarial-review'")), "a clean round is saved");

  const taken = fakeCtx([
    [/git hash-object --stdin/, "fingerprint"],
    [/fleet\.mjs' start/, started],
    [/fleet\.mjs' wait/, done([{ job: "fleet", step: "fleet", ok: false, output: PREEMPTED }])],
  ]);
  const lost = await runReview(taken.f, { flow: "adversarial-review", prepared, lenses, clis: ["claude"], model: "claude-fable-5-1", preamble: "P" });
  assert.equal(lost.reason, "step_failed");
  assert.match(lost.detail, /preempted by a newer review/);
  assert.equal(taken.agents.length, 0, "no verifier ran");
  assert.ok(!taken.runs.some((c) => c.includes("review-state.mjs' save")), "nothing was saved");

  const plain = fakeCtx([
    [/git hash-object --stdin/, "fingerprint"],
    [/fleet\.mjs' start/, started],
    [/fleet\.mjs' wait/, done([{ job: "correctness", step: "review", ok: true, output: "" }, { job: "tests", step: "review", ok: true, output: "" }])],
    [/^node -e /, "0 0"],
  ]);
  await runReview(plain.f, { flow: "adversarial-review", prepared, lenses, clis: ["claude"], preamble: "P" });
  assert.ok(writtenInputs(plain.runs).every((input) => !("model" in input)), "no model key when none was given");
  assert.ok(!("model" in plain.agents[0].options));
});

test("resolveBase explains a fetch that overran its lease, a name that is no commit, and unrelated history, and passes a good base", async () => {
  const lease = fakeCtx([[/git fetch/, () => { throw Object.assign(new Error("lease timeout: 3m elapsed"), { code: "lease_exceeded" }); }]]);
  await assert.rejects(resolveBase(lease.f, "origin/main"), /could not be fetched: the fetch did not finish within its lease \(lease timeout: 3m elapsed\)\. Check the network or the VPN/, "a lease overrun ends the round with the explanation");
  assert.match(fetchProblem("origin/x", { exitCode: 128, stderr: "fatal: Remote branch x not found in upstream origin\n" }), /The remote has no such branch/);
  assert.match(fetchProblem("origin/x", { exitCode: 128, stderr: "fatal: invalid refspec 'x'\n" }), /The remote has no such branch/);
  const killed = fakeCtx([[/git fetch/, () => { throw Object.assign(new Error("step never ran"), { code: "worker_lost" }); }]]);
  await assert.rejects(() => resolveBase(killed.f, "origin/main"), /step never ran/, "anything but a lease overrun is the runtime's to report");
  const cutOff = fakeCtx([[/git fetch/, { exitCode: 124, stdout: "", stderr: "" }]]);
  assert.match(await resolveBase(cutOff.f, "origin/main"), /could not be fetched: the fetch was cut off by the timeout program after 50 seconds/);
  const silent = fakeCtx([[/git fetch/, { exitCode: 1, stdout: "", stderr: "" }]]);
  assert.match(await resolveBase(silent.f, "origin/main"), /failed with no message \(exit 1\)/, "any silent failure names its exit code");
  const refused = fakeCtx([[/git fetch/, { exitCode: 128, stdout: "", stderr: "fatal: could not read from remote" }]]);
  assert.match(await resolveBase(refused.f, "origin/main"), /could not be fetched: fatal: could not read from remote/);
  const warned = fakeCtx([[/git fetch/, { exitCode: 124, stdout: "", stderr: "Warning: Permanently added github.com to the list of known hosts." }]]);
  assert.match(await resolveBase(warned.f, "origin/main"), /Warning: Permanently added github.com to the list of known hosts\.; the fetch was cut off by the timeout program after 50 seconds/, "a warning never hides the cut-off");
  assert.equal(fetchProblem("origin/main", { exitCode: 124, stderr: "" }), `base origin/main could not be fetched: the fetch was cut off by the timeout program after 50 seconds without finishing (a black-holed remote, or one too slow for the cap). Check the network or the VPN, or pass a local branch or commit as the base (the review then compares against what this checkout has)`, "the CI flow's message is the same text");
  assert.match(fetchProblem("origin/main", { exitCode: 128, stderr: "fatal: unable to access 'https://github.com/': Could not resolve host\n" }), /could not be fetched: fatal: unable to access.*\. Check the network or the VPN, or pass a local branch or commit as the base/, "an ordinary failure gets the remedy too");
  const badPrepare = fakeCtx([[/git fetch/, ""], [/rev-parse --verify/, { exitCode: 1, stdout: "", stderr: "" }], [/^git remote$/, "origin\n"]]);
  await assert.rejects(prepare(badPrepare.f, "adversarial-review", { base: "origin/nope" }), /base origin\/nope is not a commit here/, "prepare fails the step with the same explanation");
  assert.match(fetchProblem("origin/nope", { exitCode: 128, stderr: "fatal: couldn't find remote ref nope\n" }), /The remote has no such branch: pass a base the remote has$/, "a missing branch is not a network problem");
  assert.equal(fetchProblem("origin/+refs/heads/x:refs/heads/main", { exitCode: 1, stderr: "base origin/+refs/heads/x:refs/heads/main is not a branch name\n" }), "base origin/+refs/heads/x:refs/heads/main could not be fetched: it is not a branch name. The part after the remote must be a branch name", "a bad base name gets its own remedy, and the base is named once");
  const noRemote = fakeCtx([[/rev-parse --verify/, { exitCode: 1, stdout: "", stderr: "" }], [/^git remote$/, ""]]);
  assert.match(await resolveBase(noRemote.f, "main"), /this checkout has no remote: add one and fetch the branch, or pass a local branch that exists/);
  const unknown = fakeCtx([[/git fetch/, ""], [/rev-parse --verify/, { exitCode: 1, stdout: "", stderr: "" }], [/^git remote$/, "origin\nupstream\n"]]);
  assert.match(await resolveBase(unknown.f, "mian"), /base mian is not a commit here: fetch it, or pass base as <remote>\/<branch> for a remote this checkout has \(origin, upstream\)/);
  const unrelated = fakeCtx([[/git fetch/, ""], [/rev-parse --verify/, "abc"], [/merge-base/, { exitCode: 1, stdout: "", stderr: "" }]]);
  assert.match(await resolveBase(unrelated.f, "origin/other"), /shares no history/);
  const good = fakeCtx([[/git fetch/, ""], [/rev-parse --verify/, "abc"], [/merge-base/, "mb"]]);
  assert.equal(await resolveBase(good.f, "origin/main"), undefined);
  assert.ok(good.runs[0].includes("git fetch"), "a remote base is fetched first");
});

test("every agent is told where a copy of the repository may go: outside the run root, never in its scratch dir", () => {
  const task = verifierAgent({ cli: "claude", dir: "/state/adversarial-review", root: "/repo", preamble: "", files: "/state/files.txt", paths: [], open: 0, previous: "/state/previous.json", report: "/state/findings.json", scopeFiles: "/state/scope.txt" }).task;
  assert.match(task, /Never copy the repository, or any part of it, into your working directory or anywhere under the repository/);
  assert.match(task, /goes in a directory from `mktemp -d`,\s*outside both; delete it when you are done/);
  assert.ok(!/not \/tmp/.test(task), "no line contradicts it");
  const reviewer = reviewerInputs({ lenses: [{ name: "correctness", prompt: "x" }], clis: ["claude"], dir: "/state/adversarial-review", root: "/repo", patch: "/state/change.patch", files: "/state/files.txt", scopeFiles: "/state/scope.txt", previous: "/state/previous.json", open: 0, preamble: "", stateRoot: "/state", full: true, changed: [] })[0];
  assert.match(reviewer.task, /Never copy the repository, or any part of it/, "a reviewer is told the same");
});

test("a review fleet waits in its short polls", async () => {
  const { f, runs } = fakeCtx([
    [/fleet\.mjs' start/, started],
    [/fleet\.mjs' wait/, done([{ job: "correctness", step: "review", ok: true, output: "" }, { job: "tests", step: "review", ok: true, output: "" }])],
  ]);
  await reviewFleet(f, { ...fleetOptions, model: "claude-opus-5-5" });
  const waits = runs.filter((c) => c.includes("fleet.mjs' wait"));
  assert.ok(waits.length > 0 && waits.every((c) => c.endsWith(` ${REVIEW_POLL_MINUTES}`)), waits.join("\n"));
});
