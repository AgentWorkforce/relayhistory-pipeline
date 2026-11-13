// flows/cloud-review.flow.ts (the round as a Cloud flow run) and
// flows/lib/plan.ts (the sizing it shares with sized-review): the flow stays
// self-contained, asks exactly what a local round asks, runs every lens on
// the lead CLI, and ends on a verified report; the plan helper skips Jev only
// for an author's depth.
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { packWorkingTree } from "@relayflows/sdk";
import { getFlowDefinition } from "@relayflows/surface/runtime";
import cloudReview, {
  CAP_SLACK_MINUTES,
  DEPTH_PLANS,
  inputDescribesTree,
  lensesFor,
  manifestDifference,
  metaHead,
  MIN_REVIEWER_MINUTES,
  modelFor,
  reviewerTask,
  scopeDifference,
  secondOpinions,
  secondOpinionsFor,
  verifierTask,
  worstCaseMinutes,
} from "../cloud-review.flow.ts";
import { reviewerInputs, verifierAgent } from "./review.ts";

const FILES = "cloud-review/input/files.txt";

/** A Ctx that records every run and agent in order; `answers` map a command pattern to its output. */
function fakeCtx(answers, { failAgents = [] } = {}) {
  const events = [];
  const agents = [];
  const done = [];
  const run = async (command, options = {}) => {
    events.push(`run ${command}`);
    const hit = answers.find(([pattern]) => pattern.test(command));
    const out = hit ? hit[1] : "";
    if (options.onNonZero === "record") return typeof out === "object" ? out : { exitCode: 0, ok: true, stdout: out, stderr: "", output: out };
    return typeof out === "object" ? out.stdout : out;
  };
  const agent = (name, options) => {
    events.push(`agent ${name}`);
    agents.push({ name, options });
    return {
      gate: async () => {
        if (failAgents.some((p) => p.test(name))) throw new Error(`${name} gate failed`);
        return {};
      },
    };
  };
  return { f: { run, agent, done: (reason, opts) => done.push({ reason, ...opts }) }, events, agents, done };
}

const written = (events, path) => {
  const line = events.find((e) => e.startsWith("run printf %s ") && e.endsWith(`base64 -d > '${path}'`));
  return line ? Buffer.from(line.match(/^run printf %s '([^']*)'/)[1], "base64").toString("utf8") : undefined;
};

const HEAD = "a".repeat(40);
const record = (exitCode, stdout = "", stderr = "") => ({ exitCode, ok: exitCode === 0, stdout, stderr, output: stdout });

// The sandbox: Cloud's own commit of the upload (never meta.json's head), its index matching the manifest,
// clean, its patch matching files.txt.
const sandbox = (extra = []) => [
  ...extra,
  [/^node -e .* 'cloud-review\/input\/meta\.json'$/s, JSON.stringify({ head: HEAD })],
  [/^node -e .* 'cloud-review\/input\/change\.patch' 'cloud-review\/input\/files\.txt'$/s, "[]"],
  [/^git rev-parse --verify -q HEAD$/, "c".repeat(40)],
  [/^d=\$\(mktemp -d .*relay-index\.XXXXXX"\) && echo "\$d" && git ls-files -s -z > "\$d\/index"$/, "/tmp/relay-index.abc\n"],
  [/^node -e .* 'cloud-review\/input\/manifest' '\/tmp\/relay-index\.abc\/index'$/s, "[]"],
  [/^mktemp -d/, "/tmp/relay-context-intent.abc"],
  [/^git rev-parse --show-toplevel/, "/sandbox/repo"],
  [/git hash-object --stdin$/, "fingerprint"],
  [/^command -v 'grok'/, { exitCode: 1, ok: false, stdout: "", stderr: "", output: "" }],
  [/^node -e /, "1 3"],
];

const body = getFlowDefinition(cloudReview).body;

test("cloud-review.flow.ts imports only @relayflows/surface: the Cloud bundle is that one module", () => {
  const source = readFileSync(new URL("../cloud-review.flow.ts", import.meta.url), "utf8");
  const imports = [...source.matchAll(/^\s*import\s[^;]*?from\s+["']([^"']+)["']/gm)].map((m) => m[1]);
  assert.deepEqual(imports, ["@relayflows/surface"]);
  assert.ok(!/\bimport\(/.test(source), "no dynamic import");
  assert.ok(!/require\(\s*["']\./.test(source), "no relative require");
});

test("a Cloud reviewer and verifier are asked exactly what a local round asks them", () => {
  const lens = { name: "correctness", brief: "Find logic errors." };
  const [local] = reviewerInputs({ flow: "adversarial-review", root: "/r", dir: "d", files: FILES, lenses: [lens], clis: ["claude"], preamble: "P" });
  assert.equal(reviewerTask("/r", "P", lens), local.task);
  const verifier = verifierAgent({ root: "/r", dir: "d", files: FILES, scopeFiles: FILES, previous: "p", open: 0, paths: ["a", "b"], cli: "claude", preamble: "P" });
  assert.equal(verifierTask("/r", "P", ["a", "b"]), verifier.task);
});

test("second opinions: claude, codex and grok when asked, never one on the lead's CLI; only claude takes a model", () => {
  assert.deepEqual(secondOpinions("claude", undefined), []);
  assert.deepEqual(secondOpinions("claude", { codex: true, grok: true }).map((l) => l.name), ["codex-second-opinion", "grok-second-opinion"]);
  assert.deepEqual(secondOpinions("codex", { codex: true, grok: true }).map((l) => l.name), ["grok-second-opinion"]);
  assert.deepEqual(secondOpinions("codex", { claude: true, codex: true, grok: true }).map((l) => l.cli), ["claude", "grok"]);
  assert.deepEqual(secondOpinions("claude", { claude: true }), []);
  assert.deepEqual(modelFor("claude", "claude-opus-5-5"), { model: "claude-opus-5-5" });
  assert.deepEqual(modelFor("codex", "claude-opus-5-5"), {});
  assert.deepEqual(modelFor("grok", "claude-opus-5-5"), {});
  assert.deepEqual(modelFor("claude", undefined), {});
});

test("a standard round runs both fleets on the lead with the plan's model, removes the intent first, and writes the summary", async () => {
  const { f, events, agents, done } = fakeCtx(sandbox());
  await body(f, { depth: "standard", flows: ["context-review", "adversarial-review"], model: "claude-opus-5-5", lead: "claude", lenses: { codex: true, grok: true }, dir: "cloud-review" });
  assert.deepEqual(done.map((d) => d.reason), ["success"], JSON.stringify(done));
  const rmIntent = events.findIndex((e) => e.includes("rm -f 'cloud-review/input/intent.md'"));
  const firstAgent = events.findIndex((e) => e.startsWith("agent "));
  assert.ok(rmIntent >= 0 && rmIntent < firstAgent, "intent.md is gone before any reviewer starts");
  const leads = agents.filter((a) => !a.name.includes("second-opinion"));
  assert.deepEqual(leads.map((a) => a.name), [
    "adversarial-review-correctness", "adversarial-review-security", "adversarial-review-failure-modes", "adversarial-review-verifier",
    "context-review-intent", "context-review-verifier",
  ], "standard's lenses, in plan order, and a verifier per fleet");
  assert.ok(leads.every((a) => a.options.cli === "claude" && a.options.model === "claude-opus-5-5"), JSON.stringify(leads.map((a) => a.options.cli)));
  assert.ok(agents.every((a) => a.options.permissions?.accessPreset === "readonly"));
  assert.deepEqual(agents.map((a) => a.options.timeout), ["8m", "8m", "8m", "8m", "7m", "8m", "7m"], "reviewers 8m, the second opinion too; verifiers 7m");
  assert.match(agents[0].options.task, /Aim to finish within six minutes\. Write findings\.json with an empty findings array first/);
  assert.match(agents.find((a) => a.name.endsWith("verifier")).options.task, /Aim to finish within five minutes: you are cut off two minutes later/);
  assert.ok(agents.find((a) => a.name === "adversarial-review-correctness").options.cwd === "cloud-review/adversarial-review/correctness");
  assert.ok(agents.find((a) => a.name === "context-review-intent").options.task.includes("/tmp/relay-context-intent.abc/intent.md"));
  assert.ok(!agents.filter((a) => a.name.startsWith("adversarial")).some((a) => a.options.task.includes("intent.md")), "the adversarial fleet is never pointed at the intent");
  assert.deepEqual(agents.filter((a) => a.name.includes("second-opinion")).map((a) => [a.name, a.options.cli, "model" in a.options]), [["adversarial-review-codex-second-opinion", "codex", false]]);
  const summary = JSON.parse(written(events, "cloud-review/summary.json"));
  assert.deepEqual(summary.flows["adversarial-review"], { blocking: 1, total: 3, report: "cloud-review/adversarial-review/report.md" });
  assert.deepEqual(summary.lensesUnavailable, ["grok-second-opinion: a standard round has room for one second opinion within its 60-minute cap"]);
  assert.deepEqual(summary.lenses, ["codex-second-opinion"], "only the second opinions that ran");
  assert.ok(!events.includes("run command -v 'grok'"), "a dropped second opinion is not even looked for");
  assert.equal(summary.lead, "claude");
  assert.ok(events.some((e) => e.includes("## Lenses unavailable") || /base64 -d > 'cloud-review\/adversarial-review\/\.lenses-note'/.test(e)), "the note reaches report.md");
  assert.ok(events.at(-1) !== undefined && events.some((e) => e === "run rm -rf '/tmp/relay-context-intent.abc'"), "the intent copy is removed");
});

test("the default lead is claude: every lens and verifier on claude with the plan's model; a codex extra lens takes none", async () => {
  const { f, agents, done, events } = fakeCtx(sandbox());
  await body(f, { depth: "deep", flows: ["adversarial-review"], model: "claude-sonnet-5-5", lenses: { claude: true, codex: true }, dir: "cloud-review" });
  assert.equal(done[0].reason, "success");
  assert.equal(agents.length, 6, "deep's 4 lenses, a verifier, and the codex second opinion; claude beside a claude lead is ignored");
  const leads = agents.filter((a) => !a.name.includes("second-opinion"));
  assert.ok(leads.every((a) => a.options.cli === "claude" && a.options.model === "claude-sonnet-5-5"));
  assert.deepEqual(agents.filter((a) => a.name.includes("second-opinion")).map((a) => [a.name, a.options.cli, "model" in a.options]), [["adversarial-review-codex-second-opinion", "codex", false]]);
  assert.equal(JSON.parse(written(events, "cloud-review/summary.json")).lead, "claude");
});

test("lead codex: every lens and verifier on codex with no model; a claude extra lens takes the plan's model", async () => {
  const { f, agents, done, events } = fakeCtx(sandbox());
  await body(f, { depth: "deep", flows: ["adversarial-review"], model: "claude-sonnet-5-5", lead: "codex", lenses: { claude: true, codex: true }, dir: "cloud-review" });
  assert.equal(done[0].reason, "success");
  assert.equal(agents.length, 6, "deep's 4 lenses, a verifier, and the claude second opinion; codex beside a codex lead is ignored");
  const leads = agents.filter((a) => !a.name.includes("second-opinion"));
  assert.ok(leads.every((a) => a.options.cli === "codex" && !("model" in a.options)));
  assert.deepEqual(agents.filter((a) => a.name.includes("second-opinion")).map((a) => [a.name, a.options.cli, a.options.model]), [["adversarial-review-claude-second-opinion", "claude", "claude-sonnet-5-5"]]);
  assert.equal(JSON.parse(written(events, "cloud-review/summary.json")).lead, "codex");
});

test("a verifier that leaves no valid report fails its flow in summary.json; the run still succeeds and names it", async () => {
  const { f, events, done } = fakeCtx(
    sandbox([[/check-findings\.mjs 'cloud-review\/adversarial-review\/(report|failure-modes)/, { exitCode: 1, ok: false, stdout: "", stderr: "not readable JSON", output: "" }]]),
    { failAgents: [/verifier$/, /-failure-modes$/] },
  );
  await body(f, { depth: "small", flows: ["adversarial-review"], model: "claude-sonnet-5-5", dir: "cloud-review" });
  assert.equal(done[0].reason, "success", "summary.json was written: the failed flow is the round's verdict, not the run's");
  assert.match(done[0].detail, /^failed: adversarial-review: the verifier failed: adversarial-review-verifier gate failed/);
  const summary = JSON.parse(written(events, "cloud-review/summary.json"));
  assert.match(summary.flows["adversarial-review"].failed, /verifier failed/);
});

test("a red agent step whose file still passes the gate counts, and blocking findings are a verdict, not a failure", async () => {
  const { f, done } = fakeCtx(sandbox(), { failAgents: [/-security$/] });
  await body(f, { depth: "small", flows: ["adversarial-review"], dir: "cloud-review" });
  assert.equal(done[0].reason, "success");
  assert.match(done[0].detail, /1 blocker\/major/);
});

test("a context-review with no intent.md, or a reviewer that edited the tree, fails the flows in summary.json; the run still succeeds", async () => {
  const noIntent = fakeCtx(sandbox([[/^if \[ -s 'cloud-review\/input\/intent\.md' \]/, { exitCode: 1, ok: false, stdout: "", stderr: "", output: "" }]]));
  await body(noIntent.f, { depth: "standard", flows: ["adversarial-review", "context-review"], dir: "cloud-review" });
  assert.equal(noIntent.done[0].reason, "success");
  assert.match(noIntent.done[0].detail, /^failed: context-review: no intent\.md/);
  assert.match(JSON.parse(written(noIntent.events, "cloud-review/summary.json")).flows["context-review"].failed, /no intent\.md/);
  let n = 0;
  const edited = fakeCtx(sandbox());
  const run = edited.f.run;
  edited.f.run = async (command, options) => (/git hash-object --stdin$/.test(command) ? `fp${n++}` : run(command, options));
  await body(edited.f, { depth: "small", flows: ["adversarial-review"], dir: "cloud-review" });
  assert.equal(edited.done[0].reason, "success");
  assert.match(edited.done[0].detail, /^failed: adversarial-review: a reviewer changed the working tree/);
  assert.match(JSON.parse(written(edited.events, "cloud-review/summary.json")).flows["adversarial-review"].failed, /changed the working tree/);
});

test("bad input is refused before anything runs", async () => {
  for (const input of [
    { depth: "small", flows: ["adversarial-review"], dir: "elsewhere" },
    { depth: "small", flows: ["adversarial-review"], lead: "grok", dir: "cloud-review" },
    { depth: "huge", flows: ["adversarial-review"], dir: "cloud-review" },
    { depth: "small", flows: ["ci"], dir: "cloud-review" },
    { depth: "small", flows: ["adversarial-review", "context-review"], dir: "cloud-review" },
    { depth: "small", flows: ["context-review"], dir: "cloud-review" },
  ]) {
    const { f, agents, done } = fakeCtx(sandbox());
    await body(f, input);
    assert.equal(done[0].reason, "needs_human", JSON.stringify(input));
    assert.equal(agents.length, 0);
  }
  const missing = fakeCtx(sandbox([[/^for p in /, "cloud-review/input/change.patch"]]));
  await body(missing.f, { depth: "small", flows: ["adversarial-review"], dir: "cloud-review" });
  assert.equal(missing.done[0].reason, "step_failed");
  assert.equal(missing.agents.length, 0);
  assert.equal(written(missing.events, "cloud-review/summary.json"), undefined, "step_failed only for a run that wrote no summary");
});

test("a Fable model is refused needs_human before anything runs, whatever the lead", async () => {
  for (const lead of ["claude", "codex"]) {
    const { f, events, agents, done } = fakeCtx(sandbox());
    await body(f, { depth: "standard", flows: ["adversarial-review"], model: "claude-fable-5-1", lead, lenses: { claude: true }, dir: "cloud-review" });
    assert.equal(done[0].reason, "needs_human", lead);
    assert.equal(done[0].detail, 'model must be a Claude Sonnet or Opus model, not "claude-fable-5-1"; Fable is the persistent lead and never a worker');
    assert.equal(agents.length, 0);
    assert.deepEqual(events, [], "nothing runs");
  }
});

test("the input must describe the synced tree: the patch's paths, the manifest against the index, nothing changed on disk, and a git checkout", async () => {
  const refused = async (extra, detail) => {
    const { f, events, agents, done } = fakeCtx(sandbox(extra));
    await body(f, { depth: "small", flows: ["adversarial-review"], dir: "cloud-review" });
    assert.equal(done[0].reason, "step_failed", JSON.stringify(done));
    assert.match(done[0].detail, detail);
    assert.equal(agents.length, 0, "no reviewer starts");
    assert.ok(!events.some((e) => e.startsWith("run mktemp")), "refused before the intent is moved");
    assert.equal(written(events, "cloud-review/summary.json"), undefined);
  };
  // (a) meta.json with no string head.
  await refused([[/'cloud-review\/input\/meta\.json'$/, JSON.stringify({ why: "no string head" })]], /^cloud-review\/input\/meta\.json is unreadable: no string head$/);
  // (c) the patch and files.txt disagree, or the check could not read them.
  await refused([[/'cloud-review\/input\/files\.txt'$/, JSON.stringify(["only in the patch: flows/ci.flow.ts", "only in files.txt: app.txt"])]],
    /^the patch and cloud-review\/input\/files\.txt name different paths: only in the patch: flows\/ci\.flow\.ts; only in files\.txt: app\.txt$/);
  await refused([[/'cloud-review\/input\/files\.txt'$/, record(1, "", "ENOENT")]], /name different paths: the check did not run$/);
  // (d) the index differs from the manifest; the detail names both commits and the files.
  await refused([[/'cloud-review\/input\/manifest' '\/tmp\/relay-index\.abc\/index'$/, JSON.stringify(["missing 1: app.txt", "extra 1: stray.txt"])]],
    new RegExp(`^the synced tree \\(sandbox HEAD ${"c".repeat(40)}\\) is not the round's input \\(head ${HEAD}\\): its index differs from cloud-review/input/manifest: missing 1: app\\.txt; extra 1: stray\\.txt$`));
  await refused([[/'cloud-review\/input\/manifest' '\/tmp\/relay-index\.abc\/index'$/, record(1, "", "SyntaxError")]], /: the comparison with cloud-review\/input\/manifest did not run$/);
  // (e) a matching index, but changes on disk outside cloud-review/.
  await refused([[/^git status --porcelain --untracked-files=all -- \. ':!:cloud-review' ':!:\.relayflow' ':!:\.relayflowd'$/, " M app.txt\n?? stray.txt\n?? b\n?? c\n"]],
    /\): it has changes outside cloud-review\/ before any reviewer started: app\.txt, stray\.txt, b \(\+1 more\)$/);
  // (f) no manifest in the input.
  await refused([[/^test -s 'cloud-review\/input\/manifest'$/, record(1)]], /\): the input has no cloud-review\/input\/manifest$/);
  // (g) no git checkout: the manifest cannot be verified, so the round is refused.
  await refused([[/^git rev-parse --verify -q HEAD$/, record(128, "", "fatal: not a git repository")]],
    new RegExp(`^the synced tree has no git checkout, so cloud-review/input/manifest cannot be verified against it \\(the round's input says ${HEAD}\\)$`));
  // A sandbox at head itself is checked the same way: no shortcut.
  const atHead = fakeCtx(sandbox([[/^git rev-parse --verify -q HEAD$/, HEAD], [/'cloud-review\/input\/manifest' '\/tmp\/relay-index\.abc\/index'$/, JSON.stringify(["changed 1: app.txt (content)"])]]));
  await body(atHead.f, { depth: "small", flows: ["adversarial-review"], dir: "cloud-review" });
  assert.equal(atHead.done[0].reason, "step_failed");
  assert.ok(atHead.events.includes("run rm -rf '/tmp/relay-index.abc'"), "the index listing is removed");
  // Cloud's commit, index matching, clean: the round runs, whatever HEAD is.
  const ok = fakeCtx(sandbox());
  await body(ok.f, { depth: "small", flows: ["adversarial-review"], dir: "cloud-review" });
  assert.equal(ok.done[0].reason, "success", JSON.stringify(ok.done));
  assert.ok(!ok.events.some((e) => /git (cat-file|diff --quiet)/.test(e)), "the sandbox's HEAD is never compared with head");
});

test("manifestDifference: missing, changed (mode, content, unmerged) and extra entries, a few each with counts; only cloud-review/ and the runtime's state left out", () => {
  const entry = (mode, id, path, stage = 0) => `${mode} ${id} ${stage}\t${path}\0`;
  const A = "a".repeat(40);
  const B = "b".repeat(40);
  const want = entry("100644", A, "app.txt") + entry("100755", A, "run.sh") + entry("100644", A, "gone.txt") + entry("100644", A, "cloud-review/input/meta.json");
  assert.deepEqual(manifestDifference(want, want), []);
  const have = entry("100644", B, "app.txt") + entry("100644", A, "run.sh") + entry("100644", A, "new.txt") +
    entry("100644", A, ".relayflow/x") + entry("100644", A, ".relayflowd/y") + entry("100644", A, "cloud-review/summary.json") + entry("100644", A, ".relayflowx");
  assert.deepEqual(manifestDifference(want, have), [
    "missing 1: gone.txt",
    "changed 2: app.txt (content), run.sh (mode 100755 -> 100644)",
    "extra 2: new.txt, .relayflowx",
  ]);
  const many = ["a", "b", "c", "d", "e"].map((p) => entry("100644", A, p)).join("");
  assert.deepEqual(manifestDifference(many, ""), ["missing 5: a, b, c (+2 more)"]);
  assert.deepEqual(manifestDifference(entry("100644", A, "x"), entry("100644", A, "x", 2)), ["changed 1: x (mode 100644 -> 100644 stage 2)"]);
  assert.deepEqual(manifestDifference("100644 blob " + A + "\tapp.txt\0", want), ["the manifest is not a `git ls-files -s -z` listing"]);
  assert.deepEqual(manifestDifference(want, "garbage"), ["the sandbox's index listing is not a `git ls-files -s -z` listing"]);
});

test("scopeDifference reads a patch as review.sh does: headers, renames and quoted paths; a copy's source is not a changed path", () => {
  const patch = [
    "diff --git a/keep.txt b/moved.txt", "similarity index 98%", "rename from keep.txt", "rename to moved.txt",
    "diff --git a/x b/y.txt b/x b/y.txt", "--- a/x b/y.txt", "+++ b/x b/y.txt", "+diff --git a/not b/a-header",
    'diff --git "a/\\303\\251.txt" "b/\\303\\251.txt"',
    "diff --git a/src.txt b/copy.txt", "copy from src.txt", "copy to copy.txt",
  ].join("\n");
  const files = ["keep.txt", "moved.txt", "x b/y.txt", '"\\303\\251.txt"', "copy.txt", ""].join("\n");
  assert.deepEqual(scopeDifference(patch, files), []);
  assert.deepEqual(scopeDifference(patch, "keep.txt\nmoved.txt\nextra.txt\n"), [
    'only in the patch: "\\303\\251.txt", copy.txt, x b/y.txt',
    "only in files.txt: extra.txt",
  ]);
  assert.deepEqual(scopeDifference("diff --git weird\n", ""), ["only in the patch: weird"], "a header in no known shape names its whole rest");
  assert.deepEqual(metaHead('{"head":"abc"}'), { head: "abc" });
  assert.deepEqual(metaHead('{"head":1}'), { why: "no string head" });
  assert.match(metaHead('{"head":').why, /JSON/);
});

test("each depth fits its cap at worst, second opinions and verifiers included, with slack for the sandbox's own steps", () => {
  assert.deepEqual(Object.fromEntries(Object.entries(DEPTH_PLANS).map(([d, p]) => [d, p.cap])), { small: 30, standard: 60, deep: 90 });
  assert.deepEqual({ small: worstCaseMinutes("small"), standard: worstCaseMinutes("standard"), deep: worstCaseMinutes("deep") }, { small: 24, standard: 54, deep: 80 });
  for (const [depth, plan] of Object.entries(DEPTH_PLANS)) {
    assert.ok(worstCaseMinutes(depth) + CAP_SLACK_MINUTES <= plan.cap, `${depth}: ${worstCaseMinutes(depth)} + ${CAP_SLACK_MINUTES} > ${plan.cap}`);
    assert.ok(plan.reviewerMinutes >= MIN_REVIEWER_MINUTES && plan.verifierMinutes >= MIN_REVIEWER_MINUTES, depth);
  }
  // The adversarial fleet alone at standard or deep is within the cap too.
  assert.equal(worstCaseMinutes("standard", ["adversarial-review"]), 39);
  assert.equal(worstCaseMinutes("deep", ["adversarial-review"]), 56);
});

test("per depth: the blocker lenses always, small adversarial only, deep adds platform and verification", () => {
  const names = (flow, depth) => lensesFor(flow, depth).map((l) => l.name);
  for (const depth of ["small", "standard", "deep"]) {
    for (const lens of ["correctness", "security", "failure-modes"]) assert.ok(names("adversarial-review", depth).includes(lens), `${depth} ${lens}`);
  }
  assert.deepEqual(names("adversarial-review", "small"), ["correctness", "security", "failure-modes"]);
  assert.deepEqual(names("context-review", "small"), []);
  assert.deepEqual(names("adversarial-review", "standard"), ["correctness", "security", "failure-modes"]);
  assert.deepEqual(names("context-review", "standard"), ["intent"]);
  assert.deepEqual(names("adversarial-review", "deep"), ["correctness", "security", "failure-modes", "platform"]);
  assert.deepEqual(names("context-review", "deep"), ["intent", "verification"]);
  assert.ok(lensesFor("adversarial-review", "deep").every((l) => l.brief.length > 50), "the full briefs");
});

test("second opinions past a depth's room are dropped in asked order and named", () => {
  const all = { claude: true, codex: true, grok: true };
  assert.deepEqual(secondOpinionsFor("small", "claude", all), {
    run: [],
    dropped: [
      "codex-second-opinion: a small round has room for no second opinion within its 30-minute cap",
      "grok-second-opinion: a small round has room for no second opinion within its 30-minute cap",
    ],
  });
  assert.deepEqual(secondOpinionsFor("standard", "codex", all).run.map((l) => l.name), ["claude-second-opinion"]);
  assert.deepEqual(secondOpinionsFor("standard", "codex", all).dropped, ["grok-second-opinion: a standard round has room for one second opinion within its 60-minute cap"]);
  assert.deepEqual(secondOpinionsFor("deep", "claude", all), { run: secondOpinions("claude", all), dropped: [] });
  assert.deepEqual(secondOpinionsFor("deep", "claude", undefined), { run: [], dropped: [] });
});

test("a small round runs its three lenses and verifier at 6m each, no second opinion, and lists the asked ones as unavailable", async () => {
  const { f, agents, done, events } = fakeCtx(sandbox());
  await body(f, { depth: "small", flows: ["adversarial-review"], model: "claude-sonnet-5-5", lenses: { codex: true }, dir: "cloud-review" });
  assert.equal(done[0].reason, "success");
  assert.deepEqual(agents.map((a) => [a.name, a.options.timeout]), [
    ["adversarial-review-correctness", "6m"], ["adversarial-review-security", "6m"], ["adversarial-review-failure-modes", "6m"], ["adversarial-review-verifier", "6m"],
  ]);
  assert.match(agents[0].options.task, /Aim to finish within four minutes\./);
  const summary = JSON.parse(written(events, "cloud-review/summary.json"));
  assert.deepEqual(summary.lenses, []);
  assert.deepEqual(summary.lensesUnavailable, ["codex-second-opinion: a small round has room for no second opinion within its 30-minute cap"]);
});

test("a deep round runs six lenses, two second opinions and two verifiers at 8m each", async () => {
  const { f, agents, done } = fakeCtx(sandbox([[/^command -v 'grok'/, "/usr/bin/grok"]]));
  await body(f, { depth: "deep", flows: ["adversarial-review", "context-review"], model: "claude-opus-5-5", lenses: { codex: true, grok: true }, dir: "cloud-review" });
  assert.equal(done[0].reason, "success");
  assert.deepEqual(agents.map((a) => a.name.replace(/^(adversarial|context)-review-/, "$1:")), [
    "adversarial:correctness", "adversarial:security", "adversarial:failure-modes", "adversarial:platform",
    "adversarial:codex-second-opinion", "adversarial:grok-second-opinion", "adversarial:verifier",
    "context:intent", "context:verification", "context:verifier",
  ]);
  assert.ok(agents.every((a) => a.options.timeout === "8m"));
  assert.equal(agents.length * 8, worstCaseMinutes("deep"));
});

test("the flow's static wallclock is the deep cap; review.sh holds each depth to its own", () => {
  const source = readFileSync(new URL("../cloud-review.flow.ts", import.meta.url), "utf8");
  assert.deepEqual([...source.matchAll(/wallclock:\s*"([^"]*)"/g)].map((m) => m[1]), [`${DEPTH_PLANS.deep.cap}m`]);
  assert.equal(Math.max(...Object.values(DEPTH_PLANS).map((p) => p.cap)), DEPTH_PLANS.deep.cap);
});
