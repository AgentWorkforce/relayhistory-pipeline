import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { existsSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

import { spawn } from "node:child_process";
import { failedTests, jobEnv, PREEMPTED, SECRET_NAME, stopPrevious, stopStep, wait } from "./fleet.mjs";
import { preempted, PREEMPTED as PREEMPTED_TS } from "./fleet.ts";

const FLEET = fileURLToPath(new URL("./fleet.mjs", import.meta.url));
// The pids files go to a temp dir, never the checkout's .relayflow/.fleets.
const PIDS = mkdtempSync(join(tmpdir(), "fleet-pids-"));
const env = { ...process.env, FLEET_POLL_MS: "200", FLEET_WAIT_MS: "20000", FLEET_PIDS_DIR: PIDS };
// The module imported in-process reads the same pids directory as the fleets it inspects.
process.env.FLEET_PIDS_DIR = PIDS;
const pidsFile = (dir) => `${PIDS}/${dir.replace(/[^A-Za-z0-9._-]+/g, "_")}.pids`;
const firstPid = (dir) => Number(readFileSync(pidsFile(dir), "utf8").split("\n")[0].split(" ")[0]);
const gone = (pattern) => execFileSync("sh", ["-c", `pgrep -f '${pattern}' || true`], { encoding: "utf8" }).trim() === "";
// Names only this run's background children carry, so pgrep finds just them
// even while another copy of the suite runs beside this one.
const GRANDCHILD = `fleet-timeout-grandchild-${process.pid}`;
const MARKER = `fleet-stop-marker-${process.pid}`;
// `[x]yz` matches "xyz" but not the pattern's own pgrep command line.
const own = (name) => `[${name[0]}]${name.slice(1)}`;
const fleet = (...args) => execFileSync(process.execPath, [FLEET, ...args], { encoding: "utf8", env });

function fleetDir(jobs) {
  const dir = join(mkdtempSync(join(tmpdir(), "fleet-test-")), "fleet");
  execFileSync("mkdir", ["-p", dir]);
  writeFileSync(join(dir, "jobs.json"), JSON.stringify(jobs));
  return dir;
}

test("jobs run at the same time and a red step does not stop the steps after it", () => {
  const dir = fleetDir([
    { name: "a", steps: [{ name: "sleep", command: "sleep 2" }, { name: "red", command: "echo boom >&2; exit 3" }, { name: "after", command: "echo ran" }] },
    { name: "b", steps: [{ name: "sleep", command: "sleep 2" }] },
    { name: "c", steps: [{ name: "sleep", command: "sleep 2" }] },
  ]);
  const started = Date.now();
  fleet("start", dir);
  const status = JSON.parse(fleet("wait", dir));
  assert.ok(Date.now() - started < 5000, "three 2s jobs must overlap");
  assert.equal(status.state, "done");
  const byStep = Object.fromEntries(status.results.map((r) => [`${r.job}/${r.step}`, r]));
  assert.equal(byStep["a/red"].ok, false);
  assert.match(byStep["a/red"].output, /boom/);
  assert.equal(byStep["a/after"].ok, true);
  assert.equal(status.results.length, 5);
});

test("a step past its timeout is killed and reported red, with everything it started", () => {
  const dir = fleetDir([{ name: "slow", steps: [{ name: "hang", command: `sh -c 'sleep 30; echo ${GRANDCHILD}' & sleep 30`, timeoutMinutes: 0.02 }] }]);
  fleet("start", dir);
  const [result] = JSON.parse(fleet("wait", dir)).results;
  assert.equal(result.ok, false);
  assert.match(result.output, /killed after/);
  assert.ok(gone(own(GRANDCHILD)), "the step's background child must be stopped with it");
});

test("results from an earlier fleet in the same directory are never counted", () => {
  const dir = fleetDir([{ name: "only", steps: [{ name: "ok", command: "true" }] }]);
  writeFileSync(join(dir, "0.stale-token.result.json"), JSON.stringify([{ job: "only", step: "stale", ok: false, output: "" }]));
  fleet("start", dir);
  const status = JSON.parse(fleet("wait", dir));
  assert.deepEqual(status.results.map((r) => r.step), ["ok"]);
});

test("output stays short enough for the journal however much a step prints", () => {
  const dir = fleetDir([{ name: "loud", steps: [{ name: "spam", command: "yes spam | head -c 500000; exit 1" }] }]);
  fleet("start", dir);
  const line = fleet("wait", dir);
  assert.ok(line.length < 5000, `wait printed ${line.length} bytes`);
  const [result] = JSON.parse(line).results;
  assert.ok(readFileSync(result.log, "utf8").length >= 500000, "the full output stays in the log");
});

const cargo = (failed, names) =>
  `${names.map((n) => `test ${n} ... FAILED`).join("\n")}\ntest result: FAILED. 10 passed; ${failed} failed; 0 ignored\n`;
// One `Running` line per binary that printed a `test result:` line.
const ran = (binaries) => Array.from({ length: binaries }, (_, i) => `     Running unittests (target/t${i})`).join("\n");

test("a cargo run whose only failures are named tests lists them", () => {
  assert.deepEqual(failedTests({ stdout: cargo(2, ["a::b", "c::d"]), stderr: ran(1) }), ["a::b", "c::d"]);
});

test("a build error, an unnamed failure or a crashed binary lists none", () => {
  assert.deepEqual(failedTests({ stdout: "error[E0425]: cannot find value\n", stderr: "" }), []);
  assert.deepEqual(failedTests({ stdout: cargo(2, ["a::b"]), stderr: ran(1) }), []);
  // Two binaries started; only one printed a result, the other was killed.
  assert.deepEqual(failedTests({ stdout: cargo(1, ["a::b"]), stderr: ran(2) }), []);
});

test("a step that fails on stderr alone names the log that holds it", () => {
  const dir = fleetDir([{ name: "quiet", steps: [{ name: "err", command: "echo only-on-stderr >&2; exit 1" }] }]);
  fleet("start", dir);
  const [result] = JSON.parse(fleet("wait", dir)).results;
  assert.equal(result.ok, false);
  assert.match(readFileSync(result.errLog, "utf8"), /only-on-stderr/);
});

test("stopping a fleet stops the step each job is in, not only the job", () => {
  const dir = fleetDir([{ name: "long", steps: [{ name: "hang", command: `sh -c 'sleep 40; echo ${MARKER}' & wait` }] }]);
  fleet("start", dir);
  const deadline = Date.now() + 5000;
  while (gone(own(MARKER)) && Date.now() < deadline) execFileSync("sleep", ["0.2"]);
  assert.ok(!gone(own(MARKER)), "the step never started");
  fleet("stop", dir);
  const stopped = Date.now() + 5000;
  while (!gone(own(MARKER)) && Date.now() < stopped) execFileSync("sleep", ["0.2"]);
  assert.ok(gone(own(MARKER)), "the running step must be stopped with its job");
});

test("a pid the fleet did not start is never signalled", () => {
  const dir = fleetDir([{ name: "only", steps: [{ name: "ok", command: "true" }] }]);
  const stranger = spawn("sleep", ["30"], { detached: true, stdio: "ignore" });
  try {
    execFileSync("mkdir", ["-p", PIDS]);
    writeFileSync(pidsFile(dir), `${stranger.pid} some-other-token`);
    fleet("start", dir);
    JSON.parse(fleet("wait", dir));
    process.kill(stranger.pid, 0); // throws if it was killed
  } finally {
    stranger.kill("SIGKILL");
  }
});

test("a finished fleet leaves no pids file behind", () => {
  const dir = fleetDir([{ name: "only", steps: [{ name: "ok", command: "true" }] }]);
  fleet("start", dir);
  JSON.parse(fleet("wait", dir));
  assert.throws(() => readFileSync(pidsFile(dir)));
});

test("a job that dies takes its running step with it", () => {
  const marker = `fleet-orphan-marker-${process.pid}`;
  const dir = fleetDir([{ name: "doomed", steps: [{ name: "hang", command: `sh -c 'sleep 40; echo ${marker}' & wait` }] }]);
  fleet("start", dir);
  const deadline = Date.now() + 5000;
  while (gone(own(marker)) && Date.now() < deadline) execFileSync("sleep", ["0.2"]);
  assert.ok(!gone(own(marker)), "the step never started");
  process.kill(-firstPid(dir), "SIGKILL"); // the job only, as an OOM kill would
  assert.ok(!gone(own(marker)), "the step outlives its job until wait reaps it");
  JSON.parse(fleet("wait", dir));
  assert.ok(gone(own(marker)), "wait must stop the dead job's step");
});

test("a job whose process dies without reporting is red at once, not at the deadline", () => {
  const dir = fleetDir([{ name: "doomed", steps: [{ name: "sleep", command: "sleep 30" }] }]);
  fleet("start", dir);
  process.kill(-firstPid(dir), "SIGKILL");
  const started = Date.now();
  const [result] = JSON.parse(fleet("wait", dir)).results;
  assert.ok(Date.now() - started < 5000);
  assert.equal(result.ok, false);
  assert.match(result.output, /exited before its last step reported/);
});

test("starting a fleet again stops the jobs the last one left running, even after its directory was recreated", () => {
  const dir = fleetDir([{ name: "stale", steps: [{ name: "sleep", command: "sleep 60" }] }]);
  fleet("start", dir);
  const stale = firstPid(dir);
  execFileSync("rm", ["-rf", dir]);
  execFileSync("mkdir", ["-p", dir]);
  writeFileSync(join(dir, "jobs.json"), JSON.stringify([{ name: "fresh", steps: [{ name: "ok", command: "true" }] }]));
  fleet("start", dir);
  JSON.parse(fleet("wait", dir));
  assert.throws(() => process.kill(stale, 0), "the stale job must be gone");
});

test("a wait whose fleet a newer start or a stop ended reports preempted, whatever the stop left behind", async () => {
  assert.equal(PREEMPTED, PREEMPTED_TS, "fleet.ts reads the wording fleet.mjs writes");
  for (const takeover of ["stop", "start"]) {
    const dir = fleetDir([{ name: "a", steps: [{ name: "sleep", command: "sleep 30" }] }]);
    fleet("start", dir);
    const token = readFileSync(join(dir, "token"), "utf8");
    const waiting = spawn(process.execPath, [FLEET, "wait", dir, token], { env, stdio: ["ignore", "pipe", "ignore"] });
    let out = "";
    waiting.stdout.on("data", (chunk) => (out += chunk));
    // A stop alone (what a newer round does first), or a whole newer start.
    setTimeout(() => fleet(takeover, dir), 600);
    await new Promise((exit) => waiting.on("exit", exit));
    const status = JSON.parse(out);
    assert.equal(status.state, "done", takeover);
    assert.ok(status.results.length > 0, takeover);
    assert.ok(status.results.every((r) => !r.ok && r.output === PREEMPTED), takeover);
    assert.ok(preempted(status.results));
    if (takeover === "start") {
      assert.ok(readFileSync(pidsFile(dir), "utf8").includes(readFileSync(join(dir, "token"), "utf8")), "the newer fleet's pids file survives");
      fleet("stop", dir);
    }
  }
});

test("a stop given a token stops only that fleet and leaves another fleet's token and pids alone", () => {
  const dir = fleetDir([{ name: "a", steps: [{ name: "sleep", command: "sleep 30" }] }]);
  const token = /token (\S+)/.exec(fleet("start", dir))[1];
  const pid = firstPid(dir);
  fleet("stop", dir, "some-other-token");
  assert.equal(readFileSync(join(dir, "token"), "utf8"), token, "another fleet's stop is a no-op here");
  assert.ok(readFileSync(pidsFile(dir), "utf8").includes(String(pid)));
  assert.doesNotThrow(() => process.kill(pid, 0), "the job is still running");
  fleet("stop", dir, token);
  assert.ok(!existsSync(join(dir, "token")), "its own token is gone first");
  assert.ok(!existsSync(pidsFile(dir)));
  const exited = () => {
    try {
      process.kill(pid, 0);
      return false;
    } catch {
      return true;
    }
  };
  const until = Date.now() + 5000;
  while (!exited() && Date.now() < until) Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 100);
  assert.ok(exited(), "and the job is stopped");
});

test("a stop drops the token before it signals: the kill already finds it gone", () => {
  const dir = fleetDir([{ name: "a", steps: [{ name: "sleep", command: "sleep 30" }] }]);
  const token = /token (\S+)/.exec(fleet("start", dir))[1];
  const pid = firstPid(dir);
  const seen = [];
  // In-process, with the kill observed: the token file must already be gone at the first signal.
  stopPrevious(dir, token, (target, signal) => {
    seen.push({ target, signal, tokenPresent: existsSync(join(dir, "token")) });
    process.kill(target, signal);
  });
  assert.equal(seen.length, 1);
  assert.deepEqual(seen[0], { target: -pid, signal: "SIGTERM", tokenPresent: false });
});

test("a wait whose jobs died and whose token is gone reports every job preempted", () => {
  const dir = fleetDir([{ name: "a", steps: [{ name: "sleep", command: "sleep 30" }] }]);
  const token = /token (\S+)/.exec(fleet("start", dir))[1];
  const pid = firstPid(dir);
  try {
    process.kill(-pid, "SIGKILL");
  } catch {
    // Already gone.
  }
  execFileSync("rm", ["-f", join(dir, "token")]);
  const status = JSON.parse(fleet("wait", dir, token));
  assert.ok(status.results.length > 0 && status.results.every((r) => !r.ok && r.output === PREEMPTED), JSON.stringify(status));
});

test("a takeover between the liveness sample and the token read is a preemption, not a crash", () => {
  const dir = fleetDir([{ name: "a", steps: [{ name: "sleep", command: "sleep 30" }] }]);
  const token = /token (\S+)/.exec(fleet("start", dir))[1];
  const pid = firstPid(dir);
  // The liveness probe is where a newer fleet lands: it drops the token and kills the job, then answers dead.
  const alive = () => {
    execFileSync("rm", ["-f", join(dir, "token")]);
    try {
      process.kill(-pid, "SIGKILL");
    } catch {
      // Already gone.
    }
    return false;
  };
  const logged = [];
  const log = console.log;
  console.log = (line) => logged.push(line);
  try {
    wait(dir, token, { alive, tokenOf: (d) => (existsSync(join(d, "token")) ? readFileSync(join(d, "token"), "utf8") : "") });
  } finally {
    console.log = log;
  }
  const status = JSON.parse(logged[0]);
  assert.ok(status.results.length > 0 && status.results.every((r) => !r.ok && r.output === PREEMPTED), `read as preempted, not as a crash: ${logged[0]}`);
});

test("a token-scoped stop leaves a newer fleet's pids file alone while it is still empty", () => {
  const dir = fleetDir([{ name: "a", steps: [{ name: "quick", command: "true" }] }]);
  const token = /token (\S+)/.exec(fleet("start", dir))[1];
  JSON.parse(fleet("wait", dir, token));
  // A newer fleet has written its empty pids file and not yet its first pid.
  writeFileSync(join(dir, "token"), "newer-fleet-token");
  writeFileSync(pidsFile(dir), "");
  stopPrevious(dir, token);
  assert.ok(existsSync(pidsFile(dir)), "the empty file is the newer fleet's");
});

test("a wait for a fleet that finished leaves a newer fleet's pids file alone", () => {
  const dir = fleetDir([{ name: "a", steps: [{ name: "quick", command: "true" }] }]);
  const started = fleet("start", dir);
  const token = /token (\S+)/.exec(started)[1];
  assert.equal(token, readFileSync(join(dir, "token"), "utf8"), "start prints the token it wrote");
  assert.equal(JSON.parse(fleet("wait", dir, token)).state, "done");
  // The job is done and its result published; now a newer fleet owns the directory.
  writeFileSync(join(dir, "token"), "newer-fleet-token");
  writeFileSync(pidsFile(dir), "4242 newer-fleet-token\n");
  const again = JSON.parse(fleet("wait", dir, token));
  assert.equal(again.results[0].ok, true, "the finished fleet's own published result still stands");
  assert.deepEqual(again.results.at(-1), { job: "fleet", step: "fleet", ok: false, output: PREEMPTED }, "beside the preempted marker");
  assert.ok(preempted(again.results));
  assert.equal(readFileSync(pidsFile(dir), "utf8"), "4242 newer-fleet-token\n", "and the newer fleet's pids file is untouched");
});

test("a wait whose directory a newer fleet has emptied ends red as preempted instead of crashing or passing", () => {
  const dir = fleetDir([{ name: "a", steps: [{ name: "sleep", command: "sleep 30" }] }]);
  fleet("start", dir);
  fleet("stop", dir);
  execFileSync("rm", ["-rf", dir]);
  execFileSync("mkdir", ["-p", dir]);
  const status = JSON.parse(fleet("wait", dir));
  assert.equal(status.state, "done");
  assert.deepEqual(status.results, [{ job: "fleet", step: "fleet", ok: false, output: PREEMPTED }]);
  assert.ok(preempted(status.results) && preempted([]), "no results is never green either");
  assert.equal(preempted([{ job: "a", step: "x", ok: true, output: "" }]), false);
});

test("a wait that ends early reports the steps that finished and marks the job still running", () => {
  const dir = fleetDir([{ name: "two", steps: [{ name: "quick", command: "true" }, { name: "slow", command: "sleep 30" }] }]);
  fleet("start", dir);
  execFileSync("sleep", ["1"]);
  const status = JSON.parse(execFileSync(process.execPath, [FLEET, "wait", dir], { encoding: "utf8", env: { ...env, FLEET_WAIT_MS: "500" } }));
  fleet("stop", dir);
  assert.equal(status.state, "pending");
  assert.deepEqual(status.results.map((r) => [r.step, r.ok]), [["quick", true], ["fleet", false]]);
  assert.match(status.results[1].output, /still running/);
});

test("a wait given minutes polls for that long instead of the default", () => {
  const dir = fleetDir([{ name: "slow", steps: [{ name: "sleep", command: "sleep 30" }] }]);
  fleet("start", dir);
  const token = readFileSync(join(dir, "token"), "utf8");
  const started = Date.now();
  // FLEET_WAIT_MS stays at 20 s here: the argument, 0.01 minutes, must win.
  const status = JSON.parse(fleet("wait", dir, token, "0.01"));
  fleet("stop", dir);
  assert.equal(status.state, "pending");
  assert.ok(Date.now() - started < 10_000, "the wait ended on its own minutes, not the 20-second default");
  assert.throws(() => fleet("wait", dir, token, "0"), "a wait of no minutes is refused");
});

test("stopping a fleet also stops the step of a job that already died", () => {
  const marker = `fleet-stop-orphan-${process.pid}`;
  const dir = fleetDir([{ name: "doomed", steps: [{ name: "hang", command: `sh -c 'sleep 40; echo ${marker}' & wait` }] }]);
  fleet("start", dir);
  const deadline = Date.now() + 5000;
  while (gone(own(marker)) && Date.now() < deadline) execFileSync("sleep", ["0.2"]);
  assert.ok(!gone(own(marker)), "the step never started");
  process.kill(-firstPid(dir), "SIGKILL");
  fleet("stop", dir);
  assert.ok(gone(own(marker)), "stop must reach the dead job's step");
});

test("a job's steps start without the caller's credentials unless the job asks for them", () => {
  const secrets = { GH_TOKEN: "s", GITHUB_TOKEN: "s", JEV_TOKEN: "s", AWS_ACCESS_KEY_ID: "s", BL_API_KEY: "s", BL_WORKSPACE: "s", OP_SESSION_x: "s", NPM_TOKEN: "s" };
  const dir = fleetDir([
    { name: "lane", steps: [{ name: "env", command: "env | cut -d= -f1 | sort" }] },
    { name: "sandbox", credentials: "blaxel", steps: [{ name: "env", command: "env | cut -d= -f1 | sort" }] },
    { name: "gate", credentials: "all", steps: [{ name: "env", command: "env | cut -d= -f1 | sort" }] },
  ]);
  execFileSync(process.execPath, [FLEET, "start", dir], { encoding: "utf8", env: { ...env, ...secrets } });
  const results = JSON.parse(fleet("wait", dir)).results;
  const names = (job) => new Set(readFileSync(results.find((r) => r.job === job).log, "utf8").split("\n"));
  for (const s of Object.keys(secrets)) assert.ok(!names("lane").has(s), `${s} reached a lane job`);
  assert.ok(names("lane").has("PATH"));
  assert.ok(names("sandbox").has("BL_API_KEY") && !names("sandbox").has("GH_TOKEN"), "the sandbox job keeps only BL_*");
  assert.ok(names("gate").has("GH_TOKEN"), "the GitHub gate keeps the caller's login");
  assert.ok(SECRET_NAME.test("RELAYCONTROL_INTROSPECT_SECRET") && !SECRET_NAME.test("PATH"));
  assert.deepEqual(Object.keys(jobEnv({ A: "1", GH_TOKEN: "x" })), ["A"]);
});

test("a stop's SIGKILL reaches only the step it stopped, never the step that runs next", async () => {
  const sleeper = () => spawn("/bin/sh", ["-c", "sleep 30"], { detached: true, stdio: "ignore" });
  // The first step obeys SIGTERM at once; the job moves on to a second step before the grace runs out.
  const first = sleeper();
  const exited = new Promise((r) => first.on("exit", r));
  stopStep(first, 200);
  await exited;
  const second = sleeper();
  await new Promise((r) => setTimeout(r, 400));
  let alive = true;
  try { process.kill(second.pid, 0); } catch { alive = false; }
  process.kill(-second.pid, "SIGKILL");
  assert.ok(alive, "the delayed SIGKILL hit the next step");
});
