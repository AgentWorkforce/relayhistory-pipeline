// Run several jobs at once and report each one's steps.
//
//   node flows/lib/fleet.mjs start <dir>          start every job in <dir>/jobs.json, detached; prints "started N job(s) token <token>"
//   node flows/lib/fleet.mjs wait <dir> [token] [minutes]   poll for up to <minutes> (default 13) (for the fleet `start` recorded in <dir>/token)
//   node flows/lib/fleet.mjs stop <dir> [token]   stop every job still running, and its step (only that fleet's, when a token is given)
//   node flows/lib/fleet.mjs exec <dir> <index> <token>   (internal) run one job's steps in order
//
// The Relayflows runtime runs an authored body's steps one at a time unless
// the body fits a narrow grammar of plain commands, and agents sharing a
// directory one at a time as well; separate processes are not limited. So a
// fleet is one detached process per job. Each job's steps run in order and keep
// going past a red step, so one pass reports every failure. jobs.json is
// [{ "name", "steps": [{ "name", "command", "timeoutMinutes"?, "cargoTests"? }] }].
//
// `wait` prints {"state":"done","results":[{ "job", "step", "ok", "output", "log", "errLog", "failedTests"? }]},
// or at the deadline {"state":"pending","results":[...]}: every step that has
// finished so far, and a red entry for each job still running, so a caller
// that gives up still reports what did finish. Each job publishes after every
// step, so a job that dies keeps the verdicts it already had.
// Outputs are short tails, so the whole line stays well inside the journal's
// bounded stdout; `log` and `errLog` name the step's complete stdout and stderr.
// A step the fleet killed on its timeout says so in its output: "killed after
// N minutes".
import { execFileSync, spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { appendFileSync, closeSync, existsSync, mkdirSync, openSync, readFileSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { stateRoot } from "./state-dir.mjs";

const DEFAULT_STEP_MINUTES = 60;
const OUTPUT_CHARS = 1500;
const POLL_MS = Number(process.env.FLEET_POLL_MS ?? 10_000);
const WAIT_MS = Number(process.env.FLEET_WAIT_MS ?? 13 * 60_000);

const sleep = (ms) => Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
const tail = (text, lines) => text.trimEnd().split("\n").slice(-lines).join("\n");
const clip = (text) => (text.length > OUTPUT_CHARS ? `…${text.slice(-OUTPUT_CHARS)}` : text);
const fail = (why) => {
  console.error(why);
  process.exit(1);
};
const jobsOf = (dir) => JSON.parse(readFileSync(`${dir}/jobs.json`, "utf8"));
/** What a wait says when a newer fleet has taken its directory: one red result, so no caller reads it as green. */
export const PREEMPTED = "preempted by a newer fleet in this directory";
// Whatever the jobs published stands beside one red marker, so a fleet whose
// every job finished before the takeover still reads as preempted.
const preempted = (jobs, token, dir) => [
  ...jobs.flatMap((_, i) => resultsOf(dir, jobs, token, i, PREEMPTED)),
  { job: "fleet", step: "fleet", ok: false, output: PREEMPTED },
];
const tokenOf = (dir) => (existsSync(`${dir}/token`) ? readFileSync(`${dir}/token`, "utf8") : "");
// Under .fleets/ in the state root (lib/state-dir.mjs), outside every flow's
// own directory: callers delete and recreate theirs between runs, and the
// pids must outlive that.
// One `<pid> <token>` line per job; the file is removed once `wait` is done.
export const pidsFile = (dir) =>
  `${process.env.FLEET_PIDS_DIR ?? `${stateRoot()}/.fleets`}/${resolve(dir).replace(/[^A-Za-z0-9._-]+/g, "_")}.pids`;

const pidsOf = (dir) =>
  existsSync(pidsFile(dir))
    ? readFileSync(pidsFile(dir), "utf8").split("\n").filter(Boolean).map((line) => line.split(" "))
    : [];

// A recorded pid is only ours while its command line still carries the
// fleet's token: after a reboot or pid reuse it belongs to someone else.
const isOurs = (pid, token) => {
  try {
    return execFileSync("ps", ["-o", "command=", "-p", pid], { encoding: "utf8" }).includes(` ${token}`);
  } catch {
    return false;
  }
};

/**
 * Stop whatever the previous fleet in this directory left running: a live job
 * stops the step it is in; a dead job's step is stopped from its record.
 */
export function stopPrevious(dir, only, kill = (pid, signal) => process.kill(pid, signal)) {
  // The token goes first: the fleet's own wait then reads the takeover before
  // its jobs die, and its jobs stop publishing. With a token given, only that
  // fleet is stopped and another fleet's token stays.
  if (!only || tokenOf(dir) === only) rmSync(`${dir}/token`, { force: true });
  for (const [index, [pid, token]] of pidsOf(dir).entries()) {
    if (!token || (only && token !== only)) continue;
    if (isOurs(pid, token)) {
      try {
        kill(-Number(pid), "SIGTERM");
      } catch {
        // Already gone.
      }
    } else {
      stopOrphan(dir, index, token);
    }
  }
  // The file goes only when it is ours alone: a newer fleet's file, written empty before its first
  // pid lands, is left to it.
  const entries = pidsOf(dir);
  if (!only || (entries.length > 0 && entries.every(([, token]) => token === only))) rmSync(pidsFile(dir), { force: true });
}

function start(dir) {
  stopPrevious(dir);
  const jobs = jobsOf(dir);
  // Results carry the token they were started under; a stray job from an
  // earlier fleet can never be counted as this one's.
  const token = randomUUID();
  writeFileSync(`${dir}/token`, token);
  mkdirSync(dirname(pidsFile(dir)), { recursive: true });
  writeFileSync(pidsFile(dir), "");
  for (const index of jobs.keys()) {
    // Its own process group, so the lease that runs `start` never reaps it.
    // Absolute, so a cleanup aimed at some other fleet's relative path cannot match it.
    const child = spawn(process.execPath, [fileURLToPath(import.meta.url), "exec", resolve(dir), String(index), token], {
      detached: true,
      stdio: "ignore",
    });
    child.unref();
    // Recorded as each one starts, so an interrupted start leaves none unstoppable.
    appendFileSync(pidsFile(dir), `${child.pid} ${token}\n`);
  }
  // The caller keeps this token for its waits; reading the file later could pick up a newer fleet's.
  console.log(`started ${jobs.length} job(s) token ${token}`);
}

// The step this job is running, so a stop reaches it too: each step leads
// its own process group, outside the job's.
let running;
const STOP_GRACE_MS = 10_000;
const signalStep = (child, signal) => {
  try {
    if (child) process.kill(-child.pid, signal);
  } catch {
    // Already gone.
  }
};
/**
 * SIGTERM first, so a step can clean up after itself (remote-ci deletes its branch), then SIGKILL. The kill is
 * bound to the step that was running when the stop began: if it exits within the grace and the job moves on,
 * the next step is never killed in its place. (Changed from relay-app's copy, which re-read `running`.)
 */
export const stopStep = (child, grace = STOP_GRACE_MS) => {
  signalStep(child, "SIGTERM");
  setTimeout(() => signalStep(child, "SIGKILL"), grace).unref();
};
const stopRunning = () => stopStep(running);

// The running step's pid and start time, beside its job's results: a job that
// dies cannot stop its step, so `wait` does, once it has checked the pid still
// names that same process.
const startedAt = (pid) => {
  try {
    return execFileSync("ps", ["-o", "lstart=", "-p", String(pid)], { encoding: "utf8" }).trim();
  } catch {
    return "";
  }
};
const stepFile = (dir, index, token) => `${dir}/${index}.${token}.step`;
let stepRecord;

function stopOrphan(dir, index, token) {
  const path = stepFile(dir, index, token);
  if (!existsSync(path)) return;
  const [pid, ...started] = readFileSync(path, "utf8").trim().split(" ");
  if (pid && started.join(" ") === startedAt(pid)) {
    try {
      process.kill(-Number(pid), "SIGKILL");
    } catch {
      // Already gone.
    }
  }
  rmSync(path, { force: true });
}

/**
 * Run one step in its own process group, so a timeout stops everything it
 * started (a test binary, a watcher), not only its shell.
 */
async function sh(command, base, minutes = DEFAULT_STEP_MINUTES) {
  const out = openSync(`${base}.out`, "w");
  const err = openSync(`${base}.err`, "w");
  const child = spawn("/bin/sh", ["-c", command], { stdio: ["ignore", out, err], detached: true });
  running = child;
  if (stepRecord && child.pid) writeFileSync(stepRecord, `${child.pid} ${startedAt(child.pid)}`);
  let timedOut = false;
  const timer = setTimeout(() => {
    timedOut = true;
    stopRunning();
  }, minutes * 60_000);
  const { status, error } = await new Promise((done) => {
    child.on("error", (error) => done({ status: null, error }));
    child.on("exit", (status) => done({ status }));
  });
  clearTimeout(timer);
  running = undefined;
  if (stepRecord) rmSync(stepRecord, { force: true });
  closeSync(out);
  closeSync(err);
  return {
    ok: status === 0,
    stdout: readFileSync(`${base}.out`, "utf8"),
    stderr: readFileSync(`${base}.err`, "utf8") + (timedOut ? `\nkilled after ${minutes} minutes` : error ? `\n${error.message}` : ""),
  };
}

// Test runners print their verdict on stdout and progress noise on stderr.
const outputOf = (r) => clip(r.stderr.trim() ? `${tail(r.stdout, 40)}\n--- stderr ---\n${tail(r.stderr, 15)}` : tail(r.stdout, 60));

/** Sum `N failed` over every `test result:` line cargo printed. */
const failedCount = (stdout) =>
  [...stdout.matchAll(/^test result: \w+\. \d+ passed; (\d+) failed/gm)].reduce((sum, m) => sum + Number(m[1]), 0);

/**
 * Every test binary cargo started (stderr `Running` / `Doc-tests` lines)
 * reported a verdict (stdout `test result:`). A binary that crashed or was
 * killed reports none, and its failures have no names.
 */
const everyBinaryReported = (r) =>
  [...r.stderr.matchAll(/^\s+(Running|Doc-tests) /gm)].length === [...r.stdout.matchAll(/^test result: /gm)].length;

/**
 * The failed tests of a red cargo run, when that is all that went wrong:
 * every failure cargo counted is named and every binary reported. Anything
 * else (a build error, a crashed binary, an unnamed failure) gives none. The
 * step stays red either way; ci.flow.ts decides what a named failure means.
 */
export function failedTests(r) {
  const names = [...r.stdout.matchAll(/^test (\S+) \.\.\. FAILED$/gm)].map((m) => m[1]);
  return names.length > 0 && failedCount(r.stdout) === names.length && everyBinaryReported(r) ? names : [];
}

/** Written whole, then renamed, so `wait` never reads half a file. */
const publish = (path, results) => {
  writeFileSync(`${path}.tmp`, JSON.stringify(results));
  renameSync(`${path}.tmp`, path);
};
const resultPath = (dir, index, token) => `${dir}/${index}.${token}.result.json`;
const partialPath = (dir, index, token) => `${dir}/${index}.${token}.partial.json`;

async function exec(dir, index, token) {
  process.on("SIGTERM", () => {
    if (!running) process.exit(143);
    stopRunning();
    running.on("exit", () => process.exit(143));
  });
  const job = jobsOf(dir)[Number(index)];
  const results = [];
  const ours = () => tokenOf(dir) === token;
  stepRecord = stepFile(dir, index, token);
  try {
    for (const [i, step] of job.steps.entries()) {
      const base = `${dir}/${index}.${i}`;
      const r = await sh(step.command, base, step.timeoutMinutes);
      const names = !r.ok && step.cargoTests ? failedTests(r) : [];
      results.push({
        job: job.name, step: step.name, ok: r.ok, output: outputOf(r), log: `${base}.out`, errLog: `${base}.err`,
        ...(names.length ? { failedTests: names } : {}),
        ...(step.vouched ? { vouched: true } : {}),
      });
      if (ours()) publish(partialPath(dir, index, token), results);
    }
  } catch (error) {
    results.push({ job: job.name, step: "fleet", ok: false, output: clip(String(error?.stack ?? error)) });
  }
  // A newer fleet took this directory over: its results are not ours to write.
  if (ours()) publish(resultPath(dir, index, token), results);
}

const read = (path) => JSON.parse(readFileSync(path, "utf8"));

/** A job's final results, or what it published so far plus a red entry saying why it has no more. */
const resultsOf = (dir, jobs, token, index, why) =>
  existsSync(resultPath(dir, index, token))
    ? read(resultPath(dir, index, token))
    : [
        ...(existsSync(partialPath(dir, index, token)) ? read(partialPath(dir, index, token)) : []),
        { job: jobs[index].name, step: "fleet", ok: false, output: why },
      ];

const alive = (pid) => {
  try {
    process.kill(Number(pid), 0);
    return true;
  } catch {
    return false;
  }
};

export function wait(dir, given, probe = { alive, tokenOf }, waitMs = WAIT_MS) {
  // A newer fleet may have emptied the directory and not yet written its jobs: nothing here is ours, and nothing is green.
  let jobs;
  try {
    jobs = jobsOf(dir);
  } catch (error) {
    // Gone, or half-written by a newer fleet: either way not ours, and not green.
    if (error?.code !== "ENOENT" && !(error instanceof SyntaxError) && !(given && tokenOf(dir) !== given)) throw error;
    return console.log(JSON.stringify({ state: "done", results: preempted([], "", dir) }));
  }
  // The caller names the fleet it started; a wait that reads the token itself would adopt a newer fleet's.
  const token = given || tokenOf(dir);
  const pids = pidsOf(dir).map(([pid]) => pid);
  const deadline = Date.now() + waitMs;
  for (;;) {
    const paths = jobs.map((_, i) => resultPath(dir, i, token));
    // A job whose process died without publishing is red now, not at the budget.
    const finished = paths.map((path, i) => existsSync(path) || (pids[i] !== undefined && !probe.alive(pids[i]) && !existsSync(path)));
    // Read after liveness was sampled: a newer fleet that took the directory, killing the jobs on
    // the way, is seen before dead jobs are read as crashes. Its jobs are not ours to wait on or stop.
    if (probe.tokenOf(dir) !== token) return console.log(JSON.stringify({ state: "done", results: preempted(jobs, token, dir) }));
    if (finished.every(Boolean)) {
      for (const i of jobs.keys()) if (!existsSync(paths[i])) stopOrphan(dir, i, token);
      const results = jobs.flatMap((_, i) => resultsOf(dir, jobs, token, i, "the job's process exited before its last step reported"));
      // A newer fleet may own the directory by now: its pids file is not ours to remove.
      if (tokenOf(dir) === token) rmSync(pidsFile(dir), { force: true });
      return console.log(JSON.stringify({ state: "done", results }));
    }
    if (Date.now() > deadline) {
      for (const i of jobs.keys()) if (finished[i] && !existsSync(paths[i])) stopOrphan(dir, i, token);
      const results = jobs.flatMap((_, i) => resultsOf(dir, jobs, token, i, "still running when this wait ended"));
      return console.log(JSON.stringify({ state: "pending", results }));
    }
    sleep(POLL_MS);
  }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const [command, dir, index, token] = process.argv.slice(2);
  if (!dir) fail("usage: fleet.mjs start <dir> | wait <dir> [token] [minutes] | stop <dir> [token] | exec <dir> <index> <token>");
  if (command === "start") start(dir);
  else if (command === "wait") {
    const minutes = token === undefined ? undefined : Number(token);
    if (minutes !== undefined && !(minutes > 0)) fail(`wait minutes must be a positive number, not ${token}`);
    wait(dir, index, undefined, minutes === undefined ? WAIT_MS : minutes * 60_000);
  }
  else if (command === "stop") stopPrevious(dir, index);
  else if (command === "exec") await exec(dir, index, token);
  else fail(`unknown command ${command}`);
}
