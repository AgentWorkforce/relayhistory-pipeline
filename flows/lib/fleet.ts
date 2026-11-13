// Flow-side half of flows/lib/fleet.mjs: write the jobs, start them all at
// once, then poll in leases well under the 15-minute command ceiling.
//
// Each poll is one blocking step of the calling flow's root. While it runs,
// the root cannot follow a retry if its own 30-second worker lease is lost:
// the kernel re-dispatches the root every 30 seconds, each re-dispatch spends
// one of its transport retries, and the root only sees the loss when the poll
// returns. A short poll keeps that within the retries; a 13-minute poll can
// spend them all and leave the run terminal.
import type { Ctx } from "@relayflows/surface";
import { lib, shellWord } from "./shell.ts";

export interface FleetStep {
  name: string;
  command: string;
  /** Kill the step after this long. Default 60. */
  timeoutMinutes?: number;
  /** A cargo test run: when only named tests failed, the result lists them in `failedTests`. */
  cargoTests?: boolean;
  /** GitHub runs this exact command on this operating system (see lib/changes.ts underLocalLoad). */
  vouched?: boolean;
}

export interface FleetJob {
  name: string;
  steps: FleetStep[];
  /**
   * The credentials the job's process keeps (lib/fleet.mjs jobEnv): none by default; "all" for gate code that
   * needs the caller's login (the GitHub gate), "blaxel" for the sandbox e2e.
   */
  credentials?: "all" | "blaxel";
}

export interface FleetResult {
  job: string;
  step: string;
  ok: boolean;
  /** A short tail; `log` and `errLog` hold the complete stdout and stderr. */
  output: string;
  log?: string;
  errLog?: string;
  /** For a red `cargoTests` step whose only failures were these named tests. */
  failedTests?: string[];
  vouched?: boolean;
}

/** Write a file from the body without quoting its contents into a shell command. */
export const writeFile = (path: string, text: string): string =>
  `printf %s ${shellWord(Buffer.from(text, "utf8").toString("base64"))} | base64 -d > ${shellWord(path)}`;

/** What a job stopped by `runFleet` running out of polls says. */
export const OUT_OF_POLLS = (minutes: number): string => `still running after ${minutes} minutes of waiting; stopped`;

/** What fleet.mjs says for a job a newer fleet in the same directory took over (lib/fleet.mjs PREEMPTED). */
export const PREEMPTED = "preempted by a newer fleet in this directory";
/** Whether a newer fleet took the directory while these results were gathered; nothing in them is this fleet's verdict. */
export const preempted = (results: FleetResult[]): boolean => results.length === 0 || results.some((r) => r.output === PREEMPTED);

/** How long one `fleet.mjs wait` polls by default (lib/fleet.mjs WAIT_MS). */
export const DEFAULT_POLL_MINUTES = 13;

export async function runFleet(f: Ctx, dir: string, jobs: FleetJob[], maxPolls: number, pollMinutes = DEFAULT_POLL_MINUTES): Promise<FleetResult[]> {
  if (!(pollMinutes > 0 && pollMinutes <= 13)) throw new Error(`a fleet poll lasts more than 0 and at most 13 minutes, not ${pollMinutes}`);
  await f.run(`mkdir -p ${shellWord(dir)} && ${writeFile(`${dir}/jobs.json`, JSON.stringify(jobs))}`);
  // Each wait names this fleet by the token `start` prints, so a newer one in the same directory is never waited on or stopped as ours.
  const started = await f.run(`node ${lib("fleet.mjs")} start ${shellWord(dir)}`);
  const token = /token (\S+)/.exec(started)?.[1] ?? "";
  let results: FleetResult[] = [];
  for (let poll = 0; poll < maxPolls; poll++) {
    // The command's lease: the poll plus two minutes for node and the result, within the 15-minute ceiling.
    const wait = `node ${lib("fleet.mjs")} wait ${shellWord(dir)} ${shellWord(token)}${pollMinutes === DEFAULT_POLL_MINUTES ? "" : ` ${pollMinutes}`}`;
    const status = JSON.parse(await f.run(wait, { timeout: `${Math.min(15, Math.ceil(pollMinutes) + 2)}m` })) as {
      state: "done" | "pending";
      results: FleetResult[];
    };
    if (status.state === "done") return status.results;
    results = status.results;
  }
  // Out of polls: keep what finished, mark what did not, and leave nothing of this fleet running.
  await f.run(`node ${lib("fleet.mjs")} stop ${shellWord(dir)} ${shellWord(token)}`);
  return results.map((r) =>
    r.step === "fleet" && !r.ok ? { ...r, output: OUT_OF_POLLS(maxPolls * pollMinutes) } : r,
  );
}
