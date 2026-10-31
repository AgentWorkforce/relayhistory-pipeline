/**
 * Step runner. Every command the proof can run is a Step with its exact argv,
 * whether it mutates, what it touches, and whose approval it needs. The runner
 * prints and records each one; it executes only when the mode and the guard
 * allow it. Command output is returned to the caller and never logged here.
 */
import { spawnSync } from "node:child_process";
import { assertMayRun, GuardError, type GuardInput, type Mode } from "./guard.ts";
import type { Approval, StepRecord } from "./report.ts";

export interface Step {
  id: string;
  argv: string[];
  mutating: boolean;
  /** Resources the step reads or changes, e.g. "k8s:96bulls/deploy/96bulls-alleyoop-ingest". */
  touches: string[];
  approval: Approval;
  /** Non-secret stdin (SQL text, manifests). Secrets never travel through a Step. */
  stdin?: string;
  timeoutMs?: number;
}

export interface StepOutcome {
  record: StepRecord;
  stdout: string;
  ok: boolean;
}

export type Spawn = (argv: string[], stdin: string | undefined, timeoutMs: number) => { stdout: string; status: number | null; error?: Error };

const defaultSpawn: Spawn = (argv, stdin, timeoutMs) => {
  const r = spawnSync(argv[0]!, argv.slice(1), { input: stdin, encoding: "utf8", timeout: timeoutMs, maxBuffer: 64 * 1024 * 1024 });
  // stderr is dropped on purpose: tools echo request bodies and credentials into it.
  return { stdout: r.stdout ?? "", status: r.status, error: r.error };
};

/** POSIX single-quote each argument that needs it, so the printed command is exactly what would run. */
export function shellQuote(argv: readonly string[]): string {
  return argv.map((a) => (/^[A-Za-z0-9_@%+=:,./-]+$/.test(a) ? a : `'${a.replace(/'/g, `'\\''`)}'`)).join(" ");
}

export interface RunnerOptions {
  mode: Mode;
  identity: Omit<GuardInput, "mode" | "mutating">;
  spawn?: Spawn;
  print?: (line: string) => void;
}

export class Runner {
  readonly records: StepRecord[] = [];
  constructor(private readonly o: RunnerOptions) {}

  get mode(): Mode {
    return this.o.mode;
  }

  /**
   * Decide whether a step may execute, recording and printing the decision.
   * Used directly for in-process actions (HTTP calls through a port-forward),
   * which are described by a pseudo-argv such as ["HTTP", "POST", url].
   */
  gate(step: Step): { allowed: boolean; record: StepRecord } {
    const command = shellQuote(step.argv) + (step.stdin !== undefined ? "  <<stdin" : "");
    const base: StepRecord = { id: step.id, command, mutating: step.mutating, touches: step.touches, approval: step.approval, status: "planned" };
    const print = this.o.print ?? ((l: string) => console.log(l));
    const tag = step.mutating ? (step.approval === "khaliq" ? "MUTATE*" : "MUTATE ") : "read   ";
    if (this.o.mode === "dry-run") {
      print(`  [plan ${tag}] ${step.id}: ${command}`);
      return { allowed: false, record: base };
    }
    if (this.o.mode === "read-only" && step.mutating) {
      print(`  [skip ${tag}] ${step.id} (read-only mode)`);
      return { allowed: false, record: { ...base, status: "skipped" } };
    }
    try {
      assertMayRun({ ...this.o.identity, mode: this.o.mode, mutating: step.mutating });
    } catch (e) {
      const code = e instanceof GuardError ? e.code : "guard_error";
      print(`  [REFUSED] ${step.id}: ${code}`);
      return { allowed: false, record: { ...base, status: "refused", code } };
    }
    print(`  [run  ${tag}] ${step.id}`);
    return { allowed: true, record: { ...base, status: "ran" } };
  }

  /** Run (or plan) one step. Refusals are recorded, never thrown past the check that asked. */
  run(step: Step): StepOutcome {
    const g = this.gate(step);
    if (!g.allowed) return this.push(g.record, "", false);
    const started = Date.now();
    const r = (this.o.spawn ?? defaultSpawn)(step.argv, step.stdin, step.timeoutMs ?? 120_000);
    const durationMs = Date.now() - started;
    const ok = !r.error && r.status === 0;
    return this.push({ ...g.record, status: ok ? "ran" : "failed", exitCode: r.status ?? -1, durationMs, ...(ok ? {} : { code: r.error ? "spawn_error" : `exit_${r.status}` }) }, r.stdout, ok);
  }

  /** gate() and record the decision in one call; for planned in-process actions. */
  record(step: Step): boolean {
    const g = this.gate(step);
    this.records.push(g.record);
    return g.allowed;
  }

  /** Record an in-process action that `gate` allowed, with its outcome. */
  note(record: StepRecord): void {
    this.records.push(record);
  }

  private push(record: StepRecord, stdout: string, ok: boolean): StepOutcome {
    this.records.push(record);
    return { record, stdout, ok };
  }
}
