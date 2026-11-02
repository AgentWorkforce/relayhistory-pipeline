/**
 * Step runner. Every command the proof can run is a Step with its exact argv,
 * whether it mutates, what it touches, and whose approval it needs. The runner
 * prints and records each one; it executes only when the mode and the guard
 * allow it. Command output is returned to the caller and never logged here.
 */
import { spawnSync } from "node:child_process";
import { assertMayRun, GuardError, type GuardInput, type Mode } from "./guard.ts";
import type { Fence } from "./fence.ts";
import { admitStep } from "./ops.ts";
import { toolEnv, type ToolCredentials } from "./cluster-binding.ts";
import { sanitize, type Approval, type StepRecord } from "./report.ts";

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
  /** Binary stdout (e.g. a gzip object): returned base64-encoded instead of as text. */
  binary?: boolean;
  /**
   * Patterns to classify stderr by (e.g. ResourceNotFoundException). Only the
   * booleans are returned; stderr text is never kept, logged or recorded.
   */
  stderrPatterns?: RegExp[];
}

export interface StepOutcome {
  record: StepRecord;
  stdout: string;
  ok: boolean;
  /** One boolean per Step.stderrPatterns entry. */
  stderrMatches?: boolean[];
}

export type Spawn = (argv: string[], stdin: string | undefined, timeoutMs: number, binary: boolean | undefined, env: Record<string, string>) => { stdout: string; status: number | null; error?: Error; stderr?: string };

const defaultSpawn: Spawn = (argv, stdin, timeoutMs, binary, env) => {
  // No shell: argv goes straight to execve. stderr is dropped on purpose: tools echo request bodies and credentials into it.
  const r = spawnSync(argv[0]!, argv.slice(1), { input: stdin, timeout: timeoutMs, maxBuffer: 64 * 1024 * 1024, shell: false, env });
  const out = r.stdout ?? Buffer.alloc(0);
  return { stdout: binary ? out.toString("base64") : out.toString("utf8"), status: r.status, error: r.error, stderr: (r.stderr ?? Buffer.alloc(0)).toString("utf8") };
};

/** POSIX single-quote each argument that needs it, so the printed command is exactly what would run. */
export function shellQuote(argv: readonly string[]): string {
  return argv.map((a) => (/^[A-Za-z0-9_@%+=:,./-]+$/.test(a) ? a : `'${a.replace(/'/g, `'\\''`)}'`)).join(" ");
}

export interface RunnerOptions {
  mode: Mode;
  identity: Omit<GuardInput, "mode" | "mutating">;
  /** Every step's argv must stay inside this fence, in every mode (dry-run included). */
  fence: Fence;
  /** Post-destroy verification: only aws (and in-process) steps; there is no cluster to talk to. */
  awsOnly?: boolean;
  /**
   * The kubeconfig generated from describe-cluster (cluster-binding.ts). On the
   * proof target every non-dry kubectl/helm step requires it; the operator's
   * kubeconfig is never used.
   */
  kubeconfigPath?: string;
  baseEnv?: Record<string, string | undefined>;
  /** resolveToolCredentials' result; absent means static keys (the proof target's only kind). */
  credentials?: ToolCredentials;
  spawn?: Spawn;
  print?: (line: string) => void;
}

export class Runner {
  readonly records: StepRecord[] = [];
  constructor(private readonly o: RunnerOptions) {}

  get mode(): Mode {
    return this.o.mode;
  }

  get hasKubeconfig(): boolean {
    return !!this.o.kubeconfigPath;
  }

  /**
   * Decide whether a step may execute, recording and printing the decision.
   * Used directly for in-process actions (HTTP calls through a port-forward),
   * which are described by a pseudo-argv such as ["HTTP", "POST", url].
   */
  gate(step: Step): { allowed: boolean; record: StepRecord } {
    // Printed and recorded commands are always sanitized, dry-run included: no credential is ever echoed.
    const command = sanitize(shellQuote(step.argv) + (step.stdin !== undefined ? "  <<stdin" : ""));
    // Parse into a typed operation first; mutation is what the parser says, never just the step's label.
    const adm = admitStep(step, this.o.fence, { placeholders: this.o.mode === "dry-run" });
    const mutating = step.mutating || (adm.ok && adm.mutating);
    const base: StepRecord = { id: step.id, command, mutating, touches: step.touches.map((t) => sanitize(t)), approval: mutating ? "khaliq" : step.approval, status: "planned" };
    const print = this.o.print ?? ((l: string) => console.log(l));
    const tag = mutating ? "MUTATE*" : "read   ";
    if (!adm.ok) {
      print(`  [REFUSED] ${step.id}: ${adm.code}`);
      return { allowed: false, record: { ...base, status: "refused", code: adm.code } };
    }
    if (this.o.mode === "dry-run") {
      print(`  [plan ${tag}] ${step.id}: ${command}`);
      return { allowed: false, record: base };
    }
    if (this.o.mode === "read-only" && mutating) {
      print(`  [skip ${tag}] ${step.id} (read-only mode)`);
      return { allowed: false, record: { ...base, status: "skipped" } };
    }
    try {
      assertMayRun({ ...this.o.identity, mode: this.o.mode, mutating }, this.o.fence.proofTarget);
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
    // In-process actions are gated here but performed by the caller; they are never spawned.
    if (step.argv[0] === "HTTP" || step.argv[0] === "PROBE") return this.push({ ...g.record, status: "refused", code: "in_process_not_spawnable" }, "", false);
    if (this.o.awsOnly && step.argv[0] !== "aws") return this.push({ ...g.record, status: "refused", code: "post_destroy_aws_only" }, "", false);
    if (this.needsKubeconfig(step.argv) && !this.o.kubeconfigPath) return this.push({ ...g.record, status: "refused", code: "kubeconfig_unbound" }, "", false);
    const started = Date.now();
    const r = (this.o.spawn ?? defaultSpawn)(step.argv, step.stdin, step.timeoutMs ?? 120_000, step.binary, this.env());
    const durationMs = Date.now() - started;
    const ok = !r.error && r.status === 0;
    const out = this.push({ ...g.record, status: ok ? "ran" : "failed", exitCode: r.status ?? -1, durationMs, ...(ok ? {} : { code: r.error ? "spawn_error" : `exit_${r.status}` }) }, r.stdout, ok);
    if (step.stderrPatterns) out.stderrMatches = step.stderrPatterns.map((re) => re.test(r.stderr ?? ""));
    return out;
  }

  /** gate() and record the decision in one call; for planned in-process actions. */
  record(step: Step): boolean {
    const g = this.gate(step);
    this.records.push(g.record);
    return g.allowed;
  }

  /** kubectl and helm (except `helm show`, which reads the chart repo only) talk to the cluster. */
  needsKubeconfig(argv: readonly string[]): boolean {
    return this.o.fence.target === "proof" && (argv[0] === "kubectl" || (argv[0] === "helm" && argv[1] !== "show"));
  }

  /** The environment every spawned tool gets: the generated kubeconfig, no retargeting variables. */
  env(): Record<string, string> {
    return toolEnv(this.o.baseEnv ?? process.env, this.o.kubeconfigPath, this.o.credentials);
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
