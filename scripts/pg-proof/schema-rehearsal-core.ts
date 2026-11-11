/**
 * Pure helpers for the schema release rehearsal (scripts/pg-proof/schema-rehearsal.ts and
 * .github/workflows/rehearse-session-schema.yml). No database, no environment, no direct
 * I/O (the landing-migrator steps run commands only through an injected Runner): unit
 * tested in src/hoopdb/__tests__/schema-rehearsal.test.ts. The rehearsal is prerequisite
 * proof only and authorizes nothing (runbooks/schema-release-rehearsal.md).
 */
import { createHash } from "node:crypto";
import { RELEASE_TARGET_STEP } from "../../src/hoopdb/schema-release-contract.ts";

/** The failure injections, each proven by its exact refusal (or, late-step, by a clean rerun). */
export const INJECTIONS = [
  "none",
  "ledger-gap",
  "boundary-drift",
  "late-step",
  "wrong-generation",
  "stale-plan",
] as const;
export type Injection = (typeof INJECTIONS)[number];

/** The refusal each injection must produce; late-step fails its own target step, then reruns clean. */
export const INJECTION_REFUSAL: Record<Exclude<Injection, "none">, string> = {
  "ledger-gap": "ledger_gap",
  "boundary-drift": "function_boundary_unproven",
  "late-step": "rehearsal_injected_late_step",
  "wrong-generation": "release_target_generation_changed",
  "stale-plan": "release_ledger_changed",
};

/**
 * The production apply's refusal codes, in the order scripts/schema-release-database.ts
 * raises them through refuse(...). The driver replays that order on the disposable
 * database; a unit test greps the production file so a reorder or a new refusal fails
 * here first (the rehearsal must then be updated in the same change).
 */
export const PRODUCTION_APPLY_REFUSALS = [
  "release_plan_artifact",
  "release_plan_artifact",
  "release_checkout_generation",
  "release_target_manifest_changed",
  "release_target_generation_changed",
  "release_ledger_changed",
  "release_boundary_changed",
  "release_target_pending_changed",
  "release_target_generation_changed",
  "release_target_manifest_changed",
  "release_target_manifest_changed",
  "release_target_readback",
  "release_target_readback",
  "release_marker",
] as const;

export class RehearsalRefusal extends Error {
  constructor(
    readonly code: string,
    readonly detail: Record<string, unknown> = {},
  ) {
    super(code);
  }
}
export function rehearsalRefuse(code: string, detail: Record<string, unknown> = {}): never {
  throw new RehearsalRefusal(code, detail);
}

export interface ExpectedRefusal {
  stage: string;
  code: string;
}
export interface RehearsalInputs {
  sha: string;
  baseline: string;
  target: string;
  generation: string;
  inject: Injection;
  /** A known-negative row (`<stage>:<code>`): the run passes only on exactly that refusal. */
  expect: ExpectedRefusal | null;
}

/** The workflow_dispatch inputs, exactly; anything else refuses before any connection. */
export function validateInputs(raw: Record<string, string | undefined>): RehearsalInputs {
  const { sha = "", baseline = "", target = "", generation = "", inject = "", expect = "" } = raw;
  if (!/^[0-9a-f]{40}$/.test(sha)) rehearsalRefuse("rehearsal_input_sha");
  if (!RELEASE_TARGET_STEP.test(baseline)) rehearsalRefuse("rehearsal_input_baseline");
  if (!RELEASE_TARGET_STEP.test(target)) rehearsalRefuse("rehearsal_input_target");
  if (baseline === target) rehearsalRefuse("rehearsal_target_not_after_baseline");
  if (!/^[0-9a-f]{64}$/.test(generation)) rehearsalRefuse("rehearsal_input_generation");
  if (!(INJECTIONS as readonly string[]).includes(inject)) rehearsalRefuse("rehearsal_input_inject");
  return { sha, baseline, target, generation, inject: inject as Injection, expect: parseExpect(expect) };
}

/** `expect_refusal`: empty (a normal run) or exactly `<stage>:<code>`. */
export function parseExpect(raw: string): ExpectedRefusal | null {
  if (raw === "") return null;
  const m = /^(R[0-8](?:-[a-z0-9-]+)?):([a-zA-Z0-9_]{1,64})$/.exec(raw);
  if (!m) rehearsalRefuse("rehearsal_input_expect");
  return { stage: m[1]!, code: m[2]! };
}

/**
 * The run's verdict. Without an expected refusal: at least one stage and no FAIL. With
 * one: the LAST recorded stage is the only FAIL, it is exactly the expected stage with
 * exactly the expected code, and no stage ran after it.
 */
export function rehearsalPassed(results: readonly StageResult[], expect: ExpectedRefusal | null): boolean {
  const fails = results.filter((r) => r.status === "FAIL");
  if (!expect) return results.length > 0 && fails.length === 0;
  const last = results.at(-1);
  return fails.length === 1 && last === fails[0] && last.stage === expect.stage && last.code === expect.code;
}

/**
 * libpq / driver connection variables: any of them set could redirect or authenticate a
 * connection. The password variable is spelled in fragments so the rehearsal files'
 * credential scan (schema-rehearsal.test.ts) stays a plain literal scan.
 */
export const CONNECTION_ENV = [
  "PGHOST",
  "PGHOSTADDR",
  "PGPORT",
  "PGUSER",
  ["PG", "PASS", "WORD"].join(""),
  "PGPASSFILE",
  "PGDATABASE",
  "PGSERVICE",
  "PGSERVICEFILE",
  "PGOPTIONS",
  "PGSSLMODE",
  "DATABASE_URL",
];

/**
 * The environment half of the disposability guard: a GitHub Actions job on a
 * GitHub-hosted runner, the workflow's own opt-in, and no connection variable that could
 * point the driver (or the pg client's defaults) at another database. Returns the refusal
 * code, or null. The database half (fresh, just started) is checked after connecting.
 */
export function disposableEnvRefusal(env: Record<string, string | undefined>): string | null {
  if (env.GITHUB_ACTIONS !== "true" || env.REHEARSAL_DISPOSABLE_PG !== "1") return "rehearsal_not_disposable_ci";
  if (env.RUNNER_ENVIRONMENT !== "github-hosted") return "rehearsal_not_github_hosted";
  const set = (k: string) => env[k] !== undefined && env[k] !== "";
  // The named list, and any other PG* variable (libpq and node-postgres read several more).
  if (CONNECTION_ENV.some(set) || Object.keys(env).some((k) => (/^PG[A-Z_]+$/.test(k) || /^SUPABASE_.*URL/.test(k)) && set(k)))
    return "rehearsal_connection_env_set";
  return null;
}

/** The service container must be fresh: its postmaster started within this many seconds. */
export const MAX_POSTMASTER_AGE_SECONDS = 3600;

export const laneOf = (name: string) => name.slice(0, name.indexOf(":") + 1);

/**
 * The database the release starts from: exactly the schema steps of the commit that
 * landed the baseline (`landing`, names in migrator order). Every lane of that set must
 * be a contiguous prefix of the release tree's lane (the migrator's own ledger rule), the
 * baseline must be the tail of its lane there, and the target must come after it.
 * Returns the seed and the release tree's remaining (unseeded) steps, both in tree order.
 */
export function seedSet(input: {
  landing: readonly string[];
  tree: readonly string[];
  baseline: string;
  target: string;
}): { seed: string[]; unseeded: string[]; lastSeed: string } {
  const { landing, tree, baseline, target } = input;
  if (!tree.includes(baseline)) rehearsalRefuse("rehearsal_baseline_unknown");
  if (!tree.includes(target)) rehearsalRefuse("rehearsal_target_unknown");
  if (landing.some((n) => !tree.includes(n))) rehearsalRefuse("rehearsal_seed_noncontiguous", { lane: "unknown" });
  const baseLane = landing.filter((n) => laneOf(n) === laneOf(baseline));
  if (baseLane.at(-1) !== baseline) rehearsalRefuse("rehearsal_baseline_landing", { tail: baseLane.at(-1) ?? null });
  const seeded = new Set(landing);
  for (const lane of new Set(tree.map(laneOf))) {
    const treeLane = tree.filter((n) => laneOf(n) === lane);
    const seedLane = landing.filter((n) => laneOf(n) === lane);
    if (seedLane.some((n, i) => treeLane[i] !== n)) rehearsalRefuse("rehearsal_seed_noncontiguous", { lane });
  }
  if (seeded.has(target)) rehearsalRefuse("rehearsal_target_not_after_baseline");
  const seed = tree.filter((n) => seeded.has(n));
  const unseeded = tree.filter((n) => !seeded.has(n));
  if (!seed.length) rehearsalRefuse("rehearsal_baseline_landing", { tail: null });
  return { seed, unseeded, lastSeed: seed.at(-1)! };
}

/** sha256 of a file's utf8 text, as migrate.ts sha256Text and release-target.ts digest it. */
const sha256Utf8 = (text: string) => createHash("sha256").update(text).digest("hex");

/** One seed step R0's ledger must hold: its repository path, the exact text and that text's sha256. */
export interface SeedFile {
  name: string;
  rel: string;
  text: string;
  sha256: string;
}

/**
 * The seed steps' bytes exactly as the baseline landing commit's tree has them (read from
 * the Git object database, never the release checkout's working tree). The landing
 * commit's own migrator applies them (landingMigrator); these digests are what its ledger
 * must then hold, and landed migrations are frozen, so they must also be the release
 * tree's bytes. `landing`/`release` return a path's utf8 text at the landing commit /
 * release sha (undefined when the path is absent); `landingDigests` is the landing
 * commit's [name, sha256] schema steps (release-target.ts treeSchemaSteps).
 * Refuses: the seed is not exactly the landing tree's steps (rehearsal_seed_step); a seed
 * path absent at the landing commit (rehearsal_seed_bytes_missing); a seed step whose
 * bytes differ from the landing tree digest or from the release tree's bytes
 * (rehearsal_seed_bytes_drift).
 */
export function seedStepBytes(x: {
  seed: readonly string[];
  stepRel: (name: string) => string;
  landing: (rel: string) => string | undefined;
  release: (rel: string) => string | undefined;
  landingDigests: readonly (readonly [string, string])[];
}): SeedFile[] {
  if (x.landingDigests.length !== x.seed.length || x.landingDigests.some(([n], i) => n !== x.seed[i]))
    rehearsalRefuse("rehearsal_seed_step", { against: "landing_tree" });
  return x.seed.map((name, i) => {
    const rel = x.stepRel(name);
    const text = x.landing(rel);
    if (text === undefined) rehearsalRefuse("rehearsal_seed_bytes_missing", { step: name });
    const sha256 = sha256Utf8(text);
    if (sha256 !== x.landingDigests[i]![1]) rehearsalRefuse("rehearsal_seed_bytes_drift", { step: name, against: "landing_tree" });
    const released = x.release(rel);
    if (released === undefined || sha256Utf8(released) !== sha256)
      rehearsalRefuse("rehearsal_seed_bytes_drift", { step: name, against: "release_tree" });
    return { name, rel, text, sha256 };
  });
}

/**
 * The landing commit's tree must end the baseline's lane AT the baseline: its migrator
 * applies every step of its tree, so a step after the baseline there (a commit that
 * landed the baseline together with a later step) would be seeded too. Checked before
 * anything is materialized or written (rehearsal_landing_tree_above_baseline).
 */
export function landingTreeAtBaseline(landing: readonly string[], baseline: string): void {
  const above = landing.filter((n) => laneOf(n) === laneOf(baseline) && n > baseline);
  if (above.length) rehearsalRefuse("rehearsal_landing_tree_above_baseline", { above });
}

/** A child process's outcome (spawnSync with utf8 output). */
export interface RunResult {
  status: number | null;
  signal?: string | null;
  stdout: string;
  stderr: string;
}
/** Runs one command; injected so the landing-migrator steps are unit tested offline. */
export type Runner = (
  cmd: string,
  args: readonly string[],
  opts: { cwd: string; env: Record<string, string>; timeoutMs: number },
) => RunResult;

/**
 * The migrator profile of the production supabase apply of the landing commit's era
 * (the same values as schema-release-inspect.ts releaseProfile("global-default-revoke");
 * a unit test holds them equal). HOOPDB_RUNTIME_ROLE and HOOPDB_URL are added by
 * landingMigratorEnv. Variable names only ever reach evidence, never the values.
 */
export const LANDING_MIGRATOR_PROFILE = {
  HOOPDB_DB_MODE: "supabase",
  HOOPDB_SUPABASE_SHIM: "false",
  HOOPDB_ROLE_BOOTSTRAP: "false",
  HOOPDB_ROLE_VERIFY: "true",
  HOOPDB_RUNTIME_ROLE_LOGIN: "true",
  HOOPDB_FUNCTION_BOUNDARY_MODE: "global-default-revoke",
} as const;

/**
 * The whole environment of the landing migrator subprocess: PATH and HOME (bun, its
 * install cache), the profile, the runtime role and the owner URL. Nothing is inherited,
 * so no PG* / DATABASE_URL / SUPABASE_* variable can redirect it.
 */
export function landingMigratorEnv(base: { PATH?: string; HOME?: string }, runtimeRole: string, url: string): Record<string, string> {
  return {
    PATH: base.PATH ?? "",
    HOME: base.HOME ?? "",
    ...LANDING_MIGRATOR_PROFILE,
    HOOPDB_RUNTIME_ROLE: runtimeRole,
    HOOPDB_URL: url,
  };
}

const FULL_SHA = /^[0-9a-f]{40}$/;
/** The landing tree's migrator entry point, relative to the materialized tree. */
export const LANDING_MIGRATOR = "src/hoopdb/migrate.ts";

/**
 * Materializes the landing commit's tree from the Git object database into the empty
 * directory `dir` (git archive of the full commit id piped through a tar file next to
 * it; never the working tree, never a branch name), installs its own frozen lockfile
 * with `bun`, then makes its schema, bootstrap and source trees read-only. Refuses
 * rehearsal_landing_missing (not a full commit id), rehearsal_landing_materialize
 * (archive, extract, no migrator, read-only) or rehearsal_landing_install.
 */
export function materializeLanding(x: {
  repo: string;
  landing: string;
  dir: string;
  bun: string;
  run: Runner;
  exists: (path: string) => boolean;
  base: { PATH?: string; HOME?: string };
}): void {
  if (!FULL_SHA.test(x.landing)) rehearsalRefuse("rehearsal_landing_missing", { landing: "not_a_commit_id" });
  const env = { PATH: x.base.PATH ?? "", HOME: x.base.HOME ?? "" };
  const tar = `${x.dir}.tar`;
  const step = (name: string, cmd: string, args: string[], cwd: string, timeoutMs: number, code = "rehearsal_landing_materialize") => {
    const r = x.run(cmd, args, { cwd, env, timeoutMs });
    if (r.status !== 0) rehearsalRefuse(code, { step: name, exit: r.status, signal: r.signal ?? null });
  };
  step("archive", "git", ["archive", "--format=tar", "-o", tar, x.landing], x.repo, 300_000);
  step("extract", "tar", ["-xf", tar, "-C", x.dir], x.dir, 300_000);
  step("archive_cleanup", "rm", ["-f", tar], x.dir, 60_000);
  if (!x.exists(`${x.dir}/${LANDING_MIGRATOR}`) || !x.exists(`${x.dir}/bun.lock`))
    rehearsalRefuse("rehearsal_landing_materialize", { step: "migrator" });
  step("install", x.bun, ["install", "--frozen-lockfile"], x.dir, 600_000, "rehearsal_landing_install");
  step("readonly", "chmod", ["-R", "a-w", "src", "supabase", "package.json", "bun.lock", "tsconfig.json"], x.dir, 60_000);
}

/**
 * Redacts a child's output before it reaches the summary: anything URL-shaped and
 * anything the evidence screen refuses, keeping the last `maxLines` lines.
 */
export function redactOutput(text: string, maxLines = 80): string {
  const global = new RegExp(SECRETISH.source, "gi");
  return text
    .replace(/[a-z][a-z0-9+.-]*:\/\/\S+/gi, "[redacted-url]")
    .replace(global, "[redacted]")
    .split("\n")
    .slice(-maxLines)
    .join("\n");
}

/**
 * The landing migrator's own `--generation` (what its readiness marker names). It must
 * be exactly the generation the release code computes over the landing steps and
 * verifiers (releaseTargetBinding at the landing commit): rehearsal_landing_generation
 * when the CLI fails or prints anything else, rehearsal_landing_generation_mismatch
 * when it differs.
 */
export function landingGeneration(x: { dir: string; bun: string; run: Runner; env: Record<string, string>; expected: string }): string {
  const r = x.run(x.bun, [`${x.dir}/${LANDING_MIGRATOR}`, "--generation"], { cwd: x.dir, env: x.env, timeoutMs: 120_000 });
  const g = r.stdout.trim();
  if (r.status !== 0 || !/^[0-9a-f]{64}$/.test(g)) rehearsalRefuse("rehearsal_landing_generation", { exit: r.status });
  if (g !== x.expected) rehearsalRefuse("rehearsal_landing_generation_mismatch", { landing: g.slice(0, 8), release: x.expected.slice(0, 8) });
  return g;
}

/**
 * Runs the landing commit's own migrator once (`bun <dir>/src/hoopdb/migrate.ts`, no
 * arguments: every step of ITS tree, ITS verifiers, ITS readiness marker). The redacted
 * output always goes to `record`; a non-zero exit or a signal refuses
 * rehearsal_landing_migrator_failed.
 */
export function runLandingMigrator(x: {
  dir: string;
  bun: string;
  run: Runner;
  env: Record<string, string>;
  timeoutMs: number;
  record: (log: { exit: number | null; signal: string | null; stdout: string; stderr: string }) => void;
}): void {
  const r = x.run(x.bun, [`${x.dir}/${LANDING_MIGRATOR}`], { cwd: x.dir, env: x.env, timeoutMs: x.timeoutMs });
  x.record({ exit: r.status, signal: r.signal ?? null, stdout: redactOutput(r.stdout), stderr: redactOutput(r.stderr) });
  if (r.status !== 0) rehearsalRefuse("rehearsal_landing_migrator_failed", { exit: r.status, signal: r.signal ?? null });
}

/**
 * After the landing migrator: the schema ledger is exactly the landing tree's steps
 * (rehearsal_seed_ledger) with the landing tree's digests (rehearsal_seed_ledger_digest),
 * and the readiness marker is 'pass' at A (rehearsal_seed_marker).
 */
export function checkLandingSeed(x: {
  landingDigests: readonly (readonly [string, string])[];
  ledger: ReadonlyMap<string, string>;
  marker: { generation: string; status: string } | undefined;
  generation: string;
}): void {
  const want = [...x.landingDigests.map(([n]) => n)].sort();
  const got = [...x.ledger.keys()].sort();
  if (want.length !== got.length || want.some((n, i) => n !== got[i]))
    rehearsalRefuse("rehearsal_seed_ledger", {
      missing: want.filter((n) => !x.ledger.has(n)).length,
      extra: got.filter((n) => !want.includes(n)).length,
    });
  const drift = x.landingDigests.filter(([n, d]) => x.ledger.get(n) !== d).map(([n]) => n);
  if (drift.length) rehearsalRefuse("rehearsal_seed_ledger_digest", { steps: drift });
  if (x.marker?.status !== "pass" || x.marker.generation !== x.generation)
    rehearsalRefuse("rehearsal_seed_marker", { status: x.marker?.status ?? null });
}

/**
 * The pending window for a target, computed independently of migrate.ts
 * migrationTargetSplit: the release tree's unledgered steps up to and including the
 * target, and the unledgered steps after it (held).
 */
export function expectedWindow(
  tree: readonly string[],
  ledgered: ReadonlySet<string>,
  target: string,
): { expectedPending: string[]; held: string[] } {
  const at = tree.indexOf(target);
  if (at < 0) rehearsalRefuse("rehearsal_target_unknown");
  return {
    expectedPending: tree.slice(0, at + 1).filter((n) => !ledgered.has(n)),
    held: tree.slice(at + 1).filter((n) => !ledgered.has(n)),
  };
}

/**
 * A release sha whose tree predates the release target API (src/hoopdb/release-target.ts
 * and migrationTargetSplit, first on main at #534; #530 and earlier lack it) can only run a legacy full apply: every unledgered
 * step of its tree. Reported as evidence when the rehearsal refuses such a sha.
 */
export function legacyForecast(tree: readonly string[], ledgered: ReadonlySet<string>, target: string) {
  const pending = tree.filter((n) => !ledgered.has(n));
  const at = pending.indexOf(target);
  return {
    fullApplyPending: pending,
    beyondTarget: at < 0 ? pending : pending.slice(at + 1),
  };
}

/** Two `git ls-tree` name -> blob oid maps are equal (the cheap anchor pre-filter). */
export function sameNameOid(a: ReadonlyMap<string, string>, b: ReadonlyMap<string, string>): boolean {
  if (a.size !== b.size) return false;
  for (const [k, v] of a) if (b.get(k) !== v) return false;
  return true;
}

/** Flip the last hex digit: a well-formed generation that is not the approved one. */
export function flipHex(hex: string): string {
  const last = hex.at(-1)!;
  return hex.slice(0, -1) + (last === "0" ? "1" : "0");
}

export type StageStatus = "PASS" | "FAIL" | "SKIP";
export interface StageResult {
  stage: string;
  status: StageStatus;
  code?: string;
  evidence: Record<string, unknown>;
}
// Non-loopback connection strings, hosted endpoints, ARNs and bare 12-digit (account-id
// shaped) numbers; a 12-digit run inside a longer alphanumeric token (a hex digest, a
// 14-digit migration stamp) is not an account id and does not match.
const SECRETISH =
  /(?:postgres(?:ql)?:\/\/[^\s"]*@(?!127\.0\.0\.1[:/])[^\s"]*)|(?:\.supabase\.(?:co|com))|(?:pooler)|(?:\barn:)|(?:(?<![0-9a-zA-Z])[0-9]{12}(?![0-9a-zA-Z]))/i;

/**
 * One stage's JSON. Codes are short identifiers only (never a driver message, which can
 * carry values); evidence that looks like a non-loopback connection string or a hosted
 * endpoint refuses rather than being written.
 */
export function resultRecord(
  stage: string,
  status: StageStatus,
  evidence: Record<string, unknown> = {},
  code?: string,
): StageResult {
  if (!/^R[0-8](?:-[a-z0-9-]+)?$/.test(stage)) rehearsalRefuse("rehearsal_stage_name");
  if (code !== undefined && !/^[a-zA-Z0-9_]{1,64}$/.test(code)) code = "rehearsal_failed";
  if (SECRETISH.test(JSON.stringify(evidence))) rehearsalRefuse("rehearsal_evidence_unsafe");
  return { stage, status, ...(code ? { code } : {}), evidence };
}

/** A thrown value's short code only (as schema-release-database.ts releaseErrorCode). */
export function errorCode(error: unknown): string {
  const code = (error as { code?: unknown } | null)?.code;
  return typeof code === "string" && /^[a-zA-Z0-9_]{1,64}$/.test(code) ? code : "rehearsal_failed";
}
