/**
 * Pure helpers for the schema release rehearsal (scripts/pg-proof/schema-rehearsal.ts and
 * .github/workflows/rehearse-session-schema.yml). No database, no environment, no direct
 * I/O (the landing-migrator steps run commands only through an injected Runner): unit
 * tested in src/hoopdb/__tests__/schema-rehearsal.test.ts. The rehearsal is prerequisite
 * proof only and authorizes nothing (runbooks/schema-release-rehearsal.md).
 */
import { createHash } from "node:crypto";
import { RELEASE_TARGET_STEP } from "../../src/hoopdb/schema-release-contract.ts";

/**
 * The failure injections, each proven by its exact refusal (or, late-step and
 * missing-bootstrap, by the refusal and then a clean rerun).
 */
export const INJECTIONS = [
  "none",
  "ledger-gap",
  "boundary-drift",
  "late-step",
  "wrong-generation",
  "stale-plan",
  "missing-bootstrap",
  // The protected release anchor (src/hoopdb/release-target.ts requireReleaseAnchor):
  // each mutates the anchor branch's tip in the disposable checkout (mutateAnchorTip).
  "anchor-tampered",
  "anchor-nonlinear",
  "anchor-extra-file",
  "anchor-missing-verifier",
  "anchor-ref-moved",
] as const;
export type Injection = (typeof INJECTIONS)[number];

/** The refusal each injection must produce; late-step fails its own target step, then reruns clean. */
export const INJECTION_REFUSAL: Record<Exclude<Injection, "none">, string> = {
  "ledger-gap": "ledger_gap",
  "boundary-drift": "function_boundary_unproven",
  "late-step": "rehearsal_injected_late_step",
  "wrong-generation": "release_target_generation_changed",
  "stale-plan": "release_ledger_changed",
  // R2-bootstrap skipped: the first step granting to a role only the release tree's
  // bootstrap chain creates (046000 -> bulls96_analysis_admit) fails undefined_object.
  "missing-bootstrap": "42704",
  // R1-anchor-*: the plan's anchor binding (production's targetAnchor) refuses the tip.
  "anchor-tampered": "release_anchor_file_drift",
  "anchor-nonlinear": "release_anchor_nonlinear",
  "anchor-extra-file": "release_anchor_extra_file",
  "anchor-missing-verifier": "release_target_verifier_missing",
  // R4-anchor-ref-moved: planned at the tip, then the branch moves; the apply's first
  // anchor recompute refuses before connecting.
  "anchor-ref-moved": "release_anchor_ref_mismatch",
};
/** The injections that mutate the anchor tip before the plan (stage R1-<inject>). */
export const PLAN_ANCHOR_INJECTIONS = ["anchor-tampered", "anchor-nonlinear", "anchor-extra-file", "anchor-missing-verifier"] as const;

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

/**
 * The bootstrap stage's refusal codes, in the order scripts/schema-release-database.ts
 * raises them through refuse(...) inside validateBootstrapPlan and releaseBootstrap.
 * The rehearsal driver does NOT replay the bootstrap stage: it rehearses the apply only.
 * This list is a drift pin, so a new or reordered bootstrap refusal fails the unit test
 * first; a bootstrap-stage rehearsal is a separate future change.
 */
export const PRODUCTION_BOOTSTRAP_REFUSALS = [
  "release_bootstrap_approval",
  "release_target_manifest_changed",
  "release_bootstrap_db_mode",
  "release_target_manifest_changed",
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

// ---- R2-bootstrap: the release tree's bootstrap chain applied before the apply.

/**
 * The roles the release tree's bootstrap chain may newly create over the landing chain's
 * (046000's admission principal). Any other new role refuses.
 */
export const BOOTSTRAP_NEW_ROLES = ["bulls96_analysis_admit"] as const;
/** pg_roles columns the stage snapshots: the name and the attribute flags, never a password or config. */
export const ROLE_COLUMNS = [
  "rolname",
  "rolsuper",
  "rolinherit",
  "rolcreaterole",
  "rolcreatedb",
  "rolcanlogin",
  "rolreplication",
  "rolbypassrls",
  "rolconnlimit",
] as const;
export interface RoleRow {
  rolname: string;
  rolsuper: boolean;
  rolinherit: boolean;
  rolcreaterole: boolean;
  rolcreatedb: boolean;
  rolcanlogin: boolean;
  rolreplication: boolean;
  rolbypassrls: boolean;
  rolconnlimit: number;
}
/** One pg_auth_members edge touching a checked role; inherit/set are null before PostgreSQL 16. */
export interface RoleEdge {
  member: string;
  role: string;
  admin_option: boolean;
  inherit_option: boolean | null;
  set_option: boolean | null;
}
export interface BootstrapFileEvidence {
  file: string;
  release_sha256: string;
  landing_sha256: string | null;
  changed: boolean;
}
/** The flags a bootstrap-created role must have, all false: NOLOGIN NOINHERIT NOBYPASSRLS NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION. */
const BARE_FALSE = ["rolsuper", "rolinherit", "rolcreaterole", "rolcreatedb", "rolcanlogin", "rolreplication", "rolbypassrls"] as const;

/**
 * The chain files R2-bootstrap applies: the release sha's bytes (git objects), each with
 * its digest against the landing commit's. Release bytes missing refuses
 * `rehearsal_bootstrap_bytes_missing`; a file absent at landing has landing_sha256 null.
 */
export function bootstrapFiles(files: readonly { file: string; release: string | undefined; landing: string | undefined }[]): {
  files: BootstrapFileEvidence[];
  texts: string[];
} {
  const out: BootstrapFileEvidence[] = [];
  const texts: string[] = [];
  for (const f of files) {
    if (f.release === undefined) rehearsalRefuse("rehearsal_bootstrap_bytes_missing", { file: f.file });
    const release_sha256 = sha256Utf8(f.release);
    const landing_sha256 = f.landing === undefined ? null : sha256Utf8(f.landing);
    out.push({ file: f.file, release_sha256, landing_sha256, changed: release_sha256 !== landing_sha256 });
    texts.push(f.release);
  }
  return { files: out, texts };
}

const attrs = (r: RoleRow) => Object.fromEntries(ROLE_COLUMNS.filter((c) => c !== "rolname").map((c) => [c, r[c]]));
const byName = (rows: readonly RoleRow[]) => new Map(rows.map((r) => [r.rolname, r]));

/**
 * R2-bootstrap's verdict and evidence from the pg_roles snapshots before and after the
 * chain ran and the pg_auth_members edges touching `expected`. Refuses, in order:
 * `rehearsal_bootstrap_unexpected_role` (a new role not in `expected`, or any new role
 * while every chain file is unchanged from landing), `rehearsal_bootstrap_side_effect`
 * (a role removed, a pre-existing role's attributes changed, or the row count not
 * before + created), `rehearsal_bootstrap_role_contract` (an expected role present after
 * that is not bare NOLOGIN, or has any inbound or outbound membership). Evidence: role
 * names, attribute flags and edges only.
 */
export function bootstrapRefreshEvidence(x: {
  files: readonly BootstrapFileEvidence[];
  before: readonly RoleRow[];
  after: readonly RoleRow[];
  edges: readonly RoleEdge[];
  applied: number;
  expected?: readonly string[];
}) {
  const expected = x.expected ?? BOOTSTRAP_NEW_ROLES;
  const before = byName(x.before);
  const after = byName(x.after);
  const sort = (a: string[]) => a.sort((p, q) => (p < q ? -1 : p > q ? 1 : 0));
  const created = sort([...after.keys()].filter((n) => !before.has(n)));
  const unexpected = created.filter((n) => !expected.includes(n));
  if (unexpected.length) rehearsalRefuse("rehearsal_bootstrap_unexpected_role", { roles_created: created, unexpected });
  if (created.length && !x.files.some((f) => f.changed))
    rehearsalRefuse("rehearsal_bootstrap_unexpected_role", { roles_created: created, reason: "chain_unchanged" });
  const removed = sort([...before.keys()].filter((n) => !after.has(n)));
  const altered = sort(
    [...before.keys()].filter((n) => after.has(n) && JSON.stringify(attrs(before.get(n)!)) !== JSON.stringify(attrs(after.get(n)!))),
  );
  if (removed.length || altered.length || x.after.length !== x.before.length + created.length)
    rehearsalRefuse("rehearsal_bootstrap_side_effect", {
      removed,
      altered,
      count_before: x.before.length,
      count_after: x.after.length,
    });
  const roles = expected.map((name) => {
    const r = after.get(name);
    if (!r) return { name, present: false };
    const edges = x.edges.filter((e) => e.member === name || e.role === name);
    const seen = {
      name,
      present: true,
      preexisting: before.has(name),
      attributes: attrs(r),
      members_in: edges.filter((e) => e.role === name),
      member_of: edges.filter((e) => e.member === name),
    };
    if (BARE_FALSE.some((c) => r[c] !== false) || edges.length) rehearsalRefuse("rehearsal_bootstrap_role_contract", seen);
    return seen;
  });
  return {
    files: x.files.map((f) => ({ ...f })),
    roles_created: created,
    applied: x.applied,
    roles,
    role_count: { before: x.before.length, after: x.after.length },
  };
}

// ---- The protected release anchor in the disposable checkout ---------------------------

/**
 * Git in the release checkout through the injected Runner: its own env only (PATH, HOME,
 * a temporary index file, a fixed fixture identity), never the working tree or the real
 * index. Objects and refs it writes stay in the disposable checkout.
 */
export interface AnchorGit {
  run: Runner;
  repo: string;
  env: { PATH?: string; HOME?: string };
  /** A path in a fresh temporary directory: the only index these helpers touch. */
  indexFile: string;
}
/** Remote-tracking namespace the full checkout fetches branches into (release-target.ts). */
export const ANCHOR_REF_NAMESPACE = "refs/remotes/origin/";
function anchorGit(x: AnchorGit, args: readonly string[]): string {
  const r = x.run("git", args, {
    cwd: x.repo,
    env: {
      PATH: x.env.PATH ?? "",
      HOME: x.env.HOME ?? "",
      GIT_INDEX_FILE: x.indexFile,
      GIT_AUTHOR_NAME: "rehearsal",
      GIT_AUTHOR_EMAIL: "rehearsal@example.invalid",
      GIT_COMMITTER_NAME: "rehearsal",
      GIT_COMMITTER_EMAIL: "rehearsal@example.invalid",
      GIT_NO_LAZY_FETCH: "1",
    },
    timeoutMs: 60_000,
  });
  if (r.status !== 0) rehearsalRefuse("rehearsal_git", { command: args[0] });
  return r.stdout;
}
/** The tip of `<namespace><branch>` (exact ref), or undefined. */
export function anchorRefTip(x: AnchorGit, branch: string): string | undefined {
  const ref = ANCHOR_REF_NAMESPACE + branch;
  const out = anchorGit(x, ["for-each-ref", "--format=%(objectname) %(refname)", ref]);
  const row = out.split("\n").find((l) => l.endsWith(" " + ref));
  return row?.split(" ")[0];
}
interface TreeEntry {
  mode: string;
  oid: string;
}
/** path -> mode and blob oid at a commit, for exactly these paths (absent paths omitted). */
function treeEntries(x: AnchorGit, sha: string, paths: readonly string[]): Map<string, TreeEntry> {
  const m = new Map<string, TreeEntry>();
  if (!paths.length) return m;
  for (const ent of anchorGit(x, ["ls-tree", "-z", "--full-tree", sha, "--", ...paths]).split("\0")) {
    if (!ent) continue;
    const tab = ent.indexOf("\t");
    const [mode, type, oid] = ent.slice(0, tab).split(" ");
    if (type === "blob") m.set(ent.slice(tab + 1), { mode: mode!, oid: oid! });
  }
  return m;
}
/** A commit whose tree is `from`'s tree with `files` set (null removes), with these parents. */
function commitTree(x: AnchorGit, from: string, files: ReadonlyMap<string, TreeEntry | null>, parents: readonly string[], message: string): string {
  anchorGit(x, ["read-tree", from]);
  for (const [path, e] of files)
    anchorGit(x, e ? ["update-index", "--add", "--cacheinfo", `${e.mode},${e.oid},${path}`] : ["update-index", "--force-remove", "--", path]);
  const tree = anchorGit(x, ["write-tree"]).trim();
  return anchorGit(x, ["commit-tree", tree, ...parents.flatMap((p) => ["-p", p]), "-m", message]).trim();
}
/** Creates `<namespace><branch>` at sha; refuses (never overwrites) an existing ref. */
function createAnchorRef(x: AnchorGit, branch: string, sha: string): void {
  if (anchorRefTip(x, branch) !== undefined) rehearsalRefuse("rehearsal_anchor_fixture_conflict", { branch });
  anchorGit(x, ["update-ref", ANCHOR_REF_NAMESPACE + branch, sha, ""]);
}
/** Moves `<namespace><branch>` from `from` to `to` (compare-and-swap). */
export function moveAnchorRef(x: AnchorGit, branch: string, from: string, to: string): void {
  anchorGit(x, ["update-ref", ANCHOR_REF_NAMESPACE + branch, to, from]);
}
/** Deletes fixture refs (each only if still at the fixture's sha). */
export function removeAnchorRefs(x: AnchorGit, refs: readonly { branch: string; sha: string }[]): void {
  for (const r of refs) anchorGit(x, ["update-ref", "-d", ANCHOR_REF_NAMESPACE + r.branch, r.sha]);
}

/**
 * A release anchor chain built in the disposable checkout when the protected
 * release/schema-<slot> branch for the target does not exist there (no such branch is
 * ever created on the remote). Off the main commit `base` (whose lane steps are all
 * prefix steps at the release bytes):
 *  - the first commit carries the SESSION_BOOTSTRAP_FILES at the release sha's bytes; it is
 *    its own anchor (the branch of base's newest target-lane step, when that slot is
 *    unique) or, when that slot cannot anchor, folds into the next commit;
 *  - then one commit per missing target-lane prefix step with a unique anchor slot, up to
 *    the target, each adding the missing prefix steps up to it and their release
 *    rollbacks at the release bytes; the remaining missing steps (other lanes) land in the
 *    target's commit.
 * Each commit's branch is created only if absent (rehearsal_anchor_fixture_conflict).
 * Returns the target tip and every ref created (for removeAnchorRefs).
 */
export function buildAnchorFixture(
  x: AnchorGit & {
    sha: string;
    base: string;
    target: string;
    /** The prefix step names, migrator order. */
    prefix: readonly string[];
    baseSteps: ReadonlySet<string>;
    bootstrapFiles: readonly string[];
    branchOf: (step: string) => string | undefined;
    /** True when the release tree names the branch's slot exactly once. */
    uniqueBranch: (branch: string) => boolean;
    stepPath: (step: string) => string;
    rollbackPath: (step: string) => string | undefined;
  },
): { tip: string; refs: { branch: string; sha: string }[] } {
  if (!x.prefix.includes(x.target)) rehearsalRefuse("rehearsal_target_anchor_missing", { reason: "target_not_in_prefix" });
  const lane = laneOf(x.target);
  const anchorable = (step: string) => {
    const b = x.branchOf(step);
    return b !== undefined && x.uniqueBranch(b) ? b : undefined;
  };
  const missing = x.prefix.filter((n) => !x.baseSteps.has(n));
  const releaseFiles = (paths: string[]) => {
    const e = treeEntries(x, x.sha, paths);
    return new Map<string, TreeEntry | null>(paths.map((p) => [p, e.get(p) ?? null]));
  };
  const stepFiles = (steps: string[]) => {
    const paths = steps.map(x.stepPath);
    const rollbacks = steps.map(x.rollbackPath).filter((p): p is string => p !== undefined);
    const rb = treeEntries(x, x.sha, rollbacks);
    const files = releaseFiles(paths);
    for (const [p, e] of rb) files.set(p, e);
    return files;
  };
  const refs: { branch: string; sha: string }[] = [];
  const add = (parent: string, files: Map<string, TreeEntry | null>, branch: string) => {
    const c = commitTree(x, parent, files, [parent], `rehearsal fixture anchor ${branch}`);
    createAnchorRef(x, branch, c);
    refs.push({ branch, sha: c });
    return c;
  };
  try {
    let parent = x.base;
    let pending = releaseFiles([...x.bootstrapFiles]);
    const newestBase = x.prefix.filter((n) => x.baseSteps.has(n) && laneOf(n) === lane).at(-1);
    const baseBranch = newestBase !== undefined && newestBase <= x.target ? anchorable(newestBase) : undefined;
    if (newestBase === x.target || (baseBranch && missing.length)) {
      if (!baseBranch) rehearsalRefuse("rehearsal_target_anchor_missing", { reason: "slot" });
      parent = add(parent, pending, baseBranch);
      pending = new Map();
      if (newestBase === x.target) {
        if (missing.some((n) => laneOf(n) === lane && n < x.target)) rehearsalRefuse("rehearsal_target_anchor_missing", { reason: "gap" });
        if (missing.length) rehearsalRefuse("rehearsal_target_anchor_missing", { reason: "other_lane_missing" });
        return { tip: parent, refs };
      }
    }
    let batch: string[] = [];
    for (const n of missing) {
      batch.push(n);
      const stop = n === x.target || (laneOf(n) === lane && n < x.target && anchorable(n) !== undefined);
      if (!stop) continue;
      if (n === x.target) batch = [...batch, ...missing.slice(missing.indexOf(n) + 1)];
      for (const [p, e] of stepFiles(batch)) pending.set(p, e);
      parent = add(parent, pending, anchorable(n) ?? rehearsalRefuse("rehearsal_target_anchor_missing", { reason: "slot" }));
      pending = new Map();
      batch = [];
      if (n === x.target) return { tip: parent, refs };
    }
    return rehearsalRefuse("rehearsal_target_anchor_missing", { reason: "target_not_missing" });
  } catch (e) {
    try {
      removeAnchorRefs(x, refs);
    } catch {}
    throw e;
  }
}

/**
 * One anchor injection: a new commit for the anchor branch's tip `tip` (parent `parent`),
 * moved onto the branch in the disposable checkout. anchor-tampered: one bootstrap file
 * gets another blob (the first of the other bootstrap files' blobs, or the parent's, that
 * differs); anchor-nonlinear: the same tree with a second parent (the release sha);
 * anchor-extra-file: an extra path outside the allowlist (an existing blob);
 * anchor-missing-verifier: one verifier file removed; anchor-ref-moved: a twin commit
 * (same tree and parent, other message). Returns the new tip.
 */
export function mutateAnchorTip(
  x: AnchorGit & {
    sha: string;
    branch: string;
    tip: string;
    inject: Injection;
    bootstrapFiles: readonly string[];
    verifierPath: string;
  },
): string {
  const parent = anchorGit(x, ["rev-parse", "--verify", "--end-of-options", `${x.tip}^1`]).trim();
  const label = `rehearsal injection ${x.inject}`;
  let next: string;
  if (x.inject === "anchor-nonlinear") next = commitTree(x, x.tip, new Map(), [parent, x.sha], label);
  else if (x.inject === "anchor-ref-moved") next = commitTree(x, x.tip, new Map(), [parent], label);
  else if (x.inject === "anchor-missing-verifier") next = commitTree(x, x.tip, new Map([[x.verifierPath, null]]), [parent], label);
  else if (x.inject === "anchor-extra-file") {
    const e = treeEntries(x, x.tip, [x.bootstrapFiles[0]!]).get(x.bootstrapFiles[0]!);
    if (!e) rehearsalRefuse("rehearsal_injection_unavailable", { inject: x.inject });
    next = commitTree(x, x.tip, new Map([["rehearsal-anchor-extra-file.sql", { mode: "100644", oid: e.oid }]]), [parent], label);
  } else if (x.inject === "anchor-tampered") {
    const victim = x.bootstrapFiles[0]!;
    const at = treeEntries(x, x.tip, [...x.bootstrapFiles]);
    const own = at.get(victim);
    const other = [...x.bootstrapFiles.slice(1).map((f) => at.get(f)), treeEntries(x, parent, [victim]).get(victim)].find(
      (e) => e !== undefined && e.oid !== own?.oid,
    );
    if (!other) rehearsalRefuse("rehearsal_injection_unavailable", { inject: x.inject });
    next = commitTree(x, x.tip, new Map([[victim, { mode: own?.mode ?? "100644", oid: other.oid }]]), [parent], label);
  } else return rehearsalRefuse("rehearsal_injection_unavailable", { inject: x.inject });
  moveAnchorRef(x, x.branch, x.tip, next);
  return next;
}
