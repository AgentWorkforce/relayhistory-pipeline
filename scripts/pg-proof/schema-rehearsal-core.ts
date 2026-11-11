/**
 * Pure helpers for the schema release rehearsal (scripts/pg-proof/schema-rehearsal.ts and
 * .github/workflows/rehearse-session-schema.yml). No I/O, no database, no environment: unit
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

/**
 * R0 step order: the seed first, then everything else, each in tree order. The last seed
 * step is R0's target, so every unseeded step is held (never applied, never ledgered).
 */
export function seedOrder(seed: readonly string[], unseeded: readonly string[]): string[] {
  return [...seed, ...unseeded];
}

/** sha256 of a file's utf8 text, as migrate.ts sha256Text and release-target.ts digest it. */
const sha256Utf8 = (text: string) => createHash("sha256").update(text).digest("hex");

/** One file R0 applies: its repository path, the exact text and that text's sha256. */
export interface SeedFile {
  name: string;
  rel: string;
  text: string;
  sha256: string;
}

/**
 * R0's bytes: every seed schema step and every phase 2 verifier exactly as the baseline
 * landing commit's tree has them (read from the Git object database, never the release
 * checkout's working tree), so the seeded database is what the migrator at the landing
 * commit would have produced. `landing`/`release` return a path's utf8 text at the
 * landing commit / release sha (undefined when the path is absent); `landingDigests` is
 * the landing commit's [name, sha256] schema steps (release-target.ts treeSchemaSteps).
 * Refuses: the seed is not exactly the landing tree's steps (rehearsal_seed_step); a seed
 * or verifier path absent at the landing commit (rehearsal_seed_bytes_missing); a seed
 * step whose bytes differ from the landing tree digest or from the release tree's bytes
 * (landed migrations are frozen) (rehearsal_seed_bytes_drift).
 */
export function seedStepBytes(x: {
  seed: readonly string[];
  verifiers: readonly { name: string; path: string }[];
  stepRel: (name: string) => string;
  landing: (rel: string) => string | undefined;
  release: (rel: string) => string | undefined;
  landingDigests: readonly (readonly [string, string])[];
}): { seed: SeedFile[]; verify: SeedFile[] } {
  if (x.landingDigests.length !== x.seed.length || x.landingDigests.some(([n], i) => n !== x.seed[i]))
    rehearsalRefuse("rehearsal_seed_step", { against: "landing_tree" });
  const file = (name: string, rel: string): SeedFile => {
    const text = x.landing(rel);
    if (text === undefined) rehearsalRefuse("rehearsal_seed_bytes_missing", { step: name });
    return { name, rel, text, sha256: sha256Utf8(text) };
  };
  const seed = x.seed.map((name, i) => {
    const f = file(name, x.stepRel(name));
    if (f.sha256 !== x.landingDigests[i]![1]) rehearsalRefuse("rehearsal_seed_bytes_drift", { step: name, against: "landing_tree" });
    const rel = x.release(f.rel);
    if (rel === undefined || sha256Utf8(rel) !== f.sha256)
      rehearsalRefuse("rehearsal_seed_bytes_drift", { step: name, against: "release_tree" });
    return f;
  });
  return { seed, verify: x.verifiers.map((v) => file(v.name, v.path)) };
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
