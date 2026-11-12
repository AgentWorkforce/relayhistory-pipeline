import { expect, test } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { SESSION_BOOTSTRAP_FILES } from "../session-bootstrap-chain.ts";
import { SESSION_BOOTSTRAP_ROLES } from "../session-bootstrap-roles.ts";
import { SESSION_BOOTSTRAP_CONSUMERS, chainFileRoles, consumerStep } from "../session-bootstrap-consumers.ts";
import {
  bootstrapChainRoles,
  bootstrapConsumers,
  bootstrapRequiredRoles,
  compareSteps,
  targetRequiredRoles,
} from "../schema-release-bootstrap.ts";

const root = new URL("../../../", import.meta.url);
const migrationsDir = new URL("supabase/migrations/", root);
const migrations = readdirSync(migrationsDir)
  .filter((f) => f.endsWith(".sql"))
  .sort();
const code = (fn: () => unknown) => {
  try {
    fn();
    return "no refusal";
  } catch (e) {
    return (e as { code?: string }).code;
  }
};
const chainNames = bootstrapChainRoles().map((r) => r.name);

test("the static manifest equals the first consumer recomputed from the migrations at HEAD", () => {
  const text = new Map(migrations.map((f) => [f, readFileSync(new URL(f, migrationsDir), "utf8")]));
  const recomputed = chainNames.map((role) => {
    const word = new RegExp(`(^|[^A-Za-z0-9_])${role}([^A-Za-z0-9_]|$)`);
    const first = migrations.find((f) => word.test(text.get(f)!));
    return [role, first] as const;
  });
  const byRole = (pairs: readonly (readonly [string, string | undefined])[]) =>
    [...pairs].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
  expect(byRole(recomputed)).toEqual(byRole(SESSION_BOOTSTRAP_CONSUMERS));
  // Each first consumer is a real migration file at HEAD.
  for (const [, file] of SESSION_BOOTSTRAP_CONSUMERS) expect(migrations).toContain(file);
});

test("every chain role is mapped exactly once and no unknown role is mapped", () => {
  expect(chainNames.length).toBe(14);
  const roles = SESSION_BOOTSTRAP_CONSUMERS.map(([r]) => r);
  expect(new Set(roles).size).toBe(roles.length);
  expect([...roles].sort()).toEqual([...chainNames].sort());
  expect(bootstrapConsumers().size).toBe(14);
});

test("a malformed, duplicate, conflicting, unknown or missing manifest entry refuses", () => {
  const base = [...SESSION_BOOTSTRAP_CONSUMERS];
  const manifest = "release_bootstrap_manifest";
  // Duplicate (same mapping twice) and conflicting (same role, other file).
  expect(code(() => bootstrapConsumers([...base, base[0]]))).toBe(manifest);
  expect(code(() => bootstrapConsumers([...base, [base[0][0], "20261030020001_other.sql"]]))).toBe(manifest);
  // Unmapped chain role.
  expect(code(() => bootstrapConsumers(base.slice(1)))).toBe(manifest);
  // Unknown role.
  expect(code(() => bootstrapConsumers([...base, ["rogue_role", "20261030020000_x.sql"]]))).toBe(manifest);
  // Malformed first consumer (a step name, a path, a history-lane file).
  for (const bad of ["upstream:20261030020000_x.sql", "supabase/migrations/20261030020000_x.sql", "0001_x.sql"])
    expect(code(() => bootstrapConsumers([[base[0][0], bad], ...base.slice(1)]))).toBe(manifest);
  expect(code(() => bootstrapConsumers([["only_one"] as unknown as [string, string], ...base]))).toBe(manifest);
  // A chain role the manifest maps nowhere: targetRequiredRoles refuses too.
  expect(code(() => targetRequiredRoles("upstream:20261105047000_coordination_fact_readers.sql", base.slice(1)))).toBe(manifest);
});

test("required set: first consumer at or before the target, upstream before history lane", () => {
  expect(compareSteps("upstream:20261105046000_analysis_jobs.sql", "96bulls:0001_a.sql")).toBeLessThan(0);
  expect(compareSteps("96bulls:0001_a.sql", "upstream:20261105047000_x.sql")).toBeGreaterThan(0);
  expect(compareSteps("upstream:20261105046000_analysis_jobs.sql", "upstream:20261105046000_analysis_jobs.sql")).toBe(0);
  expect(code(() => compareSteps("other:1.sql", "96bulls:0001_a.sql"))).toBe("release_bootstrap_manifest");
  expect(targetRequiredRoles("upstream:20261105047000_coordination_fact_readers.sql")).toEqual([...chainNames].sort());
  expect(targetRequiredRoles("96bulls:0001_anything.sql")).toEqual([...chainNames].sort());
  // At a first consumer: that role is required (<=).
  expect(targetRequiredRoles("upstream:20261105046000_analysis_jobs.sql")).toContain("bulls96_analysis_admit");
  const mid = targetRequiredRoles("upstream:20261105045500_synthetic.sql");
  expect(mid).not.toContain("bulls96_analysis_admit");
  expect(mid).toContain("bulls96_digest_narrative");
  expect(mid.length).toBe(13);
  const low = targetRequiredRoles("upstream:20261105033500_synthetic.sql");
  expect(low).not.toContain("bulls96_read_api");
  expect(low).not.toContain("bulls96_privacy_eraser");
  expect(low).toContain("bulls96_project_events");
  expect(targetRequiredRoles("upstream:20261001000000_before_all.sql")).toEqual([]);
});

test("bootstrapRequiredRoles binds the manifest to the anchor's steps up to the target", () => {
  const upTo = (target: string) => {
    const t = target.slice("upstream:".length);
    return migrations.filter((f) => f <= t).map(consumerStep);
  };
  const target = "upstream:20261105047000_coordination_fact_readers.sql";
  expect(bootstrapRequiredRoles({ target, steps: upTo(target) })).toEqual([...chainNames].sort());
  // Target not an anchor step.
  expect(code(() => bootstrapRequiredRoles({ target, steps: upTo(target).slice(0, -1) }))).toBe("release_bootstrap_manifest");
  // A required role's first consumer missing from the anchor up to the target.
  const missing = upTo(target).filter((s) => s !== consumerStep("20261105034000_session_directory_read_api.sql"));
  expect(code(() => bootstrapRequiredRoles({ target, steps: missing }))).toBe("release_bootstrap_manifest");
  // A first consumer present only AFTER the target in the anchor does not count.
  const t2 = "upstream:20261105045000_digest_narrative_cache.sql";
  const reordered = [...upTo(t2).filter((s) => s !== consumerStep("20261105034000_session_directory_read_api.sql")), consumerStep("20261105034000_session_directory_read_api.sql")];
  expect(code(() => bootstrapRequiredRoles({ target: t2, steps: reordered }))).toBe("release_bootstrap_manifest");
  // A mapping above the target is allowed (not required, need not be in the anchor).
  expect(bootstrapRequiredRoles({ target: t2, steps: upTo(t2) })).not.toContain("bulls96_analysis_admit");
});

test("each HEAD chain file provisions exactly its declared roles (the stage's extractor)", () => {
  for (const file of SESSION_BOOTSTRAP_FILES)
    expect([...chainFileRoles(readFileSync(new URL(file, root), "utf8"))].sort()).toEqual(
      Object.keys(SESSION_BOOTSTRAP_ROLES[file]).sort(),
    );
});
