/** Only the protected workflow's single secret-scoped step invokes this entry point. */
import { execFileSync } from "node:child_process";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import {
  generation,
  manifestGeneration,
  MIGRATION_LEDGER,
  migrationTargetSplit,
  releaseManifestDigest,
  runMigrations,
  schemaStepDigests,
} from "../src/hoopdb/migrate.ts";
import {
  parseReleaseManifest,
  releaseTargetBinding,
  requireTargetCommit,
  type ReleaseTargetAnchor,
} from "../src/hoopdb/release-target.ts";
import {
  ROLE_VALIDATION_NAME,
  ROLE_VALIDATION_TABLE,
} from "../src/shared/db.ts";
import {
  SCHEMA_RELEASE as P,
  refuse,
} from "../src/hoopdb/schema-release-contract.ts";
import {
  inspectRelease,
  releaseClient,
  releaseProfile,
  releaseErrorCode,
} from "../src/hoopdb/schema-release-inspect.ts";
import { githubGet, githubReleaseGate } from "./schema-release-github.ts";
import {
  refreshReleaseBoundary,
  releaseBoundaryDigest,
  requireSafeReleaseBoundary,
  verifyReleaseBoundary,
} from "../src/hoopdb/schema-release-boundary.ts";
const stepNames = (value: unknown): value is string[] =>
  Array.isArray(value) &&
  value.every(
    (n) =>
      typeof n === "string" &&
      (n.startsWith("upstream:") || n.startsWith("96bulls:")),
  );
const sameNames = (a: string[], b: string[]) =>
  a.length === b.length && a.every((n, i) => n === b[i]);
export function validateReleasePlan(
  plan: any,
  input: {
    sha: string;
    generation: string;
    boundary: string;
    target: string;
    targetSha: string;
    targetGeneration: string;
    manifestDigest: string;
    planRun: string;
  },
) {
  // The target commit's manifest: its exact schema steps (the prefix) and verifier
  // digests. Its canonical digest is bound into the typed approval, and the target
  // generation must be the generation those entries compute.
  const manifest = parseReleaseManifest(plan?.targetManifest);
  const manifestNames = new Set(manifest?.steps.map(([n]) => n) ?? []);
  if (
    !manifest ||
    manifest.targetSha !== input.targetSha ||
    manifest.target !== input.target ||
    plan.targetSha !== input.targetSha ||
    plan.manifestDigest !== input.manifestDigest ||
    !/^[0-9a-f]{64}$/.test(plan.manifestDigest ?? "") ||
    releaseManifestDigest(manifest) !== plan.manifestDigest ||
    manifestGeneration(manifest) !== plan.targetGeneration ||
    manifest.steps.at(-1)?.[0] === undefined ||
    !manifestNames.has(input.target) ||
    !stepNames(plan.expectedPending) ||
    !stepNames(plan.held) ||
    plan.expectedPending.some((n: string) => !manifestNames.has(n)) ||
    plan.held.some((n: string) => manifestNames.has(n))
  )
    refuse("release_plan_artifact");
  if (
    plan?.version !== 1 ||
    plan.project !== P.project ||
    plan.sha !== input.sha ||
    plan.generation !== input.generation ||
    plan.boundary !== input.boundary ||
    plan.target !== input.target ||
    !stepNames(plan.expectedPending) ||
    !plan.expectedPending.length ||
    plan.expectedPending.at(-1) !== input.target ||
    !stepNames(plan.held) ||
    // The target generation (the target commit's), bound into the approval: equal to
    // the full generation exactly when nothing is held.
    !/^[0-9a-f]{64}$/.test(plan.targetGeneration ?? "") ||
    plan.targetGeneration !== input.targetGeneration ||
    (plan.held.length === 0) !== (plan.targetGeneration === input.generation) ||
    String(plan.runId) !== input.planRun ||
    !/^[0-9a-f]{64}$/.test(plan.ledgerSha256 ?? "") ||
    !Array.isArray(plan.inventory) ||
    !plan.inventory.length
  )
    refuse("release_plan_artifact");
  requireSafeReleaseBoundary(plan.existingBoundary);
  return {
    ledgerSha256: plan.ledgerSha256 as string,
    existingBoundary: plan.existingBoundary,
    expectedPending: plan.expectedPending as string[],
    held: plan.held as string[],
    targetGeneration: plan.targetGeneration as string,
    targetSha: plan.targetSha as string,
    manifestDigest: plan.manifestDigest as string,
  };
}
/**
 * The target anchor from the local Git object database only (no fetch): target_sha
 * must be the release sha or on its first-parent (main) history, and its tree's schema steps equal this
 * checkout's steps without `held`, by name and sha256 (release-target.ts).
 */
export async function targetAnchor(input: { sha: string; target: string; targetSha: string }, held: string[]) {
  return releaseTargetBinding({
    repo: process.cwd(),
    releaseSha: input.sha,
    targetSha: input.targetSha,
    target: input.target,
    prefix: await schemaStepDigests(held),
  });
}
export function sameAnchor(
  a: ReleaseTargetAnchor,
  b: { targetSha: string; targetGeneration: string; manifestDigest: string },
) {
  return (
    a.targetSha === b.targetSha &&
    a.targetGeneration === b.targetGeneration &&
    a.manifestDigest === b.manifestDigest
  );
}
export async function releaseDatabase() {
  const get = githubGet();
  const input = await githubReleaseGate(process.env, get, true);
  if (
    execFileSync("git", ["rev-parse", "HEAD"], {
      encoding: "utf8",
      env: { PATH: process.env.PATH },
    }).trim() !== input.sha ||
    (await generation()) !== input.generation
  )
    refuse("release_checkout_generation");
  // target_sha: a local commit, the release sha or on its first-parent chain (no network git).
  requireTargetCommit(process.cwd(), input.sha, input.targetSha);
  const profile = releaseProfile(input.boundary);
  const approvedPlan =
    input.mode === "apply"
      ? validateReleasePlan(
          JSON.parse(await readFile("release-plan/plan.json", "utf8")),
          input,
        )
      : undefined;
  // Apply, before connecting: the approved anchor recomputed from the object database
  // and this checkout (plan-vs-apply drift refuses before any connection).
  if (
    approvedPlan &&
    !sameAnchor(await targetAnchor(input, approvedPlan.held), approvedPlan)
  )
    refuse("release_target_manifest_changed");
  const client = releaseClient(process.env.SUPABASE_MIGRATION_OWNER_URL);
  try {
    await client.connect();
    const inspection = await inspectRelease(client, input.boundary);
    // The target gate (read-only): refuses an unknown, applied or passed target.
    const split = migrationTargetSplit(inspection.inventory, input.target);
    // The generation this release marks PASS: that of target_sha, whose tree must end at
    // the target with exactly this checkout's prefix (the full generation when nothing
    // is held). Refuses a held name outside this tree.
    const anchor = await targetAnchor(input, split.held);
    const targetGeneration = anchor.targetGeneration;
    if (!split.held.length && targetGeneration !== input.generation)
      refuse("release_target_generation_changed");
    if (input.mode === "plan") {
      await mkdir("release-plan", { recursive: true });
      await writeFile(
        "release-plan/plan.json",
        JSON.stringify(
          {
            version: 1,
            project: P.project,
            sha: input.sha,
            boundary: input.boundary,
            target: input.target,
            ...split,
            targetSha: anchor.targetSha,
            targetGeneration,
            manifestDigest: anchor.manifestDigest,
            targetManifest: anchor.manifest,
            runId: process.env.GITHUB_RUN_ID,
            ...inspection,
          },
          null,
          2,
        ) + "\n",
        { mode: 0o600 },
      );
      if (split.held.length)
        console.log("release_target_held " + split.held.length);
      console.log("release_readonly_plan_complete");
      return;
    }
    if (inspection.ledgerSha256 !== approvedPlan!.ledgerSha256)
      refuse("release_ledger_changed");
    if (
      releaseBoundaryDigest(inspection.existingBoundary) !==
      releaseBoundaryDigest(approvedPlan!.existingBoundary)
    )
      refuse("release_boundary_changed");
    // Plan and apply must see exactly the same pending/held split; the migrator
    // rechecks the pending list under its lock before any write.
    if (
      !sameNames(split.expectedPending, approvedPlan!.expectedPending) ||
      !sameNames(split.held, approvedPlan!.held)
    )
      refuse("release_target_pending_changed");
    if (targetGeneration !== approvedPlan!.targetGeneration)
      refuse("release_target_generation_changed");
    if (!sameAnchor(anchor, approvedPlan!)) refuse("release_target_manifest_changed");
    const result = await runMigrations(client, {
      profile,
      expectedLedgerSha256: approvedPlan!.ledgerSha256,
      target: input.target,
      expectedPending: approvedPlan!.expectedPending,
      targetGeneration: approvedPlan!.targetGeneration,
      // The target commit's verifiers, read from the object database, with their
      // digests; the migrator recomputes the manifest under its lock.
      targetBinding: {
        targetSha: approvedPlan!.targetSha,
        manifestDigest: approvedPlan!.manifestDigest,
        verifiers: anchor.verifiers,
      },
      beforeWrite: async () => {
        await githubReleaseGate(process.env, get, true);
        // Under the migrator lock: the anchor once more, before any write.
        if (!sameAnchor(await targetAnchor(input, approvedPlan!.held), approvedPlan!))
          refuse("release_target_manifest_changed");
        await refreshReleaseBoundary(client, approvedPlan!.existingBoundary);
      },
    });
    if (!sameNames(result.held, approvedPlan!.held))
      refuse("release_target_readback");
    // Read back: every approved step is ledgered and no held step is.
    const ledgered = new Set(
      (
        await client.query<{ name: string }>(
          `SELECT name FROM ${MIGRATION_LEDGER} WHERE name = ANY($1::text[])`,
          [[...approvedPlan!.expectedPending, ...approvedPlan!.held]],
        )
      ).rows.map((r) => r.name),
    );
    if (
      approvedPlan!.expectedPending.some((n) => !ledgered.has(n)) ||
      approvedPlan!.held.some((n) => ledgered.has(n))
    )
      refuse("release_target_readback");
    const marker = (
      await client.query(
        `SELECT generation,status FROM ${ROLE_VALIDATION_TABLE} WHERE name=$1`,
        [ROLE_VALIDATION_NAME],
      )
    ).rows[0];
    // Phase 2 ran on the applied tree and passed exactly the target generation (the
    // full generation when nothing is held); never the full one while steps are held.
    if (
      marker?.status !== "pass" ||
      marker.generation !== approvedPlan!.targetGeneration
    )
      refuse("release_marker");
    await verifyReleaseBoundary(client);
    if (approvedPlan!.held.length)
      console.log("release_target_held " + approvedPlan!.held.length);
    console.log("release_apply_verified");
  } finally {
    await client.end().catch(() => {});
  }
}
if (import.meta.main)
  releaseDatabase().catch((error) => {
    // A SQL error's message/detail/hint can contain values; emit only a code.
    const code = releaseErrorCode(error);
    console.error(code);
    process.exitCode = 1;
  });
