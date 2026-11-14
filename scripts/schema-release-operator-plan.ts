/**
 * PLAN ONLY. For an operator's reviewed SSH session when protected GitHub reviewers
 * are unavailable. There is deliberately no migration/apply branch or fallback.
 */
import { initScrubEngine } from "../packages/pipeline/src/lib/scrub-engine/index.ts";
import { execFileSync } from "node:child_process";
import {
  generation,
  migrationTargetSplit,
  schemaStepDigests,
} from "../src/hoopdb/migrate.ts";
import {
  releaseAnchorBinding,
  requireReleaseAnchor,
  requireTargetCommit,
} from "../src/hoopdb/release-target.ts";
import {
  releaseInputs,
  SCHEMA_RELEASE as P,
  refuse,
} from "../src/hoopdb/schema-release-contract.ts";
import {
  inspectBootstrap,
  inspectRelease,
  releaseClient,
  releaseErrorCode,
} from "../src/hoopdb/schema-release-inspect.ts";
export async function operatorPlan() {
  const input = releaseInputs(process.env);
  if (input.mode !== "plan" || process.argv.length !== 2)
    refuse("operator_plan_only");
  if (
    execFileSync("git", ["rev-parse", "HEAD"], {
      encoding: "utf8",
      env: { PATH: process.env.PATH },
    }).trim() !== input.sha ||
    (await generation()) !== input.generation
  )
    refuse("release_checkout_generation");
  // target_sha: the release sha itself, or the tip of the protected release/schema-<slot>
  // anchor branch for the target (release-target.ts requireReleaseAnchor; no network git).
  if (input.targetSha === input.sha) requireTargetCommit(process.cwd(), input.sha, input.targetSha);
  else
    requireReleaseAnchor({ repo: process.cwd(), releaseSha: input.sha, targetSha: input.targetSha, target: input.target });
  const client = releaseClient(process.env.SUPABASE_MIGRATION_OWNER_URL);
  try {
    await client.connect();
    const report = await inspectRelease(client, input.boundary, input.target);
    // The same target gate the CI plan records: held steps are never applied.
    const split = migrationTargetSplit(report.inventory, input.target);
    // The generation an apply of this plan marks PASS: target_sha's, whose tree must end
    // at the target with exactly this checkout's prefix (the full one when nothing is held).
    const anchor = releaseAnchorBinding({
      repo: process.cwd(),
      releaseSha: input.sha,
      targetSha: input.targetSha,
      target: input.target,
      prefix: await schemaStepDigests(split.held),
    });
    const targetGeneration = anchor.targetGeneration;
    if (!split.held.length && targetGeneration !== input.generation)
      refuse("release_target_generation_changed");
    // The same bootstrap section the CI plan records (READ ONLY; refuses role drift).
    const bootstrap = await inspectBootstrap(client, {
      ...input,
      steps: anchor.manifest.steps.map(([name]) => name),
    });
    console.log(
      JSON.stringify(
        {
          version: 1,
          status: "planned",
          mode: "read_only_operator_plan",
          project: P.project,
          sha: input.sha,
          boundary: input.boundary,
          target: input.target,
          ...split,
          targetSha: anchor.targetSha,
          targetGeneration,
          manifestDigest: anchor.manifestDigest,
          targetManifest: anchor.manifest,
          ...report,
          bootstrap,
        },
        null,
        2,
      ),
    );
  } finally {
    await client.end().catch(() => {});
  }
}
if (import.meta.main)
  // Scrub engine (docs/scrub-engine.md, "Default flip"): the pinned wasm-bindgen engine, read from the repo checkout.
  initScrubEngine().then(() => operatorPlan()).catch((error) => {
    console.log(
      JSON.stringify({
        version: 1,
        status: "refused",
        code: releaseErrorCode(error),
      }),
    );
    process.exitCode = 1;
  });
