/** Only the protected workflow's single secret-scoped step invokes this entry point. */
import { execFileSync } from "node:child_process";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { generation, runMigrations } from "../src/hoopdb/migrate.ts";
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
export function validateReleasePlan(
  plan: any,
  input: { sha: string; generation: string; boundary: string; planRun: string },
) {
  if (
    plan?.version !== 1 ||
    plan.project !== P.project ||
    plan.sha !== input.sha ||
    plan.generation !== input.generation ||
    plan.boundary !== input.boundary ||
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
  };
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
  const profile = releaseProfile(input.boundary);
  const approvedPlan =
    input.mode === "apply"
      ? validateReleasePlan(
          JSON.parse(await readFile("release-plan/plan.json", "utf8")),
          input,
        )
      : undefined;
  const client = releaseClient(process.env.SUPABASE_MIGRATION_OWNER_URL);
  try {
    await client.connect();
    const inspection = await inspectRelease(client, input.boundary);
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
            runId: process.env.GITHUB_RUN_ID,
            ...inspection,
          },
          null,
          2,
        ) + "\n",
        { mode: 0o600 },
      );
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
    await runMigrations(client, {
      profile,
      expectedLedgerSha256: approvedPlan!.ledgerSha256,
      beforeWrite: async () => {
        await githubReleaseGate(process.env, get, true);
        await refreshReleaseBoundary(client, approvedPlan!.existingBoundary);
      },
    });
    const marker = (
      await client.query(
        `SELECT generation,status FROM ${ROLE_VALIDATION_TABLE} WHERE name=$1`,
        [ROLE_VALIDATION_NAME],
      )
    ).rows[0];
    if (marker?.status !== "pass" || marker.generation !== input.generation)
      refuse("release_marker");
    await verifyReleaseBoundary(client);
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
