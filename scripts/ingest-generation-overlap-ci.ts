/**
 * CI entry point for the reviewed generation-overlap window (database job of
 * .github/workflows/ingest-generation-overlap.yml). Inputs are validated against the reviewed pair
 * before the owner credential is read; the connection is the schema release's strict TLS owner
 * connection and must prove the pinned owner identity. Prints and stores a nonsecret receipt;
 * errors print a fixed code only. Nothing here runs unless the workflow is dispatched.
 */
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { releaseClient, releaseErrorCode, requireReleaseOwner } from "../src/hoopdb/schema-release-inspect.ts";
import { overlapOperatorInputs, runOverlapOperator } from "../src/hoopdb/ingest-generation-overlap-operator.ts";

const RECEIPT = resolve("release-receipt/ingest-generation-overlap.json");

async function main() {
  const input = overlapOperatorInputs(process.env);
  const client = releaseClient(process.env.SUPABASE_MIGRATION_OWNER_URL);
  await client.connect();
  try {
    await requireReleaseOwner(client);
    const receipt = {
      ...(await runOverlapOperator(client, input)),
      at: new Date().toISOString(),
      sourceCommit: process.env.GITHUB_SHA ?? null,
      credentialsPersisted: false,
    };
    mkdirSync(dirname(RECEIPT), { recursive: true });
    writeFileSync(RECEIPT, JSON.stringify(receipt, null, 2) + "\n", { mode: 0o600 });
    console.log(JSON.stringify(receipt));
  } finally {
    await client.end().catch(() => {});
  }
}

if (import.meta.main)
  main().catch((error) => {
    console.error("ingest_generation_overlap_refused " + releaseErrorCode(error));
    process.exitCode = 1;
  });
